"""Scoring-integration tests for SQL.

These exercise the exact derivation `_calculate_score_for_model_series` performs to turn
aligned evaluation rows + the naive baseline into the stored SQL fields, without requiring a
database (the DB wiring itself is verified separately against dev). Three model archetypes:
a full-quantile model, a point-only model, and a crossing model.
"""
import numpy as np

from app.services.forecast_metrics import assemble_quantile_forecasts, sql_score


def _derive_score_fields(evaluation_data, naive_value):
    """Mirror of the SQL block in ScoreEvaluationService._calculate_score_for_model_series."""
    y_pred = np.array([r["predicted_value"] for r in evaluation_data])
    y_true = np.array([r["actual_value"] for r in evaluation_data])
    mae_naive = float(np.mean(np.abs(y_true - naive_value)))

    probabilistic_values = [r.get("probabilistic_values") for r in evaluation_data]
    qf, has_q, levels_count, crossing_count = assemble_quantile_forecasts(y_pred, probabilistic_values)
    scale = mae_naive if mae_naive > 0 else None
    overall, per_level = sql_score(y_true, qf, scale)
    per_quantile = {f"{lvl:.1f}": v for lvl, v in per_level.items()} if per_level else None
    return {
        "sql_score": overall,
        "sql_per_quantile": per_quantile,
        "has_quantiles": has_q,
        "quantile_levels_count": levels_count,
        "quantile_crossing_count": crossing_count,
        "mase": (float(np.mean(np.abs(y_true - y_pred))) / mae_naive) if mae_naive > 0 else 0.0,
    }


def _quantile_model_rows():
    y_true = [10.0, 12.0, 11.0]
    y_pred = [10.5, 11.5, 11.0]
    rows = []
    for yt, yp in zip(y_true, y_pred):
        pv = {f"q_0.{i}": round(yp - 1.0 + 0.25 * (i - 1), 3) for i in range(1, 10)}
        pv["q_0.5"] = yp  # median consistency
        rows.append({"actual_value": yt, "predicted_value": yp, "probabilistic_values": pv})
    return rows


def test_quantile_model_scored_with_quantiles():
    fields = _derive_score_fields(_quantile_model_rows(), naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 9
    assert fields["quantile_crossing_count"] == 0
    assert fields["sql_score"] is not None and fields["sql_score"] >= 0
    assert set(fields["sql_per_quantile"]) == {f"0.{i}" for i in range(1, 10)}


def test_point_only_model_degenerate_equals_mase():
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None},
        {"actual_value": 12.0, "predicted_value": 11.5, "probabilistic_values": {}},
        {"actual_value": 11.0, "predicted_value": 11.0, "probabilistic_values": None},
    ]
    fields = _derive_score_fields(rows, naive_value=9.0)
    assert fields["has_quantiles"] is False
    assert fields["quantile_levels_count"] == 0
    # Degenerate distribution => SQL == MASE (shared denominator).
    assert fields["sql_score"] == fields["mase"]


def test_crossing_model_repaired_and_counted():
    # q_0.8 > q_0.9 at the first timestamp -> one crossing repaired at scoring.
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.0,
         "probabilistic_values": {"q_0.1": 8.0, "q_0.5": 10.0, "q_0.8": 13.0, "q_0.9": 11.0}},
        {"actual_value": 12.0, "predicted_value": 12.0,
         "probabilistic_values": {"q_0.1": 10.0, "q_0.5": 12.0, "q_0.8": 13.0, "q_0.9": 14.0}},
    ]
    fields = _derive_score_fields(rows, naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 4
    assert fields["quantile_crossing_count"] == 1
    assert fields["sql_score"] is not None


def test_undefined_scale_yields_null_sql():
    # naive == actuals everywhere and perfect point => mae_naive == 0 => scale None => NULL SQL
    rows = [
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
    ]
    fields = _derive_score_fields(rows, naive_value=9.0)
    assert fields["sql_score"] is None
    assert fields["sql_per_quantile"] is None
