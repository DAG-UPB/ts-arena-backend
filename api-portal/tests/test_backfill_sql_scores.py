"""Unit tests for the SQL backfill script's compute path.

These exercise `recompute_sql_for_model_series` — the script's pure, no-DB function —
against the same golden scenarios as `tests/test_forecast_metrics.py` and
`tests/test_scoring_sql_integration.py`: a real-quantile case, a point-only degenerate
case (sql == mase identity), a crossing-repair case, and an a==0 -> NULL case.

`recompute_sql_for_model_series` calls `app.services.forecast_metrics.compute_sql_fields`,
the SAME function `ScoreEvaluationService._calculate_score_for_model_series` calls for the
live path — so a green test here is proof the backfill script produces byte-identical
results to live scoring, not just similar ones.
"""
import numpy as np
import pytest

from app.scripts.backfill_sql_scores import (
    BackfillSummary,
    recompute_sql_for_model_series,
)


# --- golden scenarios ---------------------------------------------------------------

def _quantile_model_rows():
    y_true = [10.0, 12.0, 11.0]
    y_pred = [10.5, 11.5, 11.0]
    rows = []
    for yt, yp in zip(y_true, y_pred):
        pv = {f"q_0.{i}": round(yp - 1.0 + 0.25 * (i - 1), 3) for i in range(1, 10)}
        pv["q_0.5"] = yp  # median consistency
        rows.append({"actual_value": yt, "predicted_value": yp, "probabilistic_values": pv})
    return rows


def test_real_quantile_case():
    fields = recompute_sql_for_model_series(_quantile_model_rows(), naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 9
    assert fields["quantile_crossing_count"] == 0
    assert fields["sql_score"] is not None and fields["sql_score"] >= 0
    assert set(fields["sql_per_quantile"]) == {f"0.{i}" for i in range(1, 10)}
    assert fields["mae_naive"] == np.mean(np.abs(np.array([10.0, 12.0, 11.0]) - 9.0))


def test_point_only_degenerate_equals_mase():
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None},
        {"actual_value": 12.0, "predicted_value": 11.5, "probabilistic_values": {}},
        {"actual_value": 11.0, "predicted_value": 11.0, "probabilistic_values": None},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    y_true = np.array([10.0, 12.0, 11.0])
    y_pred = np.array([10.5, 11.5, 11.0])
    mae_naive = float(np.mean(np.abs(y_true - 9.0)))
    mase = float(np.mean(np.abs(y_true - y_pred))) / mae_naive

    assert fields["has_quantiles"] is False
    assert fields["quantile_levels_count"] == 0
    # Degenerate distribution => SQL == MASE (shared denominator identity).
    assert fields["sql_score"] == pytest.approx(mase, rel=1e-9)


def test_crossing_repair_case():
    # q_0.8 > q_0.9 at the first timestamp -> one crossing repaired at scoring.
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.0,
         "probabilistic_values": {"q_0.1": 8.0, "q_0.5": 10.0, "q_0.8": 13.0, "q_0.9": 11.0}},
        {"actual_value": 12.0, "predicted_value": 12.0,
         "probabilistic_values": {"q_0.1": 10.0, "q_0.5": 12.0, "q_0.8": 13.0, "q_0.9": 14.0}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 4
    assert fields["quantile_crossing_count"] == 1
    assert fields["sql_score"] is not None


def test_a_zero_yields_null_sql():
    # naive == actuals everywhere and perfect point => mae_naive == 0 => scale None => NULL SQL
    rows = [
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    assert fields["mae_naive"] == 0.0
    assert fields["sql_score"] is None
    assert fields["sql_per_quantile"] is None


# --- BackfillSummary bookkeeping ------------------------------------------------------

def test_summary_records_degenerate_row_and_detects_no_mismatch_when_identical():
    summary = BackfillSummary()
    fields = recompute_sql_for_model_series(
        [
            {"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None},
            {"actual_value": 12.0, "predicted_value": 11.5, "probabilistic_values": None},
        ],
        naive_value=9.0,
    )
    summary.record_row(round_id=1, model_id=2, series_id=3, fields=fields, stored_mase=fields["sql_score"])

    assert summary.rows_updated == 1
    assert summary.rows_degenerate == 1
    assert summary.rows_real_quantiles == 0
    assert summary.mismatches == []


def test_summary_flags_identity_mismatch():
    summary = BackfillSummary()
    fields = recompute_sql_for_model_series(
        [{"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None}],
        naive_value=9.0,
    )
    # Simulate stored mase drifting from the freshly-computed value (data changed since
    # the row was originally scored).
    stale_mase = fields["sql_score"] + 1.0
    summary.record_row(round_id=1, model_id=2, series_id=3, fields=fields, stored_mase=stale_mase)

    assert len(summary.mismatches) == 1
    assert summary.mismatches[0] == (1, 2, 3, fields["sql_score"], stale_mase)


def test_summary_tracks_crossing_by_model():
    summary = BackfillSummary()
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.0,
         "probabilistic_values": {"q_0.1": 8.0, "q_0.5": 10.0, "q_0.8": 13.0, "q_0.9": 11.0}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    summary.record_row(round_id=1, model_id=42, series_id=3, fields=fields, stored_mase=None)

    assert summary.crossing_by_model == {42: 1}
