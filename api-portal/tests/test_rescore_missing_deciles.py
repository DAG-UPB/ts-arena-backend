"""The pure parts of the rescore of rows whose forecasts miss deciles."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from app.scripts.rescore_missing_deciles import (
    MAE_TOLERANCE,
    SQL_FIELDS,
    Summary,
    _parse_args,
    relative_deviation,
    rescore_pair,
    same_value,
    sql_update,
)
from app.services.evaluation_alignment import group_actuals_by_minute
from app.services.forecast_metrics import sql_score
from app.services.forecast_scoring_service import build_score_row

T0 = datetime(2026, 8, 7, 20, tzinfo=timezone.utc)
STEP = timedelta(minutes=15)
SCALE_ROW = {"scale": 2.0, "last_value": 10.0}
N = 6


def _forecasts():
    return [
        {
            "ts": T0 + i * STEP,
            "predicted_value": 10.0 + i,
            "probabilistic_values": {"q_0.1": 8.0 + i, "q_0.5": 10.0 + i, "q_0.9": 13.0 + i},
        }
        for i in range(N)
    ]


def _actuals(wobble=0.0):
    return group_actuals_by_minute(
        [{"ts": T0 + i * STEP, "value": 10.5 + i + wobble * (i % 2)} for i in range(N)]
    )


def _row_before_the_fill(forecasts, actuals):
    """A row as it was stored while only the submitted deciles were scored."""
    row = build_score_row(
        1, 2, 3, len(forecasts), [
            {"predicted_value": f["predicted_value"], "probabilistic_values": f["probabilistic_values"], "actual_value": a}
            for f, a in zip(forecasts, (v[0] for v in actuals.values()))
        ],
        SCALE_ROW, True,
    )
    y_true = np.array([v[0] for v in actuals.values()])
    submitted = {
        0.1: np.array([f["probabilistic_values"]["q_0.1"] for f in forecasts]),
        0.5: np.array([f["probabilistic_values"]["q_0.5"] for f in forecasts]),
        0.9: np.array([f["probabilistic_values"]["q_0.9"] for f in forecasts]),
    }
    overall, per_level = sql_score(y_true, submitted, SCALE_ROW["scale"])
    row.update(sql_score=overall, sql_per_quantile={f"{k:.1f}": v for k, v in per_level.items()})
    return row


def test_a_row_scored_on_three_deciles_is_rescored_on_all_nine():
    forecasts, actuals = _forecasts(), _actuals()
    stored = _row_before_the_fill(forecasts, actuals)
    assert len(stored["sql_per_quantile"]) == 3

    fresh, reproduced = rescore_pair(stored, forecasts, actuals, SCALE_ROW)
    update = sql_update(stored, fresh)

    assert reproduced
    assert set(update) == {"round_id", "model_id", "series_id", *SQL_FIELDS}
    assert (update["round_id"], update["model_id"], update["series_id"]) == (1, 2, 3)
    assert update["has_quantiles"] is True and update["quantile_levels_count"] == 3
    assert set(update["sql_per_quantile"]) == {f"0.{i}" for i in range(1, 10)}
    assert update["sql_score"] == pytest.approx(np.mean(list(update["sql_per_quantile"].values())))
    assert update["sql_score"] != pytest.approx(stored["sql_score"])
    assert fresh["mae"] == stored["mae"] and fresh["mase"] == stored["mase"]


def test_a_row_that_already_carries_the_new_values_is_left_alone():
    forecasts, actuals = _forecasts(), _actuals()
    stored = _row_before_the_fill(forecasts, actuals)
    fresh, _ = rescore_pair(stored, forecasts, actuals, SCALE_ROW)
    current = {**stored, **{k: fresh[k] for k in SQL_FIELDS}}

    assert sql_update(current, rescore_pair(current, forecasts, actuals, SCALE_ROW)[0]) is None


def test_actuals_that_move_the_mae_beyond_the_tolerance_do_not_reproduce_the_row():
    forecasts = _forecasts()
    stored = _row_before_the_fill(forecasts, _actuals())

    fresh, reproduced = rescore_pair(stored, forecasts, _actuals(wobble=1e-3), SCALE_ROW)

    assert relative_deviation(fresh["mae"], stored["mae"]) > MAE_TOLERANCE
    assert not reproduced


def test_a_small_deviation_in_mae_still_reproduces_the_row():
    forecasts = _forecasts()
    stored = _row_before_the_fill(forecasts, _actuals())

    fresh, reproduced = rescore_pair(stored, forecasts, _actuals(wobble=1e-5), SCALE_ROW)

    assert relative_deviation(fresh["mae"], stored["mae"]) == pytest.approx(1e-5, rel=1e-6)
    assert reproduced and sql_update(stored, fresh) is not None


def test_a_missing_actual_or_another_scale_does_not_reproduce_the_row():
    forecasts, actuals = _forecasts(), _actuals()
    stored = _row_before_the_fill(forecasts, actuals)

    fewer = dict(list(actuals.items())[:-1])
    assert not rescore_pair(stored, forecasts, fewer, SCALE_ROW)[1]
    assert not rescore_pair(stored, forecasts, actuals, {**SCALE_ROW, "scale": 4.0})[1]
    assert not rescore_pair(stored, forecasts, actuals, {**SCALE_ROW, "scale": 2.0 * (1 + 1e-12)})[1]
    assert not rescore_pair(stored, [], actuals, SCALE_ROW)[1]


def test_relative_deviation_and_same_value():
    assert relative_deviation(2.0, 2.0) == 0.0
    assert relative_deviation(2.2, 2.0) == pytest.approx(0.1)
    assert relative_deviation(1.0, 0.0) == float("inf") and relative_deviation(0.0, 0.0) == 0.0
    assert same_value(1.0, 1.0 + 1e-14) and not same_value(1.0, 1.0 + 1e-6)
    assert same_value(None, None) and not same_value(None, 1.0) and not same_value(1.0, None)
    assert same_value({"0.1": 1.0, "0.5": 2.0}, {"0.1": 1.0, "0.5": 2.0})
    assert not same_value({"0.1": 1.0}, {"0.1": 1.0, "0.5": 2.0})
    assert not same_value({"0.1": 1.0}, None)
    assert same_value(3, 3) and not same_value(True, False)


def test_summary_counts_exact_close_and_unverified_rows():
    forecasts, actuals = _forecasts(), _actuals()
    stored = _row_before_the_fill(forecasts, actuals)
    fresh, _ = rescore_pair(stored, forecasts, actuals, SCALE_ROW)
    update = sql_update(stored, fresh)

    summary = Summary()
    summary.record_row(stored, fresh, True, update)
    summary.record_row(stored, {**fresh, "mae": stored["mae"] * (1 + 1e-12)}, True, update)
    summary.record_row(stored, {**fresh, "mae": stored["mae"] * (1 + 5e-5)}, True, update)
    summary.record_row(stored, {**fresh, "mae": stored["mae"] * 1.001}, False, update)
    summary.record_row(stored, fresh, True, None)

    assert (summary.mae_exact, summary.mae_close, len(summary.not_reproduced)) == (2, 2, 1)
    assert summary.mae_max_rel_diff == pytest.approx(5e-5, rel=1e-3)
    assert (summary.to_write, summary.unchanged) == (3, 1)
    assert summary.transitions[(True, True)] == 3
    assert len(summary.sql_ratio) == 3 and len(summary.sql_ratio_unverified) == 1
    report = summary.report(True, {})
    assert "not reproduced" in report and "Max relative mae deviation among rescored rows: 5.0e-05" in report


def test_arguments():
    args = _parse_args(["--dry-run", "--round-id", "7"])
    assert args.dry_run and args.round_id == 7
    assert not _parse_args([]).dry_run
