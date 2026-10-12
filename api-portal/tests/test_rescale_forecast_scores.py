"""The rescale's row arithmetic and its resume rule."""
from datetime import timedelta

import numpy as np
import pytest

from app.services.forecast_metrics import MASE_METHOD, compute_score_fields
from app.services.forecast_scoring_service import NO_CONTEXT_MESSAGE, UNDEFINED_SCALE_MESSAGE
from app.scripts.rescale_forecast_scores import (
    Summary,
    _parse_args,
    expected_lag,
    is_rescaled,
    rescale_score_row,
)

OLD_METHOD = "hk2006_context_m1"


def _row(scale=2.0, status="complete", has_quantiles=False, probabilistic=None, **overrides):
    y_true = np.array([10.0, 14.0, 9.0])
    y_pred = np.array([12.0, 13.0, 9.5])
    fields = compute_score_fields(y_true, y_pred, probabilistic or [None] * 3, scale, None)
    row = {
        "round_id": 1, "model_id": 2, "series_id": 3,
        "mae": fields["mae"], "scale": scale, "mase": fields["mase"],
        "sql_score": fields["sql_score"], "sql_per_quantile": fields["sql_per_quantile"],
        "has_quantiles": fields["has_quantiles"], "data_coverage": 1.0, "final_evaluation": True,
        "evaluation_status": status, "error_message": None, "method": OLD_METHOD,
    }
    row.update(overrides)
    return row


def _scale_row(scale):
    return {"series_id": 3, "scale": scale, "m": 24}


def test_rescaled_row_equals_a_fresh_score_on_the_new_scale():
    pv = [{"q_0.1": 9.0, "q_0.5": 12.0, "q_0.9": 15.0}] * 3
    row = _row(scale=2.0, probabilistic=pv)
    update = rescale_score_row(row, _scale_row(5.0))
    fresh = compute_score_fields(np.array([10.0, 14.0, 9.0]), np.array([12.0, 13.0, 9.5]), pv, 5.0, None)

    assert update["scale"] == 5.0 and update["method"] == MASE_METHOD
    assert update["mase"] == pytest.approx(fresh["mase"], rel=1e-12)
    assert update["sql_score"] == pytest.approx(fresh["sql_score"], rel=1e-12)
    assert update["sql_per_quantile"] == pytest.approx(fresh["sql_per_quantile"], rel=1e-12)
    assert update["evaluation_status"] == "complete"


def test_point_only_sql_still_equals_mase():
    update = rescale_score_row(_row(scale=1.75), _scale_row(10.5))
    assert update["sql_score"] == pytest.approx(update["mase"], rel=1e-12)
    assert update["mase"] == _row()["mae"] / 10.5


@pytest.mark.parametrize("scale_row, message", [(None, NO_CONTEXT_MESSAGE), (_scale_row(None), UNDEFINED_SCALE_MESSAGE), (_scale_row(0.0), UNDEFINED_SCALE_MESSAGE)])
def test_a_scale_lost_to_the_longer_lag_leaves_the_row_unscored(scale_row, message):
    update = rescale_score_row(_row(status="partial"), scale_row)
    assert update["evaluation_status"] == "undefined_scale" and update["error_message"] == message
    assert update["mase"] is None and update["sql_score"] is None and update["sql_per_quantile"] is None
    assert update["scale"] == (scale_row["scale"] if scale_row else None)


def test_a_scale_gained_needs_the_forecasts():
    row = _row(scale=None, status="undefined_scale", error_message=UNDEFINED_SCALE_MESSAGE)
    assert row["mase"] is None
    assert rescale_score_row(row, _scale_row(3.0)) is None


def test_an_undefined_scale_that_stays_undefined_keeps_its_status():
    row = _row(scale=0.0, status="undefined_scale", error_message="old message")
    update = rescale_score_row(row, _scale_row(None))
    assert update["evaluation_status"] == "undefined_scale" and update["error_message"] == UNDEFINED_SCALE_MESSAGE


@pytest.mark.parametrize("status", ["no_overlap", "insufficient_data", "error"])
def test_unscored_rows_only_take_the_new_scale(status):
    row = _row(status=status, mase=None, sql_score=None, sql_per_quantile=None, error_message="why")
    update = rescale_score_row(row, _scale_row(4.0))
    assert update["scale"] == 4.0 and update["method"] == MASE_METHOD
    assert (update["evaluation_status"], update["error_message"], update["mase"]) == (status, "why", None)


def test_expected_lag_follows_the_round_frequency():
    assert expected_lag(timedelta(minutes=15)) == 96
    assert expected_lag(timedelta(hours=1)) == 24
    assert expected_lag(timedelta(days=1)) == 7


def test_a_round_is_done_only_with_both_the_new_lag_and_the_new_method():
    hour = timedelta(hours=1)
    assert is_rescaled(hour, [24], [MASE_METHOD])
    assert is_rescaled(hour, [24], [])
    assert not is_rescaled(hour, [1], [MASE_METHOD])  # scored by the new code on an old scale
    assert not is_rescaled(hour, [24], [MASE_METHOD, OLD_METHOD])
    assert not is_rescaled(timedelta(minutes=15), [24], [MASE_METHOD])


def test_summary_checks_the_invariants():
    summary = Summary()
    rows = [_row(scale=2.0), _row(scale=2.0, model_id=5)]
    updates = [rescale_score_row(r, _scale_row(4.0)) for r in rows]
    updates[1]["sql_score"] = 99.0
    summary.record_rows("1h", True, rows, updates)
    assert summary.rows == 2
    assert summary.identity_checked == 2 and len(summary.identity_mismatches) == 1
    assert summary.mase_mismatches == []
    assert summary.mase_new_recent["1h"] == [updates[0]["mase"], updates[1]["mase"]]
    assert summary.transitions[("complete", "complete")] == 2


def test_arguments():
    args = _parse_args(["--dry-run", "--round-id", "7"])
    assert args.dry_run and args.round_id == 7 and args.limit is None and args.workers == 1
    with pytest.raises(SystemExit):
        _parse_args(["--workers", "0"])
