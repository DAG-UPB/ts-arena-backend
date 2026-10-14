"""The rescore of rows whose forecasts miss deciles, on faked repositories."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pytest

import app.scripts.rescore_missing_deciles as script
from app.scripts.rescore_missing_deciles import (
    MAE_TOLERANCE,
    SQL_FIELDS,
    Summary,
    _parse_args,
    relative_deviation,
    rescore_pair,
    rescore_round,
    sql_update,
)
from app.services.evaluation_alignment import align_evaluation_data, group_actuals_by_minute
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


def _actual_rows(wobble=0.0):
    return [{"ts": T0 + i * STEP, "value": 10.5 + i + wobble * (i % 2)} for i in range(N)]


def _actuals(wobble=0.0):
    return group_actuals_by_minute(_actual_rows(wobble))


def _row_before_the_fill(forecasts, actuals):
    """A row as it was stored while only the submitted deciles were scored."""
    row = build_score_row(1, 2, 3, len(forecasts), align_evaluation_data(forecasts, actuals), SCALE_ROW, True)
    y_true = np.array([v[0] for v in actuals.values()])
    submitted = {
        level: np.array([f["probabilistic_values"][f"q_{level}"] for f in forecasts]) for level in (0.1, 0.5, 0.9)
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


def test_a_row_on_another_scale_method_does_not_reproduce():
    forecasts, actuals = _forecasts(), _actuals()
    stored = _row_before_the_fill(forecasts, actuals)

    assert rescore_pair(stored, forecasts, actuals, SCALE_ROW)[1]
    assert not rescore_pair({**stored, "method": "hk2006_context_m1"}, forecasts, actuals, SCALE_ROW)[1]


def test_relative_deviation():
    assert relative_deviation(2.0, 2.0) == 0.0
    assert relative_deviation(2.2, 2.0) == pytest.approx(0.1)
    assert relative_deviation(1.0, 0.0) == float("inf") and relative_deviation(0.0, 0.0) == 0.0


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
    assert len(summary.sql_ratio) == 3
    report = summary.report(True)
    assert "not reproduced" in report and "Max relative mae deviation among rescored rows: 5.0e-05" in report


class _Calls:
    forecast_reads = []
    actual_reads = []


class _FakeRoundRepo:
    def __init__(self, session):
        pass

    async def get_by_id(self, round_id):
        return SimpleNamespace(frequency=STEP, horizon=N * STEP)

    async def get_series_context_edges(self, round_id):
        return {3: T0 - STEP}


class _FakeForecastRepo:
    rows = []

    def __init__(self, session):
        pass

    async def get_round_forecasts(self, round_id, ts_lo=None, ts_hi=None):
        _Calls.forecast_reads.append((round_id, ts_lo, ts_hi))
        return self.rows


class _FakeScoresRepo:
    stored = []

    def __init__(self, session):
        pass

    async def get_scales(self, round_id):
        return {3: SCALE_ROW}

    async def get_score_rows(self, round_id):
        return self.stored

    async def read_actuals_as_of(self, series_ids, bucket, as_of, lo, hi):
        _Calls.actual_reads.append((series_ids, bucket, as_of, lo, hi))
        return [{"series_id": 3, **row} for row in _actual_rows()]


@pytest.mark.asyncio
async def test_a_round_is_rescored_over_the_whole_window_of_each_pair(monkeypatch):
    # Only the first three points carry three deciles; the rest carry all nine.
    own = _forecasts()
    for i in range(3, N):
        own[i]["probabilistic_values"] = {f"q_0.{j}": own[i]["predicted_value"] + (j - 5) for j in range(1, 10)}
    out_of_window = [{**own[-1], "ts": T0 + N * STEP}]
    _FakeForecastRepo.rows = [
        {"model_id": model_id, "series_id": 3, **point}
        for model_id, points in ((2, own + out_of_window), (9, _forecasts()))
        for point in points
    ]
    stored = build_score_row(1, 2, 3, N, align_evaluation_data(own, _actuals()), SCALE_ROW, True)
    stored.update(calculated_at=T0 + timedelta(days=10), sql_score=0.5, sql_per_quantile={"0.5": 0.5})
    _FakeScoresRepo.stored = [stored]
    _Calls.forecast_reads, _Calls.actual_reads = [], []
    monkeypatch.setattr(script, "ForecastRepository", _FakeForecastRepo)
    monkeypatch.setattr(script, "ForecastScoresRepository", _FakeScoresRepo)
    monkeypatch.setattr(script, "ChallengeRoundRepository", _FakeRoundRepo)

    summary = Summary()
    updates = await rescore_round(object(), 1, [(2, 3), (4, 3)], summary)

    assert [(u["model_id"], u["series_id"]) for u in updates] == [(2, 3)]
    assert summary.mae_exact == 1 and summary.to_write == 1
    assert summary.not_reproduced == [(1, 4, 3, None, None)]
    assert _Calls.forecast_reads == [(1, None, None)]
    assert _Calls.actual_reads == [([3], STEP, T0 + timedelta(days=10), T0 - STEP, T0 + (N - 1) * STEP)]


def test_arguments():
    args = _parse_args(["--dry-run", "--round-id", "7"])
    assert args.dry_run and args.round_id == 7
    assert not _parse_args([]).dry_run
