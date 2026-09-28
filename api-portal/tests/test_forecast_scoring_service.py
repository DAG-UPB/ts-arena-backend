"""`ForecastScoringService`: `forecasts.forecast_scores`, scored on its own.

These tests pin the row semantics (the arena scorer's window, coverage and finalisation
rules, with the context scale as the MASE denominator), that every model on a series is
divided by the same scale, how a scheduled run handles its transaction and lock, and that
nothing here depends on the arena scorer or its table, so that one can be deleted.

Repositories are faked; there is no DB in this suite.
"""
import ast
import inspect
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pytest

import app.database.forecasts.forecast_scores_repository as repo_module
import app.services.forecast_scoring_service as service_module
import app.services.series_scale_service as scale_module
from app.database.forecasts.models import ForecastScore
from app.services.forecast_metrics import MASE_METHOD
from app.services.forecast_scoring_service import (
    MIN_COVERAGE_FOR_FINAL,
    ForecastScoringService,
    build_score_row,
    coverage_verdict,
    series_forecast_windows,
)
from app.services.series_scale_service import (
    SOURCE_REBUILT,
    SOURCE_SERVED,
    SeriesScaleService,
    build_scale_rows,
)

UTC = timezone.utc
HOUR = timedelta(hours=1)


def _ts(hour):
    return datetime(2026, 3, 1, tzinfo=UTC) + hour * HOUR


def _aligned(pairs):
    """[(actual, predicted), ...] -> aligned evaluation rows."""
    return [{"actual_value": a, "predicted_value": p, "probabilistic_values": None} for a, p in pairs]


def _scale(value, last_value=5.0):
    return {
        "scale": value, "last_value": last_value, "m": 1, "n_points": 10, "n_pairs": 9,
        "source": SOURCE_REBUILT,
    }


def _by_pair(rows):
    return {(r["model_id"], r["series_id"]): r for r in rows}


# ---------------------------------------------------------------------------------------
# The arena scorer's rules
# ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "coverage, timeout, expected",
    [
        (1.0, False, ("complete", True)),
        (1.0, True, ("complete", True)),
        (0.5, False, ("partial", False)),
        (MIN_COVERAGE_FOR_FINAL, True, ("partial", True)),
        (0.9, True, ("insufficient_data", True)),
    ],
)
def test_coverage_verdict(coverage, timeout, expected):
    assert coverage_verdict(coverage, timeout) == expected


def test_forecast_window_is_per_series():
    windows = series_forecast_windows({10: _ts(0), 11: _ts(-1), 12: None}, HOUR, 3 * HOUR)
    assert windows == {10: (_ts(1), _ts(3)), 11: (_ts(0), _ts(2))}
    assert series_forecast_windows({10: _ts(0)}, None, 3 * HOUR) == {}


# ---------------------------------------------------------------------------------------
# build_score_row
# ---------------------------------------------------------------------------------------

def test_row_for_a_complete_pair():
    row = build_score_row(1, 2, 3, 2, _aligned([(10.0, 12.0), (20.0, 18.0)]), _scale(4.0, 15.0), False)

    assert (row["evaluation_status"], row["final_evaluation"]) == ("complete", True)
    assert row["mae"] == pytest.approx(2.0)
    assert row["rmse"] == pytest.approx(2.0)
    assert row["mase"] == pytest.approx(0.5)
    assert row["naive_mae"] == pytest.approx(5.0)  # |10 - 15|, |20 - 15|
    assert (row["n_points"], row["forecast_count"], row["data_coverage"]) == (2, 2, 1.0)
    assert row["method"] == MASE_METHOD and row["error_message"] is None


@pytest.mark.parametrize("timeout", [False, True])
def test_no_overlap_is_final_only_after_timeout(timeout):
    row = build_score_row(1, 2, 3, 4, [], _scale(1.0), timeout)
    assert row["evaluation_status"] == "no_overlap"
    assert row["final_evaluation"] is timeout
    assert row["mase"] is None and row["mae"] is None


def test_insufficient_coverage_after_timeout_has_no_score():
    row = build_score_row(1, 2, 3, 10, _aligned([(1.0, 1.0)] * 9), _scale(1.0), True)
    assert (row["evaluation_status"], row["final_evaluation"]) == ("insufficient_data", True)
    assert row["mase"] is None and row["mae"] is None
    assert "90.0%" in row["error_message"]


def test_partial_pair_is_scored_but_not_final_before_timeout():
    row = build_score_row(1, 2, 3, 10, _aligned([(1.0, 2.0)] * 5), _scale(1.0), False)
    assert (row["evaluation_status"], row["final_evaluation"]) == ("partial", False)
    assert row["mase"] == pytest.approx(1.0)


@pytest.mark.parametrize("scale_row", [None, _scale(0.0), _scale(None)])
def test_undefined_scale_excludes_the_pair_without_inf(scale_row):
    row = build_score_row(1, 2, 3, 2, _aligned([(1.0, 2.0), (2.0, 2.0)]), scale_row, False)
    assert row["evaluation_status"] == "undefined_scale"
    assert row["mase"] is None and row["sql_score"] is None
    assert row["mae"] == pytest.approx(0.5)


def test_a_non_finite_point_counts_as_missing():
    data = _aligned([(1.0, 2.0)] * 19 + [(1.0, float("nan"))])
    row = build_score_row(1, 2, 3, 20, data, _scale(1.0), True)
    assert (row["n_points"], row["data_coverage"]) == (19, pytest.approx(0.95))
    assert (row["evaluation_status"], row["final_evaluation"]) == ("partial", True)
    assert row["mae"] == pytest.approx(1.0) and np.isfinite(row["mase"])


def test_only_non_finite_points_leave_nothing_to_score():
    row = build_score_row(1, 2, 3, 2, _aligned([(1.0, float("inf")), (float("nan"), 1.0)]), _scale(1.0), True)
    assert (row["evaluation_status"], row["n_points"]) == ("no_overlap", 0)
    assert "finite" in row["error_message"]


def test_rows_share_one_key_set_matching_the_table():
    """The upsert takes its column list from the first row of each chunk."""
    rows = [
        build_score_row(1, 2, 3, 2, _aligned([(1.0, 2.0), (2.0, 2.0)]), _scale(1.0), False),
        build_score_row(1, 2, 3, 2, [], _scale(1.0), True),
        build_score_row(1, 2, 3, 10, _aligned([(1.0, 1.0)]), _scale(1.0), True),
        build_score_row(1, 2, 3, 2, _aligned([(1.0, 2.0)]), None, False),
    ]
    assert len({frozenset(r) for r in rows}) == 1
    columns = {c.name for c in ForecastScore.__table__.columns} - {"calculated_at"}
    assert set(rows[0]) == columns


# ---------------------------------------------------------------------------------------
# Scale rows
# ---------------------------------------------------------------------------------------

def test_scale_rows_use_the_context_window_and_keep_its_last_value():
    windows = {10: (_ts(0), _ts(3)), 11: (_ts(0), _ts(3)), 12: (_ts(0), _ts(3))}
    context = (
        [{"series_id": 10, "ts": _ts(h), "value": v} for h, v in [(3, 6.0), (0, 1.0), (1, 3.0), (2, 4.0)]]
        # outside the window: must not enter the scale or the last value
        + [{"series_id": 10, "ts": _ts(5), "value": 100.0}]
        # a gap at the end: the last value is the newest point that has one
        + [{"series_id": 11, "ts": _ts(1), "value": 2.0}, {"series_id": 11, "ts": _ts(2), "value": None}]
        # series 12 has nothing: no row at all
    )
    rows = {r["series_id"]: r for r in build_scale_rows(7, context, windows, HOUR, SOURCE_SERVED)}

    assert set(rows) == {10, 11}
    assert rows[10]["scale"] == pytest.approx(5.0 / 3.0)
    assert (rows[10]["n_points"], rows[10]["n_pairs"], rows[10]["m"]) == (4, 3, 1)
    assert rows[10]["last_value"] == 6.0
    assert rows[10]["source"] == SOURCE_SERVED
    assert rows[11]["scale"] is None and rows[11]["n_pairs"] == 0
    assert rows[11]["last_value"] == 2.0


# ---------------------------------------------------------------------------------------
# The service end to end, on fakes
# ---------------------------------------------------------------------------------------

class _FakeForecastRepo:
    def __init__(self, forecasts, actuals):
        # forecasts: (model_id, series_id, hour, value); actuals: {series_id: [(hour, value)]}
        self._forecasts = [
            {"model_id": m, "series_id": s, "ts": _ts(h), "predicted_value": v, "probabilistic_values": None}
            for m, s, h, v in forecasts
        ]
        self._actuals = actuals
        self.actual_reads = []

    async def get_round_forecasts(self, round_id, ts_lo=None, ts_hi=None):
        return list(self._forecasts)

    async def get_series_actuals_aggregate(self, series_id, resolution, ts_lo, ts_hi):
        self.actual_reads.append(series_id)
        return [
            {"ts": _ts(h), "value": v}
            for h, v in self._actuals.get(series_id, [])
            if ts_lo <= _ts(h) <= ts_hi
        ]


class _FakeRoundRepo:
    def __init__(self, end_time, edges):
        self._end_time = end_time
        self._edges = edges

    async def get_by_id(self, round_id):
        return SimpleNamespace(
            id=round_id,
            frequency=HOUR,
            horizon=3 * HOUR,
            end_time=self._end_time,
            created_at=_ts(0) + timedelta(minutes=5),
        )

    async def get_series_context_edges(self, round_id):
        return dict(self._edges)


class _FakeScoresRepo:
    """Stands in for ForecastScoresRepository."""

    def __init__(self, stored=None, lock=True, fail_on_upsert=False, capture=(), served=None):
        self.stored = dict(stored or {})
        self.lock = lock
        self.fail_on_upsert = fail_on_upsert
        self.capture = list(capture)
        self.served = served or {}
        self.upserted = None
        self.as_of_reads = []

    async def try_lock_round(self, round_id):
        return self.lock

    async def rounds_awaiting_capture(self, lookback):
        return list(self.capture)

    async def get_scales(self, round_id):
        return dict(self.stored)

    async def get_context_windows(self, round_id):
        return {10: (_ts(-4), _ts(0)), 11: (_ts(-4), _ts(0))}

    async def read_served_context(self, round_id):
        if round_id not in self.served:
            raise RuntimeError("context_data unavailable")
        return list(self.served[round_id])

    async def read_context_as_of(self, round_id, bucket, as_of, lo, hi):
        self.as_of_reads.append((bucket, as_of, lo, hi))
        # series 10: 1, 2, 4, 7, 11 -> diffs 1, 2, 3, 4 -> scale 2.5, last value 11
        # series 11: constant -> scale 0
        return (
            [{"series_id": 10, "ts": _ts(h), "value": v} for h, v in zip(range(-4, 1), [1.0, 2.0, 4.0, 7.0, 11.0])]
            + [{"series_id": 11, "ts": _ts(h), "value": 3.0} for h in range(-4, 1)]
        )

    async def insert_scales(self, rows):
        for row in rows:
            self.stored.setdefault(row["series_id"], row)

    async def upsert_forecast_scores(self, rows):
        if self.fail_on_upsert:
            raise RuntimeError("boom")
        self.upserted = rows
        return len(rows)


class _TxSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


_FORECASTS = (
    [(1, 10, h, 10.0) for h in (1, 2, 3)]
    + [(2, 10, h, 12.0) for h in (1, 2, 3)]
    + [(1, 11, h, 3.0) for h in (1, 2, 3)]
    + [(2, 11, h, 4.0) for h in (1, 2, 3)]
)
_ACTUALS = {10: [(h, 10.0) for h in (1, 2, 3)], 11: [(h, 3.0) for h in (1, 2, 3)]}


def _service(scores_repo, forecasts=_FORECASTS, actuals=_ACTUALS, end_time=None):
    svc = ForecastScoringService.__new__(ForecastScoringService)
    svc.db_session = _TxSession()
    svc.forecast_repo = _FakeForecastRepo(forecasts, actuals)
    svc.round_repo = _FakeRoundRepo(end_time or _ts(4), {10: _ts(0), 11: _ts(0)})
    scale_service = SeriesScaleService.__new__(SeriesScaleService)
    scale_service.repo = scores_repo
    svc.scale_service = scale_service
    svc.raw_actuals_fallback = False
    return svc


@pytest.mark.asyncio
async def test_every_model_on_a_series_is_divided_by_the_same_context_scale():
    repo = _FakeScoresRepo()
    svc = _service(repo)

    assert await svc.score_round(99) == 4

    rows = _by_pair(repo.upserted)
    assert set(rows) == {(1, 10), (2, 10), (1, 11), (2, 11)}
    assert rows[(1, 10)]["scale"] == rows[(2, 10)]["scale"] == pytest.approx(2.5)
    assert rows[(1, 10)]["mase"] == pytest.approx(0.0)
    assert rows[(2, 10)]["mase"] == pytest.approx(2.0 / 2.5)
    # The persistence forecast is the last context value, 11.
    assert rows[(2, 10)]["naive_mae"] == pytest.approx(1.0)
    # A constant context is excluded for every model alike.
    for model_id in (1, 2):
        assert rows[(model_id, 11)]["evaluation_status"] == "undefined_scale"
        assert rows[(model_id, 11)]["mase"] is None
    # The round ended long ago: every row is final.
    assert all(r["final_evaluation"] for r in rows.values())
    # The scale was rebuilt as of round creation and stored once.
    assert repo.as_of_reads[0][1] == _ts(0) + timedelta(minutes=5)
    assert repo.stored[10]["source"] == SOURCE_REBUILT
    # One actuals read per series, not per pair.
    assert sorted(svc.forecast_repo.actual_reads) == [10, 11]
    assert (svc.db_session.commits, svc.db_session.rollbacks) == (1, 0)


@pytest.mark.asyncio
async def test_a_stored_scale_is_used_as_is():
    repo = _FakeScoresRepo(stored={10: _scale(4.0, 12.0), 11: _scale(1.0, 3.0)})
    svc = _service(repo)

    await svc.score_round(99)

    assert repo.as_of_reads == []
    rows = _by_pair(repo.upserted)
    assert rows[(2, 10)]["mase"] == pytest.approx(2.0 / 4.0)
    assert rows[(2, 10)]["naive_mae"] == pytest.approx(2.0)
    assert rows[(2, 11)]["mase"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_out_of_window_points_are_neither_scored_nor_counted():
    # Hour 5 lies beyond the series' window (context edge 0 + 3 h horizon).
    forecasts = _FORECASTS + [(1, 10, 5, 99.0)]
    actuals = {**_ACTUALS, 10: _ACTUALS[10] + [(5, 10.0)]}
    repo = _FakeScoresRepo()
    svc = _service(repo, forecasts=forecasts, actuals=actuals)

    await svc.score_round(99)

    row = _by_pair(repo.upserted)[(1, 10)]
    assert (row["forecast_count"], row["n_points"], row["mae"]) == (3, 3, 0.0)


@pytest.mark.asyncio
async def test_a_round_held_elsewhere_is_skipped_untouched():
    repo = _FakeScoresRepo(lock=False)
    svc = _service(repo)

    assert await svc.score_round(99) is None

    assert repo.upserted is None and repo.as_of_reads == []
    assert (svc.db_session.commits, svc.db_session.rollbacks) == (0, 1)


@pytest.mark.asyncio
async def test_a_failing_write_rolls_the_round_back():
    repo = _FakeScoresRepo(fail_on_upsert=True)
    svc = _service(repo)

    with pytest.raises(RuntimeError):
        await svc.score_round(99)

    assert (svc.db_session.commits, svc.db_session.rollbacks) == (0, 1)


@pytest.mark.asyncio
async def test_a_round_without_forecasts_writes_nothing():
    repo = _FakeScoresRepo()
    svc = _service(repo, forecasts=[])

    assert await svc.compute_round(99) is None
    assert await svc.score_round(99) == 0
    assert repo.upserted is None


@pytest.mark.asyncio
async def test_capture_stores_the_served_scale_and_survives_a_failing_round():
    served = [{"series_id": 10, "ts": _ts(h), "value": v} for h, v in zip(range(-4, 1), [1.0, 3.0, 2.0, 4.0, 6.0])]
    repo = _FakeScoresRepo(capture=[(98, HOUR), (99, HOUR)], served={99: served})
    svc = _service(repo)

    assert await svc.capture_served_scales() == 1

    assert repo.stored[10]["source"] == SOURCE_SERVED
    assert repo.stored[10]["scale"] == pytest.approx((2 + 1 + 2 + 2) / 4)
    assert repo.stored[10]["last_value"] == 6.0
    assert (svc.db_session.commits, svc.db_session.rollbacks) == (1, 1)


# ---------------------------------------------------------------------------------------
# Independence from the arena scorer
# ---------------------------------------------------------------------------------------

def _imports(module):
    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.add(node.module)
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
    return names


def _code_strings(module):
    """String constants other than docstrings, i.e. the SQL a module runs."""
    tree = ast.parse(inspect.getsource(module))
    docstrings = {
        ast.get_docstring(node, clean=False)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    return [
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value not in docstrings
    ]


@pytest.mark.parametrize("module", [service_module, repo_module, scale_module])
def test_nothing_here_depends_on_the_arena_scorer(module):
    imports = _imports(module)
    assert "app.services.score_evaluation_service" not in imports
    assert "ChallengeScore" not in imports
    for value in _code_strings(module):
        assert not re.search(r"forecasts\.scores\b", value), value
