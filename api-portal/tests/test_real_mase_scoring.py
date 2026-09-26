"""Context-scaled MASE in the scorer: `forecasts.series_scale` and `forecasts.scores_mase`.

The live scorer writes `scores_mase` next to `scores`, from the same evaluated points and
the same coverage rules, with the (round, series) context scale as denominator. These tests
pin the row semantics, that every model on a series gets the same scale, and that nothing
in the MASE pass can affect `forecasts.scores`.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.forecast_metrics import MASE_METHOD
from app.services.score_evaluation_service import (
    MIN_COVERAGE_FOR_FINAL,
    build_mase_score_row,
    coverage_verdict,
)
from app.services.series_scale_service import (
    SOURCE_REBUILT,
    SOURCE_SERVED,
    SeriesScaleService,
    build_scale_rows,
)
from tests.test_score_evaluation_batching import (
    _by_pair,
    _FakeForecastRepo,
    _FakeRoundRepo,
    _FakeSession,
    _FakeTimeSeriesRepo,
    _service,
    _ts,
)

UTC = timezone.utc
HOUR = timedelta(hours=1)


def _hts(hour):
    """`_ts` that also reaches back before midnight, for context points."""
    return _ts(0) + hour * HOUR


def _aligned(pairs):
    """[(actual, predicted), ...] -> aligned evaluation rows."""
    return [{"actual_value": a, "predicted_value": p, "probabilistic_values": None} for a, p in pairs]


def _scale(value, m=1):
    return {"scale": value, "m": m, "n_points": 10, "n_pairs": 9, "source": SOURCE_REBUILT}


# ---------------------------------------------------------------------------------------
# coverage_verdict: one rule for both tables
# ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "coverage, timeout, expected",
    [
        (1.0, False, ("complete", True)),
        (1.0, True, ("complete", True)),
        (0.5, False, ("partial", False)),
        (0.0, False, ("pending", False)),
        (MIN_COVERAGE_FOR_FINAL, True, ("partial", True)),
        (MIN_COVERAGE_FOR_FINAL - 0.01, True, ("insufficient_data", True)),
    ],
)
def test_coverage_verdict(coverage, timeout, expected):
    assert coverage_verdict(coverage, timeout) == expected


# ---------------------------------------------------------------------------------------
# build_mase_score_row
# ---------------------------------------------------------------------------------------

def test_row_for_a_complete_pair():
    row = build_mase_score_row(7, 1, 10, 2, _aligned([(14.0, 15.0), (16.0, 14.0)]), _scale(1.75), False)
    assert row["evaluation_status"] == "complete" and row["final_evaluation"] is True
    assert row["mae"] == pytest.approx(1.5)
    assert row["mase"] == pytest.approx(1.5 / 1.75)
    assert row["sql_score"] == pytest.approx(row["mase"])  # point-only identity
    assert row["n_points"] == 2 and row["data_coverage"] == 1.0
    assert row["scale"] == 1.75 and row["method"] == MASE_METHOD
    assert row["error_message"] is None


def test_no_forecasts_gives_no_row():
    assert build_mase_score_row(7, 1, 10, 0, [], _scale(1.0), True) is None


@pytest.mark.parametrize("timeout", [False, True])
def test_no_overlap_is_final_only_after_timeout(timeout):
    row = build_mase_score_row(7, 1, 10, 3, [], _scale(1.0), timeout)
    assert row["evaluation_status"] == "no_overlap"
    assert row["final_evaluation"] is timeout
    assert row["mase"] is None and row["mae"] is None


def test_insufficient_coverage_after_timeout_has_no_score():
    row = build_mase_score_row(7, 1, 10, 3, _aligned([(1.0, 2.0)]), _scale(1.0), True)
    assert row["evaluation_status"] == "insufficient_data" and row["final_evaluation"] is True
    assert row["mase"] is None and "coverage" in row["error_message"]


def test_partial_pair_is_scored_but_not_final_before_timeout():
    row = build_mase_score_row(7, 1, 10, 3, _aligned([(1.0, 2.0)]), _scale(2.0), False)
    assert row["evaluation_status"] == "partial" and row["final_evaluation"] is False
    assert row["mase"] == pytest.approx(0.5)


@pytest.mark.parametrize("scale_row", [None, _scale(None), _scale(0.0)])
def test_undefined_scale_excludes_the_pair_without_inf(scale_row):
    row = build_mase_score_row(7, 1, 10, 2, _aligned([(5.0, 5.0), (5.0, 6.0)]), scale_row, False)
    assert row["evaluation_status"] == "undefined_scale"
    assert row["final_evaluation"] is True  # terminal: coverage is complete
    assert row["mase"] is None and row["sql_score"] is None
    assert row["mae"] == pytest.approx(0.5)  # kept, so a later scale is one division
    assert row["error_message"]


def test_rows_share_one_key_set():
    """The upsert takes its column list from the first row, so every row must match it."""
    rows = [
        build_mase_score_row(7, 1, 10, 2, _aligned([(1.0, 2.0), (3.0, 3.0)]), _scale(1.0), False),
        build_mase_score_row(7, 1, 10, 2, [], _scale(1.0), True),
        build_mase_score_row(7, 1, 10, 3, _aligned([(1.0, 2.0)]), _scale(1.0), True),
        build_mase_score_row(7, 1, 10, 2, _aligned([(1.0, 2.0), (3.0, 3.0)]), None, False),
    ]
    assert len({frozenset(r) for r in rows}) == 1


# ---------------------------------------------------------------------------------------
# build_scale_rows
# ---------------------------------------------------------------------------------------

def test_scale_rows_use_the_context_window_only():
    windows = {10: (_ts(0), _ts(3)), 11: (_ts(0), _ts(3)), 12: (_ts(0), _ts(3))}
    context = (
        [{"series_id": 10, "ts": _ts(h), "value": v} for h, v in [(0, 1.0), (1, 3.0), (2, 4.0), (3, 6.0)]]
        # outside the window: must not enter the scale
        + [{"series_id": 10, "ts": _ts(5), "value": 100.0}]
        # one point only: stored, but with no scale
        + [{"series_id": 11, "ts": _ts(1), "value": 2.0}]
        # series 12 has nothing: no row at all
    )
    rows = {r["series_id"]: r for r in build_scale_rows(7, context, windows, HOUR, SOURCE_SERVED)}

    assert set(rows) == {10, 11}
    assert rows[10]["scale"] == pytest.approx(5.0 / 3.0)
    assert (rows[10]["n_points"], rows[10]["n_pairs"], rows[10]["m"]) == (4, 3, 1)
    assert rows[10]["context_start"] == _ts(0) and rows[10]["context_end"] == _ts(3)
    assert rows[10]["source"] == SOURCE_SERVED
    assert rows[11]["scale"] is None and rows[11]["n_pairs"] == 0


# ---------------------------------------------------------------------------------------
# The scorer end to end, on the fakes of test_score_evaluation_batching
# ---------------------------------------------------------------------------------------

class _FakeScaleRepo:
    def __init__(self, stored=None, exists=True, fail_on_upsert=False):
        self.stored = dict(stored or {})
        self.exists = exists
        self.fail_on_upsert = fail_on_upsert
        self.upserted = None
        self.as_of_reads = []

    async def tables_exist(self):
        return self.exists

    async def get_scales(self, round_id):
        return dict(self.stored)

    async def get_context_windows(self, round_id):
        return {10: (_hts(-4), _ts(0)), 11: (_hts(-4), _ts(0))}

    async def read_context_as_of(self, round_id, bucket, as_of, lo, hi):
        self.as_of_reads.append((bucket, as_of, lo, hi))
        # series 10: 1, 2, 4, 7, 11 -> diffs 1, 2, 3, 4 -> scale 2.5; series 11: constant
        return (
            [{"series_id": 10, "ts": _hts(h), "value": v} for h, v in zip(range(-4, 1), [1.0, 2.0, 4.0, 7.0, 11.0])]
            + [{"series_id": 11, "ts": _hts(h), "value": 3.0} for h in range(-4, 1)]
        )

    async def insert_scales(self, rows):
        for row in rows:
            self.stored.setdefault(row["series_id"], row)

    async def upsert_mase_scores(self, rows):
        if self.fail_on_upsert:
            raise RuntimeError("boom")
        self.upserted = rows
        return len(rows)


class _TxSession(_FakeSession):
    def __init__(self):
        super().__init__()
        self.commits = 0
        self.rollbacks = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


class _RoundRepoWithCreation(_FakeRoundRepo):
    async def get_by_id(self, round_id):
        info = await super().get_by_id(round_id)
        info.created_at = _ts(0) + timedelta(minutes=5)
        return info


def _scoring_service(scale_repo):
    forecasts = (
        [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
        + [(2, 10, h, 12.0, None) for h in (1, 2, 3)]
        + [(1, 11, h, 3.0, None) for h in (1, 2, 3)]
        + [(2, 11, h, 4.0, None) for h in (1, 2, 3)]
    )
    actuals = {10: [(h, 10.0) for h in (1, 2, 3)], 11: [(h, 3.0) for h in (1, 2, 3)]}
    repo = _FakeForecastRepo(forecasts, actuals)
    round_repo = _RoundRepoWithCreation(end_time=_ts(4), series_max_ts={10: _ts(0), 11: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0, 11: 5.0}))
    svc.db_session = _TxSession()
    scale_service = SeriesScaleService.__new__(SeriesScaleService)
    scale_service.repo = scale_repo
    svc.scale_service = scale_service
    return svc, repo


@pytest.mark.asyncio
async def test_scorer_writes_context_scaled_mase_with_one_scale_per_series():
    scale_repo = _FakeScaleRepo()
    svc, repo = _scoring_service(scale_repo)

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    rows = _by_pair(scale_repo.upserted)
    assert set(rows) == {(1, 10), (2, 10), (1, 11), (2, 11)}
    # Every model on a series is divided by the same, context-derived scale.
    assert rows[(1, 10)]["scale"] == rows[(2, 10)]["scale"] == pytest.approx(2.5)
    assert rows[(1, 10)]["mase"] == pytest.approx(0.0)
    assert rows[(2, 10)]["mase"] == pytest.approx(2.0 / 2.5)
    # A constant context is excluded for every model alike.
    for model_id in (1, 2):
        assert rows[(model_id, 11)]["evaluation_status"] == "undefined_scale"
        assert rows[(model_id, 11)]["mase"] is None
    # The scale was rebuilt as of round creation and stored once.
    assert scale_repo.as_of_reads[0][1] == _ts(0) + timedelta(minutes=5)
    assert scale_repo.stored[10]["source"] == SOURCE_REBUILT
    # forecasts.scores is untouched: still the flat-naive denominator (naive error 5).
    assert _by_pair(repo.inserted)[(2, 10)]["mase"] == pytest.approx(2.0 / 5.0)
    assert svc.db_session.commits == 1


@pytest.mark.asyncio
async def test_a_stored_scale_is_used_as_is():
    scale_repo = _FakeScaleRepo(stored={10: _scale(4.0), 11: _scale(1.0)})
    svc, _ = _scoring_service(scale_repo)

    await svc.evaluate_challenge_scores(round_id=99)

    assert scale_repo.as_of_reads == []
    rows = _by_pair(scale_repo.upserted)
    assert rows[(2, 10)]["mase"] == pytest.approx(2.0 / 4.0)
    assert rows[(2, 11)]["mase"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_without_the_tables_nothing_is_attempted():
    scale_repo = _FakeScaleRepo(exists=False)
    svc, repo = _scoring_service(scale_repo)

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    assert scale_repo.upserted is None and scale_repo.as_of_reads == []
    assert repo.inserted  # forecasts.scores written as always


@pytest.mark.asyncio
async def test_a_failing_mase_pass_leaves_scores_alone():
    scale_repo = _FakeScaleRepo(fail_on_upsert=True)
    svc, repo = _scoring_service(scale_repo)

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    assert len(repo.inserted) == 4
    assert svc.db_session.rollbacks == 1 and svc.db_session.commits == 0


@pytest.mark.asyncio
async def test_compute_real_mase_does_not_write_scores():
    scale_repo = _FakeScaleRepo()
    svc, repo = _scoring_service(scale_repo)

    rows = await svc.compute_real_mase(99)

    assert repo.inserted is None
    assert {(r["model_id"], r["series_id"]) for r in rows} == {(1, 10), (2, 10), (1, 11), (2, 11)}
