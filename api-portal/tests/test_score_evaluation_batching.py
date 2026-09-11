"""Batched evaluation fetch (backend-68): same scores, far fewer queries.

The rewrite of `ScoreEvaluationService.evaluate_challenge_scores` changed only *where* the
data comes from — one round-wide forecast fetch plus one actuals/naive lookup per series,
instead of four queries per (model, series). These tests pin both halves of that claim:

1. the score rows produced are exactly what the per-pair path produced, including the
   coverage/status/finalization edge cases, and
2. the query count is per-round and per-series, not per-pair — the actual point of the
   change, and the thing a future refactor is most likely to silently undo.

Repositories are faked; there is no DB in this suite. The fakes count their calls, which is
what makes assertion 2 possible at all.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.score_evaluation_service import ScoreEvaluationService

UTC = timezone.utc


def _ts(hour):
    return datetime(2026, 3, 1, hour, 0, tzinfo=UTC)


class _FakeForecastRepo:
    """Stands in for ForecastRepository over an in-memory set of forecast rows."""

    def __init__(self, forecasts, actuals):
        # forecasts: list of (model_id, series_id, hour, value, pv)
        self._forecasts = [
            {
                "model_id": m,
                "series_id": s,
                "ts": _ts(h),
                "predicted_value": v,
                "probabilistic_values": pv,
            }
            for m, s, h, v, pv in forecasts
        ]
        # actuals: {series_id: [(hour, value), ...]}
        self._actuals = actuals
        self.calls = {"stats": 0, "round_forecasts": 0, "series_actuals": 0}
        self.inserted = None

    async def get_round_forecast_stats(self, round_id):
        self.calls["stats"] += 1
        grouped = {}
        for row in self._forecasts:
            key = (row["model_id"], row["series_id"])
            grouped.setdefault(key, []).append(row["ts"])
        return [
            {
                "model_id": m,
                "series_id": s,
                "min_ts": min(tss),
                "max_ts": max(tss),
                "count": len(tss),
            }
            for (m, s), tss in sorted(grouped.items())
        ]

    async def get_round_forecasts(self, round_id, ts_lo=None, ts_hi=None):
        self.calls["round_forecasts"] += 1
        return list(self._forecasts)

    async def get_series_actuals_aggregate(self, series_id, resolution, ts_lo, ts_hi):
        self.calls["series_actuals"] += 1
        return [
            {"ts": _ts(h), "value": v}
            for h, v in self._actuals.get(series_id, [])
            if ts_lo <= _ts(h) <= ts_hi
        ]

    async def bulk_insert_scores(self, scores):
        self.inserted = scores
        return len(scores)


class _FakeRoundRepo:
    def __init__(self, end_time, series_max_ts):
        self._end_time = end_time
        self._series_max_ts = series_max_ts
        self.calls = {"pseudo": 0, "context_edges": 0}

    async def get_by_id(self, round_id):
        return SimpleNamespace(
            id=round_id,
            frequency=timedelta(hours=1),
            horizon=timedelta(hours=3),
            end_time=self._end_time,
        )

    async def get_series_pseudo(self, round_id, series_id):
        self.calls["pseudo"] += 1
        max_ts = self._series_max_ts.get(series_id)
        return SimpleNamespace(max_ts=max_ts) if max_ts else None

    async def get_series_context_edges(self, round_id):
        self.calls["context_edges"] += 1
        return dict(self._series_max_ts)


class _FakeTimeSeriesRepo:
    def __init__(self, context_values):
        self._context_values = context_values
        self.calls = {"context": 0}

    async def get_data_by_time_range_by_resolution(
        self, series_id, start_time, end_time, resolution
    ):
        self.calls["context"] += 1
        value = self._context_values.get(series_id)
        return [{"ts": start_time, "value": value}] if value is not None else []


class _FakeSession:
    """Only the advisory lock round-trips go through the session here."""

    def __init__(self):
        self._acquired = False

    async def execute(self, _stmt):
        self._acquired = True
        return SimpleNamespace(scalar=lambda: True)


def _service(forecast_repo, round_repo, time_series_repo):
    svc = ScoreEvaluationService.__new__(ScoreEvaluationService)
    svc.forecast_repo = forecast_repo
    svc.round_repo = round_repo
    svc.time_series_repo = time_series_repo
    svc.db_session = _FakeSession()
    return svc


def _by_pair(scores):
    return {(s["model_id"], s["series_id"]): s for s in scores}


# --- backend-87: scoring is bounded by the window the series was issued ------

@pytest.mark.asyncio
async def test_out_of_window_points_are_not_scored():
    """A point outside the series' window is excluded from the score and from coverage.

    Forecast timestamps went unvalidated on upload until backend-87, and the score is an
    inner join on (series_id, ts) with no window predicate — so a point placed over an
    already-published stretch was evaluated like any other. Here hour 8 is past the 3 h
    horizon; it has a matching actual and would otherwise be scored.
    """
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3, 8)]
    actuals = {10: [(1, 10.0), (2, 10.0), (3, 10.0), (8, 10.0)]}
    repo = _FakeForecastRepo(forecasts, actuals)
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    score = _by_pair(repo.inserted)[(1, 10)]
    # Four were submitted; only the three inside [_ts(1), _ts(3)] count, and coverage is
    # measured against those three rather than against all four.
    assert score["forecast_count"] == 3
    assert score["evaluated_count"] == 3
    assert score["data_coverage"] == 1.0


@pytest.mark.asyncio
async def test_a_model_with_only_out_of_window_points_still_gets_a_row():
    """It must not vanish from the round silently — it falls through to `no_forecasts`."""
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)] + [(2, 10, 8, 10.0, None)]
    actuals = {10: [(1, 10.0), (2, 10.0), (3, 10.0), (8, 10.0)]}
    repo = _FakeForecastRepo(forecasts, actuals)
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    scores = _by_pair(repo.inserted)
    # Model 2 submitted nothing valid, so it is not stored as a scored pair (the
    # `no_forecasts` branch is skipped by the caller) — but model 1 is unaffected.
    assert (1, 10) in scores
    assert scores[(1, 10)]["evaluated_count"] == 3
    assert (2, 10) not in scores


@pytest.mark.asyncio
async def test_a_lagging_series_is_scored_on_its_own_window():
    """Series 11's context ends an hour behind series 10's, so its legitimate first point is
    an hour earlier. A single round-level `start_time` bound would drop it."""
    forecasts = [(1, 10, h, 10.0, None) for h in (2, 3, 4)] + [
        (1, 11, h, 10.0, None) for h in (1, 2, 3)
    ]
    actuals = {
        10: [(2, 10.0), (3, 10.0), (4, 10.0)],
        11: [(1, 10.0), (2, 10.0), (3, 10.0)],
    }
    repo = _FakeForecastRepo(forecasts, actuals)
    round_repo = _FakeRoundRepo(
        end_time=_ts(5), series_max_ts={10: _ts(1), 11: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0, 11: 5.0}))

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    scores = _by_pair(repo.inserted)
    assert scores[(1, 10)]["evaluated_count"] == 3
    assert scores[(1, 11)]["evaluated_count"] == 3


# --- scores produced ---------------------------------------------------------

@pytest.mark.asyncio
async def test_full_coverage_scores_and_finalizes():
    # Two models, one series, three hourly points each, all with matching actuals.
    forecasts = [
        (1, 10, h, 10.0, None) for h in (1, 2, 3)
    ] + [
        (2, 10, h, 12.0, None) for h in (1, 2, 3)
    ]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0), (2, 10.0), (3, 10.0)]})
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    assert await svc.evaluate_challenge_scores(round_id=99) is True

    scores = _by_pair(repo.inserted)
    assert set(scores) == {(1, 10), (2, 10)}
    for score in scores.values():
        assert score["evaluation_status"] == "complete"
        assert score["data_coverage"] == 1.0
        assert score["final_evaluation"] is True
        assert score["evaluated_count"] == 3

    # Perfect point forecast -> MASE 0; the other is off by 2 against a naive error of 5.
    assert scores[(1, 10)]["mase"] == pytest.approx(0.0)
    assert scores[(2, 10)]["mase"] == pytest.approx(2.0 / 5.0)


@pytest.mark.asyncio
async def test_partial_coverage_is_not_finalized_before_timeout():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    # Only one of the three timestamps has an actual.
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)]})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) + timedelta(hours=1), series_max_ts={10: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["evaluation_status"] == "partial"
    assert score["data_coverage"] == pytest.approx(1 / 3)
    assert score["final_evaluation"] is False


@pytest.mark.asyncio
async def test_insufficient_coverage_after_timeout_is_excluded():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)]})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) - timedelta(days=3), series_max_ts={10: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["evaluation_status"] == "insufficient_data"
    assert score["final_evaluation"] is True
    assert score["mase"] is None  # excluded from ELO


@pytest.mark.asyncio
async def test_no_overlapping_actuals_yields_no_overlap():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(20, 10.0)]})  # actual far outside
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["evaluation_status"] == "no_overlap"
    assert score["evaluated_count"] == 0


@pytest.mark.asyncio
async def test_missing_context_point_is_an_error_row():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)]})
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    # No context value for the series -> no naive baseline.
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["evaluation_status"] == "error"
    assert "naive forecast baseline" in score["error_message"]


# --- terminal state for rounds that can never finalize (backend-68) ----------
#
# Both branches below used to return final_evaluation=False unconditionally. The candidate
# query selects any round holding a score row with final_evaluation=FALSE, so a single such
# pair kept an otherwise fully-scored round re-evaluating every cycle, forever.

@pytest.mark.asyncio
async def test_no_overlap_stays_open_before_the_grace_period():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(20, 10.0)]})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) - timedelta(hours=2), series_max_ts={10: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    # Ground truth may still arrive within the grace period — keep re-evaluating.
    assert _by_pair(repo.inserted)[(1, 10)]["final_evaluation"] is False


@pytest.mark.asyncio
async def test_no_overlap_becomes_terminal_after_the_grace_period():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(20, 10.0)]})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) - timedelta(days=3), series_max_ts={10: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["final_evaluation"] is True
    assert score["evaluation_status"] == "no_overlap"
    # Still excluded from the leaderboard: v_ranking_base filters on mase IS NOT NULL.
    assert score["mase"] is None


@pytest.mark.asyncio
async def test_missing_context_becomes_terminal_after_the_grace_period():
    forecasts = [(1, 10, h, 10.0, None) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)]})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) - timedelta(days=3), series_max_ts={10: _ts(0)}
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["final_evaluation"] is True
    assert score["evaluation_status"] == "error"
    assert score["mase"] is None


@pytest.mark.asyncio
async def test_scored_pairs_are_unaffected_by_a_dead_pair_in_the_same_round():
    """The zombie case: one series has ground truth, the other never will. The scored
    pair keeps its own status, and the dead one no longer holds the round open."""
    forecasts = [(1, 10, 1, 10.0, None), (1, 11, 1, 10.0, None)]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)], 11: []})
    round_repo = _FakeRoundRepo(
        end_time=datetime.now(UTC) - timedelta(days=3),
        series_max_ts={10: _ts(0), 11: _ts(0)},
    )
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0, 11: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    scores = _by_pair(repo.inserted)
    assert scores[(1, 10)]["evaluation_status"] == "complete"
    assert scores[(1, 11)]["evaluation_status"] == "no_overlap"
    # Neither leaves the round in the candidate set.
    assert all(s["final_evaluation"] is True for s in scores.values())


@pytest.mark.asyncio
async def test_pairs_without_forecasts_are_not_stored():
    """Model 1 forecast both series, model 2 only series 10 — the (2, 11) cross-product
    pair must produce no row at all, exactly as `no_forecasts` did before."""
    forecasts = [
        (1, 10, 1, 10.0, None),
        (1, 11, 1, 10.0, None),
        (2, 10, 1, 10.0, None),
    ]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0)], 11: [(1, 10.0)]})
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0), 11: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0, 11: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    assert set(_by_pair(repo.inserted)) == {(1, 10), (1, 11), (2, 10)}


@pytest.mark.asyncio
async def test_bare_key_quantiles_survive_into_scoring():
    """backend-69 end to end: a stored bare-key forecast is scored as a real distribution."""
    pv = {f"0.{i}": 10.0 + i for i in range(1, 10)}
    forecasts = [(1, 10, h, 10.0, pv) for h in (1, 2, 3)]
    repo = _FakeForecastRepo(forecasts, {10: [(1, 10.0), (2, 10.0), (3, 10.0)]})
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={10: _ts(0)})
    svc = _service(repo, round_repo, _FakeTimeSeriesRepo({10: 5.0}))

    await svc.evaluate_challenge_scores(round_id=99)

    score = _by_pair(repo.inserted)[(1, 10)]
    assert score["has_quantiles"] is True
    assert score["quantile_levels_count"] == 9


# --- query counts ------------------------------------------------------------

@pytest.mark.asyncio
async def test_query_count_is_per_round_and_per_series_not_per_pair():
    """The whole point of backend-68: 5 models x 3 series used to mean ~60 queries."""
    models, series = [1, 2, 3, 4, 5], [10, 11, 12]
    forecasts = [
        (m, s, h, 10.0, None) for m in models for s in series for h in (1, 2, 3)
    ]
    actuals = {s: [(1, 10.0), (2, 10.0), (3, 10.0)] for s in series}
    repo = _FakeForecastRepo(forecasts, actuals)
    round_repo = _FakeRoundRepo(end_time=_ts(4), series_max_ts={s: _ts(0) for s in series})
    time_series_repo = _FakeTimeSeriesRepo({s: 5.0 for s in series})
    svc = _service(repo, round_repo, time_series_repo)

    await svc.evaluate_challenge_scores(round_id=99)

    assert len(repo.inserted) == len(models) * len(series)
    # Per round, regardless of how many models participated:
    assert repo.calls["stats"] == 1
    assert repo.calls["round_forecasts"] == 1
    # Per series, not per (model, series):
    assert repo.calls["series_actuals"] == len(series)
    assert round_repo.calls["pseudo"] == len(series)
    assert time_series_repo.calls["context"] == len(series)
