"""Unit tests for the authoritative daily_rankings group write.

`_store_ratings` must own its (calculation_date, scope_type, scope_id, metric)
group: delete the whole group first, then insert the current result set — all
before the commit. Exercised against a fake session capturing executed
statements — no DB.
"""
import asyncio
from datetime import date

from app.services.elo_ranking_service import EloRankingService, EloRating


class FakeSession:
    def __init__(self):
        self.statements = []  # (sql_text, params) in execution order
        self.committed_after = None  # statement count at commit time

    async def execute(self, query, params=None):
        self.statements.append((str(query), params))

    async def commit(self):
        self.committed_after = len(self.statements)


def _rating(model_id: int, score: float) -> EloRating:
    return EloRating(
        model_id=model_id,
        scope_type="global",
        scope_id=None,
        elo_score=score,
        elo_ci_lower=score - 10,
        elo_ci_upper=score + 10,
        n_matches=5,
        n_bootstraps=100,
        calculation_duration_ms=1,
    )


def _run_store(ratings, scope_id=None):
    session = FakeSession()
    service = EloRankingService(session)

    async def _no_stats(**kwargs):
        return {}

    service._get_cumulative_mase_stats = _no_stats
    asyncio.run(
        service._store_ratings(
            ratings=ratings,
            scope_type="global",
            scope_id=scope_id,
            calculation_date=date(2026, 7, 22),
            metric="mase",
        )
    )
    return session


def test_group_delete_precedes_inserts_in_same_transaction():
    session = _run_store([_rating(1, 1200.0), _rating(2, 1100.0)])

    sql_first, params_first = session.statements[0]
    assert "DELETE FROM forecasts.daily_rankings" in sql_first
    # the delete targets the whole group, not individual models
    assert "model_id" not in sql_first
    assert params_first["calculation_date"] == date(2026, 7, 22)
    assert params_first["scope_type"] == "global"
    assert params_first["metric"] == "mase"

    inserts = [s for s, _ in session.statements[1:]]
    assert len(inserts) == 2
    assert all("INSERT INTO forecasts.daily_rankings" in s for s in inserts)
    # single transaction: nothing committed until after delete + all inserts
    assert session.committed_after == 3


def test_ranks_are_dense_and_ordered_by_score():
    session = _run_store([_rating(1, 1000.0), _rating(2, 1300.0), _rating(3, 1150.0)])
    by_model = {
        p["model_id"]: p["rank_position"]
        for s, p in session.statements
        if "INSERT" in s
    }
    assert by_model == {2: 1, 3: 2, 1: 3}


def test_empty_result_set_does_not_touch_the_group():
    session = _run_store([])
    assert session.statements == []
