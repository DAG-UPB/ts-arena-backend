"""Unit tests for the probabilistic-board membership rule (backend-64).

Only forecasts that actually carried a distribution may be ranked on the SQL board.
Point-only models still get an `sql_score` — `assemble_quantile_forecasts` degrades a point
forecast to a degenerate quantile forecast, and that score is arithmetically correct — but a
model that never submitted a distribution did not compete in the probabilistic task, so it is
excluded at RANKING time, not at scoring time.

The filter is per score ROW: several models submit quantiles on some rounds only, and those
rounds are exactly the ones that belong on this board.

Exercised against a fake session capturing the emitted SQL — no DB.
"""
import asyncio

import pytest

from app.services.elo_ranking_service import EloRankingService

FILTER = "fs.has_quantiles = TRUE"


class _Result:
    @staticmethod
    def fetchall():
        return []


class FakeSession:
    def __init__(self):
        self.statements = []  # sql text in execution order

    async def execute(self, query, params=None):
        self.statements.append(str(query))
        return _Result()


def _capture(coro_factory):
    session = FakeSession()
    service = EloRankingService(session)
    asyncio.run(coro_factory(service))
    assert len(session.statements) == 1
    return session.statements[0]


# --- the filter fragment itself -------------------------------------------------

def test_sql_metric_yields_the_quantile_filter():
    assert EloRankingService(None)._metric_row_filter("sql").strip() == "AND " + FILTER


def test_mase_metric_yields_no_filter():
    assert EloRankingService(None)._metric_row_filter("mase") == ""


def test_unsupported_metric_is_rejected():
    with pytest.raises(ValueError):
        EloRankingService(None)._metric_row_filter("crps")


# --- the filter reaches both ranking query paths ---------------------------------

def test_scores_matrix_filters_on_quantiles_for_sql():
    sql = _capture(lambda s: s._get_scores_matrix(metric="sql"))
    assert FILTER in sql
    assert "AVG(fs.sql_score)" in sql


def test_scores_matrix_does_not_filter_for_mase():
    sql = _capture(lambda s: s._get_scores_matrix(metric="mase"))
    assert FILTER not in sql
    assert "AVG(fs.mase)" in sql


def test_eligibility_population_filters_on_quantiles_for_sql():
    sql = _capture(lambda s: s._get_eligible_global_model_ids(metric="sql"))
    assert FILTER in sql


def test_eligibility_population_does_not_filter_for_mase():
    sql = _capture(lambda s: s._get_eligible_global_model_ids(metric="mase"))
    assert FILTER not in sql


def test_both_sql_paths_agree_on_the_population():
    """The matrix and the eligibility fetch must select the same rows, or a model could be
    ranked on rows its eligibility was never computed from."""
    matrix_sql = _capture(lambda s: s._get_scores_matrix(metric="sql"))
    elig_sql = _capture(lambda s: s._get_eligible_global_model_ids(metric="sql"))
    assert (FILTER in matrix_sql) == (FILTER in elig_sql) is True


# --- row-level, not model-level --------------------------------------------------

def test_filter_is_row_level_not_model_level():
    """A model that submits quantiles on some rounds only keeps those rounds. The predicate
    must therefore constrain the score row and never subquery the model as a whole."""
    sql = _capture(lambda s: s._get_scores_matrix(metric="sql"))
    fragment = sql[sql.index(FILTER) - 4:sql.index(FILTER) + len(FILTER)]
    assert fragment == "AND " + FILTER
    assert "model_id IN (" not in sql
