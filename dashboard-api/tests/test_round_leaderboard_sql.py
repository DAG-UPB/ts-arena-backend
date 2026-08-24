"""The round leaderboard reports SQL beside MASE without ranking on it.

frontend-17 puts a Scaled Quantile Loss column next to MASE on the round board. Two
things have to hold for that column to be readable: the score has to survive the same
NaN/Inf sanitisation MASE gets (a NaN reaching the JSON encoder is a 500), and
`has_quantiles` has to come along, because a row scored from the degenerate
point-forecast substitution is not comparable to one scored from a real distribution
and the UI blanks it instead of showing it.

Ranking is unchanged and stays MASE-driven: SQL rides along as a reported column only.
"""
import psycopg2.extras
import pytest

from app.repositories.round_repository import RoundRepository


class FakeCursor:
    """Returns one canned result set and records the SQL it was handed."""

    def __init__(self, rows):
        self._rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed = query

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self.cursor_obj = FakeCursor(rows)

    def cursor(self, cursor_factory=None):
        assert cursor_factory is psycopg2.extras.RealDictCursor
        return self.cursor_obj


def _row(**overrides):
    row = {
        "model_id": 1,
        "readable_id": "chronos-bolt-base",
        "model_name": "Chronos Bolt Base",
        "series_id": 7,
        "series_name": "load-de",
        "forecast_count": 96,
        "mase": 0.84,
        "rmse": 120.5,
        "sql_score": 0.71,
        "has_quantiles": True,
        "rank": 1,
    }
    row.update(overrides)
    return row


def test_sql_score_and_has_quantiles_are_reported():
    repo = RoundRepository(FakeConn([_row()]))

    result = repo._get_leaderboard_from_scores(round_id=42)

    assert result[0]["sql_score"] == 0.71
    assert result[0]["has_quantiles"] is True
    assert result[0]["is_final"] is True


def test_point_only_row_keeps_its_flag():
    """A degenerate score still carries a number; the flag is what makes it hideable."""
    repo = RoundRepository(FakeConn([_row(has_quantiles=False, sql_score=0.84)]))

    result = repo._get_leaderboard_from_scores(round_id=42)

    assert result[0]["has_quantiles"] is False
    assert result[0]["sql_score"] == 0.84


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_sql_score_is_sanitised(bad):
    repo = RoundRepository(FakeConn([_row(sql_score=bad)]))

    result = repo._get_leaderboard_from_scores(round_id=42)

    assert result[0]["sql_score"] is None


def test_ranking_still_orders_by_mase_only():
    repo = RoundRepository(FakeConn([_row()]))

    repo._get_leaderboard_from_scores(round_id=42)

    query = repo.conn.cursor_obj.executed
    assert "ORDER BY cs.mase ASC NULLS LAST" in query
    assert "cs.sql_score" in query
    assert "ORDER BY cs.sql_score" not in query
