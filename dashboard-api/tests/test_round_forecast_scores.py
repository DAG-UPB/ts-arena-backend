"""Round pages read MASE and SQL from `forecasts.forecast_scores`.

The leaderboards show MASE and SQL scaled on the seasonal naive error of the context. The
round leaderboard and the series chart have to show the same numbers, for finished and
running rounds alike: the scorer re-scores a running round until its rows are final, so
both read the stored rows and nothing is computed on the fly.
"""
import psycopg2.extras

from app.repositories.round_repository import RoundRepository


class FakeCursor:
    """Hands out queued result sets in order and records every statement."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._results.pop(0)

    def fetchone(self):
        return self._results.pop(0)


class FakeConn:
    def __init__(self, results):
        self.cursor_obj = FakeCursor(results)

    def cursor(self, cursor_factory=None):
        assert cursor_factory is psycopg2.extras.RealDictCursor
        return self.cursor_obj


def _board_row(**overrides):
    row = {
        "model_id": 1,
        "readable_id": "chronos-bolt-base",
        "model_name": "Chronos Bolt Base",
        "track": "reference",
        "series_id": 7,
        "series_name": "load-de",
        "forecast_count": 96,
        "mase": 0.84,
        "rmse": 120.5,
        "sql_score": 0.71,
        "has_quantiles": True,
        "is_final": True,
        "rank": 1,
        "track_rank": 1,
    }
    row.update(overrides)
    return row


def _forecast_row(**overrides):
    row = {
        "readable_id": "chronos-bolt-base",
        "model_name": "Chronos Bolt Base",
        "ts": "2026-10-10T01:00:00Z",
        "value": 12.5,
        "confidence_intervals": None,
        "current_mase": 0.84,
    }
    row.update(overrides)
    return row


# --- /rounds/{id}/leaderboard -----------------------------------------------------------

def test_board_reads_forecast_scores_in_one_query():
    conn = FakeConn([[_board_row()]])
    RoundRepository(conn).get_round_leaderboard(round_id=42)

    assert len(conn.cursor_obj.executed) == 1
    query, params = conn.cursor_obj.executed[0]
    assert "FROM forecasts.forecast_scores fs" in query
    assert "forecasts.scores" not in query
    assert params == (42,)


def test_board_keeps_rows_with_an_evaluated_error_whether_final_or_not():
    conn = FakeConn([[_board_row()]])
    RoundRepository(conn).get_round_leaderboard(round_id=42)

    query, _ = conn.cursor_obj.executed[0]
    assert "fs.mae IS NOT NULL" in query
    assert "final_evaluation = TRUE" not in query
    assert "fs.final_evaluation as is_final" in query


def test_running_round_reports_the_stored_values():
    """A running round's rows are not final yet; their MASE and SQL are still shown."""
    rows = [
        _board_row(is_final=False, mase=0.9, sql_score=0.7, rank=1, track_rank=1),
        _board_row(model_id=9, readable_id="ext", model_name="Ext", track="open",
                   is_final=False, mase=1.1, sql_score=None, has_quantiles=False,
                   rank=2, track_rank=1),
    ]
    result = RoundRepository(FakeConn([rows])).get_round_leaderboard(round_id=42)

    assert [(r["model_id"], r["is_final"], r["mase"], r["sql_score"], r["rank"])
            for r in result] == [
        (1, False, 0.9, 0.7, 1),
        (9, False, 1.1, None, 2),
    ]


def test_undefined_scale_row_is_ranked_without_mase():
    """No usable scale: MASE and SQL stay empty, the rank by error remains."""
    rows = [
        _board_row(mase=None, sql_score=None, rank=1, track_rank=1),
        _board_row(model_id=2, readable_id="naive", model_name="Naive",
                   mase=None, sql_score=None, rank=2, track_rank=2),
    ]
    result = RoundRepository(FakeConn([rows])).get_round_leaderboard(round_id=42)

    assert [(r["model_id"], r["mase"], r["sql_score"], r["rank"]) for r in result] == [
        (1, None, None, 1),
        (2, None, None, 2),
    ]


def test_board_leaves_out_excluded_series():
    conn = FakeConn([[]])
    assert RoundRepository(conn).get_round_leaderboard(round_id=42) == []

    query, _ = conn.cursor_obj.executed[0]
    assert "ds.is_excluded = TRUE" in query


# --- /rounds/{id}/series/{series_id}/forecasts ------------------------------------------

def test_series_mase_is_the_stored_mase_of_each_model():
    conn = FakeConn([[
        _forecast_row(),
        _forecast_row(ts="2026-10-10T02:00:00Z", value=13.0),
        _forecast_row(readable_id="ext", model_name="Ext", current_mase=1.1),
    ]])
    result = RoundRepository(conn).get_series_forecasts(round_id=42, series_id=7)

    assert result["chronos-bolt-base"]["current_mase"] == 0.84
    assert result["chronos-bolt-base"]["label"] == "Chronos Bolt Base"
    assert [p["y"] for p in result["chronos-bolt-base"]["data"]] == [12.5, 13.0]
    assert result["ext"]["current_mase"] == 1.1


def test_series_mase_is_none_without_a_score():
    """Not scored yet, or no usable scale: the legend shows no MASE."""
    conn = FakeConn([[_forecast_row(current_mase=None)]])
    result = RoundRepository(conn).get_series_forecasts(round_id=42, series_id=7)

    assert result["chronos-bolt-base"]["current_mase"] is None


def test_series_reads_one_query_joined_on_round_model_and_series():
    conn = FakeConn([[]])
    assert RoundRepository(conn).get_series_forecasts(round_id=42, series_id=7) == {}

    assert len(conn.cursor_obj.executed) == 1
    query, params = conn.cursor_obj.executed[0]
    assert "LEFT JOIN forecasts.forecast_scores fs" in query
    assert "fs.round_id = f.round_id" in query
    assert "fs.model_id = f.model_id" in query
    assert "fs.series_id = f.series_id" in query
    assert "data_portal" not in query
    assert params == (42, 7)
