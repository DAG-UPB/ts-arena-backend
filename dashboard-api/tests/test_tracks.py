"""Reference Track / Open Track on the dashboard API.

The track is derived from ownership: models owned by the admin user (id 1) are the ones
implemented in ts-arena-models and form the Reference Track; every other model is Open.
ELO is still fitted once over both tracks, so a track view only drops rows. These tests
pin that down: the combined rank is never rewritten, the track rank is reported beside it,
and the filter is applied to the combined result rather than before ranking.
"""
import psycopg2.extras
import pytest
from fastapi import HTTPException

from app.core.tracks import track_for_user_id, track_sql, validate_track
from app.repositories.model_repository import ModelRepository
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


# --- the rule ---------------------------------------------------------------------------

def test_admin_owned_models_are_reference():
    assert track_for_user_id(1) == "reference"


@pytest.mark.parametrize("user_id", [2, 17, None])
def test_every_other_owner_is_open(user_id):
    assert track_for_user_id(user_id) == "open"


def test_sql_expression_uses_the_same_rule():
    assert track_sql("mi") == (
        "(CASE WHEN mi.user_id = 1 THEN 'reference' ELSE 'open' END)"
    )


def test_unknown_track_is_rejected():
    with pytest.raises(HTTPException) as exc:
        validate_track("community")
    assert exc.value.status_code == 400


@pytest.mark.parametrize("track", ["reference", "open", None])
def test_known_tracks_pass(track):
    assert validate_track(track) == track


# --- /models/rankings -------------------------------------------------------------------

def _ranking_row(**overrides):
    row = {
        "model_id": 1,
        "model_name": "Chronos Bolt Base",
        "rank_position": 1,
        "track": "reference",
        "track_rank_position": 1,
        "elo_rating_median": 1100.0,
    }
    row.update(overrides)
    return row


def test_rankings_report_track_and_track_rank():
    conn = FakeConn([[_ranking_row(), _ranking_row(model_id=9, rank_position=2,
                                                    track="open", track_rank_position=1)]])
    rows = ModelRepository(conn).get_filtered_rankings(scope_type="global")

    query, params = conn.cursor_obj.executed[0]
    assert "JOIN models.model_info mi ON mi.id = r.model_id" in query
    assert "AS track_rank_position" in query
    # No track filter: both tracks, combined rank untouched.
    assert "open" not in params and "reference" not in params
    assert [(r["rank_position"], r["track"], r["track_rank_position"]) for r in rows] == [
        (1, "reference", 1),
        (2, "open", 1),
    ]


def test_rankings_track_filter_is_a_where_clause_on_the_owner():
    conn = FakeConn([[]])
    ModelRepository(conn).get_filtered_rankings(scope_type="global", track="open")

    query, params = conn.cursor_obj.executed[0]
    assert f"AND {track_sql('mi')} = %s" in query
    # The filter precedes ORDER BY / LIMIT, so `limit` counts rows of the track.
    assert query.index(f"AND {track_sql('mi')} = %s") < query.index("LIMIT %s")
    assert params[-2:] == ("open", 100)


def test_rankings_track_filter_in_bulk_mode_limits_per_scope_after_filtering():
    conn = FakeConn([[]])
    ModelRepository(conn).get_filtered_rankings(
        scope_type="definition", track="reference", limit=10
    )

    query, params = conn.cursor_obj.executed[0]
    assert query.index(f"AND {track_sql('mi')} = %s") < query.index("WHERE _rn <= %s")
    assert params[-2:] == ("reference", 10)


# --- /rounds/{id}/leaderboard -----------------------------------------------------------

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
        "rank": 1,
        "track_rank": 1,
    }
    row.update(overrides)
    return row


def _final_board():
    return [
        _board_row(),
        _board_row(model_id=9, readable_id="ext", model_name="Ext", track="open",
                   mase=0.9, rank=2, track_rank=1),
        _board_row(model_id=2, readable_id="naive", model_name="Naive",
                   mase=1.0, rank=3, track_rank=2),
    ]


def test_final_board_ranks_per_series_across_and_within_tracks():
    conn = FakeConn([{"has_final_evaluation": True}, _final_board()])
    rows = RoundRepository(conn).get_round_leaderboard(round_id=42)

    query, _ = conn.cursor_obj.executed[1]
    assert "PARTITION BY cs.series_id ORDER BY cs.mase" in query
    assert f"PARTITION BY cs.series_id, {track_sql('mi')}" in query
    assert len(rows) == 3


def test_final_board_track_filter_keeps_the_combined_rank():
    conn = FakeConn([{"has_final_evaluation": True}, _final_board()])
    rows = RoundRepository(conn).get_round_leaderboard(round_id=42, track="reference")

    assert [(r["model_id"], r["rank"], r["track_rank"]) for r in rows] == [
        (1, 1, 1),
        (2, 3, 2),
    ]


def test_on_the_fly_board_carries_track_and_track_rank(monkeypatch):
    repo = RoundRepository(FakeConn([
        {"has_final_evaluation": False},
        [
            # Latest observed value 10, actual 12 → naive error 2.
            {"model_id": 1, "readable_id": "a", "model_name": "A", "user_id": 1,
             "series_id": 7, "series_name": "s", "predicted_value": 12.5,
             "actual_value": 12.0, "latest_observed_value": 10.0},
            {"model_id": 9, "readable_id": "e", "model_name": "E", "user_id": 5,
             "series_id": 7, "series_name": "s", "predicted_value": 12.2,
             "actual_value": 12.0, "latest_observed_value": 10.0},
            {"model_id": 2, "readable_id": "b", "model_name": "B", "user_id": 1,
             "series_id": 7, "series_name": "s", "predicted_value": 13.0,
             "actual_value": 12.0, "latest_observed_value": 10.0},
        ],
    ]))
    monkeypatch.setattr(repo, "_get_round_resolution", lambda round_id: "1h")

    rows = repo.get_round_leaderboard(round_id=42)
    assert [(r["model_id"], r["track"], r["rank"], r["track_rank"]) for r in rows] == [
        (9, "open", 1, 1),
        (1, "reference", 2, 1),
        (2, "reference", 3, 2),
    ]
