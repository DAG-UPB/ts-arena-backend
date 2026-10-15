"""What the weekly 30-day MASE endpoint asks the database and how it shapes the answer."""
from datetime import date

import psycopg2.extras
import pytest
from fastapi import HTTPException

from app.api.v1 import arena_scores as arena_routes
from app.core.tracks import track_sql
from app.repositories.mase_history_repository import MaseHistoryRepository
from app.schemas.mase_history import MaseHistorySchema

W1, W2, W3 = date(2026, 9, 28), date(2026, 10, 5), date(2026, 10, 12)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows):
        self.cursor_obj = FakeCursor(rows)

    def cursor(self, cursor_factory=None):
        assert cursor_factory is psycopg2.extras.RealDictCursor
        return self.cursor_obj


def row(model_id, week, mase, rel=None, participation=1.0, name=None, track="reference"):
    return {
        "week": week, "model_id": model_id, "model_name": name or f"model-{model_id}",
        "readable_id": f"m{model_id}", "track": track, "mase_30d": mase,
        "mase_rel_30d": rel, "participation_30d": participation,
    }


def test_query_reads_one_scope_of_the_view():
    conn = FakeConn([])
    MaseHistoryRepository(conn).get_history("frequency_horizon", "00:15:00::1 day")
    query, params = conn.cursor_obj.executed[0]
    assert "FROM forecasts.weekly_mase_30d w" in query
    assert "w.scope_type = %s AND COALESCE(w.scope_id, '') = %s" in query
    assert params == ("frequency_horizon", "00:15:00::1 day")


def test_global_scope_matches_the_null_scope_id():
    conn = FakeConn([])
    MaseHistoryRepository(conn).get_history("global", None)
    assert conn.cursor_obj.executed[0][1] == ("global", "")


def test_track_filter_adds_one_condition():
    conn = FakeConn([])
    MaseHistoryRepository(conn).get_history("global", None, "open")
    query, params = conn.cursor_obj.executed[0]
    assert f"AND {track_sql('mi')} = %s" in query
    assert params == ("global", "", "open")


def test_models_sorted_by_newest_value_without_one_last():
    rows = [
        row(1, W1, 0.9), row(1, W3, 0.8),
        row(2, W2, 0.7), row(2, W3, None, participation=0.5),
        row(3, W1, 0.6), row(3, W3, 0.6),
    ]
    history = MaseHistoryRepository(FakeConn(rows)).get_history("global", None)
    assert history["weeks"] == [W1, W2, W3]
    assert [m["model_id"] for m in history["models"]] == [3, 1, 2]
    assert history["models"][2]["latest_mase_30d"] is None
    assert [p["week"] for p in history["models"][0]["points"]] == [W1, W3]


def test_models_never_shown_are_left_out():
    rows = [row(1, W3, 0.8), row(2, W2, None, participation=0.3), row(2, W3, None, participation=0.4)]
    history = MaseHistoryRepository(FakeConn(rows)).get_history("global", None)
    assert [m["model_id"] for m in history["models"]] == [1]
    assert history["weeks"] == [W2, W3]


def test_nan_becomes_null():
    history = MaseHistoryRepository(FakeConn([row(1, W3, 0.8, rel=float("nan"))])).get_history("global", None)
    assert history["models"][0]["points"][0]["mase_rel_30d"] is None


def test_endpoint_resolves_the_scope_and_validates_against_the_schema():
    conn = FakeConn([row(1, W3, 0.8, rel=0.95)])
    body = arena_routes.get_mase_30d_history(
        definition_id=None, frequency_horizon="01:00:00::3 days", track=None, api_key="k", conn=conn,
    )
    parsed = MaseHistorySchema(**body)
    assert parsed.scope.type == "frequency_horizon"
    assert parsed.scope.id == "01:00:00::3 days"
    assert parsed.window_days == 30
    assert parsed.models[0].latest_mase_rel_30d == 0.95


def test_endpoint_rejects_two_scope_filters():
    with pytest.raises(HTTPException) as exc:
        arena_routes.get_mase_30d_history(
            definition_id=1, frequency_horizon="00:15:00::1 day", track=None, api_key="k", conn=FakeConn([]),
        )
    assert exc.value.status_code == 400


def test_endpoint_rejects_an_unknown_track():
    with pytest.raises(HTTPException) as exc:
        arena_routes.get_mase_30d_history(
            definition_id=None, frequency_horizon=None, track="both", api_key="k", conn=FakeConn([]),
        )
    assert exc.value.status_code == 400
