"""What the Arena Score repository asks the database and how it shapes the answer."""
from datetime import date

import json
import re

import psycopg2.extras
import pytest
from fastapi import HTTPException

from app.api.v1 import arena_scores as arena_routes
from app.core.tracks import track_sql
from app.repositories.arena_score_repository import ArenaScoreRepository
from app.schemas.arena_score import (
    ArenaScoreFiltersSchema,
    ArenaScoreLeaderboardSchema,
    ArenaScoreRowSchema,
    ModelArenaScoresSchema,
)

DAY = date(2026, 10, 14)


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


def db_row(**overrides):
    """One leaderboard row as the query returns it."""
    row = {
        "model_id": 1, "model_name": "Chronos 2", "readable_id": "chronos-2",
        "architecture": "encoder-decoder", "model_size": 120, "organization_name": "Amazon",
        "track": "reference", "scope_type": "global", "scope_id": None, "definition_id": None,
        "calculation_date": DAY, "score": 1030.0, "ci_lower": 1020.0, "ci_upper": 1040.0,
        "rank_best": 1, "rank_worst": 2, "provisional": False, "low_coverage": False,
        "unranked_disconnected": False, "coverage": 0.9, "weeks_played": 20,
        "tasks_played": 100, "rounds_played": 10, "mase_30d": 0.8, "participation_30d": 0.95,
        "sql_30d": 0.6, "sql_participation_30d": 0.9, "tasks_30d": 500,
        "provisional_weeks": 4, "coverage_tau": 0.5,
    }
    row.update(overrides)
    return row


def placeholders(query):
    return query.count("%s")


# --- leaderboard queries ----------------------------------------------------------------

def test_the_leaderboard_joins_the_model_its_organization_and_the_track():
    conn = FakeConn([[]])
    ArenaScoreRepository(conn).get_leaderboard("global")

    query, _ = conn.cursor_obj.executed[0]
    assert "FROM forecasts.daily_arena_scores a" in query
    assert "JOIN models.model_info mi ON mi.id = a.model_id" in query
    assert "LEFT JOIN auth.organizations o ON o.id = mi.organization_id" in query
    assert f"{track_sql('mi')} AS track" in query


def test_without_a_date_the_newest_date_of_the_scope_and_metric_is_used():
    conn = FakeConn([[]])
    ArenaScoreRepository(conn).get_leaderboard("definition", "7", metric="probabilistic")

    query, params = conn.cursor_obj.executed[0]
    assert "SELECT MAX(l.calculation_date)" in query
    assert "l.metric = %s AND l.scope_type = %s AND COALESCE(l.scope_id, '') = %s" in query
    assert "a.metric = %s AND a.scope_type = %s AND COALESCE(a.scope_id, '') = %s" in query
    assert params == ("probabilistic", "definition", "7") * 2
    assert placeholders(query) == len(params)


def test_an_explicit_date_is_used_as_given():
    conn = FakeConn([[]])
    ArenaScoreRepository(conn).get_leaderboard("global", calculation_date=DAY)

    query, params = conn.cursor_obj.executed[0]
    assert "MAX(" not in query
    assert "a.calculation_date = %s" in query
    assert params == (DAY, "point", "global")
    assert placeholders(query) == len(params)


def test_the_global_scope_and_a_whole_scope_type_have_no_scope_id_filter():
    for scope_type in ("global", "definition", "frequency_horizon"):
        conn = FakeConn([[]])
        ArenaScoreRepository(conn).get_leaderboard(scope_type, calculation_date=DAY)
        query, params = conn.cursor_obj.executed[0]
        assert "scope_id, '')" not in query
        assert params == (DAY, "point", scope_type)


def test_the_track_is_not_a_where_clause():
    conn = FakeConn([[]])
    ArenaScoreRepository(conn).get_leaderboard("global", calculation_date=DAY, track="open")

    query, params = conn.cursor_obj.executed[0]
    assert f"{track_sql('mi')} = %s" not in query
    assert "open" not in params
    assert "LIMIT" not in query


def test_the_result_carries_the_date_and_follows_the_request_when_empty():
    rows, day = ArenaScoreRepository(FakeConn([[db_row()]])).get_leaderboard("global")
    assert day == DAY and len(rows) == 1

    assert ArenaScoreRepository(FakeConn([[]])).get_leaderboard("global") == ([], None)
    assert ArenaScoreRepository(FakeConn([[]])).get_leaderboard("global", calculation_date=DAY) == ([], DAY)


def test_rows_come_back_sorted_with_track_ranges_and_cut_per_scope():
    rows = [
        db_row(model_id=1, model_name="low", score=1000.0, ci_lower=990.0, ci_upper=1010.0,
               rank_best=2, rank_worst=2),
        db_row(model_id=2, model_name="none", score=None, ci_lower=None, ci_upper=None,
               rank_best=None, rank_worst=None, unranked_disconnected=True),
        db_row(model_id=3, model_name="top", score=1100.0, ci_lower=1090.0, ci_upper=1110.0,
               rank_best=1, rank_worst=1),
    ]
    out, _ = ArenaScoreRepository(FakeConn([rows])).get_leaderboard("global", limit=2)

    assert [r["model_id"] for r in out] == [3, 1]
    assert [(r["track_rank_best"], r["track_rank_worst"]) for r in out] == [(1, 1), (2, 2)]


def test_non_finite_numbers_become_null():
    rows = [db_row(score=float("nan"), ci_lower=float("inf"), coverage=float("-inf"),
                   rank_best=None, rank_worst=None)]
    out, _ = ArenaScoreRepository(FakeConn([rows])).get_leaderboard("global")

    assert out[0]["score"] is None and out[0]["ci_lower"] is None and out[0]["coverage"] is None


def test_the_method_fields_are_cast_in_sql():
    conn = FakeConn([[]])
    ArenaScoreRepository(conn).get_leaderboard("global")

    query, _ = conn.cursor_obj.executed[0]
    assert "(a.method->>'provisional_weeks')::int AS provisional_weeks" in query
    assert "(a.method->>'tau')::float8 AS coverage_tau" in query


def test_a_row_has_exactly_the_documented_fields():
    out, _ = ArenaScoreRepository(FakeConn([[db_row()]])).get_leaderboard("global")
    assert set(out[0]) == set(ArenaScoreRowSchema.model_fields)


# --- endpoint output against the response schemas ---------------------------------------

def test_the_leaderboard_response_fits_its_schema():
    rows = [
        db_row(model_id=1, model_name="ranked"),
        db_row(model_id=2, model_name="provisional", score=1020.0, rank_best=None, rank_worst=None,
               provisional=True, weeks_played=2, mase_30d=None, sql_30d=None),
        db_row(model_id=3, model_name="disconnected", score=None, ci_lower=None, ci_upper=None,
               rank_best=None, rank_worst=None, unranked_disconnected=True, coverage=None,
               participation_30d=None, sql_participation_30d=None, tasks_30d=None,
               provisional_weeks=None, coverage_tau=None, organization_name=None,
               readable_id=None, architecture=None, model_size=None),
    ]
    response = arena_routes.get_arena_scores(
        definition_id=None, frequency_horizon=None, scope_type=None, calculation_date=None,
        metric="point", limit=100, track=None, api_key="k", conn=FakeConn([rows]),
    )

    parsed = ArenaScoreLeaderboardSchema(**response)
    assert parsed.scope.type == "global" and parsed.scope.id is None
    assert parsed.metric == "point" and parsed.track is None and parsed.calculation_date == DAY
    assert [r.model_name for r in parsed.rankings] == ["ranked", "provisional", "disconnected"]
    assert parsed.rankings[2].score is None


def test_the_envelope_follows_the_request():
    response = arena_routes.get_arena_scores(
        definition_id=None, frequency_horizon="00:15:00::1 day", scope_type=None,
        calculation_date="2026-10-01", metric="probabilistic", limit=5, track="open",
        api_key="k", conn=FakeConn([[]]),
    )

    assert response == {
        "rankings": [],
        "scope": {"type": "frequency_horizon", "id": "00:15:00::1 day"},
        "metric": "probabilistic",
        "track": "open",
        "calculation_date": date(2026, 10, 1),
    }


# --- model history ----------------------------------------------------------------------

def history_row(scope_type, scope_id, day, definition_id=None, definition_name=None, **overrides):
    row = {
        "scope_type": scope_type, "scope_id": scope_id, "definition_id": definition_id,
        "definition_name": definition_name, "calculation_date": day, "score": 1010.0,
        "ci_lower": 1000.0, "ci_upper": 1020.0, "rank_best": 3, "rank_worst": 5,
        "provisional": False, "low_coverage": False, "unranked_disconnected": False,
        "coverage": 1.0, "weeks_played": 12,
    }
    row.update(overrides)
    return row


MODEL = {"model_id": 17, "model_name": "Chronos 2", "track": "reference"}


def test_an_unknown_model_has_no_history_and_costs_one_query():
    conn = FakeConn([None])
    assert ArenaScoreRepository(conn).get_model_history(17) is None
    assert len(conn.cursor_obj.executed) == 1


def test_a_model_without_scores_has_an_empty_history():
    result = ArenaScoreRepository(FakeConn([MODEL, []])).get_model_history(17, metric="probabilistic")
    assert result == {**MODEL, "metric": "probabilistic", "scopes": []}


def test_the_history_is_grouped_per_scope_in_a_fixed_order():
    d1, d2 = date(2026, 10, 13), date(2026, 10, 14)
    rows = [
        history_row("frequency_horizon", "01:00:00::1 day", d1),
        history_row("definition", "10", d1, 10, "Ten"),
        history_row("global", None, d1),
        history_row("definition", "2", d1, 2, "Two"),
        history_row("definition", "10", d2, 10, "Ten"),
        history_row("global", None, d2),
    ]
    result = ArenaScoreRepository(FakeConn([MODEL, rows])).get_model_history(17)

    assert [(s["scope_type"], s["scope_id"]) for s in result["scopes"]] == [
        ("global", None), ("definition", "2"), ("definition", "10"),
        ("frequency_horizon", "01:00:00::1 day"),
    ]
    ten = result["scopes"][2]
    assert (ten["definition_id"], ten["definition_name"]) == (10, "Ten")
    assert [d["calculation_date"] for d in ten["daily_scores"]] == [d1, d2]
    assert result["scopes"][0]["definition_id"] is None


def test_a_history_entry_has_the_documented_fields_and_fits_the_schema():
    rows = [history_row("global", None, DAY, score=float("nan"))]
    result = ArenaScoreRepository(FakeConn([MODEL, rows])).get_model_history(17)

    assert set(result["scopes"][0]["daily_scores"][0]) == {
        "calculation_date", "score", "ci_lower", "ci_upper", "rank_best", "rank_worst",
        "provisional", "low_coverage", "unranked_disconnected", "coverage", "weeks_played",
    }
    assert result["scopes"][0]["daily_scores"][0]["score"] is None
    parsed = ModelArenaScoresSchema(**result)
    assert parsed.model_id == 17 and parsed.track == "reference"


def test_the_history_query_filters_the_model_and_metric_and_sorts_by_day():
    conn = FakeConn([MODEL, []])
    ArenaScoreRepository(conn).get_model_history(17, metric="probabilistic")

    model_query, model_params = conn.cursor_obj.executed[0]
    assert f"{track_sql('mi')} AS track" in model_query and model_params == (17,)
    query, params = conn.cursor_obj.executed[1]
    assert "a.model_id = %s AND a.metric = %s" in query
    assert query.rstrip().endswith("ORDER BY a.calculation_date")
    assert params == (17, "probabilistic")
    assert placeholders(query) == len(params)


def test_the_history_endpoint_answers_404_for_an_unknown_model():
    with pytest.raises(HTTPException) as exc:
        arena_routes.get_model_arena_scores(
            model_id=1, metric="point", api_key="k", conn=FakeConn([None]),
        )
    assert exc.value.status_code == 404


# --- filters ----------------------------------------------------------------------------

def _days(*days):
    return [{"calculation_date": day} for day in days]


def test_filter_options():
    conn = FakeConn([
        [{"id": 3, "name": "B challenge"}, {"id": 1, "name": "A challenge"}],
        [{"scope_id": "00:15:00::1 day"}, {"scope_id": "01:00:00::3 days"}],
        _days(date(2026, 10, 14), date(2026, 9, 30)),
    ])
    options = ArenaScoreRepository(conn).get_filter_options()

    assert options == {
        "definitions": [{"id": 3, "name": "B challenge"}, {"id": 1, "name": "A challenge"}],
        "frequency_horizons": ["00:15:00::1 day", "01:00:00::3 days"],
        "calculation_dates": [
            {"calculation_date": date(2026, 10, 14), "is_month_end": False},
            {"calculation_date": date(2026, 9, 30), "is_month_end": True},
        ],
    }
    definitions_sql, dates_sql = conn.cursor_obj.executed[0][0], conn.cursor_obj.executed[2][0]
    assert "ORDER BY d.name" in definitions_sql
    assert "ORDER BY a.calculation_date DESC" in dates_sql
    assert ArenaScoreFiltersSchema(**options).calculation_dates[1].is_month_end is True


# --- the endpoint documentation ---------------------------------------------------------

def _json_example(doc):
    return json.loads(re.search(r"```json\n(.*?)```", doc, re.S).group(1))


def test_the_documented_examples_fit_the_response_schemas():
    ArenaScoreLeaderboardSchema(**_json_example(arena_routes.get_arena_scores.__doc__))
    ModelArenaScoresSchema(**_json_example(arena_routes.get_model_arena_scores.__doc__))
    ArenaScoreFiltersSchema(**_json_example(arena_routes.get_arena_score_filters.__doc__))


def test_every_leaderboard_field_is_documented():
    doc = arena_routes.get_arena_scores.__doc__
    assert [name for name in ArenaScoreRowSchema.model_fields if f"`{name}`" not in doc] == []


def test_the_dates_are_the_latest_day_and_the_month_ends():
    days = _days(date(2026, 10, 14), date(2026, 10, 13), date(2026, 9, 30), date(2026, 9, 29),
                 date(2026, 8, 31), date(2026, 8, 1))
    options = ArenaScoreRepository(FakeConn([[], [], days])).get_filter_options()

    assert [(d["calculation_date"], d["is_month_end"]) for d in options["calculation_dates"]] == [
        (date(2026, 10, 14), False), (date(2026, 9, 30), True), (date(2026, 8, 31), True),
    ]


def test_a_latest_day_that_is_a_month_end_is_listed_once():
    days = _days(date(2026, 10, 31), date(2026, 10, 30), date(2026, 9, 30))
    options = ArenaScoreRepository(FakeConn([[], [], days])).get_filter_options()

    assert [(d["calculation_date"], d["is_month_end"]) for d in options["calculation_dates"]] == [
        (date(2026, 10, 31), True), (date(2026, 9, 30), True),
    ]


def test_no_stored_day_means_no_dates():
    assert ArenaScoreRepository(FakeConn([[], [], []])).get_filter_options()["calculation_dates"] == []
