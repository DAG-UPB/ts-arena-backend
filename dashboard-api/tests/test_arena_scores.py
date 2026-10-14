"""Arena Score read side: rank ranges, ordering, request validation and routing."""
import importlib
import random
import sys
from datetime import date

import pytest
from fastapi import HTTPException
from starlette.routing import Match

from app.api.v1 import arena_scores as arena_routes
from app.api.v1 import models as elo_routes
from app.core.arena_scores import (
    arrange_leaderboard,
    is_month_end,
    leaderboard_order,
    parse_date,
    resolve_scope,
    scope_order,
    track_rank_ranges,
    validate_metric,
)


def ranked(track, lo, hi):
    return (track, lo, hi, True)


def unranked(track, lo=None, hi=None):
    return (track, lo, hi, False)


# --- rank ranges ------------------------------------------------------------------------

def test_a_clear_leader_has_a_single_rank():
    ranges = track_rank_ranges([
        ranked("reference", 1100, 1110),
        ranked("reference", 1000, 1010),
        ranked("reference", 990, 1005),
    ])
    assert ranges == [(1, 1), (2, 3), (2, 3)]


def test_overlapping_models_share_the_best_rank():
    ranges = track_rank_ranges([
        ranked("reference", 934, 1059),
        ranked("reference", 913, 1039),
    ])
    assert ranges == [(1, 2), (1, 2)]


def test_a_third_model_below_only_one_of_two_overlapping_models_is_second():
    ranges = track_rank_ranges([
        ranked("reference", 1000, 1050),   # A
        ranked("reference", 990, 1040),    # B overlaps A
        ranked("reference", 940, 995),     # C below A, overlaps B
    ])
    assert ranges == [(1, 2), (1, 3), (2, 3)]


def test_a_third_model_below_both_is_third():
    ranges = track_rank_ranges([
        ranked("reference", 1000, 1050),
        ranked("reference", 990, 1040),
        ranked("reference", 900, 950),
    ])
    assert ranges == [(1, 2), (1, 2), (3, 3)]


def test_a_new_leader_pushes_the_overlapping_pair_down():
    ranges = track_rank_ranges([
        ranked("reference", 934, 1059),
        ranked("reference", 913, 1039),
        ranked("reference", 1070, 1130),
    ])
    assert ranges == [(2, 3), (2, 3), (1, 1)]


def test_identical_intervals_all_share_the_best_rank():
    ranges = track_rank_ranges([ranked("open", 10, 20)] * 4)
    assert ranges == [(1, 4)] * 4


def test_touching_intervals_overlap():
    # a lower bound has to be strictly above an upper bound to count as clearly above
    assert track_rank_ranges([ranked("open", 10, 20), ranked("open", 20, 30)]) == [(1, 2), (1, 2)]
    assert track_rank_ranges([ranked("open", 10, 20), ranked("open", 21, 30)]) == [(2, 2), (1, 1)]


def test_a_single_ranked_model_is_first():
    assert track_rank_ranges([ranked("open", 5, 6)]) == [(1, 1)]


def test_no_entries():
    assert track_rank_ranges([]) == []


def test_ranges_are_computed_within_the_own_track():
    ranges = track_rank_ranges([
        ranked("reference", 1000, 1010),
        ranked("open", 1100, 1110),      # clearly above, but another track
        ranked("reference", 900, 910),
        ranked("open", 800, 810),
    ])
    assert ranges == [(1, 1), (1, 1), (2, 2), (2, 2)]


def test_unranked_entries_neither_get_a_range_nor_count():
    ranges = track_rank_ranges([
        ranked("reference", 1000, 1010),
        unranked("reference", 1100, 1110),    # provisional or low coverage: interval ignored
        ranked("reference", 900, 910),
        unranked("reference"),                # no score at all
    ])
    assert ranges == [(1, 1), (None, None), (2, 2), (None, None)]


@pytest.mark.parametrize("lo,hi", [(None, 10.0), (10.0, None), (None, None)])
def test_a_ranked_entry_without_both_bounds_is_left_out(lo, hi):
    ranges = track_rank_ranges([ranked("open", 5.0, 6.0), ranked("open", lo, hi)])
    assert ranges == [(1, 1), (None, None)]


def _brute_force(entries):
    members = [k for k, (_, lo, hi, ok) in enumerate(entries) if ok and lo is not None and hi is not None]
    out = [(None, None)] * len(entries)
    for m in members:
        same = [o for o in members if o != m and entries[o][0] == entries[m][0]]
        best = 1 + sum(1 for o in same if entries[o][1] > entries[m][2])
        worst = 1 + sum(1 for o in same if entries[o][2] >= entries[m][1])
        out[m] = (best, worst)
    return out


def test_ranges_match_the_definition_on_random_intervals_with_many_ties():
    rng = random.Random(7)
    for _ in range(300):
        entries = []
        for _ in range(rng.randint(0, 25)):
            a, b = rng.randint(0, 12), rng.randint(0, 12)   # inverted intervals included
            entries.append((rng.choice(["reference", "open"]), float(a), float(b), rng.random() < 0.8))
        assert track_rank_ranges(entries) == _brute_force(entries)


# --- ordering and arrangement -----------------------------------------------------------

def _row(model_id, name, score, track="reference", scope=("global", None), best=None, worst=None,
         lo=None, hi=None, day=date(2026, 10, 14)):
    return {
        "model_id": model_id, "model_name": name, "score": score, "track": track,
        "scope_type": scope[0], "scope_id": scope[1], "calculation_date": day,
        "rank_best": best, "rank_worst": worst, "ci_lower": lo, "ci_upper": hi,
    }


def test_rows_sort_by_score_descending_with_unscored_models_last_by_name():
    rows = [
        _row(1, "zeta", None),
        _row(2, "Beta", 1000.0),
        _row(3, "alpha", None),
        _row(4, "Gamma", 1050.5),
        _row(5, "delta", 1000.0),
    ]
    assert [r["model_id"] for r in sorted(rows, key=leaderboard_order)] == [4, 2, 5, 3, 1]


def test_equal_scores_and_names_fall_back_to_the_model_id():
    rows = [_row(9, "same", 1000.0), _row(2, "Same", 1000.0)]
    assert [r["model_id"] for r in sorted(rows, key=leaderboard_order)] == [2, 9]


def test_negative_scores_still_sort_descending():
    rows = [_row(1, "a", -5.0), _row(2, "b", 3.0), _row(3, "c", -1.0)]
    assert [r["model_id"] for r in sorted(rows, key=leaderboard_order)] == [2, 3, 1]


def test_scopes_come_global_then_definitions_by_id_then_frequency_horizons_by_name():
    scopes = [
        ("frequency_horizon", "01:00:00::1 day"),
        ("definition", "10"),
        ("global", None),
        ("definition", "2"),
        ("frequency_horizon", "00:15:00::1 day"),
    ]
    assert sorted(scopes, key=lambda s: scope_order(*s)) == [
        ("global", None),
        ("definition", "2"),
        ("definition", "10"),
        ("frequency_horizon", "00:15:00::1 day"),
        ("frequency_horizon", "01:00:00::1 day"),
    ]


def test_arrange_adds_the_track_range_and_leaves_the_stored_ranks_alone():
    rows = [
        _row(1, "ref-a", 1040.0, "reference", best=1, worst=3, lo=1030.0, hi=1050.0),
        _row(2, "open-a", 1035.0, "open", best=1, worst=3, lo=1025.0, hi=1045.0),
        _row(3, "ref-b", 1030.0, "reference", best=2, worst=3, lo=1020.0, hi=1040.0),
        _row(4, "open-prov", 1020.0, "open"),
    ]
    out = arrange_leaderboard(rows, track=None, limit=100)

    assert [r["model_id"] for r in out] == [1, 2, 3, 4]
    assert [(r["rank_best"], r["rank_worst"]) for r in out] == [(1, 3), (1, 3), (2, 3), (None, None)]
    assert [(r["track_rank_best"], r["track_rank_worst"]) for r in out] == [
        (1, 2), (1, 1), (1, 2), (None, None),
    ]


def test_the_track_filter_only_drops_rows():
    rows = [
        _row(1, "ref-a", 1040.0, "reference", best=1, worst=2, lo=1030.0, hi=1050.0),
        _row(2, "open-a", 1035.0, "open", best=1, worst=2, lo=1025.0, hi=1045.0),
        _row(3, "ref-b", 1000.0, "reference", best=2, worst=2, lo=990.0, hi=1010.0),
    ]
    both = {r["model_id"]: r for r in arrange_leaderboard([dict(r) for r in rows], None, 100)}
    only = arrange_leaderboard([dict(r) for r in rows], "reference", 100)

    assert [r["model_id"] for r in only] == [1, 3]
    for r in only:
        for key in ("rank_best", "rank_worst", "track_rank_best", "track_rank_worst", "score"):
            assert r[key] == both[r["model_id"]][key]
    assert [(r["track_rank_best"], r["track_rank_worst"]) for r in only] == [(1, 1), (2, 2)]


def test_the_limit_applies_per_scope_after_the_track_filter_and_keeps_the_ranges():
    rows = []
    for scope in (("definition", "1"), ("definition", "2")):
        for k in range(5):
            track = "reference" if k % 2 == 0 else "open"
            rows.append(_row(
                k + 1 + 10 * int(scope[1]), f"m{k}", 1000.0 - 20 * k, track, scope,
                best=k + 1, worst=k + 1, lo=1000.0 - 20 * k - 5, hi=1000.0 - 20 * k + 5,
            ))
    full = {r["model_id"]: r for r in arrange_leaderboard([dict(r) for r in rows], "reference", 100)}
    cut = arrange_leaderboard([dict(r) for r in rows], "reference", 2)

    assert [(r["scope_id"], r["model_id"]) for r in cut] == [("1", 11), ("1", 13), ("2", 21), ("2", 23)]
    for r in cut:
        assert (r["track_rank_best"], r["track_rank_worst"]) == (
            full[r["model_id"]]["track_rank_best"], full[r["model_id"]]["track_rank_worst"])
    assert [r["track_rank_best"] for r in cut] == [1, 2, 1, 2]


def test_every_scope_is_ranked_on_its_own():
    rows = [
        _row(1, "a", 1100.0, scope=("definition", "1"), best=1, worst=1, lo=1090.0, hi=1110.0),
        _row(2, "b", 1000.0, scope=("definition", "1"), best=2, worst=2, lo=990.0, hi=1010.0),
        _row(2, "b", 1100.0, scope=("definition", "2"), best=1, worst=1, lo=1090.0, hi=1110.0),
        _row(1, "a", 1000.0, scope=("definition", "2"), best=2, worst=2, lo=990.0, hi=1010.0),
    ]
    out = arrange_leaderboard(rows, None, 100)
    assert [(r["scope_id"], r["model_id"], r["track_rank_best"]) for r in out] == [
        ("1", 1, 1), ("1", 2, 2), ("2", 2, 1), ("2", 1, 2),
    ]


# --- request validation, the same 400s as the ELO ranking endpoint ----------------------

def _elo(**kwargs):
    args = dict(definition_id=None, frequency_horizon=None, scope_type=None, calculation_date=None,
                metric="mase", limit=100, track=None, api_key="k", conn=None)
    args.update(kwargs)
    return elo_routes.get_filtered_rankings(**args)


def _arena(**kwargs):
    args = dict(definition_id=None, frequency_horizon=None, scope_type=None, calculation_date=None,
                metric="point", limit=100, track=None, api_key="k", conn=None)
    args.update(kwargs)
    return arena_routes.get_arena_scores(**args)


def _error(call, **kwargs):
    with pytest.raises(HTTPException) as exc:
        call(**kwargs)
    return exc.value.status_code, exc.value.detail


INVALID_REQUESTS = [
    dict(track="community"),
    dict(definition_id=1, frequency_horizon="00:15:00::1 day"),
    dict(scope_type="definition", definition_id=1),
    dict(scope_type="frequency_horizon", frequency_horizon="00:15:00::1 day"),
    dict(scope_type="global"),
    dict(scope_type="banana"),
    dict(calculation_date="yesterday"),
    dict(calculation_date="2026-13-01"),
    dict(calculation_date="14.10.2026"),
    # several faults at once: the same one is reported first
    dict(track="community", definition_id=1, frequency_horizon="x", calculation_date="nope"),
    dict(definition_id=1, frequency_horizon="x", calculation_date="nope"),
    dict(scope_type="banana", calculation_date="nope"),
]


@pytest.mark.parametrize("kwargs", INVALID_REQUESTS)
def test_invalid_requests_get_the_same_400_as_the_elo_endpoint(kwargs):
    elo = _error(_elo, **kwargs)
    assert elo[0] == 400
    assert _error(_arena, **kwargs) == elo


@pytest.mark.parametrize("metric", ["mase", "sql", "", "Point", "probabilistic "])
def test_unknown_metrics_are_rejected(metric):
    assert _error(_arena, metric=metric) == (400, "Invalid metric. Use 'point' or 'probabilistic'.")


@pytest.mark.parametrize("metric", ["point", "probabilistic"])
def test_known_metrics_pass(metric):
    assert validate_metric(metric) == metric


def test_the_metric_is_checked_before_everything_else():
    assert _error(_arena, metric="sql", track="community")[1].startswith("Invalid metric")


@pytest.mark.parametrize("args,expected", [
    ((None, None, None), ("global", None)),
    ((7, None, None), ("definition", "7")),
    ((0, None, None), ("definition", "0")),
    ((None, "00:15:00::1 day", None), ("frequency_horizon", "00:15:00::1 day")),
    ((None, None, "definition"), ("definition", None)),
    ((None, None, "frequency_horizon"), ("frequency_horizon", None)),
])
def test_scope_resolution(args, expected):
    assert resolve_scope(*args) == expected


def test_dates_parse_or_fail_with_the_parameter_name():
    assert parse_date("2026-10-14", "calculation_date") == date(2026, 10, 14)
    assert parse_date(None, "calculation_date") is None
    assert parse_date("", "calculation_date") is None
    with pytest.raises(HTTPException) as exc:
        parse_date("14/10/2026", "start_date")
    assert (exc.value.status_code, exc.value.detail) == (400, "Invalid start_date format. Use YYYY-MM-DD.")


@pytest.mark.parametrize("day,expected", [
    (date(2026, 10, 31), True),
    (date(2026, 10, 30), False),
    (date(2026, 2, 28), True),
    (date(2028, 2, 28), False),
    (date(2028, 2, 29), True),
    (date(2026, 12, 31), True),
    (date(2026, 1, 1), False),
])
def test_month_end(day, expected):
    assert is_month_end(day) is expected


# --- routing ----------------------------------------------------------------------------

def _app(monkeypatch):
    """The real app, without the process-wide logging setup its import would run."""
    from app.core import logging_setup
    monkeypatch.setattr(logging_setup, "configure_logging", lambda *a, **k: None)
    monkeypatch.setattr(logging_setup, "install_access_log_filter", lambda *a, **k: None)
    monkeypatch.delitem(sys.modules, "app.main", raising=False)
    return importlib.import_module("app.main").app


def _endpoint(app, path):
    scope = {"type": "http", "method": "GET", "path": path}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match == Match.FULL:
            return route.endpoint
    return None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_the_static_arena_paths_are_not_taken_by_the_model_id_route(monkeypatch):
    app = _app(monkeypatch)

    assert _endpoint(app, "/api/v1/models/arena-scores") is arena_routes.get_arena_scores
    assert _endpoint(app, "/api/v1/models/arena-score-filters") is arena_routes.get_arena_score_filters
    assert _endpoint(app, "/api/v1/models/17/arena-scores") is arena_routes.get_model_arena_scores


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_the_elo_routes_still_resolve_to_their_handlers(monkeypatch):
    app = _app(monkeypatch)

    assert _endpoint(app, "/api/v1/models/rankings") is elo_routes.get_filtered_rankings
    assert _endpoint(app, "/api/v1/models/ranking-filters") is elo_routes.get_ranking_filters
    assert _endpoint(app, "/api/v1/models/17/rankings") is elo_routes.get_model_rankings
    assert _endpoint(app, "/api/v1/models/17") is elo_routes.get_model_details
