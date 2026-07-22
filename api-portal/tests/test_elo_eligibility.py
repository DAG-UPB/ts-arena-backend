"""Unit tests for the global-ranking full-participation rule (ts-arena-1).

These exercise the pure eligibility computation
`EloRankingService._compute_eligible_global_models` against in-memory row fixtures — no DB.
The rule: a model is eligible for the OVERALL ranking iff, for every challenge in the
universe, it has seen the challenge since it joined the platform AND covered >= tau of that
challenge's rounds available since it joined. The join point is the model's earliest round
across ANY challenge; rounds before a model joined never count against it.

Row shape mirrors the SQL fetch: (model_id, definition_id, round_id, registration_start).
A round_id is shared across models (same round), so a round is "available" for a challenge
as soon as at least one model was scored in it.
"""
from datetime import datetime, timedelta

from app.services.elo_ranking_service import EloRankingService

compute = EloRankingService._compute_eligible_global_models

_D0 = datetime(2026, 1, 1)


def _day(n: int) -> datetime:
    return _D0 + timedelta(days=n)


def _rows(*specs):
    """Build rows from (model_id, definition_id, [day_indices]) specs.

    round_id is deterministic per (definition, day) so the same round is shared by every
    model that participated in it.
    """
    out = []
    for model_id, def_id, days in specs:
        for d in days:
            out.append((model_id, def_id, f"{def_id}-{d}", _day(d)))
    return out


def test_empty_returns_empty_set():
    assert compute([], 0.5) == set()


def test_full_participant_is_eligible():
    rows = _rows((1, 1, range(1, 11)), (1, 2, range(1, 11)))
    assert compute(rows, 0.5) == {1}


def test_narrow_participant_missing_a_whole_challenge_excluded():
    # model 1 covers both challenges (makes def 2 rounds available);
    # model 2 covers only challenge 1 and never enters challenge 2 -> excluded.
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 11)),
        (2, 1, range(1, 11)),  # nothing in def 2
    )
    assert compute(rows, 0.5) == {1}


def test_one_challenge_below_tau_excluded():
    # model 3 fully covers def 1 but only 4/10 of def 2 (0.4 < 0.5) -> excluded.
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 11)),
        (3, 1, range(1, 11)), (3, 2, range(1, 5)),
    )
    assert compute(rows, 0.5) == {1}


def test_near_miss_exactly_at_tau_included():
    # model 4 covers 5/10 of def 2 (== 0.5) -> stays (mirrors tirex-2's 0.571 near-miss).
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 11)),
        (4, 1, range(1, 11)), (4, 2, range(1, 6)),
    )
    assert compute(rows, 0.5) == {1, 4}


def test_tau_threshold_is_strict_below():
    # 4/10 = 0.4 fails at tau=0.5; the same coverage passes at tau=0.4 (inclusive).
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 11)),
        (3, 1, range(1, 11)), (3, 2, range(1, 5)),
    )
    assert 3 not in compute(rows, 0.5)
    assert 3 in compute(rows, 0.4)


def test_late_joiner_measured_from_join_not_penalised_for_prejoin_rounds():
    # model 6 joins on day 6 and covers every round since (5/5 in each challenge) -> eligible,
    # even though in absolute terms it only did 5 of 10 rounds per challenge.
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 11)),
        (6, 1, range(6, 11)), (6, 2, range(6, 11)),
    )
    assert compute(rows, 0.5) == {1, 6}


def test_fresh_model_not_having_seen_all_challenges_excluded():
    # Universe: def 1 runs days 1-10, def 2 only days 1-7 (no rounds after day 7).
    # veteran (1) has seen and covered both -> eligible.
    # fresh (5) joins day 8: covers def 1 since join, but def 2 has ZERO rounds since it
    # joined -> "not seen" -> held out until it has seen every challenge.
    rows = _rows(
        (1, 1, range(1, 11)), (1, 2, range(1, 8)),
        (5, 1, range(8, 11)),  # no def 2 rounds exist at/after day 8
    )
    eligible = compute(rows, 0.5)
    assert 1 in eligible
    assert 5 not in eligible
