"""Arena Score read side: request validation and the parts of a leaderboard computed on read."""
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import HTTPException

METRICS = ("point", "probabilistic")
SCOPE_TYPES = ("definition", "frequency_horizon")

_SCOPE_GROUP = {"global": 0, "definition": 1, "frequency_horizon": 2}

RankRange = Tuple[Optional[int], Optional[int]]


def validate_metric(metric: str) -> str:
    if metric not in METRICS:
        raise HTTPException(
            status_code=400,
            detail="Invalid metric. Use 'point' or 'probabilistic'.",
        )
    return metric


def resolve_scope(
    definition_id: Optional[int],
    frequency_horizon: Optional[str],
    scope_type: Optional[str],
) -> Tuple[str, Optional[str]]:
    """The (scope_type, scope_id) a request asks for; scope_id None means every scope of the type."""
    if definition_id is not None and frequency_horizon is not None:
        raise HTTPException(
            status_code=400,
            detail="Only one scope filter can be applied at a time. Provide either 'definition_id' OR 'frequency_horizon', not both.",
        )

    if scope_type is not None and (definition_id is not None or frequency_horizon is not None):
        raise HTTPException(
            status_code=400,
            detail="'scope_type' returns all scopes of a type and cannot be combined with 'definition_id' or 'frequency_horizon'.",
        )

    if scope_type is not None:
        if scope_type not in SCOPE_TYPES:
            raise HTTPException(
                status_code=400,
                detail="Invalid scope_type. Use 'definition' or 'frequency_horizon'.",
            )
        return scope_type, None
    if definition_id is not None:
        return "definition", str(definition_id)
    if frequency_horizon is not None:
        return "frequency_horizon", frequency_horizon
    return "global", None


def parse_calculation_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid calculation_date format. Use YYYY-MM-DD.",
        )


def is_month_end(day: date) -> bool:
    return date.fromordinal(day.toordinal() + 1).day == 1


def _ranges_within(bounds: Sequence[Tuple[float, float]]) -> List[Tuple[int, int]]:
    return [
        (
            1 + sum(1 for o, (lo_o, _) in enumerate(bounds) if o != m and lo_o > hi_m),
            1 + sum(1 for o, (_, hi_o) in enumerate(bounds) if o != m and hi_o >= lo_m),
        )
        for m, (lo_m, hi_m) in enumerate(bounds)
    ]


def track_rank_ranges(
    entries: Sequence[Tuple[str, Optional[float], Optional[float], bool]],
) -> List[RankRange]:
    """(rank_best, rank_worst) of each (track, ci_lower, ci_upper, ranked) entry within its track.

    best = 1 + others whose lower bound is above this upper bound; worst = 1 + others whose
    upper bound is at or above this lower bound. Unranked entries get (None, None).
    """
    members: Dict[str, List[int]] = defaultdict(list)
    for k, (track, lo, hi, is_ranked) in enumerate(entries):
        if is_ranked and lo is not None and hi is not None:
            members[track].append(k)

    out: List[RankRange] = [(None, None)] * len(entries)
    for indices in members.values():
        ranges = _ranges_within([(entries[k][1], entries[k][2]) for k in indices])
        for k, rank_range in zip(indices, ranges):
            out[k] = rank_range
    return out


def places(entries: Sequence[Tuple[Any, Optional[float], bool]]) -> List[Optional[int]]:
    """Place of each (group, score, ranked) entry: 1 + the ranked entries of its group with a higher score.

    Equal scores share a place. Unranked entries get None and do not count. The place lies
    within the rank range whenever each interval contains its score.
    """
    scores: Dict[Any, List[float]] = defaultdict(list)
    for group, score, is_ranked in entries:
        if is_ranked and score is not None:
            scores[group].append(score)
    return [
        1 + sum(1 for other in scores[group] if other > score)
        if is_ranked and score is not None else None
        for group, score, is_ranked in entries
    ]


def leaderboard_order(row: Dict[str, Any]) -> Tuple:
    """Score descending, models without a score last, then model name."""
    score = row.get("score")
    return (
        score is None,
        0.0 if score is None else -score,
        (row.get("model_name") or "").casefold(),
        row.get("model_id") or 0,
    )


def scope_order(scope_type: str, scope_id: Optional[str]) -> Tuple:
    """Global first, then definitions by id, then frequency-horizon scopes by name."""
    group = _SCOPE_GROUP.get(scope_type, len(_SCOPE_GROUP))
    if scope_type == "definition":
        try:
            return (group, 0, int(scope_id), "")
        except (TypeError, ValueError):
            pass
    return (group, 1, 0, scope_id or "")


def arrange_leaderboard(
    rows: List[Dict[str, Any]], track: Optional[str], limit: int
) -> List[Dict[str, Any]]:
    """Sort each scope, add the places and the track rank range, keep one track, cut to `limit` per scope.

    Places and ranges are computed before the track filter and the limit, so neither changes them.
    """
    by_scope: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_scope[(row["scope_type"], row["scope_id"], row["calculation_date"])].append(row)

    out: List[Dict[str, Any]] = []
    for key in sorted(by_scope, key=lambda k: scope_order(k[0], k[1]) + (k[2],)):
        scope_rows = sorted(by_scope[key], key=leaderboard_order)
        ranges = track_rank_ranges([
            (r["track"], r["ci_lower"], r["ci_upper"], r["rank_best"] is not None)
            for r in scope_rows
        ])
        for row, (best, worst) in zip(scope_rows, ranges):
            row["track_rank_best"], row["track_rank_worst"] = best, worst
        overall = places([(None, r["score"], r["rank_best"] is not None) for r in scope_rows])
        in_track = places([(r["track"], r["score"], r["rank_best"] is not None) for r in scope_rows])
        for row, place, track_place in zip(scope_rows, overall, in_track):
            row["rank"], row["track_rank"] = place, track_place
        if track is not None:
            scope_rows = [r for r in scope_rows if r["track"] == track]
        out.extend(scope_rows[:limit])
    return out
