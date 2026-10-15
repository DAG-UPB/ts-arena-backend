"""Daily Arena Score: rate every scope and metric, store it with the 30-day MASE and SQL.

Each definition is read once and reduced to pair sums and coverage tables. Its own scope is
rated at once; the global and frequency-horizon scopes fold the sums of their definitions in
as they come, so neither the rows nor the sums of all definitions are in memory together.

The naive forecast (the context's last value, flat over the horizon) joins every task as an
extra model and is the zero point of every score. Its SQL is its MAE over the scale: a point
forecast scores all quantile levels at the point.
"""
import asyncio
import logging
import time
from collections import Counter
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Tuple

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.forecasts.arena_scores_repository import ArenaScoresRepository
from app.services import arena_score as arena
from app.services.mase_window import WindowScore, compute_window_scores, definition_scopes

logger = logging.getLogger(__name__)

ERROR_COLUMN = {"point": "mae", "probabilistic": "sql_score"}
NAIVE_MODEL_ID = -1  # the internal naive forecast in the fit; never a stored row
WINDOW_COLUMNS = ("mase_30d", "participation_30d", "sql_30d", "sql_participation_30d", "tasks_30d")

GroupKey = Tuple[str, Optional[str], str]  # scope type, scope id, metric
WindowKey = Tuple[str, Optional[str], int]  # scope type, scope id, model


class ScopeSums(NamedTuple):
    """What the rating of one scope and metric needs from the definitions it holds."""

    pairs: arena.PairSums
    rounds: List[Tuple[int, int, float, int]]  # (definition, round, registration start, tasks)
    model_tasks: List[Tuple[int, int, int, int]]  # (definition, round, model, full-count tasks)


def _count_pairs(
    a: np.ndarray, b: np.ndarray, where: Optional[np.ndarray] = None
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """The distinct (a, b) pairs in order, with the number of rows of each (only rows in `where`)."""
    base = int(b.max()) + 1
    key, inverse = np.unique(a * base + b, return_inverse=True)
    count = np.bincount(inverse.ravel(), weights=where, minlength=len(key)).astype(np.int64)
    return key // base, key % base, count


def full_count(rnd: np.ndarray, series: np.ndarray, n_points: np.ndarray) -> np.ndarray:
    """Whether each row has the most points of its task; only those rows earn coverage."""
    base = int(series.max()) + 1
    _, task = np.unique(rnd * base + series, return_inverse=True)
    task = task.ravel()
    most = np.zeros(task.max() + 1, dtype=n_points.dtype)
    np.maximum.at(most, task, n_points)
    return n_points == most[task]


def naive_error(metric: str, rows: Mapping[str, np.ndarray]) -> np.ndarray:
    """The naive forecast's error on each row's points; NaN where it is not known."""
    with np.errstate(divide="ignore", invalid="ignore"):
        err = rows["naive_mae"] if metric == "point" else rows["naive_mae"] / rows["scale"]
        return np.where(np.isfinite(err) & (err >= 0), err, np.nan)


def naive_rows(rnd, series, err, n_points, day):
    """One naive row per task, from the task's row with the most points and a known naive error."""
    known = ~np.isnan(err)
    rnd, series, err, n_points, day = rnd[known], series[known], err[known], n_points[known], day[known]
    if not len(rnd):
        return rnd, series, err, n_points, day
    order = np.lexsort((-n_points, series, rnd))
    first = order[np.r_[True, (rnd[order][1:] != rnd[order][:-1]) | (series[order][1:] != series[order][:-1])]]
    return rnd[first], series[first], err[first], n_points[first], day[first]


def summarize_definition(definition_id: int, rows: Mapping[str, np.ndarray]) -> Dict[str, ScopeSums]:
    """Pair sums and coverage tables of one definition, for each metric it has a kept row in."""
    out: Dict[str, ScopeSums] = {}
    for metric, column in ERROR_COLUMN.items():
        use = ~np.isnan(rows[column])
        if not use.any():
            continue
        rnd, series, model = (rows[k][use] for k in ("round_id", "series_id", "model_id"))
        n_r, n_s, n_e, n_n, n_d = naive_rows(rnd, series, naive_error(metric, rows)[use],
                                             rows["n_points"][use], rows["day"][use])
        pairs = arena.pair_sums(
            np.r_[rnd, n_r], np.r_[series, n_s], np.r_[model, np.full(len(n_r), NAIVE_MODEL_ID)],
            np.r_[rows[column][use], n_e], np.r_[rows["n_points"][use], n_n], np.r_[rows["day"][use], n_d],
        )
        task_round, _, _ = _count_pairs(rnd, series)
        round_ids, round_tasks = np.unique(task_round, return_counts=True)
        registered = rows["registered"][use][np.unique(rnd, return_index=True)[1]]
        full = full_count(rnd, series, rows["n_points"][use])
        model_round, model_id, model_rows = _count_pairs(rnd, model, where=full)
        out[metric] = ScopeSums(
            pairs=pairs,
            rounds=[(definition_id, r, s, n) for r, s, n in
                    zip(round_ids.tolist(), registered.tolist(), round_tasks.tolist())],
            model_tasks=[(definition_id, r, m, n) for r, m, n in
                         zip(model_round.tolist(), model_id.tolist(), model_rows.tolist())],
        )
    return out


def fold_definition(
    aggregates: Dict[GroupKey, ScopeSums], definition: Dict[str, Any], sums: Mapping[str, ScopeSums]
) -> None:
    """Add a definition's sums to the global and frequency-horizon scopes it belongs to."""
    for metric, part in sums.items():
        for scope_type, scope_id in definition_scopes(definition):
            if scope_type == "definition":
                continue
            key = (scope_type, scope_id, metric)
            held = aggregates.get(key)
            aggregates[key] = part if held is None else ScopeSums(
                pairs=arena.combine_pair_sums([held.pairs, part.pairs]),
                rounds=held.rounds + part.rounds,
                model_tasks=held.model_tasks + part.model_tasks,
            )


def window_columns(scores: Iterable[WindowScore]) -> Dict[WindowKey, Dict[str, Any]]:
    """The 30-day columns of each (scope type, scope id, model), from `compute_window_scores`."""
    out: Dict[WindowKey, Dict[str, Any]] = {}
    for s in scores:
        columns = out.setdefault((s.scope_type, s.scope_id, s.model_id), {})
        columns["tasks_30d"] = s.scope_tasks
        if s.metric == "mase":
            columns.update(mase_30d=s.value, participation_30d=s.participation)
        else:
            columns.update(sql_30d=s.value, sql_participation_30d=s.participation)
    return out


def _label(scope_type: str, scope_id: Optional[str], metric: str) -> str:
    return f"{scope_type if scope_id is None else f'{scope_type} {scope_id}'} [{metric}]"


def rate_group(
    calculation_date: date,
    key: GroupKey,
    sums: ScopeSums,
    window: Mapping[WindowKey, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Rate one scope and metric: the rows for `daily_arena_scores`, one per model with a kept row."""
    scope_type, scope_id, metric = key
    label = _label(*key)
    covered = arena.coverage(sums.rounds, sums.model_tasks)
    rating = arena.rate_scope(sums.pairs, list(covered), covered, scope=label, reference=NAIVE_MODEL_ID)
    logger.info(
        "Arena Score %s: %d models, %d pairs; skipped %d for unequal point counts, %d with exactly one zero error",
        label, len(rating.models), sums.pairs.n_pairs, sums.pairs.skipped_points, len(sums.pairs.skipped_zero),
    )

    tasks_played: Counter = Counter()
    rounds_played: Counter = Counter()
    for _, _, model, tasks in sums.model_tasks:
        tasks_played[model] += tasks
        rounds_played[model] += int(tasks > 0)

    return [
        {
            "calculation_date": calculation_date,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "metric": metric,
            "model_id": m.model_id,
            "score": m.score,
            "ci_lower": m.ci_lower,
            "ci_upper": m.ci_upper,
            "rank_best": m.rank_best,
            "rank_worst": m.rank_worst,
            "provisional": m.provisional,
            "low_coverage": m.low_coverage,
            "unranked_disconnected": m.unranked_disconnected,
            "coverage": m.coverage,
            "tasks_played": tasks_played[m.model_id],
            "rounds_played": rounds_played[m.model_id],
            "weeks_played": m.weeks,
            **dict.fromkeys(WINDOW_COLUMNS),
            **window.get((scope_type, scope_id, m.model_id), {}),
            "method": rating.method,
        }
        for m in rating.models
    ]


async def compute_and_store_arena_scores(session: AsyncSession, calculation_date: date) -> Dict[str, Any]:
    """Rate every scope and metric from all final scores and store the day's rows.

    Each (date, scope, metric) group is replaced as a whole; a group that fails is logged and left
    as it was while the others go on. Returns a summary: scopes, groups, rows written per scope
    type and metric, and the failures.
    """
    started = time.monotonic()
    repo = ArenaScoresRepository(session)

    async with repo.read_transaction():
        definitions = await repo.definitions()
        window = window_columns(await compute_window_scores(session, calculation_date))

    summary: Dict[str, Any] = {"calculation_date": calculation_date, "rows": {}, "failures": []}
    groups: set = set()
    stored: set = set()  # (scope type, scope id, model) of the rows written

    async def store(key: GroupKey, sums: ScopeSums) -> None:
        scope_type, scope_id, metric = key
        groups.add(key)
        try:
            rows = await asyncio.to_thread(rate_group, calculation_date, key, sums, window)
            await repo.replace_scores(calculation_date, scope_type, scope_id, metric, rows)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error("Arena Score %s failed, nothing written for it: %s", _label(*key), e, exc_info=True)
            summary["failures"].append({
                "scope_type": scope_type, "scope_id": scope_id, "metric": metric,
                "error": f"{type(e).__name__}: {e}",
            })
            return
        per_metric = summary["rows"].setdefault(scope_type, {})
        per_metric[metric] = per_metric.get(metric, 0) + len(rows)
        stored.update((scope_type, scope_id, row["model_id"]) for row in rows)

    aggregates: Dict[GroupKey, ScopeSums] = {}
    for definition in definitions:
        definition_id = definition["definition_id"]
        async with repo.read_transaction():
            rows = await repo.kept_rows(definition_id)
        sums = await asyncio.to_thread(summarize_definition, definition_id, rows)
        del rows
        for metric, part in sums.items():
            await store(("definition", str(definition_id), metric), part)
        await asyncio.to_thread(fold_definition, aggregates, definition, sums)

    for key in list(aggregates):
        await store(key, aggregates.pop(key))

    summary["groups"] = len(groups)
    summary["scopes"] = len({(scope_type, scope_id) for scope_type, scope_id, _ in groups})
    summary["window_unmatched"] = len(set(window) - stored)
    summary["seconds"] = round(time.monotonic() - started, 1)
    if summary["window_unmatched"] and not summary["failures"]:
        logger.warning("Arena Score: %d 30-day entries belong to no stored row", summary["window_unmatched"])
    logger.info(
        "Arena Score %s: %d groups in %d scopes, %d rows, %d failed, %.1f s",
        calculation_date, summary["groups"], summary["scopes"],
        sum(n for per_metric in summary["rows"].values() for n in per_metric.values()),
        len(summary["failures"]), summary["seconds"],
    )
    return summary
