"""Rescore the SQL columns of `forecasts.forecast_scores` rows whose forecasts miss deciles, one transaction per round.

    python -m app.scripts.rescore_missing_deciles [flags]

    --dry-run                 compute in read-only transactions, write nothing
    --round-id X              only round X

One scan of `forecasts.forecasts` finds the (round, model, series) with a timestamp whose deciles are
neither all nine nor none. Each is scored again with `build_score_row` on the actuals its row was scored
on (SCD2 as of the row's `calculated_at`), and only the SQL columns are written. A row is written only when
those actuals reproduce its stored `mae` and point count; every other column stays as it is. Rows that
already carry the current values are skipped, so a rerun writes nothing.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text

# A bare script must import every mapper the relationships reach, or mapper configuration fails.
from app.database.models.model_info import ModelInfo  # noqa: F401
from app.database.auth.user import User  # noqa: F401
from app.database.auth.organization import Organization  # noqa: F401
from app.database.auth.api_key import APIKey  # noqa: F401
from app.database.connection import SessionLocal
from app.database.challenges.challenge_repository import ChallengeRoundRepository
from app.database.forecasts.forecast_scores_repository import ForecastScoresRepository
from app.database.forecasts.repository import ForecastRepository
from app.services.evaluation_alignment import align_evaluation_data, group_actuals_by_minute
from app.services.forecast_metrics import QUANTILE_LEVELS
from app.services.forecast_scoring_service import (
    build_score_row,
    series_forecast_windows,
    timedelta_to_resolution,
)
from app.services.series_scale_service import resolution_bucket

logger = logging.getLogger("rescore-missing-deciles")

TOLERANCE = 1e-9

_SCORED = ("complete", "partial", "undefined_scale")

_KEYS = ("round_id", "model_id", "series_id")

SQL_FIELDS = ("sql_score", "sql_per_quantile", "has_quantiles", "quantile_levels_count", "quantile_crossing_count")

_CANONICAL_KEYS = [f"q_{level}" for level in QUANTILE_LEVELS]
_BARE_KEYS = [str(level) for level in QUANTILE_LEVELS]

Pair = Tuple[int, int]


def close(a: Optional[float], b: Optional[float], rel: float = TOLERANCE) -> bool:
    if a is None or b is None:
        return a is b
    return a == b or abs(a - b) <= rel * max(abs(a), abs(b))


def same_value(old: Any, new: Any) -> bool:
    if isinstance(old, dict) and isinstance(new, dict):
        return old.keys() == new.keys() and all(same_value(old[k], new[k]) for k in old)
    if isinstance(old, float) or isinstance(new, float):
        return close(old, new, rel=1e-12)
    return old == new


def rescore_pair(
    stored: Dict[str, Any],
    forecast_rows: List[Dict[str, Any]],
    actuals_by_minute: Dict[datetime, List[float]],
    scale_row: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool]:
    """The row a fresh score gives on these actuals, and whether it reproduces the stored `mae`, point count and scale."""
    fresh = build_score_row(
        stored["round_id"], stored["model_id"], stored["series_id"], len(forecast_rows),
        align_evaluation_data(forecast_rows, actuals_by_minute), scale_row, stored["final_evaluation"],
    )
    reproduced = (
        fresh["mae"] is not None
        and fresh["n_points"] == stored["n_points"]
        and close(fresh["mae"], stored["mae"])
        and close(fresh["scale"], stored["scale"])
    )
    return fresh, reproduced


def sql_update(stored: Dict[str, Any], fresh: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The SQL columns to write for a row; None when it already carries these values."""
    if all(same_value(stored[k], fresh[k]) for k in SQL_FIELDS):
        return None
    return {**{k: stored[k] for k in _KEYS}, **{k: fresh[k] for k in SQL_FIELDS}}


@dataclass
class Summary:
    pairs_found: int = 0
    rounds: int = 0
    rounds_failed: int = 0
    no_score_row: List[Tuple[int, int, int]] = field(default_factory=list)
    not_scored: Counter = field(default_factory=Counter)
    checked: int = 0
    mae_exact: int = 0
    mae_close: int = 0
    mae_max_rel_diff: float = 0.0
    not_reproduced: List[tuple] = field(default_factory=list)
    unchanged: int = 0
    to_write: int = 0
    written: int = 0
    transitions: Counter = field(default_factory=Counter)
    sql_ratio: List[float] = field(default_factory=list)
    sql_ratio_unverified: List[float] = field(default_factory=list)
    pairs_by_model: Counter = field(default_factory=Counter)
    failed: List[tuple] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def record_row(
        self,
        stored: Dict[str, Any],
        fresh: Dict[str, Any],
        reproduced: bool,
        update: Optional[Dict[str, Any]],
    ) -> None:
        self.checked += 1
        old_sql, new_sql = stored["sql_score"], fresh["sql_score"]
        ratio = new_sql / old_sql if old_sql and new_sql is not None else None
        if not reproduced:
            self.not_reproduced.append((
                stored["round_id"], stored["model_id"], stored["series_id"], stored["mae"], fresh["mae"],
            ))
            if update is not None and ratio is not None:
                self.sql_ratio_unverified.append(ratio)
            return
        if fresh["mae"] == stored["mae"]:
            self.mae_exact += 1
        else:
            self.mae_close += 1
            self.mae_max_rel_diff = max(
                self.mae_max_rel_diff, abs(fresh["mae"] - stored["mae"]) / abs(stored["mae"])
            )
        if update is None:
            self.unchanged += 1
            return
        self.to_write += 1
        self.transitions[(stored["has_quantiles"], fresh["has_quantiles"])] += 1
        if ratio is not None:
            self.sql_ratio.append(ratio)

    def report(self, dry_run: bool, names: Dict[int, str]) -> str:
        def spread(values: List[float]) -> str:
            if not values:
                return "-"
            return f"min {min(values):.4f}, median {np.median(values):.4f}, max {max(values):.4f}"

        lines = [
            "",
            "=" * 72,
            f"Rescore of rows with missing deciles{' (DRY RUN, read-only)' if dry_run else ''}",
            "=" * 72,
            f"(round, model, series) found by the scan: {self.pairs_found} in {self.rounds} round(s)",
        ]
        lines += [f"  {names.get(m, m)}: {n}" for m, n in self.pairs_by_model.most_common()]
        lines += [
            f"No score row for the pair:    {len(self.no_score_row)}",
            f"Score row not scored:         {sum(self.not_scored.values())} {dict(self.not_scored)}",
            f"Scored rows checked:          {self.checked}",
            f"  mae reproduced exactly:     {self.mae_exact}",
            f"  mae within {TOLERANCE:g} relative:  {self.mae_close} (largest difference {self.mae_max_rel_diff:.1e})",
            f"  not reproduced (not written): {len(self.not_reproduced)}",
        ]
        lines += [
            f"    (round, model, series, stored mae, mae on the as-of actuals): {r}"
            for r in self.not_reproduced[:10]
        ]
        lines += [
            f"Rows already current:         {self.unchanged}",
            f"Rows to write:                {self.to_write}",
        ]
        lines += [f"  has_quantiles {old} -> {new}: {n}" for (old, new), n in self.transitions.most_common()]
        lines += [
            f"  new / old sql_score:        {spread(self.sql_ratio)}",
            f"Not reproduced, would change: {len(self.sql_ratio_unverified)} "
            f"(new / old sql_score: {spread(self.sql_ratio_unverified)})",
            f"Rows written:                 {self.written}",
            f"Rounds failed:                {self.rounds_failed}",
        ]
        lines += [f"    failed round {rid}: {err}" for rid, err in self.failed[:20]]
        lines.append(f"Elapsed: {time.monotonic() - self.started_at:.1f}s")
        lines.append("=" * 72)
        return "\n".join(lines)


async def scan_forecasts(round_id: Optional[int]) -> Dict[int, Dict[Pair, Tuple[datetime, datetime]]]:
    """`round_id -> {(model_id, series_id): (first ts, last ts)}` of forecasts with some deciles but not all nine.

    A point is left out when its `probabilistic_values` is empty or holds all nine deciles, in either
    key form, as numbers. Anything else is returned; the rescore decides what the rule changes.
    """
    sql = """
        SELECT round_id, model_id, series_id, min(ts), max(ts)
        FROM forecasts.forecasts
        WHERE round_id IS NOT NULL AND model_id IS NOT NULL
          AND probabilistic_values IS NOT NULL
          AND probabilistic_values <> CAST('null' AS jsonb)
          AND probabilistic_values <> CAST('{}' AS jsonb)
          AND NOT (
              jsonb_typeof(probabilistic_values) = 'object'
              AND (probabilistic_values ?& CAST(:canonical AS text[]) OR probabilistic_values ?& CAST(:bare AS text[]))
              AND NOT probabilistic_values @? '$.* ? (@.type() != "number")'
          )
          AND (CAST(:round_id AS integer) IS NULL OR round_id = CAST(:round_id AS integer))
        GROUP BY round_id, model_id, series_id
        ORDER BY round_id, model_id, series_id
    """
    params: Dict[str, Any] = {"canonical": _CANONICAL_KEYS, "bare": _BARE_KEYS, "round_id": round_id}
    found: Dict[int, Dict[Pair, Tuple[datetime, datetime]]] = defaultdict(dict)
    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        # Parallel query workers need shared memory.
        await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
        result = await session.execute(text(sql), params)
        for rid, model_id, series_id, first, last in result.fetchall():
            found[rid][(model_id, series_id)] = (first, last)
    return found


async def model_names(model_ids: List[int]) -> Dict[int, str]:
    if not model_ids:
        return {}
    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await session.execute(
            text("SELECT id, name FROM models.model_info WHERE id = ANY(CAST(:ids AS integer[]))"),
            {"ids": model_ids},
        )
        return {row[0]: row[1] for row in result.fetchall()}


async def rescore_round(
    session: Any,
    round_id: int,
    pairs: Dict[Pair, Tuple[datetime, datetime]],
    summary: Summary,
) -> List[Dict[str, Any]]:
    """The SQL updates for the rows of a round that are verified against their actuals. Reads only."""
    repo = ForecastScoresRepository(session)
    round_repo = ChallengeRoundRepository(session)
    info = await round_repo.get_by_id(round_id)
    bucket = resolution_bucket(timedelta_to_resolution(info.frequency))
    windows = series_forecast_windows(
        await round_repo.get_series_context_edges(round_id), info.frequency, info.horizon
    )
    forecasts: Dict[Pair, List[Dict[str, Any]]] = defaultdict(list)
    ts_lo = min(first for first, _ in pairs.values())
    ts_hi = max(last for _, last in pairs.values())
    for row in await ForecastRepository(session).get_round_forecasts(round_id, ts_lo, ts_hi):
        pair = (row["model_id"], row["series_id"])
        window = windows.get(row["series_id"])
        if pair in pairs and (window is None or window[0] <= row["ts"] <= window[1]):
            forecasts[pair].append(row)

    scales = await repo.get_scales(round_id)
    stored_rows = {
        (row["model_id"], row["series_id"]): row
        for row in await repo.get_score_rows(round_id)
        if (row["model_id"], row["series_id"]) in pairs
    }
    summary.no_score_row += [(round_id, *pair) for pair in pairs if pair not in stored_rows]

    by_vintage: Dict[datetime, List[Dict[str, Any]]] = defaultdict(list)
    for row in stored_rows.values():
        if row["evaluation_status"] in _SCORED:
            by_vintage[row["calculated_at"]].append(row)
        else:
            summary.not_scored[row["evaluation_status"]] += 1

    updates: List[Dict[str, Any]] = []
    for calculated_at, rows in sorted(by_vintage.items()):
        series_ids = sorted({row["series_id"] for row in rows})
        lo = min(pairs[(row["model_id"], row["series_id"])][0] for row in rows)
        hi = max(pairs[(row["model_id"], row["series_id"])][1] for row in rows)
        actuals: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for point in await repo.read_actuals_as_of(series_ids, bucket, calculated_at, lo - bucket, hi):
            actuals[point["series_id"]].append(point)
        for row in rows:
            fresh, reproduced = rescore_pair(
                row, forecasts[(row["model_id"], row["series_id"])],
                group_actuals_by_minute(actuals[row["series_id"]]), scales.get(row["series_id"]),
            )
            update = sql_update(row, fresh)
            summary.record_row(row, fresh, reproduced, update)
            if reproduced and update is not None:
                updates.append(update)
    return updates


async def process_round(
    round_id: int,
    pairs: Dict[Pair, Tuple[datetime, datetime]],
    args: argparse.Namespace,
    summary: Summary,
) -> None:
    async with SessionLocal() as session:
        try:
            if args.dry_run:
                await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
            repo = ForecastScoresRepository(session)
            if not args.dry_run:
                await repo.lock_round(round_id)

            updates = await rescore_round(session, round_id, pairs, summary)
            if args.dry_run or not updates:
                await session.rollback()
            else:
                await repo.update_forecast_scores(updates)
                await session.commit()
                summary.written += len(updates)
            summary.rounds += 1
        except Exception as e:
            await session.rollback()
            summary.rounds_failed += 1
            summary.failed.append((round_id, str(e)[:300]))
            logger.exception("Round %s failed", round_id)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rescore forecast_scores rows whose forecasts miss deciles.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report in read-only transactions.")
    parser.add_argument("--round-id", type=int, default=None, help="Process only this round.")
    return parser.parse_args(argv)


async def main(argv: Optional[List[str]] = None) -> Summary:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = _parse_args(argv)
    summary = Summary()

    found = await scan_forecasts(args.round_id)
    round_ids = sorted(found)
    summary.pairs_found = sum(len(found[rid]) for rid in round_ids)
    for rid in round_ids:
        summary.pairs_by_model.update(model_id for model_id, _ in found[rid])
    logger.info("%d (round, model, series) in %d round(s) (dry_run=%s)", summary.pairs_found, len(round_ids), args.dry_run)

    for i, rid in enumerate(round_ids, start=1):
        await process_round(rid, found[rid], args, summary)
        logger.info("Round %s done (%d/%d), %d row(s) written so far", rid, i, len(round_ids), summary.written)

    print(summary.report(args.dry_run, await model_names(list(summary.pairs_by_model))))
    return summary


if __name__ == "__main__":
    asyncio.run(main())
