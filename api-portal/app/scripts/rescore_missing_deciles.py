"""Rescore the SQL columns of `forecasts.forecast_scores` rows whose forecasts miss deciles, one transaction per round.

    python -m app.scripts.rescore_missing_deciles [flags]

    --dry-run                 compute in read-only transactions, write nothing
    --round-id X              only round X

One scan of `forecasts.forecasts` finds the (round, model, series) with a timestamp whose deciles are
neither all nine nor none. Each is scored again with `build_score_row` on the actuals its row was scored
on (SCD2 as of the row's `calculated_at`), and only the SQL columns are written. A row is written only when
it is on the current scale method, those actuals reproduce its stored `mae` within a relative
`MAE_TOLERANCE`, and its point count and scale match exactly; every other column stays as it is. Rows that
already carry the current values are skipped, so a rerun writes nothing.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from collections import defaultdict
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
from app.services.forecast_metrics import MASE_METHOD, QUANTILE_LEVELS
from app.services.forecast_scoring_service import (
    build_score_row,
    series_forecast_windows,
    timedelta_to_resolution,
)
from app.services.series_scale_service import resolution_bucket

logger = logging.getLogger("rescore-missing-deciles")

MAE_TOLERANCE = 1e-4

_SCORED = ("complete", "partial", "undefined_scale")

_KEYS = ("round_id", "model_id", "series_id")

SQL_FIELDS = ("sql_score", "sql_per_quantile", "has_quantiles", "quantile_levels_count", "quantile_crossing_count")

_CANONICAL_KEYS = [f"q_{level}" for level in QUANTILE_LEVELS]
_BARE_KEYS = [str(level) for level in QUANTILE_LEVELS]

Pair = Tuple[int, int]


def relative_deviation(new: float, old: float) -> float:
    if new == old:
        return 0.0
    return abs(new - old) / abs(old) if old else float("inf")


def rescore_pair(
    stored: Dict[str, Any],
    forecast_rows: List[Dict[str, Any]],
    actuals_by_minute: Dict[datetime, List[float]],
    scale_row: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool]:
    """The row a fresh score gives on these actuals, and whether it reproduces the stored row.

    The stored row has to be on the current scale method, its `mae` within `MAE_TOLERANCE` (relative),
    its point count and scale exact.
    """
    fresh = build_score_row(
        stored["round_id"], stored["model_id"], stored["series_id"], len(forecast_rows),
        align_evaluation_data(forecast_rows, actuals_by_minute), scale_row, stored["final_evaluation"],
    )
    reproduced = (
        stored["method"] == MASE_METHOD
        and fresh["mae"] is not None
        and stored["mae"] is not None
        and fresh["n_points"] == stored["n_points"]
        and fresh["scale"] == stored["scale"]
        and relative_deviation(fresh["mae"], stored["mae"]) <= MAE_TOLERANCE
    )
    return fresh, reproduced


def sql_update(stored: Dict[str, Any], fresh: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The SQL columns to write for a row; None when it already carries these values."""
    if all(stored[k] == fresh[k] for k in SQL_FIELDS):
        return None
    return {**{k: stored[k] for k in _KEYS}, **{k: fresh[k] for k in SQL_FIELDS}}


@dataclass
class Summary:
    pairs_found: int = 0
    rounds_failed: int = 0
    mae_exact: int = 0
    mae_close: int = 0
    mae_max_rel_diff: float = 0.0
    not_reproduced: List[tuple] = field(default_factory=list)
    unchanged: int = 0
    to_write: int = 0
    written: int = 0
    sql_ratio: List[float] = field(default_factory=list)
    failed: List[tuple] = field(default_factory=list)

    def record_row(
        self,
        stored: Dict[str, Any],
        fresh: Dict[str, Any],
        reproduced: bool,
        update: Optional[Dict[str, Any]],
    ) -> None:
        if not reproduced:
            self.not_reproduced.append((stored["round_id"], stored["model_id"], stored["series_id"], stored["mae"], fresh["mae"]))
            return
        deviation = relative_deviation(fresh["mae"], stored["mae"])
        if deviation == 0:
            self.mae_exact += 1
        else:
            self.mae_close += 1
        self.mae_max_rel_diff = max(self.mae_max_rel_diff, deviation)
        if update is None:
            self.unchanged += 1
            return
        self.to_write += 1
        if stored["sql_score"] and fresh["sql_score"] is not None:
            self.sql_ratio.append(fresh["sql_score"] / stored["sql_score"])

    def report(self, dry_run: bool) -> str:
        ratios = (
            f"min {min(self.sql_ratio):.4f}, median {np.median(self.sql_ratio):.4f}, max {max(self.sql_ratio):.4f}"
            if self.sql_ratio else "-"
        )
        lines = [
            "",
            "=" * 72,
            f"Rescore of rows with missing deciles{' (DRY RUN, read-only)' if dry_run else ''}",
            "=" * 72,
            f"(round, model, series) found by the scan: {self.pairs_found}",
            f"  mae reproduced exactly:       {self.mae_exact}",
            f"  mae within {MAE_TOLERANCE:.0e} relative:    {self.mae_close}",
            f"  not reproduced (not written): {len(self.not_reproduced)}",
        ]
        lines += [
            f"    (round, model, series, stored mae, mae on the as-of actuals; None: no scored row): {r}"
            for r in self.not_reproduced[:20]
        ]
        lines += [
            f"Max relative mae deviation among rescored rows: {self.mae_max_rel_diff:.1e}",
            f"Rows already current:           {self.unchanged}",
            f"Rows to write:                  {self.to_write}",
            f"  new / old sql_score:          {ratios}",
            f"Rows written:                   {self.written}",
            f"Rounds failed:                  {self.rounds_failed}",
        ]
        lines += [f"    failed round {rid}: {err}" for rid, err in self.failed[:20]]
        lines.append("=" * 72)
        return "\n".join(lines)


async def scan_forecasts(round_id: Optional[int]) -> Dict[int, List[Pair]]:
    """`round_id -> [(model_id, series_id), ...]` of forecasts with some deciles but not all nine.

    A point is left out when its `probabilistic_values` is empty or holds all nine deciles, in either
    key form, as numbers. Anything else is returned; the rescore decides what the rule changes.
    """
    sql = """
        SELECT DISTINCT round_id, model_id, series_id
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
        ORDER BY round_id, model_id, series_id
    """
    params: Dict[str, Any] = {"canonical": _CANONICAL_KEYS, "bare": _BARE_KEYS, "round_id": round_id}
    found: Dict[int, List[Pair]] = defaultdict(list)
    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        # Parallel query workers need shared memory.
        await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
        result = await session.execute(text(sql), params)
        for rid, model_id, series_id in result.fetchall():
            found[rid].append((model_id, series_id))
    return found


async def rescore_round(session: Any, round_id: int, pairs: List[Pair], summary: Summary) -> List[Dict[str, Any]]:
    """The SQL updates for the rows of a round that are verified against their actuals. Reads only."""
    repo = ForecastScoresRepository(session)
    round_repo = ChallengeRoundRepository(session)
    info = await round_repo.get_by_id(round_id)
    bucket = resolution_bucket(timedelta_to_resolution(info.frequency))
    windows = series_forecast_windows(
        await round_repo.get_series_context_edges(round_id), info.frequency, info.horizon
    )
    wanted = set(pairs)
    forecasts: Dict[Pair, List[Dict[str, Any]]] = defaultdict(list)
    for row in await ForecastRepository(session).get_round_forecasts(round_id):
        pair = (row["model_id"], row["series_id"])
        window = windows.get(row["series_id"])
        if pair in wanted and (window is None or window[0] <= row["ts"] <= window[1]):
            forecasts[pair].append(row)

    scales = await repo.get_scales(round_id)
    stored_rows = {
        (row["model_id"], row["series_id"]): row
        for row in await repo.get_score_rows(round_id)
        if (row["model_id"], row["series_id"]) in wanted
    }
    by_vintage: Dict[datetime, List[Dict[str, Any]]] = defaultdict(list)
    for pair in pairs:
        row = stored_rows.get(pair)
        if row is not None and row["evaluation_status"] in _SCORED:
            by_vintage[row["calculated_at"]].append(row)
        else:
            summary.not_reproduced.append((round_id, *pair, None, None))

    updates: List[Dict[str, Any]] = []
    for calculated_at, rows in sorted(by_vintage.items()):
        timestamps = [p["ts"] for r in rows for p in forecasts[(r["model_id"], r["series_id"])]]
        actuals: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        if timestamps:
            series_ids = sorted({row["series_id"] for row in rows})
            for point in await repo.read_actuals_as_of(
                series_ids, bucket, calculated_at, min(timestamps) - bucket, max(timestamps)
            ):
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


async def process_round(round_id: int, pairs: List[Pair], args: argparse.Namespace, summary: Summary) -> None:
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
    summary.pairs_found = sum(len(pairs) for pairs in found.values())
    logger.info("%d (round, model, series) in %d round(s) (dry_run=%s)", summary.pairs_found, len(found), args.dry_run)

    for i, round_id in enumerate(sorted(found), start=1):
        await process_round(round_id, found[round_id], args, summary)
        logger.info("Round %s done (%d/%d), %d row(s) written so far", round_id, i, len(found), summary.written)

    print(summary.report(args.dry_run))
    return summary


if __name__ == "__main__":
    asyncio.run(main())
