"""Move `forecasts.series_scale` and `forecasts.forecast_scores` to the seasonal MASE scale, one transaction per round.

    python -m app.scripts.rescale_forecast_scores [flags]

    --dry-run                 compute in read-only transactions, write nothing
    --round-id X              only round X
    --limit N                 at most N rounds
    --workers N               rounds in flight at once (default 1)
    --progress-every N        log progress every N rounds

Rebuilds each scale from the context (no forecasts are read), then per row `mase = mae / scale`
and SQL times `scale_old / scale_new`. Rounds whose scales and rows are already on the
current lag and method are skipped, so an interrupted run can be restarted.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
from app.services.forecast_metrics import MASE_METHOD, mase_scale_defined, seasonal_lag
from app.services.forecast_scoring_service import (
    NO_CONTEXT_MESSAGE,
    UNDEFINED_SCALE_MESSAGE,
    timedelta_to_resolution,
)
from app.services.series_scale_service import SeriesScaleService, resolution_bucket

logger = logging.getLogger("rescale-forecast-scores")

DEFAULT_PROGRESS_EVERY = 100

TOLERANCE = 1e-9

RECENT = timedelta(days=30)

_SCORED = ("complete", "partial", "undefined_scale")

_KEYS = ("round_id", "model_id", "series_id")


def expected_lag(frequency: Optional[timedelta]) -> int:
    return seasonal_lag(resolution_bucket(timedelta_to_resolution(frequency)))


def is_rescaled(frequency: Optional[timedelta], lags: List[int], methods: List[str]) -> bool:
    return set(lags) <= {expected_lag(frequency)} and set(methods) <= {MASE_METHOD}


def rescale_score_row(row: Dict[str, Any], scale_row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The update of one `forecast_scores` row for its new scale row; None when its SQL needs the forecasts."""
    new_scale = scale_row["scale"] if scale_row else None
    update = {
        **{k: row[k] for k in _KEYS},
        "scale": new_scale,
        "mase": row["mase"],
        "sql_score": row["sql_score"],
        "sql_per_quantile": row["sql_per_quantile"],
        "evaluation_status": row["evaluation_status"],
        "error_message": row["error_message"],
        "method": MASE_METHOD,
    }
    if row["evaluation_status"] not in _SCORED:
        return update
    if not mase_scale_defined(new_scale):
        update.update(
            mase=None,
            sql_score=None,
            sql_per_quantile=None,
            evaluation_status="undefined_scale",
            error_message=NO_CONTEXT_MESSAGE if scale_row is None else UNDEFINED_SCALE_MESSAGE,
        )
        return update
    if row["evaluation_status"] == "undefined_scale" or not mase_scale_defined(row["scale"]) or row["mae"] is None:
        return None

    ratio = row["scale"] / new_scale
    sql_per_quantile = row["sql_per_quantile"]
    update.update(
        mase=row["mae"] / new_scale,
        sql_score=row["sql_score"] * ratio if row["sql_score"] is not None else None,
        sql_per_quantile=(
            {k: (v * ratio if v is not None else None) for k, v in sql_per_quantile.items()}
            if sql_per_quantile else sql_per_quantile
        ),
    )
    return update


@dataclass
class Summary:
    rounds_processed: int = 0
    rounds_skipped_done: int = 0
    rounds_skipped_rescore: int = 0
    rounds_failed: int = 0
    rows: int = 0
    transitions: Counter = field(default_factory=Counter)
    needs_rescore: List[Tuple[int, int, int]] = field(default_factory=list)
    scale_sources: Counter = field(default_factory=Counter)
    scales_new: int = 0
    scales_deleted: int = 0
    scales_undefined: Counter = field(default_factory=Counter)
    scales_total: Counter = field(default_factory=Counter)
    scale_ratio: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    mase_old: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    mase_new: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    mase_new_recent: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    identity_checked: int = 0
    identity_mismatches: List[tuple] = field(default_factory=list)
    mase_mismatches: List[tuple] = field(default_factory=list)
    failed: List[tuple] = field(default_factory=list)
    round_seconds: List[float] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def record_scales(self, resolution: str, old: Dict[int, Dict[str, Any]], new: Dict[int, Dict[str, Any]]) -> None:
        for sid, row in new.items():
            self.scale_sources[row["source"]] += 1
            self.scales_total[resolution] += 1
            if not mase_scale_defined(row["scale"]):
                self.scales_undefined[resolution] += 1
            if sid not in old:
                self.scales_new += 1
            elif mase_scale_defined(old[sid]["scale"]) and mase_scale_defined(row["scale"]):
                self.scale_ratio[resolution].append(row["scale"] / old[sid]["scale"])
        self.scales_deleted += sum(1 for sid in old if sid not in new)

    def record_rows(
        self, resolution: str, recent: bool, rows: List[Dict[str, Any]], updates: List[Optional[Dict[str, Any]]]
    ) -> None:
        for row, update in zip(rows, updates):
            if update is None:
                continue
            self.rows += 1
            self.transitions[(row["evaluation_status"], update["evaluation_status"])] += 1
            if not row["final_evaluation"] or update["evaluation_status"] not in ("complete", "partial"):
                continue
            if row["mase"] is not None:
                self.mase_old[resolution].append(row["mase"])
            mase = update["mase"]
            self.mase_new[resolution].append(mase)
            if recent:
                self.mase_new_recent[resolution].append(mase)
            if abs(mase - row["mae"] / update["scale"]) > TOLERANCE * max(1.0, mase):
                self.mase_mismatches.append((row["round_id"], row["model_id"], row["series_id"]))
            if row["has_quantiles"] is False:
                self.identity_checked += 1
                sql = update["sql_score"]
                if sql is None or abs(sql - mase) > TOLERANCE * max(1.0, mase):
                    self.identity_mismatches.append((row["round_id"], row["model_id"], row["series_id"], mase, sql))

    def report(self, dry_run: bool) -> str:
        secs = sorted(self.round_seconds)
        pct = (lambda q: secs[min(len(secs) - 1, int(q * len(secs)))]) if secs else (lambda q: 0.0)
        med = lambda values: f"{np.median(values):.3f}" if values else "-"  # noqa: E731
        below_one = lambda values: f"{np.mean(np.asarray(values) < 1):.0%}" if values else "-"  # noqa: E731
        lines = [
            "",
            "=" * 72,
            f"Seasonal MASE rescale ({MASE_METHOD}){' (DRY RUN, read-only)' if dry_run else ''}",
            "=" * 72,
            f"Rounds processed:            {self.rounds_processed}",
            f"Rounds skipped (done):       {self.rounds_skipped_done}",
            f"Rounds skipped (rescore):    {self.rounds_skipped_rescore}",
            f"Rounds failed:               {self.rounds_failed}",
            f"Rows updated:                {self.rows}",
        ]
        lines += [f"  {old:>16} -> {new:<16} {n}" for (old, new), n in self.transitions.most_common()]
        lines.append(f"Rows needing the forecasts:  {len(self.needs_rescore)}")
        lines += [f"    (round, model, series): {r}" for r in self.needs_rescore[:10]]
        lines.append(
            f"Scales: {sum(self.scale_sources.values())} rebuilt "
            f"({', '.join(f'{k} {v}' for k, v in self.scale_sources.most_common())}), "
            f"{self.scales_new} new, {self.scales_deleted} deleted"
        )
        for res in sorted(self.scales_total):
            lines.append(
                f"  {res:>5}: undefined {self.scales_undefined[res]}/{self.scales_total[res]}, "
                f"median new/old scale {med(self.scale_ratio[res])}"
            )
        lines.append("Final scored MASE, median (share below 1):")
        for res in sorted(set(self.mase_old) | set(self.mase_new)):
            lines.append(
                f"  {res:>5}: old {med(self.mase_old[res])} ({below_one(self.mase_old[res])}), "
                f"new {med(self.mase_new[res])} ({below_one(self.mase_new[res])}), "
                f"new in the last {RECENT.days} days {med(self.mase_new_recent[res])} "
                f"({below_one(self.mase_new_recent[res])}, n={len(self.mase_new_recent[res])})"
            )
        lines += [
            f"mase == mae / scale:         {len(self.mase_mismatches)} mismatches",
            f"Point-only sql == mase:      {self.identity_checked - len(self.identity_mismatches)}"
            f"/{self.identity_checked}",
        ]
        lines += [f"    mismatch (round, model, series, mase, sql): {m}" for m in self.identity_mismatches[:5]]
        lines.append(
            f"Seconds per round:           median {pct(0.5):.2f}, p90 {pct(0.9):.2f}, max {pct(1.0):.2f}"
        )
        lines += [f"    failed round {rid}: {err}" for rid, err in self.failed[:20]]
        lines.append(f"Elapsed: {time.monotonic() - self.started_at:.1f}s")
        lines.append("=" * 72)
        return "\n".join(lines)


async def candidate_rounds(round_id: Optional[int]) -> List[Tuple[int, Optional[timedelta], List[int], List[str]]]:
    """`(round_id, frequency, distinct lags, distinct methods)` of every round with a scale or a score row."""
    clauses = [
        "(EXISTS (SELECT 1 FROM forecasts.series_scale s WHERE s.round_id = r.id)"
        " OR EXISTS (SELECT 1 FROM forecasts.forecast_scores f WHERE f.round_id = r.id))"
    ]
    params: Dict[str, Any] = {}
    if round_id is not None:
        clauses.append("r.id = :round_id")
        params["round_id"] = round_id
    sql = f"""
        SELECT r.id, r.frequency,
               ARRAY(SELECT DISTINCT s.m FROM forecasts.series_scale s WHERE s.round_id = r.id),
               ARRAY(SELECT DISTINCT f.method FROM forecasts.forecast_scores f WHERE f.round_id = r.id)
        FROM challenges.rounds r
        WHERE {' AND '.join(clauses)}
        ORDER BY r.id
    """
    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await session.execute(text(sql), params)
        return [(r[0], r[1], list(r[2]), list(r[3])) for r in result.fetchall()]


async def process_round(round_id: int, args: argparse.Namespace, summary: Summary) -> None:
    started = time.monotonic()
    async with SessionLocal() as session:
        try:
            if args.dry_run:
                await session.execute(text("SET TRANSACTION READ ONLY"))
            # Parallel query workers need shared memory; these queries are small.
            await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
            scale_service = SeriesScaleService(session)
            repo = scale_service.repo
            if not args.dry_run:
                await repo.lock_round(round_id)

            info = await ChallengeRoundRepository(session).get_by_id(round_id)
            resolution = timedelta_to_resolution(info.frequency)
            created_at, end_time = info.created_at, info.end_time
            old_scales = await repo.get_scales(round_id)
            new_rows = await scale_service.rebuild_scales(round_id, resolution, created_at)
            new_scales = {row["series_id"]: row for row in new_rows}
            score_rows = await repo.get_score_rows(round_id)
            updates = [rescale_score_row(row, new_scales.get(row["series_id"])) for row in score_rows]

            missing = [tuple(row[k] for k in _KEYS) for row, u in zip(score_rows, updates) if u is None]
            if missing:
                summary.needs_rescore.extend(missing)
                summary.rounds_skipped_rescore += 1
                await session.rollback()
                return

            if not args.dry_run:
                await repo.replace_scales(new_rows)
                await repo.delete_scales(round_id, [sid for sid in old_scales if sid not in new_scales])
                await repo.update_forecast_scores(updates)
                await session.commit()
            else:
                await session.rollback()

            recent = end_time is not None and (
                end_time if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc)
            ) > datetime.now(timezone.utc) - RECENT
            summary.rounds_processed += 1
            summary.record_scales(resolution, old_scales, new_scales)
            summary.record_rows(resolution, recent, score_rows, updates)
        except Exception as e:
            await session.rollback()
            summary.rounds_failed += 1
            summary.failed.append((round_id, str(e)[:300]))
            logger.exception("Round %s failed", round_id)
        finally:
            summary.round_seconds.append(time.monotonic() - started)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Move forecast scores to the seasonal MASE scale.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report in read-only transactions.")
    parser.add_argument("--round-id", type=int, default=None, help="Process only this round.")
    parser.add_argument("--limit", type=int, default=None, help="Max number of rounds to process.")
    parser.add_argument("--workers", type=int, default=1, help="Rounds in flight at once.")
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    return args


async def main(argv: Optional[List[str]] = None) -> Summary:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = _parse_args(argv)
    summary = Summary()

    candidates = await candidate_rounds(args.round_id)
    todo = [rid for rid, freq, lags, methods in candidates if not is_rescaled(freq, lags, methods)]
    summary.rounds_skipped_done = len(candidates) - len(todo)
    if args.limit is not None:
        todo = todo[:args.limit]
    logger.info("%d round(s) to rescale, %d already done (dry_run=%s)", len(todo), summary.rounds_skipped_done, args.dry_run)

    queue: asyncio.Queue = asyncio.Queue()
    for rid in todo:
        queue.put_nowait(rid)
    done = 0

    async def worker() -> None:
        nonlocal done
        while not queue.empty():
            await process_round(queue.get_nowait(), args, summary)
            done += 1
            if done % args.progress_every == 0 or done == len(todo):
                logger.info(
                    "Progress: %d/%d rounds, %d rows, %d failed, %.0fs",
                    done, len(todo), summary.rows, summary.rounds_failed, time.monotonic() - summary.started_at,
                )

    await asyncio.gather(*(worker() for _ in range(args.workers)))

    print(summary.report(args.dry_run))
    return summary


if __name__ == "__main__":
    asyncio.run(main())
