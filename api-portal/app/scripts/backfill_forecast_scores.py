"""
Backfill `forecasts.forecast_scores` for every round past its grace period.

Runs exactly the scheduled job's path (`ForecastScoringService.compute_round`): the same
forecast fetch, per-series window, actuals and coverage rules, then MAE, RMSE, MASE, SQL and
the persistence baseline against the (round, series) context scale. MAE is computed from the
forecasts, not backed out of the stored arena score, which is not reliable across data
revisions. Actuals are the ones available when the backfill runs, late ones included.

Scales missing from `forecasts.series_scale` are rebuilt from SCD2 as of
`rounds.created_at` and stored once. `forecasts.scores` is neither read nor written.

Candidates are rounds that ended more than the grace period ago, are not cancelled, have
participants and have no `forecasts.forecast_scores` row yet, so an interrupted run resumes
where it stopped. One transaction per round: a failing round is rolled back, reported and
skipped. Rounds the scheduled job is scoring at the same moment are skipped as well.

Usage (inside the api-portal container, or locally with DATABASE_URL set):

    python -m app.scripts.backfill_forecast_scores [--dry-run] [--limit N] [--round-id X]
        [--sample N] [--refresh] [--check-served] [--no-raw-fallback] [--dump rows.csv]

    --dry-run          Compute and report in read-only transactions. Works before the
                       migration is applied (every scale is then rebuilt, none stored).
    --limit N          Process at most N rounds.
    --round-id X       Process only round X.
    --sample N         Process N candidates spread over the whole history (spot check).
    --refresh          Also recompute rounds that already have forecast_scores rows.
    --check-served     Where a round's served context still exists in context_data,
                       compare the scale used against the one computed from it.
    --no-raw-fallback  Read actuals from the continuous aggregate only. By default a series
                       the aggregate has nothing for is read from raw data, bucketed alike
                       (dev restores lack old aggregate periods).
    --dump PATH        Write every computed row to a CSV for inspection.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import text

# Register every mapper the relationship graph reaches (ChallengeParticipant → ModelInfo
# → User/Organization/ApiKey). The database package __init__s are empty, so a bare script
# context must import these explicitly or mapper configuration fails at first query.
from app.database.models.model_info import ModelInfo  # noqa: F401
from app.database.auth.user import User  # noqa: F401
from app.database.auth.organization import Organization  # noqa: F401
from app.database.auth.api_key import APIKey  # noqa: F401
from app.database.connection import SessionLocal
from app.database.forecasts.forecast_scores_repository import ForecastScoresRepository
from app.services.forecast_scoring_service import (
    EVALUATION_TIMEOUT,
    ForecastScoringService,
    timedelta_to_resolution,
)
from app.services.series_scale_service import SOURCE_SERVED, build_scale_rows, resolution_bucket

logger = logging.getLogger("backfill-forecast-scores")

DEFAULT_PROGRESS_EVERY = 25

# Relative tolerance for "the same number" in the identity and served-scale checks.
TOLERANCE = 1e-9


class _ReadOnlyRepository(ForecastScoresRepository):
    """Reads like the real repository and writes nothing, with or without the tables."""

    def __init__(self, session, tables_exist: bool):
        super().__init__(session)
        self._tables = tables_exist

    async def get_scales(self, round_id: int) -> Dict[int, Dict[str, Any]]:
        return await super().get_scales(round_id) if self._tables else {}

    async def insert_scales(self, rows: List[Dict[str, Any]]) -> None:
        return None

    async def upsert_forecast_scores(self, rows: List[Dict[str, Any]]) -> int:
        return len(rows)


@dataclass
class Summary:
    rounds_processed: int = 0
    rounds_skipped: int = 0
    rounds_locked: int = 0
    rounds_failed: int = 0
    rows: int = 0
    status: Counter = field(default_factory=Counter)
    scale_series: int = 0
    scale_undefined: int = 0
    identity_checked: int = 0
    identity_mismatches: List[tuple] = field(default_factory=list)
    served_compared: int = 0
    served_exact: int = 0
    served_over_1pct: List[tuple] = field(default_factory=list)
    failed: List[tuple] = field(default_factory=list)
    round_seconds: List[float] = field(default_factory=list)
    dumped: List[Dict[str, Any]] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def record(self, rows: List[Dict[str, Any]]) -> None:
        self.rows += len(rows)
        scales = {}
        for row in rows:
            self.status[row["evaluation_status"]] += 1
            scales[row["series_id"]] = row["scale"]
            mase, sql = row["mase"], row["sql_score"]
            if row["has_quantiles"] is False and mase is not None:
                self.identity_checked += 1
                if sql is None or abs(sql - mase) > TOLERANCE * max(1.0, abs(mase)):
                    self.identity_mismatches.append((row["round_id"], row["model_id"], row["series_id"], mase, sql))
        self.scale_series += len(scales)
        self.scale_undefined += sum(1 for s in scales.values() if s is None or not s > 0)

    def report(self, dry_run: bool) -> str:
        secs = sorted(self.round_seconds)
        pct = (lambda q: secs[min(len(secs) - 1, int(q * len(secs)))]) if secs else (lambda q: 0.0)
        lines = [
            "",
            "=" * 72,
            f"Forecast scores backfill{' (DRY RUN, read-only)' if dry_run else ''}",
            "=" * 72,
            f"Rounds processed:        {self.rounds_processed}",
            f"Rounds skipped (empty):  {self.rounds_skipped}",
            f"Rounds skipped (locked): {self.rounds_locked}",
            f"Rounds failed:           {self.rounds_failed}",
            f"Rows:                    {self.rows}",
        ]
        lines += [f"  {status:<22} {n}" for status, n in self.status.most_common()]
        lines += [
            f"(round, series) scales:  {self.scale_series}, undefined {self.scale_undefined}",
            f"Point-only sql == mase:  {self.identity_checked - len(self.identity_mismatches)}"
            f"/{self.identity_checked}",
        ]
        for m in self.identity_mismatches[:5]:
            lines.append(f"    mismatch (round, model, series, mase, sql): {m}")
        if self.served_compared:
            lines.append(
                f"Scale vs served context: {self.served_exact}/{self.served_compared} exact, "
                f"{len(self.served_over_1pct)} off by more than 1 %"
            )
            for m in self.served_over_1pct[:5]:
                lines.append(f"    (round, series, used, served): {m}")
        lines.append(
            f"Seconds per round:       median {pct(0.5):.2f}, p90 {pct(0.9):.2f}, max {pct(1.0):.2f}"
        )
        for rid, err in self.failed[:20]:
            lines.append(f"    failed round {rid}: {err}")
        lines.append(f"Elapsed: {time.monotonic() - self.started_at:.1f}s")
        lines.append("=" * 72)
        return "\n".join(lines)


async def _tables_exist() -> bool:
    async with SessionLocal() as session:
        return await ForecastScoresRepository(session).tables_exist()


async def candidate_rounds(
    tables_exist: bool,
    refresh: bool,
    round_id: Optional[int],
    limit: Optional[int],
    sample: Optional[int] = None,
) -> List[int]:
    """Rounds past the grace period, optionally only those not backfilled yet."""
    clauses = [
        "r.end_time < now() - CAST(:grace AS interval)",
        "NOT COALESCE(r.is_cancelled, FALSE)",
        "EXISTS (SELECT 1 FROM challenges.participants p WHERE p.round_id = r.id)",
    ]
    params: Dict[str, Any] = {"grace": EVALUATION_TIMEOUT}
    if tables_exist and not refresh:
        clauses.append("NOT EXISTS (SELECT 1 FROM forecasts.forecast_scores s WHERE s.round_id = r.id)")
    if round_id is not None:
        clauses.append("r.id = :round_id")
        params["round_id"] = round_id
    order = "md5(r.id::text)" if sample is not None else "r.id"
    sql = f"SELECT r.id FROM challenges.rounds r WHERE {' AND '.join(clauses)} ORDER BY {order}"
    if sample is not None:
        limit = sample if limit is None else min(limit, sample)
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = limit
    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await session.execute(text(sql), params)
        return [row[0] for row in result.fetchall()]


async def _check_served(svc: ForecastScoringService, round_id: int, rows, summary: Summary) -> None:
    served = await svc.repo.read_served_context(round_id)
    if not served:
        return
    info = await svc.round_repo.get_by_id(round_id)
    served_rows = build_scale_rows(
        round_id, served, await svc.repo.get_context_windows(round_id),
        resolution_bucket(timedelta_to_resolution(info.frequency)), SOURCE_SERVED,
    )
    used = {row["series_id"]: row["scale"] for row in rows}
    for row in served_rows:
        sid, served_scale = row["series_id"], row["scale"]
        if sid not in used or used[sid] is None or served_scale is None:
            continue
        summary.served_compared += 1
        diff = abs(used[sid] - served_scale) / max(abs(served_scale), 1e-300)
        if diff <= TOLERANCE:
            summary.served_exact += 1
        elif diff > 0.01:
            summary.served_over_1pct.append((round_id, sid, used[sid], served_scale))


async def process_round(round_id: int, args: argparse.Namespace, tables_exist: bool, summary: Summary) -> None:
    started = time.monotonic()
    async with SessionLocal() as session:
        try:
            svc = ForecastScoringService(session, raw_actuals_fallback=not args.no_raw_fallback)
            if args.dry_run:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                svc.scale_service.repo = _ReadOnlyRepository(session, tables_exist)
            elif not await svc.repo.try_lock_round(round_id):
                summary.rounds_locked += 1
                await session.rollback()
                return

            rows = await svc.compute_round(round_id)
            if not rows:
                summary.rounds_skipped += 1
                await session.rollback()
                return

            if args.check_served:
                await _check_served(svc, round_id, rows, summary)
            await svc.repo.upsert_forecast_scores(rows)
            if args.dry_run:
                await session.rollback()
            else:
                await session.commit()

            summary.rounds_processed += 1
            summary.record(rows)
            if args.dump:
                summary.dumped.extend(rows)
        except Exception as e:
            await session.rollback()
            summary.rounds_failed += 1
            summary.failed.append((round_id, str(e)[:300]))
            logger.exception("Round %s failed", round_id)
        finally:
            summary.round_seconds.append(time.monotonic() - started)


def _write_dump(path: str, rows: List[Dict[str, Any]]) -> None:
    import pandas as pd

    frame = pd.DataFrame(rows)
    if "sql_per_quantile" in frame:
        frame["sql_per_quantile"] = frame["sql_per_quantile"].map(
            lambda v: json.dumps(v) if v is not None else None
        )
    frame.to_csv(path, index=False)
    logger.info("Wrote %d rows to %s", len(frame), path)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill forecast_scores for rounds past the grace period.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report in read-only transactions.")
    parser.add_argument("--limit", type=int, default=None, help="Max number of rounds to process.")
    parser.add_argument("--round-id", type=int, default=None, help="Process only this round.")
    parser.add_argument("--sample", type=int, default=None, help="Process N candidates spread over history.")
    parser.add_argument("--refresh", action="store_true", help="Also recompute rounds already backfilled.")
    parser.add_argument("--check-served", action="store_true", help="Compare scales with surviving served context.")
    parser.add_argument("--no-raw-fallback", action="store_true", help="Actuals from the aggregate only.")
    parser.add_argument("--dump", default=None, help="Write computed rows to this CSV.")
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    return parser.parse_args(argv)


async def main(argv: Optional[List[str]] = None) -> Summary:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = _parse_args(argv)
    summary = Summary()

    tables_exist = await _tables_exist()
    if not tables_exist and not args.dry_run:
        raise SystemExit(
            "forecasts.series_scale / forecasts.forecast_scores do not exist. Apply "
            "app/scripts/migrations/2026_forecast_scores.sql first, or pass --dry-run."
        )

    round_ids = await candidate_rounds(tables_exist, args.refresh, args.round_id, args.limit, args.sample)
    logger.info("%d round(s) to process (dry_run=%s, tables=%s)", len(round_ids), args.dry_run, tables_exist)

    for i, rid in enumerate(round_ids, start=1):
        await process_round(rid, args, tables_exist, summary)
        if i % args.progress_every == 0 or i == len(round_ids):
            logger.info(
                "Progress: %d/%d rounds, %d rows, %d failed, %.0fs",
                i, len(round_ids), summary.rows, summary.rounds_failed, time.monotonic() - summary.started_at,
            )

    if args.dump:
        _write_dump(args.dump, summary.dumped)
    print(summary.report(args.dry_run))
    return summary


if __name__ == "__main__":
    asyncio.run(main())
