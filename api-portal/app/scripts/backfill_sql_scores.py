"""
One-off backfill: recompute Scaled Quantile Loss (SQL) for historical score rows.

The SQL metric (`forecasts.scores.sql_score` + siblings) was added alongside the
existing MASE/RMSE evaluation. Score rows written before that rollout have
`sql_score IS NULL` even though they were fully evaluated (`mase IS NOT NULL`). This
script recomputes the 5 SQL columns for those rows using the EXACT SAME code path as
the live scorer (`ScoreEvaluationService._calculate_score_for_model_series`):

    app.services.forecast_metrics.compute_sql_fields(y_true, y_pred, probabilistic_values, mae_naive)

and writes back ONLY:
    sql_score, sql_per_quantile, has_quantiles, quantile_levels_count, quantile_crossing_count

It NEVER touches mase, rmse, evaluation_status, or any other column.

Candidate selection mirrors the live scorer's notion of "actually evaluated":
`sql_score IS NULL AND mase IS NOT NULL` (rows the live path stores as NULL-with-status,
e.g. no_overlap/insufficient_data/error/pending, are left untouched). Processing is
round-by-round, one DB transaction per round: a failure in one round rolls back that
round only, logs it, and moves on. The script is resumable by construction — candidates
are simply the rows still missing `sql_score`.

Usage (run inside the api-portal container, or locally with DATABASE_URL set):

    python -m app.scripts.backfill_sql_scores [--dry-run] [--limit N] [--round-id X]

    --dry-run     Compute and report, but never write to the database.
    --limit N     Process at most N rounds.
    --round-id X  Process only round X (spot-check / resume a single round).

Drift detector: for point-only rows (has_quantiles=False after recompute), the computed
sql_score must equal the stored mase (shared-denominator identity — see
forecast_metrics.py docstring). Mismatches beyond 1e-9 are reported (not skipped —
still written) since they indicate the underlying evaluation data changed since the
row was originally scored.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from app.database.challenges.challenge_repository import ChallengeRoundRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.connection import SessionLocal
from app.database.forecasts.repository import ForecastRepository
from app.services.forecast_metrics import compute_sql_fields
from app.services.score_evaluation_service import timedelta_to_resolution

logger = logging.getLogger("backfill-sql-scores")

# Rows where |computed_sql - stored_mase| exceeds this are reported as identity mismatches.
DRIFT_TOLERANCE = 1e-9

DEFAULT_PROGRESS_EVERY = 5


# --------------------------------------------------------------------------------------
# Pure compute (no DB) — unit-tested directly, and byte-identical to the live scorer.
# --------------------------------------------------------------------------------------

def recompute_sql_for_model_series(
    evaluation_data: List[Dict[str, Any]],
    naive_value: float,
) -> Dict[str, Any]:
    """Recompute the 5 SQL columns for one (round, model, series) from aligned rows.

    Mirrors exactly what `ScoreEvaluationService._calculate_score_for_model_series` does
    for the SQL block: build aligned y_true/y_pred, derive `mae_naive` (the shared
    MASE/SQL scale), then delegate to the shared `compute_sql_fields`.

    Args:
        evaluation_data: aligned rows, each with 'predicted_value', 'actual_value', and
            optionally 'probabilistic_values' — the same shape returned by
            `ForecastRepository.get_evaluation_data_by_resolution`.
        naive_value: the flat last-context-value naive baseline for this series
            (`ChallengeSeriesPseudo.max_ts` context point), same as the live path.

    Returns:
        Dict with keys 'sql_score', 'sql_per_quantile', 'has_quantiles',
        'quantile_levels_count', 'quantile_crossing_count', plus 'mae_naive' (exposed for
        the drift detector / callers that want it, not a stored column).
    """
    y_pred = np.array([item["predicted_value"] for item in evaluation_data], dtype=float)
    y_true = np.array([item["actual_value"] for item in evaluation_data], dtype=float)
    mae_naive = float(np.mean(np.abs(y_true - naive_value)))

    probabilistic_values = [item.get("probabilistic_values") for item in evaluation_data]
    sql_fields = compute_sql_fields(y_true, y_pred, probabilistic_values, mae_naive)
    return {**sql_fields, "mae_naive": mae_naive}


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------

@dataclass
class BackfillSummary:
    rounds_processed: int = 0
    rounds_skipped: int = 0
    rounds_failed: int = 0
    rows_updated: int = 0
    rows_real_quantiles: int = 0
    rows_degenerate: int = 0
    rows_uncomputable: int = 0  # candidate rows we couldn't recompute (missing naive/eval data)
    crossing_by_model: Dict[int, int] = field(default_factory=dict)
    mismatches: List[Tuple[int, int, int, float, float]] = field(default_factory=list)
    failed_rounds: List[Tuple[int, str]] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def record_row(
        self,
        round_id: int,
        model_id: int,
        series_id: int,
        fields: Dict[str, Any],
        stored_mase: Optional[float],
    ) -> None:
        self.rows_updated += 1
        if fields["has_quantiles"]:
            self.rows_real_quantiles += 1
        else:
            self.rows_degenerate += 1
            # Identity check: degenerate distribution == arena MASE of the point forecast.
            if fields["sql_score"] is not None and stored_mase is not None:
                if abs(fields["sql_score"] - stored_mase) > DRIFT_TOLERANCE:
                    self.mismatches.append(
                        (round_id, model_id, series_id, fields["sql_score"], stored_mase)
                    )

        crossing = fields["quantile_crossing_count"] or 0
        if crossing:
            self.crossing_by_model[model_id] = self.crossing_by_model.get(model_id, 0) + crossing

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.started_at

    def report(self, dry_run: bool) -> str:
        elapsed = self.elapsed_seconds()
        top_crossing = sorted(self.crossing_by_model.items(), key=lambda kv: kv[1], reverse=True)[:10]
        lines = [
            "",
            "=" * 72,
            f"SQL backfill summary{' (DRY RUN — no writes)' if dry_run else ''}",
            "=" * 72,
            f"Rounds processed:     {self.rounds_processed}",
            f"Rounds skipped:       {self.rounds_skipped}",
            f"Rounds failed:        {self.rounds_failed}",
            f"Rows updated:         {self.rows_updated}",
            f"  - real quantiles:   {self.rows_real_quantiles}",
            f"  - degenerate:       {self.rows_degenerate}",
            f"Rows uncomputable:    {self.rows_uncomputable}",
            f"Identity mismatches:  {len(self.mismatches)}",
        ]
        if self.mismatches:
            lines.append("  Examples (round, model, series, computed_sql, stored_mase):")
            for m in self.mismatches[:5]:
                lines.append(f"    {m}")
        if top_crossing:
            lines.append("Top 10 models by total quantile_crossing_count:")
            for model_id, count in top_crossing:
                lines.append(f"    model_id={model_id}: {count}")
        if self.failed_rounds:
            lines.append("Failed rounds:")
            for rid, err in self.failed_rounds:
                lines.append(f"    round_id={rid}: {err}")
        lines.append(f"Elapsed: {elapsed:.1f}s")
        lines.append("=" * 72)
        return "\n".join(lines)


# --------------------------------------------------------------------------------------
# DB-backed per-round processing
# --------------------------------------------------------------------------------------

async def _get_naive_value(
    round_repo: ChallengeRoundRepository,
    time_series_repo: TimeSeriesRepository,
    round_id: int,
    series_id: int,
    resolution: str,
) -> Optional[float]:
    """Same lookup the live scorer uses: the context point at ChallengeSeriesPseudo.max_ts."""
    pseudo_info = await round_repo.get_series_pseudo(round_id, series_id)
    if not pseudo_info or not pseudo_info.max_ts:
        return None
    context_points = await time_series_repo.get_data_by_time_range_by_resolution(
        series_id=series_id,
        start_time=pseudo_info.max_ts,
        end_time=pseudo_info.max_ts,
        resolution=resolution,
    )
    if not context_points:
        return None
    return context_points[0]["value"]


async def process_round(
    round_id: int,
    summary: BackfillSummary,
    dry_run: bool,
) -> None:
    """Process a single round in its own session/transaction.

    On any exception the whole round's writes are rolled back, the failure is logged and
    recorded in the summary, and the caller moves on to the next round.
    """
    async with SessionLocal() as session:
        try:
            forecast_repo = ForecastRepository(session)
            round_repo = ChallengeRoundRepository(session)
            time_series_repo = TimeSeriesRepository(session)

            round_info = await round_repo.get_by_id(round_id)
            if not round_info:
                logger.warning("Round %s not found — skipping", round_id)
                summary.rounds_skipped += 1
                return

            resolution = timedelta_to_resolution(round_info.frequency)
            candidates = await forecast_repo.get_sql_backfill_candidates(round_id)
            if not candidates:
                logger.info("Round %s: no candidate rows — skipping", round_id)
                summary.rounds_skipped += 1
                return

            logger.info("Round %s: %d candidate row(s), resolution=%s", round_id, len(candidates), resolution)

            # Cache naive values per series within the round (shared across models).
            naive_cache: Dict[int, Optional[float]] = {}
            updates: List[Dict[str, Any]] = []

            for cand in candidates:
                model_id = cand["model_id"]
                series_id = cand["series_id"]

                if series_id not in naive_cache:
                    naive_cache[series_id] = await _get_naive_value(
                        round_repo, time_series_repo, round_id, series_id, resolution
                    )
                naive_value = naive_cache[series_id]

                if naive_value is None:
                    logger.warning(
                        "Round %s, model %s, series %s: no naive/context value — skipping row",
                        round_id, model_id, series_id
                    )
                    summary.rows_uncomputable += 1
                    continue

                evaluation_data = await forecast_repo.get_evaluation_data_by_resolution(
                    round_id=round_id, model_id=model_id, series_id=series_id, resolution=resolution
                )
                if not evaluation_data:
                    logger.warning(
                        "Round %s, model %s, series %s: no evaluation data — skipping row",
                        round_id, model_id, series_id
                    )
                    summary.rows_uncomputable += 1
                    continue

                fields = recompute_sql_for_model_series(evaluation_data, naive_value)
                summary.record_row(round_id, model_id, series_id, fields, cand["mase"])
                updates.append({
                    "id": cand["id"],
                    "sql_score": fields["sql_score"],
                    "sql_per_quantile": fields["sql_per_quantile"],
                    "has_quantiles": fields["has_quantiles"],
                    "quantile_levels_count": fields["quantile_levels_count"],
                    "quantile_crossing_count": fields["quantile_crossing_count"],
                })

            if dry_run:
                logger.info("Round %s: dry-run, would update %d row(s)", round_id, len(updates))
                await session.rollback()
            else:
                if updates:
                    rows_affected = await forecast_repo.update_sql_fields_bulk(updates)
                    await session.commit()
                    logger.info("Round %s: updated %d row(s)", round_id, rows_affected)
                else:
                    await session.rollback()

            summary.rounds_processed += 1

        except Exception as exc:  # noqa: BLE001 - one round's failure must not stop the run
            await session.rollback()
            logger.exception("Round %s failed — rolled back, continuing", round_id)
            summary.rounds_failed += 1
            summary.failed_rounds.append((round_id, str(exc)[:300]))


async def run_backfill(
    dry_run: bool,
    limit: Optional[int],
    round_id: Optional[int],
    progress_every: int = DEFAULT_PROGRESS_EVERY,
) -> BackfillSummary:
    summary = BackfillSummary()

    async with SessionLocal() as session:
        forecast_repo = ForecastRepository(session)
        round_ids = await forecast_repo.get_round_ids_missing_sql_scores(round_id=round_id, limit=limit)

    if not round_ids:
        logger.info("No rounds need SQL backfill.")
        return summary

    logger.info("Found %d round(s) needing SQL backfill", len(round_ids))

    for i, rid in enumerate(round_ids, start=1):
        await process_round(rid, summary, dry_run)
        if i % progress_every == 0 or i == len(round_ids):
            logger.info(
                "Progress: %d/%d rounds (updated=%d rows, failed=%d rounds, elapsed=%.1fs)",
                i, len(round_ids), summary.rows_updated, summary.rounds_failed, summary.elapsed_seconds()
            )

    return summary


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill SQL (Scaled Quantile Loss) scores for historical rounds."
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and report, do not write.")
    parser.add_argument("--limit", type=int, default=None, help="Max number of rounds to process.")
    parser.add_argument("--round-id", type=int, default=None, help="Process only this round (spot check).")
    parser.add_argument(
        "--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY,
        help="Log progress every N rounds."
    )
    return parser.parse_args(argv)


async def main(argv: Optional[List[str]] = None) -> BackfillSummary:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    logger.info(
        "Starting SQL backfill (dry_run=%s, limit=%s, round_id=%s)",
        args.dry_run, args.limit, args.round_id
    )
    summary = await run_backfill(
        dry_run=args.dry_run,
        limit=args.limit,
        round_id=args.round_id,
        progress_every=args.progress_every,
    )
    print(summary.report(args.dry_run))
    return summary


if __name__ == "__main__":
    asyncio.run(main())
