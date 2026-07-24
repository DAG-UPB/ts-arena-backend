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

Data-source fallback: the continuous aggregates (`data_portal.time_series_15min/_1h/_1d`)
can have gaps relative to the raw `time_series_data` table (observed on dev, where a DB
restore only carries recent weeks of aggregate data). For each candidate row the script
tries the aggregate first, then falls back to a raw-bucketed reproduction of the same
`time_bucket(...)` semantics computed directly from `time_series_data`. A source is only
accepted if it reproduces the EXACT timestamp set the original evaluation used — enforced
by comparing the row count against the stored `evaluated_count` column, never by
timestamp count alone. See `decide_sql_source` for the exact decision table.

Batched, ts-bounded fetching: `forecasts.forecasts` is a 196M-row TimescaleDB hypertable —
a query without a `ts` predicate touches every chunk (measured ~1.5s per call; at ~2.2M
candidate rows, per-row querying is unusable). The script fetches a round's forecasts in
ONE ts-bounded query (`get_round_forecasts`, ts bounds = [start_time, end_time] padded by
7 days each way) and each series' actuals in ONE query per source, then joins forecasts
against actuals in Python (`align_evaluation_data`) instead of one SQL join per
(model, series). Rounds whose forecast fetch returns nothing (fully pruned, e.g. old
sub-Docker-volume data) are skipped in bulk without further per-candidate queries. The
decision/guard/compute semantics (`decide_sql_source`, `_source_sufficient`,
`recompute_sql_for_model_series`) are unchanged — only how their inputs are fetched.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Register every mapper the relationship graph reaches (ChallengeParticipant → ModelInfo
# → User/Organization/ApiKey). The database package __init__s are empty, so a bare script
# context must import these explicitly or mapper configuration fails at first query.
from app.database.models.model_info import ModelInfo  # noqa: F401
from app.database.auth.user import User  # noqa: F401
from app.database.auth.organization import Organization  # noqa: F401
from app.database.auth.api_key import APIKey  # noqa: F401
from app.database.challenges.challenge_repository import ChallengeRoundRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.connection import SessionLocal
from app.database.forecasts.repository import ForecastRepository
from app.services.evaluation_alignment import align_evaluation_data, group_actuals_by_minute
from app.services.forecast_metrics import compute_sql_fields
from app.services.score_evaluation_service import timedelta_to_resolution

logger = logging.getLogger("backfill-sql-scores")

# Rows where |computed_sql - stored_mase| exceeds this are reported as identity mismatches.
DRIFT_TOLERANCE = 1e-9

DEFAULT_PROGRESS_EVERY = 5

# Generous margin around a round's [start_time, end_time] used to ts-bound the batched
# forecast/actuals queries. Forecasts can extend past end_time (e.g. round 522's run
# spans 08:00 -> 08:00 the next day), so this errs wide; if a round's forecasts still
# fall outside it, the count-vs-evaluated_count guard catches it (treated as a coverage
# mismatch, not silently wrong data).
ROUND_TS_MARGIN = timedelta(days=7)


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


def _source_sufficient(evaluated_count: Optional[int], naive_value: Optional[float], row_count: int) -> bool:
    """Is one data source (aggregate or raw) faithful enough to compute from?

    Faithful means: the naive/context value resolved, AND the row count reproduces
    exactly what the original evaluation used. When the stored row has no
    `evaluated_count` to check against (pre-column data), "non-empty" is the best
    available substitute — see `decide_sql_source`'s `no_expected_count` flag.
    """
    if naive_value is None:
        return False
    if evaluated_count is not None:
        return row_count == evaluated_count
    return row_count > 0


def decide_sql_source(
    evaluated_count: Optional[int],
    aggregate: Tuple[Optional[float], int],
    raw: Tuple[Optional[float], int],
) -> Tuple[Optional[str], bool]:
    """Decide which data source (if any) faithfully reproduces the original evaluation.

    Never computes SQL from a source whose timestamp set doesn't match what MASE was
    originally computed over — that's the entire point of the guard.

    Args:
        evaluated_count: the stored `forecasts.scores.evaluated_count` for this row, or
            None on older rows written before that column was populated.
        aggregate: (naive_value, row_count) from the continuous-aggregate path.
        raw: (naive_value, row_count) from the raw-bucketed fallback path.

    Returns:
        (source, no_expected_count):
        - source: 'aggregate' if the aggregate reproduces the original evaluation,
          else 'raw' if the raw fallback does, else None (coverage mismatch — skip,
          don't write).
        - no_expected_count: True iff `evaluated_count` was None and the source was
          accepted on "resolves + non-empty" alone (still written, tracked separately
          in the summary since there was nothing to actually verify against).

    Decision table:
        evaluated_count present, aggregate row_count == evaluated_count  -> ('aggregate', False)
        evaluated_count present, aggregate miss, raw row_count == evaluated_count -> ('raw', False)
        evaluated_count present, both miss                               -> (None, False)
        evaluated_count None, aggregate resolves + non-empty             -> ('aggregate', True)
        evaluated_count None, aggregate empty, raw resolves + non-empty  -> ('raw', True)
        evaluated_count None, both empty/unresolved                      -> (None, False)
    """
    agg_naive, agg_count = aggregate
    raw_naive, raw_count = raw

    if _source_sufficient(evaluated_count, agg_naive, agg_count):
        return "aggregate", evaluated_count is None
    if _source_sufficient(evaluated_count, raw_naive, raw_count):
        return "raw", evaluated_count is None
    return None, False


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
    rows_from_aggregate: int = 0  # written rows sourced from the continuous aggregate
    rows_from_raw: int = 0  # written rows sourced from the raw-bucketed fallback
    rows_no_expected_count: int = 0  # written rows where evaluated_count was NULL (nothing to verify against)
    rows_coverage_mismatch: int = 0  # skipped: neither source reproduces the stored evaluated_count
    crossing_by_model: Dict[int, int] = field(default_factory=dict)
    mismatches: List[Tuple[int, int, int, float, float]] = field(default_factory=list)
    coverage_mismatches: List[Tuple[int, int, int, Optional[int], int, int]] = field(default_factory=list)
    failed_rounds: List[Tuple[int, str]] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def record_row(
        self,
        round_id: int,
        model_id: int,
        series_id: int,
        fields: Dict[str, Any],
        stored_mase: Optional[float],
        source: str,
        no_expected_count: bool,
    ) -> None:
        self.rows_updated += 1
        if source == "aggregate":
            self.rows_from_aggregate += 1
        elif source == "raw":
            self.rows_from_raw += 1
        if no_expected_count:
            self.rows_no_expected_count += 1

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

    def record_coverage_mismatch(
        self,
        round_id: int,
        model_id: int,
        series_id: int,
        evaluated_count: Optional[int],
        aggregate_count: int,
        raw_count: int,
    ) -> None:
        self.rows_coverage_mismatch += 1
        self.coverage_mismatches.append(
            (round_id, model_id, series_id, evaluated_count, aggregate_count, raw_count)
        )

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
            f"Rounds processed:      {self.rounds_processed}",
            f"Rounds skipped:        {self.rounds_skipped}",
            f"Rounds failed:         {self.rounds_failed}",
            f"Rows updated:          {self.rows_updated}",
            f"  - real quantiles:    {self.rows_real_quantiles}",
            f"  - degenerate:        {self.rows_degenerate}",
            f"  - from aggregate:    {self.rows_from_aggregate}",
            f"  - from raw fallback: {self.rows_from_raw}",
            f"  - no expected count: {self.rows_no_expected_count}",
            f"Rows coverage mismatch:{self.rows_coverage_mismatch}",
            f"Identity mismatches:   {len(self.mismatches)}",
        ]
        if self.mismatches:
            lines.append("  Examples (round, model, series, computed_sql, stored_mase):")
            for m in self.mismatches[:5]:
                lines.append(f"    {m}")
        if self.coverage_mismatches:
            lines.append("  Coverage mismatch examples (round, model, series, evaluated_count, agg_count, raw_count):")
            for m in self.coverage_mismatches[:5]:
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

async def _get_aggregate_naive_value(
    time_series_repo: TimeSeriesRepository,
    series_id: int,
    pseudo_info: Any,
    resolution: str,
) -> Optional[float]:
    """Same lookup the live scorer uses: the context point at ChallengeSeriesPseudo.max_ts,
    read from the continuous aggregate."""
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


async def _get_raw_naive_value(
    time_series_repo: TimeSeriesRepository,
    series_id: int,
    pseudo_info: Any,
    resolution: str,
) -> Optional[float]:
    """Raw-bucketed fallback for the same context-point lookup, computed directly from
    `data_portal.time_series_data` — see `TimeSeriesRepository.get_raw_bucketed_value_at`."""
    if not pseudo_info or not pseudo_info.max_ts:
        return None
    return await time_series_repo.get_raw_bucketed_value_at(
        series_id=series_id, resolution=resolution, target_ts=pseudo_info.max_ts
    )


class _SeriesCache:
    """Per-(round, series) memoization for pseudo-info + both naive-value sources.

    Scoped to a single round (round_id is fixed by the caller), so this only needs to key
    on series_id. Naive values are shared across every model that forecast a series, and
    the raw source is only ever probed lazily — i.e. when the aggregate turns out
    insufficient for at least one model of that series — so rounds where the aggregate is
    complete never pay for the raw fallback query at all.
    """

    def __init__(self, round_repo: ChallengeRoundRepository, time_series_repo: TimeSeriesRepository, round_id: int, resolution: str):
        self._round_repo = round_repo
        self._time_series_repo = time_series_repo
        self._round_id = round_id
        self._resolution = resolution
        self._pseudo: Dict[int, Any] = {}
        self._agg_naive: Dict[int, Optional[float]] = {}
        self._raw_naive: Dict[int, Optional[float]] = {}

    async def _pseudo_info(self, series_id: int) -> Any:
        if series_id not in self._pseudo:
            self._pseudo[series_id] = await self._round_repo.get_series_pseudo(self._round_id, series_id)
        return self._pseudo[series_id]

    async def aggregate_naive(self, series_id: int) -> Optional[float]:
        if series_id not in self._agg_naive:
            pseudo_info = await self._pseudo_info(series_id)
            self._agg_naive[series_id] = await _get_aggregate_naive_value(
                self._time_series_repo, series_id, pseudo_info, self._resolution
            )
        return self._agg_naive[series_id]

    async def raw_naive(self, series_id: int) -> Optional[float]:
        if series_id not in self._raw_naive:
            pseudo_info = await self._pseudo_info(series_id)
            self._raw_naive[series_id] = await _get_raw_naive_value(
                self._time_series_repo, series_id, pseudo_info, self._resolution
            )
        return self._raw_naive[series_id]


class _SeriesActualsCache:
    """Per-(round, series) memoization for both actuals sources, keyed by minute-
    truncated ts (`group_actuals_by_minute`) so callers can join straight against it via
    `align_evaluation_data`.

    One query per series per source, not per (model, series): every model forecasting a
    series shares the same actuals. The raw fallback is only ever fetched lazily — when
    the aggregate join for at least one model of that series comes up short — so a round
    with complete aggregate coverage never pays for the raw query at all.
    """

    def __init__(
        self,
        forecast_repo: ForecastRepository,
        resolution: str,
        ts_lo: datetime,
        ts_hi: datetime,
    ):
        self._forecast_repo = forecast_repo
        self._resolution = resolution
        self._ts_lo = ts_lo
        self._ts_hi = ts_hi
        self._agg: Dict[int, Dict[datetime, List[float]]] = {}
        self._raw: Dict[int, Dict[datetime, List[float]]] = {}

    async def aggregate(self, series_id: int) -> Dict[datetime, List[float]]:
        if series_id not in self._agg:
            rows = await self._forecast_repo.get_series_actuals_aggregate(
                series_id, self._resolution, self._ts_lo, self._ts_hi
            )
            self._agg[series_id] = group_actuals_by_minute(rows)
        return self._agg[series_id]

    async def raw(self, series_id: int) -> Dict[datetime, List[float]]:
        if series_id not in self._raw:
            rows = await self._forecast_repo.get_series_actuals_raw_bucketed(
                series_id, self._resolution, self._ts_lo, self._ts_hi
            )
            self._raw[series_id] = group_actuals_by_minute(rows)
        return self._raw[series_id]


async def resolve_evaluation_source(
    forecast_rows: List[Dict[str, Any]],
    naive_cache: _SeriesCache,
    actuals_cache: _SeriesActualsCache,
    series_id: int,
    evaluated_count: Optional[int],
) -> Tuple[Optional[str], bool, Optional[float], List[Dict[str, Any]], int, int]:
    """Resolve which source (aggregate/raw/neither) to compute this (model, series) row
    from, and the joined evaluation rows for that source.

    Same decision/guard semantics as before the batching rework — `decide_sql_source` /
    `_source_sufficient` are unchanged — only the inputs are now pre-fetched: this joins
    already-fetched `forecast_rows` against the cached actuals in Python
    (`align_evaluation_data`) instead of issuing a per-row DB query. Tries the aggregate
    first; only touches the raw fallback if the aggregate alone doesn't already settle
    the decision, so rounds with complete aggregate coverage never pay for it.

    Args:
        forecast_rows: this (model, series)'s forecast rows within the round (already
            filtered/grouped by the caller from one `get_round_forecasts` call).
        naive_cache: per-(round, series) naive-value cache (aggregate + raw).
        actuals_cache: per-(round, series) actuals cache (aggregate + raw).
        series_id: the series these forecast_rows belong to.
        evaluated_count: the stored `forecasts.scores.evaluated_count` for this row.

    Returns:
        (source, no_expected_count, naive_value, evaluation_data, aggregate_count, raw_count)
        `source` is None when neither source reproduces `evaluated_count` — caller must
        skip the row.
    """
    agg_naive = await naive_cache.aggregate_naive(series_id)
    agg_actuals = await actuals_cache.aggregate(series_id)
    agg_eval = align_evaluation_data(forecast_rows, agg_actuals)

    if _source_sufficient(evaluated_count, agg_naive, len(agg_eval)):
        return "aggregate", evaluated_count is None, agg_naive, agg_eval, len(agg_eval), 0

    raw_naive = await naive_cache.raw_naive(series_id)
    raw_actuals = await actuals_cache.raw(series_id)
    raw_eval = align_evaluation_data(forecast_rows, raw_actuals)

    source, no_expected_count = decide_sql_source(
        evaluated_count, (agg_naive, len(agg_eval)), (raw_naive, len(raw_eval))
    )
    if source == "raw":
        return "raw", no_expected_count, raw_naive, raw_eval, len(agg_eval), len(raw_eval)
    return None, False, None, [], len(agg_eval), len(raw_eval)


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

            # Generous margin: forecasts can extend past end_time (see ROUND_TS_MARGIN).
            ts_lo = round_info.start_time - ROUND_TS_MARGIN
            ts_hi = round_info.end_time + ROUND_TS_MARGIN

            # ONE ts-bounded query for the whole round's forecasts (not per model/series
            # — see module docstring). Rounds that come back empty (fully pruned data)
            # are resolved without any further per-candidate queries.
            forecast_rows = await forecast_repo.get_round_forecasts(round_id, ts_lo, ts_hi)
            updates: List[Dict[str, Any]] = []

            if not forecast_rows:
                logger.warning(
                    "Round %s: 0 forecast rows in [%s, %s] — marking all %d candidate(s) "
                    "as coverage mismatch",
                    round_id, ts_lo, ts_hi, len(candidates)
                )
                for cand in candidates:
                    summary.record_coverage_mismatch(
                        round_id, cand["model_id"], cand["series_id"], cand["evaluated_count"], 0, 0
                    )
            else:
                forecasts_by_key: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
                for row in forecast_rows:
                    forecasts_by_key.setdefault((row["model_id"], row["series_id"]), []).append(row)

                # Naive values (both sources) cached per series — shared across models.
                naive_cache = _SeriesCache(round_repo, time_series_repo, round_id, resolution)
                # Actuals (both sources) cached per series — shared across models; raw
                # fetched lazily, only if some (model, series) needs the fallback.
                actuals_cache = _SeriesActualsCache(forecast_repo, resolution, ts_lo, ts_hi)

                for cand in candidates:
                    model_id = cand["model_id"]
                    series_id = cand["series_id"]
                    evaluated_count = cand["evaluated_count"]

                    rows_for_model_series = forecasts_by_key.get((model_id, series_id), [])
                    if not rows_for_model_series:
                        # This (model, series) has no forecasts within the ts-bounded
                        # window at all — nothing to join, skip straight to reporting
                        # without touching the actuals cache.
                        summary.record_coverage_mismatch(
                            round_id, model_id, series_id, evaluated_count, 0, 0
                        )
                        continue

                    source, no_expected_count, naive_value, evaluation_data, agg_count, raw_count = (
                        await resolve_evaluation_source(
                            rows_for_model_series, naive_cache, actuals_cache, series_id, evaluated_count
                        )
                    )

                    if source is None:
                        # Neither source reproduces the original evaluation's timestamp
                        # set (or, when evaluated_count is NULL, neither has any data at
                        # all). Per the faithfulness guard: never write SQL computed over
                        # a different timestamp set than MASE was — skip and report.
                        logger.warning(
                            "Round %s, model %s, series %s: neither source reproduces "
                            "evaluated_count=%s (aggregate=%d rows, raw=%d rows) — skipping row",
                            round_id, model_id, series_id, evaluated_count, agg_count, raw_count
                        )
                        summary.record_coverage_mismatch(
                            round_id, model_id, series_id, evaluated_count, agg_count, raw_count
                        )
                        continue

                    fields = recompute_sql_for_model_series(evaluation_data, naive_value)
                    summary.record_row(
                        round_id, model_id, series_id, fields, cand["mase"], source, no_expected_count
                    )
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
