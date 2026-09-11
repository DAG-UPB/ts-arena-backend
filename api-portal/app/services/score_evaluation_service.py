"""
Service for periodic evaluation of challenge scores.
This service runs independently every 30 minutes to calculate and update scores
for active and completed challenge rounds.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.metrics import mean_squared_error
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.database.challenges.challenge_repository import ChallengeRoundRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.forecasts.repository import ForecastRepository
from app.services.evaluation_alignment import align_evaluation_data, group_actuals_by_minute
from app.services.forecast_metrics import compute_sql_fields

logger = logging.getLogger(__name__)


# Mapping from timedelta frequency to resolution view
FREQUENCY_TO_RESOLUTION: Dict[timedelta, str] = {
    timedelta(minutes=15): "15min",
    timedelta(hours=1): "1h",
    timedelta(days=1): "1d",
}

# Timeout configuration for forced finalization
EVALUATION_TIMEOUT = timedelta(days=1)  # Grace period after round end
MIN_COVERAGE_FOR_FINAL = 0.95           # Minimum 95% coverage required for valid score


def _as_utc(value: datetime) -> datetime:
    """Normalise to UTC so naive DB timestamps compare against tz-aware ones."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def series_forecast_windows(
    context_edges: Dict[int, Optional[datetime]],
    frequency: Optional[timedelta],
    horizon: Optional[timedelta],
) -> Dict[int, tuple]:
    """
    `series_id -> (first_ts, last_ts)`, the closed range of timestamps a forecast for that
    series may legitimately carry: `context_edge + frequency` through
    `context_edge + horizon`.

    The scoring backstop for backend-87. Uploads are validated against exactly this range
    (`ForecastService._expected_forecast_timestamps`), but validation only binds new uploads —
    rows already in `forecasts.forecasts` were accepted when no timestamp was checked at all.
    Bounding the score here means an out-of-window point is not evaluated even if it is
    already stored.

    Derived per series rather than from `rounds.start_time` for the same reason upload
    validation is: `start_time` is the global max context edge across the round's series, and
    a lagging series' legitimate window starts one step earlier.

    Series with no stored context edge are omitted — the caller leaves those unfiltered, since
    there is nothing to derive a window from.
    """
    if not frequency or not horizon:
        return {}
    windows = {}
    for series_id, edge in context_edges.items():
        if edge is None:
            continue
        edge = _as_utc(edge)
        windows[series_id] = (edge + frequency, edge + horizon)
    return windows


def timedelta_to_resolution(frequency: Optional[timedelta]) -> str:
    """
    Maps a timedelta frequency to the corresponding resolution view name.
    
    Args:
        frequency: The frequency as timedelta (e.g., 1 hour, 15 minutes)
        
    Returns:
        Resolution string ("15min", "1h", "1d") for use with materialized views.
        Defaults to "1h" if frequency is None or unknown.
    """
    if frequency is None:
        logger.warning("Frequency is None, defaulting to '1h' resolution")
        return "1h"
    
    resolution = FREQUENCY_TO_RESOLUTION.get(frequency)
    if not resolution:
        logger.warning(f"Unknown frequency {frequency}, defaulting to '1h' resolution")
        return "1h"
    return resolution


def evaluation_timeout_passed(round_end_time: Optional[datetime]) -> bool:
    """Has the post-round grace period for ground truth to arrive elapsed?

    A round without an end time never times out (nothing to measure the grace period
    from), which is the pre-existing behaviour.
    """
    if round_end_time is None:
        return False
    end_time = round_end_time
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    else:
        end_time = end_time.astimezone(timezone.utc)
    return datetime.now(timezone.utc) > end_time + EVALUATION_TIMEOUT


# A forecast and an actual join when their minute-truncated timestamps are equal, so any
# actual that can possibly match lies strictly within one minute of some forecast. Padding
# the actuals fetch by exactly that much makes the ts-bounded batch query provably lossless
# against the unbounded per-pair join it replaces.
_ACTUALS_TS_MARGIN = timedelta(minutes=1)


class _SeriesNaiveCache:
    """Per-series naive baseline value for one round.

    The baseline is the context point at `ChallengeSeriesPseudo.max_ts` — a property of
    (round, series), identical for every model that forecast that series. Resolving it once
    per series instead of once per (model, series) removes two queries per participant.
    """

    def __init__(
        self,
        round_repo: ChallengeRoundRepository,
        time_series_repo: TimeSeriesRepository,
        round_id: int,
        resolution: str,
    ):
        self._round_repo = round_repo
        self._time_series_repo = time_series_repo
        self._round_id = round_id
        self._resolution = resolution
        self._values: Dict[int, Optional[float]] = {}

    async def value(self, series_id: int) -> Optional[float]:
        if series_id not in self._values:
            self._values[series_id] = await self._resolve(series_id)
        return self._values[series_id]

    async def _resolve(self, series_id: int) -> Optional[float]:
        # Try ChallengeSeriesPseudo first (most accurate definition of context end).
        pseudo_info = await self._round_repo.get_series_pseudo(self._round_id, series_id)
        if not pseudo_info or not pseudo_info.max_ts:
            return None
        context_points = await self._time_series_repo.get_data_by_time_range_by_resolution(
            series_id=series_id,
            start_time=pseudo_info.max_ts,
            end_time=pseudo_info.max_ts,
            resolution=self._resolution,
        )
        if not context_points:
            return None
        return context_points[0]['value']


class _SeriesActualsCache:
    """Per-series actuals for one round, keyed by minute-truncated ts.

    Every model forecasting a series is scored against the same actuals, so this is one
    query per series rather than one per (model, series). The ts window is derived from the
    forecasts actually present for that series (see `_ACTUALS_TS_MARGIN`), which both prunes
    hypertable chunks and keeps the result identical to the unbounded SQL join.

    Series with no forecasts never trigger a query at all.
    """

    def __init__(
        self,
        forecast_repo: ForecastRepository,
        resolution: str,
        stats_by_pair: Dict[tuple, Dict[str, Any]],
    ):
        self._forecast_repo = forecast_repo
        self._resolution = resolution
        self._stats_by_pair = stats_by_pair
        self._by_minute: Dict[int, Dict[datetime, List[float]]] = {}

    def _ts_bounds(self, series_id: int) -> Optional[tuple]:
        """Widest [min_ts, max_ts] over every model that forecast this series, padded."""
        stats = [s for (_, sid), s in self._stats_by_pair.items() if sid == series_id]
        bounds = [(s["min_ts"], s["max_ts"]) for s in stats if s["min_ts"] and s["max_ts"]]
        if not bounds:
            return None
        return (
            min(lo for lo, _ in bounds) - _ACTUALS_TS_MARGIN,
            max(hi for _, hi in bounds) + _ACTUALS_TS_MARGIN,
        )

    async def by_minute(self, series_id: int) -> Dict[datetime, List[float]]:
        if series_id not in self._by_minute:
            bounds = self._ts_bounds(series_id)
            if bounds is None:
                self._by_minute[series_id] = {}
            else:
                rows = await self._forecast_repo.get_series_actuals_aggregate(
                    series_id, self._resolution, bounds[0], bounds[1]
                )
                self._by_minute[series_id] = group_actuals_by_minute(rows)
        return self._by_minute[series_id]


class ScoreEvaluationService:
    """
    Service to periodically evaluate challenge scores.
    
    This service:
    1. Finds all challenge rounds with status 'active' or 'completed' that have scores with final_evaluation=False
    2. For each round, calculates MASE and RMSE for all model/series combinations
    3. Updates the scores in the database
    4. When all data is complete and all forecasts are evaluated, sets final_evaluation=True
    """

    def __init__(self, db_session: AsyncSession):
        self.round_repo = ChallengeRoundRepository(db_session)
        self.time_series_repo = TimeSeriesRepository(db_session)
        self.forecast_repo = ForecastRepository(db_session)
        self.db_session = db_session

    async def get_ids_needing_evaluation(self) -> List[int]:
        """
        Retrieves the IDs of all rounds that currently require evaluation.
        Useful for batch processing where each evaluation runs in its own transaction.
        """
        return await self.forecast_repo.get_ids_needing_evaluation()

    async def evaluate_pending_challenges(self) -> Dict[str, Any]:
        """
        Main entry point for periodic evaluation.
        Finds and evaluates all rounds that need score updates.
        
        Returns:
            Summary dict with evaluation results
        """
        round_ids = await self.forecast_repo.get_ids_needing_evaluation()
        
        if not round_ids:
            logger.info("No rounds need evaluation at this time.")
            return {"evaluated": 0, "finalized": 0}
        
        logger.info(f"Found {len(round_ids)} round(s) needing evaluation")
        
        evaluated_count = 0
        finalized_count = 0
        
        for round_id in round_ids:
            try:
                finalized = await self.evaluate_challenge_scores(round_id)
                evaluated_count += 1
                if finalized:
                    finalized_count += 1
            except Exception as e:
                logger.exception(f"Failed to evaluate round {round_id}: {e}")
        
        logger.info(f"Evaluation complete: {evaluated_count} evaluated, {finalized_count} finalized")
        return {"evaluated": evaluated_count, "finalized": finalized_count}

    async def evaluate_challenge_scores(self, round_id: int) -> bool:
        """
        Evaluate scores for a single round.
        
        Args:
            round_id: ID of the round to evaluate

        Returns:
            True if round was finalized (final_evaluation=True), False otherwise
        """
        # Lock key constant (using 42 as the "evaluation service" namespace)
        LOCK_KEY_1 = 42
        LOCK_KEY_2 = round_id
        
        # Try to acquire advisory lock for this round to prevent concurrent evaluation
        # pg_advisory_lock persists across transaction commits, which is needed here
        # since bulk_insert_scores and mark_scores_final perform internal commits.
        result = await self.db_session.execute(
            select(func.pg_try_advisory_lock(LOCK_KEY_1, LOCK_KEY_2))
        )
        lock_acquired = result.scalar()
        
        if not lock_acquired:
            logger.info(f"Round {round_id} is currently locked by another process. Skipping evaluation.")
            return False
            
        try:
            logger.info(f"Evaluating scores for round {round_id}")
            
            # Get round details
            round_info = await self.round_repo.get_by_id(round_id)
            if not round_info:
                logger.warning(f"Round {round_id} not found")
                return False
            
            # Per-(model, series) forecast stats for the whole round in one query. This
            # also yields the participant and series sets — both were previously separate
            # `SELECT DISTINCT`s over the same rows, so deriving them here is equivalent.
            round_stats = await self.forecast_repo.get_round_forecast_stats(round_id)
            stats_by_pair = {
                (s["model_id"], s["series_id"]): s for s in round_stats
            }

            participant_model_ids = sorted({s["model_id"] for s in round_stats})
            if not participant_model_ids:
                logger.info(f"No participants found for round {round_id}")
                return False

            series_ids = sorted({s["series_id"] for s in round_stats})
            if not series_ids:
                logger.info(f"No series found for round {round_id}")
                return False

            logger.info(f"Round {round_id}: {len(participant_model_ids)} participants, {len(series_ids)} series")

            # Determine resolution from frequency
            resolution = timedelta_to_resolution(round_info.frequency)
            logger.info(f"Round {round_id}: using resolution '{resolution}' (frequency: {round_info.frequency})")

            # --- Batched fetch (backend-68) ------------------------------------------
            # Everything this round needs is read here, per round and per series, instead
            # of per (model, series). Only the source of the data changes; the scoring,
            # coverage and finalization rules below are untouched.
            forecast_rows = await self.forecast_repo.get_round_forecasts(round_id)

            # Score only what falls inside the window each series was actually issued
            # (backend-87). Forecast timestamps were unvalidated on upload until now, and the
            # score is an inner join on (series_id, ts) with no window predicate, so a point
            # placed over an already-published stretch was evaluated like any other —
            # wherever the round window sat. Uploads are now checked against exactly this
            # range; this is the backstop for rows already stored.
            #
            # Dropped points still count against coverage: `forecast_count` below is taken
            # from the filtered rows, so a model submitting out-of-window points scores as
            # having submitted fewer valid ones, not as having submitted good ones.
            windows = series_forecast_windows(
                await self.round_repo.get_series_context_edges(round_id),
                round_info.frequency,
                round_info.horizon,
            )
            if windows:
                kept = []
                dropped = 0
                for row in forecast_rows:
                    window = windows.get(row["series_id"])
                    if window is None:
                        kept.append(row)
                        continue
                    ts = _as_utc(row["ts"])
                    if window[0] <= ts <= window[1]:
                        kept.append(row)
                    else:
                        dropped += 1
                if dropped:
                    logger.warning(
                        f"Round {round_id}: excluded {dropped} out-of-window forecast "
                        f"point(s) from scoring"
                    )
                forecast_rows = kept

            forecasts_by_pair: Dict[tuple, List[Dict[str, Any]]] = {}
            for row in forecast_rows:
                forecasts_by_pair.setdefault((row["model_id"], row["series_id"]), []).append(row)

            # Re-derive the per-pair stats from the filtered rows so that `count` (the
            # coverage denominator) and the actuals ts bounds both describe in-window points
            # only. The participant and series sets stay as `get_round_forecast_stats`
            # returned them, so a model whose every point was out of window still gets a
            # score row — it falls through to the `no_forecasts` branch rather than vanishing
            # from the round silently.
            for pair, stats in stats_by_pair.items():
                rows = forecasts_by_pair.get(pair, [])
                timestamps = [_as_utc(row["ts"]) for row in rows]
                stats["count"] = len(rows)
                stats["min_ts"] = min(timestamps) if timestamps else None
                stats["max_ts"] = max(timestamps) if timestamps else None

            actuals_cache = _SeriesActualsCache(
                self.forecast_repo, resolution, stats_by_pair
            )
            naive_cache = _SeriesNaiveCache(
                self.round_repo, self.time_series_repo, round_id, resolution
            )

            # Calculate scores for each model/series combination
            all_scores = []

            for model_id in participant_model_ids:
                for series_id in series_ids:
                    try:
                        score_data = await self._calculate_score_for_model_series(
                            round_id=round_id,
                            model_id=model_id,
                            series_id=series_id,
                            forecast_stats=stats_by_pair.get((model_id, series_id)),
                            forecast_rows=forecasts_by_pair.get((model_id, series_id), []),
                            actuals_cache=actuals_cache,
                            naive_cache=naive_cache,
                            round_end_time=round_info.end_time
                        )

                        if score_data:
                            # Skip "no_forecasts" status
                            if score_data.get("evaluation_status") == "no_forecasts":
                                continue
                            all_scores.append(score_data)

                    except Exception as e:
                        logger.exception(
                            f"Error calculating score for round {round_id}, "
                            f"model {model_id}, series {series_id}: {e}"
                        )

                        # Check if timeout has passed
                        now = datetime.now(timezone.utc)
                        end_time = round_info.end_time
                        if end_time.tzinfo is None:
                            end_time = end_time.replace(tzinfo=timezone.utc)
                        else:
                            end_time = end_time.astimezone(timezone.utc)

                        timeout_passed = now > end_time + EVALUATION_TIMEOUT

                        # Add an error entry
                        # If timeout has passed, we mark it as final evaluation even on error
                        all_scores.append({
                            "round_id": round_id,
                            "model_id": model_id,
                            "series_id": series_id,
                            "mase": None,
                            "rmse": None,
                            "forecast_count": 0,
                            "actual_count": 0,
                            "evaluated_count": 0,
                            "data_coverage": 0.0,
                            "final_evaluation": timeout_passed,
                            "evaluation_status": "error",
                            "error_message": str(e)[:500],
                        })

            # Bulk insert/update scores
            if all_scores:
                rows_affected = await self.forecast_repo.bulk_insert_scores(all_scores)
                logger.info(f"Updated {rows_affected} scores for round {round_id}")
            
            return True
            
        finally:
            # Only release the lock if it was actually acquired
            if lock_acquired:
                await self.db_session.execute(
                    select(func.pg_advisory_unlock(LOCK_KEY_1, LOCK_KEY_2))
                )
                # No need to commit here as pg_advisory_unlock is immediate, 
                # and previous ops already committed.

    async def _calculate_score_for_model_series(
        self,
        round_id: int,
        model_id: int,
        series_id: int,
        forecast_stats: Optional[Dict[str, Any]],
        forecast_rows: List[Dict[str, Any]],
        actuals_cache: "_SeriesActualsCache",
        naive_cache: "_SeriesNaiveCache",
        round_end_time: Optional[datetime] = None
    ) -> Dict[str, Any] | None:
        """
        Calculate MASE, RMSE and SQL for a specific model/series combination.

        Takes its inputs pre-fetched by the caller (backend-68) rather than issuing four
        queries of its own per pair. The scoring, coverage and finalization rules are
        unchanged — the caches below return exactly what the per-pair queries returned,
        which is what makes this a pure fetch-strategy change.

        Args:
            round_id: Challenge round ID
            model_id: Model ID
            series_id: Time series ID
            forecast_stats: this pair's row from `get_round_forecast_stats`, or None when
                the pair submitted nothing (equivalent to `get_forecast_stats` -> None)
            forecast_rows: this pair's forecast rows from the round-wide fetch
            actuals_cache: per-series actuals, joined in Python
            naive_cache: per-series naive baseline value
            round_end_time: Round end time for timeout calculation
        """
        if not forecast_stats or forecast_stats['count'] == 0:
            logger.debug(f"No forecasts for model {model_id}, series {series_id}")
            return {
                "round_id": round_id,
                "model_id": model_id,
                "series_id": series_id,
                "mase": None,
                "rmse": None,
                "forecast_count": 0,
                "actual_count": 0,
                "evaluated_count": 0,
                "data_coverage": 0.0,
                "final_evaluation": False,
                "evaluation_status": "no_forecasts",
                "error_message": None,
            }
        
        forecast_count = forecast_stats['count']

        # Last context point for the naive forecast baseline. Depends on (round, series)
        # only — the cache resolves it once per series instead of once per participant.
        naive_forecast_value = await naive_cache.value(series_id)

        if naive_forecast_value is None:
            logger.warning(f"No context point for series {series_id}")
            return {
                "round_id": round_id,
                "model_id": model_id,
                "series_id": series_id,
                "mase": None,
                "rmse": None,
                "forecast_count": forecast_count,
                "actual_count": 0,
                "evaluated_count": 0,
                "data_coverage": 0.0,
                # Terminal once the grace period is over (backend-68): without a context
                # point there is no naive baseline and never will be, so re-checking every
                # cycle forever gains nothing. See the `no_overlap` branch below for why
                # this is invisible to the leaderboard.
                "final_evaluation": evaluation_timeout_passed(round_end_time),
                "evaluation_status": "error",
                "error_message": "No context point available for naive forecast baseline",
            }
        
        # Aligned evaluation data. Same inner join as before — on series_id and
        # minute-truncated ts — but performed in Python against the series' actuals
        # (fetched once for the whole round) instead of one SQL join per pair.
        evaluation_data = align_evaluation_data(
            forecast_rows, await actuals_cache.by_minute(series_id)
        )

        evaluated_count = len(evaluation_data)
        actual_count = evaluated_count
        
        if evaluated_count == 0:
            logger.debug(f"No overlapping timestamps for model {model_id}, series {series_id}")
            return {
                "round_id": round_id,
                "model_id": model_id,
                "series_id": series_id,
                "mase": None,
                "rmse": None,
                "forecast_count": forecast_count,
                "actual_count": actual_count,
                "evaluated_count": 0,
                "data_coverage": 0.0,
                # Terminal once the grace period is over (backend-68). This branch used to
                # return False unconditionally, which is what kept months-old rounds in the
                # evaluation candidate set forever: the candidate query selects any round
                # holding a score row with final_evaluation = FALSE, so a handful of pairs
                # with no ground truth kept whole rounds — otherwise fully scored — being
                # re-evaluated every cycle.
                #
                # Flipping this changes nothing downstream: `mase` is NULL here and
                # `forecasts.v_ranking_base` filters on `mase IS NOT NULL`, so these rows
                # are already absent from the leaderboard and Elo either way. Ground truth
                # that arrives after the grace period is not picked back up — a deliberate
                # call (2026-07-24) to keep the mechanism simple.
                "final_evaluation": evaluation_timeout_passed(round_end_time),
                "evaluation_status": "no_overlap",
                "error_message": "No overlapping timestamps between forecasts and actuals",
            }
        
        # Calculate data coverage
        data_coverage = evaluated_count / forecast_count if forecast_count > 0 else 0.0
        
        # Determine evaluation status
        if data_coverage >= 1.0:
            evaluation_status = "complete"
        elif data_coverage > 0:
            evaluation_status = "partial"
        else:
            evaluation_status = "pending"
        
        # Check if evaluation timeout has passed
        timeout_passed = evaluation_timeout_passed(round_end_time)

        # Determine if final evaluation
        # Complete = 100% coverage -> final
        # Timeout passed with >= 95% coverage -> final (valid score)
        # Timeout passed with < 95% coverage -> final but excluded (mase=NULL)
        if evaluation_status == "complete":
            final_evaluation = True
        elif timeout_passed:
            final_evaluation = True
            if data_coverage < MIN_COVERAGE_FOR_FINAL:
                # Insufficient data after timeout - exclude from ELO
                logger.info(
                    f"Timeout: round {round_id}, model {model_id}, series {series_id} "
                    f"has only {data_coverage:.1%} coverage - marking as insufficient_data"
                )
                return {
                    "round_id": round_id,
                    "model_id": model_id,
                    "series_id": series_id,
                    "mase": None,  # Excluded from ELO
                    "rmse": None,
                    "forecast_count": forecast_count,
                    "actual_count": actual_count,
                    "evaluated_count": evaluated_count,
                    "data_coverage": data_coverage,
                    "final_evaluation": True,
                    "evaluation_status": "insufficient_data",
                    "error_message": f"Timeout: only {data_coverage:.1%} coverage (min {MIN_COVERAGE_FOR_FINAL:.0%} required)",
                }
            else:
                # Sufficient data after timeout - keep as valid partial
                evaluation_status = "partial"
        else:
            final_evaluation = False
        
        # Aligned arrays
        y_pred = np.array([item["predicted_value"] for item in evaluation_data])
        y_true = np.array([item["actual_value"] for item in evaluation_data])
        
        # Calculate RMSE
        rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
        
        # Calculate MASE
        # naive_forecast_value is already determined
        mae_model = float(np.mean(np.abs(y_true - y_pred)))
        mae_naive = float(np.mean(np.abs(y_true - naive_forecast_value)))
        
        if mae_naive > 0:
            mase = mae_model / mae_naive
        elif mae_naive == 0 and mae_model == 0:
            mase = 0.0
        else:
            mase = float('inf')

        # --- Scaled Quantile Loss (SQL) ---------------------------------------------
        # Reuse mae_naive as the SQL scale `a` (same denominator as MASE, so SQL and MASE
        # are directly comparable). NULL when the scale is undefined (mae_naive == 0),
        # mirroring the MASE edge case. `compute_sql_fields` is the single source of truth
        # for this block, shared with the historical backfill script so both produce
        # byte-identical results for the same inputs.
        probabilistic_values = [item.get("probabilistic_values") for item in evaluation_data]
        sql_fields = compute_sql_fields(y_true, y_pred, probabilistic_values, mae_naive)

        return {
            "round_id": round_id,
            "model_id": model_id,
            "series_id": series_id,
            "mase": mase,
            "rmse": rmse,
            **sql_fields,
            "forecast_count": forecast_count,
            "actual_count": actual_count,
            "evaluated_count": evaluated_count,
            "data_coverage": data_coverage,
            "final_evaluation": final_evaluation,
            "evaluation_status": evaluation_status,
            "error_message": None,
        }

    async def _should_finalize_round(self, round_info: Any) -> bool:
        """
        Determine if a round should be marked as final.
        """
        now = datetime.now(timezone.utc)
        
        end_time = round_info.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        else:
            end_time = end_time.astimezone(timezone.utc)
        
        # 1 hour buffer
        finalization_buffer = timedelta(hours=1)
        
        if now < end_time + finalization_buffer:
            return False
        
        # Check if all scores are complete
        all_complete = await self.forecast_repo.check_all_scores_complete(round_info.id)
        
        return all_complete
