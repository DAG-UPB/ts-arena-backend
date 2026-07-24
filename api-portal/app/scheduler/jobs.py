from __future__ import annotations
import asyncio
import logging
import functools
import time
from typing import Any, Dict, Callable, Awaitable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.connection import SessionLocal
from app.services.challenge_service import ChallengeService
from app.services.score_evaluation_service import ScoreEvaluationService
from app.services.elo_ranking_service import EloRankingService
from app.scheduler.dependencies import get_scheduler


# --- Eval-job hang guards (backend-48) -------------------------------------
# A single run of the periodic scores-evaluation job must never outlive its own
# fire cadence: it runs with max_running_jobs=1, so a run that hangs holds the
# only slot forever and every later fire queues up behind it until the backlog
# starves all job acquisition (the 2026-07 incident).
#
# EVAL_JOB_HARD_TIMEOUT_SECONDS is the outer backstop: comfortably above normal
# runtime (seconds to a couple of minutes, even with a batch of rounds) but well
# under the 10-minute (600 s) fire cadence, so a hung run is cancelled and the
# single running slot is freed before the next fire.
EVAL_JOB_HARD_TIMEOUT_SECONDS = 480  # 8 minutes

# Postgres-side guards applied to every session the eval job opens, so a query
# or lock that hangs is killed by the database (a clean, logged error) before
# the asyncio backstop above ever trips. Both sit below the hard timeout.
EVAL_STATEMENT_TIMEOUT_MS = 240_000  # 4 minutes
EVAL_LOCK_TIMEOUT_MS = 30_000        # 30 seconds


async def _apply_eval_session_timeouts(session: AsyncSession) -> None:
    """Bound how long any statement/lock in this eval session may block.

    Applied per session the eval job uses. The values are session-scoped GUCs;
    the eval sessions are short-lived and, on the hang path, the connection is
    invalidated on cancellation, so nothing lingers. Both bounds only ever kill
    a genuinely runaway statement/lock — normal eval, round-creation and ELO
    queries all complete far below them.
    """
    await session.execute(text(f"SET statement_timeout = {EVAL_STATEMENT_TIMEOUT_MS}"))
    await session.execute(text(f"SET lock_timeout = {EVAL_LOCK_TIMEOUT_MS}"))


def job_error_handler(func: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """
    Decorator that wraps job functions with comprehensive error handling.
    Ensures jobs never crash the scheduler due to unhandled exceptions.
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        logger = logging.getLogger("challenge-scheduler")
        job_name = func.__name__
        
        try:
            logger.debug(f"Starting job: {job_name}")
            await func(*args, **kwargs)
            logger.debug(f"Completed job: {job_name}")
        except Exception as e:
            logger.error(
                f"Job '{job_name}' failed with error: {e}",
                exc_info=True,
                extra={"job_name": job_name, "job_args": args, "job_kwargs": kwargs}
            )
            # Don't re-raise - we want to catch all exceptions to prevent scheduler crashes
    
    return wrapper


@job_error_handler
async def create_round_from_definition_job(definition_id: int) -> None:
    """
    Job function that creates a new challenge round from a definition.
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info(f"Starting job to create round from definition {definition_id}")

    try:
        # Get scheduler from global reference
        scheduler = get_scheduler()
        
        async with SessionLocal() as session:
            challenge_service = ChallengeService(session, scheduler=scheduler)
            
            round_obj = await challenge_service.create_round_from_definition(definition_id)
            
            logger.info(f"Successfully created round '{round_obj.name}' (ID: {round_obj.id})")

    except Exception as e:
        logger.exception(f"Failed to create round from definition {definition_id}. Error: {e}")
        raise  # Re-raise to let decorator handle it


@job_error_handler
async def prepare_round_context_data_job(round_id: int) -> None:
    """
    Job function that prepares context data for a challenge round.
    Called at registration_start to ensure fresh data.
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info(f"Starting context data preparation for round {round_id}")

    try:
        async with SessionLocal() as session:
            challenge_service = ChallengeService(session)
            
            # Execute preparation
            await challenge_service.prepare_round_context_data(round_id)
            
            logger.info(f"Successfully prepared context data for round {round_id}")

    except Exception as e:
        logger.exception(f"Failed to prepare context data for round {round_id}: {e}")
        raise  # Re-raise to let decorator handle it


@job_error_handler
async def periodic_challenge_scores_evaluation_job() -> None:
    """
    Periodic job that evaluates challenge scores for all active and completed challenges.
    
    This job runs every 10 minutes and:
    1. Finds all challenges with status 'active' or 'completed' that have final_evaluation=False
    2. Calculates MASE and RMSE scores for all participants
    3. Updates scores in the database
    4. Marks challenges as final when all data is complete
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info("Starting periodic challenge scores evaluation job")

    try:
        # Hard timeout so a hung run can never hold the max_running_jobs=1 slot
        # past the fire cadence. On timeout the coroutine is cancelled and
        # returns, freeing the slot for the next fire (backend-48).
        async with asyncio.timeout(EVAL_JOB_HARD_TIMEOUT_SECONDS):
            # Step 1: Retrieve list of Rounds to evaluate (Short-lived Session)
            round_ids = []
            async with SessionLocal() as session:
                await _apply_eval_session_timeouts(session)
                score_service = ScoreEvaluationService(session)
                round_ids = await score_service.get_ids_needing_evaluation()

            if not round_ids:
                logger.info("No rounds need evaluation at this time.")
                return

            logger.info(f"Found {len(round_ids)} round(s) needing evaluation")

            # Step 2: Process each round in a separate session
            # This prevents one long transaction from holding a DB connection for the entire batch.
            evaluated_count = 0
            finalized_count = 0

            for round_id in round_ids:
                try:
                    async with SessionLocal() as session:
                        await _apply_eval_session_timeouts(session)
                        score_service = ScoreEvaluationService(session)
                        finalized = await score_service.evaluate_challenge_scores(round_id)

                        evaluated_count += 1
                        if finalized:
                            finalized_count += 1
                except Exception as e:
                    logger.error(f"Error evaluating round {round_id} in periodic job: {e}")
                    # Continue with next round instead of failing the whole job

            logger.info(
                f"Periodic evaluation complete: "
                f"{evaluated_count} rounds evaluated, "
                f"{finalized_count} finalized"
            )

    except TimeoutError:
        # Loud, unmissable — the opposite of the silent stall this replaces.
        logger.critical(
            "periodic_challenge_scores_evaluation_job exceeded its %ss hard timeout and was "
            "cancelled to free the max_running_jobs=1 slot for the next fire. A run this slow "
            "means a hung DB session or lock — investigate; the slot is now free.",
            EVAL_JOB_HARD_TIMEOUT_SECONDS,
            exc_info=True,
        )
        raise  # Re-raise to let decorator handle it
    except Exception as e:
        logger.exception(f"Failed to run periodic challenge scores evaluation: {e}")
        raise  # Re-raise to let decorator handle it


@job_error_handler
async def periodic_elo_ranking_calculation_job() -> None:
    """
    Periodic job that calculates bootstrapped ELO ratings for all models.
    
    This job runs 4x daily (every 6 hours) and:
    1. Calculates global ELO rating across all challenges
    2. Calculates per-definition ELO ratings
    3. Calculates per-frequency+horizon ELO ratings
    4. Stores results in forecasts.daily_rankings table
    5. Logs timing metrics for performance monitoring
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info("Starting periodic ELO ranking calculation job")
    
    start_time = time.time()
    
    try:
        async with SessionLocal() as session:
            elo_service = EloRankingService(session)

            # Calculate and store ELO ratings for both the point metric (MASE) and the
            # probabilistic metric (SQL). Each is a separate ranking dimension.
            for metric in EloRankingService.SUPPORTED_METRICS:
                results = await elo_service.calculate_and_store_all_ratings(
                    n_bootstraps=500,
                    metric=metric
                )

                n_global = len(results.get("global", []))
                n_definitions = len(results.get("per_definition", []))
                n_freq_horizon = len(results.get("per_frequency_horizon", []))
                total_duration_ms = results.get("total_duration_ms", 0)

                logger.info(
                    f"✅ ELO [{metric}] calculation done. "
                    f"Global: {n_global}, Definitions: {n_definitions}, FreqHorizon: {n_freq_horizon}, "
                    f"Calculation time: {total_duration_ms}ms"
                )

            logger.info(
                f"✅ ELO calculation SUCCESS in {time.time() - start_time:.1f}s "
                f"(metrics: {', '.join(EloRankingService.SUPPORTED_METRICS)})"
            )

    
    except Exception as e:
        duration_seconds = time.time() - start_time
        logger.error(
            f"❌ ELO calculation FAILED after {duration_seconds:.1f}s: {e}",
            exc_info=True
        )
        raise  # Re-raise to let decorator handle it



@job_error_handler
async def startup_elo_check_job() -> None:
    """
    Startup job that checks if ELO ratings have been calculated today.
    If not, triggers a calculation immediately.
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info("Checking if ELO ratings have been calculated today...")
    
    start_time = time.time()
    
    try:
        async with SessionLocal() as session:
            elo_service = EloRankingService(session)

            # Compute any metric not yet calculated today (MASE and SQL).
            pending = [
                metric for metric in EloRankingService.SUPPORTED_METRICS
                if not await elo_service.has_calculated_today(metric=metric)
            ]
            if not pending:
                logger.info("ELO ratings already calculated today for all metrics. Skipping startup calculation.")
                return

            logger.info(f"No ELO ratings for today (metrics: {', '.join(pending)}). Starting calculation...")

            for metric in pending:
                results = await elo_service.calculate_and_store_all_ratings(
                    n_bootstraps=500,
                    metric=metric
                )
                if not results:
                    logger.info(f"Startup ELO [{metric}] calculation: No data available for ranking.")
                    continue
                n_global = len(results.get('global', []))
                n_definitions = len(results.get('per_definition', []))
                n_freq_horizon = len(results.get('per_frequency_horizon', []))
                total_time = results.get('total_duration_ms', 0)
                logger.info(
                    f"✅ Startup ELO [{metric}] complete. "
                    f"Global: {n_global}, Definitions: {n_definitions}, FreqHorizon: {n_freq_horizon}, "
                    f"Calculation time: {total_time}ms"
                )

            logger.info(f"✅ Startup ELO calculation complete in {time.time() - start_time:.1f}s")

    
    except Exception as e:
        duration_seconds = time.time() - start_time
        logger.error(
            f"❌ Startup ELO check FAILED after {duration_seconds:.1f}s: {e}",
            exc_info=True
        )
        raise  # Re-raise to let decorator handle it
