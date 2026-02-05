from __future__ import annotations
import logging
import functools
from typing import Any, Dict, Callable, Awaitable
from app.database.connection import SessionLocal
from app.services.challenge_service import ChallengeService
from app.services.score_evaluation_service import ScoreEvaluationService
from app.services.elo_ranking_service import EloRankingService
from app.scheduler.dependencies import get_scheduler


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
        # Step 1: Retrieve list of Rounds to evaluate (Short-lived Session)
        round_ids = []
        async with SessionLocal() as session:
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
    3. Stores results in forecasts.elo_ratings table
    4. Logs timing metrics for performance monitoring
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info("Starting periodic ELO ranking calculation job")
    
    try:
        async with SessionLocal() as session:
            elo_service = EloRankingService(session)
            
            # Calculate and store all ELO ratings
            results = await elo_service.calculate_and_store_all_ratings(
                n_bootstraps=500
            )

            
            # Log results
            n_definitions = len(results.get("per_definition", []))
            
            logger.info(
                f"ELO calculation complete: {n_definitions} definitions processed. "
                f"Total time: {results['total_duration_ms']}ms"
            )

    
    except Exception as e:
        logger.exception(f"Failed to run periodic ELO ranking calculation: {e}")
        raise  # Re-raise to let decorator handle it


@job_error_handler
async def startup_elo_check_job() -> None:
    """
    Startup job that checks if ELO ratings have been calculated today.
    If not, triggers a calculation immediately.
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info("Checking if ELO ratings have been calculated today...")
    
    try:
        async with SessionLocal() as session:
            elo_service = EloRankingService(session)
            
            # Check if already calculated today
            if await elo_service.has_calculated_today():
                logger.info("ELO ratings already calculated today. Skipping startup calculation.")
                return
            
            logger.info("No ELO ratings for today. Starting calculation...")
            
            # Run the calculation
            results = await elo_service.calculate_and_store_all_ratings(
                n_bootstraps=500
            )

            
            # Handle case where no data is available
            if not results:
                logger.info("Startup ELO calculation: No data available for ranking.")
                return
            
            n_definitions = len(results.get('per_definition') or [])
            total_time = results.get('total_duration_ms', 0)
            
            logger.info(
                f"Startup ELO calculation complete. "
                f"Processed {n_definitions} definitions in {total_time}ms"
            )

    
    except Exception as e:
        logger.exception(f"Failed to run startup ELO check: {e}")
        raise  # Re-raise to let decorator handle it
