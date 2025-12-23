from __future__ import annotations
import logging
import functools
from typing import Any, Dict, Callable, Awaitable
from app.database.connection import SessionLocal
from app.services.challenge_service import ChallengeService
from app.services.score_evaluation_service import ScoreEvaluationService
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
async def create_challenge_from_schedule_job(schedule_params: Dict[str, Any]) -> None:
    """
    Job function that creates a new challenge based on schedule parameters.
    It delegates the actual business logic to the ChallengeService.
    
    Args:
        schedule_params: Dictionary containing challenge creation parameters
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info(f"Starting job to create challenge from schedule with params: {schedule_params}")

    try:
        # Get scheduler from global reference
        scheduler = get_scheduler()
        
        async with SessionLocal() as session:
            challenge_service = ChallengeService(session, scheduler=scheduler)
            
            challenge = await challenge_service.create_challenge_from_schedule(schedule_params)
            
            logger.info(f"Successfully created challenge '{challenge.name}' (ID: {challenge.id})")

    except Exception as e:
        logger.exception(f"Failed to create challenge from schedule. Error: {e}")
        raise  # Re-raise to let decorator handle it


@job_error_handler
async def prepare_challenge_context_data_job(
    challenge_id: int, 
    preparation_params: Dict[str, Any]
) -> None:
    """
    Job function that prepares context data for a challenge.
    Called at registration_start to ensure fresh data.
    """
    logger = logging.getLogger("challenge-scheduler")
    logger.info(f"Starting context data preparation for challenge {challenge_id}")

    try:
        async with SessionLocal() as session:
            challenge_service = ChallengeService(session)
            
            # Execute preparation with stored parameters
            await challenge_service._execute_context_data_preparation(
                challenge_id=challenge_id,
                preparation_params=preparation_params
            )
            
            logger.info(f"Successfully prepared context data for challenge {challenge_id}")

    except Exception as e:
        logger.exception(f"Failed to prepare context data for challenge {challenge_id}: {e}")
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
        async with SessionLocal() as session:
            score_service = ScoreEvaluationService(session)
            result = await score_service.evaluate_pending_challenges()
            
            logger.info(
                f"Periodic evaluation complete: "
                f"{result['evaluated']} challenges evaluated, "
                f"{result['finalized']} finalized"
            )
    
    except Exception as e:
        logger.exception(f"Failed to run periodic challenge scores evaluation: {e}")
        raise  # Re-raise to let decorator handle it
