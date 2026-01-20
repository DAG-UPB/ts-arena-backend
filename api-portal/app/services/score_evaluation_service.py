"""
Service for periodic evaluation of challenge scores.
This service runs independently every 10 minutes to calculate and update scores
for active and completed challenges.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.metrics import mean_squared_error
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.challenges.challenge_repository import ChallengeRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.forecasts.repository import ForecastRepository

logger = logging.getLogger(__name__)


# Mapping from timedelta frequency to resolution view
FREQUENCY_TO_RESOLUTION: Dict[timedelta, str] = {
    timedelta(minutes=15): "15min",
    timedelta(hours=1): "1h",
    timedelta(days=1): "1d",
}


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


class ScoreEvaluationService:
    """
    Service to periodically evaluate challenge scores.
    
    This service:
    1. Finds all challenges with status 'active' or 'completed' that have scores with final_evaluation=False
    2. For each challenge, calculates MASE and RMSE for all model/series combinations
    3. Updates the scores in the database
    4. When all data is complete and all forecasts are evaluated, sets final_evaluation=True
    """

    def __init__(self, db_session: AsyncSession):
        self.challenge_repo = ChallengeRepository(db_session)
        self.time_series_repo = TimeSeriesRepository(db_session)
        self.forecast_repo = ForecastRepository(db_session)
        self.db_session = db_session

    async def get_ids_needing_evaluation(self) -> List[int]:
        """
        Retrieves the IDs of all challenges that currently require evaluation.
        Useful for batch processing where each evaluation runs in its own transaction.
        """
        challenges_to_evaluate = await self.forecast_repo.get_challenges_needing_evaluation()
        return [c["challenge_id"] for c in challenges_to_evaluate]

    async def evaluate_pending_challenges(self) -> Dict[str, Any]:
        """
        Main entry point for periodic evaluation.
        Finds and evaluates all challenges that need score updates.
        
        Returns:
            Summary dict with evaluation results
        """
        challenges_to_evaluate = await self.forecast_repo.get_challenges_needing_evaluation()
        
        if not challenges_to_evaluate:
            logger.info("No challenges need evaluation at this time.")
            return {"evaluated": 0, "finalized": 0}
        
        logger.info(f"Found {len(challenges_to_evaluate)} challenge(s) needing evaluation")
        
        evaluated_count = 0
        finalized_count = 0
        
        for challenge_info in challenges_to_evaluate:
            challenge_id = challenge_info["challenge_id"]
            try:
                finalized = await self.evaluate_challenge_scores(challenge_id)
                evaluated_count += 1
                if finalized:
                    finalized_count += 1
            except Exception as e:
                logger.exception(f"Failed to evaluate challenge {challenge_id}: {e}")
        
        logger.info(f"Evaluation complete: {evaluated_count} evaluated, {finalized_count} finalized")
        return {"evaluated": evaluated_count, "finalized": finalized_count}

    async def evaluate_challenge_scores(self, challenge_id: int) -> bool:
        """
        Evaluate scores for a single challenge.
        
        Args:
            challenge_id: ID of the challenge to evaluate
            
        Returns:
            True if challenge was finalized (final_evaluation=True), False otherwise
        """
        logger.info(f"Evaluating scores for challenge {challenge_id}")
        
        # Get challenge details
        challenge = await self.challenge_repo.get_challenge_by_id(challenge_id)
        if not challenge:
            logger.warning(f"Challenge {challenge_id} not found")
            return False
        
        # Get all participants (models that submitted forecasts)
        participant_model_ids = await self.forecast_repo.get_challenge_participants(challenge_id)
        if not participant_model_ids:
            logger.info(f"No participants found for challenge {challenge_id}")
            return False
        
        # Get all series for this challenge
        series_ids = await self.forecast_repo.get_challenge_series_ids(challenge_id)
        if not series_ids:
            logger.info(f"No series found for challenge {challenge_id}")
            return False
        
        logger.info(f"Challenge {challenge_id}: {len(participant_model_ids)} participants, {len(series_ids)} series")
        
        # Determine resolution from challenge frequency
        resolution = timedelta_to_resolution(challenge.frequency)
        logger.info(f"Challenge {challenge_id}: using resolution '{resolution}' (frequency: {challenge.frequency})")
        
        # Calculate scores for each model/series combination
        all_scores = []
        
        for model_id in participant_model_ids:
            for series_id in series_ids:
                try:
                    score_data = await self._calculate_score_for_model_series(
                        challenge_id=challenge_id,
                        model_id=model_id,
                        series_id=series_id,
                        resolution=resolution
                    )
                    
                    if score_data:
                        # Skip "no_forecasts" status
                        if score_data.get("evaluation_status") == "no_forecasts":
                            continue
                        all_scores.append(score_data)
                        
                except Exception as e:
                    logger.exception(
                        f"Error calculating score for challenge {challenge_id}, "
                        f"model {model_id}, series {series_id}: {e}"
                    )
                    # Add an error entry
                    all_scores.append({
                        "challenge_id": challenge_id,
                        "model_id": model_id,
                        "series_id": series_id,
                        "mase": None,
                        "rmse": None,
                        "forecast_count": 0,
                        "actual_count": 0,
                        "evaluated_count": 0,
                        "data_coverage": 0.0,
                        "final_evaluation": False,
                        "evaluation_status": "error",
                        "error_message": str(e)[:500],
                    })
        
        # Bulk insert/update scores
        if all_scores:
            rows_affected = await self.forecast_repo.bulk_insert_scores(all_scores)
            logger.info(f"Updated {rows_affected} scores for challenge {challenge_id}")
        
        # Check if we should finalize this challenge
        should_finalize = await self._should_finalize_challenge(challenge)
        
        if should_finalize:
            await self.forecast_repo.mark_challenge_scores_final(challenge_id)
            logger.info(f"Challenge {challenge_id} scores marked as final")
            return True
        
        return False

    async def _calculate_score_for_model_series(
        self,
        challenge_id: int,
        model_id: int,
        series_id: int,
        resolution: str = "1h"
    ) -> Dict[str, Any] | None:
        """
        Calculate MASE and RMSE for a specific model/series combination.
        """
        # Get forecast stats (min_ts, max_ts, count)
        forecast_stats = await self.forecast_repo.get_forecast_stats(
            challenge_id=challenge_id,
            model_id=model_id,
            series_id=series_id
        )
        
        if not forecast_stats or forecast_stats['count'] == 0:
            logger.debug(f"No forecasts for model {model_id}, series {series_id}")
            return {
                "challenge_id": challenge_id,
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
        
        # Get last context point for naive forecast baseline
        # Try to use ChallengeSeriesPseudo first (most accurate definition of context end)
        pseudo_info = await self.challenge_repo.get_challenge_series_pseudo(challenge_id, series_id)
        naive_forecast_value = None
        
        if pseudo_info and pseudo_info.max_ts:
            context_points = await self.time_series_repo.get_data_by_time_range_by_resolution(
                series_id=series_id,
                start_time=pseudo_info.max_ts,
                end_time=pseudo_info.max_ts,
                resolution=resolution
            )
            if context_points:
                naive_forecast_value = context_points[0]['value']
        
        if naive_forecast_value is None:
            logger.warning(f"No context point for series {series_id}")
            return {
                "challenge_id": challenge_id,
                "model_id": model_id,
                "series_id": series_id,
                "mase": None,
                "rmse": None,
                "forecast_count": forecast_count,
                "actual_count": 0,
                "evaluated_count": 0,
                "data_coverage": 0.0,
                "final_evaluation": False,
                "evaluation_status": "error",
                "error_message": "No context point available for naive forecast baseline",
            }
        
        # Get aligned evaluation data directly from DB (INNER JOIN)
        # Uses the appropriate continuous aggregate view based on resolution
        evaluation_data = await self.forecast_repo.get_evaluation_data_by_resolution(
            challenge_id=challenge_id,
            model_id=model_id,
            series_id=series_id,
            resolution=resolution
        )
        
        evaluated_count = len(evaluation_data)
        actual_count = evaluated_count
        
        if evaluated_count == 0:
            logger.debug(f"No overlapping timestamps for model {model_id}, series {series_id}")
            return {
                "challenge_id": challenge_id,
                "model_id": model_id,
                "series_id": series_id,
                "mase": None,
                "rmse": None,
                "forecast_count": forecast_count,
                "actual_count": actual_count,
                "evaluated_count": 0,
                "data_coverage": 0.0,
                "final_evaluation": False,
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
        
        return {
            "challenge_id": challenge_id,
            "model_id": model_id,
            "series_id": series_id,
            "mase": mase,
            "rmse": rmse,
            "forecast_count": forecast_count,
            "actual_count": actual_count,
            "evaluated_count": evaluated_count,
            "data_coverage": data_coverage,
            "final_evaluation": False,
            "evaluation_status": evaluation_status,
            "error_message": None,
        }

    async def _should_finalize_challenge(self, challenge: Any) -> bool:
        """
        Determine if a challenge should be marked as final.
        """
        now = datetime.now(timezone.utc)
        
        end_time = challenge.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        else:
            end_time = end_time.astimezone(timezone.utc)
        
        # 1 hour buffer
        finalization_buffer = timedelta(hours=1)
        
        if now < end_time + finalization_buffer:
            return False
        
        # Check if all scores are complete
        all_complete = await self.forecast_repo.check_all_scores_complete(challenge.id)
        
        return all_complete
