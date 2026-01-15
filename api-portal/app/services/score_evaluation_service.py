"""
Service for periodic evaluation of challenge scores.
This service runs independently every 10 minutes to calculate and update scores
for active and completed challenges.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.metrics import mean_squared_error
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.challenges.challenge_repository import ChallengeRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.forecasts.repository import ForecastRepository

logger = logging.getLogger(__name__)


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
        
        horizon_start = challenge.start_time
        horizon_end = challenge.end_time
        
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
        
        # Calculate scores for each model/series combination
        all_scores = []
        
        for model_id in participant_model_ids:
            for series_id in series_ids:
                try:
                    score_data = await self._calculate_score_for_model_series(
                        challenge_id=challenge_id,
                        model_id=model_id,
                        series_id=series_id,
                        horizon_start=horizon_start,
                        horizon_end=horizon_end
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
        horizon_start: datetime,
        horizon_end: datetime
    ) -> Dict[str, Any] | None:
        """
        Calculate MASE and RMSE for a specific model/series combination.
        """
        # Get forecasts
        forecasts = await self.forecast_repo.get_forecasts_by_challenge_and_model(
            challenge_id=challenge_id,
            model_id=model_id,
            series_id=series_id
        )
        
        forecast_count = len(forecasts) if forecasts else 0
        
        if not forecasts:
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
        
        # Get actuals
        actuals = await self.time_series_repo.get_data_by_time_range(
            series_id=series_id,
            start_time=horizon_start,
            end_time=horizon_end
        )
        
        actual_count = len(actuals) if actuals else 0
        
        if not actuals:
            logger.debug(f"No actuals yet for series {series_id}")
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
                "evaluation_status": "awaiting_actuals",
                "error_message": None,
            }
        
        # Get last context point for naive forecast baseline
        last_context_point = await self.time_series_repo.get_last_n_points(
            series_id=series_id,
            n=1,
            before_time=horizon_start
        )
        
        if not last_context_point:
            logger.warning(f"No context point for series {series_id}")
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
                "evaluation_status": "error",
                "error_message": "No context point available for naive forecast baseline",
            }
        
        # Normalize timestamps to UTC
        def normalize_to_utc(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        
        forecast_map = {normalize_to_utc(f.ts): f.predicted_value for f in forecasts}
        actual_map = {normalize_to_utc(a['ts']): a['value'] for a in actuals}
        
        # Get overlapping timestamps
        common_timestamps = sorted(set(forecast_map.keys()) & set(actual_map.keys()))
        evaluated_count = len(common_timestamps)
        
        if not common_timestamps:
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
        y_pred = np.array([forecast_map[ts] for ts in common_timestamps])
        y_true = np.array([actual_map[ts] for ts in common_timestamps])
        
        # Calculate RMSE
        rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
        
        # Calculate MASE
        naive_forecast_value = last_context_point[0]['value']
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
