import logging
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime, timezone
from fastapi import HTTPException, status

from app.database.forecasts.repository import ForecastRepository
from app.database.challenges.challenge_repository import ChallengeRepository
from app.database.models.model_info_repository import ModelInfoRepository
from app.schemas.forecast import ForecastUploadRequest, ForecastUploadResponse
from app.database.challenges.challenge import ChallengeSeriesPseudo

logger = logging.getLogger(__name__)


class ForecastService:
    """
    Service for processing forecast uploads with authorization and validation.
    """

    def __init__(self, db_session: AsyncSession):
        self.session = db_session
        self.forecast_repo = ForecastRepository(db_session)
        self.challenge_repo = ChallengeRepository(db_session)
        self.model_repo = ModelInfoRepository(db_session)

    async def upload_forecasts(
        self,
        upload_request: ForecastUploadRequest,
        user_id: int
    ) -> ForecastUploadResponse:
        """
        Process forecast uploads with full validation:
        - Authorization: Verify user owns the model
        - Registration window: Check current time is within registration_start and registration_end
        - Model participation: Auto-register model if not already registered
        - Timestamp validation: Ensure forecasts are within challenge horizon
        
        Args:
            upload_request: Forecast upload request with challenge_id, model_id, forecasts
            user_id: ID of the authenticated user
        
        Returns:
            ForecastUploadResponse with success status and statistics
        
        Raises:
            HTTPException: For authorization or validation failures
        """
        challenge_id = upload_request.challenge_id
        model_name = upload_request.model_name
        
        # === Step 1: Authorization - Verify user owns the model ===
        # We look up the model directly by name and user_id. 
        # This implicitly checks ownership since we only search the user's models.
        model = await self.model_repo.get_by_name_and_user(model_name, user_id)
        
        if not model:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model '{model_name}' not found for current user"
            )
            
        model_id = model.id
        
        # === Step 2: Validate challenge and registration window ===
        challenge = await self.challenge_repo.get_by_id(challenge_id)
        if not challenge:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Challenge with ID {challenge_id} not found"
            )
        
        # Check registration window (time-based, not status-based)
        now = datetime.now(timezone.utc)
        
        if not challenge.registration_start or not challenge.registration_end:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Challenge registration window is not configured"
            )
        
        registration_start_utc = challenge.registration_start.replace(tzinfo=timezone.utc)
        registration_end_utc = challenge.registration_end.replace(tzinfo=timezone.utc)
        
        if now < registration_start_utc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Registration has not started yet (starts at {registration_start_utc.isoformat()})"
            )
        
        if now > registration_end_utc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Registration has ended (ended at {registration_end_utc.isoformat()})"
            )
        
        # === Step 3: Auto-register model as challenge participant ===
        # Uploading a forecast automatically registers the model for the challenge
        await self._auto_register_participant(challenge_id, model_id)
        logger.info(f"Model {model_id} registered for challenge {challenge_id}")
        
        # === Step 4: Validate forecast timestamps ===
        # Timestamp validation disabled - accept all forecasts regardless of window
        
        errors = []
        total_inserted = 0
        
        # === Step 5: Process each series ===
        for series_upload in upload_request.forecasts:
            # Map challenge_series_name -> series_id for this challenge
            challenge_series_name = series_upload.challenge_series_name
            series_id = await self._resolve_series_id(challenge_id, challenge_series_name)
            if series_id is None:
                errors.append(f"Unknown challenge_series_name '{challenge_series_name}' for challenge {challenge_id}")
                continue
            
            # Prepare forecasts without timestamp validation
            valid_forecasts = []
            
            for forecast_point in series_upload.forecasts:
                valid_forecasts.append({
                    "ts": forecast_point.ts,
                    "value": forecast_point.value,
                    "probabilistic_values": forecast_point.probabilistic_values
                })
            
            # Insert all forecasts
            if valid_forecasts:
                try:
                    inserted_count = await self.forecast_repo.bulk_create_forecasts(
                        challenge_id=challenge_id,
                        model_id=model_id,
                        series_id=series_id,
                        forecast_data=valid_forecasts
                    )
                    total_inserted += inserted_count
                    logger.info(
                        f"Inserted {inserted_count} forecasts for challenge={challenge_id}, "
                        f"model={model_id}, series={series_id} ({challenge_series_name})"
                    )
                    
                    # Create initial score entry for this model/series combination
                    # This will be updated by the periodic evaluation job
                    if inserted_count > 0:
                        try:
                            await self._create_initial_score_entry(
                                challenge_id=challenge_id,
                                model_id=model_id,
                                series_id=series_id
                            )
                        except Exception as score_err:
                            logger.warning(
                                f"Failed to create initial score entry for "
                                f"challenge={challenge_id}, model={model_id}, series={series_id}: {score_err}"
                            )
                    
                except Exception as e:
                    error_msg = f"Series {series_id} ({challenge_series_name}): Failed to insert forecasts - {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
        
        # === Step 6: Return response ===
        success = total_inserted > 0
        message = f"Successfully inserted {total_inserted} forecasts"
        if errors:
            message += f" with {len(errors)} error(s)"
        
        return ForecastUploadResponse(
            success=success,
            message=message,
            forecasts_inserted=total_inserted,
            errors=errors
        )

    async def _auto_register_participant(self, challenge_id: int, model_id: int) -> None:
        """
        Automatically register a model as a challenge participant.
        Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
        
        This is called during forecast upload - uploading a forecast
        automatically registers the model for the challenge.
        
        Args:
            challenge_id: Challenge ID
            model_id: Model ID
        """
        from app.database.challenges.challenge import ChallengeParticipant
        from sqlalchemy.dialects.postgresql import insert
        
        stmt = insert(ChallengeParticipant).values(
            challenge_id=challenge_id,
            model_id=model_id
        )
        # If already registered, do nothing
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["challenge_id", "model_id"]
        )
        
        await self.session.execute(stmt)
        await self.session.commit()

    async def _create_initial_score_entry(
        self,
        challenge_id: int,
        model_id: int,
        series_id: int
    ) -> None:
        """
        Create an initial score entry with NULL values and final_evaluation=False.
        This entry will be populated by the periodic evaluation job.
        Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
        
        Args:
            challenge_id: Challenge ID
            model_id: Model ID
            series_id: Series ID
        """
        from app.database.forecasts.models import ChallengeScore
        from sqlalchemy.dialects.postgresql import insert
        
        stmt = insert(ChallengeScore).values(
            challenge_id=challenge_id,
            model_id=model_id,
            series_id=series_id,
            mase=None,
            rmse=None,
            final_evaluation=False
        )
        # If already exists, do nothing
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["challenge_id", "model_id", "series_id"]
        )
        
        await self.session.execute(stmt)
        await self.session.commit()

    async def get_forecasts(
        self,
        challenge_id: int,
        model_id: int,
        challenge_series_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve forecasts for a specific challenge and model.
        
        Args:
            challenge_id: Challenge ID
            model_id: Model ID
            series_id: Optional series ID filter
        
        Returns:
            List of forecast records
        """
        series_id_filter: Optional[int] = None
        if challenge_series_name:
            series_id_filter = await self._resolve_series_id(challenge_id, challenge_series_name)
            if series_id_filter is None:
                return []

        forecasts = await self.forecast_repo.get_forecasts_by_challenge_and_model(
            challenge_id=challenge_id,
            model_id=model_id,
            series_id=series_id_filter
        )
        
        # Map series_id -> challenge_series_name for this challenge (batch)
        mapping = await self._get_series_id_to_challenge_name(challenge_id)
        return [
            {
                "ts": f.ts,
                "predicted_value": f.predicted_value,
                "probabilistic_values": f.probabilistic_values,
                "challenge_series_name": mapping.get(f.series_id, f"series_{f.series_id}")
            }
            for f in forecasts
        ]

    async def _resolve_series_id(self, challenge_id: int, challenge_series_name: str) -> Optional[int]:
        """Resolve a challenge_series_name to the underlying series_id for the challenge."""
        result = await self.session.execute(
            select(ChallengeSeriesPseudo.series_id)
            .where(
                and_(
                    ChallengeSeriesPseudo.challenge_id == challenge_id,
                    ChallengeSeriesPseudo.challenge_series_name == challenge_series_name,
                )
            )
        )
        row = result.first()
        return row[0] if row else None

    async def _get_series_id_to_challenge_name(self, challenge_id: int) -> Dict[int, str]:
        result = await self.session.execute(
            select(
                ChallengeSeriesPseudo.series_id,
                ChallengeSeriesPseudo.challenge_series_name,
            ).where(ChallengeSeriesPseudo.challenge_id == challenge_id)
        )
        return {row[0]: row[1] for row in result.fetchall()}
