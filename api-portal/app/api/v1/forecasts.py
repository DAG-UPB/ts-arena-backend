# app/api/v1/forecasts.py
"""Forecast API endpoints for uploading and retrieving forecasts."""
from fastapi import APIRouter, Depends, HTTPException, status, Security
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.api.dependencies import get_db, require_user_auth, require_auth
from app.database.auth.api_key_repository import APIKeyRepository
from app.services.forecast_service import ForecastService
from app.schemas.forecast import (
    ForecastUploadRequest,
    ForecastUploadResponse,
    ForecastListResponse,
    ForecastResponse
)

router = APIRouter(prefix="/forecasts", tags=["forecasts"])


async def get_forecast_service(db: AsyncSession = Depends(get_db)) -> ForecastService:
    """Dependency to get ForecastService instance."""
    return ForecastService(db)

from app.services.model_info_service import ModelInfoService
async def get_model_info_service(db: AsyncSession = Depends(get_db)) -> ModelInfoService:
    return ModelInfoService(db)


@router.post(
    "/upload",
    response_model=ForecastUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload forecasts for a challenge round",
    description=(
        "Upload forecasts for a challenge round. Requirements:\n"
        "- Valid API key in X-API-Key header\n"
        "- User must own the model\n"
        "- Current time must be within challenge registration window (registration_start to registration_end)\n"
        "- Forecast timestamps must be within challenge horizon\n"
        "- Use challenge_series_name identifiers from the challenge context instead of raw series_id\n\n"
        "**Auto-Registration**: Uploading a forecast automatically registers "
        "your model as a participant in the challenge round. No pre-registration required!"
    )
)
async def upload_forecasts(
    upload_request: ForecastUploadRequest,
    current_user: dict = Depends(require_user_auth),
    service: ForecastService = Depends(get_forecast_service)
) -> ForecastUploadResponse:
    """
    Upload forecasts for a challenge round.
    
    The endpoint validates:
    1. User authorization (owns the model)
    2. Registration window (current time within registration_start and registration_end)
    
    **Auto-Registration**: The model is automatically registered as a challenge
    participant when uploading forecasts. No separate registration step needed.
    
    Returns summary of inserted forecasts and any errors.
    """
    user_id = current_user.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user authentication"
        )
    
    return await service.upload_forecasts(
        upload_request=upload_request,
        user_id=user_id
    )


@router.get(
    "/{round_id}/{model_id}",
    response_model=ForecastListResponse,
    summary="Get forecasts for a challenge round and model",
    description=(
        "Retrieve all forecasts for a specific model in a challenge round. "
        "Optionally filter by challenge_series_name."
    )
)
async def get_forecasts(
    round_id: int,
    model_id: int,
    challenge_series_name: Optional[str] = None,
    current_user: dict = Depends(require_auth),
    service: ForecastService = Depends(get_forecast_service),
    model_service: ModelInfoService = Depends(get_model_info_service)
) -> ForecastListResponse:
    """
    Retrieve forecasts for a specific challenge round and model.
    Optionally filter by series_id.
    
    - **Internal Service**: Can see all forecasts.
    - **Regular User**: Can ONLY see forecasts for their own models.
    """
    # Verify ownership
    if current_user.get("role") != "internal":
        # Check if model belongs to user
        model = await model_service.get_model(model_id)
        if not model:
             raise HTTPException(status_code=404, detail="Model not found")
             
        if model.user_id != current_user["user_id"]:
             raise HTTPException(status_code=403, detail="Not authorized to view these forecasts")

    forecasts_data = await service.get_forecasts(
        round_id=round_id,
        model_id=model_id,
        challenge_series_name=challenge_series_name
    )
    
    forecasts = [
        ForecastResponse(
            ts=f["ts"],
            predicted_value=f["predicted_value"],
            probabilistic_values=f.get("probabilistic_values"),
            challenge_series_name=f["challenge_series_name"]
        )
        for f in forecasts_data
    ]
    
    return ForecastListResponse(
        round_id=round_id,
        model_id=model_id,
        forecasts=forecasts
    )


from app.api.dependencies import get_challenge_service
from app.services.challenge_service import ChallengeService


@router.get(
    "/naive-template/{round_id}",
    response_model=ForecastUploadRequest,
    summary="Get a naive forecast template for easy upload",
    description=(
        "Returns a ready-to-upload naive forecast template matching the ForecastUploadRequest structure. "
        "Uses persistence (last context value) as the prediction method. "
        "Timestamps are correctly calculated based on round horizon and frequency. "
        "The template includes model_name='Naive' and can be posted to /forecasts/upload if a corresponding model is registered."
    )
)
async def get_naive_forecast_template(
    round_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
) -> ForecastUploadRequest:
    """
    Returns a naive forecast template ready for direct upload.
    
    The template uses the last known context value as the prediction 
    for all forecast timestamps (persistence/naive method).
    
    Response structure matches ForecastUploadRequest exactly:
    - round_id: The challenge round ID
    - model_name: "Naive" (persistence baseline)
    - forecasts: List of series with their forecast data points
    """
    try:
        template = await challenge_service.generate_naive_forecast_template(round_id)
        return ForecastUploadRequest(**template)
    except ValueError as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=str(e))
