from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from fastapi.responses import StreamingResponse
from app.api.dependencies import get_challenge_service, require_auth, get_export_service
from app.schemas.challenge import (
    ChallengeDefinitionResponse,
    ChallengeRoundResponse,
    ChallengeRoundResponse,
    ChallengeContextData,
    RoundStatus,
    ChallengeRoundData,
)
from app.services.challenge_service import ChallengeService
from datetime import datetime, timezone
from app.services.export_service import ExportService


router = APIRouter(prefix="/challenge", tags=["challenge"])


# ==========================================================
# Challenge Definitions (what types of challenges exist)
# ==========================================================

@router.get("/definitions", response_model=List[ChallengeDefinitionResponse])
async def get_challenge_definitions(
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Get all active challenge definitions.
    
    Returns the available challenge types that participants can join.
    Includes domain, frequency, forecast horizon, and context length info.
    """
    definitions = await challenge_service.list_definitions(active_only=True)
    return definitions


@router.get("/definitions/{definition_id}", response_model=ChallengeDefinitionResponse)
async def get_challenge_definition(
    definition_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Get a single challenge definition by ID.
    """
    definition = await challenge_service.get_definition(definition_id)
    if not definition:
        raise HTTPException(status_code=404, detail="Challenge definition not found")
    return definition


# ==========================================================
# Challenge Rounds (instances of challenges for participation)
# ==========================================================

@router.get("/rounds", response_model=List[ChallengeRoundResponse])
async def get_challenge_rounds(
    status: Optional[List[str]] = Query(
        None, 
        description="Filter by round status (registration, active, completed)"
    ),
    definition_id: Optional[int] = Query(
        None,
        description="Filter by challenge definition ID"
    ),
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Get challenge rounds.
    
    By default returns rounds with status 'registration'.
    Participants can see what rounds are open for joining.
    """
    if status is None:
        status = ["registration"]
    
    rounds = await challenge_service.list_rounds(
        statuses=status,
        definition_id=definition_id
    )
    
    # Sort by registration_start (ascending)
    return sorted(
        rounds,
        key=lambda r: r.registration_start if r.registration_start else datetime.max.replace(tzinfo=timezone.utc)
    )


@router.get("/rounds/{round_id}", response_model=ChallengeRoundResponse)
async def get_challenge_round(
    round_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Get a single challenge round by its ID.
    
    Returns detailed information about the round including timing
    and the parent challenge definition info.
    """
    round_obj = await challenge_service.get_round(round_id)
    if not round_obj:
        raise HTTPException(status_code=404, detail="Challenge round not found")
    return round_obj


@router.get(
    "/rounds/{round_id}/context-data",
    response_model=List[ChallengeContextData],
    summary="Get context data for a challenge round",
)
async def get_round_context_data(
    round_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Returns all context data points for the specified challenge round.
    
    Data is grouped by anonymized series name (challenge_series_name).
    Each series includes the data frequency and timestamped value pairs.
    """
    try:
        return await challenge_service.get_context_data_bulk(round_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get(
    "/rounds/{round_id}/data",
    response_model=ChallengeRoundData,
    summary="Get complete round data (Context + Forecasts + Actuals)",
    include_in_schema=False,
)
async def get_round_data(
    round_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Returns comprehensive data for the challenge round:
    - **Context**: Historical data available at round creation (Time Travel).
    - **Forecasts**: All submitted forecasts for the round.
    - **Actuals**: Ground truth data available at evaluation time (Time Travel).
    """
    try:
        return await challenge_service.get_round_data(round_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get(
    "/export/{year}/{month}",
    response_class=StreamingResponse,
    summary="Export monthly challenge data as Zip/Parquet",
    include_in_schema=False
)
async def export_monthly_data(
    year: int,
    month: int,
    definition_id: Optional[int] = Query(None, description="Filter by challenge definition ID"),
    current_user: dict = Depends(require_auth),
    export_service: ExportService = Depends(get_export_service)
):
    """
    Exports all challenge data for the specified month.
    
    Returns a ZIP file containing:
    - **rounds_metadata.parquet**: Metadata for all rounds in the month.
    - **context.parquet**: Historical context data (Time Travel).
    - **actuals.parquet**: Ground truth actuals (Time Travel).
    - **forecasts.parquet**: Submitted forecasts.
    """
    try:
        if month < 1 or month > 12:
            raise HTTPException(status_code=400, detail="Invalid month")
            
        zip_buffer = await export_service.export_monthly_data(year, month, definition_id)
        
        filename = f"challenge_export_{year}_{month:02d}"
        
        if definition_id:
            filename += f"_def{definition_id}"
        filename += ".zip"
        
        return StreamingResponse(
            iter([zip_buffer.getvalue()]), 
            media_type="application/zip", 
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail="Error generating export")
