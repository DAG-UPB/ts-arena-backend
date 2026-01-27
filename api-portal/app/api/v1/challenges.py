from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from app.api.dependencies import get_challenge_service, require_auth
from app.schemas.challenge import (
    ChallengeDefinitionResponse,
    ChallengeRoundResponse,
    ChallengeRoundFull,
    ChallengeContextData,
    RoundStatus,
)
from app.services.challenge_service import ChallengeService
from datetime import datetime, timezone


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
        description="Filter by round status (announced, registration, active, completed)"
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
    
    By default returns rounds with status 'announced' or 'registration'.
    Participants can see what rounds are open for joining.
    """
    if status is None:
        status = ["announced", "registration"]
    
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


# ==========================================================
# Legacy endpoints for backwards compatibility
# ==========================================================

@router.get("/{challenge_id}", response_model=ChallengeRoundFull, deprecated=True)
async def get_challenge(
    challenge_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    DEPRECATED: Use /rounds/{round_id} instead.
    
    Get a single challenge (round) by its ID.
    """
    round_obj = await challenge_service.get_round(challenge_id)
    if not round_obj:
        raise HTTPException(status_code=404, detail="Challenge not found")
    return round_obj


@router.get("/", response_model=List[ChallengeRoundFull], deprecated=True)
async def get_all_challenges(
    status: Optional[List[str]] = Query(None),
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    DEPRECATED: Use /rounds instead.
    
    Get all challenges (rounds).
    """
    if status is None:
        status = ["registration", "announced"]
    
    rounds = await challenge_service.list_rounds(statuses=status)
    
    return sorted(
        rounds,
        key=lambda c: c.registration_start if c.registration_start else datetime.max.replace(tzinfo=timezone.utc)
    )


@router.get(
    "/{challenge_id}/context-data",
    response_model=List[ChallengeContextData],
    deprecated=True
)
async def get_context_data_bulk(
    challenge_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    DEPRECATED: Use /rounds/{round_id}/context-data instead.
    """
    try:
        return await challenge_service.get_context_data_bulk(challenge_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
