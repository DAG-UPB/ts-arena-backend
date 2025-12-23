from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from app.api.dependencies import get_challenge_service, require_auth
from app.schemas.challenge import Challenge, ChallengeContextData
from app.services.challenge_service import ChallengeService
from datetime import datetime, timezone


router = APIRouter(prefix="/challenge", tags=["challenge"])


@router.get("/{challenge_id}", response_model=Challenge)
async def get_challenge(
    challenge_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """Get a single challenge by its ID."""
    challenge = await challenge_service.get_challenge(challenge_id)
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    return challenge

@router.get("/", response_model=List[Challenge])
async def get_all_challenges(
    status: Optional[List[str]] = Query(None),
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Get all challenges. 
    By default (if status is not provided), returns challenges with status 'registration' or 'announced'.
    """
    if status is None:
        status = ["registration", "announced"]
        
    challenges = await challenge_service.list_challenges(statuses=status)
    
    # Sort by registration_start (ascending)
    # Handle potentially None registration_start by treating it as max date (end of list)
    return sorted(
        challenges, 
        key=lambda c: c.registration_start if c.registration_start else datetime.max.replace(tzinfo=timezone.utc)
    )


@router.get(
    "/{challenge_id}/context-data",
    response_model=List[ChallengeContextData],
    summary="Get context data points for a challenge",
)
async def get_context_data_bulk(
    challenge_id: int,
    current_user: dict = Depends(require_auth),
    challenge_service: ChallengeService = Depends(get_challenge_service)
):
    """
    Returns all context data points stored in the database
    for the specified challenge.
    """
    try:
        return await challenge_service.get_context_data_bulk(challenge_id)
    except ValueError as e:
        # Optional: if your service method raises an error for an invalid ID
        raise HTTPException(status_code=404, detail=str(e))
