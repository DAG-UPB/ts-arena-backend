from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from app.core.dependencies import get_api_key
from app.core.utils import parse_comma_separated
from app.database.connection import get_db_connection
from app.repositories.challenge_repository import ChallengeRepository
from app.repositories.round_repository import RoundRepository
from app.schemas.challenge import (
    ChallengeRoundSchema,
    ChallengeSeriesSchema,
    ChallengeMetadataSchema,
    TimeSeriesDataSchema
)
from app.schemas.round import (
    RoundMetaSchema,
    RoundModelListSchema
)

router = APIRouter(prefix="/api/v1/rounds", tags=["Rounds"])


@router.get("/metadata", response_model=ChallengeMetadataSchema)
async def get_rounds_metadata(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Available filter options for round listing.
    """
    repo = ChallengeRepository(conn)
    metadata = repo.get_challenge_metadata()
    return metadata


@router.get("", response_model=List[ChallengeRoundSchema])
async def list_rounds(
    # Existing filters
    status: Optional[str] = Query(
        None, 
        description="Comma-separated status values (e.g. 'active,completed')"
    ),
    from_date: Optional[datetime] = Query(
        None, 
        alias="from",
        description="Rounds with end_time >= from"
    ),
    to_date: Optional[datetime] = Query(
        None, 
        alias="to",
        description="Rounds with end_time <= to"
    ),
    # Filter parameters
    frequency: Optional[str] = Query(
        None,
        description="Comma-separated frequencies in ISO 8601 format (e.g., 'PT1H,P1D')",
        example="PT1H,P1D"
    ),
    domain: Optional[str] = Query(
        None,
        description="Comma-separated list of domains (e.g., 'Energy,Finance')",
        example="Energy,Finance"
    ),
    category: Optional[str] = Query(
        None,
        description="Comma-separated list of categories (e.g., 'Electricity,Gas')",
        example="Electricity"
    ),
    subcategory: Optional[str] = Query(
        None,
        description="Comma-separated list of subcategories (e.g., 'Load,Generation')",
        example="Load,Generation"
    ),
    horizon: Optional[str] = Query(
        None,
        description="Comma-separated list of horizons in ISO 8601 format (e.g., 'PT6H,P1D')",
        example="PT6H,P1D"
    ),
    definition_id: Optional[int] = Query(
        None,
        description="Filter by definition ID"
    ),
    # Dependencies
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all challenge rounds with optional filters.
    """
    # Parse comma-separated parameters
    status_list = parse_comma_separated(status)
    frequency_list = parse_comma_separated(frequency)
    domain_list = parse_comma_separated(domain)
    category_list = parse_comma_separated(category)
    subcategory_list = parse_comma_separated(subcategory)
    horizon_list = parse_comma_separated(horizon)
    
    repo = ChallengeRepository(conn)
    rounds = repo.list_rounds(
        status=status_list,
        from_date=from_date,
        to_date=to_date,
        domains=domain_list,
        categories=category_list,
        subcategories=subcategory_list,
        frequencies=frequency_list,
        horizons=horizon_list,
        definition_id=definition_id
    )
    return rounds


@router.get("/{round_id}", response_model=RoundMetaSchema)
async def get_round_meta(
    round_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Metadata for a challenge round.
    """
    repo = RoundRepository(conn)
    meta = repo.get_round_meta(round_id)
    
    if not meta:
        raise HTTPException(status_code=404, detail="Challenge round not found")
    
    return meta


@router.get("/{round_id}/series", response_model=List[ChallengeSeriesSchema])
async def get_round_series(
    round_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Time series for a challenge round.
    """
    repo = ChallengeRepository(conn)
    series = repo.get_challenge_series(round_id)
    return series


@router.get("/{round_id}/leaderboard", response_model=List)
async def get_round_leaderboard(
    round_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get leaderboard (rankings) for a specific round.
    """
    repo = ChallengeRepository(conn)
    leaderboard = repo.get_round_leaderboard(round_id)
    return leaderboard


@router.get("/{round_id}/models/{model_id}", response_model=dict)
async def get_model_round_performance(
    round_id: int,
    model_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get metrics for a specific model in a round.
    """
    repo = ChallengeRepository(conn)
    perf = repo.get_model_performance_in_round(round_id, model_id)
    
    if not perf:
        raise HTTPException(status_code=404, detail="Model performance not found for this round")
        
    return perf


@router.get("/{round_id}/series/{series_id}/data", response_model=TimeSeriesDataSchema)
async def get_series_data(
    round_id: int,
    series_id: int,
    start_time: datetime = Query(..., description="Start timestamp"),
    end_time: datetime = Query(..., description="End timestamp"),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Time series data for a series in a round.
    """
    repo = ChallengeRepository(conn)
    data = repo.get_challenge_data_for_series(round_id, series_id, start_time, end_time)
    
    return {"data": data}

@router.get("/rounds/{round_id}/models", response_model=List[RoundModelListSchema])
async def list_models_for_round(
    round_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List of all models for a round.
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = RoundRepository(conn)
    models = repo.list_models_for_round(round_id)
    return models
