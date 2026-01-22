from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from app.core.dependencies import get_api_key
from app.core.utils import parse_comma_separated
from app.database.connection import get_db_connection
from app.repositories.challenge_repository import ChallengeRepository
from app.schemas.challenge import (
    ChallengeSchema,
    ChallengeMetaSchema,
    ChallengeSeriesSchema,
    ChallengeMetadataSchema,
    TimeSeriesDataSchema,
    TimeSeriesDataPoint
)

router = APIRouter(prefix="/api/v1/challenges", tags=["Challenges"])


@router.get("/metadata", response_model=ChallengeMetadataSchema)
async def get_challenge_metadata(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Available filter options for challenge listing.
    
    Returns all unique values for:
    - **frequencies**: Available time series frequencies (ISO 8601)
    - **horizons**: Available forecast horizons (ISO 8601)
    - **domains**: Available domains
    - **categories**: Available categories
    - **subcategories**: Available subcategories
    - **statuses**: Available challenge statuses
    
    **Use Case:**
    Frontend can pre-fill filter dropdowns/multi-selects before
    actual challenge data is fetched.
    
    **Performance:**
    Lightweight Query - only unique values, no complete challenge data.
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = ChallengeRepository(conn)
    metadata = repo.get_challenge_metadata()
    return metadata


@router.get("", response_model=List[ChallengeSchema])
async def list_challenges(
    # Existing filters
    status: Optional[str] = Query(
        None, 
        description="Comma-separated status values (e.g. 'active,completed')"
    ),
    from_date: Optional[datetime] = Query(
        None, 
        alias="from",
        description="Challenges with end_time >= from"
    ),
    to_date: Optional[datetime] = Query(
        None, 
        alias="to",
        description="Challenges with end_time <= to"
    ),
    # NEW: Filter parameters
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
    # Dependencies
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all challenges with optional filters.
    
    **Filter Parameters:**
    - `status`: Comma-separated status values (e.g. "active,completed")
    - `from`: Challenges with end_time >= from
    - `to`: Challenges with end_time <= to
    - `frequency`: Frequencies in ISO 8601 (e.g. "PT1H" for 1 hour, "P1D" for 1 day)
    - `domain`: Domains (e.g. "Energy,Finance")
    - `category`: Categories (e.g. "Electricity")
    - `subcategory`: Subcategories (e.g. "Load,Generation")
    - `horizon`: Forecast horizons in ISO 8601 (e.g. "PT6H,P1D")
    
    **ISO 8601 Duration Examples:**
    - `PT15M` = 15 minutes
    - `PT1H` = 1 hour
    - `PT6H` = 6 hours
    - `P1D` = 1 day
    - `P7D` = 7 days
    
    **Headers:**
    - X-API-Key: Valid API key
    """
    # Parse comma-separated parameters
    status_list = parse_comma_separated(status)
    frequency_list = parse_comma_separated(frequency)
    domain_list = parse_comma_separated(domain)
    category_list = parse_comma_separated(category)
    subcategory_list = parse_comma_separated(subcategory)
    horizon_list = parse_comma_separated(horizon)
    
    repo = ChallengeRepository(conn)
    challenges = repo.list_challenges(
        status=status_list,
        from_date=from_date,
        to_date=to_date,
        domains=domain_list,
        categories=category_list,
        subcategories=subcategory_list,
        frequencies=frequency_list,
        horizons=horizon_list
    )
    return challenges


@router.get("/{challenge_id}", response_model=ChallengeMetaSchema)
async def get_challenge_meta(
    challenge_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Metadata for a challenge.
    
    **Headers:**
    - X-API-Key: Valid API key
    """
    repo = ChallengeRepository(conn)
    meta = repo.get_challenge_meta(challenge_id)
    
    if not meta:
        raise HTTPException(status_code=404, detail="Challenge not found")
    
    return meta


@router.get("/{challenge_id}/series", response_model=List[ChallengeSeriesSchema])
async def get_challenge_series(
    challenge_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Time series for a challenge.
    
    **Headers:**
    - X-API-Key: Valid API key
    """
    repo = ChallengeRepository(conn)
    series = repo.get_challenge_series(challenge_id)
    return series


@router.get("/{challenge_id}/series/{series_id}/data", response_model=TimeSeriesDataSchema)
async def get_series_data(
    challenge_id: int,
    series_id: int,
    start_time: datetime = Query(..., description="Start timestamp"),
    end_time: datetime = Query(..., description="End timestamp"),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Time series data for a series.
    
    **Headers:**
    - X-API-Key: Valid API key
    """
    repo = ChallengeRepository(conn)
    data = repo.get_challenge_data_for_series(challenge_id, series_id, start_time, end_time)
    
    return {"data": data}
