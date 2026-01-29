from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional

from app.core.dependencies import get_api_key
from app.database.connection import get_db_connection
from app.repositories.model_repository import ModelRepository
from app.repositories.forecast_repository import ForecastRepository
from app.schemas.common import RankingResponseSchema, ModelRankingSchema
from app.schemas.model import ModelSchema, ModelDetailSchema

router = APIRouter(prefix="/api/v1", tags=["Models"])


@router.get("/challenges/{challenge_id}/models", response_model=List[ModelSchema])
async def list_models_for_challenge(
    challenge_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List of all models for a challenge.
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = ModelRepository(conn)
    models = repo.list_models_for_challenge(challenge_id)
    return models


@router.get("/models/rankings")
async def get_filtered_rankings(
    time_range: Optional[str] = Query(
        None,
        description="Time range: 7d, 30d, 90d, 365d",
        example="30d"
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
    frequency: Optional[str] = Query(
        None,
        description="Comma-separated list of frequencies in ISO 8601 format (e.g., 'PT1H,P1D')",
        example="PT1H,P1D"
    ),
    horizon: Optional[str] = Query(
        None,
        description="Comma-separated list of horizons in ISO 8601 format (e.g., 'PT6H,P1D')",
        example="PT6H,P1D"
    ),
    definition_id: Optional[int] = Query(
        None,
        description="Filter by definition ID (e.g., 'Day-Ahead Power')",
        example=1
    ),
    min_rounds: int = Query(
        1,
        ge=1,
        description="Minimum number of rounds a model must have participated in",
        example=3
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of results",
        example=100
    ),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Enhanced Model Rankings with Multiple Filter Dimensions.
    
    This endpoint allows filtering model rankings by multiple dimensions:
    - **Time Range**: Filter by challenge end time (7d, 30d, 90d, 365d)
    - **Domain**: Filter by one or more domains (e.g., Energy, Finance)
    - **Category**: Filter by one or more categories (e.g., Electricity, Gas)
    - **Subcategory**: Filter by one or more subcategories (e.g., Load, Generation)
    - **Frequency**: Filter by data frequency in ISO 8601 format (e.g., PT1H for 1 hour)
    - **Horizon**: Filter by forecast horizon in ISO 8601 format (e.g., P1D for 1 day)
    - **Definition**: Filter by challenge definition (e.g., Day-Ahead Power)
    - **Min Rounds**: Show only models that participated in at least N rounds
    
    **Filter Format:**
    - Multiple values: Comma-separated (e.g., `domain=Energy,Finance`)
    - ISO 8601 Duration Examples:
      - `PT15M` = 15 minutes
      - `PT1H` = 1 hour
      - `PT6H` = 6 hours
      - `P1D` = 1 day
      - `P7D` = 7 days
    
    **Response:**
    ```json
    {
      "rankings": [
        {
          "model_name": "ExampleModel",
          "challenges_participated": 10,
          "avg_mase": 0.85,
          "stddev_mase": 0.12,
          "min_mase": 0.65,
          "max_mase": 1.05,
          "domains_covered": ["Energy", "Finance"],
          "categories_covered": ["Electricity"],
          "subcategories_covered": ["Load"],
          "frequencies_covered": ["PT1H", "P1D"],
          "horizons_covered": ["PT6H", "P1D"]
        }
      ],
      "filters_applied": {
        "time_range": "30d",
        "domain": ["Energy", "Finance"],
        "min_rounds": 3
      }
    }
    ```
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Only models with valid MASE scores are included (NULL, NaN, Infinity filtered out)
    - Rankings are sorted by avg_mase (ascending), then challenges_participated (descending)
    - Empty filters mean no filtering on that dimension (show all)
    """
    # Parse comma-separated parameters
    domains_list = [d.strip() for d in domain.split(',')] if domain else None
    categories_list = [c.strip() for c in category.split(',')] if category else None
    subcategories_list = [s.strip() for s in subcategory.split(',')] if subcategory else None
    frequencies_list = [f.strip() for f in frequency.split(',')] if frequency else None
    horizons_list = [h.strip() for h in horizon.split(',')] if horizon else None
    
    # Get filtered rankings
    repo = ModelRepository(conn)
    rankings = repo.get_filtered_rankings(
        time_range=time_range,
        domains=domains_list,
        categories=categories_list,
        subcategories=subcategories_list,
        frequencies=frequencies_list,
        horizons=horizons_list,
        definition_id=definition_id,
        min_rounds=min_rounds,
        limit=limit
    )
    
    # Build filters_applied dict for transparency
    filters_applied = {}
    if time_range:
        filters_applied['time_range'] = time_range
    if domains_list:
        filters_applied['domain'] = domains_list
    if categories_list:
        filters_applied['category'] = categories_list
    if subcategories_list:
        filters_applied['subcategory'] = subcategories_list
    if frequencies_list:
        filters_applied['frequency'] = frequencies_list
    if horizons_list:
        filters_applied['horizon'] = horizons_list
    if definition_id:
        filters_applied['definition_id'] = definition_id
    if min_rounds > 1:
        filters_applied['min_rounds'] = min_rounds
    if limit != 100:
        filters_applied['limit'] = limit
    
    return {
        "rankings": rankings,
        "filters_applied": filters_applied
    }


@router.get("/models/ranking-filters")
async def get_ranking_filters(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get Available Filter Options for Model Rankings.
    
    This endpoint returns all available filter values that can be used with
    the `/models/rankings` endpoint. This allows for dynamic UI filter construction.
    
    **Returns:**
    ```json
    {
      "domains": ["Energy", "Finance", "Weather"],
      "categories": ["Electricity", "Gas", "Stock Prices"],
      "subcategories": ["Load", "Generation", "Wind"],
      "frequencies": ["PT15M", "PT1H", "P1D"],
      "horizons": ["PT1H", "PT6H", "P1D", "P7D"],
      "time_ranges": ["7d", "30d", "90d", "365d"]
    }
    ```
    
    **Notes:**
    - Only values present in the database are returned
    - Frequencies and horizons are returned in ISO 8601 format
    - Lists are sorted alphabetically (except time_ranges which are in logical order)
    - Empty lists mean no data available for that dimension
    
    **Headers:**
    - X-API-Key: Valid API key required
    """
    repo = ModelRepository(conn)
    filter_options = repo.get_available_filter_options()
    
    return filter_options


@router.get("/models/{model_id}", response_model=ModelDetailSchema)
async def get_model_details(
    model_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get detailed information about a model.
    """
    repo = ModelRepository(conn)
    model = repo.get_model_details(model_id)
    
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
        
    return model


@router.get("/models/{model_id}/series/{series_id}/forecasts")
async def get_model_series_forecasts(
    model_id: int,
    series_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get all forecasts made by this model for a specific series, along with Ground Truth.
    """
    repo = ForecastRepository(conn)
    data = repo.get_model_series_long_term_forecasts(model_id, series_id)
    return data
