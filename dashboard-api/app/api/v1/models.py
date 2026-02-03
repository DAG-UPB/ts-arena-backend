from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional

from app.core.dependencies import get_api_key
from app.database.connection import get_db_connection
from app.repositories.model_repository import ModelRepository
from app.repositories.forecast_repository import ForecastRepository
from app.schemas.common import (
    RankingResponseSchema,
    ModelRankingSchema,
    ModelRankingsResponseSchema
)
from app.schemas.model import ModelSchema, ModelDetailSchema, ModelSeriesByDefinitionSchema
from app.schemas.forecast import ModelSeriesForecastsAcrossRoundsSchema

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
    definition_names: Optional[str] = Query(
        None,
        description="Comma-separated list of definition names (e.g., 'Day-Ahead Power,Week-Ahead Power')",
        example="Day-Ahead Power,Week-Ahead Power"
    ),
    definition_ids: Optional[str] = Query(
        None,
        description="Comma-separated list of definition IDs (e.g., '1,2,3')",
        example="1,2"
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
    - **Definition Names**: Filter by challenge definition names (e.g., Day-Ahead Power)
    - **Definition IDs**: Filter by challenge definition IDs (e.g., 1, 2, 3)
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
    definition_names_list = [d.strip() for d in definition_names.split(',')] if definition_names else None
    definition_ids_list = [int(d.strip()) for d in definition_ids.split(',')] if definition_ids else None
    
    # Get filtered rankings
    repo = ModelRepository(conn)
    rankings = repo.get_filtered_rankings(
        time_range=time_range,
        domains=domains_list,
        categories=categories_list,
        subcategories=subcategories_list,
        frequencies=frequencies_list,
        horizons=horizons_list,
        definition_names=definition_names_list,
        definition_ids=definition_ids_list,
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
    if definition_names:
        filters_applied['definition_names'] = definition_names
    if definition_ids_list:
        filters_applied['definition_ids'] = definition_ids_list
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
      "time_ranges": ["7d", "30d", "90d", "365d"],
      "definition_names": ["Day-Ahead Power", "Week-Ahead Power"],
      "definition_ids": [{"id": 1, "name": "Day-Ahead Power"}, {"id": 2, "name": "Week-Ahead Power"}]
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


@router.get("/models/{model_id}/rankings", response_model=ModelRankingsResponseSchema)
async def get_model_rankings(
    model_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get rankings for a model across all definitions it participated in.
    
    Returns rankings for 7 days, 30 days, 90 days, and 365 days time ranges.
    For each definition and time range, provides:
    - Rank among all models in that definition
    - Total number of models
    - Rounds participated
    - Average MASE score
    - Standard deviation, min, and max MASE scores
    
    **Response Example:**
    ```json
    {
      "model_id": 123,
      "model_name": "ExampleModel",
      "definition_rankings": [
        {
          "definition_id": 1,
          "definition_name": "Day-Ahead Power Forecast",
          "rankings_7d": {
            "rank": 5,
            "total_models": 20,
            "rounds_participated": 7,
            "avg_mase": 0.85,
            "stddev_mase": 0.12,
            "min_mase": 0.65,
            "max_mase": 1.05
          },
          "rankings_30d": { ... },
          "rankings_90d": { ... },
          "rankings_365d": { ... }
        }
      ]
    }
    ```
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Only includes definitions where the model has participated
    - Rankings are null for time ranges where no data exists
    - Rankings are based on average MASE score (lower is better)
    - Only valid MASE scores are considered (NaN, Infinity filtered out)
    """
    repo = ModelRepository(conn)
    result = repo.get_model_rankings_by_definition(model_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Model not found")
    
    return result


@router.get(
    "/models/{model_id}/definitions/{definition_id}/series/{series_id}/forecasts",
    response_model=ModelSeriesForecastsAcrossRoundsSchema
)
async def get_model_series_forecasts_across_rounds(
    model_id: int,
    definition_id: int,
    series_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get forecasts for one model and one series across all rounds of a definition.
    
    This endpoint returns detailed information about a model's forecasts for a specific
    series across all rounds belonging to a particular challenge definition. It clearly
    distinguishes between two important cases:
    
    1. **Series not part of the round**: The series exists during the definition timespan
       but was not included in that specific round's challenge (series_in_round=False)
    
    2. **Series part of round but no forecast**: The series was included in the round,
       but the model did not submit a forecast for it (series_in_round=True, forecast_exists=False)
    
    3. **Forecast submitted**: The series was in the round and the model submitted forecasts
       (series_in_round=True, forecast_exists=True, with forecast data points)
    
    **Path Parameters:**
    - model_id: ID of the model
    - definition_id: ID of the challenge definition
    - series_id: ID of the time series
    
    **Response Structure:**
    ```json
    {
      "model_id": 123,
      "model_readable_id": "example-model",
      "model_name": "Example Model",
      "definition_id": 1,
      "definition_name": "Day-Ahead Power Forecast",
      "series_id": 456,
      "series_name": "Power Load - Region A",
      "rounds": [
        {
          "round_id": 1001,
          "round_name": "Day-Ahead Power - 2024-01-01",
          "start_time": "2024-01-01T00:00:00Z",
          "end_time": "2024-01-02T00:00:00Z",
          "series_in_round": true,
          "forecast_exists": true,
          "forecasts": [
            {
              "ts": "2024-01-02T00:00:00Z",
              "y": 1234.5,
              "ci": {"0.025": 1200.0, "0.975": 1270.0}
            }
          ]
        },
        {
          "round_id": 1002,
          "round_name": "Day-Ahead Power - 2024-01-02",
          "start_time": "2024-01-02T00:00:00Z",
          "end_time": "2024-01-03T00:00:00Z",
          "series_in_round": true,
          "forecast_exists": false,
          "forecasts": null
        },
        {
          "round_id": 1003,
          "round_name": "Day-Ahead Power - 2024-01-03",
          "start_time": "2024-01-03T00:00:00Z",
          "end_time": "2024-01-04T00:00:00Z",
          "series_in_round": false,
          "forecast_exists": false,
          "forecasts": null
        }
      ]
    }
    ```
    
    **Use Cases:**
    - Track model performance over time for a specific series
    - Identify missing forecasts (participation gaps)
    - Distinguish between series not being in scope vs missing forecasts
    - Analyze model consistency across rounds
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Returns 404 if model, definition, or series not found
    - Rounds are ordered by start_time (ascending)
    - All rounds of the definition are included, regardless of forecast submission
    - Confidence intervals (ci) are optional and depend on whether the model provided them
    """
    repo = ForecastRepository(conn)
    result = repo.get_model_series_forecasts_across_rounds(model_id, definition_id, series_id)
    
    if not result:
        raise HTTPException(
            status_code=404, 
            detail="Model, definition, or series not found"
        )
    
    return result


@router.get(
    "/models/{model_id}/series-by-definition",
    response_model=ModelSeriesByDefinitionSchema
)
async def get_model_series_by_definition(
    model_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get all series grouped by definition for a specific model.
    
    This endpoint returns all time series that a model has forecasted for,
    organized by challenge definition. If a series appears in multiple definitions,
    it will be listed under each definition separately.
    
    **Path Parameters:**
    - model_id: ID of the model
    
    **Response Structure:**
    ```json
    {
      "model_id": 123,
      "model_readable_id": "example-model",
      "model_name": "Example Model",
      "definitions": [
        {
          "definition_id": 1,
          "definition_name": "Day-Ahead Power Forecast",
          "series": [
            {
              "series_id": 456,
              "series_name": "Power Load - Region A",
              "series_unique_id": "power_load_region_a",
              "forecast_count": 240,
              "rounds_participated": 10
            },
            {
              "series_id": 457,
              "series_name": "Power Load - Region B",
              "series_unique_id": "power_load_region_b",
              "forecast_count": 180,
              "rounds_participated": 8
            }
          ]
        },
        {
          "definition_id": 2,
          "definition_name": "Week-Ahead Power Forecast",
          "series": [
            {
              "series_id": 456,
              "series_name": "Power Load - Region A",
              "series_unique_id": "power_load_region_a",
              "forecast_count": 48,
              "rounds_participated": 4
            }
          ]
        }
      ]
    }
    ```
    
    **Use Cases:**
    - Get an overview of all series a model has forecasted
    - Understand model's participation across different challenge definitions
    - Identify series that span multiple definitions
    - Analyze forecast activity and round participation per series
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Returns 404 if model not found
    - Series appearing in multiple definitions are listed separately under each
    - Definitions are ordered alphabetically by name
    - Series within each definition are ordered alphabetically by name
    - forecast_count includes all forecast data points across all rounds
    - rounds_participated shows distinct rounds where forecasts were submitted
    """
    repo = ModelRepository(conn)
    result = repo.get_model_series_by_definition(model_id)
    
    if not result:
        raise HTTPException(
            status_code=404,
            detail="Model not found"
        )
    
    return result

