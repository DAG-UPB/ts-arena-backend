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


@router.get("/models/rankings")
async def get_filtered_rankings(
    definition_id: Optional[int] = Query(
        None,
        description="Filter by challenge definition ID (scope_type='definition')",
        example=1
    ),
    frequency_horizon: Optional[str] = Query(
        None,
        description="Filter by frequency::horizon combination (scope_type='frequency_horizon'), e.g., '00:15:00::1 day'",
        example="00:15:00::1 day"
    ),
    calculation_date: Optional[str] = Query(
        None,
        description="Filter by calculation date (YYYY-MM-DD). Defaults to today if not provided.",
        example="2025-12-31"
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
    Model Rankings with Scope-Based Filtering.
    
    This endpoint returns model rankings based on ELO scores from monthly snapshots 
    and the latest daily rankings.
    
    **Scope Types:**
    - **Global** (default): No filter parameters - returns global ELO rankings
    - **Definition**: Filter by `definition_id` - returns definition-specific ELO rankings
    - **Frequency/Horizon**: Filter by `frequency_horizon` - returns frequency/horizon-specific ELO rankings
    
    **Important:** Only ONE scope filter can be applied at a time. Providing both
    `definition_id` and `frequency_horizon` will result in an error.
    
    **Frequency/Horizon Format:**
    - Format: `frequency::horizon` (e.g., `00:15:00::1 day`)
    - Frequency examples: `00:15:00` (15 min), `01:00:00` (1 hour)
    - Horizon examples: `1 day`, `7 days`
    
    **Calculation Date:**
    - If not provided: returns the latest rankings (most recent calculation)
    - If provided (format: YYYY-MM-DD): returns rankings for that specific date
    - Use `/models/ranking-filters` endpoint to see available calculation dates
    - Only month-end dates and the latest date are stored
    
    **Response:**
    ```json
    {
      "rankings": [
        {
          "model_id": 1,
          "model_name": "ExampleModel",
          "architecture": "decoder-only",
          "model_size": 200,
          "organization_name": "UPB",
          "elo_rating_median": 1337.5,
          "elo_ci_lower": 1300.2,
          "elo_ci_upper": 1374.8,
          "matches_played": 42,
          "n_bootstraps": 1000,
          "rank_position": 1,
          "avg_mase": 0.85,
          "mase_std": 0.12,
          "evaluated_count_in_month": 156,
          "calculation_date": "2025-12-31"
        }
      ],
      "scope": {
        "type": "global",
        "id": null
      }
    }
    ```
    
    **Response Fields:**
    
    *Model Identifiers:*
    - `model_id`: Model identifier
    - `model_name`: Name of the model
    - `architecture`: Model architecture type (e.g., decoder-only, encoder-decoder, ...)
    - `model_size`: Number of parameters in the model in millions
    - `organization_name`: Organization name (if applicable)
    
    *ELO Metrics:*
    - `elo_rating_median`: Median ELO rating score from bootstrap samples
    - `elo_ci_lower`: Lower bound of ELO confidence interval
    - `elo_ci_upper`: Upper bound of ELO confidence interval
    - `matches_played`: Number of matches/comparisons used for ELO calculation
    - `n_bootstraps`: Number of bootstrap iterations performed
    - `rank_position`: Rank position within the scope (1 = best)
    
    *MASE Metrics:*
    - `avg_mase`: Average MASE score across all evaluations in the month
    - `mase_std`: Standard deviation of MASE scores
    - `evaluated_count_in_month`: Number of evaluations in the calculation month
    
    *Metadata:*
    - `calculation_date`: Date of the ranking calculation
    
    **Headers:**
    - X-API-Key: Valid API key required
    """
    # Validate that only one scope filter is provided
    if definition_id is not None and frequency_horizon is not None:
        raise HTTPException(
            status_code=400,
            detail="Only one scope filter can be applied at a time. Provide either 'definition_id' OR 'frequency_horizon', not both."
        )
    
    # Determine scope_type and scope_id
    if definition_id is not None:
        scope_type = "definition"
        scope_id = str(definition_id)
    elif frequency_horizon is not None:
        scope_type = "frequency_horizon"
        scope_id = frequency_horizon
    else:
        scope_type = "global"
        scope_id = None
    
    # Parse calculation_date or use None for latest
    calc_date = None
    if calculation_date:
        try:
            from datetime import date
            calc_date = date.fromisoformat(calculation_date)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid calculation_date format. Use YYYY-MM-DD."
            )
    
    # Get filtered rankings
    repo = ModelRepository(conn)
    rankings = repo.get_filtered_rankings(
        scope_type=scope_type,
        scope_id=scope_id,
        calculation_date=calc_date,
        limit=limit
    )
    
    return {
        "rankings": rankings,
        "scope": {
            "type": scope_type,
            "id": scope_id
        }
    }


@router.get("/models/ranking-filters")
async def get_ranking_filters(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get Available Filter Options for Model Rankings.
    
    This endpoint returns all available filter values that can be used with
    the `/models/rankings` endpoint.
    
    **Returns:**
    ```json
    {
      "definitions": [{"id": 1, "name": "Day-Ahead Power"}, {"id": 2, "name": "Week-Ahead Power"}],
      "frequency_horizons": ["00:15:00::1 day", "01:00:00::1 day", "01:00:00::7 days"],
      "calculation_dates": [
        {"calculation_date": "2026-01-31", "is_month_end": true},
        {"calculation_date": "2026-02-07", "is_month_end": false}
      ]
    }
    ```
    
    **Notes:**
    - Only values present in the database are returned
    - `definitions` contains available definition IDs and names for scope_type='definition'
    - `frequency_horizons` contains available frequency::horizon combinations for scope_type='frequency_horizon'
    - `calculation_dates` contains available calculation dates with month-end indicator, sorted by date descending
    
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


@router.get("/models/{model_id}/rankings")
async def get_model_rankings(
    model_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get ELO rankings for a model across all definitions it participated in.
    
    Returns daily ELO rankings for the last 30 days (from today back to 30 days ago).
    For each definition and date, provides:
    - ELO score (median)
    - ELO confidence interval (lower and upper bounds)
    - Rank position
    
    **Response Example:**
    ```json
    {
      "model_id": 123,
      "model_name": "ExampleModel",
      "definition_rankings": [
        {
          "definition_id": 1,
          "definition_name": "Day-Ahead Power Forecast",
          "daily_rankings": [
            {
              "calculation_date": "2025-01-08",
              "elo_score": 1337.5,
              "elo_ci_lower": 1300.2,
              "elo_ci_upper": 1374.8,
              "rank_position": 1
            },
            {
              "calculation_date": "2025-01-09",
              "elo_score": 1342.1,
              "elo_ci_lower": 1305.0,
              "elo_ci_upper": 1379.2,
              "rank_position": 1
            }
          ]
        }
      ]
    }
    ```
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Only includes definitions where the model has ELO rankings in the last 30 days
    - Daily rankings are sorted by date (ascending)
    - ELO scores are based on bootstrapped comparisons from the daily ranking calculations
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
    start_time: Optional[str] = Query(None, description="Filter forecasts from this date (YYYY-mm-dd)", example="2025-12-01"),
    end_time: Optional[str] = Query(None, description="Filter forecasts until this date (YYYY-mm-dd)", example="2025-12-31"),
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
    
    **Query Parameters:**
    - start_time: Optional start date (YYYY-mm-dd) to filter forecasts and ground truth
    - end_time: Optional end date (YYYY-mm-dd) to filter forecasts and ground truth
    
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
      ],
      "ground_truth": [
        {
          "ts": "2024-01-02T00:00:00Z",
          "value": 1230.2
        },
        {
          "ts": "2024-01-02T00:15:00Z",
          "value": 1245.8
        }
      ]
    }
    ```
    
    **Use Cases:**
    - Track model performance over time for a specific series
    - Identify missing forecasts (participation gaps)
    - Distinguish between series not being in scope vs missing forecasts
    - Analyze model consistency across rounds
    - Compare forecasts against ground truth values
    
    **Headers:**
    - X-API-Key: Valid API key required
    
    **Notes:**
    - Returns 404 if model, definition, or series not found
    - Rounds are ordered by start_time (ascending)
    - All rounds of the definition are included, regardless of forecast submission
    - Confidence intervals (ci) are optional and depend on whether the model provided them
    - Ground truth data is fetched from the appropriate resolution table (15min, 1h, or 1d)
    - Ground truth and forecasts are filtered by the same date range when provided
    """
    repo = ForecastRepository(conn)
    result = repo.get_model_series_forecasts_across_rounds(
        model_id, 
        definition_id, 
        series_id,
        start_time=start_time,
        end_time=end_time
    )
    
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
              "rounds_participated": 10
            },
            {
              "series_id": 457,
              "series_name": "Power Load - Region B",
              "series_unique_id": "power_load_region_b",
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

