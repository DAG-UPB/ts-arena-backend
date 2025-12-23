from fastapi import APIRouter, Depends
from typing import Dict, List

from app.core.dependencies import get_api_key
from app.database.connection import get_db_connection
from app.repositories.forecast_repository import ForecastRepository
from app.schemas.forecast import ForecastsResponseSchema, ForecastDataPoint

router = APIRouter(prefix="/api/v1/challenges", tags=["Forecasts"])


@router.get("/{challenge_id}/series/{series_id}/forecasts", response_model=ForecastsResponseSchema)
async def get_series_forecasts(
    challenge_id: int,
    series_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Forecasts for a series, grouped by model.
    
    **Returns:**
    ```json
    {
      "forecasts": {
        "model_name": [
          {"ts": "2024-01-01T00:00:00Z", "y": 123.45, "ci": {...}},
          ...
        ]
      }
    }
    ```
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = ForecastRepository(conn)
    forecasts = repo.get_series_forecasts(challenge_id, series_id)
    
    return {"forecasts": forecasts}
