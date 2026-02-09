"""Forecast schemas aligned with forecasts.forecasts table structure."""
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime

# ==========================================================================
# Upload Schemas
# ==========================================================================
class ForecastDataPoint(BaseModel):
    """A single forecast data point for upload."""
    ts: datetime = Field(..., description="Forecast timestamp")
    value: float = Field(..., description="Predicted value")
    probabilistic_values: Optional[Dict[str, float]] = Field(
        None, 
        description="Optional probabilistic forecasts (e.g., quantiles)"
    )


class ForecastSeriesUpload(BaseModel):
    """Forecasts for a single time series referenced by challenge_series_name."""
    challenge_series_name: str = Field(..., description="Challenge-scoped series identifier")
    forecasts: List[ForecastDataPoint] = Field(..., description="List of forecast data points")



class ForecastUploadRequest(BaseModel):
    """Request payload for uploading forecasts."""
    round_id: int = Field(..., description="ID of the challenge round")
    model_name: str = Field(..., description="Name of the model making predictions")
    forecasts: List[ForecastSeriesUpload] = Field(
        ..., 
        description="Forecasts for multiple time series"
    )


class ForecastUploadResponse(BaseModel):
    """Response after forecast upload."""
    success: bool
    message: str
    forecasts_inserted: int
    errors: List[str] = Field(default_factory=list)


# ==========================================================================
# Database Schemas
# ==========================================================================
class ForecastInDB(BaseModel):
    """Forecast record as stored in database (single row)."""
    id: int
    round_id: int
    model_id: int
    series_id: int
    ts: datetime
    predicted_value: float
    probabilistic_values: Optional[Dict[str, float]] = None
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


class ForecastResponse(BaseModel):
    """Forecast data for API responses."""
    ts: datetime
    predicted_value: float
    probabilistic_values: Optional[Dict[str, float]] = None
    challenge_series_name: str
    
    model_config = ConfigDict(from_attributes=True)


class ForecastListResponse(BaseModel):
    """Response for listing forecasts."""
    round_id: int
    model_id: int
    forecasts: List[ForecastResponse]

