"""Forecast schemas aligned with forecasts.forecasts table structure."""
import logging
from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.services.forecast_metrics import clean_probabilistic_values

logger = logging.getLogger(__name__)

# ==========================================================================
# Upload Schemas
# ==========================================================================
class ForecastDataPoint(BaseModel):
    """A single forecast data point for upload."""
    ts: datetime = Field(..., description="Forecast timestamp")
    value: float = Field(..., description="Predicted value")
    probabilistic_values: Optional[Dict[str, float]] = Field(
        None,
        description="Optional probabilistic forecasts (quantiles q_0.1…q_0.9)"
    )

    @field_validator("probabilistic_values", mode="before")
    @classmethod
    def _validate_probabilistic_values(cls, v):
        """Tolerant validation: keep only q_0.1…q_0.9 keys with finite values.

        Unknown/malformed keys are dropped and logged rather than failing the upload, so
        legacy or slightly-off submitters are not rejected. Empty/None pass through.
        """
        cleaned, dropped = clean_probabilistic_values(v)
        if dropped:
            logger.warning(
                "Dropped %d invalid probabilistic_values key(s): %s",
                len(dropped), dropped,
            )
        return cleaned


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

