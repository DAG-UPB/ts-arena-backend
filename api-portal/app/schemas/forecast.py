"""Forecast schemas aligned with forecasts.forecasts table structure."""
import logging
from pydantic import BaseModel, Field, ConfigDict, model_validator
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
        description="Optional probabilistic forecasts (quantiles q_0.1…q_0.9; bare '0.1'…'0.9' also accepted)"
    )
    dropped_probabilistic_keys: List[str] = Field(
        default_factory=list,
        exclude=True,
        description="Keys discarded while cleaning this point — reported per series by the "
                    "upload service, never persisted.",
    )

    @model_validator(mode="before")
    @classmethod
    def _clean_probabilistic_values(cls, data):
        """Tolerant validation: keep the nine deciles in either key form, canonicalised.

        Unknown/malformed keys are dropped rather than failing the upload, so legacy or
        slightly-off submitters are not rejected. Empty/None pass through.

        What was dropped is recorded on the point instead of logged here: this runs once
        per data point, and logging at that granularity floods the logs on every upload —
        thousands of lines per cycle, which materially slowed incident diagnosis during
        backend-48. `ForecastService.upload_forecasts` aggregates these into one line per
        series (backend-69).
        """
        if not isinstance(data, dict):
            return data
        cleaned, dropped = clean_probabilistic_values(data.get("probabilistic_values"))
        return {**data, "probabilistic_values": cleaned, "dropped_probabilistic_keys": dropped}


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
    """Response after forecast upload.

    Everything added here is additive (backend-94): older participant clients that only read
    `success` / `message` / `forecasts_inserted` / `errors` keep working unchanged.
    """
    success: bool
    message: str
    forecasts_inserted: int
    model_id: Optional[int] = Field(
        None,
        description="Numeric id the upload was attributed to. Use it with "
                    "GET /forecasts/{round_id}/{model_id} to read the stored forecast back.",
    )
    points_inserted: int = Field(
        0, description="Forecast points stored (same as forecasts_inserted; named for symmetry)."
    )
    probabilistic_points_inserted: int = Field(
        0,
        description="Of those, how many carried usable quantiles. A submitter can confirm "
                    "probabilistic persistence from this alone, without a readback.",
    )
    errors: List[str] = Field(
        default_factory=list,
        description="Conditions that cost you data: a series was rejected, or points were "
                    "dropped. For backward compatibility this also still carries the "
                    "advisories now repeated in `warnings`.",
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Advisories that did NOT cost you data — repaired quantile crossings, "
                    "unrecognised keys dropped from an otherwise-accepted point. Clients "
                    "should fail on `errors`, not on these.",
    )


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

