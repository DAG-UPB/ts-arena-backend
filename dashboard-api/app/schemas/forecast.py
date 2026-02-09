from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.schemas.round import ForecastDataPoint


class GroundTruthDataPoint(BaseModel):
    """Ground truth data point."""
    ts: datetime
    value: float


class RoundForecastStatus(BaseModel):
    """Forecast status for a specific round."""
    round_id: int
    round_name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    series_in_round: bool  # Whether the series is part of this round
    forecast_exists: bool  # Whether a forecast was submitted for this round
    forecasts: Optional[List[ForecastDataPoint]] = None  # The actual forecast data points if they exist


class ModelSeriesForecastsAcrossRoundsSchema(BaseModel):
    """Forecasts for one model and one series across all rounds of a definition."""
    model_id: int
    model_readable_id: str
    model_name: str
    definition_id: int
    definition_name: str
    series_id: int
    series_name: str
    rounds: List[RoundForecastStatus]
    ground_truth: List[GroundTruthDataPoint]
