from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any


class ForecastDataPoint(BaseModel):
    """Single forecast data point."""
    ts: datetime
    y: float
    ci: Optional[Dict[str, Any]] = None  # confidence intervals (JSONB)


class ForecastsResponseSchema(BaseModel):
    """All forecasts for a series."""
    forecasts: Dict[str, Dict[str, List[ForecastDataPoint]|str|float|None]]  # key = readable_id of model
