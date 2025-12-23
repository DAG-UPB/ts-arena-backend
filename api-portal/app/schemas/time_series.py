# app/schemas/time_series.py
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

# ==========================================================================
# Base Schemas
# ==========================================================================

class TimeSeriesBase(BaseModel):
    name: str = Field(..., description="A unique, human-readable name for the time series.", example="Stromverbrauch_Gebäude_A")
    description: Optional[str] = Field(None, description="A detailed description of the time series.", example="Stündlicher Stromverbrauch des Hauptgebäudes in kWh.")
    granularity: str = Field(..., description="Temporal granularity of the data (e.g. '15m', '1h').", example="1h")
    forecast_horizon: str = Field(..., description="Supported forecast horizon.", example="24h")
    available_metrics: List[str] = Field(default=[], description="List of available metrics/time series.", example=["power", "voltage"])
    update_frequency: str = Field(..., description="How often the data is updated (e.g. 'daily', 'hourly').", example="hourly")
    endpoint_prefix: str = Field(..., description="Unique prefix for API endpoints.", example="power-consumption")
    ts_timezone: Optional[str] = Field(None, description="The timezone of the time series (e.g. 'UTC' or 'Europe/Berlin').")

# ==========================================================================
# Schemas for API Operations
# ==========================================================================

class TimeSeriesCreate(TimeSeriesBase):
    pass

class TimeSeriesUpdate(BaseModel):
    name: Optional[str] = Field(None, description="The new name for the time series.")
    description: Optional[str] = Field(None, description="The new description for the time series.")
    granularity: Optional[str] = Field(None, description="The new temporal granularity.")
    forecast_horizon: Optional[str] = Field(None, description="The new forecast horizon.")
    available_metrics: Optional[List[str]] = Field(None, description="The new available metrics.")
    update_frequency: Optional[str] = Field(None, description="The new update frequency.")
    endpoint_prefix: Optional[str] = Field(None, description="The new endpoint prefix.")

# ==========================================================================
# Schemas for Database Models (Response Models)
# ==========================================================================

class TimeSeries(TimeSeriesBase):
    series_id: int = Field(..., description="The unique ID of the time series.")
    created_at: datetime = Field(..., description="The timestamp of creation.")
    updated_at: datetime = Field(..., description="The timestamp of the last update.")

    class Config:
        orm_mode = True

# ==========================================================================
# Schemas for Time Series Data Points
# ==========================================================================

class TimeSeriesDataPoint(BaseModel):
    ts: datetime = Field(..., description="The timestamp of the data point.")
    value: float = Field(..., description="The numeric value of the data point.")

class TimeSeriesData(BaseModel):
    series_id: int
    data: List[TimeSeriesDataPoint]
