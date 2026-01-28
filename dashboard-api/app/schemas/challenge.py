from pydantic import BaseModel, field_serializer
from datetime import datetime, timedelta
from typing import Optional, List, Any
import isodate


def serialize_timedelta_to_iso8601(value: Optional[timedelta]) -> Optional[str]:
    """
    Convert timedelta to ISO 8601 duration format (PostgreSQL interval format).
    
    Args:
        value: timedelta object or None
        
    Returns:
        ISO 8601 duration string like 'P1D' (1 day), 'PT1H' (1 hour), 'PT15M' (15 minutes), or None
    """
    if value is None:
        return None
    
    return isodate.duration_isoformat(value)


class ChallengeDefinitionSchema(BaseModel):
    """Schema for Challenge Definitions (templates)."""
    id: int
    schedule_id: str
    name: str
    description: Optional[str] = None
    domains: List[str] = []
    categories: List[str] = []
    subcategories: List[str] = []
    frequency: Optional[timedelta] = None
    horizon: Optional[timedelta] = None
    created_at: Optional[datetime] = None
    
    @field_serializer('frequency', 'horizon')
    def serialize_durations(self, value: Optional[timedelta], info) -> Optional[str]:
        return serialize_timedelta_to_iso8601(value)


class ChallengeRoundSchema(BaseModel):
    """Schema for Challenge Rounds (instantiations)."""
    model_config = {"protected_namespaces": ()}
    
    id: int  # Round ID
    definition_id: Optional[int] = None
    name: Optional[str] = None 
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    start_time: Optional[datetime] = None  
    end_time: Optional[datetime] = None
    description: Optional[str] = None
    status: str
    n_time_series: int
    context_length: Optional[Any] = None  # interval type from postgres
    horizon: Optional[timedelta] = None  # ISO 8601 duration string
    frequency: Optional[timedelta] = None  # Challenge frequency as ISO 8601 duration
    created_at: Optional[datetime] = None
    model_count: Optional[int] = 0
    forecast_count: Optional[int] = 0
    
    # Metadata arrays (inherited from definition)
    domains: Optional[List[str]] = []
    categories: Optional[List[str]] = []
    subcategories: Optional[List[str]] = []

    @field_serializer('frequency', 'horizon')
    def serialize_durations(self, value: Optional[timedelta], info) -> Optional[str]:
        """Convert timedelta to ISO 8601 duration format for API responses."""
        return serialize_timedelta_to_iso8601(value)




class ChallengeMetaSchema(BaseModel):
    """Challenge metadata."""
    challenge_id: int
    name: Optional[str] = None
    description: Optional[str] = None
    status: str
    context_length: Optional[Any] = None
    horizon: Optional[timedelta] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None

    @field_serializer('horizon')
    def serialize_durations(self, value: Optional[timedelta], info) -> Optional[str]:
        """Convert timedelta to ISO 8601 duration format for API responses."""
        return serialize_timedelta_to_iso8601(value)


class ChallengeSeriesSchema(BaseModel):
    """Time series for a challenge."""
    series_id: int
    name: Optional[str] = None
    description: Optional[str] = None
    frequency: Optional[timedelta] = None  # Changed from str to timedelta (INTERVAL from DB)
    horizon: Optional[Any] = None
    unique_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    context_start_time: Optional[datetime] = None
    context_end_time: Optional[datetime] = None
    
    # NEW: Domain fields
    domain: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    
    @field_serializer('frequency')
    def serialize_frequency(self, value: Optional[timedelta]) -> Optional[str]:
        """Convert timedelta to ISO 8601 duration format for API responses."""
        return serialize_timedelta_to_iso8601(value)


class ChallengeMetadataSchema(BaseModel):
    """Available filter options for challenge listing."""
    frequencies: List[str] = []
    horizons: List[str] = []
    domains: List[str] = []
    categories: List[str] = []
    subcategories: List[str] = []
    statuses: List[str] = []
    
    class Config:
        json_schema_extra = {
            "example": {
                "frequencies": ["PT15M", "PT1H"],
                "horizons": ["P1D", "P3D"],
                "domains": ["Energy"],
                "categories": ["Electricity"],
                "subcategories": ["Load", "Generation", "Price"],
                "statuses": ["active", "registration", "announced"]
            }
        }


class TimeSeriesDataPoint(BaseModel):
    """Data point of a time series."""
    ts: datetime
    value: float


class TimeSeriesDataSchema(BaseModel):
    """Time series data."""
    data: List[TimeSeriesDataPoint]
