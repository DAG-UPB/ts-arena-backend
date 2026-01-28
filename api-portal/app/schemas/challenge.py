from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, field_serializer
from datetime import datetime, timedelta
import isodate


class RoundStatus(str, Enum):
    """Possible statuses for a challenge round."""
    REGISTRATION = "registration"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


# ==========================================================
# Challenge Definition schemas (for participants)
# ==========================================================

class ChallengeDefinitionBase(BaseModel):
    """Base schema for challenge definitions."""
    schedule_id: str = Field(..., description="Unique schedule identifier")
    name: str
    description: Optional[str] = None
    domain: Optional[str] = None
    subdomain: Optional[str] = None
    context_length: int = Field(..., description="Number of historical data points")
    horizon: timedelta
    frequency: timedelta
    model_config = ConfigDict(from_attributes=True)

    @field_serializer('horizon', 'frequency')
    def serialize_timedelta(self, td: timedelta, _info):
        """Serialize timedelta as ISO 8601 duration string."""
        return isodate.duration_isoformat(td)


class ChallengeDefinitionResponse(ChallengeDefinitionBase):
    """
    Challenge definition response for participants.
    Excludes internal fields like n_time_series, cron_schedule, etc.
    """
    id: int
    is_active: bool


class ChallengeDefinitionFull(ChallengeDefinitionBase):
    """Full challenge definition (for admin use)."""
    id: int
    n_time_series: int
    cron_schedule: Optional[str] = None
    registration_duration: Optional[timedelta] = None
    evaluation_delay: Optional[timedelta] = None
    is_active: bool
    run_on_startup: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_serializer('registration_duration', 'evaluation_delay')
    def serialize_optional_timedelta(self, td: Optional[timedelta], _info):
        """Serialize optional timedelta as ISO 8601 duration string."""
        if td is None:
            return None
        return isodate.duration_isoformat(td)


# ==========================================================
# Challenge Round schemas (for participants)
# ==========================================================

class ChallengeRoundBase(BaseModel):
    """Base schema for challenge rounds."""
    name: str
    description: Optional[str] = None
    context_length: int = Field(..., description="Number of historical data points to use as context")
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    horizon: timedelta
    frequency: Optional[timedelta] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

    @field_serializer('horizon', 'frequency')
    def serialize_timedelta(self, td: Optional[timedelta], _info):
        """Serialize timedelta as ISO 8601 duration string."""
        if td is None:
            return None
        return isodate.duration_isoformat(td)


class ChallengeRoundCreate(BaseModel):
    """Schema for creating challenge rounds (internal/scheduler use)."""
    definition_id: int
    name: str
    description: Optional[str] = None
    context_length: int
    horizon: timedelta
    frequency: Optional[timedelta] = None
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class ChallengeRoundResponse(ChallengeRoundBase):
    """
    Challenge round response for participants.
    Includes definition info but excludes internal details.
    """
    id: int
    status: RoundStatus
    # Definition info
    definition_id: Optional[int] = None
    definition_name: Optional[str] = None
    domain: Optional[str] = None
    subdomain: Optional[str] = None
    # Timing
    created_at: Optional[datetime] = None


class ChallengeRoundFull(ChallengeRoundBase):
    """Full challenge round (for admin/internal use)."""
    id: int
    definition_id: Optional[int] = None
    status: RoundStatus
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# ==========================================================
# Context Data schemas
# ==========================================================

class ContextDataPoint(BaseModel):
    ts: datetime
    value: float


class ChallengeContextData(BaseModel):
    """Context data for a challenge round, grouped by challenge_series_name."""
    challenge_series_name: str
    frequency: Optional[timedelta] = None
    data: List[ContextDataPoint]
    
    @field_serializer('frequency')
    def serialize_frequency(self, frequency: Optional[timedelta], _info):
        """Serialize frequency as ISO 8601 duration string."""
        if frequency is None:
            return None
        return isodate.duration_isoformat(frequency)

