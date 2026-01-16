from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, field_serializer
from datetime import datetime, timedelta
import uuid
import isodate

class ChallengeBase(BaseModel):
    name: str
    description: Optional[str] = None
    context_length: int = Field(..., description="Number of historical data points to use as context")
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    horizon: timedelta
    frequency: Optional[timedelta] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

class ChallengeCreate(ChallengeBase):
    pass

class Challenge(ChallengeBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    preparation_params: Optional[Dict[str, Any]] = None
    model_config = ConfigDict(from_attributes=True)

class ContextDataPoint(BaseModel):
    ts: datetime
    value: float

class ChallengeContextData(BaseModel):
    """Context data for a challenge, grouped by challenge_series_name."""
    challenge_series_name: str
    frequency: Optional[timedelta] = None
    data: List[ContextDataPoint]
    
    @field_serializer('frequency')
    def serialize_frequency(self, frequency: Optional[timedelta], _info):
        """Serialize frequency as ISO 8601 duration string."""
        if frequency is None:
            return None
        return isodate.duration_isoformat(frequency)

class ChallengeStatus(BaseModel):
    challenge_id: int
    challenge_name: str
    status: str
    start_date: datetime
    end_date: datetime