from pydantic import BaseModel, field_serializer
from datetime import datetime, timedelta
from typing import Optional, Any

from app.core.utils import serialize_timedelta_to_iso8601


class RoundMetaSchema(BaseModel):
    """Round metadata."""
    round_id: int
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
    

class RoundModelListSchema(BaseModel):
    """Round schema with metadata."""
    readable_id: str
    name: str
    model_family: Optional[str] = None
    model_size: Optional[int] = None
    hosting: Optional[str] = None
    architecture: Optional[str] = None
    pretraining_data: Optional[str] = None
    publishing_date: Optional[datetime] = None