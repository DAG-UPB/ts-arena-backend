from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field

from app.core.tracks import Track
from app.schemas.arena_score import ArenaScoreScopeSchema
from app.schemas.common import ModelFieldsSchema


class MaseWeekSchema(BaseModel):
    week: date = Field(..., description="Monday 00:00 UTC; the window covers the rounds that ended in the 30 days before")
    mase_30d: Optional[float] = Field(None, description="Geometric mean MASE in the window. Null below 80 % participation")
    mase_rel_30d: Optional[float] = Field(
        None,
        description="Geometric mean of the model's MASE divided by the field's on the same tasks; 1 = the typical model. Null below 80 % participation",
    )
    participation_30d: float = Field(..., description="Share of the scope's tasks in the window the model was scored on (0 to 1)")


class MaseHistoryModelSchema(ModelFieldsSchema):
    model_id: int
    model_name: str
    readable_id: Optional[str] = None
    track: Track
    latest_mase_30d: Optional[float] = Field(None, description="mase_30d in the newest week")
    latest_mase_rel_30d: Optional[float] = Field(None, description="mase_rel_30d in the newest week")
    points: List[MaseWeekSchema] = Field(..., description="Weeks with at least one scored task, oldest first")


class MaseHistorySchema(BaseModel):
    """Weekly 30-day MASE of the models of one scope."""

    scope: ArenaScoreScopeSchema
    track: Optional[Track] = Field(None, description="Track filter of the request")
    window_days: int
    min_participation: float
    weeks: List[date] = Field(..., description="Every week of the scope, oldest first")
    models: List[MaseHistoryModelSchema] = Field(
        ..., description="Models with a value in at least one week, best newest mase_30d first"
    )
