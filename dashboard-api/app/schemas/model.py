from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional

class ModelSchema(BaseModel):
    readable_id: str
    name: str
    model_family: str | None
    model_size: int | None
    hosting: str | None
    architecture: str | None
    pretraining_data: str | None
    publishing_date: datetime | None

class ModelDetailSchema(ModelSchema):
    """Model with aggregated statistics."""
    parameters: dict | None = None
    challenges_participated: int = 0
    forecasts_made: int = 0


class SeriesInDefinitionSchema(BaseModel):
    """Series information within a definition."""
    series_id: int
    series_name: str
    series_unique_id: Optional[str] = None
    rounds_participated: int


class DefinitionWithSeriesSchema(BaseModel):
    """Definition with its associated series."""
    definition_id: int
    definition_name: str
    series: List[SeriesInDefinitionSchema]


class ModelSeriesByDefinitionSchema(BaseModel):
    """Model's series grouped by definition."""
    model_id: int
    model_readable_id: str
    model_name: str
    definitions: List[DefinitionWithSeriesSchema]
