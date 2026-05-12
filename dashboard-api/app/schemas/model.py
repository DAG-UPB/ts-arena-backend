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
    # Optional discovery / provenance metadata. See ticket #43.
    paper_url: str | None = None
    repo_url: str | None = None
    website_url: str | None = None
    description: str | None = None
    arxiv_id: str | None = None


class ModelListItemSchema(BaseModel):
    """Single row in the `GET /models` listing — keeps the payload thin."""
    id: int
    readable_id: str | None
    name: str
    model_family: str | None
    model_size: int | None
    architecture: str | None
    paper_url: str | None = None
    repo_url: str | None = None
    website_url: str | None = None
    arxiv_id: str | None = None


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


class ModelActiveRoundSchema(BaseModel):
    """A round the model is currently registered for (status in {registration, active})."""
    round_id: int
    round_name: str
    description: Optional[str] = None
    definition_id: Optional[int] = None
    definition_name: Optional[str] = None
    status: str
    registration_start: Optional[datetime] = None
    registration_end: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    frequency: Optional[str] = None
    horizon: Optional[str] = None


class ModelActiveRoundsResponseSchema(BaseModel):
    """Response wrapper for the model's active and upcoming rounds."""
    model_id: int
    model_readable_id: str
    model_name: str
    rounds: List[ModelActiveRoundSchema]
