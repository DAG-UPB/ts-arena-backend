from datetime import datetime
from pydantic import BaseModel

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
