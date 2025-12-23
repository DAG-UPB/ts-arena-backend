# app/schemas/model_info.py
from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime, date


class ModelInfoCreate(BaseModel):
    name: str
    model_type: Optional[str] = None
    model_family: Optional[str] = None
    model_size: Optional[int] = None
    hosting: Optional[str] = None
    architecture: Optional[str] = None
    pretraining_data: Optional[str] = None
    publishing_date: Optional[date] = None
    parameters: Optional[dict] = None

class ModelInfoCreateInternal(ModelInfoCreate):
    organization_id: int

class ModelInfo(BaseModel):
    name: str
    readable_id: Optional[str] = None
    model_type: Optional[str] = None
    model_family: Optional[str] = None
    model_size: Optional[int] = None
    hosting: Optional[str] = None
    architecture: Optional[str] = None
    pretraining_data: Optional[str] = None
    publishing_date: Optional[date] = None
    organization_id: Optional[int] = None
    parameters: Optional[dict] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)
