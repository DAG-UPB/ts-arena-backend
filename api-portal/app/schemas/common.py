# app/schemas/common.py

from pydantic import BaseModel
from typing import List, Optional

class ResponseModel(BaseModel):
    status: str
    message: Optional[str] = None

class ErrorResponseModel(BaseModel):
    status: str
    error: str
    details: Optional[str] = None

class MetadataModel(BaseModel):
    name: str
    description: str
    granularity: str
    forecast_horizon: str
    available_metrics: List[str]
    update_frequency: str