from pydantic import BaseModel, ConfigDict
from datetime import datetime

class OrganizationCreate(BaseModel):
    name: str

class OrganizationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    created_at: datetime
