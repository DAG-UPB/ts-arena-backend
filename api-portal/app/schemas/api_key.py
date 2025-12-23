from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime


class APIKeyCreate(BaseModel):
    """Schema for creating a new API key"""
    user_id: int
    description: Optional[str] = None


class APIKeyResponse(BaseModel):
    """Schema for API key response (includes the actual key - shown only once!)"""
    api_key: str
    user_id: int
    description: Optional[str] = None


class APIKeyList(BaseModel):
    """Schema for listing API keys (without the actual key)"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    user_id: int
    description: Optional[str] = None
    is_active: bool
    created_at: datetime
    last_used: Optional[datetime] = None