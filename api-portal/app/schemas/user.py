from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime

class UserCreate(BaseModel):
    """Schema for creating a new user"""
    username: str
    email: Optional[str] = None
    organization_id: Optional[int] = None
    user_type: Optional[str] = 'external'

class UserResponse(BaseModel):
    """Schema for user response"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    username: str
    email: Optional[str] = None
    organization_id: Optional[int] = None
    user_type: str
    created_at: datetime
