from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.api.dependencies import require_internal_auth, get_user_service
from app.schemas.user import UserCreate, UserResponse

router = APIRouter(
    prefix="/users",
    tags=["users", "admin"],
    dependencies=[Depends(require_internal_auth)]
)

@router.post("/", response_model=UserResponse)
async def create_user(
    user: UserCreate,
    current_user: dict = Depends(require_internal_auth),
    user_service = Depends(get_user_service)
):
    """Create a new user"""
    return await user_service.create_user(user)

@router.get("/", response_model=List[UserResponse])
async def list_users(
    current_user: dict = Depends(require_internal_auth),
    user_service = Depends(get_user_service)
):
    """List all users"""
    return await user_service.list_users()
