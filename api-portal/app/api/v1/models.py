# app/api/v1/models.py
from fastapi import APIRouter, Depends, HTTPException, Header, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.api.dependencies import get_db, require_user_auth, require_auth, require_internal_user
from app.services.model_info_service import ModelInfoService
from app.schemas.model_info import ModelInfo, ModelInfoCreate, ModelInfoCreateInternal

router = APIRouter(prefix="/models", tags=["models"])

async def get_model_info_service(db: AsyncSession = Depends(get_db)) -> ModelInfoService:
    return ModelInfoService(db)

@router.post("/register", response_model=ModelInfo, status_code=201)
async def register_model(
    payload: ModelInfoCreate,
    current_user: dict = Depends(require_user_auth),
    service: ModelInfoService = Depends(get_model_info_service)
):
    """
    Register a new model for the authenticated user.
    
    The model will be associated with the user based on the provided API key.
    The organization is automatically determined from the user's profile.
    """
    try:
        # Use the user_id from the authenticated user
        return await service.register_model(payload, user_info=current_user)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/register/admin", response_model=ModelInfo, status_code=201, tags=["admin"])
async def register_model_admin(
    payload: ModelInfoCreateInternal,
    current_user: dict = Depends(require_internal_user),
    service: ModelInfoService = Depends(get_model_info_service)
):
    """
    Register a new model with explicit organization assignment (Admin/Internal only).
    """
    try:
        return await service.register_model(
            payload, 
            user_info=current_user, 
            organization_id_override=payload.organization_id
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[ModelInfo])
async def list_models(
    user_id: Optional[int] = Query(None, description="Filter models by user ID"),
    current_user: dict = Depends(require_auth),
    service: ModelInfoService = Depends(get_model_info_service)
):
    """
    List all models, optionally filtered by user.
    
    - **Internal Service**: Can see all models or filter by any user_id.
    - **Regular User**: Can ONLY see their own models. user_id filter is ignored/enforced.
    """
    # If user is not internal, force filter to their own user_id
    if current_user.get("role") != "internal":
        user_id = current_user["user_id"]
        
    return await service.list_models(user_id=user_id)
