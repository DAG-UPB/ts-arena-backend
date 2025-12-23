from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.api.dependencies import require_auth, get_current_user, require_service_auth, get_api_key_repository, verify_api_key_for_swagger
from app.database.auth.api_key_repository import APIKeyRepository
from app.schemas.api_key import APIKeyCreate, APIKeyResponse, APIKeyList

router = APIRouter(
    prefix="/api-keys",
    tags=["api-keys", "admin"],
    dependencies=[Depends(require_service_auth)]
)

@router.post("/generate", response_model=APIKeyResponse)
async def generate_user_api_key(
    key_request: APIKeyCreate,
    current_user: dict = Depends(require_service_auth),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository)
):
    """Generate a new API key for a user"""
    
    # Create the API key in the database
    result = await api_key_repo.create_api_key(key_request)
    
    return result

@router.get("/list", response_model=List[APIKeyList])
async def list_api_keys(
    current_user: dict = Depends(require_service_auth),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository)
):
    """List all API keys (internal services only)"""
    
    keys = await api_key_repo.list_api_keys()
    return keys

@router.delete("/revoke/{user_id}")
async def revoke_api_key(
    user_id: int,
    current_user: dict = Depends(require_service_auth),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository)
):
    """Revoke all API keys for a user"""
    
    success = await api_key_repo.revoke_api_key(user_id)
    
    if success:
        return {"message": f"API keys for user {user_id} revoked"}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active API keys found for user {user_id}"
        )

@router.get("/me")
async def get_current_user_info(current_user: dict = Depends(verify_api_key_for_swagger)):
    """Get information about the current user"""
    return current_user
