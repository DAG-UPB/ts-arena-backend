from app.services.user_service import UserService
import logging

logger = logging.getLogger(__name__)
from app.database.auth.user_repository import UserRepository
from fastapi import Depends, Request, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.database.connection import get_db
from app.services.challenge_service import ChallengeService
from app.services.model_info_service import ModelInfoService
from app.services.export_service import ExportService
from app.database.auth.api_key_repository import APIKeyRepository
from app.core.config import Config

# FastAPI Security Scheme for Swagger UI Integration
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_plugin_manager(request: Request):
    """Dependency to retrieve the global PluginManager from the app state."""
    return request.app.state.plugin_manager

async def get_api_key_repository(db: AsyncSession = Depends(get_db)) -> APIKeyRepository:
    """Dependency for APIKeyRepository with DB session."""
    return APIKeyRepository(db)

async def _verify_api_key_logic(
    api_key: str,
    api_key_repo: APIKeyRepository
) -> dict:
    """
    Core logic to verify an API key.
    Returns user info dict if valid, raises HTTPException if invalid.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key required",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Check internal service API keys first
    if api_key == Config.API_KEY:
        logger.info(f"Auth: Service API Key used. Access granted as internal/service.")
        return {"type": "service", "authenticated": True, "role": "internal"}
    
    # Check external user API keys in the database
    user_info = await api_key_repo.verify_api_key(api_key)
    if user_info:
        logger.info(f"Auth: User API Key verified. UserID: {user_info.get('user_id')}, Role: {user_info.get('role')}, Type: {user_info.get('user_type')}")
        return user_info
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

async def verify_api_key_for_swagger(
    api_key: str = Security(api_key_header),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository)
) -> dict:
    """Security dependency for Swagger UI integration"""
    return await _verify_api_key_logic(api_key, api_key_repo)

async def get_current_user(
    api_key: Optional[str] = Security(api_key_header),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository)
) -> Optional[dict]:
    """Dependency for authenticated users with FastAPI Security Scheme"""
    if api_key:
        return await _verify_api_key_logic(api_key, api_key_repo)
    return None

async def require_auth(current_user: Optional[dict] = Depends(get_current_user)) -> dict:
    """Dependency that requires authentication"""
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return current_user

async def require_user_auth(current_user: Optional[dict] = Depends(get_current_user)) -> dict:
    """Dependency that requires user authentication (not just internal services)"""
    if not current_user or current_user.get("type") == "service":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User authentication required",
        )
    return current_user

async def require_internal_user(current_user: dict = Depends(require_user_auth)) -> dict:
    """Dependency that requires the user to be an internal user"""
    if current_user.get("user_type") != "internal":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Internal user privileges required",
        )
    return current_user

async def require_service_auth(current_user: Optional[dict] = Depends(get_current_user)) -> dict:
    """Dependency that requires service authentication (API Key only)"""
    if not current_user or current_user.get("type") != "service":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Service authentication required",
        )
    return current_user

async def require_internal_auth(current_user: Optional[dict] = Depends(get_current_user)) -> dict:
    """Dependency that requires internal/admin authentication (Service or Internal User)"""
    if not current_user:
        logger.warning("Auth: require_internal_auth failed - No current_user")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    
    role = current_user.get("role")
    logger.debug(f"Auth: require_internal_auth check. Role found: {role}")

    if role != "internal":
        logger.warning(f"Auth: Access DENIED for require_internal_auth. UserID: {current_user.get('user_id')}, Role: {role}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Internal/Admin privileges required",
        )
    return current_user


async def get_challenge_service(
    db: AsyncSession = Depends(get_db),
) -> ChallengeService:
    """Dependency for ChallengeService with DB session."""
    return ChallengeService(db)


async def get_model_info_service(
    db: AsyncSession = Depends(get_db),
) -> ModelInfoService:
    """Dependency for ModelInfoService with DB session."""
    return ModelInfoService(db)


async def get_export_service(
    db: AsyncSession = Depends(get_db),
    challenge_service: ChallengeService = Depends(get_challenge_service)
) -> ExportService:
    """Dependency for ExportService."""
    return ExportService(db, challenge_service)


async def get_user_service(db: AsyncSession = Depends(get_db)) -> UserService:
    repo = UserRepository(db)
    return UserService(repo)
