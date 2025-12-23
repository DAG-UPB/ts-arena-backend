from fastapi import HTTPException, status
from app.core.config import settings


def verify_api_key(api_key: str) -> bool:
    """Validates the API-Key against the configured key."""
    if api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key"
        )
    return True
