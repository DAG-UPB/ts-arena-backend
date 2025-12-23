from fastapi import Depends, HTTPException, status, Header
from typing import Optional

from app.core.security import verify_api_key


async def get_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """Extracts and validates API-Key from Header."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key missing"
        )
    verify_api_key(x_api_key)
    return x_api_key
