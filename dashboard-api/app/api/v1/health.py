from fastapi import APIRouter
from datetime import datetime

from app.core.config import settings
from app.schemas.common import HealthSchema, APIInfoSchema

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthSchema)
async def health_check():
    """Health Check Endpoint (no API-Key required)."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "version": settings.API_VERSION
    }


@router.get("/api/v1/info", response_model=APIInfoSchema)
async def api_info():
    """API Information (no API-Key required)."""
    return {
        "title": settings.API_TITLE,
        "version": settings.API_VERSION,
        "description": "TS-Arena Dashboard API - Provides access to challenge and forecast data"
    }
