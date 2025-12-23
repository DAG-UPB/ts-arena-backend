from fastapi import APIRouter, Depends, HTTPException
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.connection import get_db
from app.schemas.organization import OrganizationCreate, OrganizationResponse
from app.services.organization_service import OrganizationService
from app.api.dependencies import require_internal_auth

router = APIRouter(prefix="/organizations", tags=["organizations", "admin"])

def get_organization_service(db: AsyncSession = Depends(get_db)) -> OrganizationService:
    return OrganizationService(db)

@router.post("/", response_model=OrganizationResponse, status_code=201)
async def create_organization(
    payload: OrganizationCreate,
    current_user: dict = Depends(require_internal_auth),
    service: OrganizationService = Depends(get_organization_service)
):
    """
    Create a new organization.
    Requires internal/admin authentication.
    """
    return await service.create_organization(payload)

@router.get("/", response_model=List[OrganizationResponse])
async def list_organizations(
    current_user: dict = Depends(require_internal_auth),
    service: OrganizationService = Depends(get_organization_service)
):
    """
    List all organizations.
    Requires internal/admin authentication.
    """
    return await service.list_organizations()
