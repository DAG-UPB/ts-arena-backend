from app.database.auth.organization_repository import OrganizationRepository
from app.schemas.organization import OrganizationCreate, OrganizationResponse
from sqlalchemy.ext.asyncio import AsyncSession

class OrganizationService:
    def __init__(self, session: AsyncSession):
        self.repo = OrganizationRepository(session)

    async def create_organization(self, org: OrganizationCreate) -> OrganizationResponse:
        return await self.repo.create(org)

    async def list_organizations(self) -> list[OrganizationResponse]:
        return await self.repo.list()
