from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database.auth.organization import Organization
from app.schemas.organization import OrganizationCreate

class OrganizationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, org: OrganizationCreate) -> Organization:
        db_org = Organization(name=org.name)
        self.session.add(db_org)
        await self.session.commit()
        await self.session.refresh(db_org)
        return db_org

    async def list(self) -> list[Organization]:
        result = await self.session.execute(select(Organization))
        return result.scalars().all()

    async def get_by_id(self, org_id: int) -> Organization | None:
        return await self.session.get(Organization, org_id)
