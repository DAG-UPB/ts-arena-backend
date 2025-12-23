from app.database.connection import get_db
from app.database.auth.user import User
from app.schemas.user import UserCreate
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

class UserRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_user(self, user: UserCreate):
        """Create a new user"""
        new_user = User(**user.model_dump())
        self.db.add(new_user)
        await self.db.commit()
        await self.db.refresh(new_user)
        return new_user

    async def list_users(self):
        """List all users"""
        result = await self.db.execute(select(User))
        return result.scalars().all()
