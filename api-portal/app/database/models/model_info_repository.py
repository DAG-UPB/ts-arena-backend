# app/database/repositories/model_info_repository.py
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database.models.model_info import ModelInfo
import logging

logger = logging.getLogger(__name__)

class ModelInfoRepository:
    """Repository for CRUD operations on the model_info table."""

    def __init__(self, session: AsyncSession):
        self.session = session


    async def create(
        self,
        name: str,
        user_id: int,
        model_type: Optional[str] = None,
        parameters: Optional[dict] = None,
        readable_id: Optional[str] = None,
        model_family: Optional[str] = None,
        model_size: Optional[int] = None,
        organization_id: Optional[int] = None,
        hosting: Optional[str] = None,
        architecture: Optional[str] = None,
        pretraining_data: Optional[str] = None,
        publishing_date: Optional[str] = None,
        paper_url: Optional[str] = None,
        repo_url: Optional[str] = None,
        website_url: Optional[str] = None,
        description: Optional[str] = None,
        arxiv_id: Optional[str] = None,
    ) -> ModelInfo:
        """
        Create and persist a ModelInfo instance.

        Optional discovery / provenance fields (paper_url, repo_url,
        website_url, description, arxiv_id) accept None and are stored as-is.
        """
        try:
            obj = ModelInfo(
                name=name,
                user_id=user_id,
                model_type=model_type,
                parameters=parameters,
                readable_id=readable_id,
                model_family=model_family,
                model_size=model_size,
                organization_id=organization_id,
                hosting=hosting,
                architecture=architecture,
                pretraining_data=pretraining_data,
                publishing_date=publishing_date,
                paper_url=paper_url,
                repo_url=repo_url,
                website_url=website_url,
                description=description,
                arxiv_id=arxiv_id,
            )

            self.session.add(obj)
            await self.session.commit()
            await self.session.refresh(obj)
            return obj
        except Exception as e:
            logger.error(f"Error creating ModelInfo: {e}")
            await self.session.rollback()
            raise


    async def get_by_id(self, model_id: int) -> Optional[ModelInfo]:
        """Get a model by its ID."""
        result = await self.session.execute(
            select(ModelInfo).where(
                ModelInfo.id == model_id
            )
        )
        return result.scalar_one_or_none()

    async def list(self, skip: int = 0, limit: int = 100) -> List[ModelInfo]:
        """List all models with pagination."""
        result = await self.session.execute(
            select(ModelInfo).offset(skip).limit(limit)
        )
        return result.scalars().all()


    async def list_by_user(self, user_id: int) -> List[ModelInfo]:
        """List all models for a specific user."""
        result = await self.session.execute(
            select(ModelInfo).where(ModelInfo.user_id == user_id)
        )
        return result.scalars().all()

    async def get_by_name_and_user(self, name: str, user_id: int) -> Optional[ModelInfo]:
        """Get a model by its name and user ID."""
        result = await self.session.execute(
            select(ModelInfo).where(
                ModelInfo.name == name,
                ModelInfo.user_id == user_id
            )
        )
        return result.scalar_one_or_none()
