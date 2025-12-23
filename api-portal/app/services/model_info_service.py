# app/services/model_info_service.py
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.models.model_info_repository import ModelInfoRepository
from app.schemas.model_info import ModelInfoCreate, ModelInfo
from app.services.utils import generate_readable_id

class ModelInfoService:
    def __init__(self, session: AsyncSession):
        self.repo = ModelInfoRepository(session)

    async def register_model(self, data: ModelInfoCreate, user_info: dict, organization_id_override: Optional[int] = None) -> ModelInfo:
        """Register a new model for the authenticated user."""
        
        user_id = user_info["user_id"]
        user_type = user_info.get("user_type", "external")
        user_org_id = user_info.get("organization_id")
        
        # Determine organization_id
        organization_id = None
        if user_type == "internal" and organization_id_override is not None:
            # Internal users can specify organization_id
            organization_id = organization_id_override
        else:
            # External users are bound to their organization
            organization_id = user_org_id

        # Create model with user_id
        readable_id = generate_readable_id(data.name)
        obj = await self.repo.create(
            name=data.name,
            user_id=user_id,
            model_type=data.model_type,
            parameters=data.parameters,
            readable_id=readable_id,
            model_family=data.model_family,
            model_size=data.model_size,
            organization_id=organization_id,
            hosting=data.hosting,
            architecture=data.architecture,
            pretraining_data=data.pretraining_data,
            publishing_date=data.publishing_date
        )
        return ModelInfo.model_validate(obj)

    async def list_models(self, user_id: Optional[int] = None) -> List[ModelInfo]:
        """List all models, optionally filtered by user."""
        if user_id:
            rows = await self.repo.list_by_user(user_id)
        else:
            rows = await self.repo.list()
        return [ModelInfo.model_validate(r) for r in rows]
