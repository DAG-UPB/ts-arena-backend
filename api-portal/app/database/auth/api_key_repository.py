from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload
from typing import Optional, List
from datetime import datetime, timezone
import hashlib
import secrets

from app.database.auth.api_key import APIKey
from app.schemas.api_key import APIKeyCreate, APIKeyResponse, APIKeyList


class APIKeyRepository:
    def __init__(self, session: AsyncSession):
        self.session = session
    
    def _hash_api_key(self, api_key: str) -> str:
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    def generate_api_key(self) -> str:
        return secrets.token_urlsafe(32)
    
    async def create_api_key(self, key_request: APIKeyCreate) -> APIKeyResponse:
        """Create a new API key for a user"""
        api_key = self.generate_api_key()
        api_key_hash = self._hash_api_key(api_key)
        
        db_api_key = APIKey(
            key_hash=api_key_hash,
            user_id=key_request.user_id,
            description=key_request.description,
            is_active=True
        )
        
        self.session.add(db_api_key)
        await self.session.commit()
        await self.session.refresh(db_api_key)
        
        # Return the actual API key (shown only once!)
        return APIKeyResponse(
            api_key=api_key,
            user_id=db_api_key.user_id,
            description=db_api_key.description
        )
    
    async def verify_api_key(self, api_key: str) -> Optional[dict]:
        """Verify an API key and return user information"""
        api_key_hash = self._hash_api_key(api_key)
        
        stmt = select(APIKey).options(selectinload(APIKey.user)).where(
            APIKey.key_hash == api_key_hash,
            APIKey.is_active == True
        )
        result = await self.session.execute(stmt)
        db_api_key = result.scalar_one_or_none()
        
        if not db_api_key:
            return None
        
        # Update last_used timestamp
        await self._update_last_used(db_api_key.id)
        
        user = db_api_key.user
        
        # Robust check for internal user type
        user_type_str = (user.user_type or '').lower().strip() if user else ''
        is_internal = user_type_str == 'internal'
        
        return {
            "type": "user",
            "authenticated": True,
            "user_id": db_api_key.user_id,
            "user_type": user.user_type if user else 'external',
            "role": "internal" if is_internal else "user",
            "organization_id": user.organization_id if user else None,
            "created_at": db_api_key.created_at
        }
    
    async def list_api_keys(self) -> List[APIKeyList]:
        """List all active API keys"""
        stmt = select(APIKey).where(APIKey.is_active == True).order_by(APIKey.created_at.desc())
        result = await self.session.execute(stmt)
        db_api_keys = result.scalars().all()
        
        return [
            APIKeyList(
                id=key.id,
                user_id=key.user_id,
                description=key.description,
                is_active=key.is_active,
                created_at=key.created_at,
                last_used=key.last_used
            )
            for key in db_api_keys
        ]
    
    async def revoke_api_key(self, user_id: int) -> bool:
        """Revoke all API keys for a user"""
        stmt = update(APIKey).where(
            APIKey.user_id == user_id,
            APIKey.is_active == True
        ).values(is_active=False)
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount > 0
    
    async def deactivate_api_key(self, api_key_hash: str) -> bool:
        """Deactivate an API key by its hash"""
        stmt = update(APIKey).where(
            APIKey.key_hash == api_key_hash
        ).values(is_active=False)
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount > 0
    
    async def _update_last_used(self, api_key_id: int) -> None:
        """Internal method to update the last_used timestamp"""
        stmt = update(APIKey).where(
            APIKey.id == api_key_id
        ).values(last_used=datetime.now(timezone.utc))
        
        await self.session.execute(stmt)
        await self.session.commit()
    
