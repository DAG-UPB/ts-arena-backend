from sqlalchemy import Column, DateTime, Boolean, Text, ForeignKey, Integer
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database.connection import Base

class APIKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = {'schema': 'auth'}

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("auth.users.id", ondelete="CASCADE"), nullable=False, index=True)
    key_hash = Column(Text, nullable=False, unique=True)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_used = Column(DateTime(timezone=True))

    user = relationship("User")

