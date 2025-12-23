from sqlalchemy import Column, Text, DateTime, func, ForeignKey, Integer, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import UniqueConstraint
from app.database.connection import Base
from sqlalchemy.orm import relationship

class ModelInfo(Base):
    __tablename__ = 'model_info'
    __table_args__ = (
        UniqueConstraint('user_id', 'name', name='_user_model_name_uc'),
        {'schema': 'models'}
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("auth.users.id", ondelete="CASCADE"))
    organization_id = Column(Integer, ForeignKey("auth.organizations.id"))
    name = Column(Text, nullable=False)
    readable_id = Column(Text, unique=True, nullable=True)
    model_type = Column(Text)
    model_family = Column(Text)
    model_size = Column(Integer) # in millions
    hosting = Column(Text)
    architecture = Column(Text)
    pretraining_data = Column(Text)
    publishing_date = Column(Date)
    parameters = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="models")
    organization = relationship("Organization", back_populates="models")
    participants = relationship("ChallengeParticipant", back_populates="model")
    forecasts = relationship("Forecast", back_populates="model")
    scores = relationship("ChallengeScore", back_populates="model")

