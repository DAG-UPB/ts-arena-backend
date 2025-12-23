from sqlalchemy import (
    Column, String, DateTime, ForeignKey, Integer, Float, Text, Interval, BigInteger
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.schema import UniqueConstraint
from app.database.connection import Base
from sqlalchemy.orm import relationship


class Challenge(Base):
    __tablename__ = 'challenges'
    __table_args__ = {'schema': 'challenges'}
    
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    context_length = Column(Integer, nullable=False)
    registration_start = Column(DateTime(timezone=True))
    registration_end = Column(DateTime(timezone=True))
    horizon = Column(Interval, nullable=False)
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    preparation_params = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    participants = relationship("ChallengeParticipant", back_populates="challenge")
    context_data = relationship("ChallengeContextData", back_populates="challenge")
    forecasts = relationship("Forecast", back_populates="challenge")
    scores = relationship("ChallengeScore", back_populates="challenge")
    series_pseudo = relationship("ChallengeSeriesPseudo", back_populates="challenge")


class ChallengeParticipant(Base):
    __tablename__ = 'challenge_participants'
    __table_args__ = (
        UniqueConstraint('challenge_id', 'model_id', name='_challenge_model_uc'),
        {'schema': 'challenges'}
    )

    id = Column(Integer, primary_key=True)
    challenge_id = Column(Integer, ForeignKey('challenges.challenges.id', ondelete="CASCADE"))
    model_id = Column(Integer, ForeignKey('models.model_info.id', ondelete="CASCADE"))
    registered_at = Column(DateTime(timezone=True), server_default=func.now())

    challenge = relationship("Challenge", back_populates="participants")
    model = relationship("ModelInfo", back_populates="participants")


class ChallengeContextData(Base):
    __tablename__ = 'challenge_context_data'
    __table_args__ = {'schema': 'challenges'}

    id = Column(BigInteger, primary_key=True)
    challenge_id = Column(Integer, ForeignKey('challenges.challenges.id', ondelete="CASCADE"))
    series_id = Column(Integer, ForeignKey('data_portal.time_series.series_id', ondelete="CASCADE"), nullable=False)
    ts = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    value = Column(Float)
    series_metadata = Column("metadata", JSONB)

    challenge = relationship("Challenge", back_populates="context_data")


class ChallengeSeriesPseudo(Base):
    __tablename__ = 'challenge_series_pseudo'
    __table_args__ = {'schema': 'challenges'}

    id = Column(Integer, primary_key=True)
    challenge_id = Column(Integer, ForeignKey('challenges.challenges.id', ondelete="CASCADE"))
    series_id = Column(Integer, ForeignKey('data_portal.time_series.series_id', ondelete="CASCADE"), nullable=False)
    challenge_series_name = Column(Text, nullable=False)
    min_ts = Column(DateTime(timezone=True))
    max_ts = Column(DateTime(timezone=True))
    value_avg = Column(Float)
    value_std = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    challenge = relationship("Challenge", back_populates="series_pseudo")


class VChallengeWithStatus(Base):
    """
    Read-only model for the challenges.v_challenges_with_status view.
    """
    __tablename__ = 'v_challenges_with_status'
    __table_args__ = {'schema': 'challenges', 'info': dict(is_view=True)}

    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    context_length = Column(Integer, nullable=False)
    registration_start = Column(DateTime(timezone=True))
    registration_end = Column(DateTime(timezone=True))
    horizon = Column(Interval, nullable=False)
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    preparation_params = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    status = Column(String)

