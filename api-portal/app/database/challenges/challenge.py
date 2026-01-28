from sqlalchemy import (
    Column, String, DateTime, ForeignKey, Integer, Float, Text, Interval, BigInteger, Boolean, ARRAY
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from app.database.connection import Base
from sqlalchemy.orm import relationship


class ChallengeDefinition(Base):
    """
    Challenge definition/template from YAML configuration.
    Represents a recurring challenge type.
    """
    __tablename__ = 'definitions'
    __table_args__ = {'schema': 'challenges'}
    
    id = Column(Integer, primary_key=True)
    schedule_id = Column(Text, unique=True, nullable=False)  # YAML id
    name = Column(Text, nullable=False)
    description = Column(Text)
    domains = Column(ARRAY(Text))
    subdomains = Column(ARRAY(Text))
    categories = Column(ARRAY(Text))
    subcategories = Column(ARRAY(Text))
    context_length = Column(Integer, nullable=False)
    horizon = Column(Interval, nullable=False)
    frequency = Column(Interval, nullable=False)
    cron_schedule = Column(Text)
    n_time_series = Column(Integer, nullable=False)
    registration_duration = Column(Interval)
    evaluation_delay = Column(Interval)
    is_active = Column(Boolean, default=True, nullable=False)
    run_on_startup = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    rounds = relationship("ChallengeRound", back_populates="definition")
    series_assignments = relationship("ChallengeDefinitionSeriesScd2", back_populates="definition")


class ChallengeDefinitionSeriesScd2(Base):
    """
    SCD Type 2 table tracking which time series are assigned to 
    each challenge definition over time.
    """
    __tablename__ = 'definition_series_scd2'
    __table_args__ = {'schema': 'challenges'}
    
    sk = Column(BigInteger, primary_key=True)
    definition_id = Column(Integer, ForeignKey('challenges.definitions.id', ondelete="CASCADE"), nullable=False)
    series_id = Column(Integer, ForeignKey('data_portal.time_series.series_id', ondelete="CASCADE"), nullable=False)
    is_required = Column(Boolean, default=True, nullable=False)
    valid_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    valid_to = Column(DateTime(timezone=True))
    is_current = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    definition = relationship("ChallengeDefinition", back_populates="series_assignments")


class ChallengeRound(Base):
    """
    Individual challenge round instance. 
    Each row represents one execution of a ChallengeDefinition.
    """
    __tablename__ = 'rounds'
    __table_args__ = {'schema': 'challenges'}
    
    id = Column(Integer, primary_key=True)
    definition_id = Column(Integer, ForeignKey('challenges.definitions.id'))
    name = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    context_length = Column(Integer, nullable=False)
    horizon = Column(Interval, nullable=False)
    frequency = Column(Interval, nullable=True)
    registration_start = Column(DateTime(timezone=True))
    registration_end = Column(DateTime(timezone=True))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    preparation_params = Column(JSONB, nullable=True)
    status = Column(Text, default='announced')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    definition = relationship("ChallengeDefinition", back_populates="rounds")
    participants = relationship("ChallengeParticipant", back_populates="round")
    context_data = relationship("ChallengeContextData", back_populates="round")
    forecasts = relationship("Forecast", back_populates="round")
    scores = relationship("ChallengeScore", back_populates="round")
    series_pseudo = relationship("ChallengeSeriesPseudo", back_populates="round")


class ChallengeParticipant(Base):
    __tablename__ = 'participants'
    __table_args__ = (
        UniqueConstraint('round_id', 'model_id', name='_round_model_uc'),
        {'schema': 'challenges'}
    )

    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey('challenges.rounds.id', ondelete="CASCADE"))
    model_id = Column(Integer, ForeignKey('models.model_info.id', ondelete="CASCADE"))
    registered_at = Column(DateTime(timezone=True), server_default=func.now())

    round = relationship("ChallengeRound", back_populates="participants")
    model = relationship("ModelInfo", back_populates="participants")


class ChallengeContextData(Base):
    __tablename__ = 'context_data'
    __table_args__ = {'schema': 'challenges'}

    id = Column(BigInteger, primary_key=True)
    round_id = Column(Integer, ForeignKey('challenges.rounds.id', ondelete="CASCADE"))
    series_id = Column(Integer, ForeignKey('data_portal.time_series.series_id', ondelete="CASCADE"), nullable=False)
    ts = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    value = Column(Float)
    series_metadata = Column("metadata", JSONB)

    round = relationship("ChallengeRound", back_populates="context_data")


class ChallengeSeriesPseudo(Base):
    __tablename__ = 'series_pseudo'
    __table_args__ = {'schema': 'challenges'}

    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey('challenges.rounds.id', ondelete="CASCADE"))
    series_id = Column(Integer, ForeignKey('data_portal.time_series.series_id', ondelete="CASCADE"), nullable=False)
    challenge_series_name = Column(Text, nullable=False)
    min_ts = Column(DateTime(timezone=True))
    max_ts = Column(DateTime(timezone=True))
    value_avg = Column(Float)
    value_std = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    round = relationship("ChallengeRound", back_populates="series_pseudo")


class VChallengeRoundWithStatus(Base):
    """
    Read-only model for the challenges.v_rounds_with_status view.
    """
    __tablename__ = 'v_rounds_with_status'
    __table_args__ = {'schema': 'challenges', 'info': dict(is_view=True)}

    id = Column(Integer, primary_key=True)
    definition_id = Column(Integer)
    name = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    context_length = Column(Integer, nullable=False)
    horizon = Column(Interval, nullable=False)
    frequency = Column(Interval, nullable=True)
    registration_start = Column(DateTime(timezone=True))
    registration_end = Column(DateTime(timezone=True))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    preparation_params = Column(JSONB, nullable=True)
    status = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    # From joined definition
    definition_schedule_id = Column(Text)
    definition_name = Column(Text)
    definition_domains = Column(ARRAY(Text))
    definition_subdomains = Column(ARRAY(Text))
    definition_categories = Column(ARRAY(Text))
    definition_subcategories = Column(ARRAY(Text))
    computed_status = Column(String)
