from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    ForeignKey,
    UniqueConstraint,
    BigInteger,
    Boolean,
    SmallInteger,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database.connection import Base


class Forecast(Base):
    __tablename__ = "forecasts"
    __table_args__ = (
        UniqueConstraint("round_id", "model_id", "series_id", "ts"),
        {"schema": "forecasts"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    round_id = Column(Integer, ForeignKey("challenges.rounds.id", ondelete="CASCADE"), nullable=False)
    model_id = Column(Integer, ForeignKey("models.model_info.id", ondelete="CASCADE"), nullable=False)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), nullable=False)
    ts = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    predicted_value = Column(Float, nullable=False)
    probabilistic_values = Column("probabilistic_values", JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    round = relationship("ChallengeRound", back_populates="forecasts")
    model = relationship("ModelInfo", back_populates="forecasts")


class ChallengeScore(Base):
    __tablename__ = "scores"
    __table_args__ = (
        UniqueConstraint("round_id", "model_id", "series_id"),
        {"schema": "forecasts"},
    )

    id = Column(Integer, primary_key=True, index=True)
    round_id = Column(Integer, ForeignKey("challenges.rounds.id", ondelete="CASCADE"), nullable=False)
    model_id = Column(Integer, ForeignKey("models.model_info.id", ondelete="CASCADE"), nullable=False)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), nullable=False)
    mase = Column(Float)
    rmse = Column(Float)
    # Probabilistic evaluation (Scaled Quantile Loss)
    sql_score = Column(Float)
    sql_per_quantile = Column(JSONB)
    has_quantiles = Column(Boolean)
    quantile_levels_count = Column(Integer)
    quantile_crossing_count = Column(Integer)
    forecast_count = Column(Integer, default=0)
    actual_count = Column(Integer, default=0)
    evaluated_count = Column(Integer, default=0)
    data_coverage = Column(Float, default=0.0)
    final_evaluation = Column("final_evaluation", Boolean, server_default="false")
    evaluation_status = Column(String, default="pending")
    error_message = Column(String, nullable=True)
    calculated_at = Column(DateTime(timezone=True), server_default=func.now())

    round = relationship("ChallengeRound", back_populates="scores")
    model = relationship("ModelInfo", back_populates="scores")


class SeriesScale(Base):
    """MASE denominator of one (round, series): the in-sample naive error of its context."""

    __tablename__ = "series_scale"
    __table_args__ = {"schema": "forecasts"}

    round_id = Column(Integer, ForeignKey("challenges.rounds.id", ondelete="CASCADE"), primary_key=True)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), primary_key=True)
    m = Column(SmallInteger, nullable=False)
    scale = Column(Float)
    n_points = Column(Integer, nullable=False)
    n_pairs = Column(Integer, nullable=False)
    context_start = Column(DateTime(timezone=True))
    context_end = Column(DateTime(timezone=True))
    source = Column(String, nullable=False)
    computed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class MaseScore(Base):
    """MASE and SQL of one (round, model, series) against `SeriesScale`."""

    __tablename__ = "scores_mase"
    __table_args__ = {"schema": "forecasts"}

    round_id = Column(Integer, ForeignKey("challenges.rounds.id", ondelete="CASCADE"), primary_key=True)
    model_id = Column(Integer, ForeignKey("models.model_info.id", ondelete="CASCADE"), primary_key=True)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), primary_key=True)
    mae = Column(Float)
    n_points = Column(Integer)
    scale = Column(Float)
    mase = Column(Float)
    sql_score = Column(Float)
    sql_per_quantile = Column(JSONB)
    has_quantiles = Column(Boolean)
    quantile_levels_count = Column(Integer)
    quantile_crossing_count = Column(Integer)
    forecast_count = Column(Integer)
    data_coverage = Column(Float)
    final_evaluation = Column(Boolean, nullable=False, server_default="false")
    evaluation_status = Column(String, nullable=False)
    error_message = Column(String)
    method = Column(String, nullable=False)
    calculated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
