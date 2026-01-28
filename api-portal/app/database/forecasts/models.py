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
