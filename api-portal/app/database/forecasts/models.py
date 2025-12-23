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
        UniqueConstraint("challenge_id", "model_id", "series_id", "ts"),
        {"schema": "forecasts"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    challenge_id = Column(Integer, ForeignKey("challenges.challenges.id"), nullable=False)
    model_id = Column(Integer, ForeignKey("models.model_info.id"), nullable=False)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), nullable=False)
    ts = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    predicted_value = Column(Float, nullable=False)
    probabilistic_values = Column("probabilistic_values", JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    challenge = relationship("Challenge", back_populates="forecasts")
    model = relationship("ModelInfo", back_populates="forecasts")


class ChallengeScore(Base):
    __tablename__ = "challenge_scores"
    __table_args__ = (
        UniqueConstraint("challenge_id", "model_id", "series_id"),
        {"schema": "forecasts"},
    )

    id = Column(Integer, primary_key=True, index=True)
    challenge_id = Column(Integer, ForeignKey("challenges.challenges.id"), nullable=False)
    model_id = Column(Integer, ForeignKey("models.model_info.id"), nullable=False)
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), nullable=False)
    mase = Column(Float)
    rmse = Column(Float)
    final_evaluation = Column("final_evaluation", Boolean, server_default="false")
    calculated_at = Column(DateTime(timezone=True), server_default=func.now())

    challenge = relationship("Challenge", back_populates="scores")
    model = relationship("ModelInfo", back_populates="scores")
