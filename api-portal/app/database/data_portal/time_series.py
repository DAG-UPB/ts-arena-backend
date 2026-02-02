# app/database/models/time_series.py
from sqlalchemy import Column, Integer, DateTime, Text, ForeignKey, func, Float, Interval
from sqlalchemy.orm import relationship
from app.database.connection import Base

class DomainCategoryModel(Base):
    __tablename__ = "domain_category"
    __table_args__ = {"schema": "data_portal"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(Text, nullable=False)
    category = Column(Text)
    subcategory = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    time_series = relationship("TimeSeriesModel", back_populates="domain_category")


class TimeSeriesModel(Base):
    __tablename__ = "time_series"
    __table_args__ = {"schema": "data_portal"}
    
    series_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    api_endpoint = Column(Text)
    frequency = Column(Interval)
    unit = Column(Text)
    update_frequency = Column(Text, nullable=False)
    domain_category_id = Column(Integer, ForeignKey("data_portal.domain_category.id"))
    unique_id = Column(Text, unique=True, nullable=False)
    ts_timezone = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    data = relationship("TimeSeriesDataModel", back_populates="series", cascade="all, delete-orphan")
    domain_category = relationship("DomainCategoryModel", back_populates="time_series")


class TimeSeriesDataModel(Base):
    __tablename__ = "time_series_data"
    __table_args__ = {"schema": "data_portal"}
    
    series_id = Column(Integer, ForeignKey("data_portal.time_series.series_id", ondelete="CASCADE"), primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    series = relationship("TimeSeriesModel", back_populates="data")

    def __repr__(self):
        return f"<TimeSeriesDataModel(series_id='{self.series_id}', ts='{self.ts}')>"


# ==========================================================================
# Read-Only Models for Continuous Aggregate Views
# ==========================================================================

class TimeSeriesData15minModel(Base):
    """Read-only model for time_series_15min continuous aggregate."""
    __tablename__ = "time_series_15min"
    __table_args__ = {"schema": "data_portal"}
    
    series_id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)
    sample_count = Column(Integer)
    min_value = Column(Float)
    max_value = Column(Float)


class TimeSeriesData1hModel(Base):
    """Read-only model for time_series_1h continuous aggregate."""
    __tablename__ = "time_series_1h"
    __table_args__ = {"schema": "data_portal"}
    
    series_id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)
    sample_count = Column(Integer)
    min_value = Column(Float)
    max_value = Column(Float)


class TimeSeriesData1dModel(Base):
    """Read-only model for time_series_1d continuous aggregate."""
    __tablename__ = "time_series_1d"
    __table_args__ = {"schema": "data_portal"}
    
    series_id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)
    sample_count = Column(Integer)
    min_value = Column(Float)
    max_value = Column(Float)