# app/services/data_service.py
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta, timezone
from app.database.repositories.time_series_repository import TimeSeriesRepository
import logging

logger = logging.getLogger(__name__)

class DataService:
    """Service for data operations with time series (read-only)."""

    def __init__(self, db_session):
        """Initializes the DataService."""
        self.db_session = db_session
        self.time_series_repo = TimeSeriesRepository(db_session)

    async def get_all_time_series_metadata(self) -> List[Dict[str, Any]]:
        """Retrieves all time series metadata from the database."""
        time_series = await self.time_series_repo.get_all_time_series()
        return [ts.__dict__ for ts in time_series]

    async def get_metadata(self, unique_id: str) -> Optional[Any]:
        """Retrieves the metadata for a specific time series from the database."""
        return await self.time_series_repo.get_time_series_by_unique_id(unique_id)


    async def get_data(
        self,
        unique_id: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        resolution: str = "raw"
    ) -> List[Dict[str, Any]]:
        """
        Retrieves persisted time series data from the database.
        If no time range is specified, a default time range is calculated.
        
        Args:
            unique_id: The unique id of the time series.
            start_date: Optional start date.
            end_date: Optional end date.
            resolution: Data resolution ("raw", "15min", "1h", "1d").
        """
        ts = await self.time_series_repo.get_time_series_by_unique_id(unique_id)
        if not ts:
            raise ValueError(f"Time series '{unique_id}' not found.")

        if not start_date or not end_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)
            end_date = datetime.now(timezone.utc)

        def to_datetime_utc(dt):
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        start_dt = to_datetime_utc(start_date)
        end_dt = to_datetime_utc(end_date)
        
        return await self.time_series_repo.get_data_by_time_range_by_resolution(
            series_id=ts.series_id,
            start_time=start_dt,
            end_time=end_dt,
            resolution=resolution
        )
