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

    async def get_metadata(self, endpoint_prefix: str) -> Optional[Any]:
        """Retrieves the metadata for a specific time series from the database."""
        return await self.time_series_repo.get_time_series_by_endpoint_prefix(endpoint_prefix)


    async def get_data(
        self,
        endpoint_prefix: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves persisted time series data from the database.
        If no time range is specified, a default time range is calculated.
        """
        ts = await self.time_series_repo.get_time_series_by_endpoint_prefix(endpoint_prefix)
        if not ts:
            raise ValueError(f"Time series '{endpoint_prefix}' not found.")

        # Calculate default time range if none is specified
        if not start_date or not end_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)  # Default: last 7 days
            end_date = datetime.now(timezone.utc)

        # Date conversion
        def to_datetime_utc(dt):
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        filter_params = {
            "start_date": to_datetime_utc(start_date),
            "end_date": to_datetime_utc(end_date)
        }
        
        return await self.time_series_repo.query_time_series_data(ts.series_id, filter_params)
