# app/database/data_portal/time_series_repository.py
from typing import List, Optional, Dict, Any, NamedTuple, Union, Type
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, desc, and_, text, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.database.data_portal.time_series import (
    TimeSeriesModel, 
    TimeSeriesDataModel, 
    DomainCategoryModel,
    TimeSeriesData15minModel,
    TimeSeriesData1hModel,
    TimeSeriesData1dModel
)
import logging
import re
import isodate

logger = logging.getLogger(__name__)


# ==========================================================================
# Resolution to Model Mapping
# ==========================================================================

# Maps resolution strings to the appropriate Continuous Aggregate Model
RESOLUTION_MODEL_MAP: Dict[str, Type] = {
    "15min": TimeSeriesData15minModel,
    "15 minutes": TimeSeriesData15minModel,
    "1h": TimeSeriesData1hModel,
    "1 hour": TimeSeriesData1hModel,
    "1d": TimeSeriesData1dModel,
    "1 day": TimeSeriesData1dModel,
    "raw": TimeSeriesDataModel,  # For Admin/Debug only
}

# Maps resolution strings to the SQL interval literal used by TimescaleDB's time_bucket(),
# mirroring the continuous aggregate definitions in init_db.sql (~line 782): each aggregate
# is `time_bucket(<interval>, ts)` grouped AVG(value) over data_portal.time_series_data.
# Used by the raw-bucketed fallback (`get_raw_bucketed_value_at`) when a continuous
# aggregate has no/incomplete data for the requested time range (e.g. a dev restore that
# only covers recent weeks) — never by the live aggregate-reading path above.
# timedelta values (not strings): asyncpg encodes them as typed `interval` params,
# which the CAST(:interval AS interval) in the raw-bucketing queries requires.
RESOLUTION_TO_BUCKET_INTERVAL: Dict[str, timedelta] = {
    "15min": timedelta(minutes=15),
    "15 minutes": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1 hour": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1 day": timedelta(days=1),
}

# Continuous aggregate view + time_bucket() literal per resolution, for the publication-edge
# union read below (backend-87). The bucket literals must match the aggregate definitions in
# init_db.sql exactly — the union's live branch has to produce the same buckets the
# materialised branch holds.
RESOLUTION_TO_VIEW: Dict[str, str] = {
    "15min": "data_portal.time_series_15min",
    "15 minutes": "data_portal.time_series_15min",
    "1h": "data_portal.time_series_1h",
    "1 hour": "data_portal.time_series_1h",
    "1d": "data_portal.time_series_1d",
    "1 day": "data_portal.time_series_1d",
}

RESOLUTION_TO_BUCKET_LITERAL: Dict[str, str] = {
    "15min": "15 minutes",
    "15 minutes": "15 minutes",
    "1h": "1 hour",
    "1 hour": "1 hour",
    "1d": "1 day",
    "1 day": "1 day",
}

class AggregateSource(NamedTuple):
    """How to read a continuous aggregate for one round (backend-87).

    `realtime` — the aggregate already unions the live tail itself, so read it directly.
    `edge`     — otherwise, the watermark to split our own union at; None means nothing is
                 materialised and the live branch covers everything.
    """

    realtime: bool
    edge: Optional[datetime]


# Maps resolution strings to timedelta for validation
RESOLUTION_INTERVALS: Dict[str, timedelta] = {
    "15min": timedelta(minutes=15),
    "15 minutes": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1 hour": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1 day": timedelta(days=1),
}


def in_progress_bucket_start(
    resolution: str,
    now: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    Start of the continuous aggregate bucket that currently contains `now` — the one bucket
    that is still filling, and the only one whose AVG is not yet the value it will settle on.

    This replaces what the aggregates' `end_offset` used to do (backend-87). `end_offset`
    conflated "incomplete" with "recent": it withheld the filling bucket, but in doing so it
    also made the view unable to hold *any* bucket newer than the watermark, including
    future-dated ones. For a source that publishes ahead of delivery — SMARD day-ahead prices
    are public from ~12:45 CET on D-1 — that capped `max(ts)` at ~now, so a round's
    `start_time` (derived as `max_ts + frequency`) opened inside already-published data and the
    forecast was a lookup rather than a forecast.

    Excluding exactly this one bucket keeps the protection and drops the amputation: older
    buckets are returned as before (short ones included — see below), and future buckets are
    returned in full.

    Deliberately NOT a `sample_count` completeness test, which is what backend-87 first
    proposed. Measured on dev 2026-09-11:

    - `bucket_width / series.frequency` is not an integer for 110 of 287 series: a 15-minute
      bucket over a 10-minute series (all Gridstatus, all Tankerkoenig) holds 1 or 2 points,
      never a fixed count, so an equality test is undefined rather than merely strict.
    - Short buckets are normal, not artefacts. Tankerkoenig 1 h buckets over 14 days: 1527 at
      6/6 but 234 at 5/6 and 15 at 4/6. And for the def-1 price series the only partial buckets
      in 14 days sit at 2026-08-28 07:00 and 2026-09-08 21:00 — historical ingest gaps strictly
      in the past, not at the *now* edge. Filtering on completeness would punch holes in the
      middle of the context handed to participants.
    - `time_series.imputation_policy` is NULL for all 287 series, so it cannot inform a guard.

    Returns the bucket start, or None for resolutions with no fixed bucket width ("raw"), for
    which there is nothing to trim.

    The floor matches `time_bucket()`'s origin: 15 min, 1 h and 1 d all divide the Unix epoch
    evenly, so flooring the epoch second is the same boundary TimescaleDB computes.
    """
    width = RESOLUTION_TO_BUCKET_INTERVAL.get(resolution)
    if width is None:
        return None

    now = now or datetime.now(timezone.utc)
    width_seconds = int(width.total_seconds())
    epoch_seconds = int(now.timestamp())
    return datetime.fromtimestamp(
        epoch_seconds - (epoch_seconds % width_seconds), tz=timezone.utc
    )


def parse_interval_string_to_timedelta(interval_str: str) -> timedelta:
    """
    Convert various interval string formats to Python timedelta.
    
    NOTE: This function is used for ORM queries where SQLAlchemy/asyncpg expects
    timedelta objects for INTERVAL column comparisons. For raw SQL with text() and CAST,
    use the string directly as asyncpg expects strings for CAST(:param AS INTERVAL).
    
    Supports:
    - ISO 8601 durations: 'PT1H', 'PT15M', 'P1D'
    - PostgreSQL INTERVAL strings: '1 hour', '15 minutes', '1 day'
    
    Args:
        interval_str: Interval string in ISO 8601 or PostgreSQL format
        
    Returns:
        timedelta object
        
    Raises:
        ValueError: If the interval string cannot be parsed
    """
    interval_str = interval_str.strip()
    
    # Try ISO 8601 format first (e.g., 'PT1H', 'PT15M', 'P1D')
    if interval_str.startswith('P'):
        try:
            duration = isodate.parse_duration(interval_str)
            # isodate can return timedelta or Duration, ensure we get timedelta
            if isinstance(duration, timedelta):
                return duration
            else:
                # Convert Duration to timedelta (approximation for months/years)
                return duration.totimedelta(start=datetime.now())
        except (isodate.ISO8601Error, AttributeError) as e:
            logger.warning(f"Failed to parse ISO 8601 duration '{interval_str}': {e}")
    
    # Try PostgreSQL INTERVAL format (e.g., '1 hour', '15 minutes', '1 day')
    match = re.match(r'^(\d+)\s*(minute|hour|day|week)s?$', interval_str.lower())
    
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'minute':
            return timedelta(minutes=value)
        elif unit == 'hour':
            return timedelta(hours=value)
        elif unit == 'day':
            return timedelta(days=value)
        elif unit == 'week':
            return timedelta(weeks=value)
    
    raise ValueError(
        f"Invalid interval format: '{interval_str}'. "
        f"Expected ISO 8601 (e.g., 'PT1H', 'PT15M') or PostgreSQL format (e.g., '1 hour', '15 minutes')"
    )


class TimeSeriesRepository:
    """
    Repository for reading time series metadata and data points (read-only).
    Supports querying by time range, last N points, bulk operations, and copy functions.
    """
    
    def __init__(self, session: AsyncSession):
        self.session = session

    # ==========================================================================
    # Metadata Operations (TimeSeriesModel) - Read-Only
    # ==========================================================================

    async def get_time_series_by_id(self, series_id: int) -> Optional[TimeSeriesModel]:
        """Retrieves a time series by its ID."""
        try:
            query = select(TimeSeriesModel).where(TimeSeriesModel.series_id == series_id)
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error retrieving time series with ID {series_id}: {e}")
            raise
            
    async def get_time_series_by_name(self, name: str) -> Optional[TimeSeriesModel]:
        """Retrieves a time series by its name."""
        try:
            query = select(TimeSeriesModel).where(TimeSeriesModel.name == name)
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error retrieving time series with name '{name}': {e}")
            raise
        
    async def get_time_series_by_unique_id(self, unique_id: str) -> Optional[TimeSeriesModel]:
        """Retrieves a time series by its unique id."""
        try:
            query = select(TimeSeriesModel).where(TimeSeriesModel.unique_id == unique_id)
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error retrieving time series with unique id '{unique_id}': {e}")
            raise

    async def get_all_time_series(self, skip: int = 0, limit: int = 100) -> List[TimeSeriesModel]:
        """Retrieves a list of all time series metadata entries."""
        try:
            query = select(TimeSeriesModel).offset(skip).limit(limit)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error retrieving all time series: {e}")
            raise

    async def filter_time_series_by_metadata(
        self,
        domains: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        subcategories: Optional[List[str]] = None,
        frequency: Optional[str] = None,
        unit: Optional[str] = None
    ) -> List[int]:
        """
        Filters time series by metadata and returns only their series_id.
        
        Args:
            domains: Filter by list of domains (from domain_category table)
            categories: Filter by list of categories (from domain_category table)
            subcategories: Filter by list of subcategories (from domain_category table)
            frequency: Filter by frequency
            unit: Filter by unit
            
        Returns:
            List of series_id that match the filter criteria
        """
        try:
            # Build the base query
            query = select(TimeSeriesModel.series_id)
            
            # Build filter conditions
            conditions = []
            
            # If any domain_category filter is specified, we need to join
            if domains or categories or subcategories:
                # Join with domain_category table using ORM relationship
                query = query.join(
                    DomainCategoryModel,
                    TimeSeriesModel.domain_category_id == DomainCategoryModel.id
                )
                
                if domains:
                    conditions.append(DomainCategoryModel.domain.in_(domains))
                if categories:
                    conditions.append(DomainCategoryModel.category.in_(categories))
                if subcategories:
                    conditions.append(DomainCategoryModel.subcategory.in_(subcategories))
            
            # Add time series specific filters
            if frequency:
                # Convert string to timedelta for asyncpg compatibility
                # asyncpg expects timedelta objects for INTERVAL columns
                frequency_td = parse_interval_string_to_timedelta(frequency)
                conditions.append(TimeSeriesModel.frequency == frequency_td)
            if unit:
                conditions.append(TimeSeriesModel.unit == unit)
            
            # Apply conditions if any
            if conditions:
                query = query.where(and_(*conditions))
            
            # Execute query
            result = await self.session.execute(query)
            
            series_ids = [row[0] for row in result.fetchall()]
            logger.info(f"Filtered time series by metadata: found {len(series_ids)} matching series")
            return series_ids
        except Exception as e:
            logger.error(f"Error filtering time series by metadata: {e}")
            raise

    # ==========================================================================
    # Single Time Series Data Operations - By Time Range
    # ==========================================================================

    async def get_data_by_time_range(
        self,
        series_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Retrieves data points for a single time series within a time range.
        
        Args:
            series_id: ID of the time series
            start_time: Start of the time range (inclusive)
            end_time: End of the time range (inclusive)
            
        Returns:
            List of data points with 'ts' and 'value' keys
        """
        try:
            query = select(
                TimeSeriesDataModel.ts,
                TimeSeriesDataModel.value
            ).where(
                and_(
                    TimeSeriesDataModel.series_id == series_id,
                    TimeSeriesDataModel.ts >= start_time,
                    TimeSeriesDataModel.ts <= end_time
                )
            ).order_by(TimeSeriesDataModel.ts)
            
            result = await self.session.execute(query)
            return [{"ts": row.ts, "value": row.value} for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error querying time series data for series_id {series_id}: {e}")
            raise

    async def get_last_n_points(
        self,
        series_id: int,
        n: int,
        before_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves the last N data points for a single time series.
        
        Args:
            series_id: ID of the time series
            n: Number of points to retrieve
            before_time: Optional cutoff time (exclusive). If None, gets the latest N points.
            
        Returns:
            List of data points ordered by time (ascending)
        """
        try:
            query = select(
                TimeSeriesDataModel.ts,
                TimeSeriesDataModel.value
            ).where(
                TimeSeriesDataModel.series_id == series_id
            )
            
            if before_time:
                query = query.where(TimeSeriesDataModel.ts < before_time)
            
            # Get last N points by descending order, then reverse
            query = query.order_by(desc(TimeSeriesDataModel.ts)).limit(n)
            
            result = await self.session.execute(query)
            data = [{"ts": row.ts, "value": row.value} for row in result.fetchall()]
            
            # Reverse to get chronological order
            return list(reversed(data))
        except Exception as e:
            logger.error(f"Error querying last {n} points for series_id {series_id}: {e}")
            raise

    # ==========================================================================
    # Bulk Time Series Data Operations
    # ==========================================================================

    async def get_bulk_data_by_time_range(
        self,
        series_ids: List[int],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Retrieves data for multiple time series within a time range.
        
        Args:
            series_ids: List of time series IDs
            start_time: Start of the time range (inclusive)
            end_time: End of the time range (inclusive)
            
        Returns:
            Dictionary mapping series_id to list of data points
        """
        try:
            query = select(
                TimeSeriesDataModel.series_id,
                TimeSeriesDataModel.ts,
                TimeSeriesDataModel.value
            ).where(
                and_(
                    TimeSeriesDataModel.series_id.in_(series_ids),
                    TimeSeriesDataModel.ts >= start_time,
                    TimeSeriesDataModel.ts <= end_time
                )
            ).order_by(TimeSeriesDataModel.series_id, TimeSeriesDataModel.ts)
            
            result = await self.session.execute(query)
            
            # Group by series_id
            data_by_series = {}
            for row in result.fetchall():
                if row.series_id not in data_by_series:
                    data_by_series[row.series_id] = []
                data_by_series[row.series_id].append({"ts": row.ts, "value": row.value})
            
            return data_by_series
        except Exception as e:
            logger.error(f"Error querying bulk time series data: {e}")
            raise

    async def get_bulk_last_n_points(
        self,
        series_ids: List[int],
        n: int,
        before_time: Optional[datetime] = None
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Retrieves the last N points for multiple time series.
        
        Args:
            series_ids: List of time series IDs
            n: Number of points to retrieve per series
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Dictionary mapping series_id to list of data points
        """
        try:
            # Use window function to get last N points per series
            # This is more efficient than querying each series separately
            subquery_parts = []
            for series_id in series_ids:
                sq = select(
                    TimeSeriesDataModel.series_id,
                    TimeSeriesDataModel.ts,
                    TimeSeriesDataModel.value
                ).where(
                    TimeSeriesDataModel.series_id == series_id
                )
                
                if before_time:
                    sq = sq.where(TimeSeriesDataModel.ts < before_time)
                
                sq = sq.order_by(desc(TimeSeriesDataModel.ts)).limit(n)
                subquery_parts.append(sq)
            
            # Execute all subqueries and combine results
            data_by_series = {}
            for sq in subquery_parts:
                result = await self.session.execute(sq)
                for row in result.fetchall():
                    if row.series_id not in data_by_series:
                        data_by_series[row.series_id] = []
                    data_by_series[row.series_id].append({"ts": row.ts, "value": row.value})
            
            # Reverse each series to get chronological order
            for series_id in data_by_series:
                data_by_series[series_id] = list(reversed(data_by_series[series_id]))
            
            return data_by_series
        except Exception as e:
            logger.error(f"Error querying bulk last {n} points: {e}")
            raise

    # ==========================================================================
    # Copy Functions - Time Series Data to Challenge Context Data
    # ==========================================================================

    async def copy_last_n_to_challenge(
        self,
        series_id: int,
        series_name: str,
        round_id: int,
        n: int,
        before_time: Optional[datetime] = None
    ) -> int:
        """
        Copies the last N data points from a time series to challenge context data.
        
        Args:
            series_id: Source time series ID
            series_name: Series identifier for challenge context data
            round_id: Target round ID
            n: Number of points to copy
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Number of rows copied
        """
        try:
            # Get the last N points
            data = await self.get_last_n_points(series_id, n, before_time)
            
            if not data:
                logger.warning(f"No data found to copy for series_id {series_id}")
                return 0
            
            # Prepare bulk insert
            values = [
                {
                    "round_id": round_id,
                    "series_id": series_id,
                    "ts": point["ts"],
                    "value": point["value"],
                    "metadata": None
                }
                for point in data
            ]
            
            # Use raw SQL for better performance with TimescaleDB
            stmt = text("""
                INSERT INTO challenges.context_data 
                (round_id, series_id, ts, value, metadata)
                VALUES (:round_id, :series_id, :ts, :value, :metadata)
                ON CONFLICT (round_id, series_id, ts) DO NOTHING
            """)
            
            for value in values:
                await self.session.execute(stmt, value)
            
            await self.session.flush()
            
            logger.info(f"Copied {len(data)} points from series_id {series_id} to round {round_id}")
            return len(data)
        except Exception as e:
            logger.error(f"Error copying data to round: {e}")
            raise


    async def copy_bulk_to_challenge(
        self,
        series_mapping: Dict[int, str],
        round_id: int,
        n: int,
        before_time: Optional[datetime] = None
    ) -> Dict[int, int]:
        """
        Copies data from multiple time series to challenge context data.
        Either specify n for last N points, or start_time/end_time for time range.
        
        Args:
            series_mapping: Dictionary mapping series_id to series_name for challenge
            round_id: Target round ID
            n: Number of last points to copy per series (mutually exclusive with time range)            
        Returns:
            Dictionary mapping series_id to number of rows copied
        """
        try:
            result = {}
            
            # Copy last N points for each series
            for series_id, series_name in series_mapping.items():
                count = await self.copy_last_n_to_challenge(
                    series_id, series_name, round_id, n, before_time
                )
                result[series_id] = count
            logger.info(f"Bulk copied data to round {round_id}: {sum(result.values())} total points")
            return result
        except Exception as e:
            logger.error(f"Error in bulk copy to round: {e}")
            raise

    # ==========================================================================
    # Data Availability Check
    # ==========================================================================

    async def filter_time_series_with_recent_data(
        self,
        domains: Optional[List[str]] = None,
        subdomains: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        subcategories: Optional[List[str]] = None,
        frequency: Optional[str] = None,
        only_with_recent_data: bool = True
    ) -> List[int]:
        """
        Filters time series by metadata and data availability using v_data_availability view.
        
        Args:
            domains: Filter by domains (None or empty = no filter)
            subdomains: Filter by subdomains (None or empty = no filter)
            categories: Filter by categories
            subcategories: Filter by subcategories
            frequency: Filter by frequency
            only_with_recent_data: If True, only return series with recent data based on their frequency
            
        Returns:
            List of series_id that match the filter criteria and have recent data
        """
        try:
            # Build query using the v_data_availability view
            # Build query dynamically to avoid asyncpg type inference issues
            query_parts = ["SELECT series_id FROM data_portal.v_data_availability WHERE 1=1"]
            params = {}
            
            # Add filters only if values are provided (not None or "mixed")
            # Add filters only if values are provided
            if domains and "mixed" not in domains:
                query_parts.append("AND domain = ANY(:domains)")
                params["domains"] = domains
            
            if subdomains and "mixed" not in subdomains:
                query_parts.append("AND subdomain = ANY(:subdomains)")
                params["subdomains"] = subdomains
            
            if categories and "mixed" not in categories:
                query_parts.append("AND category = ANY(:categories)")
                params["categories"] = categories
            
            if subcategories and "mixed" not in subcategories:
                query_parts.append("AND subcategory = ANY(:subcategories)")
                params["subcategories"] = subcategories
            
            if frequency:
                # Convert string to timedelta for asyncpg compatibility
                # asyncpg expects timedelta objects for INTERVAL columns
                # Use <= to include finer granularity series (e.g., 3min series for 15min challenge)
                frequency_td = parse_interval_string_to_timedelta(frequency)
                query_parts.append("AND frequency <= :frequency")
                params["frequency"] = frequency_td
            
            if only_with_recent_data:
                query_parts.append("AND has_recent_data = TRUE")
            
            query_parts.append("ORDER BY series_id")
            query = text(" ".join(query_parts))
            
            result = await self.session.execute(query, params)
            series_ids = [row[0] for row in result.fetchall()]
            
            logger.info(
                f"Filtered time series with recent data: "
                f"Filtered time series with recent data: "
                f"domains={domains}, subdomains={subdomains}, categories={categories}, subcategories={subcategories}, "
                f"frequency={frequency}, only_recent={only_with_recent_data} -> "
                f"found {len(series_ids)} series"
            )
            return series_ids
        except Exception as e:
            logger.error(f"Error filtering time series with recent data: {e}")
            raise

    async def check_data_completeness(
        self,
        series_ids: List[int],
        start_time: datetime,
        end_time: datetime,
        expected_frequency: str,
        completeness_threshold: float = 0.5
    ) -> bool:
        """
        Checks if data for the given series is complete within the time range
        based on the expected frequency.
        
        Args:
            series_ids: List of series IDs to check
            start_time: Start of the time range (inclusive)
            end_time: End of the time range (inclusive)
            expected_frequency: Expected frequency string (e.g., "15 minutes", "1 hour", "PT15M")
            completeness_threshold: Minimum ratio of actual/expected data points (default 0.95 = 95%)
            
        Returns:
            True if all series have sufficient data coverage, False otherwise
        """
        if not expected_frequency:
            logger.warning("No expected_frequency provided, using basic existence check")
            # Fallback to simple existence check
            for series_id in series_ids:
                query = select(TimeSeriesDataModel).where(
                    TimeSeriesDataModel.series_id == series_id,
                    TimeSeriesDataModel.ts >= start_time,
                    TimeSeriesDataModel.ts <= end_time
                ).limit(1)
                
                result = await self.session.execute(query)
                if result.scalar_one_or_none() is None:
                    logger.warning(f"No data found for series {series_id} in range {start_time} to {end_time}")
                    return False
            return True
        
        try:
            # Parse frequency to timedelta
            frequency_td = parse_interval_string_to_timedelta(expected_frequency)
            
            # Calculate expected number of data points
            time_range = end_time - start_time
            expected_points = int(time_range / frequency_td) + 1  # +1 to include both endpoints
            
            if expected_points <= 0:
                logger.warning(f"Invalid expected_points calculation: {expected_points} for range {start_time} to {end_time}")
                return False
            
            logger.info(
                f"Checking data completeness: frequency={expected_frequency}, "
                f"range={time_range}, expected_points={expected_points}, "
                f"threshold={completeness_threshold}"
            )
            
            # Check each series
            for series_id in series_ids:
                # Count actual data points in the range
                query = select(func.count(TimeSeriesDataModel.series_id)).where(
                    TimeSeriesDataModel.series_id == series_id,
                    TimeSeriesDataModel.ts >= start_time,
                    TimeSeriesDataModel.ts <= end_time
                )
                
                result = await self.session.execute(query)
                actual_points = result.scalar_one()
                
                if actual_points == 0:
                    logger.warning(
                        f"Data completeness check failed for series {series_id}: "
                        f"no data points found in range {start_time} to {end_time}"
                    )
                    return False
                
                # Calculate completeness ratio
                completeness_ratio = actual_points / expected_points
                
                if completeness_ratio < completeness_threshold:
                    logger.warning(
                        f"Data completeness check failed for series {series_id}: "
                        f"actual_points={actual_points}, expected_points={expected_points}, "
                        f"ratio={completeness_ratio:.2%} < threshold={completeness_threshold:.2%}"
                    )
                    return False
                
                logger.info(
                    f"Series {series_id} completeness OK: "
                    f"actual_points={actual_points}, expected_points={expected_points}, "
                    f"ratio={completeness_ratio:.2%}"
                )
            
            return True
            
        except ValueError as e:
            logger.error(f"Failed to parse expected_frequency '{expected_frequency}': {e}")
            # Fallback to basic existence check if frequency parsing fails
            for series_id in series_ids:
                query = select(TimeSeriesDataModel).where(
                    TimeSeriesDataModel.series_id == series_id,
                    TimeSeriesDataModel.ts >= start_time,
                    TimeSeriesDataModel.ts <= end_time
                ).limit(1)
                
                result = await self.session.execute(query)
                if result.scalar_one_or_none() is None:
                    logger.warning(f"No data found for series {series_id} in range {start_time} to {end_time}")
                    return False
            return True

    # ==========================================================================
    # Context Data Statistics
    # ==========================================================================

    async def calculate_context_data_stats(
        self,
        round_id: int,
        series_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Calculates statistics for context data of a specific series in a challenge.
        
        Args:
            round_id: The round ID
            series_id: The series ID
            
        Returns:
            Dictionary with keys: min_ts, max_ts, value_avg, value_std
            Returns None if no data found
        """
        try:
            query = text("""
                SELECT 
                    MIN(ts) as min_ts,
                    MAX(ts) as max_ts,
                    AVG(value) as value_avg,
                    STDDEV(value) as value_std
                FROM challenges.context_data
                WHERE round_id = :round_id
                  AND series_id = :series_id
            """)
            
            result = await self.session.execute(
                query, 
                {"round_id": round_id, "series_id": series_id}
            )
            row = result.fetchone()
            
            if row and row.min_ts is not None:
                return {
                    "min_ts": row.min_ts,
                    "max_ts": row.max_ts,
                    "value_avg": float(row.value_avg) if row.value_avg is not None else None,
                    "value_std": float(row.value_std) if row.value_std is not None else None
                }
            
            logger.warning(f"No context data found for round {round_id}, series {series_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating context data stats: {e}")
            raise

    # ==========================================================================
    # Resolution-Based Data Access (Continuous Aggregate Views)
    # ==========================================================================

    async def get_aggregate_source(self, resolution: str) -> "AggregateSource":
        """
        How this resolution's aggregate should be read, resolved once per round.

        If the aggregate already has real-time aggregation on
        (`timescaledb.materialized_only = false`), TimescaleDB performs the
        materialised/live union itself and the view already returns the publication edge —
        so we read it directly and do no union of our own.

        Otherwise we supply the split point and do the union in the query (backend-87). The
        split is the newest materialised bucket, which in materialised-only mode is what
        `max(ts)` returns.

        Checking the mode rather than assuming it matters because the two environments differ
        and may converge later: the `ALTER` is impossible on dev (backend-88) but works on
        prod, verified there under `BEGIN … ROLLBACK`. Reading `max(ts)` unconditionally would
        be actively harmful the moment prod is switched — with real-time aggregation on, an
        unfiltered `max(ts)` over the view computes the live branch for *every* series above
        the watermark, not a cheap index max.
        """
        if resolution == "raw" or RESOLUTION_MODEL_MAP.get(resolution) is None:
            return AggregateSource(realtime=False, edge=None)

        view = RESOLUTION_TO_VIEW[resolution]
        schema, _, name = view.partition(".")
        realtime = await self.session.execute(
            text("""
                SELECT NOT materialized_only
                  FROM timescaledb_information.continuous_aggregates
                 WHERE view_schema = :schema AND view_name = :name
            """),
            {"schema": schema, "name": name},
        )
        if realtime.scalar():
            return AggregateSource(realtime=True, edge=None)

        model = RESOLUTION_MODEL_MAP[resolution]
        edge = await self.session.execute(select(func.max(model.ts)))
        return AggregateSource(realtime=False, edge=edge.scalar())

    async def get_last_n_points_by_resolution(
        self,
        series_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None,
        source: Optional["AggregateSource"] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves the last N data points from the appropriate view based on resolution.

        For an aggregate resolution this reads the aggregate UNION a live `time_bucket` over
        raw above the aggregate's watermark — which is precisely what TimescaleDB's own
        real-time aggregation does internally (backend-87).

        We do it in the query rather than by setting `timescaledb.materialized_only = false`
        because that setting cannot be applied on **dev**, and dev is where changes are
        validated. On dev, TimescaleDB does not intercept DDL at all: `ALTER MATERIALIZED
        VIEW` fails with "is not a materialized view" (a cagg is `relkind = 'v'`, so the
        statement only works when TimescaleDB rewrites it), and `ALTER VIEW` *and even*
        `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous, ...)` fail with
        "unrecognized parameter namespace timescaledb" — from psql as superuser and owner.
        Ruled out: client (psql and JDBC alike), licence (`timescale`, not apache), ownership,
        extension version (2.24.0 on disk and installed, matching), and deprecation (real-time
        aggregation is alive in 2.24). Dev's cagg refresh policies have also never run, which
        is the same fault seen from the other side.

        Prod is healthy — its refresh policies have run 50k+ times — so the ALTER would very
        likely succeed there. This union is used regardless, because it is the only form that
        works on both, and an A-path that cannot be exercised on dev cannot be verified before
        it reaches prod. It stays correct if real-time aggregation is ever enabled: the two
        branches are split at the watermark, so they are disjoint either way, and enabling it
        would make this a safe simplification rather than a behaviour change.

        Why this matters: without the live branch the aggregate can never return a bucket past
        its watermark, `end_offset` holds that watermark at ~now, and so `max(ts)` — which the
        round's forecast window is derived from — capped at ~now. For SMARD day-ahead prices,
        public from ~12:45 CET on D-1, that put the whole forecast window inside already
        published data.

        The split point is spliced into the SQL as a literal on purpose. As a bind parameter
        it is opaque at plan time, so Postgres cannot exclude chunks and scans every one of
        them: measured at ~28x slower (a constant ~57 ms/series that did not vary with the
        watermark lag at all). It is a `datetime` we computed, never user input. With the
        literal, the cost is 93 ms for a whole 15-series context read at the design watermark
        lag, against 31.5 ms materialised-only — once per round.

        Args:
            series_id: ID of the time series
            n: Number of points to retrieve
            resolution: Target resolution ("15min", "1h", "1d", "raw")
            before_time: Optional cutoff time (exclusive)
            source: how to read the aggregate, from `get_aggregate_source`. Resolve it once
                per round and pass it in; omitted, it is looked up per call.

        Returns:
            List of data points ordered by time (ascending)

        Raises:
            ValueError: If resolution is not recognized
        """
        model = RESOLUTION_MODEL_MAP.get(resolution)
        if not model:
            raise ValueError(f"Unknown resolution: {resolution}. Valid: {list(RESOLUTION_MODEL_MAP.keys())}")

        try:
            if resolution == "raw":
                query = (
                    select(model.ts, model.value)
                    .where(model.series_id == series_id)
                )
                if before_time:
                    query = query.where(model.ts < before_time)
                query = query.order_by(desc(model.ts)).limit(n)
                result = await self.session.execute(query)
                data = [{"ts": row.ts, "value": row.value} for row in result.fetchall()]
                return list(reversed(data))

            if source is None:
                source = await self.get_aggregate_source(resolution)

            return await self._read_aggregate_with_live_tail(
                series_id=series_id,
                n=n,
                resolution=resolution,
                before_time=before_time,
                source=source,
            )
        except Exception as e:
            logger.error(f"Error querying last {n} points for series_id {series_id} with resolution {resolution}: {e}")
            raise

    async def _read_aggregate_with_live_tail(
        self,
        series_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime],
        source: "AggregateSource",
    ) -> List[Dict[str, Any]]:
        """The union read described in `get_last_n_points_by_resolution`, newest N first."""
        view = RESOLUTION_TO_VIEW[resolution]
        bucket = RESOLUTION_TO_BUCKET_LITERAL[resolution]

        # Drop the bucket that is still filling. The live branch below exposes it for the first
        # time — `end_offset` used to withhold it, at the cost of withholding the future with
        # it — and a partially averaged bucket must not become the context edge, or
        # `start_time = max_ts + frequency` opens the round over a stretch already partly
        # observed. Exactly one bucket goes; past short buckets and future ones stay.
        filling_bucket = in_progress_bucket_start(resolution)

        if source.realtime:
            # The aggregate already unions its own live tail, so it reaches the publication
            # edge on its own — no union of ours, and no watermark to split at.
            branches = f"""
                SELECT ts, value, sample_count
                  FROM {view}
                 WHERE series_id = :series_id
            """
        elif source.edge is None:
            # Nothing materialised: the live branch alone covers everything.
            branches = f"""
                SELECT time_bucket(interval '{bucket}', ts) AS ts,
                       AVG(value) AS value,
                       COUNT(*) AS sample_count
                  FROM data_portal.time_series_data
                 WHERE series_id = :series_id
                 GROUP BY 1
            """
        else:
            edge = self._timestamptz_literal(source.edge)
            branches = f"""
                SELECT ts, value, sample_count
                  FROM {view}
                 WHERE series_id = :series_id AND ts < {edge}
                UNION ALL
                SELECT time_bucket(interval '{bucket}', ts) AS ts,
                       AVG(value) AS value,
                       COUNT(*) AS sample_count
                  FROM data_portal.time_series_data
                 WHERE series_id = :series_id AND ts >= {edge}
                 GROUP BY 1
            """

        predicates = []
        if filling_bucket is not None:
            predicates.append(f"u.ts <> {self._timestamptz_literal(filling_bucket)}")
        if before_time is not None:
            predicates.append(f"u.ts < {self._timestamptz_literal(before_time)}")
        where = ("WHERE " + " AND ".join(predicates)) if predicates else ""

        query = text(f"""
            SELECT u.ts, u.value, u.sample_count
              FROM ({branches}) u
            {where}
             ORDER BY u.ts DESC
             LIMIT :n
        """)

        result = await self.session.execute(query, {"series_id": series_id, "n": n})
        data = [
            {"ts": row.ts, "value": row.value, "sample_count": row.sample_count}
            for row in result.fetchall()
        ]
        # Reverse to get chronological order
        return list(reversed(data))

    @staticmethod
    def _timestamptz_literal(value: datetime) -> str:
        """
        Render a datetime as a SQL `timestamptz` literal, for splicing into the union read.

        Splicing rather than binding is deliberate — see `get_last_n_points_by_resolution` for
        why (chunk exclusion). The type check is the guard that keeps it safe: only a real
        `datetime` is ever formatted, so there is no string from any caller reaching the SQL.
        """
        if not isinstance(value, datetime):
            raise TypeError(f"expected datetime for SQL timestamptz literal, got {type(value)!r}")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return f"timestamptz '{value.astimezone(timezone.utc).isoformat()}'"

    async def get_data_by_time_range_by_resolution(
        self,
        series_id: int,
        start_time: datetime,
        end_time: datetime,
        resolution: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieves data points for a single time series within a time range
        from the appropriate view based on resolution.
        
        Args:
            series_id: ID of the time series
            start_time: Start of the time range (inclusive)
            end_time: End of the time range (inclusive)
            resolution: Target resolution ("15min", "1h", "1d", "raw")
            
        Returns:
            List of data points with 'ts', 'value', and optionally 'sample_count' keys
        """
        model = RESOLUTION_MODEL_MAP.get(resolution)
        if not model:
            raise ValueError(f"Unknown resolution: {resolution}. Valid: {list(RESOLUTION_MODEL_MAP.keys())}")
        
        try:
            if resolution == "raw":
                query = select(
                    model.ts,
                    model.value
                ).where(
                    and_(
                        model.series_id == series_id,
                        model.ts >= start_time,
                        model.ts <= end_time
                    )
                ).order_by(model.ts)
            else:
                query = select(
                    model.ts,
                    model.value,
                    model.sample_count
                ).where(
                    and_(
                        model.series_id == series_id,
                        model.ts >= start_time,
                        model.ts <= end_time
                    )
                ).order_by(model.ts)
            
            result = await self.session.execute(query)
            
            if resolution == "raw":
                return [{"ts": row.ts, "value": row.value} for row in result.fetchall()]
            else:
                return [{"ts": row.ts, "value": row.value, "sample_count": row.sample_count} for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error querying time series data for series_id {series_id} with resolution {resolution}: {e}")
            raise

    async def get_raw_bucketed_value_at(
        self,
        series_id: int,
        resolution: str,
        target_ts: datetime
    ) -> Optional[float]:
        """
        Fallback for the single-bucket lookup in `get_data_by_time_range_by_resolution`
        (used for the naive/context value) when the continuous aggregate has no data for
        this series/time — e.g. a dev restore that only covers recent weeks. Reproduces
        the aggregate's bucketing directly from `data_portal.time_series_data`:
        AVG(value) over the bucket that contains `target_ts`, using TimescaleDB's
        `time_bucket()` so boundaries are identical to the continuous aggregate's.

        Does NOT read from or modify the continuous aggregates, and is never used by the
        live scoring path.

        Args:
            series_id: ID of the time series
            resolution: Target resolution ("15min", "1h", "1d")
            target_ts: timestamp whose containing bucket should be averaged

        Returns:
            The bucket's average value, or None if there is no raw data in that bucket.
        """
        interval = RESOLUTION_TO_BUCKET_INTERVAL.get(resolution)
        if not interval:
            raise ValueError(
                f"Unknown resolution for raw bucketing: {resolution}. "
                f"Valid: {list(RESOLUTION_TO_BUCKET_INTERVAL.keys())}"
            )

        # The ts range bound makes the scan sargable on the (series_id, ts) index; the
        # time_bucket equality then picks the exact bucket within that window.
        query = text("""
            SELECT AVG(value) AS value
            FROM data_portal.time_series_data
            WHERE series_id = :series_id
              AND ts >= CAST(:target_ts AS timestamptz) - CAST(:interval AS interval)
              AND ts <  CAST(:target_ts AS timestamptz) + CAST(:interval AS interval)
              AND time_bucket(CAST(:interval AS interval), ts) = time_bucket(CAST(:interval AS interval), CAST(:target_ts AS timestamptz))
        """)
        result = await self.session.execute(
            query, {"series_id": series_id, "interval": interval, "target_ts": target_ts}
        )
        row = result.first()
        if row is None or row.value is None:
            return None
        return float(row.value)

    async def validate_series_for_resolution(
        self,
        series_id: int,
        resolution: str
    ) -> bool:
        """
        Validates that a series is available in the requested resolution view.
        A series is available if its native frequency <= target resolution.
        
        Args:
            series_id: ID of the time series
            resolution: Target resolution ("15min", "1h", "1d")
            
        Returns:
            True if series is available in this resolution, False otherwise
        """
        series = await self.get_time_series_by_id(series_id)
        if not series or not series.frequency:
            return False
        
        target_interval = RESOLUTION_INTERVALS.get(resolution)
        if not target_interval:
            logger.warning(f"Unknown resolution for validation: {resolution}")
            return False
        
        # Series is available if native frequency <= target resolution
        return series.frequency <= target_interval

    async def copy_last_n_to_challenge_by_resolution(
        self,
        series_id: int,
        series_name: str,
        round_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None,
        source: Optional["AggregateSource"] = None,
    ) -> int:
        """
        Copies the last N data points from the appropriate resolution view to challenge context data.
        
        Args:
            series_id: Source time series ID
            series_name: Series identifier for challenge context data
            round_id: Target round ID
            n: Number of points to copy
            resolution: Target resolution ("15min", "1h", "1d")
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Number of rows copied
        """
        try:
            # Get the last N points from the resolution view
            data = await self.get_last_n_points_by_resolution(
                series_id, n, resolution, before_time, source
            )
            
            if not data:
                logger.warning(f"No data found to copy for series_id {series_id} with resolution {resolution}")
                return 0
            
            # Prepare bulk insert
            values = [
                {
                    "round_id": round_id,
                    "series_id": series_id,
                    "ts": point["ts"],
                    "value": point["value"],
                    "metadata": None
                }
                for point in data
            ]
            
            # Use raw SQL for better performance with TimescaleDB
            stmt = text("""
                INSERT INTO challenges.context_data 
                (round_id, series_id, ts, value, metadata)
                VALUES (:round_id, :series_id, :ts, :value, :metadata)
                ON CONFLICT (round_id, series_id, ts) DO NOTHING
            """)
            
            for value in values:
                await self.session.execute(stmt, value)
            
            await self.session.flush()
            
            logger.info(f"Copied {len(data)} points from series_id {series_id} (resolution: {resolution}) to round {round_id}")
            return len(data)
        except Exception as e:
            logger.error(f"Error copying data to round with resolution {resolution}: {e}")
            raise

    async def copy_bulk_to_challenge_by_resolution(
        self,
        series_mapping: Dict[int, str],
        round_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None
    ) -> Dict[int, int]:
        """
        Copies data from multiple time series (from appropriate resolution view) to challenge context data.
        
        Args:
            series_mapping: Dictionary mapping series_id to series_name for challenge
            round_id: Target round ID
            n: Number of last points to copy per series
            resolution: Target resolution ("15min", "1h", "1d")
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Dictionary mapping series_id to number of rows copied
        """
        try:
            result = {}

            # Resolved once for the whole round and shared by every series (backend-87) —
            # how the aggregate must be read is a property of the aggregate, not the series.
            source = await self.get_aggregate_source(resolution)

            # Copy last N points for each series from the resolution view
            for series_id, series_name in series_mapping.items():
                count = await self.copy_last_n_to_challenge_by_resolution(
                    series_id, series_name, round_id, n, resolution, before_time, source,
                )
                result[series_id] = count
            
            logger.info(f"Bulk copied data (resolution: {resolution}) to round {round_id}: {sum(result.values())} total points")
            return result
        except Exception as e:
            logger.error(f"Error in bulk copy to round with resolution {resolution}: {e}")
            raise
