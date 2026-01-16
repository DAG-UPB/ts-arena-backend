# app/database/data_portal/time_series_repository.py
from typing import List, Optional, Dict, Any, Union, Type
from datetime import datetime, timedelta
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

# Maps resolution strings to timedelta for validation
RESOLUTION_INTERVALS: Dict[str, timedelta] = {
    "15min": timedelta(minutes=15),
    "15 minutes": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1 hour": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1 day": timedelta(days=1),
}


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
        
    async def get_time_series_by_endpoint_prefix(self, endpoint_prefix: str) -> Optional[TimeSeriesModel]:
        """Retrieves a time series by its endpoint prefix."""
        try:
            query = select(TimeSeriesModel).where(TimeSeriesModel.endpoint_prefix == endpoint_prefix)
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error retrieving time series with endpoint prefix '{endpoint_prefix}': {e}")
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
        domain: Optional[str] = None,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        frequency: Optional[str] = None,
        unit: Optional[str] = None
    ) -> List[int]:
        """
        Filters time series by metadata and returns only their series_id.
        
        Args:
            domain: Filter by domain (from domain_category table)
            category: Filter by category (from domain_category table)
            subcategory: Filter by subcategory (from domain_category table)
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
            if domain or category or subcategory:
                # Join with domain_category table using ORM relationship
                query = query.join(
                    DomainCategoryModel,
                    TimeSeriesModel.domain_category_id == DomainCategoryModel.id
                )
                
                if domain:
                    conditions.append(DomainCategoryModel.domain == domain)
                if category:
                    conditions.append(DomainCategoryModel.category == category)
                if subcategory:
                    conditions.append(DomainCategoryModel.subcategory == subcategory)
            
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
        challenge_id: int,
        n: int,
        before_time: Optional[datetime] = None
    ) -> int:
        """
        Copies the last N data points from a time series to challenge context data.
        
        Args:
            series_id: Source time series ID
            series_name: Series identifier for challenge context data
            challenge_id: Target challenge ID
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
                    "challenge_id": challenge_id,
                    "series_id": series_id,
                    "ts": point["ts"],
                    "value": point["value"],
                    "metadata": None
                }
                for point in data
            ]
            
            # Use raw SQL for better performance with TimescaleDB
            stmt = text("""
                INSERT INTO challenges.challenge_context_data 
                (challenge_id, series_id, ts, value, metadata)
                VALUES (:challenge_id, :series_id, :ts, :value, :metadata)
                ON CONFLICT (challenge_id, series_id, ts) DO NOTHING
            """)
            
            for value in values:
                await self.session.execute(stmt, value)
            
            await self.session.flush()
            
            logger.info(f"Copied {len(data)} points from series_id {series_id} to challenge {challenge_id}")
            return len(data)
        except Exception as e:
            logger.error(f"Error copying data to challenge: {e}")
            raise


    async def copy_bulk_to_challenge(
        self,
        series_mapping: Dict[int, str],
        challenge_id: int,
        n: int,
        before_time: Optional[datetime] = None
    ) -> Dict[int, int]:
        """
        Copies data from multiple time series to challenge context data.
        Either specify n for last N points, or start_time/end_time for time range.
        
        Args:
            series_mapping: Dictionary mapping series_id to series_name for challenge
            challenge_id: Target challenge ID
            n: Number of last points to copy per series (mutually exclusive with time range)            
        Returns:
            Dictionary mapping series_id to number of rows copied
        """
        try:
            result = {}
            
            # Copy last N points for each series
            for series_id, series_name in series_mapping.items():
                count = await self.copy_last_n_to_challenge(
                    series_id, series_name, challenge_id, n, before_time
                )
                result[series_id] = count
            logger.info(f"Bulk copied data to challenge {challenge_id}: {sum(result.values())} total points")
            return result
        except Exception as e:
            logger.error(f"Error in bulk copy to challenge: {e}")
            raise

    # ==========================================================================
    # Data Availability Check
    # ==========================================================================

    async def filter_time_series_with_recent_data(
        self,
        domain: Optional[str] = None,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        frequency: Optional[str] = None,
        only_with_recent_data: bool = True
    ) -> List[int]:
        """
        Filters time series by metadata and data availability using v_data_availability view.
        
        Args:
            domain: Filter by domain (None or "mixed" = no filter)
            category: Filter by category
            subcategory: Filter by subcategory (None or "mixed" = no filter)
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
            if domain and domain != "mixed":
                query_parts.append("AND domain = :domain")
                params["domain"] = domain
            
            if category and category != "mixed":
                query_parts.append("AND category = :category")
                params["category"] = category
            
            if subcategory and subcategory != "mixed":
                query_parts.append("AND subcategory = :subcategory")
                params["subcategory"] = subcategory
            
            if frequency:
                # Convert string to timedelta for asyncpg compatibility
                # asyncpg expects timedelta objects for INTERVAL columns
                frequency_td = parse_interval_string_to_timedelta(frequency)
                query_parts.append("AND frequency = :frequency")
                params["frequency"] = frequency_td
            
            if only_with_recent_data:
                query_parts.append("AND has_recent_data = TRUE")
            
            query_parts.append("ORDER BY series_id")
            query = text(" ".join(query_parts))
            
            result = await self.session.execute(query, params)
            series_ids = [row[0] for row in result.fetchall()]
            
            logger.info(
                f"Filtered time series with recent data: "
                f"domain={domain}, category={category}, subcategory={subcategory}, "
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
        challenge_id: int,
        series_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Calculates statistics for context data of a specific series in a challenge.
        
        Args:
            challenge_id: The challenge ID
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
                FROM challenges.challenge_context_data
                WHERE challenge_id = :challenge_id
                  AND series_id = :series_id
            """)
            
            result = await self.session.execute(
                query, 
                {"challenge_id": challenge_id, "series_id": series_id}
            )
            row = result.fetchone()
            
            if row and row.min_ts is not None:
                return {
                    "min_ts": row.min_ts,
                    "max_ts": row.max_ts,
                    "value_avg": float(row.value_avg) if row.value_avg is not None else None,
                    "value_std": float(row.value_std) if row.value_std is not None else None
                }
            
            logger.warning(f"No context data found for challenge {challenge_id}, series {series_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating context data stats: {e}")
            raise

    # ==========================================================================
    # Resolution-Based Data Access (Continuous Aggregate Views)
    # ==========================================================================

    async def get_last_n_points_by_resolution(
        self,
        series_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves the last N data points from the appropriate view based on resolution.
        
        Args:
            series_id: ID of the time series
            n: Number of points to retrieve
            resolution: Target resolution ("15min", "1h", "1d", "raw")
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            List of data points ordered by time (ascending)
            
        Raises:
            ValueError: If resolution is not recognized
        """
        model = RESOLUTION_MODEL_MAP.get(resolution)
        if not model:
            raise ValueError(f"Unknown resolution: {resolution}. Valid: {list(RESOLUTION_MODEL_MAP.keys())}")
        
        try:
            # Build query based on model type (raw vs aggregate)
            if resolution == "raw":
                query = select(
                    model.ts,
                    model.value
                ).where(
                    model.series_id == series_id
                )
            else:
                query = select(
                    model.ts,
                    model.value,
                    model.sample_count
                ).where(
                    model.series_id == series_id
                )
            
            if before_time:
                query = query.where(model.ts < before_time)
            
            query = query.order_by(desc(model.ts)).limit(n)
            
            result = await self.session.execute(query)
            
            if resolution == "raw":
                data = [{"ts": row.ts, "value": row.value} for row in result.fetchall()]
            else:
                data = [{"ts": row.ts, "value": row.value, "sample_count": row.sample_count} for row in result.fetchall()]
            
            # Reverse to get chronological order
            return list(reversed(data))
        except Exception as e:
            logger.error(f"Error querying last {n} points for series_id {series_id} with resolution {resolution}: {e}")
            raise

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
        challenge_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None
    ) -> int:
        """
        Copies the last N data points from the appropriate resolution view to challenge context data.
        
        Args:
            series_id: Source time series ID
            series_name: Series identifier for challenge context data
            challenge_id: Target challenge ID
            n: Number of points to copy
            resolution: Target resolution ("15min", "1h", "1d")
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Number of rows copied
        """
        try:
            # Get the last N points from the resolution view
            data = await self.get_last_n_points_by_resolution(series_id, n, resolution, before_time)
            
            if not data:
                logger.warning(f"No data found to copy for series_id {series_id} with resolution {resolution}")
                return 0
            
            # Prepare bulk insert
            values = [
                {
                    "challenge_id": challenge_id,
                    "series_id": series_id,
                    "ts": point["ts"],
                    "value": point["value"],
                    "metadata": None
                }
                for point in data
            ]
            
            # Use raw SQL for better performance with TimescaleDB
            stmt = text("""
                INSERT INTO challenges.challenge_context_data 
                (challenge_id, series_id, ts, value, metadata)
                VALUES (:challenge_id, :series_id, :ts, :value, :metadata)
                ON CONFLICT (challenge_id, series_id, ts) DO NOTHING
            """)
            
            for value in values:
                await self.session.execute(stmt, value)
            
            await self.session.flush()
            
            logger.info(f"Copied {len(data)} points from series_id {series_id} (resolution: {resolution}) to challenge {challenge_id}")
            return len(data)
        except Exception as e:
            logger.error(f"Error copying data to challenge with resolution {resolution}: {e}")
            raise

    async def copy_bulk_to_challenge_by_resolution(
        self,
        series_mapping: Dict[int, str],
        challenge_id: int,
        n: int,
        resolution: str,
        before_time: Optional[datetime] = None
    ) -> Dict[int, int]:
        """
        Copies data from multiple time series (from appropriate resolution view) to challenge context data.
        
        Args:
            series_mapping: Dictionary mapping series_id to series_name for challenge
            challenge_id: Target challenge ID
            n: Number of last points to copy per series
            resolution: Target resolution ("15min", "1h", "1d")
            before_time: Optional cutoff time (exclusive)
            
        Returns:
            Dictionary mapping series_id to number of rows copied
        """
        try:
            result = {}
            
            # Copy last N points for each series from the resolution view
            for series_id, series_name in series_mapping.items():
                count = await self.copy_last_n_to_challenge_by_resolution(
                    series_id, series_name, challenge_id, n, resolution, before_time
                )
                result[series_id] = count
            
            logger.info(f"Bulk copied data (resolution: {resolution}) to challenge {challenge_id}: {sum(result.values())} total points")
            return result
        except Exception as e:
            logger.error(f"Error in bulk copy to challenge with resolution {resolution}: {e}")
            raise
