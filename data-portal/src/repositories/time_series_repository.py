"""Repository for writing time series data to TimescaleDB"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import isodate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def validate_and_normalize_interval(interval_str: str) -> str:
    """
    Validate and normalize interval strings to ISO 8601 format.
    
    Supports:
    - ISO 8601 durations: 'PT1H', 'PT15M', 'P1D' -> validates and returns as-is
    - PostgreSQL INTERVAL strings: '1 hour', '15 minutes', '1 day' -> converts to ISO 8601
    
    Args:
        interval_str: Interval string in ISO 8601 or PostgreSQL format
        
    Returns:
        ISO 8601 duration string (e.g., 'PT1H', 'PT15M', 'P1D')
        
    Raises:
        ValueError: If the interval string cannot be parsed
    """
    interval_str = interval_str.strip()
    
    # Try ISO 8601 format first (e.g., 'PT1H', 'PT15M', 'P1D')
    if interval_str.startswith('P'):
        try:
            # Validate that it's a valid ISO 8601 duration
            duration = isodate.parse_duration(interval_str)
            # Convert to timedelta if needed
            if not isinstance(duration, timedelta):
                duration = duration.totimedelta(start=datetime.now())
            # Return original ISO 8601 string if valid
            return interval_str
        except (isodate.ISO8601Error, AttributeError) as e:
            logger.warning(f"Failed to parse ISO 8601 duration '{interval_str}': {e}")
    
    # Try to parse PostgreSQL INTERVAL format (e.g., '1 hour', '15 minutes', '1 day')
    match = re.match(r'^(\d+)\s*(minute|hour|day|week|second)s?$', interval_str.lower())
    
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        
        # Convert to ISO 8601 duration format
        if unit == 'second':
            return f"PT{value}S"
        elif unit == 'minute':
            return f"PT{value}M"
        elif unit == 'hour':
            return f"PT{value}H"
        elif unit == 'day':
            return f"P{value}D"
        elif unit == 'week':
            return f"P{value}W"
    
    raise ValueError(
        f"Invalid interval format: '{interval_str}'. "
        f"Expected ISO 8601 (e.g., 'PT1H', 'PT15M') or PostgreSQL format (e.g., '1 hour', '15 minutes')"
    )

class TimeSeriesDataRepository:
    """Repository for time series data operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_or_create_series_id(
        self, 
        name: str, 
        endpoint_prefix: str,
        description: str = "",
        frequency: str = "1 hour",
        unit: str = "",
        domain: str = "",
        subdomain: str = "",
        update_frequency: str = "1 day"
    ) -> int:
        """
        Get existing series_id or create new time series metadata entry.
        
        Args:
            name: Name of the time series
            endpoint_prefix: Unique endpoint prefix
            description: Description
            frequency: Data frequency as ISO 8601 (e.g., 'PT1H', 'PT15M') or PostgreSQL format (e.g., '1 hour', '15 minutes')
            unit: Unit of measurement
            domain: Domain category
            subdomain: Subdomain category
            update_frequency: How often data is updated, as ISO 8601 or PostgreSQL format
            
        Returns:
            series_id
        """
        # Get or create domain_category_id
        domain_category_id = await self._get_or_create_domain_category_id(domain, subdomain)
        
        # Try to find existing series
        query = text("""
            SELECT series_id FROM data_portal.time_series WHERE endpoint_prefix = :endpoint_prefix
        """)
        result = await self.session.execute(query, {"endpoint_prefix": endpoint_prefix})
        row = result.fetchone()
        
        if row:
            return row[0]
        
        # Validate and normalize interval strings to ISO 8601, then convert to timedelta
        try:
            frequency_iso = validate_and_normalize_interval(frequency)
            frequency_dt = isodate.parse_duration(frequency_iso)
            # Convert to timedelta if needed
            if not isinstance(frequency_dt, timedelta):
                frequency_dt = frequency_dt.totimedelta(start=datetime.now())
            
            update_frequency_iso = validate_and_normalize_interval(update_frequency)
            update_frequency_dt = isodate.parse_duration(update_frequency_iso)
            # Convert to timedelta if needed
            if not isinstance(update_frequency_dt, timedelta):
                update_frequency_dt = update_frequency_dt.totimedelta(start=datetime.now())
        except ValueError as e:
            logger.error(f"Failed to parse interval for series '{name}': {e}")
            raise
        
        # Create new series - use strings with CAST in SQL for asyncpg + text()
        insert_query = text("""
            INSERT INTO data_portal.time_series (
                name, description, frequency, unit, update_frequency, 
                domain_category_id, endpoint_prefix
            )
            VALUES (
                :name, :description, :frequency, :unit, :update_frequency,
                :domain_category_id, :endpoint_prefix
            )
            RETURNING series_id
        """)
        
        result = await self.session.execute(
            insert_query,
            {
                "name": name,
                "endpoint_prefix": endpoint_prefix,
                "description": description,
                "frequency": frequency_dt,  # Use timedelta directly
                "unit": unit,
                "update_frequency": update_frequency,
                "domain_category_id": domain_category_id
            }
        )
        await self.session.commit()
        
        row = result.fetchone()
        series_id = row[0] if row else None
        
        if series_id is None:
            raise ValueError(f"Failed to create time series for {name}")
        
        logger.info(f"Created new time series: {name} (series_id={series_id})")
        return series_id
    
    async def upsert_data_points(
        self,
        series_id: int,
        data_points: List[Dict[str, Any]]
    ) -> int:
        """
        Insert or update time series data points using PostgreSQL UPSERT.
        
        Args:
            series_id: The series ID
            data_points: List of dicts with 'timestamp' and 'value' keys
            
        Returns:
            Number of rows affected
        """
        if not data_points:
            return 0
        
        # Prepare data for bulk insert
        values = []
        for point in data_points:
            timestamp = point.get('ts')
            value = point.get('value')
            
            if timestamp is None or value is None:
                continue
            
            # Convert timestamp to datetime if it's a string
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            values.append({
                'series_id': series_id,
                'ts': timestamp,
                'value': float(value),
                'updated_at': datetime.utcnow()
            })
        
        if not values:
            logger.warning(f"No valid data points to insert for series_id={series_id}")
            return 0
        
        # Use PostgreSQL INSERT ... ON CONFLICT DO UPDATE
        stmt = text("""
            INSERT INTO data_portal.time_series_data (series_id, ts, value, updated_at)
            VALUES (:series_id, :ts, :value, :updated_at)
            ON CONFLICT (series_id, ts) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at
        """)
        
        try:
            for value_dict in values:
                await self.session.execute(stmt, value_dict)
            
            await self.session.commit()
            logger.info(f"Upserted {len(values)} data points for series_id={series_id}")
            return len(values)
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Failed to upsert data points for series_id={series_id}: {e}", exc_info=True)
            raise
    
    async def get_latest_timestamp(self, series_id: int) -> Optional[datetime]:
        """
        Get the most recent timestamp for a given series.
        
        Args:
            series_id: The series ID
            
        Returns:
            Latest ts or None if no data exists
        """
        query = text("""
            SELECT MAX(ts) FROM data_portal.time_series_data WHERE series_id = :series_id
        """)
        result = await self.session.execute(query, {"series_id": series_id})
        row = result.fetchone()
        return row[0] if row and row[0] else None
    
    async def get_data_count(self, series_id: int) -> int:
        """
        Get total count of data points for a series.
        
        Args:
            series_id: The series ID
            
        Returns:
            Count of data points
        """
        query = text("""
            SELECT COUNT(*) FROM data_portal.time_series_data WHERE series_id = :series_id
        """)
        result = await self.session.execute(query, {"series_id": series_id})
        row = result.fetchone()
        return row[0] if row else 0
    
    async def _get_or_create_domain_category_id(self, domain: str, subdomain: str) -> int:
        """
        Get existing domain_category_id or create new domain category entry.
        
        Args:
            domain: Domain name
            subdomain: Subdomain name (used as category)
            
        Returns:
            domain_category_id
        """
        # Try to find existing domain_category
        query = text("""
            SELECT id FROM data_portal.domain_category 
            WHERE domain = :domain AND category = :subdomain
        """)
        result = await self.session.execute(query, {"domain": domain, "subdomain": subdomain})
        row = result.fetchone()
        
        if row:
            return row[0]
        
        # Create new domain_category
        insert_query = text("""
            INSERT INTO data_portal.domain_category (domain, category)
            VALUES (:domain, :subdomain)
            RETURNING id
        """)
        
        result = await self.session.execute(
            insert_query,
            {"domain": domain, "subdomain": subdomain}
        )
        await self.session.commit()
        
        row = result.fetchone()
        domain_category_id = row[0] if row else None
        
        if domain_category_id is None:
            raise ValueError(f"Failed to create domain category for {domain}/{subdomain}")
        
        logger.info(f"Created new domain category: {domain}/{subdomain} (id={domain_category_id})")
        return domain_category_id
