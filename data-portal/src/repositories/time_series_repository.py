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
        category: str = "",
        subcategory: str = "",
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
            category: Category
            subcategory: Subcategory
            update_frequency: How often data is updated, as ISO 8601 or PostgreSQL format
            
        Returns:
            series_id
        """
        # Get or create domain_category_id
        domain_category_id = await self._get_or_create_domain_category_id(domain, category, subcategory)
        
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
    
    async def _get_or_create_domain_category_id(self, domain: str, category: str, subcategory: str) -> int:
        """
        Get existing domain_category_id or create new domain category entry.
        
        Args:
            domain: Domain name
            category: Category name
            subcategory: Subcategory name
            
        Returns:
            domain_category_id
        """
        # Try to find existing domain_category
        # Use IS NOT DISTINCT FROM for nullable columns to handle NULL/None correctly
        # Note: In asyncpg/SQLAlchemy text(), we need to handle NULLs explicitly if we used =
        # but IS NOT DISTINCT FROM works for both NULL and values.
        # However, for simplicity with empty strings defaulting to '', we check:
        # If the input is empty string, we treat it as such. 
        # init_db.sql schema allows NULL for category/subcategory. 
        # But here we are passing strings (default "").
        # Let's assume we store empty strings as NULL in DB? 
        # Or store them as empty strings? 
        # The schema has strict UNIQUE(domain, category, subcategory).
        # Typically NULL != NULL in SQL unique constraint (except in recent PG versions with NULLS NOT DISTINCT).
        # Let's check init_db.sql again.
        # It's standard UNIQUE. So (A, NULL, NULL) and (A, NULL, NULL) would be duplicate key violation if we insert strictly?
        # No, standard SQL says unique allows multiple NULLs. 
        # BUT we want to reuse the ID.
        # If we insert NULLs, we might get multiple rows which is bad for "domain_category".
        # So we probably want to treat empty string as NULL or just Use empty string if that's the convention.
        # The sources.yaml implies strings. 
        # Let's stick to strings for now. If they are None, we pass None.
        
        # Adjust arguments to be optional? The signature above has them as str = "".
        
        query = text("""
            SELECT id FROM data_portal.domain_category 
            WHERE domain = :domain 
            AND (category = :category OR (category IS NULL AND :category IS NULL))
            AND (subcategory = :subcategory OR (subcategory IS NULL AND :subcategory IS NULL))
        """)
        
        # Convert empty strings to None if preferred, OR keep as empty strings.
        # For now, let's keep strict equality but handle the NULL case if passed.
        
        result = await self.session.execute(query, {
            "domain": domain, 
            "category": category if category else None, 
            "subcategory": subcategory if subcategory else None
        })
        row = result.fetchone()
        
        if row:
            return row[0]
        
        # Create new domain_category
        insert_query = text("""
            INSERT INTO data_portal.domain_category (domain, category, subcategory)
            VALUES (:domain, :category, :subcategory)
            RETURNING id
        """)
        
        result = await self.session.execute(
            insert_query,
            {
                "domain": domain, 
                "category": category if category else None,
                "subcategory": subcategory if subcategory else None
            }
        )
        await self.session.commit()
        
        row = result.fetchone()
        domain_category_id = row[0] if row else None
        
        if domain_category_id is None:
            raise ValueError(f"Failed to create domain category for {domain}/{category}/{subcategory}")
        
        logger.info(f"Created new domain category: {domain}/{category}/{subcategory} (id={domain_category_id})")
        return domain_category_id
