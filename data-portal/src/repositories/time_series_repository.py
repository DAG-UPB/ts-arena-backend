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
    
    if interval_str.startswith('P'):
        try:
            duration = isodate.parse_duration(interval_str)
            if not isinstance(duration, timedelta):
                duration = duration.totimedelta(start=datetime.now())
            return interval_str
        except (isodate.ISO8601Error, AttributeError) as e:
            logger.warning(f"Failed to parse ISO 8601 duration '{interval_str}': {e}")
    
    match = re.match(r'^(\d+)\s*(minute|hour|day|week|second)s?$', interval_str.lower())
    
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        
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
        unique_id: str,
        description: str = "",
        frequency: str = "1 hour",
        unit: str = "",
        domain: str = "",
        category: str = "",
        subcategory: str = "",
        imputation_policy: Optional[str] = None,
        update_frequency: str = "1 day"
    ) -> int:
        """
        Get existing series_id or create new time series metadata entry.
        
        Args:
            name: Name of the time series
            unique_id: Unique unique id
            description: Description
            frequency: Data frequency as ISO 8601 (e.g., 'PT1H', 'PT15M') or PostgreSQL format (e.g., '1 hour', '15 minutes')
            unit: Unit of measurement
            domain: Domain category
            category: Category
            category: Category
            subcategory: Subcategory
            imputation_policy: Imputation policy (e.g., 'linear', 'ffill')
            update_frequency: How often data is updated, as ISO 8601 or PostgreSQL format
            
        Returns:
            series_id
        """
        domain_category_id = await self._get_or_create_domain_category_id(domain, category, subcategory)
        
        query = text("""
            SELECT series_id FROM data_portal.time_series WHERE unique_id = :unique_id
        """)
        result = await self.session.execute(query, {"unique_id": unique_id})
        row = result.fetchone()
        
        if row:
            return row[0]
        
        try:
            frequency_iso = validate_and_normalize_interval(frequency)
            frequency_dt = isodate.parse_duration(frequency_iso)
            if not isinstance(frequency_dt, timedelta):
                frequency_dt = frequency_dt.totimedelta(start=datetime.now())
            
            update_frequency_iso = validate_and_normalize_interval(update_frequency)
            update_frequency_dt = isodate.parse_duration(update_frequency_iso)
            if not isinstance(update_frequency_dt, timedelta):
                update_frequency_dt = update_frequency_dt.totimedelta(start=datetime.now())
        except ValueError as e:
            logger.error(f"Failed to parse interval for series '{name}': {e}")
            raise
        
        insert_query = text("""
            INSERT INTO data_portal.time_series (
                name, description, frequency, unit, update_frequency, 
                imputation_policy, domain_category_id, unique_id
            )
            VALUES (
                :name, :description, :frequency, :unit, :update_frequency,
                :imputation_policy, :domain_category_id, :unique_id
            )
            RETURNING series_id
        """)
        
        result = await self.session.execute(
            insert_query,
            {
                "name": name,
                "unique_id": unique_id,
                "description": description,
                "frequency": frequency_dt,  # Use timedelta directly
                "unit": unit,
                "update_frequency": update_frequency,
                "unit": unit,
                "update_frequency": update_frequency,
                "imputation_policy": imputation_policy,
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
    
    async def update_series_timezone(self, series_id: int, timezone: str) -> None:
        """
        Update the timezone for a time series.
        
        Args:
            series_id: The series ID
            timezone: Timezone string (e.g., 'US/Pacific', 'UTC', '+02:00')
        """
        if not timezone:
            return
            
        # Check if already set to avoid unnecessary updates? 
        # But upsert/update is cheap enough.
        
        query = text("""
            UPDATE data_portal.time_series 
            SET ts_timezone = :timezone 
            WHERE series_id = :series_id
            AND (ts_timezone IS NULL OR ts_timezone != :timezone)
        """)
        
        await self.session.execute(query, {
            "series_id": series_id,
            "timezone": timezone
        })
        await self.session.commit()
    
    async def upsert_data_points(
        self,
        series_id: int,
        data_points: List[Dict[str, Any]]
    ) -> int:
        """
        Insert or update time series data points using PostgreSQL UPSERT.
        Uses bulk insert with jsonb_array_elements for efficient batch processing.
        
        Args:
            series_id: The series ID
            data_points: List of dicts with 'ts' and 'value' keys
            
        Returns:
            Number of rows affected
        """
        import json
        
        if not data_points:
            return 0
        
        # Prepare and deduplicate data by timestamp (keep last value for duplicates)
        temp_dict = {}
        for point in data_points:
            timestamp = point.get('ts')
            value = point.get('value')
            
            if timestamp is None or value is None:
                continue
            
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            # Overwrite with later value if duplicate timestamp exists
            temp_dict[timestamp] = {
                'series_id': series_id,
                'ts': timestamp.isoformat(),
                'value': float(value)
            }
        
        values = list(temp_dict.values())
        
        if not values:
            logger.warning(f"No valid data points to insert for series_id={series_id}")
            return 0
        
        # Bulk upsert using jsonb_array_elements - single query instead of N queries
        stmt = text("""
            WITH input_data AS (
                SELECT 
                    (d->>'series_id')::int AS series_id,
                    (d->>'ts')::timestamptz AS ts,
                    (d->>'value')::double precision AS value
                FROM jsonb_array_elements(CAST(:data AS jsonb)) d
            )
            INSERT INTO data_portal.time_series_data (series_id, ts, value, updated_at)
            SELECT series_id, ts, value, NOW()
            FROM input_data
            ON CONFLICT (series_id, ts) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at
        """)
        
        try:
            await self.session.execute(stmt, {'data': json.dumps(values)})
            await self.session.commit()
            logger.info(f"Bulk upserted {len(values)} data points for series_id={series_id}")
            return len(values)
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Failed to bulk upsert data points for series_id={series_id}: {e}", exc_info=True)
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
        
        query = text("""
            SELECT id FROM data_portal.domain_category 
            WHERE domain = :domain 
            AND (category = :category OR (category IS NULL AND :category IS NULL))
            AND (subcategory = :subcategory OR (subcategory IS NULL AND :subcategory IS NULL))
        """)
                
        result = await self.session.execute(query, {
            "domain": domain, 
            "category": category if category else None, 
            "subcategory": subcategory if subcategory else None
        })
        row = result.fetchone()
        
        if row:
            return row[0]
        
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
