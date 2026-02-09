"""Repository for SCD Type 2 time series data operations"""

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class TimeSeriesDataSCD2Repository:
    """
    Repository for SCD Type 2 (Slowly Changing Dimension Type 2) operations.
    Tracks historical changes to time series data values.
    """
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def upsert_data_points(
        self,
        series_id: int,
        data_points: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Insert or update time series data points using a robust and simplified
        SCD Type 2 approach. This method is designed to be atomic and
        efficient, handling bulk data by leveraging advanced SQL features.

        Args:
            series_id: The ID of the time series.
            data_points: A list of dictionaries, each with 'timestamp' and 'value'.

        Returns:
            A dictionary with counts of inserted, updated, and unchanged data points.
        """
        if not data_points:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}

        # Prepare data for bulk processing and deduplicate by timestamp.
        # If multiple values exist for the same timestamp, keep only the last one.
        # This prevents unique constraint violations when input data contains duplicates.
        temp_dict = {}
        for p in data_points:
            if p.get('ts') is not None:
                ts = datetime.fromisoformat(p['ts'].replace('Z', '+00:00')) if isinstance(p['ts'], str) else p['ts']
                # Value can be None for gap markers
                value = float(p['value']) if p.get('value') is not None else None
                # Default quality_code to 0 (original) if not provided
                quality_code = p.get('quality_code', 0)
                # Overwrite with later value if duplicate timestamp exists
                temp_dict[ts] = {
                    "series_id": series_id,
                    "ts": ts,
                    "value": value,
                    "quality_code": quality_code
                }
        
        values_to_upsert = list(temp_dict.values())

        if not values_to_upsert:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}

        try:
            # This single query handles inserts, and updates of existing records
            # in a single, atomic operation.
            upsert_query = text("""
WITH input_data(series_id, ts, value, quality_code) AS (
  SELECT (d->>'series_id')::int,
         (d->>'ts')::timestamptz,
         (d->>'value')::double precision,
         COALESCE((d->>'quality_code')::smallint, 0)
  FROM jsonb_array_elements(CAST(:data AS jsonb)) d
),
closed AS (
  UPDATE data_portal.time_series_data_scd2 t
  SET valid_to = NOW(),
      is_current = FALSE,
      updated_at = NOW()
  FROM input_data i
  WHERE t.series_id = i.series_id
    AND t.ts = i.ts
    AND t.is_current = TRUE
    AND (t.value IS DISTINCT FROM i.value OR t.quality_code IS DISTINCT FROM i.quality_code)
  RETURNING t.series_id, t.ts, t.sk
),
new_records AS (
  INSERT INTO data_portal.time_series_data_scd2
    (series_id, ts, value, quality_code, valid_from, valid_to, is_current, created_at)
  SELECT i.series_id, i.ts, i.value, i.quality_code, NOW(), NULL, TRUE, NOW()
  FROM input_data i
  WHERE EXISTS (SELECT 1 FROM closed c WHERE c.series_id = i.series_id AND c.ts = i.ts)
     OR NOT EXISTS (
       SELECT 1 FROM data_portal.time_series_data_scd2 t
       WHERE t.series_id = i.series_id 
         AND t.ts = i.ts 
         AND t.is_current = TRUE
     )
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM new_records) AS inserted_count,
  (SELECT COUNT(*) FROM closed) AS updated_count;

            """)

            result = await self.session.execute(
                upsert_query,
                {'data': json.dumps([
                    {
                        'series_id': v['series_id'], 
                        'ts': v['ts'].isoformat(), 
                        'value': v['value'],
                        'quality_code': v.get('quality_code', 0)
                    }
                    for v in values_to_upsert
                ])}
            )
            
            counts = result.fetchone()
            inserted = counts[0] if counts else 0
            updated = counts[1] if counts else 0
            unchanged = len(values_to_upsert) - (inserted + updated)

            stats = {
                'inserted': inserted,
                'updated': updated,
                'unchanged': unchanged if unchanged >= 0 else 0
            }

            await self.session.commit()

            logger.info(
                f"SCD2 upsert for series_id={series_id}: "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['unchanged']} unchanged."
            )

            return stats

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Failed to upsert SCD2 data for series_id={series_id}: {e}", exc_info=True)
            raise
    
    async def _close_current_version(self, sk: int) -> None:
        """
        Close the current version by setting valid_to = NOW() and is_current = FALSE.
        
        Args:
            sk: Surrogate key of the version to close
        """
        close_query = text("""
            UPDATE data_portal.time_series_data_scd2
            SET valid_to = NOW(),
                is_current = FALSE,
                updated_at = NOW()
            WHERE sk = :sk
        """)
        
        await self.session.execute(close_query, {'sk': sk})
    
    async def _insert_new_version(
        self,
        series_id: int,
        ts: datetime,
        value: float
    ) -> None:
        """
        Insert a new current version with conflict handling for race conditions.
        
        Args:
            series_id: The series ID
            ts: Data timestamp
            value: Data value
        """
        # Use INSERT ... ON CONFLICT to handle concurrent inserts gracefully
        # The UNIQUE constraint on (series_id, ts, valid_from) allows us to catch duplicates
        insert_query = text("""
            INSERT INTO data_portal.time_series_data_scd2 
                (series_id, ts, value, valid_from, valid_to, is_current, created_at)
            VALUES 
                (:series_id, :ts, :value, NOW(), NULL, TRUE, NOW())
            ON CONFLICT (series_id, ts, valid_from) DO NOTHING
        """)
        
        await self.session.execute(
            insert_query,
            {
                'series_id': series_id,
                'ts': ts,
                'value': value
            }
        )
    
    async def get_current_data(
        self,
        series_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get current (latest) version of data points.
        
        Args:
            series_id: The series ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of data points with timestamp and value
        """
        query_str = """
            SELECT ts, value, valid_from, valid_to
            FROM data_portal.time_series_data_scd2
            WHERE series_id = :series_id
              AND is_current = TRUE
        """
        
        params = {'series_id': series_id}
        
        if start_date:
            query_str += " AND ts >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query_str += " AND ts <= :end_date"
            params['end_date'] = end_date
        
        query_str += " ORDER BY ts"
        
        result = await self.session.execute(text(query_str), params)
        rows = result.fetchall()
        
        return [
            {
                'ts': row[0].isoformat(),
                'value': row[1],
                'valid_from': row[2].isoformat() if row[2] else None,
                'valid_to': row[3].isoformat() if row[3] else None
            }
            for row in rows
        ]
    
    async def get_data_at_time(
        self,
        series_id: int,
        as_of_time: datetime,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get data as it was at a specific point in time (time travel query).
        
        Args:
            series_id: The series ID
            as_of_time: The point in time to query
            start_date: Optional start date filter for data timestamps
            end_date: Optional end date filter for data timestamps
            
        Returns:
            List of data points as they were at as_of_time
        """
        query_str = """
            SELECT ts, value, valid_from, valid_to
            FROM data_portal.time_series_data_scd2
            WHERE series_id = :series_id
              AND valid_during @> :as_of_time
        """
        
        params = {
            'series_id': series_id,
            'as_of_time': as_of_time
        }
        
        if start_date:
            query_str += " AND ts >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query_str += " AND ts <= :end_date"
            params['end_date'] = end_date
        
        query_str += " ORDER BY ts"
        
        result = await self.session.execute(text(query_str), params)
        rows = result.fetchall()
        
        return [
            {
                'ts': row[0].isoformat(),
                'value': row[1],
                'valid_from': row[2].isoformat() if row[2] else None,
                'valid_to': row[3].isoformat() if row[3] else None
            }
            for row in rows
        ]
    
    async def get_value_history(
        self,
        series_id: int,
        ts: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get complete history of value changes for a specific data point.
        
        Args:
            series_id: The series ID
            ts: The data ts to get history for
            
        Returns:
            List of all versions ordered by valid_from
        """
        query = text("""
            SELECT sk, value, valid_from, valid_to, is_current, created_at, updated_at
            FROM data_portal.time_series_data_scd2
            WHERE series_id = :series_id
              AND ts = :ts
            ORDER BY valid_from
        """)
        
        result = await self.session.execute(
            query,
            {'series_id': series_id, 'ts': ts}
        )
        rows = result.fetchall()
        
        return [
            {
                'sk': row[0],
                'value': row[1],
                'valid_from': row[2].isoformat() if row[2] else None,
                'valid_to': row[3].isoformat() if row[3] else None,
                'is_current': row[4],
                'created_at': row[5].isoformat() if row[5] else None,
                'updated_at': row[6].isoformat() if row[6] else None
            }
            for row in rows
        ]
    
    async def get_changes_summary(
        self,
        series_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get summary statistics about data changes.
        
        Args:
            series_id: The series ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Dict with change statistics
        """
        query_str = """
            SELECT 
                COUNT(DISTINCT ts) as total_datapoints,
                COUNT(*) as total_versions,
                COUNT(*) - COUNT(DISTINCT ts) as total_changes,
                COUNT(CASE WHEN is_current = FALSE THEN 1 END) as historical_versions
            FROM data_portal.time_series_data_scd2
            WHERE series_id = :series_id
        """
        
        params = {'series_id': series_id}
        
        if start_date:
            query_str += " AND ts >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query_str += " AND ts <= :end_date"
            params['end_date'] = end_date
        
        result = await self.session.execute(text(query_str), params)
        row = result.fetchone()
        
        return {
            'series_id': series_id,
            'total_datapoints': row[0] if row else 0,
            'total_versions': row[1] if row else 0,
            'total_changes': row[2] if row else 0,
            'historical_versions': row[3] if row else 0
        }
