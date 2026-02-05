import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
import psycopg2.extras

# Import utilities
from app.core.utils import parse_iso8601_to_interval_list


class ChallengeRepository:
    """Repository for Challenge data (ported from arena-app/src/database.py)."""
    
    def __init__(self, conn):
        self.conn = conn
    
        
    def list_definitions(self) -> List[Dict[str, Any]]:
        """List all challenge definitions."""
        query = """
            SELECT 
                id,
                schedule_id,
                name,
                description,
                domains,
                categories,
                subcategories,
                frequency,
                horizon,
                created_at
            FROM challenges.definitions
            ORDER BY name;
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return [dict(row) for row in cur.fetchall()]

    def get_definition(self, definition_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific challenge definition."""
        query = """
            SELECT 
                id,
                schedule_id,
                name,
                description,
                domains,
                categories,
                subcategories,
                frequency,
                horizon,
                created_at
            FROM challenges.definitions
            WHERE id = %s;
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (definition_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_definition_series(self, definition_id: int) -> List[Dict[str, Any]]:
        """List all time series that have appeared in any round of this definition."""
        query = """
            SELECT DISTINCT
                ts.series_id,
                ts.name,
                ts.description,
                ts.frequency,
                ts.unique_id,
                dc.domain,
                dc.category,
                dc.subcategory
            FROM challenges.rounds r
            JOIN challenges.series_pseudo csp ON csp.round_id = r.id
            JOIN data_portal.time_series ts ON ts.series_id = csp.series_id
            LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
            WHERE r.definition_id = %s
            ORDER BY ts.name;
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (definition_id,))
            return [dict(row) for row in cur.fetchall()]

    def list_rounds(
        self,
        status: Optional[List[str]] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        domains: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        subcategories: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,  # ISO 8601 Strings
        horizons: Optional[List[str]] = None,     # ISO 8601 Strings
        definition_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        List all challenge rounds with optional filters.
        """
        
        # Use new view with challenge frequency
        query = """
            SELECT 
                round_id as id,
                COALESCE(definition_id, 0) as definition_id,
                name,
                description,
                registration_start,
                registration_end,
                start_time,
                end_time,
                status,
                n_time_series,
                context_length,
                horizon,
                frequency,
                created_at,
                model_count,
                forecast_count,
                -- Metadata arrays
                domains,
                categories,
                subcategories
            FROM challenges.v_rounds_with_metadata
            WHERE 1=1
        """
        
        params = []

        if definition_id:
            query += " AND definition_id = %s"
            params.append(definition_id)
        
        if status and len(status) > 0:
            placeholders = ','.join(['%s'] * len(status))
            query += f" AND status IN ({placeholders})"
            params.extend(status)
        
        if from_date:
            query += " AND end_time >= %s"
            params.append(from_date)
        
        if to_date:
            query += " AND end_time <= %s"
            params.append(to_date)
        
        if domains and len(domains) > 0:
            placeholders = ','.join(['%s'] * len(domains))
            query += f" AND domains && ARRAY[{placeholders}]::TEXT[]"
            params.extend(domains)
        
        if categories and len(categories) > 0:
            placeholders = ','.join(['%s'] * len(categories))
            query += f" AND categories && ARRAY[{placeholders}]::TEXT[]"
            params.extend(categories)
        
        if subcategories and len(subcategories) > 0:
            placeholders = ','.join(['%s'] * len(subcategories))
            query += f" AND subcategories && ARRAY[{placeholders}]::TEXT[]"
            params.extend(subcategories)
        
        # Filter by challenge frequency (ISO 8601 → INTERVAL, direct comparison)
        if frequencies and len(frequencies) > 0:
            try:
                interval_strings = parse_iso8601_to_interval_list(frequencies)
                frequency_conditions = []
                for interval_str in interval_strings:
                    frequency_conditions.append(f"frequency = INTERVAL '{interval_str}'")
                query += f" AND ({' OR '.join(frequency_conditions)})"
            except ValueError as e:
                print(f"ERROR: Invalid frequency format: {e}", file=sys.stderr)
                # Optional: raise HTTPException or ignore
        
        if horizons and len(horizons) > 0:
            try:
                interval_strings = parse_iso8601_to_interval_list(horizons)
                horizon_conditions = []
                for interval_str in interval_strings:
                    horizon_conditions.append(f"horizon = INTERVAL '{interval_str}'")
                query += f" AND ({' OR '.join(horizon_conditions)})"
            except ValueError as e:
                print(f"ERROR: Invalid horizon format: {e}", file=sys.stderr)
        
        query += """
            ORDER BY
                CASE status
                    WHEN 'active' THEN 0
                    WHEN 'registration' THEN 1
                    WHEN 'completed' THEN 2
                    ELSE 4
                END,
                created_at DESC;
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            results = []
            for row in cur.fetchall():
                row_dict = dict(row)
                # Convert timedelta to ISO 8601 strings
                
                results.append(row_dict)
            return results
    
    
    def get_challenge_series(self, challenge_id: int) -> List[Dict[str, Any]]:
        """Time series for a challenge with domain information."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    csp.series_id,
                    ts.name,
                    ts.description,
                    ts.frequency as frequency,
                    c.horizon,
                    ts.unique_id,
                    c.start_time as start_time,
                    c.end_time as end_time,
                    c.registration_start as registration_start,
                    c.registration_end as registration_end,
                    csp.min_ts as context_start_time,
                    csp.max_ts as context_end_time,
                    dc.domain,
                    dc.category,
                    dc.subcategory
                FROM challenges.series_pseudo csp
                JOIN data_portal.time_series ts ON ts.series_id = csp.series_id
                JOIN challenges.v_rounds_with_status c ON c.id = csp.round_id
                LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
                WHERE csp.round_id = %s
                ORDER BY ts.name ASC;
                """,
                (challenge_id,),
            )
            return [dict(row) for row in cur.fetchall()]
    
    def get_challenge_metadata(self) -> Dict[str, List[str]]:
        """
        Returns all available filter options.
        
        Returns:
            Dict with lists of unique values for each filter dimension
        """
        query = """
            WITH metadata AS (
                SELECT 
                    -- Unnest arrays to individual values
                    UNNEST(domains) as domain,
                    UNNEST(categories) as category,
                    UNNEST(subcategories) as subcategory,
                    frequency,  -- Challenge frequency (direct, not unnested)
                    horizon,
                    status
                FROM challenges.v_rounds_with_metadata
            )
            SELECT
                -- Aggregate unique values
                ARRAY_AGG(DISTINCT domain ORDER BY domain) 
                    FILTER (WHERE domain IS NOT NULL) as domains,
                ARRAY_AGG(DISTINCT category ORDER BY category) 
                    FILTER (WHERE category IS NOT NULL) as categories,
                ARRAY_AGG(DISTINCT subcategory ORDER BY subcategory) 
                    FILTER (WHERE subcategory IS NOT NULL) as subcategories,
                ARRAY_AGG(DISTINCT frequency ORDER BY frequency) 
                    FILTER (WHERE frequency IS NOT NULL) as frequencies,
                ARRAY_AGG(DISTINCT horizon ORDER BY horizon) 
                    FILTER (WHERE horizon IS NOT NULL) as horizons,
                ARRAY_AGG(DISTINCT status ORDER BY status) 
                    FILTER (WHERE status IS NOT NULL) as statuses
            FROM metadata;
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            
            if not result:
                return {
                    "frequencies": [],
                    "horizons": [],
                    "domains": [],
                    "categories": [],
                    "subcategories": [],
                    "statuses": []
                }
            
            # Convert timedelta to ISO 8601 strings
            from app.schemas.challenge import serialize_timedelta_to_iso8601
            
            return {
                "frequencies": [serialize_timedelta_to_iso8601(f) for f in (result['frequencies'] or [])],
                "horizons": [serialize_timedelta_to_iso8601(h) for h in (result['horizons'] or [])],
                "domains": result['domains'] or [],
                "categories": result['categories'] or [],
                "subcategories": result['subcategories'] or [],
                "statuses": result['statuses'] or []
            }
    # Frequency-to-resolution mapping for auto-derivation
    FREQUENCY_RESOLUTION_MAP = {
        # timedelta values mapped to resolution strings
        # Using seconds for comparison
        900: "15min",      # 15 minutes
        3600: "1h",        # 1 hour  
        86400: "1d",       # 1 day
    }
    
    def get_challenge_frequency(self, challenge_id: int) -> Optional[str]:
        """
        Retrieves the challenge frequency and maps it to a resolution string.
        
        Args:
            challenge_id: ID of the challenge
            
        Returns:
            Resolution string ("15min", "1h", "1d") or None if not found
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT frequency
                FROM challenges.rounds
                WHERE id = %s
                """,
                (challenge_id,),
            )
            row = cur.fetchone()
            if not row or not row.get('frequency'):
                return None
            
            # frequency is a timedelta from psycopg2
            frequency = row['frequency']
            total_seconds = int(frequency.total_seconds())
            
            return self.FREQUENCY_RESOLUTION_MAP.get(total_seconds)
    
    def get_challenge_data_for_series(
        self, 
        challenge_id: int,
        series_id: int, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Time series data for a series. Resolution is auto-derived from challenge frequency.
        
        Args:
            challenge_id: ID of the challenge (used to derive resolution)
            series_id: ID of the series
            start_time: Start time
            end_time: End time
        """
        # Auto-derive resolution from challenge frequency
        resolution = self.get_challenge_frequency(challenge_id)
        
        # Input validation / Table mapping
        table_map = {
            "15min": "data_portal.time_series_15min",
            "1h": "data_portal.time_series_1h",
            "1d": "data_portal.time_series_1d",
        }
        
        table_name = table_map.get(resolution)
        if not table_name:
            # Default to raw if unknown frequency
            print(f"WARNING: Unknown resolution '{resolution}' for challenge {challenge_id}, defaulting to raw.", file=sys.stderr)
            table_name = "data_portal.time_series_data"

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = f"""
                SELECT ts, value
                FROM {table_name}
                WHERE series_id = %s AND ts >= %s AND ts <= %s
                ORDER BY ts;
            """
            cur.execute(query, (series_id, start_time, end_time))
            return [dict(row) for row in cur.fetchall()]
