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
    
    def list_challenges(
        self,
        status: Optional[List[str]] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        # NEW: Filter parameters
        domains: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        subcategories: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,  # ISO 8601 Strings
        horizons: Optional[List[str]] = None,     # ISO 8601 Strings
    ) -> List[Dict[str, Any]]:
        """
        List all challenges with optional filters.
        
        Args:
            status: List of status values (e.g. ['active', 'completed'])
            from_date: Challenges with end_time >= from_date
            to_date: Challenges with end_time <= to_date
            domains: List of domains (e.g. ['Energy', 'Finance'])
            categories: List of categories
            subcategories: List of subcategories
            frequencies: List of frequencies as ISO 8601 (e.g. ['PT1H', 'P1D'])
            horizons: List of horizons as ISO 8601
        """
        print(f"DEBUG: list_challenges(status={status}, from={from_date}, to={to_date}, "
              f"domains={domains}, categories={categories}, subcategories={subcategories}, "
              f"frequencies={frequencies}, horizons={horizons})", file=sys.stderr)
        
        # Use new view
        query = """
            SELECT 
                challenge_id,
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
                created_at,
                model_count,
                forecast_count,
                -- NEW: Metadata arrays
                domains,
                categories,
                subcategories,
                frequencies
            FROM challenges.v_challenges_with_metadata
            WHERE 1=1
        """
        
        params = []
        
        # Filter by status
        if status and len(status) > 0:
            placeholders = ','.join(['%s'] * len(status))
            query += f" AND status IN ({placeholders})"
            params.extend(status)
        
        # Filter by date
        if from_date:
            query += " AND end_time >= %s"
            params.append(from_date)
        
        if to_date:
            query += " AND end_time <= %s"
            params.append(to_date)
        
        # NEW: Filter by domain (Array-Overlap)
        if domains and len(domains) > 0:
            placeholders = ','.join(['%s'] * len(domains))
            query += f" AND domains && ARRAY[{placeholders}]::TEXT[]"
            params.extend(domains)
        
        # NEW: Filter by category
        if categories and len(categories) > 0:
            placeholders = ','.join(['%s'] * len(categories))
            query += f" AND categories && ARRAY[{placeholders}]::TEXT[]"
            params.extend(categories)
        
        # NEW: Filter by subcategory
        if subcategories and len(subcategories) > 0:
            placeholders = ','.join(['%s'] * len(subcategories))
            query += f" AND subcategories && ARRAY[{placeholders}]::TEXT[]"
            params.extend(subcategories)
        
        # NEW: Filter by frequency (ISO 8601 → INTERVAL)
        if frequencies and len(frequencies) > 0:
            try:
                interval_strings = parse_iso8601_to_interval_list(frequencies)
                interval_conditions = []
                for interval_str in interval_strings:
                    interval_conditions.append(f"INTERVAL '{interval_str}'")
                query += f" AND frequencies && ARRAY[{','.join(interval_conditions)}]::INTERVAL[]"
            except ValueError as e:
                print(f"ERROR: Invalid frequency format: {e}", file=sys.stderr)
                # Optional: raise HTTPException or ignore
        
        # NEW: Filter by horizon (direct comparison, not an array)
        if horizons and len(horizons) > 0:
            try:
                interval_strings = parse_iso8601_to_interval_list(horizons)
                horizon_conditions = []
                for interval_str in interval_strings:
                    horizon_conditions.append(f"horizon = INTERVAL '{interval_str}'")
                query += f" AND ({' OR '.join(horizon_conditions)})"
            except ValueError as e:
                print(f"ERROR: Invalid horizon format: {e}", file=sys.stderr)
        
        # Sorting
        query += """
            ORDER BY
                CASE status
                    WHEN 'active' THEN 0
                    WHEN 'registration' THEN 1
                    WHEN 'completed' THEN 2
                    WHEN 'announced' THEN 3
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
                from app.schemas.challenge import serialize_timedelta_to_iso8601
                
                if row_dict.get('frequencies'):
                    row_dict['frequencies'] = [
                        serialize_timedelta_to_iso8601(f) for f in row_dict['frequencies']
                    ]
                
                # Convert horizon timedelta to ISO 8601
                if row_dict.get('horizon'):
                    row_dict['horizon'] = serialize_timedelta_to_iso8601(row_dict['horizon'])
                
                results.append(row_dict)
            print(f"DEBUG: list_challenges found {len(results)} challenges.", file=sys.stderr)
            return results
    
    def get_challenge_meta(self, challenge_id: int) -> Optional[Dict[str, Any]]:
        """Fetch metadata for a challenge."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.id as challenge_id,
                    c.name,
                    c.description,
                    c.status,
                    c.context_length,
                    c.horizon,
                    c.start_time,
                    c.end_time,
                    c.registration_start,
                    c.registration_end
                FROM challenges.v_challenges_with_status c 
                WHERE c.id = %s
                """,
                (challenge_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    
    def get_challenge_series(self, challenge_id: int) -> List[Dict[str, Any]]:
        """Zeitreihen für eine Challenge mit Domain-Informationen."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    csp.series_id,
                    ts.name,
                    ts.description,
                    ts.frequency as frequency,
                    c.horizon,
                    ts.endpoint_prefix,
                    c.start_time as start_time,
                    c.end_time as end_time,
                    c.registration_start as registration_start,
                    c.registration_end as registration_end,
                    cdr.min_ts as context_start_time,
                    cdr.max_ts as context_end_time,
                    -- NEW: Domain information
                    dc.domain,
                    dc.category,
                    dc.subcategory
                FROM challenges.challenge_series_pseudo csp
                JOIN data_portal.time_series ts ON ts.series_id = csp.series_id
                JOIN challenges.v_challenges_with_status c ON c.id = csp.challenge_id
                JOIN challenges.v_challenge_context_data_range cdr 
                    ON cdr.challenge_id = csp.challenge_id 
                    AND cdr.series_id = csp.series_id
                -- NEW: Domain join
                LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
                WHERE csp.challenge_id = %s
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
                    UNNEST(frequencies) as frequency,
                    horizon,
                    status
                FROM challenges.v_challenges_with_metadata
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
    
    def get_challenge_data_for_series(
        self, 
        series_id: int, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Time series data for a series."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, value
                FROM data_portal.time_series_data
                WHERE series_id = %s AND ts >= %s AND ts <= %s
                ORDER BY ts;
                """,
                (series_id, start_time, end_time),
            )
            return [dict(row) for row in cur.fetchall()]
