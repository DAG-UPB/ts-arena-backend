import sys
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import psycopg2.extras
import math
import isodate

from app.schemas.model import ModelSchema

def sanitize_float(value: Any) -> Any:
    """Converts inf, -inf, nan to None for JSON compatibility."""
    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value):
            return None
    return value


class ModelRepository:
    """Repository for model data."""
    
    def __init__(self, conn):
        self.conn = conn
    
    def list_models_for_challenge(self, challenge_id: int) -> List[Dict[str, Any]]:
        """List of all models for a challenge."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    mi.readable_id,
                    COALESCE(mi.name, 'model') AS name,
                    mi.model_family,
                    mi.model_size,
                    mi.hosting,
                    mi.architecture,
                    mi.pretraining_data,
                    mi.publishing_date
                FROM forecasts.forecasts f
                JOIN models.model_info mi ON mi.id = f.model_id
                JOIN auth.users u ON u.id = mi.user_id
                WHERE f.challenge_id = %s
                ORDER BY 1;
                """,
                (challenge_id,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            return rows

    def get_model_details(self, model_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed model info and stats."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    mi.*,
                    (SELECT COUNT(DISTINCT round_id) FROM forecasts.scores WHERE model_id = mi.id) as challenges_participated,
                    (SELECT COALESCE(SUM(forecast_count), 0) FROM forecasts.scores WHERE model_id = mi.id) as forecasts_made
                FROM models.model_info mi
                WHERE mi.id = %s
                """,
                (model_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    
    def get_global_rankings(
        self, 
        range_key: Optional[str] = None
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Optional[datetime]]]:
        """
        Global model rankings.
        
        Args:
            range_key: "7d", "30d", "90d", "365d", or None (all ranges)
        
        Returns:
            (results, ranges) - results is Dict[range_label, rankings]
        """
        now = datetime.utcnow()
        ranges: Dict[str, Optional[datetime]] = {
            "Last 7 days": now - timedelta(days=7),
            "Last 30 days": now - timedelta(days=30),
            "Last 90 days": now - timedelta(days=90),
            "Last 365 days": now - timedelta(days=365),
        }
        
        # Mapping of short keys to labels
        range_mapping = {
            "7d": "Last 7 days",
            "30d": "Last 30 days",
            "90d": "Last 90 days",
            "365d": "Last 365 days",
        }
        
        results: Dict[str, List[Dict[str, Any]]] = {}
        
        # If range_key is given, calculate only this range
        if range_key:
            label = range_mapping.get(range_key)
            if not label:
                return {}, ranges
            ranges_to_compute = {label: ranges[label]}
        else:
            ranges_to_compute = ranges
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for label, since in ranges_to_compute.items():
                query = """
                    SELECT
                        mi.name AS model_name,
                        COUNT(cs.challenge_id) AS n_completed,
                        AVG(cs.mase) AS avg_mase
                    FROM forecasts.scores cs
                    JOIN models.model_info mi ON mi.id = cs.model_id
                    JOIN auth.users u ON u.id = mi.user_id
                    JOIN challenges.rounds c ON c.id = cs.challenge_id
                    WHERE cs.mase IS NOT NULL
                """
                params = []
                if since:
                    query += " AND c.end_time >= %s"
                    params.append(since)
                
                query += """
                    GROUP BY mi.name
                    ORDER BY avg_mase ASC NULLS LAST, n_completed DESC;
                """
                cur.execute(query, tuple(params))
                rows = [dict(r) for r in cur.fetchall()]
                # Clean up float values for JSON compatibility
                for row in rows:
                    for key, value in row.items():
                        row[key] = sanitize_float(value)
                results[label] = rows
        
        return results, ranges
    
    def get_filtered_rankings(
        self,
        time_range: Optional[str] = None,
        domains: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        subcategories: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        horizons: Optional[List[str]] = None,
        definition_id: Optional[int] = None,
        min_challenges: int = 1,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Filtered rankings based on multiple dimensions.
        
        Args:
            time_range: Time range (7d, 30d, 90d, 365d)
            domains: List of domains (e.g. ["Energy", "Finance"])
            categories: List of categories (e.g. ["Electricity", "Gas"])
            subcategories: List of subcategories (e.g. ["Load", "Generation"])
            frequencies: List of frequencies as ISO 8601 (e.g. ["PT1H", "P1D"])
            horizons: List of horizons as ISO 8601 (e.g. ["PT6H", "P1D"])
            min_challenges: Minimum number of participated challenges
            limit: Max. number of results
        
        Returns:
            List of dicts with ranking information
        """
        query = """
            SELECT
                cs.model_id,
                MAX(mi.name) AS model_name,
                COUNT(DISTINCT cs.challenge_id) AS challenges_participated,
                AVG(cs.mase) AS avg_mase,
                STDDEV(cs.mase) AS stddev_mase,
                MIN(cs.mase) AS min_mase,
                MAX(cs.mase) AS max_mase,
                ARRAY_AGG(DISTINCT vr.domain ORDER BY vr.domain) FILTER (WHERE vr.domain IS NOT NULL) AS domains_covered,
                ARRAY_AGG(DISTINCT vr.category ORDER BY vr.category) FILTER (WHERE vr.category IS NOT NULL) AS categories_covered,
                ARRAY_AGG(DISTINCT vr.frequency::INTERVAL ORDER BY vr.frequency) FILTER (WHERE vr.frequency IS NOT NULL) AS frequencies_covered,
                ARRAY_AGG(DISTINCT vr.horizon::INTERVAL ORDER BY vr.horizon) FILTER (WHERE vr.horizon IS NOT NULL) AS horizons_covered
            FROM forecasts.v_ranking_base vr
            JOIN forecasts.scores cs ON cs.round_id = vr.challenge_id AND cs.model_id = vr.model_id
            JOIN models.model_info mi ON mi.id = cs.model_id
            LEFT JOIN challenges.rounds r ON r.id = cs.round_id
            WHERE 1=1
        """
        
        params = []
        
        # Time range filter
        if time_range:
            now = datetime.utcnow()
            range_mapping = {
                "7d": timedelta(days=7),
                "30d": timedelta(days=30),
                "90d": timedelta(days=90),
                "365d": timedelta(days=365),
            }
            delta = range_mapping.get(time_range)
            if delta:
                since = now - delta
                query += " AND challenge_end_time >= %s"
                params.append(since)
        
        # Domain filter
        if domains and len(domains) > 0:
            placeholders = ','.join(['%s'] * len(domains))
            query += f" AND domain IN ({placeholders})"
            params.extend(domains)
        
        # Category filter
        if categories and len(categories) > 0:
            placeholders = ','.join(['%s'] * len(categories))
            query += f" AND category IN ({placeholders})"
            params.extend(categories)
        
        # Subcategory filter
        if subcategories and len(subcategories) > 0:
            placeholders = ','.join(['%s'] * len(subcategories))
            query += f" AND subcategory IN ({placeholders})"
            params.extend(subcategories)
        
        # Frequency filter (ISO 8601 → PostgreSQL INTERVAL)
        if frequencies and len(frequencies) > 0:
            interval_conditions = []
            for freq_iso in frequencies:
                try:
                    duration = isodate.parse_duration(freq_iso)
                    seconds = int(duration.total_seconds())
                    interval_conditions.append(f"frequency = INTERVAL '{seconds} seconds'")
                except Exception as e:
                    # Skip invalid ISO 8601 strings
                    print(f"Warning: Could not parse frequency '{freq_iso}': {e}", file=sys.stderr)
                    continue
            
            if interval_conditions:
                query += " AND (" + " OR ".join(interval_conditions) + ")"
        
        # Horizon filter (ISO 8601 → PostgreSQL INTERVAL)
        if horizons and len(horizons) > 0:
            interval_conditions = []
            for horizon_iso in horizons:
                try:
                    duration = isodate.parse_duration(horizon_iso)
                    seconds = int(duration.total_seconds())
                    interval_conditions.append(f"horizon = INTERVAL '{seconds} seconds'")
                except Exception as e:
                    # Skip invalid ISO 8601 strings
                    print(f"Warning: Could not parse horizon '{horizon_iso}': {e}", file=sys.stderr)
                    continue
            
            if interval_conditions:
                query += " AND (" + " OR ".join(interval_conditions) + ")"
        
        # Definition ID filter
        if definition_id:
            query += " AND r.definition_id = %s"
            params.append(definition_id)
        
        # Group and aggregate
        query += """
            GROUP BY model_id, model_name
            HAVING COUNT(DISTINCT challenge_id) >= %s
            ORDER BY avg_mase ASC NULLS LAST, challenges_participated DESC
            LIMIT %s;
        """
        params.extend([min_challenges, limit])
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            rows = [dict(r) for r in cur.fetchall()]
            
            # Clean up float values and convert INTERVAL to ISO 8601
            for row in rows:
                for key, value in row.items():
                    if key in ('frequencies_covered', 'horizons_covered') and value:
                        # Convert PostgreSQL INTERVAL strings to ISO 8601
                        row[key] = [self._interval_to_iso8601(iv) for iv in value]
                    else:
                        row[key] = sanitize_float(value)
            
            return rows
    
    def _interval_to_iso8601(self, interval_value) -> str:
        """
        Converts PostgreSQL INTERVAL to ISO 8601 Duration.
        
        Args:
            interval_value: PostgreSQL INTERVAL as timedelta object or string 
                           (e.g. timedelta(hours=1) or "1:00:00", "1 day")
        
        Returns:
            ISO 8601 Duration String (e.g. "PT1H", "P1D")
        """
        try:
            # If already a timedelta object, convert directly
            if isinstance(interval_value, timedelta):
                return isodate.duration_isoformat(interval_value)
            
            # If it is a string, parse it
            interval_str = str(interval_value)
            
            # Format: "HH:MM:SS"
            if ':' in interval_str:
                parts = interval_str.split(':')
                if len(parts) == 3:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    seconds = int(float(parts[2]))
                    total_seconds = hours * 3600 + minutes * 60 + seconds
                    duration = timedelta(seconds=total_seconds)
                    return isodate.duration_isoformat(duration)
            
            # Format: "X days" or "X day"
            if 'day' in interval_str:
                days = int(interval_str.split()[0])
                duration = timedelta(days=days)
                return isodate.duration_isoformat(duration)
            
            # Fallback: return as-is
            return interval_str
            
        except Exception as e:
            print(f"Warning: Could not convert interval '{interval_value}' to ISO 8601: {e}", file=sys.stderr)
            return str(interval_value)
    
    def get_available_filter_options(self) -> Dict[str, Any]:
        """
        Returns all available filter values.
        
        Returns:
            Dict with domains, categories, subcategories, frequencies, horizons
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get unique domains
            cur.execute("""
                SELECT DISTINCT domain
                FROM forecasts.v_ranking_base
                WHERE domain IS NOT NULL
                ORDER BY domain;
            """)
            domains = [row['domain'] for row in cur.fetchall()]
            
            # Get unique categories
            cur.execute("""
                SELECT DISTINCT category
                FROM forecasts.v_ranking_base
                WHERE category IS NOT NULL
                ORDER BY category;
            """)
            categories = [row['category'] for row in cur.fetchall()]
            
            # Get unique subcategories
            cur.execute("""
                SELECT DISTINCT subcategory
                FROM forecasts.v_ranking_base
                WHERE subcategory IS NOT NULL
                ORDER BY subcategory;
            """)
            subcategories = [row['subcategory'] for row in cur.fetchall()]
            
            # Get unique frequencies (convert to ISO 8601)
            cur.execute("""
                SELECT DISTINCT frequency
                FROM forecasts.v_ranking_base
                WHERE frequency IS NOT NULL
                ORDER BY frequency;
            """)
            frequencies = []
            for row in cur.fetchall():
                freq_interval = row['frequency']
                # psycopg2 returns timedelta for INTERVAL
                if isinstance(freq_interval, timedelta):
                    iso_freq = isodate.duration_isoformat(freq_interval)
                    frequencies.append(iso_freq)
            
            # Get unique horizons (convert to ISO 8601)
            cur.execute("""
                SELECT DISTINCT horizon
                FROM forecasts.v_ranking_base
                WHERE horizon IS NOT NULL
                ORDER BY horizon;
            """)
            horizons = []
            for row in cur.fetchall():
                horizon_interval = row['horizon']
                # psycopg2 returns timedelta for INTERVAL
                if isinstance(horizon_interval, timedelta):
                    iso_horizon = isodate.duration_isoformat(horizon_interval)
                    horizons.append(iso_horizon)
            
            return {
                "domains": domains,
                "categories": categories,
                "subcategories": subcategories,
                "frequencies": frequencies,
                "horizons": horizons,
                "time_ranges": ["7d", "30d", "90d", "365d"]
            }
