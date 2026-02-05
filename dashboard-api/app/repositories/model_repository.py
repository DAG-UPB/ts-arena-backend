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
        definition_name: Optional[str] = None,
        definition_id: Optional[int] = None,
        min_rounds: int = 1,
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
            definition_name: Challenge definition name to filter by
            definition_id: Challenge definition ID to filter by
            min_rounds: Minimum number of participated rounds
            limit: Max. number of results
        
        Returns:
            List of dicts with ranking information
        """
        # Determine ELO join condition based on definition filter
        # If filtering by definition, join to definition-specific ELO; otherwise use global ELO
        if definition_id is not None:
            elo_join_condition = "er.model_id = vr.model_id AND er.definition_id = %s"
            elo_join_param = definition_id
        elif definition_name is not None:
            # Need to resolve definition_id from name for ELO join
            elo_join_condition = "er.model_id = vr.model_id AND er.definition_id = (SELECT id FROM challenges.definitions WHERE name = %s LIMIT 1)"
            elo_join_param = definition_name
        else:
            elo_join_condition = "er.model_id = vr.model_id AND er.definition_id IS NULL"
            elo_join_param = None
        
        query = f"""
            SELECT
                vr.model_id,
                MAX(mi.name) AS model_name,
                COUNT(DISTINCT vr.round_id) AS rounds_participated,
                AVG(vr.mase) AS avg_mase,
                STDDEV(vr.mase) AS stddev_mase,
                MIN(vr.mase) AS min_mase,
                MAX(vr.mase) AS max_mase,
                MAX(er.elo_score) AS elo_score,
                ARRAY_AGG(DISTINCT vr.domain ORDER BY vr.domain) FILTER (WHERE vr.domain IS NOT NULL) AS domains_covered,
                ARRAY_AGG(DISTINCT vr.category ORDER BY vr.category) FILTER (WHERE vr.category IS NOT NULL) AS categories_covered,
                ARRAY_AGG(DISTINCT vr.frequency::INTERVAL ORDER BY vr.frequency) FILTER (WHERE vr.frequency IS NOT NULL) AS frequencies_covered,
                ARRAY_AGG(DISTINCT vr.horizon::INTERVAL ORDER BY vr.horizon) FILTER (WHERE vr.horizon IS NOT NULL) AS horizons_covered,
                ARRAY_AGG(DISTINCT ARRAY[cd.id::TEXT, cd.name] ORDER BY cd.name) FILTER (WHERE cd.id IS NOT NULL) AS challenge_definitions
            FROM forecasts.v_ranking_base vr
            JOIN models.model_info mi ON mi.id = vr.model_id
            LEFT JOIN challenges.rounds r ON r.id = vr.round_id
            LEFT JOIN challenges.definitions cd ON cd.id = r.definition_id
            LEFT JOIN forecasts.elo_ratings er ON {elo_join_condition}
            WHERE 1=1
        """
        
        params = []
        if elo_join_param is not None:
            params.append(elo_join_param)
        
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
                query += " AND round_end_time >= %s"
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
        
        # Definition name filter
        if definition_name:
            query += " AND cd.name = %s"
            params.append(definition_name)
        
        # Definition ID filter
        if definition_id:
            query += " AND cd.id = %s"
            params.append(definition_id)
        
        # Group and aggregate
        query += """
            GROUP BY vr.model_id, model_name
            HAVING COUNT(DISTINCT vr.round_id) >= %s
            ORDER BY avg_mase ASC NULLS LAST, rounds_participated DESC
            LIMIT %s;
        """
        params.extend([min_rounds, limit])
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            rows = [dict(r) for r in cur.fetchall()]
            
            # Clean up float values and convert INTERVAL to ISO 8601
            for row in rows:
                for key, value in row.items():
                    if key in ('frequencies_covered', 'horizons_covered') and value:
                        # Convert PostgreSQL INTERVAL strings to ISO 8601
                        row[key] = [self._interval_to_iso8601(iv) for iv in value]
                    elif key == 'challenge_definitions' and value:
                        # Convert [[id, name], ...] to [{"id": id, "name": name}, ...]
                        row[key] = [
                            {"id": int(item[0]), "name": item[1]} 
                            for item in value if item and len(item) >= 2
                        ]
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
            Dict with domains, categories, subcategories, frequencies, horizons, time_ranges, definition_names
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
            
            # Get unique challenge definitions with IDs and names
            cur.execute("""
                SELECT id, name
                FROM challenges.definitions
                WHERE id IS NOT NULL
                ORDER BY name
            """)
            definitions = [{'id': row['id'], 'name': row['name']} for row in cur.fetchall()]
            
            return {
                "domains": domains,
                "categories": categories,
                "subcategories": subcategories,
                "frequencies": frequencies,
                "horizons": horizons,
                "time_ranges": ["7d", "30d", "90d", "365d"],
                "definitions": definitions
            }
    
    def get_model_rankings_by_definition(
        self,
        model_id: int
    ) -> Dict[str, Any]:
        """
        Get rankings for a model across all definitions it participated in.
        Returns rankings for 7d, 30d, 90d, and 365d time ranges.
        
        Args:
            model_id: The model ID
            
        Returns:
            Dict with model info and rankings grouped by definition
        """
        now = datetime.utcnow()
        time_ranges = {
            "7d": now - timedelta(days=7),
            "30d": now - timedelta(days=30),
            "90d": now - timedelta(days=90),
            "365d": now - timedelta(days=365),
        }
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get model name
            cur.execute(
                """
                SELECT id, name
                FROM models.model_info
                WHERE id = %s
                """,
                (model_id,)
            )
            model_row = cur.fetchone()
            if not model_row:
                return None
            
            model_name = model_row['name']
            
            # Get all definitions the model participated in
            cur.execute(
                """
                SELECT DISTINCT cd.id, cd.name
                FROM forecasts.v_ranking_base fv
                JOIN challenges.rounds r ON r.id = fv.round_id
                JOIN challenges.definitions cd ON cd.id = r.definition_id
                WHERE fv.model_id = %s
                ORDER BY cd.name
                """,
                (model_id,)
            )
            definitions = [dict(row) for row in cur.fetchall()]
            
            # For each definition, calculate rankings for each time range
            definition_rankings = []
            for definition in definitions:
                definition_id = definition['id']
                definition_name = definition['name']
                
                rankings_data = {
                    "definition_id": definition_id,
                    "definition_name": definition_name,
                    "rankings_7d": None,
                    "rankings_30d": None,
                    "rankings_90d": None,
                    "rankings_365d": None,
                }
                
                # Calculate rankings for each time range
                for range_key, since_date in time_ranges.items():
                    # Get model's ranking in this definition for this time range
                    cur.execute(
                        """
                        WITH model_scores AS (
                            SELECT
                                fv.model_id,
                                fv.model_name,
                                COUNT(DISTINCT fv.round_id) as rounds_participated,
                                AVG(fv.mase) as avg_mase,
                                STDDEV(fv.mase) as stddev_mase,
                                MIN(fv.mase) as min_mase,
                                MAX(fv.mase) as max_mase
                            FROM forecasts.v_ranking_base fv
                            JOIN challenges.rounds r ON r.id = fv.round_id
                            WHERE r.definition_id = %s
                                AND r.registration_start >= %s
                                AND fv.mase IS NOT NULL
                                AND fv.mase NOT IN ('NaN', 'Infinity', '-Infinity')
                            GROUP BY fv.model_id, fv.model_name
                        ),
                        ranked_models AS (
                            SELECT
                                model_id,
                                model_name,
                                rounds_participated,
                                avg_mase,
                                stddev_mase,
                                min_mase,
                                max_mase,
                                RANK() OVER (ORDER BY avg_mase ASC NULLS LAST, rounds_participated DESC) as rank,
                                COUNT(*) OVER () as total_models
                            FROM model_scores
                        )
                        SELECT
                            rank,
                            total_models,
                            rounds_participated,
                            avg_mase,
                            stddev_mase,
                            min_mase,
                            max_mase
                        FROM ranked_models
                        WHERE model_id = %s
                        """,
                        (definition_id, since_date, model_id)
                    )
                    
                    ranking_row = cur.fetchone()
                    if ranking_row:
                        ranking_dict = dict(ranking_row)
                        # Sanitize float values
                        for key, value in ranking_dict.items():
                            ranking_dict[key] = sanitize_float(value)
                        rankings_data[f"rankings_{range_key}"] = ranking_dict
                
                definition_rankings.append(rankings_data)
            
            return {
                "model_id": model_id,
                "model_name": model_name,
                "definition_rankings": definition_rankings
            }

    def get_model_series_by_definition(self, model_id: int) -> Optional[Dict[str, Any]]:
        """
        Get all series grouped by definition for a specific model.
        
        Series that appear in multiple definitions will be listed under each definition.
        Includes forecast counts and rounds participated for each series.
        
        Args:
            model_id: The ID of the model
            
        Returns:
            Dictionary with model info and definitions with their series,
            or None if model not found
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get model info
            cur.execute("""
                SELECT id, readable_id, name
                FROM models.model_info
                WHERE id = %s
            """, (model_id,))
            model_row = cur.fetchone()
            if not model_row:
                return None
            
            # Get all series grouped by definition
            cur.execute("""
                SELECT 
                    d.id as definition_id,
                    d.name as definition_name,
                    ts.series_id,
                    ts.name as series_name,
                    ts.unique_id as series_unique_id,
                    COUNT(DISTINCT f.id) as forecast_count,
                    COUNT(DISTINCT r.id) as rounds_participated
                FROM forecasts.forecasts f
                JOIN challenges.rounds r ON r.id = f.round_id
                JOIN challenges.definitions d ON d.id = r.definition_id
                JOIN data_portal.time_series ts ON ts.series_id = f.series_id
                WHERE f.model_id = %s
                GROUP BY d.id, d.name, ts.series_id, ts.name, ts.unique_id
                ORDER BY d.name, ts.name
            """, (model_id,))
            rows = [dict(r) for r in cur.fetchall()]
            
            # Group by definition
            definitions_dict = {}
            for row in rows:
                def_id = row['definition_id']
                if def_id not in definitions_dict:
                    definitions_dict[def_id] = {
                        'definition_id': def_id,
                        'definition_name': row['definition_name'],
                        'series': []
                    }
                
                definitions_dict[def_id]['series'].append({
                    'series_id': row['series_id'],
                    'series_name': row['series_name'],
                    'series_unique_id': row['series_unique_id'],
                    'forecast_count': row['forecast_count'],
                    'rounds_participated': row['rounds_participated']
                })
            
            return {
                'model_id': model_row['id'],
                'model_readable_id': model_row['readable_id'],
                'model_name': model_row['name'],
                'definitions': list(definitions_dict.values())
            }

