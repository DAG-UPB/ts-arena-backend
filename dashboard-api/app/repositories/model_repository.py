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
        scope_type: str = "global",
        scope_id: Optional[str] = None,
        calculation_date = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get model rankings from daily_rankings based on scope, enriched with MASE statistics.
        
        Args:
            scope_type: One of 'global', 'definition', or 'frequency_horizon'
            scope_id: The scope identifier:
                - None for 'global'
                - Definition ID as string for 'definition'
                - Frequency::horizon string for 'frequency_horizon' (e.g., '00:15:00::1 day')
            calculation_date: Date object for filtering rankings (defaults to today if None)
            limit: Max. number of results
        
        Returns:
            List of dicts with ranking information including ELO scores, MASE statistics, and metadata
        """
        # Default to today if no calculation_date provided
        if calculation_date is None:
            from datetime import date
            calculation_date = date.today()
        
        # Build scope filter conditions
        scope_filter = "er.scope_type = %s"
        params = [scope_type]
        
        # global scope has no additional filter
        if scope_type == "definition":
            scope_filter += " AND er.scope_id = %s"
            params.append(scope_id)
        elif scope_type == "frequency_horizon":
            scope_filter += " AND er.scope_id = %s"
            params.append(scope_id)
        
        # Build the query with CTEs
        query = f"""
            WITH scope_elo AS (
                SELECT DISTINCT ON (er.model_id)
                    er.model_id,
                    mi.name AS model_name,
                    mi.readable_id,
                    mi.architecture,
                    mi.model_size,
                    u.username,
                    o.name AS organization_name,
                    er.elo_rating_median AS elo_score,
                    er.elo_ci_lower,
                    er.elo_ci_upper,
                    (er.elo_rating_median - er.elo_ci_lower) AS elo_ci_lower_diff,
                    (er.elo_ci_upper - er.elo_rating_median) AS elo_ci_upper_diff,
                    er.matches_played AS n_matches,
                    er.n_bootstraps,
                    er.calculated_at,
                    er.calculation_date,
                    er.rank_position
                FROM forecasts.daily_rankings er
                JOIN models.model_info mi ON er.model_id = mi.id
                JOIN auth.users u ON mi.user_id = u.id
                LEFT JOIN auth.organizations o ON mi.organization_id = o.id
                WHERE {scope_filter}
                  AND er.calculation_date = %s
                ORDER BY er.model_id, er.calculation_date DESC
            ),
            scope_mase AS (
                SELECT 
                    fs.model_id,
                    AVG(fs.mase) AS mase_avg,
                    STDDEV(fs.mase) AS mase_std,
                    COUNT(*) AS n_evaluations
                FROM forecasts.scores fs
                JOIN challenges.rounds cr ON fs.round_id = cr.id
                WHERE fs.final_evaluation = TRUE
                  AND fs.mase IS NOT NULL
                  AND fs.mase != 'NaN'
                  AND fs.mase != 'Infinity'
                  AND fs.mase != '-Infinity'
                  AND cr.end_time <= %s
                GROUP BY fs.model_id
            )
            SELECT 
                se.model_id,
                se.model_name,
                se.readable_id,
                se.architecture,
                se.model_size,
                se.username,
                se.organization_name,
                se.elo_score,
                se.elo_ci_lower,
                se.elo_ci_upper,
                se.elo_ci_lower_diff,
                se.elo_ci_upper_diff,
                se.n_matches,
                se.n_bootstraps,
                se.rank_position,
                sm.mase_avg,
                sm.mase_std,
                sm.n_evaluations,
                se.calculated_at,
                se.calculation_date
            FROM scope_elo se
            LEFT JOIN scope_mase sm ON se.model_id = sm.model_id
            ORDER BY se.rank_position ASC
            LIMIT %s;
        """
        
        params.extend([calculation_date, calculation_date, limit])
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            rows = [dict(r) for r in cur.fetchall()]
            
            # Clean up float values for JSON compatibility
            for row in rows:
                for key, value in row.items():
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
        Returns all available filter values for the rankings endpoint.
        
        Returns:
            Dict with definitions and frequency_horizons
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get unique challenge definitions with IDs and names
            cur.execute("""
                SELECT id, name
                FROM challenges.definitions
                WHERE id IS NOT NULL
                ORDER BY name
            """)
            definitions = [{'id': row['id'], 'name': row['name']} for row in cur.fetchall()]
            
            # Get unique frequency_horizon combinations from daily_rankings
            cur.execute("""
                SELECT DISTINCT scope_id
                FROM forecasts.daily_rankings
                WHERE scope_type = 'frequency_horizon'
                  AND scope_id IS NOT NULL
                ORDER BY scope_id;
            """)
            frequency_horizons = [row['scope_id'] for row in cur.fetchall()]
            
            return {
                "definitions": definitions,
                "frequency_horizons": frequency_horizons
            }
    
    def get_model_rankings_by_definition(
        self,
        model_id: int
    ) -> Dict[str, Any]:
        """
        Get ELO rankings for a model across all definitions it participated in.
        Returns daily ELO rankings for the last 30 days.
        
        Args:
            model_id: The model ID
            
        Returns:
            Dict with model info and ELO rankings grouped by definition for the last 30 days
        """
        from datetime import date
        
        # Calculate date range: today and last 30 days
        today = date.today()
        start_date = today - timedelta(days=30)
        
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
            
            # Get all definitions the model has ELO rankings for in the last 30 days
            cur.execute(
                """
                SELECT DISTINCT 
                    dr.scope_id,
                    cd.id as definition_id,
                    cd.name as definition_name
                FROM forecasts.daily_rankings dr
                JOIN challenges.definitions cd ON cd.id = CAST(dr.scope_id AS INTEGER)
                WHERE dr.model_id = %s
                  AND dr.scope_type = 'definition'
                  AND dr.calculation_date BETWEEN %s AND %s
                ORDER BY cd.name
                """,
                (model_id, start_date, today)
            )
            definitions = [dict(row) for row in cur.fetchall()]
            
            # For each definition, get daily ELO rankings for the last 30 days
            definition_rankings = []
            for definition in definitions:
                definition_id = definition['definition_id']
                definition_name = definition['definition_name']
                
                # Get all daily rankings for this definition in the last 30 days
                cur.execute(
                    """
                    SELECT
                        dr.calculation_date,
                        dr.elo_rating_median as elo_score,
                        dr.elo_ci_lower,
                        dr.elo_ci_upper,
                        dr.rank_position
                    FROM forecasts.daily_rankings dr
                    WHERE dr.model_id = %s
                      AND dr.scope_type = 'definition'
                      AND dr.scope_id = %s
                      AND dr.calculation_date BETWEEN %s AND %s
                    ORDER BY dr.calculation_date ASC
                    """,
                    (model_id, str(definition_id), start_date, today)
                )
                
                daily_rankings = []
                for row in cur.fetchall():
                    ranking_dict = dict(row)
                    # Sanitize float values
                    for key, value in ranking_dict.items():
                        ranking_dict[key] = sanitize_float(value)
                    daily_rankings.append(ranking_dict)
                
                definition_rankings.append({
                    "definition_id": definition_id,
                    "definition_name": definition_name,
                    "daily_rankings": daily_rankings
                })
            
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

