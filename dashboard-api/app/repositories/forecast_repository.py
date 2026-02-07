import sys
from typing import List, Dict, Any
from datetime import datetime
import psycopg2.extras


class ForecastRepository:
    """Repository for forecast data."""
    
    # Frequency-to-resolution mapping for auto-derivation
    FREQUENCY_RESOLUTION_MAP = {
        # timedelta values mapped to resolution strings (using seconds for comparison)
        900: "15min",      # 15 minutes
        3600: "1h",        # 1 hour  
        86400: "1d",       # 1 day
    }
    
    def __init__(self, conn):
        self.conn = conn
    
    def _get_series_resolution(self, series_id: int) -> str:
        """Derive resolution from series frequency directly."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT frequency FROM data_portal.time_series WHERE series_id = %s", (series_id,))
            row = cur.fetchone()
            if not row or not row.get('frequency'):
                return "raw"
            
            frequency = row['frequency'] # timedelta or str? psycopg returns timedelta usually for interval
            if hasattr(frequency, 'total_seconds'):
                total_seconds = int(frequency.total_seconds())
                return self.FREQUENCY_RESOLUTION_MAP.get(total_seconds, "raw")
            return "raw"
    

    def get_model_series_long_term_forecasts(
        self,
        model_id: int,
        series_id: int
    ) -> Dict[str, Any]:
        """
        Get all forecasts for a specific model and series, plus Ground Truth.
        """
        resolution = self._get_series_resolution(series_id)
        
        table_map = {
            "raw": "data_portal.time_series_data",
            "15min": "data_portal.time_series_15min",
            "1h": "data_portal.time_series_1h",
            "1d": "data_portal.time_series_1d",
        }
        table_name = table_map.get(resolution, "data_portal.time_series_data")
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Get Forecasts
            cur.execute("""
                SELECT
                    f.ts,
                    f.predicted_value as value,
                    f.probabilistic_values as confidence_intervals,
                    f.created_at,
                    c.name as round_name,
                    c.id as round_id
                FROM forecasts.forecasts f
                JOIN challenges.rounds c ON c.id = f.challenge_id
                WHERE f.model_id = %s AND f.series_id = %s
                ORDER BY f.ts ASC
            """, (model_id, series_id))
            forecasts = [dict(r) for r in cur.fetchall()]
            
            if not forecasts:
                return {"forecasts": [], "ground_truth": []}

            # 2. Get Ground Truth (range based on forecasts)
            min_ts = forecasts[0]['ts']
            # We might want a bit of history before the first forecast? 
            # User said "GT & Pred for the series since the first participation", implies from min_ts onwards.
            # But continuous plot usually implies showing context. Let's start from min_ts.
            
            cur.execute(f"""
                SELECT ts, value::FLOAT as value
                FROM {table_name}
                WHERE series_id = %s AND ts >= %s
                ORDER BY ts ASC
            """, (series_id, min_ts))
            gt = [dict(r) for r in cur.fetchall()]
            
            return {
                "forecasts": forecasts,
                "ground_truth": gt,
                "resolution": resolution
            }

    def get_model_series_forecasts_across_rounds(
        self,
        model_id: int,
        definition_id: int,
        series_id: int,
        start_time: str = None,
        end_time: str = None
    ) -> Dict[str, Any]:
        """
        Get forecasts for a specific model and series across all rounds of a definition.
        
        Returns metadata about the model, series, definition, and a list of rounds with:
        - Whether the series was part of each round
        - Whether forecasts were submitted for each round
        - The actual forecast data points if they exist
        
        This allows distinguishing between:
        1. Series not part of the round (series_in_round=False)
        2. Series part of the round but no forecast submitted (series_in_round=True, forecast_exists=False)
        3. Series part of the round and forecast submitted (series_in_round=True, forecast_exists=True)
        
        Args:
            start_time: Optional start date in YYYY-mm-dd format to filter forecasts
            end_time: Optional end date in YYYY-mm-dd format to filter forecasts
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
            
            # Get definition info
            cur.execute("""
                SELECT id, name
                FROM challenges.definitions
                WHERE id = %s
            """, (definition_id,))
            definition_row = cur.fetchone()
            if not definition_row:
                return None
            
            # Get series info
            cur.execute("""
                SELECT series_id, name
                FROM data_portal.time_series
                WHERE series_id = %s
            """, (series_id,))
            series_row = cur.fetchone()
            if not series_row:
                return None
            
            # Get all rounds for this definition
            cur.execute("""
                SELECT 
                    r.id,
                    r.name,
                    r.start_time,
                    r.end_time,
                    EXISTS (
                        SELECT 1 
                        FROM challenges.series_pseudo sp 
                        WHERE sp.round_id = r.id AND sp.series_id = %s
                    ) as series_in_round
                FROM challenges.rounds r
                WHERE r.definition_id = %s
                ORDER BY r.start_time ASC
            """, (series_id, definition_id))
            rounds = [dict(r) for r in cur.fetchall()]
            
            # For each round, check if forecasts exist and get them
            result_rounds = []
            for round_info in rounds:
                round_id = round_info['id']
                series_in_round = round_info['series_in_round']
                
                forecasts = []
                forecast_exists = False
                
                if series_in_round:
                    # Check if forecasts exist for this round
                    # Build query with optional date filters
                    query = """
                        SELECT 
                            f.ts,
                            f.predicted_value as y,
                            f.probabilistic_values as ci
                        FROM forecasts.forecasts f
                        WHERE f.round_id = %s 
                            AND f.model_id = %s 
                            AND f.series_id = %s
                    """
                    params = [round_id, model_id, series_id]
                    
                    if start_time:
                        query += " AND f.ts >= %s::date"
                        params.append(start_time)
                    if end_time:
                        query += " AND f.ts < (%s::date + INTERVAL '1 day')"
                        params.append(end_time)
                    
                    query += " ORDER BY f.ts ASC"
                    
                    cur.execute(query, params)
                    forecast_rows = [dict(r) for r in cur.fetchall()]
                    
                    if forecast_rows:
                        forecast_exists = True
                        forecasts = forecast_rows
                
                result_rounds.append({
                    "round_id": round_id,
                    "round_name": round_info['name'],
                    "start_time": round_info['start_time'],
                    "end_time": round_info['end_time'],
                    "series_in_round": series_in_round,
                    "forecast_exists": forecast_exists,
                    "forecasts": forecasts if forecasts else None
                })
            
            return {
                "model_id": model_row['id'],
                "model_readable_id": model_row['readable_id'],
                "model_name": model_row['name'],
                "definition_id": definition_row['id'],
                "definition_name": definition_row['name'],
                "series_id": series_row['series_id'],
                "series_name": series_row['name'],
                "rounds": result_rounds
            }

