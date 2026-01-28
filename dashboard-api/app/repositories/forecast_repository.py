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
    
    def _get_challenge_resolution(self, challenge_id: int) -> str:
        """
        Retrieves the challenge frequency and maps it to a resolution string.
        
        Returns:
            Resolution string ("15min", "1h", "1d") or "raw" if not found
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT frequency
                FROM forecasts.scores
                WHERE id = %s
                """,
                (challenge_id,),
            )
            row = cur.fetchone()
            if not row or not row.get('frequency'):
                return "raw"
            
            # frequency is a timedelta from psycopg2
            frequency = row['frequency']
            total_seconds = int(frequency.total_seconds())
            
            return self.FREQUENCY_RESOLUTION_MAP.get(total_seconds, "raw")

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
    
    def get_series_forecasts(
        self, 
        challenge_id: int, 
        series_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Forecasts for a series, grouped by model.
        Resolution is auto-derived from challenge frequency.
        Returns Dict: {model_label: {label, current_mase, data: [...]}}
        """
        # Auto-derive resolution from challenge frequency
        resolution = self._get_challenge_resolution(challenge_id)
        
        # Table mapping
        table_map = {
            "raw": "data_portal.time_series_data",
            "15min": "data_portal.time_series_15min",
            "1h": "data_portal.time_series_1h",
            "1d": "data_portal.time_series_1d",
        }
        
        table_name = table_map.get(resolution)
        if not table_name:
             print(f"WARNING: Unknown resolution '{resolution}' in get_series_forecasts, defaulting to raw.", file=sys.stderr)
             table_name = "data_portal.time_series_data"

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = f"""
                SELECT
                    f.id as forecast_id,
                    f.created_at,
                    mi.parameters,
                    mi.readable_id,
                    mi.name AS model_name,
                    f.ts,
                    f.predicted_value as value,
                    f.probabilistic_values as confidence_intervals,
                    ccd.latest_value as latest_observed_value,
                    tsd.value::FLOAT as current_value
                FROM forecasts.forecasts f
                JOIN models.model_info mi ON mi.id = f.model_id
                JOIN challenges.v_context_data_range as ccd ON ccd.challenge_id = f.challenge_id AND ccd.series_id = f.series_id
                LEFT JOIN {table_name} tsd ON tsd.series_id = f.series_id AND tsd.ts = f.ts
                WHERE f.challenge_id = %s AND f.series_id = %s
                ORDER BY f.created_at ASC, f.ts ASC;
            """
            
            cur.execute(query, (challenge_id, series_id))
            rows = [dict(row) for row in cur.fetchall()]
            
            if not rows:
                return {}
            
            grouped: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                key = self._format_model_readable_id(r)
                label = self._format_model_label(r)
                if key not in grouped:
                    grouped[key] = {
                        "label": label,
                        "current_mase": None,
                        "data": [],
                        "_mae_model_sum": 0.0,
                        "_mae_naive_sum": 0.0,
                        "_mae_count": 0
                    }
                
                grouped[key]["data"].append({
                    "ts": r["ts"],
                    "y": r["value"],
                    "ci": r["confidence_intervals"]
                })

                current_val = r["current_value"]
                latest_val = r["latest_observed_value"]
                pred_val = r["value"]

                if current_val is not None and latest_val is not None and pred_val is not None:
                    grouped[key]["_mae_model_sum"] += abs(pred_val - current_val)
                    grouped[key]["_mae_naive_sum"] += abs(latest_val - current_val)
                    grouped[key]["_mae_count"] += 1
            
            for key, item in grouped.items():
                if item["_mae_count"] > 0:
                    mae_model = item["_mae_model_sum"] / item["_mae_count"]
                    mae_naive = item["_mae_naive_sum"] / item["_mae_count"]
                    
                    if mae_naive != 0:
                        item["current_mase"] = mae_model / mae_naive
                
                del item["_mae_model_sum"]
                del item["_mae_naive_sum"]
                del item["_mae_count"]
            
            return grouped
        
    def _format_model_readable_id(self, row: Dict[str, Any]) -> str:
        """Format: 'readable_id' (without org for now)."""
        readable_id = row.get("readable_id") or "model_id"
        return f"{readable_id}"
    
    def _format_model_label(self, row: Dict[str, Any]) -> str:
        """Format: 'name' (without org for now)."""
        name = row.get("model_name") or row.get("readable_id") or "model"
        return f"{name}"

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
