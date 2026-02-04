import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
import psycopg2.extras


class RoundRepository:
    """Repository for Round data (ported from arena-app/src/database.py)."""

    # Frequency-to-resolution mapping for auto-derivation
    FREQUENCY_RESOLUTION_MAP = {
        # timedelta values mapped to resolution strings (using seconds for comparison)
        900: "15min",      # 15 minutes
        3600: "1h",        # 1 hour  
        86400: "1d",       # 1 day
    }
    
    def __init__(self, conn):
        self.conn = conn

    def get_round_meta(self, round_id: int) -> Optional[Dict[str, Any]]:
        """Fetch metadata for a round."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.id as round_id,
                    c.name,
                    c.description,
                    c.status,
                    c.context_length,
                    c.horizon,
                    c.start_time,
                    c.end_time,
                    c.registration_start,
                    c.registration_end
                FROM challenges.v_rounds_with_status c 
                WHERE c.id = %s
                """,
                (round_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        
    def list_models_for_round(self, round_id: int) -> List[Dict[str, Any]]:
        """List of all models for a round."""
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
                WHERE f.round_id = %s
                ORDER BY 1;
                """,
                (round_id,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            return rows
        
        
    def _get_round_resolution(self, round_id: int) -> str:
        """
        Retrieves the round frequency and maps it to a resolution string.
        
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
                (round_id,),
            )
            row = cur.fetchone()
            if not row or not row.get('frequency'):
                return "raw"
            
            # frequency is a timedelta from psycopg2
            frequency = row['frequency']
            total_seconds = int(frequency.total_seconds())
            
            return self.FREQUENCY_RESOLUTION_MAP.get(total_seconds, "raw")
        

    def get_series_forecasts(
        self, 
        round_id: int, 
        series_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Forecasts for a series, grouped by model.
        Resolution is auto-derived from round frequency.
        Returns Dict: {model_label: {label, current_mase, data: [...]}}
        """
        # Auto-derive resolution from round frequency
        resolution = self._get_round_resolution(round_id)
        
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
            
            cur.execute(query, (round_id, series_id))
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