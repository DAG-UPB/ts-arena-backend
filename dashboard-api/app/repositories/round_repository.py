import sys
import math
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

    # Resolution to table name mapping
    RESOLUTION_TABLE_MAP = {
        "raw": "data_portal.time_series_data",
        "15min": "data_portal.time_series_15min",
        "1h": "data_portal.time_series_1h",
        "1d": "data_portal.time_series_1d",
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
                FROM challenges.rounds
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
        

    def _format_model_readable_id(self, row: Dict[str, Any]) -> str:
        """Format: 'readable_id' (without org for now)."""
        readable_id = row.get("readable_id") or "model_id"
        return f"{readable_id}"
    
        
    def _format_model_label(self, row: Dict[str, Any]) -> str:
        """Format: 'name' (without org for now)."""
        name = row.get("model_name") or row.get("readable_id") or "model"
        return f"{name}"

    def _calculate_mase(
        self, 
        mae_model_sum: float, 
        mae_naive_sum: float, 
        count: int
    ) -> Optional[float]:
        """
        Calculate MASE (Mean Absolute Scaled Error) from accumulated MAE values.
        
        Args:
            mae_model_sum: Sum of absolute errors for model predictions
            mae_naive_sum: Sum of absolute errors for naive baseline (last observed value)
            count: Number of data points
            
        Returns:
            MASE value or None if it cannot be calculated (count=0 or mae_naive=0)
        """
        if count <= 0:
            return None
        
        mae_model = mae_model_sum / count
        mae_naive = mae_naive_sum / count
        
        if mae_naive == 0:
            return None
        
        return mae_model / mae_naive

    def _get_table_name_for_resolution(self, resolution: str, context: str = "") -> str:
        """
        Get the appropriate table name for a given resolution.
        
        Args:
            resolution: Resolution string ("15min", "1h", "1d", "raw")
            context: Optional context for warning message (e.g., method name)
            
        Returns:
            Table name string, defaults to raw data table if resolution unknown
        """
        table_name = self.RESOLUTION_TABLE_MAP.get(resolution)
        if not table_name:
            print(f"WARNING: Unknown resolution '{resolution}' in {context}, defaulting to raw.", file=sys.stderr)
            table_name = self.RESOLUTION_TABLE_MAP["raw"]
        return table_name

    def _accumulate_mae_values(
        self,
        accumulator: Dict[str, Any],
        predicted: Optional[float],
        actual: Optional[float],
        naive: Optional[float]
    ) -> None:
        """
        Accumulate MAE values for MASE calculation if all values are present.
        
        Modifies the accumulator dict in place, expecting keys:
        - 'mae_model_sum' or '_mae_model_sum'
        - 'mae_naive_sum' or '_mae_naive_sum'  
        - 'count' or '_mae_count'
        
        Args:
            accumulator: Dict with sum/count keys to update
            predicted: Model's predicted value
            actual: Actual/current observed value
            naive: Naive baseline value (last observed)
        """
        if actual is None or predicted is None or naive is None:
            return
        
        # Support both naming conventions
        model_key = '_mae_model_sum' if '_mae_model_sum' in accumulator else 'mae_model_sum'
        naive_key = '_mae_naive_sum' if '_mae_naive_sum' in accumulator else 'mae_naive_sum'
        count_key = '_mae_count' if '_mae_count' in accumulator else 'count'
        
        accumulator[model_key] += abs(predicted - actual)
        accumulator[naive_key] += abs(naive - actual)
        accumulator[count_key] += 1
        

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
        table_name = self._get_table_name_for_resolution(resolution, "get_series_forecasts")

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Optimized query: Use CTE to compute latest_observed_value ONCE
            # instead of a correlated subquery that runs for every row
            query = f"""
                WITH latest_obs AS (
                    -- Compute once: get the latest observed value from series_pseudo
                    SELECT tsd_v.value as latest_observed_value
                    FROM {table_name} tsd_v
                    INNER JOIN challenges.series_pseudo sp 
                        ON sp.series_id = tsd_v.series_id 
                        AND tsd_v.ts = sp.max_ts
                    WHERE sp.round_id = %s AND sp.series_id = %s
                    LIMIT 1
                )
                SELECT
                    f.id as forecast_id,
                    f.created_at,
                    mi.parameters,
                    mi.readable_id,
                    mi.name AS model_name,
                    f.ts,
                    f.predicted_value as value,
                    f.probabilistic_values as confidence_intervals,
                    lo.latest_observed_value,
                    tsd.value::FLOAT as current_value
                FROM forecasts.forecasts f
                JOIN models.model_info mi ON mi.id = f.model_id
                LEFT JOIN {table_name} tsd ON tsd.series_id = f.series_id AND tsd.ts = f.ts
                CROSS JOIN latest_obs lo
                WHERE f.round_id = %s AND f.series_id = %s
                ORDER BY f.created_at ASC, f.ts ASC;
            """
            
            cur.execute(query, (round_id, series_id, round_id, series_id))
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

                self._accumulate_mae_values(
                    grouped[key],
                    predicted=r["value"],
                    actual=r["current_value"],
                    naive=r["latest_observed_value"]
                )
            
            for key, item in grouped.items():
                item["current_mase"] = self._calculate_mase(
                    item["_mae_model_sum"],
                    item["_mae_naive_sum"],
                    item["_mae_count"]
                )
                
                del item["_mae_model_sum"]
                del item["_mae_naive_sum"]
                del item["_mae_count"]
            
            return grouped

    def get_round_leaderboard(self, round_id: int) -> List[Dict[str, Any]]:
        """
        Get leaderboard (rankings) for a specific round.
        
        If final_evaluation is True in forecasts.scores, use pre-calculated scores.
        Otherwise, calculate MASE on-the-fly from forecasts.
        
        Returns:
            List of dicts with model rankings, sorted by avg_mase ascending
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Check if final evaluation exists for this round
            cur.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM forecasts.scores 
                    WHERE round_id = %s AND final_evaluation = TRUE
                ) as has_final_evaluation
                """,
                (round_id,)
            )
            result = cur.fetchone()
            has_final_evaluation = result['has_final_evaluation'] if result else False
            
            if has_final_evaluation:
                return self._get_leaderboard_from_scores(round_id)
            else:
                print(f"Round not yet completely evaluated: {round_id}. Calculating mase for leaderboard on-the-fly from forecasts.")
                return self._calculate_leaderboard_on_the_fly(round_id)

    def _get_leaderboard_from_scores(self, round_id: int) -> List[Dict[str, Any]]:
        """
        Get leaderboard from pre-calculated scores in forecasts.scores table.
        Returns one row per model-series combination, ranked per series.
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    mi.id as model_id,
                    mi.readable_id,
                    mi.name as model_name,
                    cs.series_id,
                    ts.name as series_name,
                    cs.forecast_count,
                    cs.mase,
                    cs.rmse,
                    RANK() OVER (PARTITION BY cs.series_id ORDER BY cs.mase ASC NULLS LAST) as rank
                FROM forecasts.scores cs
                JOIN models.model_info mi ON mi.id = cs.model_id
                LEFT JOIN data_portal.time_series ts ON ts.series_id = cs.series_id
                WHERE cs.round_id = %s
                    AND cs.final_evaluation = TRUE
                    AND cs.mase IS NOT NULL
                ORDER BY cs.series_id ASC, rank ASC, mi.name ASC
                """,
                (round_id,)
            )
            rows = [dict(r) for r in cur.fetchall()]
            
            # Sanitize values and add is_final flag
            for row in rows:
                row['is_final'] = True
                # Sanitize float values
                for key in ['mase', 'rmse']:
                    if row.get(key) is not None:
                        if math.isinf(row[key]) or math.isnan(row[key]):
                            row[key] = None
            
            return rows

    def _calculate_leaderboard_on_the_fly(self, round_id: int) -> List[Dict[str, Any]]:
        """
        Calculate leaderboard on-the-fly from forecasts when final_evaluation is not available.
        Uses the same MASE calculation logic as get_series_forecasts.
        Returns one row per model-series combination.
        """
        resolution = self._get_round_resolution(round_id)
        table_name = self._get_table_name_for_resolution(resolution, "_calculate_leaderboard_on_the_fly")
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get all forecasts with actuals and naive baseline for MASE calculation
            query = f"""
                WITH series_latest AS (
                    -- Get latest observed value per series for naive forecast
                    SELECT 
                        sp.series_id,
                        tsd.value as latest_observed_value
                    FROM challenges.series_pseudo sp
                    JOIN {table_name} tsd ON tsd.series_id = sp.series_id AND tsd.ts = sp.max_ts
                    WHERE sp.round_id = %s
                )
                SELECT
                    mi.id as model_id,
                    mi.readable_id,
                    mi.name as model_name,
                    f.series_id,
                    ts.name as series_name,
                    f.predicted_value,
                    tsd.value as actual_value,
                    sl.latest_observed_value
                FROM forecasts.forecasts f
                JOIN models.model_info mi ON mi.id = f.model_id
                LEFT JOIN data_portal.time_series ts ON ts.series_id = f.series_id
                LEFT JOIN {table_name} tsd ON tsd.series_id = f.series_id AND tsd.ts = f.ts
                LEFT JOIN series_latest sl ON sl.series_id = f.series_id
                WHERE f.round_id = %s
            """
            
            cur.execute(query, (round_id, round_id))
            rows = [dict(r) for r in cur.fetchall()]
            
            if not rows:
                return []
            
            # Aggregate by model and series to calculate MASE per model-series
            model_series_data: Dict[tuple, Dict[str, Any]] = {}
            
            for r in rows:
                model_id = r['model_id']
                series_id = r['series_id']
                key = (model_id, series_id)
                
                if key not in model_series_data:
                    model_series_data[key] = {
                        'model_id': model_id,
                        'readable_id': r['readable_id'],
                        'model_name': r['model_name'],
                        'series_id': series_id,
                        'series_name': r['series_name'],
                        'mae_model_sum': 0.0,
                        'mae_naive_sum': 0.0,
                        'count': 0
                    }
                
                self._accumulate_mae_values(
                    model_series_data[key],
                    predicted=r['predicted_value'],
                    actual=r['actual_value'],
                    naive=r['latest_observed_value']
                )
            
            # Calculate MASE for each model-series combination
            leaderboard = []
            for key, data in model_series_data.items():
                mase = self._calculate_mase(
                    data['mae_model_sum'],
                    data['mae_naive_sum'],
                    data['count']
                )
                
                leaderboard.append({
                    'model_id': data['model_id'],
                    'readable_id': data['readable_id'],
                    'model_name': data['model_name'],
                    'series_id': data['series_id'],
                    'series_name': data['series_name'],
                    'forecast_count': data['count'],
                    'mase': mase,
                    'rmse': None,  # Not calculated on-the-fly
                    'is_final': False
                })
            
            # Sort by series_id first, then by mase within each series
            leaderboard.sort(key=lambda x: (
                x['series_id'] or 0,
                x['mase'] is None, 
                x['mase'] or float('inf'),
                x['model_name'] or ''
            ))
            
            # Group by series_id and filter out series with all invalid MASE values
            from itertools import groupby
            filtered_leaderboard = []
            
            for series_id, series_group in groupby(leaderboard, key=lambda x: x['series_id']):
                series_items = list(series_group)
                
                # Check if all MASE values are None or infinity
                valid_mase_exists = any(
                    item['mase'] is not None and not math.isinf(item['mase'])
                    for item in series_items
                )
                
                if not valid_mase_exists:
                    print(f"WARNING: Series {series_id} has no valid MASE values (all None or Infinity). Skipping series in leaderboard.", file=sys.stderr)
                    continue
                
                # Add rank per series
                for rank, item in enumerate(series_items, start=1):
                    item['rank'] = rank
                    filtered_leaderboard.append(item)
            
            return filtered_leaderboard