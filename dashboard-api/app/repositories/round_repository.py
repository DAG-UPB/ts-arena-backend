import logging
import math
from typing import List, Dict, Any, Optional
import psycopg2.extras

from app.core.tracks import track_sql

logger = logging.getLogger(__name__)


class RoundRepository:
    """Repository for Round data (ported from arena-app/src/database.py)."""

    def __init__(self, conn):
        self.conn = conn

    def get_round_status(self, round_id: int) -> Optional[str]:
        """Get the status of a round using the computed status from view.
        
        The status is dynamically computed from timestamps in the view,
        with 'cancelled' if is_cancelled flag is True.
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT status
                FROM challenges.v_rounds_with_status 
                WHERE id = %s
                """,
                (round_id,),
            )
            row = cur.fetchone()
            return row['status'] if row else None

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
                    c.frequency,
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
        
        
    def _format_model_readable_id(self, row: Dict[str, Any]) -> str:
        """Format: 'readable_id' (without org for now)."""
        readable_id = row.get("readable_id") or "model_id"
        return f"{readable_id}"
    
        
    def _format_model_label(self, row: Dict[str, Any]) -> str:
        """Format: 'name' (without org for now)."""
        name = row.get("model_name") or row.get("readable_id") or "model"
        return f"{name}"

    def get_series_forecasts(
        self, 
        round_id: int, 
        series_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Forecasts for a series, grouped by model.
        ``current_mase`` is the model's stored MASE for the series, as on the round leaderboard.
        Returns Dict: {model_label: {label, current_mase, data: [...]}}
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    mi.readable_id,
                    mi.name AS model_name,
                    f.ts,
                    f.predicted_value as value,
                    f.probabilistic_values as confidence_intervals,
                    fs.mase as current_mase
                FROM forecasts.forecasts f
                JOIN models.model_info mi ON mi.id = f.model_id
                LEFT JOIN forecasts.forecast_scores fs
                    ON fs.round_id = f.round_id
                    AND fs.model_id = f.model_id
                    AND fs.series_id = f.series_id
                WHERE f.round_id = %s AND f.series_id = %s
                ORDER BY f.created_at ASC, f.ts ASC;
                """,
                (round_id, series_id),
            )
            rows = [dict(row) for row in cur.fetchall()]

        grouped: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            key = self._format_model_readable_id(r)
            if key not in grouped:
                grouped[key] = {
                    "label": self._format_model_label(r),
                    "current_mase": _finite_or_none(r["current_mase"]),
                    "data": [],
                }
            grouped[key]["data"].append({
                "ts": r["ts"],
                "y": r["value"],
                "ci": r["confidence_intervals"]
            })
        return grouped

    def get_round_leaderboard(self, round_id: int, track: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get leaderboard (rankings) for a specific round from forecasts.forecast_scores.

        Finished and running rounds read the same rows: the scorer re-scores a round until
        its rows are final, so ``is_final`` is per row. One row per model and series that has
        an evaluated error. ``mase`` and ``sql_score`` are None where the series' scale is
        undefined; the row is still ranked.

        Args:
            round_id: The round
            track: 'reference' or 'open' to keep one track only; None keeps both.
                Filtering drops rows only: ``rank`` stays the per-series rank across
                both tracks, ``track_rank`` is the per-series rank within the track.
        
        Returns:
            List of dicts with model rankings, sorted by series and rank.
        """
        rows = self._get_leaderboard_from_scores(round_id)

        # Filtered after ranking, so `rank` keeps its meaning across both tracks.
        if track is not None:
            rows = [row for row in rows if row['track'] == track]
        return rows

    def _get_leaderboard_from_scores(self, round_id: int) -> List[Dict[str, Any]]:
        """
        Get leaderboard from forecasts.forecast_scores.
        Returns one row per model-series combination, ranked per series, both across
        tracks (``rank``) and within the model's own track (``track_rank``).
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    mi.id as model_id,
                    mi.readable_id,
                    mi.name as model_name,
                    {track_sql('mi')} as track,
                    fs.series_id,
                    ts.name as series_name,
                    fs.forecast_count,
                    fs.mase,
                    fs.rmse,
                    -- SQL is reported next to MASE, never used to rank. has_quantiles lets the
                    -- UI blank a score computed from point forecasts only.
                    fs.sql_score,
                    fs.has_quantiles,
                    fs.final_evaluation as is_final,
                    -- One scale per round and series: MAE orders like MASE and also ranks a
                    -- series whose scale is undefined.
                    RANK() OVER (PARTITION BY fs.series_id ORDER BY fs.mae ASC) as rank,
                    RANK() OVER (
                        PARTITION BY fs.series_id, {track_sql('mi')}
                        ORDER BY fs.mae ASC
                    ) as track_rank
                FROM forecasts.forecast_scores fs
                JOIN challenges.rounds cr ON cr.id = fs.round_id
                JOIN models.model_info mi ON mi.id = fs.model_id
                LEFT JOIN data_portal.time_series ts ON ts.series_id = fs.series_id
                WHERE fs.round_id = %s
                    AND fs.mae IS NOT NULL
                    -- Exclude series marked as excluded in definition_series_scd2
                    AND NOT EXISTS (
                        SELECT 1 FROM challenges.definition_series_scd2 ds
                        WHERE ds.definition_id = cr.definition_id 
                          AND ds.series_id = fs.series_id
                          AND ds.is_excluded = TRUE
                    )
                ORDER BY fs.series_id ASC, rank ASC, mi.name ASC
                """,
                (round_id,)
            )
            rows = [dict(r) for r in cur.fetchall()]

        for row in rows:
            for key in ['mase', 'rmse', 'sql_score']:
                row[key] = _finite_or_none(row.get(key))
        return rows


def _finite_or_none(value: Optional[float]) -> Optional[float]:
    if value is None or math.isinf(value) or math.isnan(value):
        return None
    return value
