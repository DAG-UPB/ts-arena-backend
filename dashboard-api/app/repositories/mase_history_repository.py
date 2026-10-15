from datetime import date
from typing import Any, Dict, List, Optional

import psycopg2.extras

from app.core.tracks import track_sql
from app.repositories.model_repository import sanitize_float

WINDOW_DAYS = 30
MIN_PARTICIPATION = 0.8

_POINT_COLUMNS = ("week", "mase_30d", "mase_rel_30d", "participation_30d")


def _latest_order(model: Dict[str, Any]) -> tuple:
    value = model["latest_mase_30d"]
    return (value is None, 0.0 if value is None else value, model["model_name"].casefold(), model["model_id"])


class MaseHistoryRepository:
    """Reads the weekly 30-day MASE of every model in one scope."""

    def __init__(self, conn):
        self.conn = conn

    def get_history(
        self, scope_type: str, scope_id: Optional[str], track: Optional[str] = None
    ) -> Dict[str, Any]:
        params: List[Any] = [scope_type, scope_id or ""]
        track_filter = ""
        if track is not None:
            track_filter = f"AND {track_sql('mi')} = %s"
            params.append(track)
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT w.week, w.model_id, mi.name AS model_name, mi.readable_id,
                       {track_sql('mi')} AS track,
                       w.mase_30d, w.mase_rel_30d, w.participation_30d
                FROM forecasts.weekly_mase_30d w
                JOIN models.model_info mi ON mi.id = w.model_id
                WHERE w.scope_type = %s AND COALESCE(w.scope_id, '') = %s
                {track_filter}
                ORDER BY w.model_id, w.week
                """,
                tuple(params),
            )
            rows = [{k: sanitize_float(v) for k, v in r.items()} for r in cur.fetchall()]

        weeks: List[date] = sorted({r["week"] for r in rows})
        latest = weeks[-1] if weeks else None
        models: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            model = models.setdefault(row["model_id"], {
                "model_id": row["model_id"],
                "model_name": row["model_name"],
                "readable_id": row["readable_id"],
                "track": row["track"],
                "latest_mase_30d": None,
                "latest_mase_rel_30d": None,
                "points": [],
            })
            model["points"].append({c: row[c] for c in _POINT_COLUMNS})
            if row["week"] == latest:
                model["latest_mase_30d"] = row["mase_30d"]
                model["latest_mase_rel_30d"] = row["mase_rel_30d"]

        shown = [m for m in models.values() if any(p["mase_30d"] is not None for p in m["points"])]
        return {
            "window_days": WINDOW_DAYS,
            "min_participation": MIN_PARTICIPATION,
            "weeks": weeks,
            "models": sorted(shown, key=_latest_order),
        }
