from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras

from app.core.arena_scores import arrange_leaderboard, is_month_end, scope_order
from app.core.tracks import track_sql
from app.repositories.model_repository import sanitize_float

_DEFINITION_JOIN = (
    "LEFT JOIN challenges.definitions d "
    "ON a.scope_type = 'definition' AND a.scope_id = d.id::text"
)

_HISTORY_COLUMNS = (
    "calculation_date", "score", "ci_lower", "ci_upper", "rank_best", "rank_worst",
    "provisional", "low_coverage", "unranked_disconnected", "coverage", "weeks_played",
)


def _scope_where(alias: str, scope_id: Optional[str]) -> str:
    sql = f"{alias}.metric = %s AND {alias}.scope_type = %s"
    if scope_id is not None:
        sql += f" AND COALESCE({alias}.scope_id, '') = %s"
    return sql


def _clean(row: Dict[str, Any]) -> Dict[str, Any]:
    """NaN and infinity become None so the row is valid JSON."""
    return {key: sanitize_float(value) for key, value in row.items()}


def _method_number(value: Any, as_int: bool = False) -> Optional[float]:
    """A number read from the stored method; anything else is None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = sanitize_float(value)
    if value is None:
        return None
    return int(value) if as_int else float(value)


class ArenaScoreRepository:
    """Reads the stored Arena Scores."""

    def __init__(self, conn):
        self.conn = conn

    def get_leaderboard(
        self,
        scope_type: str,
        scope_id: Optional[str] = None,
        calculation_date: Optional[date] = None,
        metric: str = "point",
        limit: int = 100,
        track: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[date]]:
        """Leaderboard rows of one scope, or of every scope of a type when scope_id is None.

        Without a calculation_date the newest date that has rows for the scope (or, for a
        type, for any scope of it) and metric is used. `limit` applies per scope, after the
        track filter. Returns the rows and the date they belong to.
        """
        scope_params: List[Any] = [metric, scope_type]
        if scope_id is not None:
            scope_params.append(scope_id)

        if calculation_date is None:
            date_filter = f"""a.calculation_date = (
                    SELECT MAX(l.calculation_date)
                    FROM forecasts.daily_arena_scores l
                    WHERE {_scope_where('l', scope_id)}
                )"""
            date_params: List[Any] = scope_params
        else:
            date_filter = "a.calculation_date = %s"
            date_params = [calculation_date]

        query = f"""
            SELECT
                a.model_id,
                mi.name AS model_name,
                mi.readable_id,
                mi.architecture,
                mi.model_size,
                o.name AS organization_name,
                {track_sql('mi')} AS track,
                a.scope_type,
                a.scope_id,
                d.id AS definition_id,
                a.calculation_date,
                a.score,
                a.ci_lower,
                a.ci_upper,
                a.rank_best,
                a.rank_worst,
                a.provisional,
                a.low_coverage,
                a.unranked_disconnected,
                a.coverage,
                a.weeks_played,
                a.tasks_played,
                a.rounds_played,
                a.mase_30d,
                a.participation_30d,
                a.sql_30d,
                a.sql_participation_30d,
                a.tasks_30d,
                a.method -> 'provisional_weeks' AS provisional_weeks,
                a.method -> 'tau' AS coverage_tau
            FROM forecasts.daily_arena_scores a
            JOIN models.model_info mi ON mi.id = a.model_id
            LEFT JOIN auth.organizations o ON o.id = mi.organization_id
            {_DEFINITION_JOIN}
            WHERE {date_filter}
              AND {_scope_where('a', scope_id)}
        """

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(date_params + scope_params))
            rows = [_clean(dict(r)) for r in cur.fetchall()]

        for row in rows:
            row["provisional_weeks"] = _method_number(row["provisional_weeks"], as_int=True)
            row["coverage_tau"] = _method_number(row["coverage_tau"])

        resolved = rows[0]["calculation_date"] if rows else calculation_date
        return arrange_leaderboard(rows, track, limit), resolved

    def get_model_history(
        self,
        model_id: int,
        metric: str = "point",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Optional[Dict[str, Any]]:
        """Daily Arena Scores of one model grouped by scope; None if the model does not exist."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT mi.id AS model_id, mi.name AS model_name, {track_sql('mi')} AS track
                FROM models.model_info mi
                WHERE mi.id = %s
                """,
                (model_id,),
            )
            model = cur.fetchone()
            if not model:
                return None

            query = f"""
                SELECT
                    a.scope_type,
                    a.scope_id,
                    d.id AS definition_id,
                    d.name AS definition_name,
                    {', '.join('a.' + c for c in _HISTORY_COLUMNS)}
                FROM forecasts.daily_arena_scores a
                {_DEFINITION_JOIN}
                WHERE a.model_id = %s AND a.metric = %s
            """
            params: List[Any] = [model_id, metric]
            if start_date is not None:
                query += " AND a.calculation_date >= %s"
                params.append(start_date)
            if end_date is not None:
                query += " AND a.calculation_date <= %s"
                params.append(end_date)
            query += " ORDER BY a.calculation_date"
            cur.execute(query, tuple(params))
            rows = [_clean(dict(r)) for r in cur.fetchall()]

        scopes: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
        for row in rows:
            key = (row["scope_type"], row["scope_id"])
            if key not in scopes:
                scopes[key] = {
                    "scope_type": row["scope_type"],
                    "scope_id": row["scope_id"],
                    "definition_id": row["definition_id"],
                    "definition_name": row["definition_name"],
                    "daily_scores": [],
                }
            scopes[key]["daily_scores"].append({c: row[c] for c in _HISTORY_COLUMNS})

        return {
            "model_id": model["model_id"],
            "model_name": model["model_name"],
            "track": model["track"],
            "metric": metric,
            "scopes": [scopes[key] for key in sorted(scopes, key=lambda k: scope_order(*k))],
        }

    def get_filter_options(self) -> Dict[str, Any]:
        """Definitions, frequency-horizon scopes and calculation dates that have Arena Scores."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT d.id, d.name
                FROM forecasts.daily_arena_scores a
                JOIN challenges.definitions d ON a.scope_id = d.id::text
                WHERE a.scope_type = 'definition'
                ORDER BY d.name
            """)
            definitions = [{"id": row["id"], "name": row["name"]} for row in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT a.scope_id
                FROM forecasts.daily_arena_scores a
                WHERE a.scope_type = 'frequency_horizon' AND a.scope_id IS NOT NULL
                ORDER BY a.scope_id
            """)
            frequency_horizons = [row["scope_id"] for row in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT a.calculation_date
                FROM forecasts.daily_arena_scores a
                ORDER BY a.calculation_date DESC
            """)
            calculation_dates = [
                {
                    "calculation_date": row["calculation_date"],
                    "is_month_end": is_month_end(row["calculation_date"]),
                }
                for row in cur.fetchall()
            ]

        return {
            "definitions": definitions,
            "frequency_horizons": frequency_horizons,
            "calculation_dates": calculation_dates,
        }
