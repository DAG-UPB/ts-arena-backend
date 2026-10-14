"""Reads the rows the Arena Score rates and writes `forecasts.daily_arena_scores`."""
from contextlib import asynccontextmanager
from datetime import date
from typing import Any, AsyncIterator, Dict, List, Optional

import numpy as np
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

_FETCH_ROWS = 50_000

# (name, dtype) of the arrays `kept_rows` returns
ROW_COLUMNS = (
    ("round_id", np.int64),
    ("series_id", np.int64),
    ("model_id", np.int64),
    ("day", np.int64),
    ("registered", np.float64),
    ("mae", np.float64),
    ("sql_score", np.float64),
    ("n_points", np.int64),
)

SCORE_COLUMNS = (
    "calculation_date", "scope_type", "scope_id", "metric", "model_id",
    "score", "ci_lower", "ci_upper", "rank_best", "rank_worst",
    "provisional", "low_coverage", "unranked_disconnected", "coverage",
    "tasks_played", "rounds_played", "weeks_played",
    "mase_30d", "participation_30d", "sql_30d", "sql_participation_30d", "tasks_30d",
    "method",
)

# `day` is the UTC day number of the round's registration start, `registered` its epoch seconds.
# An error that does not count is NULL: not finite or negative, or an SQL score without quantiles.
_KEPT_ROWS = text("""
    SELECT f.round_id, f.series_id, f.model_id,
           (r.registration_start AT TIME ZONE 'UTC')::date - DATE '1970-01-01' AS day,
           EXTRACT(EPOCH FROM r.registration_start)::float8 AS registered,
           CASE WHEN f.mae >= 0 AND f.mae < 'Infinity'::float8 THEN f.mae END AS mae,
           CASE WHEN f.has_quantiles AND f.sql_score >= 0 AND f.sql_score < 'Infinity'::float8
                THEN f.sql_score END AS sql_score,
           f.n_points
    FROM forecasts.forecast_scores f
    JOIN challenges.rounds r ON r.id = f.round_id
    WHERE r.definition_id = :definition_id
      AND f.final_evaluation
      AND NOT COALESCE(r.is_cancelled, FALSE)
      AND f.evaluation_status IN ('complete', 'partial')
      AND r.registration_start IS NOT NULL
      AND f.n_points IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM challenges.definition_series_scd2 ds
          WHERE ds.definition_id = r.definition_id
            AND ds.series_id = f.series_id
            AND ds.is_excluded
      )
""")

_DELETE_GROUP = text("""
    DELETE FROM forecasts.daily_arena_scores
    WHERE calculation_date = :calculation_date
      AND scope_type = :scope_type
      AND COALESCE(scope_id, '') = COALESCE(:scope_id, '')
      AND metric = :metric
""")

_INSERT = text(
    f"INSERT INTO forecasts.daily_arena_scores ({', '.join(SCORE_COLUMNS)}) "
    f"VALUES ({', '.join(':' + c for c in SCORE_COLUMNS)})"
).bindparams(bindparam("method", type_=JSONB))


class ArenaScoresRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @asynccontextmanager
    async def read_transaction(self) -> AsyncIterator[None]:
        """A read transaction without parallel query workers (they need shared memory), ended by a rollback."""
        try:
            await self.session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
            yield
        finally:
            await self.session.rollback()

    async def definitions(self) -> List[Dict[str, Any]]:
        """Every definition with its frequency and horizon as interval text, the scope ids of the rankings."""
        result = await self.session.execute(
            text("SELECT id, frequency::text, horizon::text FROM challenges.definitions ORDER BY id")
        )
        return [
            {"definition_id": row[0], "frequency": row[1], "horizon": row[2]}
            for row in result.fetchall()
        ]

    async def kept_rows(self, definition_id: int) -> Dict[str, np.ndarray]:
        """The rows of one definition that count, as one array per `ROW_COLUMNS` entry.

        Streamed in chunks, so the rows never exist as Python objects all at once. `mae` and
        `sql_score` are NaN where that error does not count for the row.
        """
        result = await self.session.stream(_KEPT_ROWS, {"definition_id": definition_id})
        chunks = [
            [np.array(values, dtype=dtype) for values, (_, dtype) in zip(zip(*rows), ROW_COLUMNS)]
            async for rows in result.partitions(_FETCH_ROWS)
        ]
        return {
            name: np.concatenate([chunk[k] for chunk in chunks]) if chunks else np.empty(0, dtype)
            for k, (name, dtype) in enumerate(ROW_COLUMNS)
        }

    async def replace_scores(
        self,
        calculation_date: date,
        scope_type: str,
        scope_id: Optional[str],
        metric: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Delete the (date, scope, metric) group and insert `rows`. Does not commit.

        An empty `rows` leaves the group as it is: it cannot be told from an upstream outage.
        """
        if not rows:
            return 0
        await self.session.execute(
            _DELETE_GROUP,
            {
                "calculation_date": calculation_date,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "metric": metric,
            },
        )
        await self.session.execute(_INSERT, rows)
        return len(rows)
