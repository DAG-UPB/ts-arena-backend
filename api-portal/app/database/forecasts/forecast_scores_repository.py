"""Reads and writes for `forecasts.series_scale` and `forecasts.forecast_scores`."""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import delete, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from app.database.challenges.challenge import ChallengeContextData, ChallengeSeriesPseudo
from app.database.forecasts.models import ForecastScore, SeriesScale

_UPSERT_CHUNK = 1000

# Advisory lock namespace per round; the arena scorer uses 42.
ROUND_LOCK_NAMESPACE = 43


class ForecastScoresRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def tables_exist(self) -> bool:
        result = await self.session.execute(
            text(
                "SELECT to_regclass('forecasts.series_scale') IS NOT NULL "
                "AND to_regclass('forecasts.forecast_scores') IS NOT NULL"
            )
        )
        return bool(result.scalar())

    async def try_lock_round(self, round_id: int) -> bool:
        result = await self.session.execute(
            select(func.pg_try_advisory_xact_lock(ROUND_LOCK_NAMESPACE, round_id))
        )
        return bool(result.scalar())

    async def lock_round(self, round_id: int) -> None:
        """Waits for the round's scorer lock; released at the end of the transaction."""
        await self.session.execute(select(func.pg_advisory_xact_lock(ROUND_LOCK_NAMESPACE, round_id)))

    async def rounds_awaiting_capture(self, lookback: timedelta) -> List[Tuple[int, Optional[timedelta]]]:
        """`(round_id, frequency)` of rounds with served context but no scale yet."""
        result = await self.session.execute(
            text("""
                SELECT r.id, r.frequency
                FROM challenges.rounds r
                WHERE r.registration_start <= now()
                  AND r.registration_start > now() - CAST(:lookback AS interval)
                  AND NOT COALESCE(r.is_cancelled, FALSE)
                  AND EXISTS (SELECT 1 FROM challenges.series_pseudo sp WHERE sp.round_id = r.id)
                  AND NOT EXISTS (SELECT 1 FROM forecasts.series_scale s WHERE s.round_id = r.id)
                ORDER BY r.id
            """),
            {"lookback": lookback},
        )
        return [(row[0], row[1]) for row in result.fetchall()]

    async def rounds_to_score(self, lookback: timedelta) -> List[int]:
        """Active or completed rounds with participants, not final yet, ended within `lookback`."""
        result = await self.session.execute(
            text("""
                SELECT r.id
                FROM challenges.v_rounds_with_status r
                WHERE r.status IN ('active', 'completed')
                  AND r.end_time > now() - CAST(:lookback AS interval)
                  AND EXISTS (SELECT 1 FROM challenges.participants p WHERE p.round_id = r.id)
                  AND (
                      NOT EXISTS (SELECT 1 FROM forecasts.forecast_scores s WHERE s.round_id = r.id)
                      OR EXISTS (
                          SELECT 1 FROM forecasts.forecast_scores s
                          WHERE s.round_id = r.id AND NOT s.final_evaluation
                      )
                  )
                ORDER BY r.id
            """),
            {"lookback": lookback},
        )
        return [row[0] for row in result.fetchall()]

    async def get_scales(self, round_id: int) -> Dict[int, Dict[str, Any]]:
        result = await self.session.execute(
            select(SeriesScale).where(SeriesScale.round_id == round_id)
        )
        return {
            row.series_id: {
                "series_id": row.series_id,
                "m": row.m,
                "scale": row.scale,
                "last_value": row.last_value,
                "n_points": row.n_points,
                "n_pairs": row.n_pairs,
                "source": row.source,
            }
            for row in result.scalars()
        }

    async def get_context_windows(self, round_id: int) -> Dict[int, Tuple[datetime, datetime]]:
        """`series_id -> (min_ts, max_ts)` of each series' context; series missing a bound are left out."""
        result = await self.session.execute(
            select(
                ChallengeSeriesPseudo.series_id,
                ChallengeSeriesPseudo.min_ts,
                ChallengeSeriesPseudo.max_ts,
            ).where(ChallengeSeriesPseudo.round_id == round_id)
        )
        return {
            series_id: (min_ts, max_ts)
            for series_id, min_ts, max_ts in result.fetchall()
            if min_ts is not None and max_ts is not None
        }

    async def read_served_context(self, round_id: int) -> List[Dict[str, Any]]:
        """The context as served; only complete while the round is in registration."""
        result = await self.session.execute(
            select(
                ChallengeContextData.series_id,
                ChallengeContextData.ts,
                ChallengeContextData.value,
            ).where(ChallengeContextData.round_id == round_id)
        )
        return [{"series_id": s, "ts": ts, "value": v} for s, ts, v in result.fetchall()]

    async def read_context_as_of(
        self,
        round_id: int,
        bucket: timedelta,
        as_of: datetime,
        lo: datetime,
        hi: datetime,
    ) -> List[Dict[str, Any]]:
        """Rebuild a round's bucketed context from SCD2 as it stood at `as_of`.

        Takes the newest non-NULL version per (series, ts): NULL versions never overwrite a value.
        `lo`/`hi` must span every series' window; only literal bounds let TimescaleDB exclude chunks.
        """
        query = text("""
            SELECT series_id,
                   time_bucket(CAST(:bucket AS interval), ts) AS ts,
                   avg(value) AS value
            FROM (
                SELECT DISTINCT ON (d.series_id, d.ts) d.series_id, d.ts, d.value
                FROM data_portal.time_series_data_scd2 d
                JOIN challenges.series_pseudo sp
                  ON sp.round_id = :round_id
                 AND sp.series_id = d.series_id
                 AND d.ts >= sp.min_ts
                 AND d.ts < sp.max_ts + CAST(:bucket AS interval)
                WHERE d.ts >= CAST(:lo AS timestamptz)
                  AND d.ts < CAST(:hi AS timestamptz) + CAST(:bucket AS interval)
                  AND d.valid_from <= CAST(:as_of AS timestamptz)
                  AND d.value IS NOT NULL
                ORDER BY d.series_id, d.ts, d.valid_from DESC
            ) as_of
            GROUP BY 1, 2
        """)
        result = await self.session.execute(
            query,
            {"round_id": round_id, "bucket": bucket, "as_of": as_of, "lo": lo, "hi": hi},
        )
        return [{"series_id": s, "ts": ts, "value": v} for s, ts, v in result.fetchall()]

    async def read_actuals_as_of(
        self,
        series_ids: List[int],
        bucket: timedelta,
        as_of: datetime,
        lo: datetime,
        hi: datetime,
    ) -> List[Dict[str, Any]]:
        """Bucketed actuals of the series from SCD2 as they stood at `as_of`, for `lo <= ts < hi + bucket`.

        Same rule as `read_context_as_of`: the newest non-NULL version per (series, ts).
        """
        query = text("""
            SELECT series_id,
                   time_bucket(CAST(:bucket AS interval), ts) AS ts,
                   avg(value) AS value
            FROM (
                SELECT DISTINCT ON (d.series_id, d.ts) d.series_id, d.ts, d.value
                FROM data_portal.time_series_data_scd2 d
                WHERE d.series_id = ANY(CAST(:series_ids AS integer[]))
                  AND d.ts >= CAST(:lo AS timestamptz)
                  AND d.ts < CAST(:hi AS timestamptz) + CAST(:bucket AS interval)
                  AND d.valid_from <= CAST(:as_of AS timestamptz)
                  AND d.value IS NOT NULL
                ORDER BY d.series_id, d.ts, d.valid_from DESC
            ) as_of
            GROUP BY 1, 2
        """)
        result = await self.session.execute(
            query,
            {"series_ids": series_ids, "bucket": bucket, "as_of": as_of, "lo": lo, "hi": hi},
        )
        return [{"series_id": s, "ts": ts, "value": v} for s, ts, v in result.fetchall()]

    async def insert_scales(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        stmt = insert(SeriesScale).values(rows).on_conflict_do_nothing(
            index_elements=["round_id", "series_id"]
        )
        await self.session.execute(stmt)

    async def replace_scales(self, rows: List[Dict[str, Any]]) -> None:
        """Insert or overwrite scale rows. Does not commit."""
        keys = ("round_id", "series_id")
        for i in range(0, len(rows), _UPSERT_CHUNK):
            chunk = rows[i:i + _UPSERT_CHUNK]
            stmt = insert(SeriesScale).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=list(keys),
                set_={
                    **{c: stmt.excluded[c] for c in chunk[0] if c not in keys},
                    "computed_at": func.now(),
                },
            )
            await self.session.execute(stmt)

    async def delete_scales(self, round_id: int, series_ids: List[int]) -> None:
        if series_ids:
            await self.session.execute(
                delete(SeriesScale).where(
                    SeriesScale.round_id == round_id, SeriesScale.series_id.in_(series_ids)
                )
            )

    async def get_score_rows(self, round_id: int) -> List[Dict[str, Any]]:
        result = await self.session.execute(
            select(
                ForecastScore.round_id,
                ForecastScore.model_id,
                ForecastScore.series_id,
                ForecastScore.mae,
                ForecastScore.n_points,
                ForecastScore.scale,
                ForecastScore.mase,
                ForecastScore.sql_score,
                ForecastScore.sql_per_quantile,
                ForecastScore.has_quantiles,
                ForecastScore.quantile_levels_count,
                ForecastScore.quantile_crossing_count,
                ForecastScore.data_coverage,
                ForecastScore.final_evaluation,
                ForecastScore.evaluation_status,
                ForecastScore.error_message,
                ForecastScore.method,
                ForecastScore.calculated_at,
            ).where(ForecastScore.round_id == round_id)
        )
        return [dict(row._mapping) for row in result]

    async def update_forecast_scores(self, rows: List[Dict[str, Any]]) -> None:
        """Update rows by primary key; every dict carries the same keys. Does not commit."""
        for i in range(0, len(rows), _UPSERT_CHUNK):
            await self.session.execute(update(ForecastScore), rows[i:i + _UPSERT_CHUNK])

    async def upsert_forecast_scores(self, rows: List[Dict[str, Any]]) -> int:
        """Does not commit."""
        keys = ("round_id", "model_id", "series_id")
        written = 0
        # asyncpg caps a statement at 32767 bind parameters; a row has 20.
        for i in range(0, len(rows), _UPSERT_CHUNK):
            chunk = rows[i:i + _UPSERT_CHUNK]
            stmt = insert(ForecastScore).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=list(keys),
                set_={
                    **{c: stmt.excluded[c] for c in chunk[0] if c not in keys},
                    "calculated_at": func.now(),
                },
            )
            result = await self.session.execute(stmt)
            written += result.rowcount or 0
        return written

    async def window_aggregates(
        self, start: datetime, end: datetime, method: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Scored final rows of rounds ended in [start, end): task counts per definition and
        log sums per (definition, model) for MASE and, on rows with quantiles, SQL."""
        result = await self.session.execute(
            text("""
                WITH scored AS (
                    SELECT f.round_id, f.series_id, f.model_id, r.definition_id,
                           d.frequency::text AS frequency, d.horizon::text AS horizon,
                           f.mase,
                           CASE WHEN f.has_quantiles THEN f.sql_score END AS sql_score
                    FROM forecasts.forecast_scores f
                    JOIN challenges.rounds r ON r.id = f.round_id
                    JOIN challenges.definitions d ON d.id = r.definition_id
                    WHERE f.final_evaluation
                      AND f.evaluation_status IN ('complete', 'partial')
                      AND f.mase IS NOT NULL
                      AND f.method = :method
                      AND NOT COALESCE(r.is_cancelled, FALSE)
                      AND r.end_time >= :start AND r.end_time < :end
                      AND NOT EXISTS (
                          SELECT 1 FROM challenges.definition_series_scd2 ds
                          WHERE ds.definition_id = r.definition_id
                            AND ds.series_id = f.series_id
                            AND ds.is_excluded
                      )
                )
                SELECT definition_id, frequency, horizon, model_id,
                       GROUPING(model_id) = 1 AS is_total,
                       count(DISTINCT (round_id, series_id)) AS tasks,
                       count(*) FILTER (WHERE mase > 0) AS mase_positive,
                       sum(ln(mase)) FILTER (WHERE mase > 0) AS mase_log_sum,
                       count(sql_score) AS sql_tasks,
                       count(*) FILTER (WHERE sql_score > 0) AS sql_positive,
                       sum(ln(sql_score)) FILTER (WHERE sql_score > 0) AS sql_log_sum
                FROM scored
                GROUP BY GROUPING SETS (
                    (definition_id, frequency, horizon),
                    (definition_id, frequency, horizon, model_id)
                )
            """),
            {"start": start, "end": end, "method": method},
        )
        definitions, models = [], []
        for row in result.mappings():
            (definitions if row["is_total"] else models).append(dict(row))
        return definitions, models
