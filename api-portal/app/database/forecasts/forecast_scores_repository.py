"""Reads and writes for `forecasts.series_scale` and `forecasts.forecast_scores`."""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, text
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

    async def insert_scales(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        stmt = insert(SeriesScale).values(rows).on_conflict_do_nothing(
            index_elements=["round_id", "series_id"]
        )
        await self.session.execute(stmt)

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
