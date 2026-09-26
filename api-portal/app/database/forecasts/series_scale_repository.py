"""Reads and writes for context-scaled MASE (`forecasts.series_scale`, `forecasts.scores_mase`)."""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from app.database.challenges.challenge import ChallengeContextData, ChallengeSeriesPseudo
from app.database.forecasts.models import MaseScore, SeriesScale

_UPSERT_CHUNK = 1000


class SeriesScaleRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def tables_exist(self) -> bool:
        """Both tables are present, i.e. the migration has been applied to this database."""
        result = await self.session.execute(
            text(
                "SELECT to_regclass('forecasts.series_scale') IS NOT NULL "
                "AND to_regclass('forecasts.scores_mase') IS NOT NULL"
            )
        )
        return bool(result.scalar())

    async def get_scales(self, round_id: int) -> Dict[int, Dict[str, Any]]:
        """`series_id -> stored scale row` for one round."""
        result = await self.session.execute(
            select(SeriesScale).where(SeriesScale.round_id == round_id)
        )
        return {
            row.series_id: {
                "series_id": row.series_id,
                "m": row.m,
                "scale": row.scale,
                "n_points": row.n_points,
                "n_pairs": row.n_pairs,
                "source": row.source,
            }
            for row in result.scalars()
        }

    async def get_context_windows(self, round_id: int) -> Dict[int, Tuple[datetime, datetime]]:
        """`series_id -> (min_ts, max_ts)` of each series' context, from `series_pseudo`.

        `series_pseudo` keeps these for every round. Series without both bounds are left out:
        there is no context window to read.
        """
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
        """The context exactly as served for a round, from `challenges.context_data`.

        Only valid at round creation, straight after the context was written: the table is a
        serving cache for registration and is not kept, so a later read may find nothing or
        only part of it. Everything after round creation uses `read_context_as_of`.
        """
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
        """Rebuild a round's context from SCD2 history, as the data stood at `as_of`.

        With `as_of = rounds.created_at` this reproduces the served context: the context was
        read from the resolution aggregate over `time_series_data`, and SCD2 mirrors that
        table except for NULL gap markers, which are dropped here. Values are bucketed to the
        round resolution over each series' `[min_ts, max_ts]` from `series_pseudo`.

        `lo`/`hi` must span every series' window. They have to be literal bounds on `d.ts`:
        per-series bounds arriving through the join do not let TimescaleDB exclude chunks,
        which costs tens of seconds per round.
        """
        query = text("""
            SELECT d.series_id,
                   time_bucket(CAST(:bucket AS interval), d.ts) AS ts,
                   avg(d.value) AS value
            FROM data_portal.time_series_data_scd2 d
            JOIN challenges.series_pseudo sp
              ON sp.round_id = :round_id
             AND sp.series_id = d.series_id
             AND d.ts >= sp.min_ts
             AND d.ts < sp.max_ts + CAST(:bucket AS interval)
            WHERE d.ts >= CAST(:lo AS timestamptz)
              AND d.ts < CAST(:hi AS timestamptz) + CAST(:bucket AS interval)
              AND d.valid_from <= CAST(:as_of AS timestamptz)
              AND d.valid_during @> CAST(:as_of AS timestamptz)
              AND d.value IS NOT NULL
            GROUP BY 1, 2
        """)
        result = await self.session.execute(
            query,
            {"round_id": round_id, "bucket": bucket, "as_of": as_of, "lo": lo, "hi": hi},
        )
        return [{"series_id": s, "ts": ts, "value": v} for s, ts, v in result.fetchall()]

    async def insert_scales(self, rows: List[Dict[str, Any]]) -> None:
        """Store scales. An existing (round, series) scale is never overwritten."""
        if not rows:
            return
        stmt = insert(SeriesScale).values(rows).on_conflict_do_nothing(
            index_elements=["round_id", "series_id"]
        )
        await self.session.execute(stmt)

    async def upsert_mase_scores(self, rows: List[Dict[str, Any]]) -> int:
        """Insert or refresh `forecasts.scores_mase` rows. Does not commit."""
        keys = ("round_id", "model_id", "series_id")
        written = 0
        # asyncpg caps a statement at 32767 bind parameters; a row has 19.
        for i in range(0, len(rows), _UPSERT_CHUNK):
            chunk = rows[i:i + _UPSERT_CHUNK]
            stmt = insert(MaseScore).values(chunk)
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
