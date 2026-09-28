"""Reads and writes for `ForecastScoringService`: `forecasts.series_scale` and
`forecasts.forecast_scores`, and the rounds the service works on.

Nothing here reads `forecasts.scores`: the service picks its rounds by its own table.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from app.database.challenges.challenge import ChallengeContextData, ChallengeSeriesPseudo
from app.database.forecasts.models import ForecastScore, SeriesScale

_UPSERT_CHUNK = 1000

# First key of the per-round advisory lock, so the scheduled job and the backfill never
# score the same round at once. The arena scorer locks its rounds under 42.
ROUND_LOCK_NAMESPACE = 43


class ForecastScoresRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def tables_exist(self) -> bool:
        """Both tables are present, i.e. the migration has been applied to this database."""
        result = await self.session.execute(
            text(
                "SELECT to_regclass('forecasts.series_scale') IS NOT NULL "
                "AND to_regclass('forecasts.forecast_scores') IS NOT NULL"
            )
        )
        return bool(result.scalar())

    async def try_lock_round(self, round_id: int) -> bool:
        """Take the round's advisory lock for the current transaction, without waiting."""
        result = await self.session.execute(
            select(func.pg_try_advisory_xact_lock(ROUND_LOCK_NAMESPACE, round_id))
        )
        return bool(result.scalar())

    async def rounds_awaiting_capture(self, lookback: timedelta) -> List[Tuple[int, Optional[timedelta]]]:
        """`(round_id, frequency)` of rounds whose context was served but has no scale yet.

        The context is written together with `series_pseudo` in one transaction, so a round
        with `series_pseudo` rows has its complete served context in `context_data`. Only
        rounds whose registration started within `lookback` qualify: `context_data` is a
        serving cache and is not kept, so later rounds are rebuilt from SCD2 instead.
        """
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
        """Active or completed rounds with participants that are not final yet.

        A round is final once it has `forecast_scores` rows and none of them is pending.
        Rounds that ended more than `lookback` ago are left out, so a round that never gets
        a row (every forecast outside its window) drops out on its own; the backfill covers
        anything older. The status comes from `v_rounds_with_status`, so cancelled rounds
        are never scored.
        """
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
        """`series_id -> stored scale row` for one round."""
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

        Only reliable while the round is in registration: the table is a serving cache and
        is not kept, so a later read may find nothing or only part of it. Everything after
        that uses `read_context_as_of`.
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

        With `as_of = rounds.created_at` this reproduces the served context, which was read
        from the resolution aggregate over `time_series_data`. What that table held at a
        point in time is, per (series, ts), the newest SCD2 version with a value written by
        then. NULL gap markers never reach `time_series_data` (its `value` is NOT NULL), so
        they never remove a value there, even where SCD2 records them as the version that
        superseded it. The fuel-price series do exactly that minutes after every point,
        which is why "the version valid at `as_of`" would lose most of their context.
        Values are bucketed to the round resolution over each series' `[min_ts, max_ts]`
        from `series_pseudo`.

        `lo`/`hi` must span every series' window. They have to be literal bounds on `d.ts`:
        per-series bounds arriving through the join do not let TimescaleDB exclude chunks,
        which costs tens of seconds per round.
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
        """Store scales. An existing (round, series) scale is never overwritten."""
        if not rows:
            return
        stmt = insert(SeriesScale).values(rows).on_conflict_do_nothing(
            index_elements=["round_id", "series_id"]
        )
        await self.session.execute(stmt)

    async def upsert_forecast_scores(self, rows: List[Dict[str, Any]]) -> int:
        """Insert or refresh `forecasts.forecast_scores` rows. Does not commit."""
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
