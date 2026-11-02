"""Detect a scheduler loop that sleeps while its schedules are overdue.

APScheduler ``4.0.0a6`` sleeps without a deadline when ``get_next_schedule_run_time()``
returns nothing, and wakes only on a ``ScheduleAdded``/``ScheduleUpdated`` event. If the
``schedules`` table is replaced underneath it (e.g. a database restore), no event is ever
published and the loop never wakes. The process stays healthy, so the crash monitor does
not notice.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_SCHEDULES_PRESENT = text("SELECT to_regclass('schedules') IS NOT NULL")

# A restart while pg_restore is still loading would fire jobs against a half-restored
# database. Wait until it has finished.
_RESTORE_RUNNING = text(
    """
    SELECT EXISTS (SELECT 1
                     FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND application_name = 'pg_restore')
    """
)

# Mirrors acquire_schedules (not paused, lease free). Rows held by this scheduler are
# skipped: an undeserializable row stays leased to it without ever being released, which
# a restart would not fix. Times come from the app clock, like APScheduler's own.
_FIND_OVERDUE = text(
    """
    SELECT s.id
      FROM schedules s
     WHERE s.next_fire_time < :cutoff
       AND s.paused IS FALSE
       AND (s.acquired_by IS NULL OR s.acquired_until < :now)
       AND s.acquired_by IS DISTINCT FROM :identity
     ORDER BY s.id
    """
)


async def find_overdue_schedules(
    database_url: str,
    grace_seconds: float,
    identity: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> list[str]:
    """Return ids of unacquired schedules whose fire time passed more than ``grace_seconds`` ago.

    Read-only. Returns an empty list on any failure, so a transient DB problem never
    triggers a restart.
    """
    log = logger or logging.getLogger("challenge-scheduler")
    now = datetime.now(timezone.utc)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            if not (await conn.execute(_SCHEDULES_PRESENT)).scalar():
                return []
            if (await conn.execute(_RESTORE_RUNNING)).scalar():
                return []
            rows = await conn.execute(
                _FIND_OVERDUE,
                {
                    "cutoff": now - timedelta(seconds=grace_seconds),
                    "now": now,
                    "identity": identity,
                },
            )
            return [row.id for row in rows.fetchall()]
    except Exception as exc:
        log.warning(f"Could not check for overdue schedules: {exc}")
        return []
    finally:
        await engine.dispose()
