"""Repair for orphaned APScheduler ``tasks.running_jobs`` counters (backend-75).

APScheduler v4 tracks the free execution slots of a task as
``tasks.max_running_jobs - tasks.running_jobs``. It increments ``running_jobs`` when a
job is acquired and decrements it when the job is released — each in the *same*
transaction as the matching write to ``jobs.acquired_by``. So "counter above zero with
no acquired job row" is never a legitimate state; a reader either sees both writes or
neither.

If a run dies between acquire and release (the process is killed, or the scheduler
instance is discarded mid-job by the crash-restart path), the increment commits and the
decrement never does. The counter is then **orphaned**: with ``max_running_jobs=1`` the
task has zero free slots forever, so every later fire is enqueued, never acquired, and
discarded once its ``start_deadline`` passes. The job never runs again.

APScheduler ``4.0.0a6`` cannot repair this on its own:

* ``cleanup()`` and ``reap_abandoned_jobs()`` both work by finding *surviving job rows*
  whose lease has lapsed and releasing them. An orphaned counter has no job row behind
  it, so both are blind to it.
* ``add_task()``'s on-conflict UPDATE — which ``configure_task()`` runs on every startup
  — rewrites ``func``/``job_executor``/``max_running_jobs``/``misfire_grace_time``/
  ``metadata`` and deliberately leaves ``running_jobs`` untouched.

Restarting the service therefore does **not** clear it. That is what stopped the ELO
ranking job on 2026-08-04 and left both public leaderboards frozen on a ten-day-old
snapshot until it was found on 2026-08-17.

Two repairs live here, each matched to its concurrency situation:

``reconcile_running_job_counters``
    Exact recompute of every task's counter from the job rows. Only safe while nothing
    is acquiring jobs, so it runs at scheduler start and after a crash-restart, before
    the runner task is spawned.

``find_orphaned_counters`` / ``clear_orphaned_counter``
    A deliberately narrow repair for use *while the scheduler is live*: a task whose
    counter is above zero with no acquired job row at all. The clear is a
    compare-and-swap on the observed counter value, so a concurrent acquisition makes it
    a no-op rather than clobbering a legitimate increment. The caller is expected to
    require the anomaly on two consecutive observations before clearing.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Present only once the APScheduler data store has created its schema — i.e. not on the
# very first startup against an empty database. Every statement below is guarded by it.
_TABLES_PRESENT = text(
    "SELECT to_regclass('tasks') IS NOT NULL AND to_regclass('jobs') IS NOT NULL"
)

# Exact recompute. ``previous`` is read from the pre-update snapshot in the subquery, so
# the RETURNING clause can report both the stale value and the corrected one.
_RECONCILE = text(
    """
    UPDATE tasks AS t
    SET running_jobs = c.actual
    FROM (
        SELECT t2.id,
               t2.running_jobs AS previous,
               (SELECT count(*)
                  FROM jobs j
                 WHERE j.task_id = t2.id
                   AND j.acquired_by IS NOT NULL) AS actual
          FROM tasks t2
    ) AS c
    WHERE t.id = c.id
      AND t.running_jobs <> c.actual
    RETURNING c.id AS id, c.previous AS previous, c.actual AS actual
    """
)

_FIND_ORPHANED = text(
    """
    SELECT t.id, t.running_jobs
      FROM tasks t
     WHERE t.running_jobs > 0
       AND NOT EXISTS (SELECT 1
                         FROM jobs j
                        WHERE j.task_id = t.id
                          AND j.acquired_by IS NOT NULL)
    """
)

# Compare-and-swap: ``running_jobs = :observed`` is re-checked against the committed row
# at write time, so an acquisition that landed since the observation makes this a no-op.
_CLEAR_ORPHANED = text(
    """
    UPDATE tasks AS t
    SET running_jobs = 0
    WHERE t.id = :task_id
      AND t.running_jobs = :observed
      AND NOT EXISTS (SELECT 1
                        FROM jobs j
                       WHERE j.task_id = t.id
                         AND j.acquired_by IS NOT NULL)
    """
)


async def _tables_present(conn) -> bool:
    return bool((await conn.execute(_TABLES_PRESENT)).scalar())


async def reconcile_running_job_counters(
    database_url: str,
    logger: Optional[logging.Logger] = None,
) -> list[tuple[str, int, int]]:
    """Recompute every task's ``running_jobs`` from its acquired job rows.

    Returns the corrections made as ``(task_id, previous, actual)`` tuples.

    Counting *all* rows with a non-null ``acquired_by`` — not only those with a live
    lease — is deliberate. A lapsed lease is still counted by the counter, and
    APScheduler's own ``cleanup()`` will release that row and decrement in due course;
    excluding it here would double-count the correction and drive the counter negative.

    Must only be called while nothing is acquiring jobs. Never raises: a failure here
    must not stop the scheduler from starting.
    """
    log = logger or logging.getLogger("challenge-scheduler")
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as conn:
            if not await _tables_present(conn):
                log.debug(
                    "Scheduler tables not present yet; skipping running_jobs reconciliation."
                )
                return []

            corrections = [
                (row.id, row.previous, row.actual)
                for row in (await conn.execute(_RECONCILE)).fetchall()
            ]
    except Exception as exc:  # pragma: no cover - defensive, startup must not fail here
        log.error(f"Failed to reconcile running_jobs counters: {exc}", exc_info=True)
        return []
    finally:
        await engine.dispose()

    for task_id, previous, actual in corrections:
        log.critical(
            f"Repaired stale scheduler slot counter: task '{task_id}' had "
            f"running_jobs={previous} with {actual} acquired job(s). The task was "
            f"holding execution slots it was not using and its fires were being "
            f"discarded unexecuted (backend-75). Reset to {actual}."
        )
    if not corrections:
        log.debug("running_jobs counters consistent; no reconciliation needed.")
    return corrections


async def find_orphaned_counters(
    database_url: str,
    logger: Optional[logging.Logger] = None,
) -> dict[str, int]:
    """Return ``{task_id: running_jobs}`` for tasks with a counter but no acquired job.

    Safe to call against a live scheduler: it only reads. Returns an empty mapping on
    any failure, so a transient DB problem reads as "nothing to repair" rather than
    triggering one.
    """
    log = logger or logging.getLogger("challenge-scheduler")
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as conn:
            if not await _tables_present(conn):
                return {}
            return {row.id: row.running_jobs for row in (await conn.execute(_FIND_ORPHANED)).fetchall()}
    except Exception as exc:
        log.warning(f"Could not check for orphaned running_jobs counters: {exc}")
        return {}
    finally:
        await engine.dispose()


async def clear_orphaned_counter(
    database_url: str,
    task_id: str,
    observed: int,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Compare-and-swap ``running_jobs`` from ``observed`` back to 0 for ``task_id``.

    Returns True only if the counter was actually cleared. A concurrent acquisition —
    which changes the counter and adds an acquired job row in one transaction — makes
    this a no-op and returns False.
    """
    log = logger or logging.getLogger("challenge-scheduler")
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as conn:
            if not await _tables_present(conn):
                return False
            result = await conn.execute(
                _CLEAR_ORPHANED, {"task_id": task_id, "observed": observed}
            )
            cleared = bool(result.rowcount)
    except Exception as exc:
        log.warning(f"Could not clear orphaned running_jobs counter for '{task_id}': {exc}")
        return False
    finally:
        await engine.dispose()

    if cleared:
        log.critical(
            f"Repaired stale scheduler slot counter while running: task '{task_id}' "
            f"held running_jobs={observed} with no acquired job. Its fires were being "
            f"discarded unexecuted (backend-75). Reset to 0; the next fire will run."
        )
    else:
        log.info(
            f"Orphaned counter for '{task_id}' resolved itself before repair "
            f"(observed running_jobs={observed}); no change made."
        )
    return cleared
