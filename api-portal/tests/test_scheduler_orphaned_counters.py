"""Regression tests for orphaned ``tasks.running_jobs`` counters (backend-75).

A job that dies between being acquired and being released leaves APScheduler's slot
counter incremented with no job row behind it. With ``max_running_jobs=1`` the task then
has zero free slots forever: every later fire is enqueued, never acquired, and discarded
at its ``start_deadline``. APScheduler cannot repair this (its own repair paths key off
surviving job rows), and the on-conflict UPDATE it runs at startup leaves the counter
alone — so a restart does not clear it either. That is what froze the public leaderboards
on a ten-day-old ELO snapshot in 2026-08.

These are pure unit tests over the monitor's decision logic: the two DB helpers are
faked, so no database is touched. The SQL itself is verified live on dev. Async
coroutines are driven with ``asyncio.run`` to match the plain-pytest convention used
elsewhere in this suite (no pytest-asyncio).
"""
from __future__ import annotations

import asyncio

import pytest

import app.scheduler.scheduler as scheduler_module
from app.scheduler.scheduler import (
    ORPHANED_COUNTER_CONFIRMATIONS,
    ChallengeScheduler,
)


DB_URL = "postgresql+asyncpg://test:test@localhost:5432/test"


@pytest.fixture
def sched() -> ChallengeScheduler:
    """A scheduler wired to a URL that is never connected to."""
    return ChallengeScheduler(database_url=DB_URL)


@pytest.fixture
def repair_calls(monkeypatch):
    """Fake the two DB helpers; record every clear attempt.

    ``orphaned`` is the mapping the monitor will see on each check — mutate it between
    checks to simulate the counter changing underneath.
    """
    state = {"orphaned": {}, "cleared": [], "clear_result": True}

    async def fake_find(database_url, logger=None):
        assert database_url == DB_URL
        return dict(state["orphaned"])

    async def fake_clear(database_url, task_id, observed, logger=None):
        state["cleared"].append((task_id, observed))
        return state["clear_result"]

    monkeypatch.setattr(scheduler_module, "find_orphaned_counters", fake_find)
    monkeypatch.setattr(scheduler_module, "clear_orphaned_counter", fake_clear)
    return state


def _check(sched: ChallengeScheduler, times: int = 1) -> None:
    for _ in range(times):
        asyncio.run(sched._check_for_orphaned_counters())


def test_single_observation_does_not_clear(sched, repair_calls):
    """One sighting is never enough — it must be confirmed first."""
    repair_calls["orphaned"] = {"elo_job": 1}

    _check(sched)

    assert repair_calls["cleared"] == []


def test_cleared_once_confirmed(sched, repair_calls):
    """A counter that is still standing at the same value on the next check is repaired."""
    repair_calls["orphaned"] = {"elo_job": 1}

    _check(sched, times=ORPHANED_COUNTER_CONFIRMATIONS)

    assert repair_calls["cleared"] == [("elo_job", 1)]


def test_transient_anomaly_is_not_cleared(sched, repair_calls):
    """A counter that resolves itself between checks must be left alone."""
    repair_calls["orphaned"] = {"elo_job": 1}
    _check(sched)

    repair_calls["orphaned"] = {}
    _check(sched, times=ORPHANED_COUNTER_CONFIRMATIONS)

    assert repair_calls["cleared"] == []


def test_changed_counter_restarts_confirmation(sched, repair_calls):
    """A different counter value is a different observation, not a confirmation.

    A counter that moves is a task that is actually doing something; the repair must
    start counting again rather than acting on the earlier sighting.
    """
    repair_calls["orphaned"] = {"elo_job": 1}
    _check(sched)

    repair_calls["orphaned"] = {"elo_job": 2}
    _check(sched)

    assert repair_calls["cleared"] == []

    _check(sched)
    assert repair_calls["cleared"] == [("elo_job", 2)]


def test_each_task_confirmed_independently(sched, repair_calls):
    """Confirmations are tracked per task, not globally."""
    repair_calls["orphaned"] = {"elo_job": 1}
    _check(sched)

    repair_calls["orphaned"] = {"elo_job": 1, "eval_job": 1}
    _check(sched)

    assert repair_calls["cleared"] == [("elo_job", 1)]

    _check(sched)
    assert ("eval_job", 1) in repair_calls["cleared"]


def test_failed_clear_is_retried_from_scratch(sched, repair_calls):
    """If the compare-and-swap loses to a concurrent acquisition, re-confirm first."""
    repair_calls["orphaned"] = {"elo_job": 1}
    repair_calls["clear_result"] = False

    _check(sched, times=ORPHANED_COUNTER_CONFIRMATIONS)
    assert repair_calls["cleared"] == [("elo_job", 1)]

    # Still reported as orphaned: one more check must not immediately clear again.
    _check(sched)
    assert repair_calls["cleared"] == [("elo_job", 1)]

    _check(sched)
    assert repair_calls["cleared"] == [("elo_job", 1), ("elo_job", 1)]


def test_healthy_scheduler_never_writes(sched, repair_calls):
    """No anomaly reported means no write, however long the monitor runs."""
    repair_calls["orphaned"] = {}

    _check(sched, times=10)

    assert repair_calls["cleared"] == []
    assert sched._orphan_watch == {}
