"""Regression tests for a scheduler loop that sleeps past its due schedules (backend-118).

After the dev database was dropped and restored underneath it, APScheduler's schedule loop
slept with no deadline and never woke, while the process stayed healthy. The monitor now
restarts the scheduler once overdue, unacquired schedules are seen on consecutive checks.

Pure unit tests over the monitor's decision logic: the DB helper is faked.
"""
from __future__ import annotations

import asyncio

import pytest

import app.scheduler.scheduler as scheduler_module
from app.scheduler.scheduler import STALL_CONFIRMATIONS, ChallengeScheduler


DB_URL = "postgresql+asyncpg://test:test@localhost:5432/test"


@pytest.fixture
def sched() -> ChallengeScheduler:
    return ChallengeScheduler(database_url=DB_URL)


@pytest.fixture
def overdue(monkeypatch):
    state = {"ids": []}

    async def fake_find(database_url, grace_seconds, logger=None):
        assert database_url == DB_URL
        return list(state["ids"])

    monkeypatch.setattr(scheduler_module, "find_overdue_schedules", fake_find)
    return state


def _check(sched: ChallengeScheduler, times: int = 1) -> list[bool]:
    return [asyncio.run(sched._check_for_stall()) for _ in range(times)]


def test_healthy_scheduler_is_never_restarted(sched, overdue):
    assert _check(sched, times=10) == [False] * 10


def test_single_observation_does_not_restart(sched, overdue):
    overdue["ids"] = ["smard_load_challenge_24h_15min"]

    assert _check(sched) == [False]


def test_confirmed_stall_restarts(sched, overdue):
    overdue["ids"] = ["smard_load_challenge_24h_15min"]

    assert _check(sched, times=STALL_CONFIRMATIONS)[-1] is True


def test_transient_overdue_resets_confirmation(sched, overdue):
    overdue["ids"] = ["periodic_forecast_scoring"]
    _check(sched)

    overdue["ids"] = []
    _check(sched)

    overdue["ids"] = ["periodic_forecast_scoring"]
    assert _check(sched) == [False]


def test_confirmation_restarts_after_a_restart(sched, overdue):
    overdue["ids"] = ["periodic_forecast_scoring"]
    assert _check(sched, times=STALL_CONFIRMATIONS)[-1] is True

    assert _check(sched) == [False]


def test_monitor_restarts_on_confirmed_stall(sched, monkeypatch):
    """The monitor calls the existing restart path once the stall is confirmed."""
    restarts = []
    checks = iter([False, True])

    async def fake_check():
        return next(checks)

    async def fake_restart():
        restarts.append(True)
        sched._shutdown_event.set()

    async def fast_sleep(_seconds):
        return None

    monkeypatch.setattr(scheduler_module, "STALL_CHECK_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(scheduler_module, "ORPHANED_COUNTER_CHECK_INTERVAL_SECONDS", 1e9)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(sched, "_check_for_stall", fake_check)
    monkeypatch.setattr(sched, "_attempt_restart", fake_restart)
    sched._started = True

    asyncio.run(sched._monitor_scheduler())

    assert restarts == [True]
