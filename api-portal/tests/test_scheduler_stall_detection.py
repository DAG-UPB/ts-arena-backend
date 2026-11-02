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
    state = {"ids": [], "identity": None}

    async def fake_find(database_url, grace_seconds, identity, logger=None):
        assert database_url == DB_URL
        state["identity"] = identity
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


def test_own_identity_is_excluded(sched, overdue):
    """Rows leased to this scheduler (e.g. undeserializable ones) never count as a stall."""
    _check(sched)

    assert overdue["identity"] == sched.scheduler.identity


def test_transient_overdue_resets_confirmation(sched, overdue):
    overdue["ids"] = ["periodic_forecast_scoring"]
    _check(sched)

    overdue["ids"] = []
    _check(sched)

    overdue["ids"] = ["periodic_forecast_scoring"]
    assert _check(sched) == [False]


def test_confirmation_count_resets_after_a_confirmed_stall(sched, overdue):
    overdue["ids"] = ["periodic_forecast_scoring"]
    assert _check(sched, times=STALL_CONFIRMATIONS)[-1] is True

    assert _check(sched) == [False]


@pytest.fixture
def fast_monitor(monkeypatch, sched):
    """Run the monitor loop without waiting; each check interval elapses immediately."""
    async def fast_sleep(_seconds):
        return None

    monkeypatch.setattr(scheduler_module, "STALL_CHECK_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(scheduler_module, "RESTART_RETRY_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(scheduler_module, "ORPHANED_COUNTER_CHECK_INTERVAL_SECONDS", 1e9)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fast_sleep)
    return sched


def _run_monitor(sched, monkeypatch, checks, max_loops=10):
    """Drive the monitor with scripted stall results; return the number of restarts."""
    restarts = []
    results = iter(checks)
    loops = {"n": 0}

    async def fake_check():
        loops["n"] += 1
        if loops["n"] >= max_loops:
            sched._shutdown_event.set()
        return next(results, False)

    async def fake_restart():
        restarts.append(True)

    monkeypatch.setattr(sched, "_check_for_stall", fake_check)
    monkeypatch.setattr(sched, "_attempt_restart", fake_restart)
    sched._started = True
    asyncio.run(sched._monitor_scheduler())
    return len(restarts)


def test_monitor_restarts_on_confirmed_stall(fast_monitor, monkeypatch):
    assert _run_monitor(fast_monitor, monkeypatch, [False, True]) == 1


def test_monitor_does_not_restart_again_within_cooldown(fast_monitor, monkeypatch):
    """A stall that is back right after a restart is logged, not restarted in a loop."""
    assert _run_monitor(fast_monitor, monkeypatch, [True, True, True]) == 1


def test_monitor_skips_stall_check_while_not_started(fast_monitor, monkeypatch):
    calls = []

    async def fake_check():
        calls.append(True)
        return True

    async def stop_after_first_sleep(_seconds):
        fast_monitor._shutdown_event.set()

    monkeypatch.setattr(fast_monitor, "_check_for_stall", fake_check)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", stop_after_first_sleep)
    fast_monitor._started = False

    asyncio.run(fast_monitor._monitor_scheduler())

    assert calls == []


def test_failed_restart_is_retried(fast_monitor, monkeypatch):
    """A restart that fails leaves _started False; the monitor must keep retrying."""
    attempts = []

    async def failing_then_ok_restart():
        attempts.append(True)
        if len(attempts) < 3:
            fast_monitor._restart_pending = True
            return
        fast_monitor._restart_pending = False
        fast_monitor._started = True
        fast_monitor._shutdown_event.set()

    monkeypatch.setattr(fast_monitor, "_attempt_restart", failing_then_ok_restart)
    fast_monitor._started = False
    fast_monitor._restart_pending = True

    asyncio.run(fast_monitor._monitor_scheduler())

    assert len(attempts) == 3


def test_pending_retry_lifts_the_attempt_cap(fast_monitor, monkeypatch):
    """After the cap is reached, a pending restart is retried instead of given up for good."""
    attempts = []

    async def restart():
        attempts.append(fast_monitor._restart_count)
        fast_monitor._shutdown_event.set()

    monkeypatch.setattr(fast_monitor, "_attempt_restart", restart)
    fast_monitor._started = False
    fast_monitor._restart_pending = True
    fast_monitor._restart_count = fast_monitor._max_restart_attempts

    asyncio.run(fast_monitor._monitor_scheduler())

    assert attempts == [0]
