"""The Arena Score flag, job and schedule, with ``SessionLocal``, the service and the scheduler faked."""
from __future__ import annotations

import asyncio
import importlib
import logging
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from apscheduler import CoalescePolicy, ConflictPolicy, ScheduleLookupError

import app.core.config as config_module
import app.scheduler.jobs as jobs
import app.scheduler.scheduler as scheduler_module
from app.scheduler.scheduler import ChallengeScheduler

SCHEDULE_ID = "periodic_arena_score_calculation"
DB_URL = "postgresql+asyncpg://test:test@localhost:5432/test"


def _messages(caplog, min_level=logging.INFO):
    return [r.getMessage() for r in caplog.records if r.levelno >= min_level]


# --- the flag ---------------------------------------------------------------

@pytest.fixture
def load_flag(monkeypatch):
    """Re-import the config under a given ARENA_SCORE_JOB_ENABLED and return the flag."""
    def load(value):
        if value is None:
            monkeypatch.delenv("ARENA_SCORE_JOB_ENABLED", raising=False)
        else:
            monkeypatch.setenv("ARENA_SCORE_JOB_ENABLED", value)
        return importlib.reload(config_module).Config.ARENA_SCORE_JOB_ENABLED

    yield load
    monkeypatch.undo()
    importlib.reload(config_module)


@pytest.mark.parametrize("value", [None, "", "0", "false", "no", "off", "2", "enabled"])
def test_flag_is_off_unless_set_to_a_true_value(load_flag, value):
    assert load_flag(value) is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "Yes", " true "])
def test_flag_is_on_for_one_true_and_yes_in_any_case(load_flag, value):
    assert load_flag(value) is True


# --- the job ----------------------------------------------------------------

def _summary(**changes):
    return {
        "calculation_date": date(2026, 10, 14),
        "rows": {"global": {"point": 12, "probabilistic": 9}, "definition": {"point": 40}},
        "failures": [],
        "groups": 3,
        "scopes": 2,
        "window_unmatched": 0,
        "seconds": 45.2,
        **changes,
    }


@pytest.fixture
def arena(monkeypatch):
    """The database and the service the job talks to, recording what it asks of them."""
    state = SimpleNamespace(tables=True, statements=[], dates=[], error=None, summary=_summary())

    class Session:
        async def execute(self, statement):
            state.statements.append(str(statement))
            return SimpleNamespace(scalar=lambda: state.tables)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    async def compute(session, calculation_date):
        state.dates.append(calculation_date)
        if state.error:
            raise state.error
        return state.summary

    monkeypatch.setattr(jobs, "SessionLocal", Session)
    monkeypatch.setattr(jobs, "compute_and_store_arena_scores", compute)
    return state


def _run_job():
    asyncio.run(jobs.periodic_arena_score_calculation_job.__wrapped__())


def test_skips_without_calling_the_service_when_its_tables_are_missing(arena, caplog):
    arena.tables = False
    with caplog.at_level(logging.INFO, logger="challenge-scheduler"):
        _run_job()
    assert arena.dates == []
    assert "Arena Score skipped: its tables do not exist yet." in _messages(caplog)


def test_the_table_check_covers_both_tables(arena):
    _run_job()
    assert len(arena.statements) == 1
    assert "forecasts.daily_arena_scores" in arena.statements[0]
    assert "forecasts.forecast_scores" in arena.statements[0]


def test_calls_the_service_once_with_the_utc_date(arena, monkeypatch):
    class Clock:
        @staticmethod
        def now(tz=None):
            utc = datetime(2026, 10, 14, 23, 30, tzinfo=timezone.utc)
            if tz is None:  # a local-time call at UTC+2 already sees the 15th
                return utc.astimezone(timezone(timedelta(hours=2))).replace(tzinfo=None)
            return utc.astimezone(tz)

    monkeypatch.setattr(jobs, "datetime", Clock)
    _run_job()
    assert arena.dates == [date(2026, 10, 14)]


@pytest.mark.parametrize("rows, shown", [
    (_summary()["rows"], "global/point=12, global/probabilistic=9, definition/point=40"),
    ({}, "none"),
])
def test_logs_one_summary_line_without_errors(arena, caplog, rows, shown):
    arena.summary = _summary(rows=rows)
    with caplog.at_level(logging.INFO, logger="challenge-scheduler"):
        _run_job()
    assert f"Arena Score job done for 2026-10-14 in 45.2 s: rows {shown}; failed groups: 0" in _messages(caplog)
    assert _messages(caplog, logging.ERROR) == []


def test_failures_are_listed_in_one_error_line(arena, caplog):
    arena.summary = _summary(failures=[
        {"scope_type": "definition", "scope_id": "7", "metric": "point", "error": "ValueError: boom\n[SQL: ...]"},
        {"scope_type": "global", "scope_id": None, "metric": "probabilistic", "error": "KeyError: 3"},
    ])
    with caplog.at_level(logging.INFO, logger="challenge-scheduler"):
        _run_job()
    assert _messages(caplog, logging.ERROR) == [
        "Arena Score job: 2 group(s) failed: definition 7 [point]: ValueError: boom; global [probabilistic]: KeyError: 3"
    ]
    assert any("failed groups: 2" in message for message in _messages(caplog))


def test_a_service_error_is_logged_and_stays_out_of_the_scheduler(arena, caplog):
    arena.error = RuntimeError("database gone")
    with pytest.raises(RuntimeError):
        _run_job()
    with caplog.at_level(logging.INFO, logger="challenge-scheduler"):
        asyncio.run(jobs.periodic_arena_score_calculation_job())
    assert any("database gone" in message for message in _messages(caplog, logging.ERROR))


# --- the schedule -----------------------------------------------------------

class _RecordingScheduler:
    def __init__(self, configure_error=None, remove_error=None):
        self.configure_error = configure_error
        self.remove_error = remove_error
        self.task = None
        self.schedule = None
        self.removed = []

    async def configure_task(self, func, **kwargs):
        if self.configure_error:
            raise self.configure_error
        self.task = (func, kwargs)

    async def add_schedule(self, **kwargs):
        self.schedule = kwargs

    async def remove_schedule(self, id):
        self.removed.append(id)
        if self.remove_error:
            raise self.remove_error


def _schedule(monkeypatch, inner, enabled):
    monkeypatch.setattr(scheduler_module.Config, "ARENA_SCORE_JOB_ENABLED", enabled)
    scheduler = ChallengeScheduler.__new__(ChallengeScheduler)
    scheduler.scheduler = inner
    scheduler.logger = logging.getLogger("test-scheduler")
    asyncio.run(scheduler.schedule_periodic_arena_score_calculation())


def test_enabled_runs_four_times_a_day_at_forty_past(monkeypatch, caplog):
    inner = _RecordingScheduler()
    with caplog.at_level(logging.INFO, logger="test-scheduler"):
        _schedule(monkeypatch, inner, enabled=True)

    func, task_kwargs = inner.task
    assert func is jobs.periodic_arena_score_calculation_job
    assert task_kwargs == {"max_running_jobs": 1, "misfire_grace_time": 3600}
    schedule = inner.schedule
    assert schedule["func_or_task_id"] is jobs.periodic_arena_score_calculation_job
    assert schedule["id"] == SCHEDULE_ID
    assert (schedule["trigger"].hour, schedule["trigger"].minute) == ("3,9,15,21", "40")
    assert schedule["coalesce"] is CoalescePolicy.latest
    assert schedule["misfire_grace_time"] == 3600
    assert schedule["conflict_policy"] is ConflictPolicy.replace
    assert inner.removed == []
    assert ("Scheduled periodic Arena Score calculation job (runs at 03:40, 09:40, 15:40, 21:40 UTC)"
            in _messages(caplog))


def test_disabled_schedules_nothing_and_removes_a_persisted_schedule(monkeypatch, caplog):
    inner = _RecordingScheduler()
    with caplog.at_level(logging.INFO, logger="test-scheduler"):
        _schedule(monkeypatch, inner, enabled=False)

    assert inner.task is None
    assert inner.schedule is None
    assert inner.removed == [SCHEDULE_ID]
    assert "Arena Score job disabled (ARENA_SCORE_JOB_ENABLED is off)" in _messages(caplog)


def test_a_schedule_not_found_on_removal_is_ignored(monkeypatch, caplog):
    inner = _RecordingScheduler(remove_error=ScheduleLookupError(SCHEDULE_ID))
    with caplog.at_level(logging.INFO, logger="test-scheduler"):
        _schedule(monkeypatch, inner, enabled=False)

    assert inner.removed == [SCHEDULE_ID]
    assert _messages(caplog, logging.ERROR) == []
    assert "Arena Score job disabled (ARENA_SCORE_JOB_ENABLED is off)" in _messages(caplog)


@pytest.mark.parametrize("enabled, failing", [
    (True, {"configure_error": RuntimeError("data store down")}),
    (False, {"remove_error": RuntimeError("data store down")}),
])
def test_a_failing_schedule_is_logged_and_does_not_raise(monkeypatch, caplog, enabled, failing):
    with caplog.at_level(logging.INFO, logger="test-scheduler"):
        _schedule(monkeypatch, _RecordingScheduler(**failing), enabled=enabled)
    assert any("Failed to schedule Arena Score job" in message for message in _messages(caplog, logging.ERROR))


@pytest.fixture
def quiet_scheduler(monkeypatch):
    """A scheduler with its runner, monitor and database helpers stubbed, recording the schedule calls."""
    scheduler = ChallengeScheduler(database_url=DB_URL, restart_delay=0)
    calls = []

    async def nothing(*args, **kwargs):
        return None

    async def record(name):
        calls.append(name)

    monkeypatch.setattr(scheduler_module, "reconcile_running_job_counters", nothing)
    for name in ("_run_scheduler_with_crash_handling", "_wait_until_ready", "_stop_scheduler_task",
                 "_delayed_startup_elo_check", "_monitor_scheduler"):
        monkeypatch.setattr(scheduler, name, nothing)
    monkeypatch.setattr(scheduler, "_create_scheduler", lambda: object())
    for name in ("schedule_periodic_scores_evaluation", "schedule_periodic_elo_calculation",
                 "schedule_periodic_arena_score_calculation", "schedule_periodic_participation_check",
                 "schedule_periodic_forecast_scoring"):
        monkeypatch.setattr(scheduler, name, lambda name=name: record(name))
    return scheduler, calls


def test_start_and_restart_schedule_the_job_right_after_elo(quiet_scheduler):
    scheduler, calls = quiet_scheduler
    for entry_point in (scheduler.start, scheduler._attempt_restart):
        calls.clear()
        asyncio.run(entry_point())
        assert calls[calls.index("schedule_periodic_elo_calculation") + 1] == (
            "schedule_periodic_arena_score_calculation"
        )
