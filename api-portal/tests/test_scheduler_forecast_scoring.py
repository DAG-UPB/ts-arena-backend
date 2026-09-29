"""The forecast scoring job and its schedule, with ``SessionLocal`` and the service faked."""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

import app.scheduler.jobs as jobs
from app.scheduler.scheduler import ChallengeScheduler


class _FakeSession:
    async def execute(self, *args, **kwargs):
        return None

    async def commit(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeScoringService:
    tables = True
    ids = [1]
    fail_on: set = set()
    hang_seconds = 0.0
    scored: list = []
    captures = 0

    def __init__(self, session):
        self._session = session

    async def tables_exist(self):
        return type(self).tables

    async def capture_served_scales(self):
        type(self).captures += 1
        return 2

    async def rounds_to_score(self):
        return list(type(self).ids)

    async def score_round(self, round_id):
        if type(self).hang_seconds:
            await asyncio.sleep(type(self).hang_seconds)
        if round_id in type(self).fail_on:
            raise RuntimeError("boom")
        type(self).scored.append(round_id)
        return 3


@pytest.fixture(autouse=True)
def _patch_job_dependencies(monkeypatch):
    monkeypatch.setattr(jobs, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(jobs, "ForecastScoringService", _FakeScoringService)
    monkeypatch.setattr(jobs, "FORECAST_SCORING_JOB_HARD_TIMEOUT_SECONDS", 0.2)
    _FakeScoringService.tables = True
    _FakeScoringService.ids = [1]
    _FakeScoringService.fail_on = set()
    _FakeScoringService.hang_seconds = 0.0
    _FakeScoringService.scored = []
    _FakeScoringService.captures = 0
    yield


def _raw_job():
    return jobs.periodic_forecast_scoring_job.__wrapped__()


def test_does_nothing_before_its_tables_exist():
    _FakeScoringService.tables = False
    asyncio.run(_raw_job())
    assert _FakeScoringService.captures == 0
    assert _FakeScoringService.scored == []


def test_captures_then_scores_every_round_despite_a_failing_one():
    _FakeScoringService.ids = [7, 8, 9]
    _FakeScoringService.fail_on = {8}
    asyncio.run(_raw_job())
    assert _FakeScoringService.captures == 1
    assert _FakeScoringService.scored == [7, 9]


def test_a_spent_budget_leaves_the_remaining_rounds_to_the_next_run(monkeypatch, caplog):
    monkeypatch.setattr(jobs, "FORECAST_SCORING_RUN_BUDGET_SECONDS", 0)
    _FakeScoringService.ids = [7, 8, 9]
    with caplog.at_level(logging.INFO, logger="challenge-scheduler"):
        asyncio.run(_raw_job())
    assert _FakeScoringService.scored == []
    assert any("3 round(s) left for the next run" in r.getMessage() for r in caplog.records)


def test_a_hung_run_is_cancelled_within_the_bound():
    _FakeScoringService.hang_seconds = 30.0
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(_raw_job())
    assert time.monotonic() - start < 5.0


def test_a_timeout_is_logged_loudly(caplog):
    _FakeScoringService.hang_seconds = 30.0
    with caplog.at_level(logging.CRITICAL, logger="challenge-scheduler"):
        asyncio.run(jobs.periodic_forecast_scoring_job())
    assert any("hard timeout" in r.getMessage() for r in caplog.records if r.levelno >= logging.CRITICAL)


class _RecordingScheduler:
    def __init__(self, fail=False):
        self.fail = fail
        self.task = None
        self.schedule = None

    async def configure_task(self, func, **kwargs):
        if self.fail:
            raise RuntimeError("data store down")
        self.task = (func, kwargs)

    async def add_schedule(self, **kwargs):
        self.schedule = kwargs


def _scheduler(inner):
    scheduler = ChallengeScheduler.__new__(ChallengeScheduler)
    scheduler.scheduler = inner
    scheduler.logger = logging.getLogger("test-scheduler")
    return scheduler


def test_schedule_sits_between_the_arena_scorer_runs():
    inner = _RecordingScheduler()
    asyncio.run(_scheduler(inner).schedule_periodic_forecast_scoring())

    func, task_kwargs = inner.task
    assert func is jobs.periodic_forecast_scoring_job
    assert task_kwargs["max_running_jobs"] == 1
    assert inner.schedule["id"] == "periodic_forecast_scoring"
    assert "15,45" in repr(inner.schedule["trigger"])
    assert inner.schedule["misfire_grace_time"] == 300


def test_a_failing_schedule_does_not_raise():
    asyncio.run(_scheduler(_RecordingScheduler(fail=True)).schedule_periodic_forecast_scoring())
