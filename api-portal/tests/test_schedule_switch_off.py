"""Schedules can be switched off and limited to an environment (backend-114).

The loader is driven against a fake APScheduler and a faked definition sync, so no
database is touched.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
import yaml

import app.database.connection as connection_module
import app.scheduler.jobs as jobs_module
import app.scheduler.scheduler as scheduler_module
import app.services.challenge_service as challenge_service_module
from app.scheduler.schedule_selection import (
    ROUND_TASK_ID,
    in_environment,
    is_active,
    stale_round_schedule_ids,
)
from app.scheduler.scheduler import ChallengeScheduler


DB_URL = "postgresql+asyncpg://test:test@localhost:5432/test"
PARAMS = {"frequency": "1 hour", "forecast_horizon": "1 day", "context_length": 10, "n_time_series": 1}


def test_round_task_id_matches_the_job():
    job = jobs_module.create_round_from_definition_job
    assert f"{job.__module__}:{job.__qualname__}" == ROUND_TASK_ID


def test_in_environment():
    assert in_environment({}, None)
    assert in_environment({}, "dev")
    assert not in_environment({"environments": ["dev"]}, None)
    assert in_environment({"environments": ["dev"]}, "dev")
    assert not in_environment({"environments": ["dev"]}, "prod")


def test_is_active():
    assert is_active({})
    assert is_active({"active": True})
    assert not is_active({"active": False})


def test_stale_ids_ignore_other_tasks():
    existing = [
        ("a", ROUND_TASK_ID),
        ("gone", ROUND_TASK_ID),
        ("periodic_forecast_scoring", "app.scheduler.jobs:periodic_forecast_scoring_job"),
    ]
    assert stale_round_schedule_ids(existing, {"a"}) == ["gone"]


class FakeAPScheduler:
    """``schedules`` maps id -> task id; ``args`` keeps the stored job arguments."""

    def __init__(self):
        self.schedules: dict[str, str] = {}
        self.args: dict[str, list] = {}

    async def add_schedule(self, func_or_task_id, trigger, id, args, **kwargs):
        self.schedules[id] = ROUND_TASK_ID
        self.args[id] = list(args)

    async def get_schedules(self):
        return [
            SimpleNamespace(id=i, task_id=t, args=tuple(self.args.get(i, ())))
            for i, t in self.schedules.items()
        ]

    async def remove_schedule(self, schedule_id):
        del self.schedules[schedule_id]


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def env(monkeypatch, tmp_path):
    """A scheduler with a fake data store; definitions get ids 1, 2, ... by schedule id."""
    synced: dict[str, int] = {}
    created: list[int] = []
    failing: set[str] = set()

    async def fake_sync(self, schedule_id, schedule_config):
        if schedule_id in failing:
            raise RuntimeError("database unavailable")
        return synced.setdefault(schedule_id, len(synced) + 1)

    async def fake_ensure_started(self):
        return None

    monkeypatch.setattr(connection_module, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(
        challenge_service_module.ChallengeService, "sync_definition_from_yaml", fake_sync
    )
    monkeypatch.setattr(ChallengeScheduler, "_ensure_started", fake_ensure_started)

    sched = ChallengeScheduler(database_url=DB_URL)
    sched.scheduler = FakeAPScheduler()

    async def fake_create_round(self, definition_id):
        created.append(definition_id)
        return SimpleNamespace(name="r", id=definition_id)

    monkeypatch.setattr(
        challenge_service_module.ChallengeService, "create_round_from_definition", fake_create_round
    )
    monkeypatch.setattr(jobs_module, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(jobs_module, "get_scheduler", lambda: sched)

    def load(schedules, environment=None):
        path = tmp_path / "schedules.yaml"
        path.write_text(yaml.safe_dump({"schedules": schedules}))
        monkeypatch.setattr(scheduler_module.Config, "ARENA_ENVIRONMENT", environment)
        asyncio.run(sched.load_recurring_schedules(str(path)))

    return SimpleNamespace(sched=sched, synced=synced, created=created, failing=failing, load=load)


def _schedule(schedule_id, **extra):
    return {"id": schedule_id, "cron": "0 8 * * *", "params": PARAMS, **extra}


def _run_job(env, definition_id):
    asyncio.run(jobs_module.create_round_from_definition_job(definition_id))


def test_removed_schedule_loses_its_job_but_keeps_its_definition(env):
    env.load([_schedule("a"), _schedule("b")])
    assert set(env.sched.scheduler.schedules) == {"a", "b"}

    env.load([_schedule("a")])

    assert set(env.sched.scheduler.schedules) == {"a"}
    assert "b" in env.synced
    _run_job(env, env.synced["b"])
    assert env.created == []


def test_inactive_schedule_loses_its_job_and_still_syncs(env):
    env.load([_schedule("a"), _schedule("b")])

    env.load([_schedule("a"), _schedule("b", active=False)])

    assert set(env.sched.scheduler.schedules) == {"a"}
    _run_job(env, env.synced["b"])
    _run_job(env, env.synced["a"])
    assert env.created == [env.synced["a"]]


def test_reactivated_schedule_resumes(env):
    env.load([_schedule("a", active=False)])
    assert env.sched.scheduler.schedules == {}

    env.load([_schedule("a")])

    assert set(env.sched.scheduler.schedules) == {"a"}
    _run_job(env, env.synced["a"])
    assert env.created == [env.synced["a"]]


def test_dev_only_schedule_is_skipped_without_environment(env):
    env.load([_schedule("a"), _schedule("cov", environments=["dev"])])

    assert set(env.sched.scheduler.schedules) == {"a"}
    assert "cov" not in env.synced


def test_dev_only_schedule_runs_on_dev(env):
    env.load([_schedule("a"), _schedule("cov", environments=["dev"])], environment="dev")

    assert set(env.sched.scheduler.schedules) == {"a", "cov"}


def test_other_tasks_are_never_removed(env):
    env.sched.scheduler.schedules["periodic_forecast_scoring"] = "app.scheduler.jobs:periodic_forecast_scoring_job"

    env.load([_schedule("a")])

    assert "periodic_forecast_scoring" in env.sched.scheduler.schedules


def test_missing_config_does_not_block_rounds(env, tmp_path):
    asyncio.run(env.sched.load_recurring_schedules(str(tmp_path / "missing.yaml")))

    _run_job(env, 7)

    assert env.created == [7]


def test_failed_sync_keeps_the_stored_schedule_running(env):
    """A transient DB error during a reload must not stop a schedule that is still wanted."""
    env.load([_schedule("a"), _schedule("b")])
    env.failing.add("b")

    env.load([_schedule("a"), _schedule("b")])

    assert set(env.sched.scheduler.schedules) == {"a", "b"}
    _run_job(env, env.synced["b"])
    assert env.created == [env.synced["b"]]


def test_dev_only_schedule_in_store_is_removed_without_environment(env):
    env.load([_schedule("a"), _schedule("cov", environments=["dev"])], environment="dev")

    env.load([_schedule("a"), _schedule("cov", environments=["dev"])])

    assert set(env.sched.scheduler.schedules) == {"a"}
    _run_job(env, env.synced["cov"])
    assert env.created == []


def test_shipped_config_keeps_every_schedule_with_environment_unset(env):
    """Prod parity: the shipped YAML removes nothing and blocks no definition."""
    shipped = yaml.safe_load(
        (scheduler_module.Path(__file__).parent.parent / "app/configs/challenge_schedules.yaml").read_text()
    )["schedules"]

    env.load(shipped)

    assert set(env.sched.scheduler.schedules) == {s["id"] for s in shipped}
    for definition_id in env.synced.values():
        _run_job(env, definition_id)
    assert sorted(env.created) == sorted(env.synced.values())


def test_guard_fails_open_when_config_never_loads(env):
    allowed = asyncio.run(env.sched.round_definition_allowed(1, timeout=0.01))

    assert allowed is True


def test_run_on_startup_creates_a_round(env):
    env.load([_schedule("a", run_on_startup=True)])

    assert env.created == [env.synced["a"]]
