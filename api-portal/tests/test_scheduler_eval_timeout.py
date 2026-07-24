"""Regression tests for the eval-job hang guard (backend-48).

The periodic scores-evaluation job runs with ``max_running_jobs=1``. If a single run
hangs (a stuck DB session/lock), it holds the only slot forever and every later fire
queues up until the backlog starves all job acquisition — the silent stall that killed
round creation for days in the 2026-07 incident.

The fix wraps the job body in ``asyncio.timeout`` so a hung run is cancelled and the
coroutine *returns* instead of hanging. In real APScheduler a returned coroutine frees
the ``max_running_jobs=1`` slot, so "the coroutine returns within the bound instead of
hanging" is exactly the proof that the slot is freed and the next fire can run.

These are pure unit tests: ``SessionLocal`` and ``ScoreEvaluationService`` are faked, so
no database is touched. Async coroutines are driven with ``asyncio.run`` to match the
plain-pytest convention used elsewhere in this suite (no pytest-asyncio).
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

import app.scheduler.jobs as jobs


# --- Fakes ----------------------------------------------------------------

class _FakeSession:
    """Async-context-manager session whose ``execute`` (used only by the
    SET statement_timeout / lock_timeout guards) is a no-op."""

    async def execute(self, *args, **kwargs):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _fake_session_local_factory():
    """Return a callable usable as ``SessionLocal()`` -> async context manager."""
    def _factory():
        return _FakeSession()
    return _factory


class _FakeScoreService:
    """Configurable stand-in for ScoreEvaluationService.

    ``hang_seconds`` > 0 makes ``evaluate_challenge_scores`` sleep (simulating a stuck
    run); ``hang_on_ids`` makes ``get_ids_needing_evaluation`` sleep instead.
    """

    ids = [1]
    hang_seconds = 0.0
    hang_on_ids = False
    evaluated_rounds: list[int] = []

    def __init__(self, session):
        self._session = session

    async def get_ids_needing_evaluation(self):
        if self.hang_on_ids and self.hang_seconds:
            await asyncio.sleep(self.hang_seconds)
        return list(type(self).ids)

    async def evaluate_challenge_scores(self, round_id):
        if not self.hang_on_ids and self.hang_seconds:
            await asyncio.sleep(self.hang_seconds)
        type(self).evaluated_rounds.append(round_id)
        return False


@pytest.fixture(autouse=True)
def _patch_job_dependencies(monkeypatch):
    """Fake out DB session + service, and shrink the hard timeout for fast tests."""
    monkeypatch.setattr(jobs, "SessionLocal", _fake_session_local_factory())
    monkeypatch.setattr(jobs, "ScoreEvaluationService", _FakeScoreService)
    monkeypatch.setattr(jobs, "EVAL_JOB_HARD_TIMEOUT_SECONDS", 0.2)
    # Reset shared state on the fake between tests.
    _FakeScoreService.ids = [1]
    _FakeScoreService.hang_seconds = 0.0
    _FakeScoreService.hang_on_ids = False
    _FakeScoreService.evaluated_rounds = []
    yield


# --- Tests ----------------------------------------------------------------

def test_hung_eval_run_times_out_and_returns_instead_of_hanging():
    """A run that hangs far longer than the bound is cancelled and the coroutine
    RETURNS (raises TimeoutError) well within the bound — i.e. the slot is freed."""
    _FakeScoreService.hang_seconds = 30.0  # >> 0.2s hard timeout

    raw_job = jobs.periodic_challenge_scores_evaluation_job.__wrapped__

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(raw_job())
    elapsed = time.monotonic() - start

    # Cancelled near the 0.2s bound, nowhere near the 30s fake hang.
    assert elapsed < 5.0, f"job did not cancel promptly (took {elapsed:.2f}s)"


def test_hang_in_ids_fetch_also_times_out():
    """The timeout also covers a hang in the initial ids-fetch session, not just
    the per-round loop."""
    _FakeScoreService.hang_on_ids = True
    _FakeScoreService.hang_seconds = 30.0

    raw_job = jobs.periodic_challenge_scores_evaluation_job.__wrapped__
    with pytest.raises(TimeoutError):
        asyncio.run(raw_job())


def test_timeout_is_logged_loudly_not_swallowed_silently(caplog):
    """The decorated job (as APScheduler calls it) logs the timeout at CRITICAL —
    the opposite of the silent stall — and returns without raising to the caller."""
    _FakeScoreService.hang_seconds = 30.0

    with caplog.at_level(logging.CRITICAL, logger="challenge-scheduler"):
        # The job_error_handler decorator catches and logs; it must not hang.
        asyncio.run(jobs.periodic_challenge_scores_evaluation_job())

    critical = [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert critical, "hung eval run produced no CRITICAL log line"
    assert any("hard timeout" in r.getMessage() for r in critical)


def test_next_fire_runs_after_a_hang():
    """After a hung run has been cancelled (slot freed), a subsequent fire runs to
    completion — proving the hang does not permanently wedge the job."""
    raw_job = jobs.periodic_challenge_scores_evaluation_job.__wrapped__

    # Fire 1: hangs -> cancelled.
    _FakeScoreService.hang_seconds = 30.0
    with pytest.raises(TimeoutError):
        asyncio.run(raw_job())

    # Fire 2: healthy -> completes and actually evaluates the round.
    _FakeScoreService.hang_seconds = 0.0
    _FakeScoreService.evaluated_rounds = []
    asyncio.run(raw_job())
    assert _FakeScoreService.evaluated_rounds == [1]


def test_healthy_run_applies_no_timeout_penalty():
    """A normal (non-hanging) run completes cleanly and evaluates all pending rounds."""
    _FakeScoreService.ids = [7, 8, 9]
    _FakeScoreService.hang_seconds = 0.0
    _FakeScoreService.evaluated_rounds = []

    raw_job = jobs.periodic_challenge_scores_evaluation_job.__wrapped__
    asyncio.run(raw_job())
    assert _FakeScoreService.evaluated_rounds == [7, 8, 9]
