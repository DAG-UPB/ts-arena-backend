from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from fastapi import FastAPI

if TYPE_CHECKING:
    from app.scheduler.scheduler import ChallengeScheduler

# Global scheduler reference for jobs
_scheduler_instance: Optional["ChallengeScheduler"] = None


def set_scheduler(scheduler: Optional["ChallengeScheduler"]) -> None:
    """Set the global scheduler instance."""
    global _scheduler_instance
    _scheduler_instance = scheduler


def get_scheduler() -> Optional["ChallengeScheduler"]:
    """Get the global scheduler instance."""
    return _scheduler_instance


def get_services(app: FastAPI) -> dict[str, Any]:
    """Central access point for services/repos/logger from app.state."""
    return {
        "challenge_service": getattr(app.state, "challenge_service", None),
        "challenge_repo": getattr(app.state, "challenge_repo", None),
        "logger": getattr(app.state, "logger", None),
    }
