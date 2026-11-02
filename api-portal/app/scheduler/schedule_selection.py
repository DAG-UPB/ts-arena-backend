"""Which schedules from the YAML create rounds in this environment.

``active: false`` stops a schedule from creating rounds while its definition and history
stay visible. ``environments: [dev, ...]`` limits a schedule to the listed values of
``ARENA_ENVIRONMENT``; schedules without it run everywhere.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

ROUND_TASK_ID = "app.scheduler.jobs:create_round_from_definition_job"


def in_environment(schedule_config: dict[str, Any], environment: Optional[str]) -> bool:
    environments = schedule_config.get("environments")
    if not environments:
        return True
    return environment is not None and environment in environments


def is_active(schedule_config: dict[str, Any]) -> bool:
    return schedule_config.get("active", True) is not False


def stale_round_schedule_ids(
    existing: Iterable[tuple[str, str]], wanted_ids: set[str]
) -> list[str]:
    """Ids of round-creating schedules in the data store that the YAML no longer wants.

    ``existing`` holds ``(schedule_id, task_id)`` pairs. Schedules of other tasks (scoring,
    rankings, ...) are never touched.
    """
    return sorted(
        schedule_id
        for schedule_id, task_id in existing
        if task_id == ROUND_TASK_ID and schedule_id not in wanted_ids
    )
