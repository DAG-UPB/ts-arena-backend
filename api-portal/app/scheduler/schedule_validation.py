"""Validation of the recurring challenge schedule config.

A round's registration window is ``[fire_time, fire_time + registration_duration)``.
Two windows that overlap starve each other: the model uploader works through rounds
strictly sequentially, so the later challenge only starts once the earlier one has
finished its whole model roster, and by then a short window has largely elapsed —
the tail of the roster never submits.

Widening the window is not a way out. A registration window may never exceed the
series frequency, so 15-minute-frequency challenges are capped at 15 minutes and the
schedule itself has to keep their windows disjoint.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

# Mirrors the fallback in ChallengeService.sync_definition_from_yaml.
DEFAULT_REGISTRATION_DURATION = timedelta(hours=1)

# Expand over more than a week so weekly and monthly crons are compared fairly
# against the daily ones rather than only where they happen to line up.
DEFAULT_HORIZON = timedelta(days=8)


def parse_duration(duration_str: str) -> timedelta:
    """Parse a ``"<n> <unit>"`` duration as used in challenge_schedules.yaml."""
    parts = duration_str.split()
    value = int(parts[0])
    unit = parts[1].lower()
    if "minute" in unit:
        return timedelta(minutes=value)
    if "hour" in unit:
        return timedelta(hours=value)
    if "day" in unit:
        return timedelta(days=value)
    raise ValueError(f"Unsupported duration unit: {unit}")


@dataclass(frozen=True)
class RegistrationWindow:
    """One concrete registration window produced by a schedule's cron."""

    schedule_id: str
    start: datetime
    end: datetime


def expand_windows(
    schedules: Iterable[Dict[str, Any]],
    *,
    reference_time: Optional[datetime] = None,
    horizon: timedelta = DEFAULT_HORIZON,
) -> List[RegistrationWindow]:
    """Expand every schedule's cron into the windows it opens within the horizon.

    Entries without an ``id`` or ``cron`` are skipped — ``load_recurring_schedules``
    rejects those separately, and this function should not be the thing that fails.
    """
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    limit = reference_time + horizon
    windows: List[RegistrationWindow] = []

    for schedule in schedules:
        schedule_id = schedule.get("id")
        cron_expression = schedule.get("cron")
        if not schedule_id or not cron_expression:
            continue

        params = schedule.get("params") or {}
        raw_duration = params.get("registration_duration")
        duration = (
            parse_duration(raw_duration) if raw_duration else DEFAULT_REGISTRATION_DURATION
        )

        trigger = CronTrigger.from_crontab(
            cron_expression, timezone=timezone.utc, start_time=reference_time
        )
        while (fire_time := trigger.next()) is not None and fire_time < limit:
            windows.append(
                RegistrationWindow(
                    schedule_id=schedule_id,
                    start=fire_time,
                    end=fire_time + duration,
                )
            )

    return windows


def find_overlaps(
    schedules: Iterable[Dict[str, Any]],
    *,
    reference_time: Optional[datetime] = None,
    horizon: timedelta = DEFAULT_HORIZON,
) -> List[Tuple[str, str, RegistrationWindow, RegistrationWindow]]:
    """Find schedule pairs whose registration windows overlap.

    Windows are half-open, so windows that merely abut (one ends exactly as the next
    begins) do not count as overlapping. A schedule can also collide with *itself* when
    its registration duration outlasts its own cron period; that is reported too.

    Returns one entry per offending pair — not per repetition — as
    ``(schedule_a, schedule_b, window_a, window_b)`` with the earlier window first.
    """
    windows = sorted(
        expand_windows(schedules, reference_time=reference_time, horizon=horizon),
        key=lambda w: (w.start, w.end, w.schedule_id),
    )

    overlaps: List[Tuple[str, str, RegistrationWindow, RegistrationWindow]] = []
    reported: set[Tuple[str, str]] = set()
    active: List[RegistrationWindow] = []

    for window in windows:
        # Drop windows that closed before this one opened; the rest still overlap it.
        active = [w for w in active if w.end > window.start]
        for earlier in active:
            pair = tuple(sorted((earlier.schedule_id, window.schedule_id)))
            if pair in reported:
                continue
            reported.add(pair)
            overlaps.append(
                (earlier.schedule_id, window.schedule_id, earlier, window)
            )
        active.append(window)

    return overlaps


def describe_overlaps(
    overlaps: Iterable[Tuple[str, str, RegistrationWindow, RegistrationWindow]],
) -> List[str]:
    """Render overlaps as one human-readable line each, for logging."""
    messages = []
    for schedule_a, schedule_b, window_a, window_b in overlaps:
        if schedule_a == schedule_b:
            messages.append(
                f"'{schedule_a}' overlaps itself: its registration window "
                f"({window_a.start:%H:%M}-{window_a.end:%H:%M} UTC) is still open when "
                f"its next round starts at {window_b.start:%H:%M} UTC"
            )
        else:
            messages.append(
                f"'{schedule_a}' ({window_a.start:%H:%M}-{window_a.end:%H:%M} UTC) overlaps "
                f"'{schedule_b}' ({window_b.start:%H:%M}-{window_b.end:%H:%M} UTC)"
            )
    return messages
