"""Registration windows must never overlap.

The uploader processes rounds sequentially, so two challenges whose registration
windows overlap starve each other; a 15-minute-frequency challenge cannot compensate
by widening its window. The last test here guards the shipped config itself.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from app.scheduler.schedule_validation import (
    describe_overlaps,
    expand_windows,
    find_overlaps,
    parse_duration,
)

REFERENCE = datetime(2026, 1, 1, tzinfo=timezone.utc)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "app/configs/challenge_schedules.yaml"


def schedule(schedule_id: str, cron: str, registration_duration: str = "15 minutes"):
    return {
        "id": schedule_id,
        "cron": cron,
        "params": {"registration_duration": registration_duration},
    }


def overlapping_pairs(schedules):
    return {
        tuple(sorted((a, b)))
        for a, b, _, _ in find_overlaps(schedules, reference_time=REFERENCE)
    }


@pytest.mark.parametrize(
    "text,expected",
    [
        ("15 minutes", timedelta(minutes=15)),
        ("1 hour", timedelta(hours=1)),
        ("3 days", timedelta(days=3)),
    ],
)
def test_parse_duration(text, expected):
    assert parse_duration(text) == expected


def test_parse_duration_rejects_unknown_unit():
    with pytest.raises(ValueError):
        parse_duration("5 fortnights")


def test_identical_windows_overlap():
    # The def 3 / def 9 collision that started this: same cron, same duration.
    schedules = [schedule("a", "0 20 * * *"), schedule("b", "0 20 * * *")]
    assert overlapping_pairs(schedules) == {("a", "b")}


def test_nested_window_overlaps():
    # A 15-minute window sitting inside somebody else's hour.
    schedules = [
        schedule("outer", "0 9 * * *", "1 hour"),
        schedule("inner", "30 9 * * *", "15 minutes"),
    ]
    assert overlapping_pairs(schedules) == {("inner", "outer")}


def test_abutting_windows_do_not_overlap():
    # Windows are half-open: ending exactly as the next opens is legal.
    schedules = [
        schedule("first", "0 9 * * *", "1 hour"),
        schedule("second", "0 10 * * *", "1 hour"),
    ]
    assert overlapping_pairs(schedules) == set()


def test_separated_windows_do_not_overlap():
    schedules = [
        schedule("first", "0 20 * * *"),
        schedule("second", "30 20 * * *"),
    ]
    assert overlapping_pairs(schedules) == set()


def test_schedule_overlapping_itself_is_reported():
    # Hourly cron with a two-hour registration duration: each round is still open
    # when the next one starts.
    schedules = [schedule("greedy", "0 * * * *", "2 hours")]
    assert overlapping_pairs(schedules) == {("greedy", "greedy")}


def test_each_offending_pair_reported_once_not_per_day():
    schedules = [schedule("a", "0 20 * * *"), schedule("b", "0 20 * * *")]
    overlaps = find_overlaps(schedules, reference_time=REFERENCE)
    assert len(overlaps) == 1
    assert len(describe_overlaps(overlaps)) == 1


def test_weekly_and_daily_crons_are_compared():
    # A weekly cron only collides on one day of the week; the horizon must be long
    # enough to see it.
    schedules = [
        schedule("daily", "0 20 * * *"),
        schedule("weekly", "5 20 * * 3"),
    ]
    assert overlapping_pairs(schedules) == {("daily", "weekly")}


def test_entries_without_cron_are_skipped():
    schedules = [{"id": "no-cron", "params": {}}, schedule("ok", "0 20 * * *")]
    windows = expand_windows(schedules, reference_time=REFERENCE)
    assert {w.schedule_id for w in windows} == {"ok"}


def test_missing_registration_duration_falls_back_to_one_hour():
    schedules = [{"id": "bare", "cron": "0 20 * * *", "params": {}}]
    window = expand_windows(schedules, reference_time=REFERENCE)[0]
    assert window.end - window.start == timedelta(hours=1)


def test_shipped_config_has_no_overlapping_registration_windows():
    schedules = yaml.safe_load(CONFIG_PATH.read_text())["schedules"]
    overlaps = find_overlaps(schedules, reference_time=REFERENCE)
    assert not overlaps, "; ".join(describe_overlaps(overlaps))
