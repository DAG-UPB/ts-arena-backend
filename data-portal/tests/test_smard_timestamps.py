"""SMARD timestamps are UTC instants — pin that, and pin the DST behaviour.

The defect this guards against (backend-90): epoch milliseconds were converted with
`pd.to_datetime(..., unit="ms")`, producing the UTC *wall clock*, and that wall clock was
then re-labelled as Europe/Berlin with `tz_localize`. Every point was stored one Berlin
UTC offset early — 2 h in CEST, 1 h in CET — and the spring-forward hour was silently
dropped by `nonexistent='shift_forward'`.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.plugins.data_sources.smard_plugin import SmardDataPortal

UTC = dt.timezone.utc


def _epoch_ms(iso_utc: str) -> int:
    return int(dt.datetime.fromisoformat(iso_utc).timestamp() * 1000)


def _run(portal, points, **kwargs):
    """Drive get_processed_history over a fixed series, no network."""
    portal.download_historical_data = lambda **_: {"series": points}
    return portal.get_processed_history(**kwargs)


@pytest.fixture
def portal():
    return SmardDataPortal()


@pytest.mark.parametrize(
    "iso_utc, expected_berlin",
    [
        # last quarter-hour of a CEST delivery day - the case in the bug report
        ("2026-09-12T21:45:00+00:00", "2026-09-12T23:45:00+02:00"),
        # first quarter-hour of the same delivery day
        ("2026-09-11T22:00:00+00:00", "2026-09-12T00:00:00+02:00"),
        # winter: the offset is 1 h, not 2
        ("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+01:00"),
    ],
)
def test_instant_is_preserved(portal, iso_utc, expected_berlin):
    ms = _epoch_ms(iso_utc)
    out = _run(portal, [[ms, 42.0]], start_date=dt.datetime(2026, 1, 1, tzinfo=UTC))

    assert len(out) == 1
    assert out[0]["ts"] == expected_berlin
    # the rendering is cosmetic; the instant must be exactly what SMARD sent
    assert dt.datetime.fromisoformat(out[0]["ts"]) == dt.datetime.fromisoformat(iso_utc)


def test_spring_forward_hour_is_not_dropped(portal):
    """2026-03-29 04:00-04:45 CEST used to collide onto 03:00 and vanish."""
    reals = [
        "2026-03-29T01:45:00+00:00",
        "2026-03-29T02:00:00+00:00",
        "2026-03-29T02:15:00+00:00",
        "2026-03-29T02:30:00+00:00",
        "2026-03-29T02:45:00+00:00",
        "2026-03-29T03:00:00+00:00",
    ]
    points = [[_epoch_ms(iso), float(i)] for i, iso in enumerate(reals)]
    out = _run(portal, points, start_date=dt.datetime(2026, 3, 1, tzinfo=UTC))

    got = [dt.datetime.fromisoformat(e["ts"]) for e in out]
    assert got == [dt.datetime.fromisoformat(iso) for iso in reals]
    assert len(set(got)) == len(reals), "no two points may collapse onto one instant"


def test_fall_back_hour_keeps_both_occurrences(portal):
    """02:00 CEST and 02:00 CET are distinct instants and must stay distinct."""
    reals = ["2025-10-26T00:00:00+00:00", "2025-10-26T01:00:00+00:00"]
    points = [[_epoch_ms(iso), float(i)] for i, iso in enumerate(reals)]
    out = _run(portal, points, start_date=dt.datetime(2025, 10, 1, tzinfo=UTC))

    got = [dt.datetime.fromisoformat(e["ts"]) for e in out]
    assert got == [dt.datetime.fromisoformat(iso) for iso in reals]
    berlin = [g.astimezone(ZoneInfo("Europe/Berlin")).strftime("%H:%M") for g in got]
    assert berlin == ["02:00", "02:00"], "both occurrences render as 02:00 local"


def test_start_date_filter_is_compared_as_an_instant(portal):
    """A naive start_date means UTC; it must not be re-read as Berlin local."""
    points = [
        [_epoch_ms("2026-09-12T21:00:00+00:00"), 1.0],
        [_epoch_ms("2026-09-12T21:45:00+00:00"), 2.0],
    ]
    out = _run(portal, points, start_date=dt.datetime(2026, 9, 12, 21, 30))

    assert [e["value"] for e in out] == [2.0]


def test_none_values_are_dropped(portal):
    points = [
        [_epoch_ms("2026-09-12T21:00:00+00:00"), None],
        [_epoch_ms("2026-09-12T21:45:00+00:00"), 2.0],
    ]
    out = _run(portal, points, start_date=dt.datetime(2026, 1, 1, tzinfo=UTC))

    assert [e["value"] for e in out] == [2.0]
