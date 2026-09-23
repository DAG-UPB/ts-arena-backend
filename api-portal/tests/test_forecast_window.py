"""backend-87 / backend-97: the forecast window each series was issued, enforced twice.

1. an upload's timestamps must match the window its series was actually issued
   (`ForecastService._expected_forecast_timestamps`);
2. a point outside that window is not scored even if it is already stored
   (`series_forecast_windows`).

Both are per series: `rounds.start_time` is the global max context edge, and a lagging
series' honest window starts earlier than it.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.services.forecast_service import ForecastService
from app.services.score_evaluation_service import series_forecast_windows

UTC = timezone.utc



# --- 1. upload validation is per series, not per round ------------------------------------

def _expected(edge, frequency=timedelta(minutes=15), count=4):
    return ForecastService._expected_forecast_timestamps(edge, frequency, count)


def test_expected_timestamps_start_one_step_after_the_context_edge():
    edge = datetime(2026, 9, 11, 19, 45, tzinfo=UTC)
    assert _expected(edge) == [
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 15, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 30, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 45, tzinfo=UTC),
    ]


def test_naive_context_edge_is_read_as_utc():
    naive = datetime(2026, 9, 11, 19, 45)
    aware = datetime(2026, 9, 11, 19, 45, tzinfo=UTC)
    assert _expected(naive) == _expected(aware)


@pytest.mark.parametrize(
    "edge,frequency,count",
    [(None, timedelta(minutes=15), 4), (datetime(2026, 9, 11, tzinfo=UTC), None, 4),
     (datetime(2026, 9, 11, tzinfo=UTC), timedelta(minutes=15), None)],
)
def test_window_that_cannot_be_derived_skips_the_check(edge, frequency, count):
    """No context stored, or no frequency/horizon on the round: return None so the caller
    skips validation rather than rejecting an upload on incomplete information."""
    assert ForecastService._expected_forecast_timestamps(edge, frequency, count) is None


def test_lagging_series_gets_its_own_window():
    """Round 12246 had a 15-minute spread between its series' context edges, while
    `start_time` is the global max. Validating the lagging series against `start_time` would
    reject an honest upload, so each series is validated against its own edge."""
    leading = datetime(2026, 9, 5, 13, 30, tzinfo=UTC)
    lagging = datetime(2026, 9, 5, 13, 15, tzinfo=UTC)
    round_start_time = leading + timedelta(minutes=15)  # 13:45, what start_time would be

    lagging_window = _expected(lagging)
    assert lagging_window[0] == datetime(2026, 9, 5, 13, 30, tzinfo=UTC)
    assert lagging_window[0] < round_start_time  # strict start_time would have rejected it


# --- 2. scoring is bounded by the same per-series window ----------------------------------

def test_scoring_window_spans_first_step_to_horizon_end():
    edges = {68: datetime(2026, 9, 11, 19, 45, tzinfo=UTC)}
    windows = series_forecast_windows(edges, timedelta(minutes=15), timedelta(days=1))
    assert windows[68] == (
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 12, 19, 45, tzinfo=UTC),
    )


def test_backdated_point_falls_outside_the_scoring_window():
    """The exploit shape: a point placed over an already-published stretch. Once the window
    moves past the publication edge, such a point is outside it and is not evaluated."""
    edge = datetime(2026, 9, 11, 19, 45, tzinfo=UTC)
    windows = series_forecast_windows({68: edge}, timedelta(minutes=15), timedelta(days=1))
    lo, hi = windows[68]

    backdated = datetime(2026, 9, 11, 15, 45, tzinfo=UTC)  # inside published data
    honest = datetime(2026, 9, 12, 10, 0, tzinfo=UTC)

    assert not (lo <= backdated <= hi)
    assert lo <= honest <= hi


def test_series_without_a_context_edge_is_omitted():
    edges = {68: None, 69: datetime(2026, 9, 11, 19, 45, tzinfo=UTC)}
    windows = series_forecast_windows(edges, timedelta(minutes=15), timedelta(days=1))
    assert 68 not in windows
    assert 69 in windows


@pytest.mark.parametrize("frequency,horizon", [(None, timedelta(days=1)), (timedelta(minutes=15), None)])
def test_no_window_without_frequency_and_horizon(frequency, horizon):
    edges = {68: datetime(2026, 9, 11, 19, 45, tzinfo=UTC)}
    assert series_forecast_windows(edges, frequency, horizon) == {}
