"""backend-87: the context edge, and the forecast window derived from it.

Three pieces, all of which have to hold together for the FPRP claim ("committed before the
ground truth exists") to be true for a source that publishes ahead of delivery:

1. the context may reach the publication edge, but must not be set from a bucket that is
   still filling (`in_progress_bucket_start`);
2. an upload's timestamps must match the window its series was actually issued
   (`ForecastService._expected_forecast_timestamps`);
3. a point outside that window is not scored even if it is already stored
   (`series_forecast_windows`).

The defect these cover: the continuous aggregates' positive `end_offset` capped `max(ts)` at
~now, so `start_time = max_ts + frequency` opened inside data SMARD had already published,
and neither upload nor scoring bounded the window. Definitions 1 and 4 were a lookup rather
than a forecast from 2026-04-28 onward.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.database.data_portal.time_series_repository import (
    RESOLUTION_TO_BUCKET_LITERAL,
    RESOLUTION_TO_VIEW,
    AggregateSource,
    TimeSeriesRepository,
    in_progress_bucket_start,
)
from app.services.forecast_service import ForecastService
from app.services.score_evaluation_service import series_forecast_windows

UTC = timezone.utc


# --- 1. the still-filling bucket, and only that one --------------------------------------

@pytest.mark.parametrize(
    "resolution,now,expected",
    [
        ("15min", datetime(2026, 9, 11, 10, 7, 30, tzinfo=UTC), datetime(2026, 9, 11, 10, 0, tzinfo=UTC)),
        ("15min", datetime(2026, 9, 11, 10, 44, 59, tzinfo=UTC), datetime(2026, 9, 11, 10, 30, tzinfo=UTC)),
        ("1h", datetime(2026, 9, 11, 10, 7, 30, tzinfo=UTC), datetime(2026, 9, 11, 10, 0, tzinfo=UTC)),
        ("1d", datetime(2026, 9, 11, 10, 7, 30, tzinfo=UTC), datetime(2026, 9, 11, 0, 0, tzinfo=UTC)),
    ],
)
def test_in_progress_bucket_is_the_one_containing_now(resolution, now, expected):
    assert in_progress_bucket_start(resolution, now) == expected


def test_bucket_boundary_belongs_to_the_new_bucket():
    # Exactly on the boundary the new bucket has just opened and holds no complete data yet.
    now = datetime(2026, 9, 11, 10, 0, tzinfo=UTC)
    assert in_progress_bucket_start("1h", now) == now


def test_raw_has_no_bucket_to_trim():
    # "raw" is not bucketed, so there is no filling bucket and nothing to exclude.
    assert in_progress_bucket_start("raw", datetime(2026, 9, 11, 10, 7, tzinfo=UTC)) is None


def test_trim_excludes_only_the_filling_bucket_not_the_future():
    """The whole point of backend-87: future buckets must survive the guard.

    `end_offset` withheld the filling bucket by withholding *everything* newer than the
    watermark, which is what capped the context at ~now for day-ahead prices. Excluding a
    single bucket start leaves both the past and the future intact.
    """
    now = datetime(2026, 9, 11, 7, 36, tzinfo=UTC)
    filling = in_progress_bucket_start("1h", now)

    past = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)
    future = datetime(2026, 9, 11, 19, 0, tzinfo=UTC)  # published day-ahead, 12 h out

    assert past != filling
    assert future != filling
    assert filling == datetime(2026, 9, 11, 7, 0, tzinfo=UTC)


def test_short_past_buckets_are_not_excluded():
    """Measured on dev: the only partial def-1 buckets in 14 days were historical ingest
    gaps (2026-08-28 07:00 at 1/4, 2026-09-08 21:00 at 3/4), strictly in the past. A
    completeness test would drop them and punch holes in the middle of the context; the
    positional guard does not look at `sample_count` at all."""
    now = datetime(2026, 9, 11, 7, 36, tzinfo=UTC)
    gap_bucket = datetime(2026, 8, 28, 7, 0, tzinfo=UTC)
    assert in_progress_bucket_start("1h", now) != gap_bucket


# --- 2. upload validation is per series, not per round ------------------------------------

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


# --- 3. scoring is bounded by the same per-series window ----------------------------------

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


# --- 4. the union read's SQL literal ------------------------------------------------------

def test_timestamptz_literal_is_utc_normalised():
    naive = datetime(2026, 9, 11, 19, 45)
    assert TimeSeriesRepository._timestamptz_literal(naive) == (
        "timestamptz '2026-09-11T19:45:00+00:00'"
    )


def test_timestamptz_literal_converts_other_offsets():
    from datetime import timedelta as td
    berlin = datetime(2026, 9, 11, 21, 45, tzinfo=timezone(td(hours=2)))
    assert TimeSeriesRepository._timestamptz_literal(berlin) == (
        "timestamptz '2026-09-11T19:45:00+00:00'"
    )


@pytest.mark.parametrize("value", ["2026-09-11'; DROP TABLE forecasts.forecasts; --", 1757600000, None])
def test_timestamptz_literal_refuses_non_datetime(value):
    """The split point is spliced into the SQL rather than bound, so that chunk exclusion can
    happen at plan time. This type check is what keeps that safe — nothing but a datetime we
    computed can ever reach the statement."""
    with pytest.raises(TypeError):
        TimeSeriesRepository._timestamptz_literal(value)


def test_bucket_literals_match_the_aggregate_views():
    """The union's live branch must produce the same buckets the materialised branch holds, so
    these two maps have to stay aligned with the aggregates in init_db.sql."""
    assert RESOLUTION_TO_VIEW["15min"] == "data_portal.time_series_15min"
    assert RESOLUTION_TO_BUCKET_LITERAL["15min"] == "15 minutes"
    assert RESOLUTION_TO_VIEW["1h"] == "data_portal.time_series_1h"
    assert RESOLUTION_TO_BUCKET_LITERAL["1h"] == "1 hour"
    assert RESOLUTION_TO_VIEW["1d"] == "data_portal.time_series_1d"
    assert RESOLUTION_TO_BUCKET_LITERAL["1d"] == "1 day"
    assert set(RESOLUTION_TO_VIEW) == set(RESOLUTION_TO_BUCKET_LITERAL)


# --- 5. the read adapts to whether the aggregate already unions its own tail ---------------

@pytest.mark.asyncio
async def test_realtime_aggregate_is_read_directly_without_our_union():
    """If `materialized_only = false` is ever set — impossible on dev (backend-88), but it
    works on prod — the view reaches the publication edge by itself. Doing our union on top
    would be redundant, and computing the split via an unfiltered `max(ts)` would be actively
    expensive: it would aggregate the live branch for every series."""
    repo = TimeSeriesRepository.__new__(TimeSeriesRepository)
    captured = {}

    class _Session:
        async def execute(self, stmt, params=None):
            captured["sql"] = str(stmt)
            captured["params"] = params
            class _R:
                def fetchall(self):
                    return []
            return _R()

    repo.session = _Session()
    await repo._read_aggregate_with_live_tail(
        series_id=68, n=10, resolution="15min", before_time=None,
        source=AggregateSource(realtime=True, edge=None),
    )
    sql = captured["sql"]
    assert "data_portal.time_series_15min" in sql
    assert "UNION ALL" not in sql
    assert "time_bucket" not in sql


@pytest.mark.asyncio
async def test_materialized_only_aggregate_gets_the_union_split_at_the_watermark():
    repo = TimeSeriesRepository.__new__(TimeSeriesRepository)
    captured = {}

    class _Session:
        async def execute(self, stmt, params=None):
            captured["sql"] = str(stmt)
            class _R:
                def fetchall(self):
                    return []
            return _R()

    repo.session = _Session()
    edge = datetime(2026, 9, 6, 0, 15, tzinfo=UTC)
    await repo._read_aggregate_with_live_tail(
        series_id=68, n=10, resolution="15min", before_time=None,
        source=AggregateSource(realtime=False, edge=edge),
    )
    sql = captured["sql"]
    assert "UNION ALL" in sql
    assert "data_portal.time_series_15min" in sql
    assert "data_portal.time_series_data" in sql
    # Split spliced as a literal, both sides, so chunk exclusion happens at plan time.
    assert sql.count("timestamptz '2026-09-06T00:15:00+00:00'") == 2
    assert "time_bucket(interval '15 minutes'" in sql
