"""SCD2 upsert passes constant bounds, so old chunks drop out of the plan."""

import asyncio
import datetime as dt

from src.repositories.time_series_scd2_repository import (
    MAX_PUBLISH_LEAD,
    TimeSeriesDataSCD2Repository,
)

UTC = dt.timezone.utc


class _Result:
    def fetchone(self):
        return (2, 1)


class _Session:
    def __init__(self):
        self.calls = []

    async def execute(self, query, params):
        self.calls.append((str(query), params))
        return _Result()

    async def commit(self):
        pass

    async def rollback(self):
        pass


def _upsert(points):
    session = _Session()
    stats = asyncio.run(TimeSeriesDataSCD2Repository(session).upsert_data_points(7, points))
    return session.calls[0], stats


def test_update_is_bounded_by_series_ts_range_and_valid_from():
    (sql, params), stats = _upsert([
        {"ts": "2026-11-02T12:00:00+00:00", "value": 1.0},
        {"ts": "2026-11-01T12:00:00Z", "value": 2.0},
        {"ts": "2026-11-02T13:00:00+01:00", "value": 3.0},
    ])
    lo = dt.datetime(2026, 11, 1, 12, tzinfo=UTC)
    assert params["series_id"] == 7
    assert params["min_ts"] == lo
    assert params["max_ts"] == dt.datetime(2026, 11, 2, 12, tzinfo=UTC)
    assert params["min_valid_from"] == lo - MAX_PUBLISH_LEAD
    assert "t.valid_from >= :min_valid_from" in sql
    assert stats == {"inserted": 2, "updated": 1, "unchanged": 0}


def test_naive_timestamps_are_read_as_utc():
    (_, params), _ = _upsert([{"ts": dt.datetime(2026, 11, 2, 12), "value": 1.0}])
    assert params["min_ts"] == dt.datetime(2026, 11, 2, 12, tzinfo=UTC)
    assert '"ts": "2026-11-02T12:00:00+00:00"' in params["data"]
