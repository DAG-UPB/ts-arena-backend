"""Bright Sky forecast plugin: only forecast records, one request per station."""

import datetime as dt
from pathlib import Path

import pytest

from src.plugins.base_plugin import TimeSeriesDefinition
from src.plugins.data_sources.brightsky_plugin import BrightSkyForecastPlugin, forecast_points
from src.scheduler.plugin_loader import PluginLoader

UTC = dt.timezone.utc
SOURCES = Path(__file__).resolve().parents[1] / "src/plugins/configs/sources.yaml"


def _payload(records, forecast_ids=(1,), observation_ids=(2,)):
    sources = [{"id": i, "observation_type": "forecast"} for i in forecast_ids]
    sources += [{"id": i, "observation_type": "current"} for i in observation_ids]
    return {"weather": records, "sources": sources}


def _series(uid, station, field):
    return TimeSeriesDefinition(
        unique_id=uid, name=uid, description="", frequency="1 hour", unit="",
        domain="weather", category="forecast",
        extract_filter={"wmo_station_id": station, "field": field},
    )


def test_forecast_points_drops_observations_and_missing_values():
    payload = _payload([
        {"timestamp": "2026-11-02T12:00:00+00:00", "source_id": 2, "temperature": 9.0, "solar": 0.1},
        {"timestamp": "2026-11-02T13:00:00+00:00", "source_id": 1, "temperature": 8.5, "solar": None},
        {"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1, "temperature": 8.0, "solar": 0.05},
    ])
    points = forecast_points(payload, ["temperature", "solar"])
    assert points["temperature"] == [
        {"ts": "2026-11-02T13:00:00+00:00", "value": 8.5},
        {"ts": "2026-11-02T14:00:00+00:00", "value": 8.0},
    ]
    assert points["solar"] == [{"ts": "2026-11-02T14:00:00+00:00", "value": 0.05}]


@pytest.fixture
def plugin():
    series = [
        _series("t_a", "10147", "temperature"),
        _series("w_a", "10147", "wind_speed"),
        _series("t_b", "10870", "temperature"),
    ]
    return BrightSkyForecastPlugin("g", {"horizon_hours": 48}, series, "30 minutes")


def test_one_request_per_station_over_the_horizon(plugin):
    calls = []

    def fake_fetch(station, start, end):
        calls.append((station, start, end))
        return _payload([{"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1,
                          "temperature": float(station[-1]), "wind_speed": 12.0}])

    plugin._fetch_station = fake_fetch
    result = plugin._fetch_all(dt.datetime(2026, 11, 2, 13, 33, 8, tzinfo=UTC))

    start = dt.datetime(2026, 11, 2, 13, tzinfo=UTC)
    assert calls == [
        ("10147", start, start + dt.timedelta(hours=48)),
        ("10870", start, start + dt.timedelta(hours=48)),
    ]
    assert result["t_a"][0]["value"] == 7.0
    assert result["w_a"][0]["value"] == 12.0
    assert result["t_b"][0]["value"] == 0.0


def test_failed_station_is_skipped(plugin):
    def fake_fetch(station, start, end):
        if station == "10147":
            raise ConnectionError("boom")
        return _payload([{"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1, "temperature": 3.0}])

    plugin._fetch_station = fake_fetch
    result = plugin._fetch_all(dt.datetime(2026, 11, 2, 13, tzinfo=UTC))
    assert set(result) == {"t_b"}


def test_all_stations_failing_raises_for_the_retry(plugin):
    def fake_fetch(station, start, end):
        raise ConnectionError("boom")

    plugin._fetch_station = fake_fetch
    with pytest.raises(RuntimeError):
        plugin._fetch_all(dt.datetime(2026, 11, 2, 13, tzinfo=UTC))


def test_shipped_config_loads_the_weather_group():
    plugins = PluginLoader(str(SOURCES)).load_multi_series_plugins()
    group = plugins["brightsky-dwd-mosmix-forecast"]
    assert isinstance(group, BrightSkyForecastPlugin)
    series = group.get_series_definitions()
    assert len(series) == 36
    assert len({s.unique_id for s in series}) == 36
    for s in series:
        assert s.unique_id.startswith("fc_dwd_mosmix_")
        assert s.domain == "weather"
        assert s.frequency == "1 hour"
        assert s.extract_filter["field"] in {"temperature", "wind_speed", "solar"}
        assert isinstance(s.extract_filter["wmo_station_id"], str)
        assert "Deutscher Wetterdienst" in s.description
