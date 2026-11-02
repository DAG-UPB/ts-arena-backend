"""Bright Sky plugin: observations and forecasts kept apart, one request per station."""

import datetime as dt
from pathlib import Path

import pytest

from src.plugins.base_plugin import TimeSeriesDefinition
from src.plugins.data_sources.brightsky_plugin import BrightSkyPlugin, station_points
from src.scheduler.plugin_loader import PluginLoader

UTC = dt.timezone.utc
SOURCES = Path(__file__).resolve().parents[1] / "src/plugins/configs/sources.yaml"
NOW = dt.datetime(2026, 11, 2, 13, 33, 8, tzinfo=UTC)
HOUR = dt.datetime(2026, 11, 2, 13, tzinfo=UTC)


def _payload(records, forecast_ids=(1,), observation_ids=(2,)):
    sources = [{"id": i, "observation_type": "forecast"} for i in forecast_ids]
    sources += [{"id": i, "observation_type": "current"} for i in observation_ids]
    return {"weather": records, "sources": sources}


MIXED = _payload([
    {"timestamp": "2026-11-02T12:00:00+00:00", "source_id": 2, "temperature": 9.0, "solar": 0.1},
    {"timestamp": "2026-11-02T13:00:00+00:00", "source_id": 1, "temperature": 8.5, "solar": None},
    {"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1, "temperature": 8.0, "solar": 0.05},
])


def _series(uid, station, field):
    return TimeSeriesDefinition(
        unique_id=uid, name=uid, description="", frequency="1 hour", unit="",
        domain="weather", category="x",
        extract_filter={"wmo_station_id": station, "field": field},
    )


def _plugin(kind):
    series = [
        _series("t_a", "10147", "temperature"),
        _series("w_a", "10147", "wind_speed"),
        _series("t_b", "10863", "temperature"),
    ]
    return BrightSkyPlugin("g", {"kind": kind, "horizon_hours": 48}, series, "30 minutes")


def test_forecast_keeps_only_forecast_records():
    points = station_points(MIXED, ["temperature", "solar"], "forecast")
    assert points["temperature"] == [
        {"ts": "2026-11-02T13:00:00+00:00", "value": 8.5},
        {"ts": "2026-11-02T14:00:00+00:00", "value": 8.0},
    ]
    assert points["solar"] == [{"ts": "2026-11-02T14:00:00+00:00", "value": 0.05}]


def test_observation_keeps_only_measured_records():
    points = station_points(MIXED, ["temperature", "solar"], "observation")
    assert points["temperature"] == [{"ts": "2026-11-02T12:00:00+00:00", "value": 9.0}]
    assert points["solar"] == [{"ts": "2026-11-02T12:00:00+00:00", "value": 0.1}]


def test_field_filled_from_another_source_is_dropped():
    payload = _payload([
        {"timestamp": "2026-11-02T12:00:00+00:00", "source_id": 2, "temperature": 9.0,
         "solar": 0.2, "fallback_source_ids": {"solar": 3}},
    ], observation_ids=(2, 3))
    points = station_points(payload, ["temperature", "solar"], "observation")
    assert len(points["temperature"]) == 1
    assert points["solar"] == []


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError):
        _plugin("nowcast")


def _record_calls(plugin, payload):
    calls = []

    def fake_fetch(station, start, end):
        calls.append((station, start, end))
        return payload

    plugin._fetch_station = fake_fetch
    return calls


def test_forecast_window_runs_from_this_hour_over_the_horizon():
    plugin = _plugin("forecast")
    calls = _record_calls(plugin, _payload([
        {"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1, "temperature": 7.0, "wind_speed": 12.0},
    ]))
    result = plugin._fetch_all(NOW, "2026-10-01T00:00:00")
    end = HOUR + dt.timedelta(hours=48)
    assert calls == [("10147", HOUR, end), ("10863", HOUR, end)]
    assert result["t_a"][0]["value"] == 7.0
    assert result["w_a"][0]["value"] == 12.0


def test_observation_window_runs_from_start_date_to_this_hour():
    plugin = _plugin("observation")
    calls = _record_calls(plugin, MIXED)
    result = plugin._fetch_all(NOW, "2026-10-03T13:33:08.5")
    start = dt.datetime(2026, 10, 3, 13, tzinfo=UTC)
    assert calls == [("10147", start, HOUR), ("10863", start, HOUR)]
    assert result["t_a"] == [{"ts": "2026-11-02T12:00:00+00:00", "value": 9.0}]


def test_observation_window_reaches_back_at_least_the_lookback():
    plugin = _plugin("observation")
    calls = _record_calls(plugin, MIXED)
    plugin._fetch_all(NOW, "2026-11-01T13:33:08")
    assert calls[0][1] == HOUR - dt.timedelta(hours=72)


def test_observation_ignores_synop_sources():
    payload = {"weather": [{"timestamp": "2026-11-02T12:30:00+00:00", "source_id": 5, "temperature": 9.0}],
               "sources": [{"id": 5, "observation_type": "synop"}]}
    assert station_points(payload, ["temperature"], "observation") == {"temperature": []}


def test_failed_station_is_skipped():
    plugin = _plugin("forecast")

    def fake_fetch(station, start, end):
        if station == "10147":
            raise ConnectionError("boom")
        return _payload([{"timestamp": "2026-11-02T14:00:00+00:00", "source_id": 1, "temperature": 3.0}])

    plugin._fetch_station = fake_fetch
    assert set(plugin._fetch_all(NOW)) == {"t_b"}


def test_all_stations_failing_raises_for_the_retry():
    plugin = _plugin("observation")

    def fake_fetch(station, start, end):
        raise ConnectionError("boom")

    plugin._fetch_station = fake_fetch
    with pytest.raises(RuntimeError):
        plugin._fetch_all(NOW)


def test_shipped_config_pairs_observations_with_forecasts():
    plugins = PluginLoader(str(SOURCES)).load_multi_series_plugins()
    obs = plugins["brightsky-dwd-observation"]
    fc = plugins["brightsky-dwd-mosmix-forecast"]
    assert (obs.kind, fc.kind) == ("observation", "forecast")
    assert obs.lookback_hours >= 48
    assert all(s.imputation_policy == "none" for s in obs.get_series_definitions())

    def keys(group, prefix):
        out = set()
        for s in group.get_series_definitions():
            assert s.unique_id.startswith(prefix + "_")
            assert s.domain == "weather"
            assert s.frequency == "1 hour"
            assert isinstance(s.extract_filter["wmo_station_id"], str)
            assert "Deutscher Wetterdienst" in s.description
            out.add((s.extract_filter["wmo_station_id"], s.extract_filter["field"]))
        return out

    obs_keys = keys(obs, "dwd_obs")
    assert obs_keys == keys(fc, "fc_dwd_mosmix")
    assert len(obs_keys) == 36
    assert {f for _, f in obs_keys} == {"temperature", "wind_speed", "solar"}
