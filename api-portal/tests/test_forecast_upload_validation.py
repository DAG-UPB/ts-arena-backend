"""Upload-path validation + quantile crossing repair tests."""
from datetime import datetime, timezone

import pytest

from app.schemas.forecast import ForecastDataPoint
from app.services.forecast_metrics import repair_point_quantiles


def _dp(pv):
    return ForecastDataPoint(ts=datetime(2026, 1, 1, tzinfo=timezone.utc), value=42.0, probabilistic_values=pv)


# --- validator: key/value filtering ------------------------------------------

def test_valid_full_quantiles_pass_through():
    pv = {f"q_0.{i}": float(i) for i in range(1, 10)}
    dp = _dp(pv)
    assert dp.probabilistic_values == pv


def test_unknown_keys_are_dropped_not_rejected():
    dp = _dp({"q_0.1": 1.0, "q_1.0": 5.0, "median": 3.0, "mean": 2.0})
    assert dp.probabilistic_values == {"q_0.1": 1.0}


def test_nan_and_inf_values_dropped():
    dp = _dp({"q_0.1": 1.0, "q_0.5": float("nan"), "q_0.9": float("inf")})
    assert dp.probabilistic_values == {"q_0.1": 1.0}


def test_empty_dict_passes():
    assert _dp({}).probabilistic_values == {}


def test_none_passes():
    assert _dp(None).probabilistic_values is None


def test_legacy_payload_without_field():
    dp = ForecastDataPoint(ts=datetime(2026, 1, 1, tzinfo=timezone.utc), value=42.0)
    assert dp.probabilistic_values is None


# --- repair_point_quantiles --------------------------------------------------

def test_repair_sorts_crossing():
    repaired, was = repair_point_quantiles({"q_0.1": 5.0, "q_0.5": 3.0, "q_0.9": 4.0})
    assert was is True
    # values sorted ascending, reassigned to levels in order
    assert repaired == {"q_0.1": 3.0, "q_0.5": 4.0, "q_0.9": 5.0}


def test_repair_noop_when_monotone():
    pv = {"q_0.1": 1.0, "q_0.5": 2.0, "q_0.9": 3.0}
    repaired, was = repair_point_quantiles(pv)
    assert was is False and repaired == pv


def test_repair_noop_on_empty_or_single():
    assert repair_point_quantiles({}) == ({}, False)
    assert repair_point_quantiles({"q_0.5": 1.0}) == ({"q_0.5": 1.0}, False)
    assert repair_point_quantiles(None) == (None, False)
