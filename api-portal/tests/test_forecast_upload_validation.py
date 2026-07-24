"""Upload-path validation + quantile crossing repair tests."""
from datetime import datetime, timezone

import pytest

from app.schemas.forecast import ForecastDataPoint
from app.services.forecast_metrics import (
    canonical_quantile_key,
    clean_probabilistic_values,
    parse_probabilistic_values,
    repair_point_quantiles,
)


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


# --- backend-69: both quantile key forms are accepted ------------------------

def test_canonical_quantile_key_accepts_both_forms():
    assert canonical_quantile_key("q_0.1") == "q_0.1"
    assert canonical_quantile_key("0.1") == "q_0.1"
    assert canonical_quantile_key("0.9") == "q_0.9"


@pytest.mark.parametrize(
    "key",
    ["0.10", "q_0.15", "0.0", "1.0", "q_1.0", "median", "", ".1", None, 0.1],
)
def test_canonical_quantile_key_rejects_non_deciles(key):
    assert canonical_quantile_key(key) is None


def test_bare_keys_are_normalised_on_upload_not_dropped():
    """The regression backend-69 is about: '0.1'…'0.9' used to be discarded at write time,
    silently destroying every quantile the statistical baselines submitted."""
    dp = _dp({f"0.{i}": float(i) for i in range(1, 10)})
    assert dp.probabilistic_values == {f"q_0.{i}": float(i) for i in range(1, 10)}
    assert dp.dropped_probabilistic_keys == []


def test_mixed_key_forms_normalise_to_one():
    dp = _dp({"q_0.1": 1.0, "0.5": 5.0, "0.9": 9.0})
    assert dp.probabilistic_values == {"q_0.1": 1.0, "q_0.5": 5.0, "q_0.9": 9.0}


def test_canonical_key_wins_over_bare_duplicate_either_order():
    assert clean_probabilistic_values({"q_0.1": 1.0, "0.1": 99.0}) == ({"q_0.1": 1.0}, ["0.1"])
    assert clean_probabilistic_values({"0.1": 99.0, "q_0.1": 1.0}) == ({"q_0.1": 1.0}, [])


def test_bare_keys_with_bad_values_still_dropped():
    dp = _dp({"0.1": 1.0, "0.5": float("nan"), "0.9": float("inf")})
    assert dp.probabilistic_values == {"q_0.1": 1.0}
    assert sorted(dp.dropped_probabilistic_keys) == ["0.5", "0.9"]


def test_dropped_keys_are_reported_on_the_point_not_logged():
    dp = _dp({"q_0.1": 1.0, "median": 3.0, "mean": 2.0})
    assert dp.probabilistic_values == {"q_0.1": 1.0}
    assert sorted(dp.dropped_probabilistic_keys) == ["mean", "median"]


def test_dropped_keys_never_serialise_into_the_payload():
    dp = _dp({"q_0.1": 1.0, "median": 3.0})
    assert "dropped_probabilistic_keys" not in dp.model_dump()


# --- backend-69: the read path scores both forms -----------------------------

def test_read_path_parses_bare_keys():
    """Without this, stored bare-key forecasts take the degenerate branch of
    assemble_quantile_forecasts and score as if no distribution was submitted."""
    assert parse_probabilistic_values({f"0.{i}": float(i) for i in range(1, 10)}) == {
        round(i / 10, 1): float(i) for i in range(1, 10)
    }


def test_read_path_parses_canonical_keys():
    assert parse_probabilistic_values({"q_0.1": 1.0, "q_0.9": 9.0}) == {0.1: 1.0, 0.9: 9.0}


def test_read_path_canonical_wins_over_bare_duplicate_either_order():
    assert parse_probabilistic_values({"q_0.1": 1.0, "0.1": 99.0}) == {0.1: 1.0}
    assert parse_probabilistic_values({"0.1": 99.0, "q_0.1": 1.0}) == {0.1: 1.0}


def test_read_path_still_rejects_non_deciles():
    assert parse_probabilistic_values({"0.10": 1.0, "median": 2.0, "1.0": 3.0}) == {}


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
