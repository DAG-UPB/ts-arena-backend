"""Real MASE (Hyndman & Koehler 2006): context scale and per-evaluation fields.

Reference values marked "GluonTS" were produced with gluonts 0.16
(`gluonts.evaluation.metrics.calculate_seasonal_error` + `mase`, past data passed through
`np.ma.masked_invalid` as its `Evaluator` does) on exactly these fixtures. GluonTS is not a
service dependency, so the numbers are pinned here instead of recomputed.
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from app.services.forecast_metrics import (
    MASE_SEASONAL_LAG,
    compute_real_mase_fields,
    context_scale,
    mase_scale_defined,
)

HOUR = timedelta(hours=1)
T0 = datetime(2026, 1, 5, tzinfo=timezone.utc)


def _grid(values, freq=HOUR, start=T0):
    """Context on a regular grid; `None`/NaN entries are dropped (a gap, not a value)."""
    return [start + i * freq for i in range(len(values))], list(values)


def hyndman_koehler_mase(past, actual, forecast, m=1):
    """Textbook MASE on a gap-free positional series (the definition, written out)."""
    past = np.asarray(past, dtype=float)
    scale = np.mean(np.abs(past[m:] - past[:-m]))
    return np.mean(np.abs(np.asarray(actual) - np.asarray(forecast))) / scale


def _hourly_fixture():
    t = np.arange(7 * 24 + 24)
    y = 100 + 20 * np.sin(2 * np.pi * t / 24) + 0.5 * t
    return y[:168], y[168:]


# ---------------------------------------------------------------------------------------
# context_scale
# ---------------------------------------------------------------------------------------

def test_default_lag_is_non_seasonal():
    assert MASE_SEASONAL_LAG == 1


def test_issue_example_matches_hand_written_and_gluonts():
    # Context 10,12,11,13,15; actuals 14,16; forecast 15,14 -> standard MASE 0.857.
    ts, vals = _grid([10, 12, 11, 13, 15])
    scale, m_used, n_points, n_pairs = context_scale(ts, vals, HOUR)
    assert (scale, m_used, n_points, n_pairs) == (1.75, 1, 5, 4)

    fields = compute_real_mase_fields(np.array([14.0, 16.0]), np.array([15.0, 14.0]), [None, None], scale)
    assert fields["mae"] == pytest.approx(1.5)
    assert fields["mase"] == pytest.approx(0.8571428571428571)  # GluonTS
    assert fields["mase"] == pytest.approx(hyndman_koehler_mase([10, 12, 11, 13, 15], [14, 16], [15, 14]))


@pytest.mark.parametrize(
    "forecast_kind, expected",
    [("flat", 3.966155572449507), ("seasonal_copy_plus_10", 0.60253611517315)],  # GluonTS
)
def test_hourly_daily_cycle_matches_gluonts(forecast_kind, expected):
    past, future = _hourly_fixture()
    forecast = np.full(24, past[-1]) if forecast_kind == "flat" else past[-24:] + 10.0
    ts, vals = _grid(past)

    scale, _, n_points, n_pairs = context_scale(ts, vals, HOUR)
    assert scale == pytest.approx(3.3193031083709568)  # GluonTS seasonal_error, m=1
    assert (n_points, n_pairs) == (168, 167)

    fields = compute_real_mase_fields(future, forecast, [None] * 24, scale)
    assert fields["mase"] == pytest.approx(expected)
    assert fields["mase"] == pytest.approx(hyndman_koehler_mase(past, future, forecast))


def test_seasonal_lag_matches_gluonts():
    past, future = _hourly_fixture()
    ts, vals = _grid(past)
    scale, m_used, _, n_pairs = context_scale(ts, vals, HOUR, m=24)
    assert (m_used, n_pairs) == (24, 144)
    assert scale == pytest.approx(12.0)  # GluonTS, m=24
    fields = compute_real_mase_fields(future, np.full(24, past[-1]), [None] * 24, scale)
    assert fields["mase"] == pytest.approx(1.0970727099928699)  # GluonTS


@pytest.mark.parametrize("gap", ["missing_timestamp", "none_value", "nan_value"])
def test_gap_pairs_are_skipped_like_gluonts_masked(gap):
    # 10, 12, <gap>, 13, 15, 14: only 12-10, 15-13 and 14-15 are lag-1 pairs.
    values = [10, 12, None, 13, 15, 14]
    ts, vals = _grid(values)
    if gap == "missing_timestamp":
        ts, vals = ts[:2] + ts[3:], vals[:2] + vals[3:]
    elif gap == "nan_value":
        vals[2] = float("nan")

    scale, m_used, n_points, n_pairs = context_scale(ts, vals, HOUR)
    assert (m_used, n_points, n_pairs) == (1, 5, 3)
    assert scale == pytest.approx(1.6666666666666667)  # GluonTS, masked
    fields = compute_real_mase_fields(np.array([14.0, 16.0]), np.array([15.0, 14.0]), [None, None], scale)
    assert fields["mase"] == pytest.approx(0.9)  # GluonTS, masked


def test_lag_is_by_timestamp_not_position():
    # A positional lag would difference 12 and 30 across the missing 3-hour stretch.
    ts = [T0, T0 + HOUR, T0 + 5 * HOUR, T0 + 6 * HOUR]
    scale, _, n_points, n_pairs = context_scale(ts, [10.0, 12.0, 30.0, 31.0], HOUR)
    assert (n_points, n_pairs) == (4, 2)
    assert scale == pytest.approx(1.5)


def test_input_order_does_not_matter():
    ts, vals = _grid([10, 12, 11, 13, 15])
    order = [3, 0, 4, 1, 2]
    assert context_scale([ts[i] for i in order], [vals[i] for i in order], HOUR) == context_scale(ts, vals, HOUR)


def test_fifteen_minute_frequency():
    q = timedelta(minutes=15)
    ts, vals = _grid([1.0, 2.0, 4.0, 7.0], freq=q)
    assert context_scale(ts, vals, q)[0] == pytest.approx(2.0)
    # The wrong frequency finds no neighbours at all (the grid spans only 45 minutes).
    assert context_scale(ts, vals, HOUR)[0] is None


@pytest.mark.parametrize("n, expected_m", [(24, 1), (25, 24)])
def test_short_context_falls_back_to_lag_one(n, expected_m):
    # `len(context) <= m` -> m = 1 (GluonTS gives scale 1.0 for 0..23 with m=24).
    ts, vals = _grid(np.arange(float(n)))
    scale, m_used, _, _ = context_scale(ts, vals, HOUR, m=24)
    assert m_used == expected_m
    assert scale == pytest.approx(1.0 if expected_m == 1 else 24.0)


def test_constant_context_gives_zero_scale_and_no_mase():
    ts, vals = _grid([5.0] * 10)
    scale, _, _, n_pairs = context_scale(ts, vals, HOUR)
    assert scale == 0.0 and n_pairs == 9
    assert not mase_scale_defined(scale)

    fields = compute_real_mase_fields(np.array([5.0, 6.0]), np.array([5.0, 5.0]), [None, None], scale)
    assert fields["mase"] is None and fields["sql_score"] is None
    assert fields["mae"] == pytest.approx(0.5)  # raw MAE kept for later rescaling


@pytest.mark.parametrize("values", [[], [7.0], [None, 3.0, None]])
def test_no_lag_pair_gives_undefined_scale(values):
    ts, vals = _grid(values)
    scale, _, _, n_pairs = context_scale(ts, vals, HOUR)
    assert scale is None and n_pairs == 0
    fields = compute_real_mase_fields(np.array([1.0]), np.array([2.0]), [None], scale)
    assert fields["mase"] is None and fields["sql_score"] is None


@pytest.mark.parametrize("scale", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_undefined_scales_never_yield_inf(scale):
    fields = compute_real_mase_fields(np.array([1.0, 2.0]), np.array([2.0, 2.0]), [None, None], scale)
    assert fields["mase"] is None
    assert fields["sql_score"] is None


# ---------------------------------------------------------------------------------------
# compute_real_mase_fields: SQL against the same scale
# ---------------------------------------------------------------------------------------

def test_point_only_sql_equals_mase():
    y_true = np.array([14.0, 16.0, 13.0])
    y_pred = np.array([15.0, 14.0, 13.5])
    fields = compute_real_mase_fields(y_true, y_pred, [None] * 3, 1.75)
    assert fields["has_quantiles"] is False
    assert fields["sql_score"] == pytest.approx(fields["mase"], abs=1e-12)


def test_quantile_sql_uses_context_scale():
    y_true = np.array([10.0, 20.0])
    y_pred = np.array([12.0, 18.0])
    pv = [{"q_0.1": 8.0, "q_0.5": 12.0, "q_0.9": 16.0}, {"q_0.1": 15.0, "q_0.5": 18.0, "q_0.9": 25.0}]
    scale = 2.0
    fields = compute_real_mase_fields(y_true, y_pred, pv, scale)

    def ql(y, q, level):
        return 2 * abs((y - q) * ((y <= q) - level))

    expected = np.mean([
        np.mean([ql(10, 8, 0.1), ql(20, 15, 0.1)]) / scale,
        np.mean([ql(10, 12, 0.5), ql(20, 18, 0.5)]) / scale,
        np.mean([ql(10, 16, 0.9), ql(20, 25, 0.9)]) / scale,
    ])
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 3
    assert fields["sql_score"] == pytest.approx(expected)
    assert set(fields["sql_per_quantile"]) == {"0.1", "0.5", "0.9"}
    assert fields["mase"] == pytest.approx(2.0 / scale)


def test_fields_carry_mae_and_point_count():
    fields = compute_real_mase_fields(np.array([1.0, 2.0, 3.0, 4.0]), np.array([1.0, 1.0, 1.0, 1.0]), [None] * 4, 3.0)
    assert fields["mae"] == pytest.approx(1.5)
    assert fields["n_points"] == 4
    assert fields["scale"] == 3.0
    assert fields["mase"] == pytest.approx(0.5)
