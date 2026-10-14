"""Golden-value and edge-case tests for the pure SQL metric module."""
from statistics import NormalDist

import numpy as np
import pytest

from app.services.forecast_metrics import (
    QUANTILE_LEVELS,
    assemble_quantile_forecasts,
    compute_sql_fields,
    naive_scale,
    parse_probabilistic_values,
    quantile_loss,
    repair_crossing,
    sql_score,
)


# --- quantile_loss -----------------------------------------------------------

def test_quantile_loss_golden_single_point():
    # y=10, ŷ^(0.1)=12 => 1(y<=ŷ)=1 => ρ = 2·|(10-12)·(1-0.1)| = 2·1.8 = 3.6
    loss = quantile_loss(np.array([10.0]), np.array([12.0]), 0.1)
    assert loss[0] == pytest.approx(3.6)


def test_quantile_loss_underprediction():
    # y=10, ŷ^(0.9)=8 => 1(y<=ŷ)=0 => ρ = 2·|(10-8)·(0-0.9)| = 2·1.8 = 3.6
    loss = quantile_loss(np.array([10.0]), np.array([8.0]), 0.9)
    assert loss[0] == pytest.approx(3.6)


def test_quantile_loss_perfect_is_zero():
    loss = quantile_loss(np.array([5.0, 7.0]), np.array([5.0, 7.0]), 0.5)
    assert np.allclose(loss, 0.0)


# --- naive_scale -------------------------------------------------------------

def test_naive_scale_basic():
    # mean|y - 10| for y=[8,12,10] = (2+2+0)/3
    assert naive_scale(np.array([8.0, 12.0, 10.0]), 10.0) == pytest.approx(4.0 / 3.0)


def test_naive_scale_zero_returns_none():
    assert naive_scale(np.array([5.0, 5.0]), 5.0) is None


# --- sql_score: the key degenerate == MASE identity --------------------------

def test_sql_degenerate_equals_mase():
    """All nine deciles == point forecast => SQL == arena-MASE of that point forecast."""
    rng = np.random.default_rng(0)
    y_true = rng.normal(50, 10, size=24)
    y_pred = rng.normal(50, 10, size=24)
    naive_value = 48.0

    scale = naive_scale(y_true, naive_value)
    mase = float(np.mean(np.abs(y_true - y_pred))) / scale

    degenerate = {level: y_pred.copy() for level in QUANTILE_LEVELS}
    overall, per_level = sql_score(y_true, degenerate, scale)

    assert overall == pytest.approx(mase, rel=1e-12)
    assert len(per_level) == 9


def test_sql_perfect_median_and_symmetric_band():
    y_true = np.array([10.0, 20.0, 30.0])
    scale = naive_scale(y_true, 0.0)  # nonzero
    # Perfect point forecast; symmetric band around it.
    qf = {
        0.1: y_true - 2, 0.2: y_true - 1.5, 0.3: y_true - 1.0, 0.4: y_true - 0.5,
        0.5: y_true.copy(),
        0.6: y_true + 0.5, 0.7: y_true + 1.0, 0.8: y_true + 1.5, 0.9: y_true + 2.0,
    }
    overall, per_level = sql_score(y_true, qf, scale)
    # median level is a perfect forecast => zero loss there
    assert per_level[0.5] == pytest.approx(0.0)
    assert overall > 0.0


def test_sql_scale_none_returns_none():
    overall, per_level = sql_score(np.array([1.0]), {0.5: np.array([1.0])}, None)
    assert overall is None and per_level == {}


def test_sql_empty_quantiles_returns_none():
    overall, per_level = sql_score(np.array([1.0]), {}, 2.0)
    assert overall is None and per_level == {}


def test_sql_partial_levels_only_scores_submitted():
    y_true = np.array([10.0, 12.0])
    scale = naive_scale(y_true, 0.0)
    qf = {0.5: y_true.copy(), 0.9: y_true + 1.0}
    overall, per_level = sql_score(y_true, qf, scale)
    assert set(per_level) == {0.5, 0.9}
    assert overall == pytest.approx(np.mean([per_level[0.5], per_level[0.9]]))


# --- parse_probabilistic_values ----------------------------------------------

def test_parse_valid_keys():
    parsed = parse_probabilistic_values({"q_0.1": 1.0, "q_0.9": 9.0})
    assert parsed == {0.1: 1.0, 0.9: 9.0}


def test_parse_drops_bad_keys_and_nonfinite():
    parsed = parse_probabilistic_values(
        {"q_0.1": 1.0, "q_1.0": 5.0, "median": 3.0, "q_0.5": float("nan"), "q_0.7": float("inf")}
    )
    assert parsed == {0.1: 1.0}


def test_parse_empty_and_none():
    assert parse_probabilistic_values(None) == {}
    assert parse_probabilistic_values({}) == {}


# --- repair_crossing ---------------------------------------------------------

def test_repair_crossing_counts_and_sorts():
    # timestamp 0 crosses (0.8 > 0.9), timestamp 1 is fine
    qf = {0.8: np.array([5.0, 1.0]), 0.9: np.array([3.0, 2.0])}
    repaired, count = repair_crossing(qf)
    assert count == 1
    assert np.allclose(repaired[0.8], [3.0, 1.0])
    assert np.allclose(repaired[0.9], [5.0, 2.0])


# --- assemble_quantile_forecasts ---------------------------------------------

def test_assemble_bare_key_forecasts_are_not_degenerate():
    """backend-69: stored '0.1'…'0.9' keys must reach the scorer as a real distribution.

    Before the fix these parsed to {} and fell into the degenerate branch below, so the
    four statistical baselines were scored as if they had submitted no quantiles at all.
    """
    y_pred = np.array([10.0, 20.0])
    bare = [
        {f"0.{i}": float(10 + i) for i in range(1, 10)},
        {f"0.{i}": float(20 + i) for i in range(1, 10)},
    ]
    canonical = [
        {f"q_0.{i}": float(10 + i) for i in range(1, 10)},
        {f"q_0.{i}": float(20 + i) for i in range(1, 10)},
    ]
    qf_bare, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, bare)
    assert has_q is True and levels_count == 9 and crossing == 0

    # Byte-identical to the same forecast expressed in the canonical form.
    qf_canonical, *_ = assemble_quantile_forecasts(y_pred, canonical)
    assert set(qf_bare) == set(qf_canonical)
    for level in qf_bare:
        assert np.allclose(qf_bare[level], qf_canonical[level])


def test_assemble_point_only_is_degenerate():
    y_pred = np.array([1.0, 2.0, 3.0])
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, [None, {}, None])
    assert has_q is False and levels_count == 0 and crossing == 0
    assert set(qf) == set(QUANTILE_LEVELS)
    for level in QUANTILE_LEVELS:
        assert np.allclose(qf[level], y_pred)


def test_assemble_full_quantiles_and_median_consistency():
    y_pred = np.array([10.0, 20.0])
    pv = [
        {f"q_0.{i}": float(10 + i) for i in range(1, 10)},
        {f"q_0.{i}": float(20 + i) for i in range(1, 10)},
    ]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert has_q is True and levels_count == 9 and crossing == 0
    assert np.allclose(qf[0.5], [15.0, 25.0])


def _column(qf, t):
    return [float(qf[level][t]) for level in QUANTILE_LEVELS]


def test_assemble_interpolates_between_neighbouring_levels():
    y_pred = np.array([12.0, 4.0])
    pv = [{"q_0.1": 8.0, "q_0.5": 12.0, "q_0.9": 20.0}, {"q_0.1": 0.0, "q_0.5": 4.0, "q_0.9": 4.0}]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert has_q is True and levels_count == 3 and crossing == 0
    assert set(qf) == set(QUANTILE_LEVELS)
    assert _column(qf, 0) == pytest.approx([8, 9, 10, 11, 12, 14, 16, 18, 20])
    assert _column(qf, 1) == pytest.approx([0, 1, 2, 3, 4, 4, 4, 4, 4])


def test_assemble_extrapolates_from_the_two_outermost_levels():
    y_pred = np.array([12.0, 10.0])
    pv = [{"q_0.3": 10.0, "q_0.5": 12.0, "q_0.7": 18.0}, {"q_0.2": 6.0, "q_0.8": 12.0}]
    qf, *_ = assemble_quantile_forecasts(y_pred, pv)
    # below 0.3 the line through (0.3, 10) and (0.5, 12); above 0.7 the one through (0.5, 12) and (0.7, 18)
    assert _column(qf, 0) == pytest.approx([8, 9, 10, 11, 12, 15, 18, 21, 24])
    assert _column(qf, 1) == pytest.approx([5, 6, 7, 8, 9, 10, 11, 12, 13])


def test_assemble_timestamp_with_one_decile_falls_back_to_point():
    y_pred = np.array([10.0, 10.0, 10.0])
    pv = [{"q_0.1": 8.0, "q_0.5": 10.0, "q_0.9": 12.0}, {"q_0.1": 8.0}, None]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert has_q is True and levels_count == 3 and crossing == 0
    assert _column(qf, 1) == [8.0] + [10.0] * 8
    assert _column(qf, 2) == [10.0] * 9


def test_assemble_without_two_deciles_at_any_timestamp_is_point_only():
    y_pred = np.array([10.0, 10.0, 10.0])
    pv = [{"q_0.5": 9.0}, {"q_0.9": 11.0}, None]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert has_q is False and crossing == 0
    assert levels_count == 2  # the levels still show that something was sent
    for t in range(3):
        assert _column(qf, t) == [10.0] * 9


def test_assemble_levels_count_is_the_distinct_levels_submitted():
    pv = [{"q_0.1": 1.0, "q_0.9": 9.0}, {"q_0.5": 5.0, "q_0.9": 9.0}]
    *_, levels_count, _ = assemble_quantile_forecasts(np.array([5.0, 5.0]), pv)
    assert levels_count == 3


def test_assemble_full_set_is_unchanged():
    y_pred = np.array([10.0, 20.0])
    pv = [{f"q_0.{i}": 10.0 + i ** 2 / 10 for i in range(1, 10)}, {f"0.{i}": 20.0 - 1 / i for i in range(1, 10)}]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert has_q is True and levels_count == 9 and crossing == 0
    assert _column(qf, 0) == [10.0 + i ** 2 / 10 for i in range(1, 10)]
    assert _column(qf, 1) == [20.0 - 1 / i for i in range(1, 10)]


def test_assemble_sorts_the_submitted_deciles_before_filling():
    pv = [{"q_0.1": 5.0, "q_0.5": 3.0, "q_0.9": 9.0}]
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(np.array([4.0]), pv)
    assert has_q is True and levels_count == 3 and crossing == 1
    assert _column(qf, 0) == pytest.approx([3, 3.5, 4, 4.5, 5, 6, 7, 8, 9])


def _bell_sql(submitted_levels):
    """SQL of a forecast exactly calibrated for standard-normal actuals that sends only `submitted_levels`."""
    normal = NormalDist()
    n = 20_000
    y_true = np.array([normal.inv_cdf((i + 0.5) / n) for i in range(n)])
    quantiles = {level: normal.inv_cdf(level) for level in submitted_levels}
    fields = compute_sql_fields(
        y_true, np.zeros(n), [{f"q_{level}": value for level, value in quantiles.items()}] * n, 1.0
    )
    unfilled, _ = sql_score(y_true, {level: np.full(n, value) for level, value in quantiles.items()}, 1.0)
    return fields["sql_score"], unfilled


def test_filling_missing_deciles_costs_a_fraction_of_a_percent_instead_of_gaining_a_fifth():
    nine, _ = _bell_sql(QUANTILE_LEVELS)
    three, three_unfilled = _bell_sql((0.1, 0.5, 0.9))
    assert three / nine - 1 == pytest.approx(0.004, abs=0.002)
    assert three_unfilled / nine - 1 == pytest.approx(-0.19, abs=0.005)

    two, two_unfilled = _bell_sql((0.1, 0.9))
    assert two / nine - 1 == pytest.approx(0.004, abs=0.002)
    assert two_unfilled / nine - 1 == pytest.approx(-0.43, abs=0.005)


def test_partial_deciles_are_scored_on_all_nine():
    fields = compute_sql_fields(
        np.array([10.0, 12.0]), np.array([11.0, 11.0]), [{"q_0.1": 9.0, "q_0.9": 13.0}] * 2, 2.0
    )
    assert fields["has_quantiles"] is True and fields["quantile_levels_count"] == 2
    assert set(fields["sql_per_quantile"]) == {f"0.{i}" for i in range(1, 10)}
    assert fields["sql_score"] == pytest.approx(np.mean(list(fields["sql_per_quantile"].values())))


def test_assemble_repairs_crossing():
    y_pred = np.array([10.0])
    pv = [{"q_0.8": 9.0, "q_0.9": 3.0}]  # crossing
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(y_pred, pv)
    assert crossing == 1
    assert qf[0.8][0] == pytest.approx(3.0)
    assert qf[0.9][0] == pytest.approx(9.0)
    assert _column(qf, 0) == sorted(_column(qf, 0))


def test_assemble_repairs_crossing_in_a_full_set_and_counts_the_timestamp():
    values = [float(i) for i in range(1, 10)]
    crossed = dict(zip((f"q_0.{i}" for i in range(1, 10)), [1.0, 2.0, 4.0, 3.0, 5.0, 6.0, 7.0, 8.0, 9.0]))
    ordered = dict(zip((f"q_0.{i}" for i in range(1, 10)), values))
    qf, has_q, levels_count, crossing = assemble_quantile_forecasts(np.array([5.0, 5.0]), [crossed, ordered])
    assert has_q is True and levels_count == 9 and crossing == 1
    assert _column(qf, 0) == values and _column(qf, 1) == values
