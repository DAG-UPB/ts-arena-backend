"""Arena Score rating core: the running example, recovery, invariances, guards and rules."""
import logging
import math
from collections import defaultdict
from itertools import combinations

import numpy as np
import pytest

from app.services import arena_score as arena
from app.services.arena_score import ArenaScoreFitError, Coverage, POINTS_PER_UNIT

LOAD, BIKES = 1, 2
A, B, C = 1, 2, 3
POINTS = 24

# (round, series, {model: MAE}); the rounds are 28 days apart: each is its own week and block
TOY = [
    (1, LOAD, {A: 100, B: 90}),
    (1, BIKES, {A: 4.0, B: 3.6}),
    (2, LOAD, {A: 110, B: 100}),
    (2, BIKES, {A: 5.0, B: 4.5}),
    (3, LOAD, {A: 95, B: 950}),
    (3, BIKES, {A: 4.0, B: 3.8}),
    (4, LOAD, {A: 100, B: 95, C: 60}),
    (4, BIKES, {A: 4.4, B: 4.0, C: 2.4}),
    (5, LOAD, {A: 120, B: 110, C: 115}),
    (5, BIKES, {A: 3.0, B: 3.2, C: 3.3}),
]


def toy_day(rnd):
    return 28 * (rnd - 1)


def monday(rnd):
    return 4 + 7 * rnd  # consecutive rounds fall in consecutive weeks


def replace_task(tasks, rnd, series, errs):
    return [(r, s, errs if (r, s) == (rnd, series) else e) for r, s, e in tasks]


def without(tasks, model, rounds):
    """The tasks with the model's rows removed in the given rounds."""
    return [(r, s, {m: e for m, e in errs.items() if not (m == model and r in rounds)})
            for r, s, errs in tasks]


def build(tasks, day_of=toy_day, beta=1.0):
    """Pair sums of tasks (round, series, {model: err or (err, n_points)})."""
    r, s, m, e, n, d = [], [], [], [], [], []
    for rnd, series, errs in tasks:
        for model, value in errs.items():
            err, points = value if isinstance(value, tuple) else (value, POINTS)
            r.append(rnd), s.append(series), m.append(model), e.append(err)
            n.append(points), d.append(day_of(rnd))
    return arena.pair_sums(r, s, m, e, n, d, beta=beta)


def all_covered(models):
    return {m: Coverage(True, 1.0) for m in models}


def rate(tasks, day_of=toy_day, covered=None, **kwargs):
    models = sorted({m for _, _, errs in tasks for m in errs})
    pairs = build(tasks, day_of, kwargs.get("beta", 1.0))
    result = arena.rate_scope(pairs, models, covered or all_covered(models), **kwargs)
    return {row.model_id: row for row in result.models}, result.method


def synthetic(rng, theta, weeks, series, miss=0.3, noise=0.0, drift=None):
    """Tasks with err = scale * exp(-strength + noise), one round per week; scales span decades."""
    scale = np.exp(rng.normal(0.0, 3.0, series))
    tasks = []
    for week in range(weeks):
        for s in range(series):
            present = [m for m in range(len(theta)) if rng.random() >= miss]
            tasks.append((week, s, {m: float(scale[s] * math.exp(
                -(theta[m] + (0.0 if drift is None else drift[week, m])) + noise * rng.normal()))
                for m in present}))
    return tasks


# ---------------------------------------------------------------- running example


def test_toy_pairs_and_weights():
    pairs = build(TOY)
    assert pairs.n_pairs == 18
    assert (pairs.skipped_points, pairs.skipped_zero) == (0, ())
    assert (pairs.first_day, pairs.last_day) == (0, 112)
    assert pairs.w.sum() == pytest.approx(12.0)
    by_key = {(d, i, j): (w, wy) for d, i, j, w, wy in
              zip(pairs.day, pairs.i, pairs.j, pairs.w, pairs.wy, strict=True)}
    assert len(by_key) == 9 and all(i < j for _, i, j in by_key)
    assert by_key[(0, A, B)] == pytest.approx((2.0, 90 / 190 + 3.6 / 7.6))
    assert by_key[(56, A, B)] == pytest.approx((2.0, 950 / 1045 + 3.8 / 7.8))
    assert by_key[(84, A, C)] == pytest.approx((1.0, 0.5 * (60 / 160 + 2.4 / 6.8)))
    assert {d for d, _, j in by_key if j == C} == {84, 112}
    assert all(w == 2.0 for (d, _, _), (w, _) in by_key.items() if d <= 56)  # two tasks, w = 1
    assert all(w == 1.0 for (d, _, _), (w, _) in by_key.items() if d >= 84)  # two tasks, w = 1/2


def test_toy_has_18_pairs_with_their_weights():
    one_day_each = [(k, s, errs) for k, (_, s, errs) in enumerate(TOY)]
    pairs = build(one_day_each, day_of=lambda k: k)
    assert pairs.n_pairs == 18 and len(pairs.day) == 18
    expected = []
    for k, (_, _, errs) in enumerate(one_day_each):
        for a, b in combinations(sorted(errs), 2):
            w = 1.0 / (len(errs) - 1)
            expected.append((k, a, b, w, w * errs[b] / (errs[a] + errs[b])))
    got = sorted(zip(pairs.day, pairs.i, pairs.j, pairs.w, pairs.wy, strict=True))
    assert [g[:3] for g in got] == [e[:3] for e in expected]
    assert [g[3:] for g in got] == pytest.approx([e[3:] for e in expected])
    assert [e[3] for e in expected].count(1.0) == 6 and [e[3] for e in expected].count(0.5) == 12


def test_toy_scores_weeks_and_evidence():
    rows, method = rate(TOY)
    assert [rows[m].theta for m in (A, B, C)] == pytest.approx([-0.0216, -0.1386, 0.1602], abs=5e-5)
    assert sum(rows[m].theta for m in (A, B, C)) == pytest.approx(0.0, abs=1e-9)
    assert [round(rows[m].score) for m in (A, B, C)] == [996, 976, 1028]
    assert [rows[m].weeks for m in (A, B, C)] == [5, 5, 2]
    assert [rows[m].evidence for m in (A, B, C)] == pytest.approx([10.0, 10.0, 4.0])
    assert method["fit_max_grad"] <= arena.GTOL and method["refit_max_grad"] <= arena.GTOL


def test_toy_pooled_sigma_rho_and_anchor():
    _, method = rate(TOY)
    assert method["sigma_week"] == pytest.approx(0.411, abs=5e-4)
    assert method["sigma_week_points"] == pytest.approx(71, abs=0.5)
    assert method["rho"] == 0.0
    assert (method["pool_size"], method["pool_min_weeks"]) == (3, 2)
    assert (method["anchor_size"], method["anchor_min_weeks"]) == (2, 5)
    assert (method["circle_days"], method["n_boot"], method["block_days"]) == (140, 200, 28)
    assert (method["seed"], method["beta"]) == (arena.SEED, 1.0)
    assert (method["tau"], method["provisional_weeks"]) == (0.5, 4)
    assert (method["n_pairs"], method["n_fitted"], method["n_models"]) == (18, 3, 3)


def test_toy_weekly_strengths():
    rows, _ = rate(TOY, n_boot=0)
    theta = np.array([rows[m].theta for m in (A, B, C)])
    pairs = build(TOY)
    i, j = pairs.i - 1, pairs.j - 1
    model, _, resid = arena._week_strengths(i, j, (pairs.day + 3) // 7, pairs.w, pairs.wy, theta, 3)
    shown = np.round(1000 + POINTS_PER_UNIT * (resid + theta[model])).astype(int)
    assert shown[model == 0].tolist() == [958, 958, 1122, 948, 1003]
    assert shown[model == 1].tolist() == [1015, 1014, 851, 977, 1016]
    assert shown[model == 2].tolist() == [1077, 981]


def test_toy_floor_and_combined_interval():
    rows, _ = rate(TOY)
    assert [round(rows[m].floor) for m in (A, B, C)] == [63, 63, 99]
    assert (round(rows[C].ci_lower), round(rows[C].ci_upper)) == (929, 1127)
    assert (round(rows[A].ci_lower), round(rows[A].ci_upper)) == (934, 1059)
    assert (round(rows[B].ci_lower), round(rows[B].ci_upper)) == (913, 1039)
    for row in rows.values():
        assert row.ci_lower == pytest.approx(min(row.boot_lower, row.score - row.floor))
        assert row.ci_upper == pytest.approx(max(row.boot_upper, row.score + row.floor))


def test_toy_bootstrap_bounds():
    rows, _ = rate(TOY)
    for m, (lo, hi) in {A: (978, 1032), B: (940, 994), C: (980, 1078)}.items():
        assert rows[m].boot_lower == pytest.approx(lo, abs=5)
        assert rows[m].boot_upper == pytest.approx(hi, abs=5)
    rows, _ = rate(TOY, n_boot=2000)
    for m, (lo, hi) in {A: (978, 1028), B: (944, 995), C: (980, 1077)}.items():
        assert rows[m].boot_lower == pytest.approx(lo, abs=3)
        assert rows[m].boot_upper == pytest.approx(hi, abs=3)


def test_toy_models_are_absent_from_refits_without_their_rounds():
    rows, _ = rate(TOY)
    assert rows[A].boot_refits == rows[B].boot_refits == 200
    assert 175 <= rows[C].boot_refits <= 195  # absent when neither round 4 nor 5 is drawn: 8 %


def test_toy_rank_ranges_and_provisional_model():
    rows, _ = rate(TOY)
    assert (rows[A].rank_best, rows[A].rank_worst) == (1, 2)
    assert (rows[B].rank_best, rows[B].rank_worst) == (1, 2)
    assert rows[C].provisional and (rows[C].rank_best, rows[C].rank_worst) == (None, None)
    assert not rows[A].provisional and not rows[B].provisional
    assert not any(r.low_coverage or r.unranked_disconnected for r in rows.values())


def test_toy_bikes_scaled_by_1000_leaves_everything_unchanged():
    scaled = [(r, s, {m: e * 1000 if s == BIKES else e for m, e in errs.items()}) for r, s, errs in TOY]
    rows, _ = rate(TOY)
    rows_scaled, _ = rate(scaled)
    assert [round(rows_scaled[m].score) for m in (A, B, C)] == [996, 976, 1028]
    for m in (A, B, C):
        assert rows_scaled[m].score == pytest.approx(rows[m].score, abs=1e-4)
        assert rows_scaled[m].ci_lower == pytest.approx(rows[m].ci_lower, abs=1e-4)
        assert ((rows_scaled[m].rank_best, rows_scaled[m].rank_worst)
                == (rows[m].rank_best, rows[m].rank_worst))


def test_toy_blow_up_is_bounded():
    def scores(blow_up):
        rows, _ = rate(replace_task(TOY, 3, LOAD, {A: 95, B: blow_up}), n_boot=0)
        return round(rows[A].score), round(rows[B].score)

    assert scores(90) == (980, 992)
    assert scores(950) == (996, 976)
    assert scores(95000) == (1000, 972)


def test_toy_missing_forecast_is_no_comparison():
    rows, _ = rate(replace_task(TOY, 5, BIKES, {A: 3.0, B: 3.2}), n_boot=0)
    assert round(rows[C].score) == 1040


def test_toy_is_deterministic_and_the_seed_is_recorded():
    first, method = rate(TOY)
    again, method_again = rate(TOY)
    other, method_other = rate(TOY, seed=5)
    assert first == again and method == method_again
    assert method_other["seed"] == 5 and other[A].boot_upper != first[A].boot_upper
    assert other[A].score == first[A].score


def test_toy_row_order_does_not_matter():
    order = np.random.default_rng(3).permutation(len(TOY))
    rows, _ = rate(TOY)
    shuffled, _ = rate([TOY[k] for k in order])
    assert {m: r.score for m, r in shuffled.items()} == pytest.approx({m: r.score for m, r in rows.items()})


def test_two_models_differ_by_points_per_unit_times_the_log_error_ratio():
    rows, _ = rate([(r, s, {1: 50.0, 2: 100.0}) for r in range(3) for s in (1, 2)], n_boot=0)
    assert rows[1].score - rows[2].score == pytest.approx(POINTS_PER_UNIT * math.log(2), abs=1e-4)
    assert rows[1].score == pytest.approx(1000 + POINTS_PER_UNIT * math.log(2) / 2, abs=1e-4)


# ---------------------------------------------------------------- fit properties


def test_fit_recovers_the_strengths_from_per_pair_sums():
    theta = np.array([0.5, -0.2, -0.3])
    i, j = np.array([0, 0, 1]), np.array([1, 2, 2])
    w = np.array([3.0, 2.0, 5.0])
    wy = w / (1.0 + np.exp(-(theta[i] - theta[j])))
    assert arena.fit(i, j, w, wy, 3) == pytest.approx(theta - theta.mean(), abs=1e-6)


def test_exact_recovery_on_noise_free_data_with_unequal_series_sets():
    rng = np.random.default_rng(1)
    truth = np.array([0.8, 0.1, -0.2, -0.5, 0.0, -0.2])
    tasks = synthetic(rng, truth, weeks=12, series=9, miss=0.5)
    assert any(len(e) < len(truth) for _, _, e in tasks)
    rows, _ = rate(tasks, day_of=monday, n_boot=0)
    theta = np.array([rows[m].theta for m in range(len(truth))])
    assert theta == pytest.approx(truth - truth.mean(), abs=1e-6)
    assert rows[0].score - rows[3].score == pytest.approx(POINTS_PER_UNIT * 1.3, abs=1e-3)


def test_stationary_under_an_independent_loop_likelihood():
    rng = np.random.default_rng(2)
    tasks = synthetic(rng, np.array([0.5, 0.0, -0.3, 0.2, -0.4]), weeks=10, series=6, miss=0.4, noise=0.6)
    rows, _ = rate(tasks, day_of=monday, n_boot=0)
    theta = {m: r.theta for m, r in rows.items()}
    grad = defaultdict(float)
    for _, _, errs in tasks:
        if len(errs) < 2:
            continue
        w = 1.0 / (len(errs) - 1)
        for a, b in combinations(sorted(errs), 2):
            y = errs[b] / (errs[a] + errs[b])
            p = 1.0 / (1.0 + math.exp(-(theta[a] - theta[b])))
            grad[a] += w * (y - p)
            grad[b] -= w * (y - p)
    assert max(abs(g) for g in grad.values()) < 1e-5
    assert sum(theta.values()) == pytest.approx(0.0, abs=1e-9)


def test_rescaling_one_series_by_1000_keeps_scores_intervals_and_ranks():
    rng = np.random.default_rng(4)
    tasks = synthetic(rng, np.array([0.6, 0.2, 0.0, -0.3, -0.5]), weeks=30, series=5, miss=0.3, noise=0.4)
    scaled = [(r, s, {m: e * 1000 if s == 2 else e for m, e in errs.items()}) for r, s, errs in tasks]
    rows, _ = rate(tasks, day_of=monday)
    rows_scaled, _ = rate(scaled, day_of=monday)
    for m in rows:
        assert rows_scaled[m].score == pytest.approx(rows[m].score, abs=1e-3)
        assert rows_scaled[m].ci_lower == pytest.approx(rows[m].ci_lower, abs=1e-3)
        assert ((rows_scaled[m].rank_best, rows_scaled[m].rank_worst)
                == (rows[m].rank_best, rows[m].rank_worst))


def test_stop_rule_holds_for_the_fit_and_every_refit_with_heavy_tails():
    rng = np.random.default_rng(16)
    tasks = synthetic(rng, rng.normal(0.0, 0.5, 25), weeks=45, series=6, miss=0.6, noise=0.8)
    tasks = [(r, s, {m: e * math.exp(rng.choice([-4.0, 4.0])) if rng.random() < 0.05 else e
                     for m, e in errs.items()}) for r, s, errs in tasks]
    _, method = rate(tasks, day_of=monday)
    assert 0 < method["fit_max_grad"] <= arena.GTOL
    assert 0 < method["refit_max_grad"] <= arena.GTOL


def test_beta_divides_the_fitted_strengths():
    rng = np.random.default_rng(5)
    truth = np.array([0.7, 0.0, -0.4, -0.3])
    tasks = synthetic(rng, truth, weeks=8, series=6, miss=0.3)
    for beta in (0.5, 2.0):
        rows, method = rate(tasks, day_of=monday, beta=beta, n_boot=0)
        theta = np.array([rows[m].theta for m in range(len(truth))])
        assert theta == pytest.approx(truth - truth.mean(), abs=1e-6)
        assert method["beta"] == beta
    with pytest.raises(ValueError):
        arena.rate_scope(build(tasks, monday), range(4), all_covered(range(4)), beta=2.0)
    with pytest.raises(ValueError):
        build(tasks, monday, beta=0.0)


def test_beta_scales_every_quantity_on_the_strength_axis():
    rng = np.random.default_rng(17)
    tasks = synthetic(rng, np.array([0.5, 0.0, -0.3, 0.2]), weeks=30, series=4, miss=0.2, noise=0.5)
    squared = [(r, s, {m: e ** 2 for m, e in errs.items()}) for r, s, errs in tasks]
    base, base_method = rate(squared, day_of=monday)  # the same outcomes as beta = 2 on the errors
    scaled, method = rate(tasks, day_of=monday, beta=2.0)
    for m in base:
        for field in ("score", "ci_lower", "ci_upper", "boot_lower", "boot_upper"):
            got, want = getattr(scaled[m], field) - 1000, (getattr(base[m], field) - 1000) / 2
            assert got == pytest.approx(want, abs=1e-6)
        assert scaled[m].theta == pytest.approx(base[m].theta / 2)
        assert scaled[m].floor == pytest.approx(base[m].floor / 2)
        assert (scaled[m].rank_best, scaled[m].rank_worst) == (base[m].rank_best, base[m].rank_worst)
    assert method["sigma_week"] == pytest.approx(base_method["sigma_week"] / 2)
    assert method["rho"] == pytest.approx(base_method["rho"])


def test_fit_raises_instead_of_returning_an_unconverged_result(monkeypatch):
    monkeypatch.setattr(arena, "MAX_ITER", 1)
    with pytest.raises(ArenaScoreFitError):
        rate(TOY)


# ---------------------------------------------------------------- weeks, anchor, pool


def test_weeks_start_on_monday():
    day_of = {1: 3, 2: 4, 3: 10, 4: 11}  # Sunday, Monday, Sunday, Monday: three weeks
    rows, _ = rate([(r, 1, {A: 1.0, B: 2.0}) for r in day_of], day_of=day_of.get)
    assert rows[A].weeks == rows[B].weeks == 3


def test_no_model_with_two_weeks_means_no_interval_and_everyone_provisional():
    rows, method = rate([(0, s, {A: 1.0 + s, B: 2.0, C: 1.5}) for s in range(3)])
    assert method["sigma_week"] is None and method["rho"] is None
    for row in rows.values():
        assert row.weeks == 1 and row.provisional and row.score is not None
        assert row.ci_lower is None and row.ci_upper is None and row.floor is None
        assert (row.rank_best, row.rank_worst) == (None, None)


def test_pool_falls_back_from_26_to_8_to_2_weeks():
    rng = np.random.default_rng(9)
    theta = np.array([0.2, 0.0, -0.2])
    for weeks, threshold in ((30, 26), (10, 8), (5, 2)):
        tasks = synthetic(rng, theta, weeks=weeks, series=4, miss=0.0, noise=0.3)
        _, method = rate(tasks, day_of=monday, n_boot=0)
        assert (method["pool_min_weeks"], method["pool_size"]) == (threshold, 3)
    tasks = without(synthetic(rng, theta, weeks=30, series=4, miss=0.0, noise=0.3), 2, set(range(9, 30)))
    _, method = rate(tasks, day_of=monday, n_boot=0)
    assert (method["pool_min_weeks"], method["pool_size"]) == (26, 2)


def test_anchor_set_is_the_models_with_the_most_weeks_when_nobody_has_26():
    rng = np.random.default_rng(10)
    tasks = synthetic(rng, np.array([0.2, 0.0, -0.2, 0.1]), weeks=12, series=4, miss=0.0, noise=0.3)
    tasks = without(without(tasks, 2, {0, 1, 2}), 3, {1, 3, 5, 7, 9, 11})
    rows, method = rate(tasks, day_of=monday)
    assert [rows[m].weeks for m in range(4)] == [12, 12, 9, 6]
    assert (method["anchor_min_weeks"], method["anchor_size"]) == (12, 2)


def test_anchor_set_takes_models_from_26_weeks_on():
    rng = np.random.default_rng(10)
    tasks = synthetic(rng, np.array([0.2, 0.0, -0.2]), weeks=40, series=3, miss=0.0, noise=0.3)
    rows, method = rate(without(tasks, 2, set(range(14))), day_of=monday)
    assert rows[2].weeks == 26
    assert (method["anchor_min_weeks"], method["anchor_size"]) == (26, 3)
    rows, method = rate(without(tasks, 2, set(range(15))), day_of=monday)
    assert rows[2].weeks == 25
    assert (method["anchor_min_weeks"], method["anchor_size"]) == (26, 2)


# ---------------------------------------------------------------- floor: linked weeks


def f_closed_form(weeks, rho):
    return 1 + 2 * (rho / (1 - rho) - rho * (1 - rho ** weeks) / (weeks * (1 - rho) ** 2))


@pytest.mark.parametrize("weeks", [1, 2, 5, 26, 90])
@pytest.mark.parametrize("rho", [0.0, 0.39, 0.8])
def test_linkage_factor_matches_its_closed_form(weeks, rho):
    assert arena._linkage(weeks, rho) == pytest.approx(f_closed_form(weeks, rho) if weeks > 1 else 1.0)


def reference_floor(tasks, day_of, theta):
    """Weekly strengths by bisection, pooled sigma and rho, with plain loops."""
    sides = defaultdict(list)
    for rnd, _, errs in tasks:
        if len(errs) < 2:
            continue
        w = 1.0 / (len(errs) - 1)
        week = (day_of(rnd) + 3) // 7
        for a, b in combinations(sorted(errs), 2):
            y = errs[b] / (errs[a] + errs[b])
            sides[(a, week)].append((w, y, theta[b]))
            sides[(b, week)].append((w, 1 - y, theta[a]))
    resid = defaultdict(dict)
    for (m, week), items in sides.items():
        lo, hi = theta[m] - 30, theta[m] + 30
        for _ in range(200):
            mid = (lo + hi) / 2
            f = sum(w * (y - 1 / (1 + math.exp(-(mid - th)))) for w, y, th in items)
            lo, hi = (mid, hi) if f > 0 else (lo, mid)
        resid[m][week] = (lo + hi) / 2 - theta[m]
    counts = {m: len(v) for m, v in resid.items()}
    for threshold in (26, 8, 2):
        pool = [m for m in resid if counts[m] >= threshold]
        if pool:
            break
    dev = {m: {k: e - np.mean(list(v.values())) for k, e in v.items()} for m, v in resid.items()}
    sum_sq = sum(d ** 2 for m in pool for d in dev[m].values())
    sigma = math.sqrt(sum_sq / sum(counts[m] - 1 for m in pool))
    cross = sum(dev[m][k] * dev[m][k + 1] for m in pool for k in dev[m] if k + 1 in dev[m])
    return sigma, max(cross / sum_sq, 0.0), counts


def test_floor_follows_an_independent_computation_with_linked_weeks_and_gaps():
    rng = np.random.default_rng(11)
    weeks, models = 70, 5
    drift = np.zeros((weeks, models))
    for k in range(1, weeks):
        drift[k] = 0.7 * drift[k - 1] + 0.3 * rng.normal(size=models)
    drift[:, 4] = 2.0 * rng.normal(size=weeks)  # a noisy short record: outside the pool of 26 weeks
    tasks = synthetic(rng, np.array([0.4, 0.0, -0.1, -0.3, 0.2]), weeks, series=5, miss=0.0, drift=drift)
    tasks = without(tasks, 3, {k for k in range(weeks) if k % 3 == 1})
    tasks = without(tasks, 4, set(range(weeks - 12)))
    rows, method = rate(tasks, day_of=monday, n_boot=0)
    sigma, rho, counts = reference_floor(tasks, monday, {m: r.theta for m, r in rows.items()})
    assert method["sigma_week"] == pytest.approx(sigma, rel=1e-9)
    assert method["rho"] == pytest.approx(rho, rel=1e-9) and 0.3 < rho < 0.95
    assert (method["pool_min_weeks"], method["pool_size"]) == (26, 4)
    assert all(rows[m].weeks == counts[m] for m in rows)
    assert rows[3].weeks < rows[0].weeks and rows[4].weeks == 12
    for m, row in rows.items():
        half = 1.96 * sigma * math.sqrt(f_closed_form(counts[m], rho) / counts[m])
        assert row.floor == pytest.approx(POINTS_PER_UNIT * half, rel=1e-9)
        assert row.ci_lower == pytest.approx(row.score - row.floor)
        assert row.ci_upper == pytest.approx(row.score + row.floor)
        assert row.boot_lower is None and row.boot_upper is None


def test_rho_counts_only_consecutive_calendar_weeks():
    rng = np.random.default_rng(12)
    drift = np.zeros((40, 3))
    for k in range(1, 40):
        drift[k] = 0.8 * drift[k - 1] + 0.3 * rng.normal(size=3)
    tasks = synthetic(rng, np.array([0.2, 0.0, -0.2]), 40, series=4, miss=0.0, drift=drift)
    _, linked = rate(tasks, day_of=monday, n_boot=0)
    _, spaced = rate(tasks, day_of=lambda r: 4 + 14 * r, n_boot=0)
    assert linked["rho"] > 0.3 and spaced["rho"] == 0.0


def test_a_negative_lag_correlation_is_set_to_zero():
    rng = np.random.default_rng(13)
    drift = np.zeros((40, 3))
    drift[::2] = 0.5
    drift += 0.05 * rng.normal(size=(40, 3))
    tasks = synthetic(rng, np.array([0.2, 0.0, -0.2]), 40, series=4, miss=0.0, drift=drift)
    _, method = rate(tasks, day_of=monday, n_boot=0)
    assert method["rho"] == 0.0 and method["sigma_week"] > 0


# ---------------------------------------------------------------- bootstrap blocks


def test_block_counts_cover_consecutive_days_around_the_circle():
    counts = arena._block_counts(np.array([[0, 100], [130, 130]]), 140)
    assert counts.shape == (2, 140) and counts.sum(axis=1).tolist() == [56, 56]
    assert counts[0, :28].tolist() == [1] * 28 and counts[0, 100:128].tolist() == [1] * 28
    assert counts[0, 28:100].sum() == 0 and counts[0, 128:].sum() == 0
    assert counts[1, 130:].tolist() == [2] * 10 and counts[1, :18].tolist() == [2] * 18
    assert counts[1, 18:130].sum() == 0


def test_days_in_one_block_are_drawn_together_far_days_are_not():
    starts = np.random.default_rng(0).integers(0, 140, size=(4000, 5))
    counts = arena._block_counts(starts, 140)
    assert np.mean(counts[:, 10] == counts[:, 13]) > 0.75
    assert np.mean(counts[:, 10] == counts[:, 80]) < 0.5
    assert np.corrcoef(counts[:, 10], counts[:, 13])[0, 1] > 0.8
    assert np.corrcoef(counts[:, 10], counts[:, 80])[0, 1] < 0.1


def test_rounds_inside_one_block_are_always_drawn_together():
    tasks = TOY[:6]
    rows, method = rate(tasks, day_of=lambda r: 10 * (r - 1))  # 21 days: one block covers the circle
    assert method["circle_days"] == 28
    for row in rows.values():
        assert row.boot_lower == pytest.approx(row.score, abs=1e-6)
        assert row.boot_upper == pytest.approx(row.score, abs=1e-6)


def test_circle_is_the_scope_days_rounded_up_to_whole_blocks():
    assert rate(TOY)[1]["circle_days"] == 140  # 113 days
    assert rate(TOY, day_of=lambda r: 7 * (r - 1))[1]["circle_days"] == 56  # 29 days
    assert rate(TOY, day_of=lambda r: 7 * (r - 1) - (r == 5))[1]["circle_days"] == 28  # 28 days
    assert rate(TOY, day_of=lambda r: 5 * (r - 1) + 20_000)[1]["circle_days"] == 28  # 21 days


def test_the_day_range_is_the_one_of_the_kept_tasks():
    pairs = build(TOY)
    assert (pairs.first_day, pairs.last_day) == (0, 112)
    pairs = build(TOY + [(9, LOAD, {A: 1.0})])  # a task with one model is not kept
    assert (pairs.first_day, pairs.last_day) == (0, 112)


def test_refits_without_an_anchor_model_are_dropped_and_presence_follows_the_draws():
    # Y is the only model with two weeks and so the anchor set; X and Z each play one week against Y
    tasks = [(1, s, {1: 1.0 + s, 2: 2.0}) for s in (1, 2)] + [(2, s, {2: 2.0, 3: 1.5 + s}) for s in (1, 2)]
    day_of = {1: 0, 2: 60}.get
    rows, method = rate(tasks, day_of=day_of, n_boot=200, seed=3)
    circle = method["circle_days"]
    assert circle == 84
    starts = np.random.default_rng(3).integers(0, circle, size=(200, circle // 28))
    counts = arena._block_counts(starts, circle)
    x_drawn, z_drawn = counts[:, 0] > 0, counts[:, 60] > 0
    assert method["anchor_size"] == 1 and rows[2].weeks == 2
    assert method["refits_unanchored"] == int(np.sum(~x_drawn & ~z_drawn)) > 0
    assert rows[1].boot_refits == int(x_drawn.sum()) and rows[3].boot_refits == int(z_drawn.sum())
    assert rows[2].boot_refits == int(np.sum(x_drawn | z_drawn))


def test_refits_whose_models_split_into_groups_still_converge():
    # two pairs of models are joined only by a bridge on day 120; a resample without the bridge splits them
    tasks = [(1, s, {1: 1.0 + s, 2: 2.0}) for s in (1, 2)]
    tasks += [(2, s, {3: 1.0, 4: 2.0 + s}) for s in (1, 2)]
    tasks += [(3, s, {2: 1.0 + s, 3: 2.0}) for s in (1, 2)]
    rows, method = rate(tasks, day_of={1: 0, 2: 60, 3: 120}.get, seed=1)
    assert method["refits_split"] > 0 and method["refit_max_grad"] <= arena.GTOL
    for row in rows.values():
        assert np.isfinite([row.score, row.boot_lower, row.boot_upper, row.ci_lower, row.ci_upper]).all()


def test_bootstrap_bounds_need_20_refits_and_the_floor_stands_alone_without_them():
    rows, _ = rate(TOY, n_boot=arena.MIN_PRESENT_REFITS - 1)
    for row in rows.values():
        assert row.boot_lower is None and row.boot_upper is None
        assert row.ci_lower == pytest.approx(row.score - row.floor)
        assert row.ci_upper == pytest.approx(row.score + row.floor)
    tasks = [(r, s, {A: 1.0 + 0.1 * r, B: 2.0, C: 1.5} if r == 5 else {A: 1.0 + 0.1 * r, B: 2.0})
             for r in range(1, 6) for s in (1, 2)]
    seen = set()
    for seed in range(10):
        rows, _ = rate(tasks, day_of=toy_day, n_boot=24, seed=seed)
        for row in rows.values():
            assert (row.boot_lower is not None) == (row.boot_refits >= arena.MIN_PRESENT_REFITS)
            seen.add((row.model_id, row.boot_lower is not None))
    assert {(A, True), (B, True), (C, False)} <= seen


# ---------------------------------------------------------------- rank ranges


def test_rank_ranges_from_intervals():
    lower, upper = np.array([10.0, 9.0, 5.0, 8.0]), np.array([12.0, 11.0, 6.0, 13.0])
    best, worst = arena._rank_ranges(lower, upper)
    assert best.tolist() == [1, 1, 4, 1] and worst.tolist() == [3, 3, 4, 3]
    lower, upper = np.array([10.0, 11.0, 0.0]), np.array([12.0, 13.0, 1.0])  # two overlap, one below both
    best, worst = arena._rank_ranges(lower, upper)
    assert best.tolist() == [1, 1, 3] and worst.tolist() == [2, 2, 3]
    lower, upper = np.array([10.0, 11.0, 5.0]), np.array([12.0, 13.0, 10.5])  # the third clears only one
    best, worst = arena._rank_ranges(lower, upper)
    assert best.tolist() == [1, 1, 2] and worst.tolist() == [3, 2, 3]
    best, worst = arena._rank_ranges(np.array([10.0, 12.0]), np.array([12.0, 14.0]))  # touching bounds
    assert best.tolist() == [1, 1] and worst.tolist() == [2, 2]


def test_a_clearly_leading_model_gets_a_single_rank():
    rng = np.random.default_rng(6)
    tasks = synthetic(rng, np.array([1.5, 0.02, 0.0, 0.01, -1.5]), weeks=60, series=8, miss=0.2, noise=0.3)
    rows, _ = rate(tasks, day_of=monday)
    assert (rows[0].rank_best, rows[0].rank_worst) == (1, 1)
    assert (rows[4].rank_best, rows[4].rank_worst) == (5, 5)
    for m in (1, 2, 3):
        assert (rows[m].rank_best, rows[m].rank_worst) == (2, 4)
    assert all(not r.provisional and not r.low_coverage for r in rows.values())


def test_provisional_models_neither_rank_nor_push_others_down():
    rng = np.random.default_rng(7)
    tasks = synthetic(rng, np.array([0.3, 0.0, -0.3, 2.0]), weeks=40, series=6, miss=0.1, noise=0.3)
    rows, _ = rate(without(tasks, 3, set(range(38))), day_of=monday)
    assert rows[3].weeks == 2 and rows[3].provisional
    assert rows[3].score > max(rows[m].score for m in (0, 1, 2))
    assert (rows[3].rank_best, rows[3].rank_worst) == (None, None)
    for m in (0, 1, 2):
        assert 1 <= rows[m].rank_best <= rows[m].rank_worst <= 3


def test_two_poor_rounds_rank_below_sixty_two_slightly_better_ones():
    rng = np.random.default_rng(18)
    tasks = synthetic(rng, np.array([0.05, 0.0, 0.0, -0.8]), weeks=62, series=5, miss=0.0, noise=0.3)
    rows, _ = rate(without(tasks, 3, set(range(60))), day_of=monday)
    assert rows[3].weeks == 2 and rows[0].weeks == 62
    assert rows[3].score < min(rows[m].score for m in (0, 1, 2))
    assert rows[3].provisional and rows[3].rank_best is None


def test_intervals_widen_as_weeks_get_fewer():
    rng = np.random.default_rng(19)
    tasks = synthetic(rng, np.array([0.2, 0.1, 0.0, -0.1, -0.2]), weeks=60, series=5, miss=0.0, noise=0.4)
    rows, _ = rate(without(without(tasks, 3, set(range(59))), 4, set(range(57))), day_of=monday)
    assert [rows[m].weeks for m in (0, 4, 3)] == [60, 3, 1]
    width = {m: rows[m].ci_upper - rows[m].ci_lower for m in rows}
    assert width[3] > width[4] > width[0]


def test_four_weeks_are_ranked_three_are_provisional():
    rng = np.random.default_rng(8)
    tasks = synthetic(rng, np.array([0.3, 0.0, -0.3, 0.1, 0.2]), weeks=30, series=6, miss=0.0, noise=0.3)
    rows, _ = rate(without(without(tasks, 3, set(range(26))), 4, set(range(27))), day_of=monday)
    assert (rows[3].weeks, rows[4].weeks) == (4, 3)
    assert not rows[3].provisional and rows[3].rank_best is not None
    assert rows[4].provisional and rows[4].rank_best is None


# ---------------------------------------------------------------- guards


def test_both_errors_zero_is_a_draw():
    pairs = build([(1, LOAD, {A: 0.0, B: 0.0})])
    assert (pairs.n_pairs, pairs.w.tolist(), pairs.wy.tolist()) == (1, [1.0], [0.5])


def test_exactly_one_zero_error_skips_the_pair_and_reports_it():
    pairs = build([(7, LOAD, {A: 0.0, B: 1.0, C: 2.0})])
    assert pairs.skipped_zero == ((7, LOAD, A, B), (7, LOAD, A, C))
    assert pairs.n_pairs == 1 and (pairs.i.tolist(), pairs.j.tolist()) == ([B], [C])
    assert pairs.w.tolist() == [0.5]  # k = 3 stays, the weight is not recomputed
    assert pairs.wy.tolist() == pytest.approx([0.5 * 2 / 3])


def test_unequal_point_counts_skip_only_that_pair():
    pairs = build([(1, LOAD, {A: (1.0, 24), B: (2.0, 24), C: (4.0, 12)})])
    assert pairs.skipped_points == 2 and pairs.n_pairs == 1
    assert (pairs.i.tolist(), pairs.j.tolist(), pairs.w.tolist()) == ([A], [B], [0.5])


def test_unequal_points_are_checked_before_zero_errors():
    pairs = build([(1, LOAD, {A: (0.0, 24), B: (2.0, 12)})])
    assert pairs.skipped_points == 1 and pairs.skipped_zero == () and pairs.n_pairs == 0
    assert len(pairs.day) == 0 and (pairs.first_day, pairs.last_day) == (0, 0)


def test_tasks_with_a_single_model_carry_no_information():
    pairs = build([(1, LOAD, {A: 1.0}), (1, BIKES, {A: 1.0, B: 2.0})])
    assert pairs.n_pairs == 1 and pairs.w.tolist() == [1.0]


def test_pair_sums_reject_inconsistent_input():
    with pytest.raises(ValueError):
        arena.pair_sums([1, 1], [1, 1], [A, A], [1.0, 2.0], [1, 1], [0, 0])  # duplicate row
    with pytest.raises(ValueError):
        arena.pair_sums([1, 1], [1, 1], [A, B], [1.0, 2.0], [1, 1], [0, 1])  # one task on two days
    with pytest.raises(ValueError):
        arena.pair_sums([1, 1], [1, 1], [A, B], [1.0, -2.0], [1, 1], [0, 0])  # negative error
    with pytest.raises(ValueError):
        arena.pair_sums([1, 1], [1, 1], [A, B], [1.0, float("nan")], [1, 1], [0, 0])
    with pytest.raises(ValueError):
        arena.pair_sums([1], [1, 1], [A], [1.0], [1], [0])  # lengths differ


def test_empty_input_gives_empty_pair_sums_and_null_rows():
    pairs = arena.pair_sums([], [], [], [], [], [])
    assert pairs.n_pairs == 0 and len(pairs.day) == 0 and pairs.first_day is None
    rating = arena.rate_scope(pairs, [A, B], all_covered([A, B]))
    assert all(r.unranked_disconnected and r.score is None for r in rating.models)


def test_pair_sums_match_a_loop_over_the_tasks_in_every_chunking(monkeypatch):
    rng = np.random.default_rng(13)
    tasks = []
    for rnd in range(15):
        for s in range(6):
            present = [m for m in range(7) if rng.random() < 0.6]
            tasks.append((rnd, s, {m: (float(rng.choice([0.0, 0.0, 1.0, 2.5, 7.0, 0.3])),
                                       int(rng.choice([24, 24, 24, 12]))) for m in present}))
    expected, counts = defaultdict(lambda: [0.0, 0.0]), [0, 0, 0]
    for rnd, _, errs in tasks:
        if len(errs) < 2:
            continue
        w = 1.0 / (len(errs) - 1)
        for a, b in combinations(sorted(errs), 2):
            (ea, na), (eb, nb) = errs[a], errs[b]
            if na != nb:
                counts[0] += 1
            elif (ea == 0) != (eb == 0):
                counts[1] += 1
            else:
                y = 0.5 if ea == eb == 0 else eb / (ea + eb)
                expected[(3 * rnd, a, b)][0] += w
                expected[(3 * rnd, a, b)][1] += w * y
                counts[2] += 1
    for chunk in (10 ** 6, 40, 3):
        monkeypatch.setattr(arena, "_CHUNK_PAIRS", chunk)
        pairs = build(tasks, day_of=lambda r: 3 * r)
        got = {(d, i, j): (w, wy) for d, i, j, w, wy in
               zip(pairs.day, pairs.i, pairs.j, pairs.w, pairs.wy, strict=True)}
        assert got.keys() == expected.keys()
        for key, value in expected.items():
            assert got[key] == pytest.approx(value, abs=1e-12)
        assert (pairs.skipped_points, len(pairs.skipped_zero), pairs.n_pairs) == tuple(counts)


def test_combining_definitions_equals_one_call_on_all_rows():
    rng = np.random.default_rng(14)
    tasks = synthetic(rng, np.array([0.3, 0.0, -0.3, 0.1]), weeks=10, series=6, miss=0.3, noise=0.5)
    whole = build(tasks, monday)
    joined = arena.combine_pair_sums(build([t for t in tasks if t[1] % 3 == k], monday) for k in range(3))
    assert ((joined.n_pairs, joined.first_day, joined.last_day)
            == (whole.n_pairs, whole.first_day, whole.last_day))
    for field in ("day", "i", "j"):
        assert getattr(joined, field).tolist() == getattr(whole, field).tolist()
    for field in ("w", "wy"):
        assert getattr(joined, field) == pytest.approx(getattr(whole, field), abs=1e-12)
    with pytest.raises(ValueError):
        arena.combine_pair_sums([whole, build(tasks, monday, beta=2.0)])
    assert arena.combine_pair_sums([]).n_pairs == 0


# ---------------------------------------------------------------- connectivity


def test_models_outside_the_largest_group_get_null_rows(caplog):
    with caplog.at_level(logging.WARNING, logger="app.services.arena_score"):
        rows, method = rate(TOY + [(6, LOAD, {5: 50, 6: 100})], scope="point/definition/7")
    assert [round(rows[m].score) for m in (A, B, C)] == [996, 976, 1028]
    for m in (5, 6):
        row = rows[m]
        assert row.unranked_disconnected and not row.provisional
        assert (row.theta, row.score, row.ci_lower, row.ci_upper) == (None,) * 4
        assert (row.rank_best, row.rank_worst) == (None, None)
        assert row.weeks == 1 and row.evidence == 1.0
    assert method["n_fitted"] == 3 and method["n_models"] == 5
    assert "point/definition/7" in caplog.text and "[5, 6]" in caplog.text


def test_a_model_without_any_kept_pair_gets_a_null_row():
    rows, _ = rate(TOY + [(5, 3, {9: 1.0})])
    assert rows[9].unranked_disconnected and rows[9].score is None
    assert rows[9].weeks == 0 and rows[9].evidence == 0.0
    assert round(rows[C].score) == 1028


def test_the_larger_group_wins_and_ties_go_to_more_evidence():
    big = [(1, s, {1: 1.0 + s, 2: 2.0, 3: 3.0}) for s in range(3)]
    two = [(2, s, {7: 1.0, 8: 2.0 + 0.1 * s}) for s in range(10)]
    rows, _ = rate(big + two, day_of=lambda r: 28 * r)
    assert rows[1].score is not None and rows[7].score is None
    few = [(1, s, {1: 1.0, 2: 2.0 + s}) for s in range(2)]
    many = [(2, s, {5: 1.0, 6: 2.0 + s}) for s in range(5)]
    rows, _ = rate(few + many, day_of=lambda r: 28 * r)
    assert rows[5].score is not None and rows[1].score is None


# ---------------------------------------------------------------- coverage rule


def plays(definition, rounds, series, model):
    return [(definition, r, r, s, model) for r in rounds for s in series]


def coverage_of(rows):
    """Run the rule on rows (definition, round, registration start, series, model)."""
    tasks, own, start = defaultdict(set), defaultdict(int), {}
    for d, r, t, series, model in rows:
        tasks[(d, r)].add(series)
        own[(d, r, model)] += 1
        start[(d, r)] = t
    return arena.coverage([(d, r, start[(d, r)], len(s)) for (d, r), s in tasks.items()],
                          [(d, r, m, n) for (d, r, m), n in own.items()])


def test_coverage_counts_tasks_since_the_first_kept_row():
    rows = plays(1, range(1, 11), (1, 2), "full")                                   # from round 1
    rows += plays(1, range(3, 11), (1, 2), "late")                                  # joins in round 3
    rows += plays(1, (3, 5, 7, 9), (1, 2), "half-series") + plays(1, (4, 6, 8, 10), (1,), "half-series")
    rows += plays(1, (3,), (1, 2), "sparse") + plays(1, (6, 9), (1, 2), "sparse")   # 6 of 16 tasks
    cov = coverage_of(rows)
    assert cov["full"] == Coverage(True, 1.0) and cov["late"] == Coverage(True, 1.0)
    assert cov["half-series"].value == pytest.approx(12 / 16) and cov["half-series"].covered
    assert cov["sparse"].value == pytest.approx(6 / 16) and not cov["sparse"].covered


def test_coverage_threshold_is_inclusive():
    rows = plays(1, range(1, 5), (1, 2), "all") + plays(1, range(1, 5), (1,), "half")
    rows += plays(1, (1, 2, 3), (1,), "short")
    cov = coverage_of(rows)
    assert cov["half"] == Coverage(True, 0.5)
    assert cov["short"].value == pytest.approx(3 / 8) and not cov["short"].covered


def test_a_task_counts_once_anyone_has_a_kept_row_even_alone():
    rows = plays(1, range(1, 5), (1, 2), "alone-in-series-2") + plays(1, range(1, 5), (1,), "other")
    cov = coverage_of(rows)
    assert cov["other"].value == pytest.approx(4 / 8) and cov["other"].covered


def test_skipping_single_series_costs_coverage_like_skipping_rounds():
    # definition 1 has series {1, 2, 3}, definition 2 has {2, 3, 4}: overlapping, unequal sets
    everyone = plays(1, range(1, 5), (1, 2, 3), "x") + plays(2, range(11, 15), (2, 3, 4), "x")
    rows = list(everyone)
    for model in ("skips-series", "skips-rounds", "skips-more-series"):
        rows += plays(2, range(11, 15), (2, 3, 4), model)
    rows += plays(1, range(1, 5), (1, 2), "skips-series")           # leaves out series 3: 8 of 12
    rows += plays(1, (1, 2), (1, 2, 3), "skips-rounds")             # leaves out rounds 3, 4: 6 of 12
    rows += plays(1, range(1, 5), (1,), "skips-more-series")        # 4 of 12
    cov = coverage_of(rows)
    assert cov["skips-series"].value == pytest.approx(8 / 12) and cov["skips-series"].covered
    assert cov["skips-rounds"].value == pytest.approx(6 / 12) and cov["skips-rounds"].covered
    assert cov["skips-more-series"].value == pytest.approx(4 / 12) and not cov["skips-more-series"].covered


def test_coverage_is_the_minimum_over_the_definitions_of_the_scope():
    rows = plays(1, range(1, 5), (1,), "x") + plays(2, range(11, 15), (1,), "x")
    rows += plays(1, range(1, 5), (1,), "y") + plays(2, range(11, 14), (1,), "y")
    rows += plays(1, range(1, 5), (1,), "z") + plays(2, (11,), (1,), "z")
    cov = coverage_of(rows)
    assert cov["y"].value == pytest.approx(3 / 4) and cov["y"].covered
    assert cov["z"].value == pytest.approx(1 / 4) and not cov["z"].covered


def test_a_definition_without_a_round_since_joining_is_unseen_and_fails_the_rule():
    rows = plays(2, (1, 2), (1,), "x")                          # definition 2 ran before
    rows += plays(1, range(3, 7), (1,), "x") + plays(1, range(3, 7), (1,), "late")
    cov = coverage_of(rows)
    assert cov["late"].covered is False and cov["late"].value == pytest.approx(1.0)
    assert cov["x"].covered is True and cov["x"].value == pytest.approx(1.0)


def test_coverage_ignores_rounds_before_the_first_kept_row():
    rows = plays(1, range(1, 9), (1, 2), "x") + plays(1, range(6, 9), (1, 2), "newcomer")
    assert coverage_of(rows)["newcomer"] == Coverage(True, 1.0)


def test_rounds_without_tasks_count_as_seen_but_not_in_the_ratio():
    cov = arena.coverage([(1, 1, 0, 2), (2, 2, 1, 0)], [(1, 1, "m", 2)])
    assert cov["m"] == Coverage(True, 1.0)
    assert arena.coverage([], []) == {}
    with pytest.raises(ValueError):
        arena.coverage([(1, 1, 0, 2)], [(1, 9, "m", 1)])


def test_toy_coverage():
    rows = [(1, r, 28 * (r - 1), s, m) for r, s, errs in TOY for m in errs]
    assert coverage_of(rows) == {A: Coverage(True, 1.0), B: Coverage(True, 1.0), C: Coverage(True, 1.0)}


def test_a_low_coverage_model_is_flagged_unranked_and_stays_in_the_fit():
    rng = np.random.default_rng(15)
    tasks = synthetic(rng, np.array([0.4, 0.0, -0.3, 0.2]), weeks=40, series=6, miss=0.0, noise=0.4)
    models = [0, 1, 2, 3]
    ok = all_covered(models)
    low = {**ok, 3: Coverage(False, 0.4)}
    pairs = build(tasks, monday)
    base = {r.model_id: r for r in arena.rate_scope(pairs, models, ok).models}
    flagged = {r.model_id: r for r in arena.rate_scope(pairs, models, low).models}
    assert flagged[3].low_coverage and flagged[3].coverage == 0.4
    assert (flagged[3].rank_best, flagged[3].rank_worst) == (None, None)
    assert flagged[3].score == base[3].score and flagged[3].ci_lower == base[3].ci_lower
    for m in (0, 1, 2):
        assert flagged[m].score == base[m].score
        assert flagged[m].rank_worst <= 3 and not flagged[m].low_coverage
    assert base[3].rank_best is not None and not base[3].low_coverage


def test_rate_scope_checks_its_inputs():
    pairs = build(TOY)
    with pytest.raises(ValueError):
        arena.rate_scope(pairs, [A, B], all_covered([A, B, C]))
    with pytest.raises(ValueError):
        arena.rate_scope(pairs, [A, B, C], all_covered([A, B]))
