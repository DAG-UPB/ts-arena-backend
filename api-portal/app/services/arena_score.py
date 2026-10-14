"""Arena Score rating core: graded Bradley-Terry fit on per-series error ratios.

Pure functions on numpy arrays, no database access. For one scope and metric:

    pair_sums()          kept rows of a definition -> pair sums per (day, model pair)
    combine_pair_sums()  join the definitions of a scope
    coverage()           coverage rule from per-round task counts
    rate_scope()         fit, block bootstrap, weekly floor, rank ranges

A task is one (round, series). The day and week of a round are those of its registration
start: `day` is an integer UTC day number (days since 1970-01-01), weeks start on Monday.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

POINTS_PER_UNIT = 400.0 / math.log(10.0)  # 174 points = factor e in error
TAU = 0.5  # coverage rule: share of a definition's tasks since a model joined
PROVISIONAL_WEEKS = 4  # fewer weeks played: score and CI shown, no rank
BLOCK_DAYS = 28  # bootstrap block length
N_BOOT = 200  # refits
SEED = 0  # fixed seed of the block draws
LONG_RECORD_WEEKS = 26  # pool for sigma_week and anchor set of the bootstrap
POOL_FALLBACK_WEEKS = (8, 2)  # pool thresholds when nobody has LONG_RECORD_WEEKS
MIN_PRESENT_REFITS = 20  # fewer refits with a model: no bootstrap bounds
GTOL = 1e-6  # stop: max |gradient of the negative log-likelihood|
MAX_ITER = 1000  # more iterations: the fit fails

_Z95 = 1.96
_CI_PERCENTILES = (2.5, 97.5)
_ROOT_BRACKET = 30.0
_CHUNK_PAIRS = 1_000_000


class ArenaScoreFitError(RuntimeError):
    """A fit or root search did not converge."""


@dataclass(frozen=True)
class PairSums:
    """Sums of w and w*y per (day, model pair); model ids i < j, y from i's side."""

    day: np.ndarray
    i: np.ndarray
    j: np.ndarray
    w: np.ndarray
    wy: np.ndarray
    n_pairs: int = 0  # kept pairs before summing
    skipped_points: int = 0  # pairs skipped for unequal point counts
    skipped_zero: Tuple[Tuple, ...] = ()  # (round, series, model, model), exactly one error zero
    first_day: Optional[int] = None  # day range of the kept tasks
    last_day: Optional[int] = None
    beta: float = 1.0


@dataclass(frozen=True)
class Coverage:
    covered: bool
    value: Optional[float]  # min over definitions of n_i / N_i


@dataclass(frozen=True)
class ModelRating:
    model_id: Any
    theta: Optional[float]
    score: Optional[float]
    ci_lower: Optional[float]
    ci_upper: Optional[float]
    boot_lower: Optional[float]  # bootstrap bounds alone, display scale
    boot_upper: Optional[float]
    boot_refits: int  # refits the model was present in
    floor: Optional[float]  # half-width of the weekly floor, points
    weeks: int
    evidence: float
    coverage: Optional[float]
    provisional: bool
    low_coverage: bool
    unranked_disconnected: bool
    rank_best: Optional[int]
    rank_worst: Optional[int]


@dataclass(frozen=True)
class ScopeRating:
    models: List[ModelRating]
    method: Dict[str, Any]


def _empty_pair_sums(**kwargs) -> PairSums:
    ints, floats = np.empty(0, np.int64), np.empty(0, np.float64)
    return PairSums(ints, ints, ints, floats, floats, **kwargs)


def pair_sums(round_id, series_id, model_id, err, n_points, day, beta: float = 1.0) -> PairSums:
    """Pairwise outcomes of kept rows, summed per (day, model pair).

    Rows of a task share `day`. A task lies in one definition, so definitions can be summed
    separately and joined with `combine_pair_sums`.
    """
    round_id, series_id, model_id = np.asarray(round_id), np.asarray(series_id), np.asarray(model_id)
    err = np.asarray(err, dtype=np.float64)
    n_points = np.asarray(n_points)
    day = np.asarray(day, dtype=np.int64)
    n_rows = err.shape[0]
    if any(a.shape != (n_rows,) for a in (round_id, series_id, model_id, n_points, day)):
        raise ValueError("input arrays must be one-dimensional and of equal length")
    if beta <= 0:
        raise ValueError("beta must be positive")
    if n_rows == 0:
        return _empty_pair_sums(beta=beta)
    if not np.all(np.isfinite(err) & (err >= 0)):
        raise ValueError("err must be finite and >= 0")

    order = np.lexsort((model_id, series_id, round_id))
    r, s, m = round_id[order], series_id[order], model_id[order]
    e, n, d = err[order], n_points[order], day[order]
    if beta != 1.0:
        e = e ** beta
        if not np.all(np.isfinite(e)):
            raise ValueError("err ** beta overflows")
    new_task = np.r_[True, (r[1:] != r[:-1]) | (s[1:] != s[:-1])]
    if np.any(~new_task[1:] & (m[1:] == m[:-1])):
        raise ValueError("duplicate (round, series, model) row")
    starts = np.flatnonzero(new_task)
    sizes = np.diff(np.r_[starts, n_rows])
    if np.any(d != np.repeat(d[starts], sizes)):
        raise ValueError("rows of one task must share the same day")
    kept = sizes >= 2
    if not kept.any():
        return _empty_pair_sums(beta=beta)

    day_codes, task_day = np.unique(d[starts], return_inverse=True)
    model_codes, model_code = np.unique(m, return_inverse=True)
    n_models = len(model_codes)
    if len(day_codes) * n_models * n_models >= 2 ** 62:
        raise ValueError("too many days and models to index")

    keys, key_w, key_wy = [], [], []
    n_pairs = skipped_points = 0
    zero: List[Tuple] = []
    for k in np.unique(sizes[kept]):
        tasks = np.flatnonzero(sizes == k)
        iu, ju = np.triu_indices(k, 1)
        w = 1.0 / (k - 1)
        per_chunk = max(1, _CHUNK_PAIRS // len(iu))
        for c0 in range(0, len(tasks), per_chunk):
            t = tasks[c0:c0 + per_chunk]
            pi = (starts[t][:, None] + iu).ravel()
            pj = (starts[t][:, None] + ju).ravel()
            ei, ej = e[pi], e[pj]
            equal = n[pi] == n[pj]
            zi, zj = ei == 0.0, ej == 0.0
            one_zero = equal & (zi != zj)
            keep = equal & ~one_zero
            skipped_points += int(np.count_nonzero(~equal))
            if one_zero.any():
                a, b = pi[one_zero], pj[one_zero]
                zero.extend(zip(r[a].tolist(), s[a].tolist(), m[a].tolist(), m[b].tolist()))
            both = zi & zj
            y = np.where(both, 0.5, ej / np.where(both, 1.0, ei + ej))[keep]
            if not len(y):
                continue
            n_pairs += len(y)
            pi, pj = pi[keep], pj[keep]
            tday = np.repeat(task_day[t], len(iu))[keep]
            key, inv = np.unique(
                (tday * n_models + model_code[pi]) * n_models + model_code[pj], return_inverse=True)
            keys.append(key)
            key_w.append(np.bincount(inv, minlength=len(key)) * w)
            key_wy.append(np.bincount(inv, weights=y, minlength=len(key)) * w)

    extra = dict(skipped_points=skipped_points, skipped_zero=tuple(zero), beta=beta,
                 first_day=int(d[starts][kept].min()), last_day=int(d[starts][kept].max()))
    if not keys:
        return _empty_pair_sums(**extra)
    key, inv = np.unique(np.concatenate(keys), return_inverse=True)
    return PairSums(
        day=day_codes[key // (n_models * n_models)],
        i=model_codes[(key // n_models) % n_models],
        j=model_codes[key % n_models],
        w=np.bincount(inv, weights=np.concatenate(key_w), minlength=len(key)),
        wy=np.bincount(inv, weights=np.concatenate(key_wy), minlength=len(key)),
        n_pairs=n_pairs,
        **extra,
    )


def combine_pair_sums(parts: Iterable[PairSums]) -> PairSums:
    """Join the pair sums of several definitions, summing equal (day, model pair) rows."""
    parts = list(parts)
    if not parts:
        return _empty_pair_sums()
    if len({p.beta for p in parts}) > 1:
        raise ValueError("pair sums were built with different beta")
    day, i, j, w, wy = (np.concatenate([getattr(p, f) for p in parts])
                        for f in ("day", "i", "j", "w", "wy"))
    if len(day):
        order = np.lexsort((j, i, day))
        day, i, j, w, wy = day[order], i[order], j[order], w[order], wy[order]
        change = (day[1:] != day[:-1]) | (i[1:] != i[:-1]) | (j[1:] != j[:-1])
        head = np.flatnonzero(np.r_[True, change])
        day, i, j = day[head], i[head], j[head]
        w, wy = np.add.reduceat(w, head), np.add.reduceat(wy, head)
    firsts = [p.first_day for p in parts if p.first_day is not None]
    lasts = [p.last_day for p in parts if p.last_day is not None]
    return PairSums(
        day=day, i=i, j=j, w=w, wy=wy,
        n_pairs=sum(p.n_pairs for p in parts),
        skipped_points=sum(p.skipped_points for p in parts),
        skipped_zero=tuple(z for p in parts for z in p.skipped_zero),
        first_day=min(firsts) if firsts else None,
        last_day=max(lasts) if lasts else None,
        beta=parts[0].beta,
    )


def coverage(
    rounds: Iterable[Tuple[Any, Any, Any, int]],
    model_tasks: Iterable[Tuple[Any, Any, Any, int]],
) -> Dict[Any, Coverage]:
    """Coverage rule per model of one scope and metric.

    rounds:      (definition, round, registration_start, tasks), tasks = tasks of the round in
                 which anyone has a kept row
    model_tasks: (definition, round, model, tasks), tasks = tasks of the round in which the
                 model has a kept row
    """
    rounds = list(rounds)
    position = {(d, r): k for k, (d, r, _, _) in enumerate(rounds)}
    definitions = {d: n for n, d in enumerate(sorted({d for d, _, _, _ in rounds}))}
    r_def = np.array([definitions[d] for d, _, _, _ in rounds], dtype=np.int64)
    r_tasks = np.array([t for _, _, _, t in rounds], dtype=np.float64)
    r_rank = np.unique(np.array([s for _, _, s, _ in rounds]), return_inverse=True)[1]
    n_def = len(definitions)

    per_model: Dict[Any, Tuple[List[int], List[float]]] = {}
    for d, r, model, tasks in model_tasks:
        if tasks <= 0:
            continue
        if (d, r) not in position:
            raise ValueError(f"model_tasks refers to an unknown round: {(d, r)}")
        idx, count = per_model.setdefault(model, ([], []))
        idx.append(position[(d, r)])
        count.append(tasks)

    out: Dict[Any, Coverage] = {}
    for model, (idx, count) in per_model.items():
        idx = np.array(idx)
        since = r_rank >= r_rank[idx].min()
        seen = np.bincount(r_def[since], minlength=n_def) > 0
        total = np.bincount(r_def[since], r_tasks[since], n_def)
        own = np.bincount(r_def[idx], np.array(count, dtype=np.float64), n_def)
        counted = total > 0
        out[model] = Coverage(
            covered=bool(seen.all() and np.all(own[counted] >= TAU * total[counted])),
            value=float(np.min(own[counted] / total[counted])) if counted.any() else None,
        )
    return out


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return np.exp(-np.logaddexp(0.0, -x))


def _gradient(theta, i, j, w, wy, n):
    """Gradient of the negative log-likelihood and the pair probabilities."""
    s = _sigmoid(theta[i] - theta[j])
    g = w * s - wy
    return np.bincount(i, g, n) - np.bincount(j, g, n), s


def _newton(i, j, w, wy, n, theta0):
    """Damped Newton on the negative log-likelihood: (theta, iterations, max |gradient|)."""
    theta = np.array(theta0, dtype=np.float64)
    diag = np.arange(n)
    grad, s = _gradient(theta, i, j, w, wy, n)
    for it in range(MAX_ITER + 1):
        gmax = float(np.max(np.abs(grad), initial=0.0))
        if gmax <= GTOL:
            return theta, it, gmax
        if it == MAX_ITER or not math.isfinite(gmax):
            break
        a = w * s * (1.0 - s)
        off = np.bincount(i * n + j, a, n * n).reshape(n, n)
        hess = -(off + off.T)
        hess[diag, diag] = off.sum(axis=0) + off.sum(axis=1)
        step = -np.linalg.lstsq(hess, grad, rcond=None)[0]
        norm, t = np.linalg.norm(grad), 1.0
        while True:
            cand = theta + t * step
            cand_grad, cand_s = _gradient(cand, i, j, w, wy, n)
            if np.linalg.norm(cand_grad) <= (1.0 - 1e-4 * t) * norm or t < 1e-10:
                break
            t *= 0.5
        theta, grad, s = cand, cand_grad, cand_s
    raise ArenaScoreFitError(f"no convergence in {MAX_ITER} iterations, max |gradient| {gmax:.3g}")


def fit(i, j, w, wy, n) -> np.ndarray:
    """Centred strengths from per-pair sums (one entry per model pair, indices 0..n-1)."""
    theta = _newton(np.asarray(i), np.asarray(j), np.asarray(w, float), np.asarray(wy, float),
                    n, np.zeros(n))[0]
    return theta - theta.mean()


def _components(n: int, i: np.ndarray, j: np.ndarray) -> np.ndarray:
    """Component label per node (its smallest node index); isolated nodes label themselves."""
    reach = np.eye(n)
    reach[i, j] = reach[j, i] = 1.0
    while True:
        nxt = ((reach @ reach) > 0).astype(np.float64)
        if np.array_equal(nxt, reach):
            return reach.argmax(axis=1)
        reach = nxt


def _weeks_played(i, j, week, n) -> np.ndarray:
    span = int(week.max() - week.min()) + 1
    key = np.unique(np.concatenate([i, j]) * span + np.tile(week - week.min(), 2))
    return np.bincount(key // span, minlength=n)


def _week_strengths(i, j, week, w, wy, theta, n):
    """Strength each (model, week) implies on its own, others held at theta.

    Returns (model, week, strength - theta) sorted by model, then week.
    """
    model, other = np.concatenate([i, j]), np.concatenate([j, i])
    wk, ww, yy = np.tile(week, 2), np.tile(w, 2), np.concatenate([wy, w - wy])
    wk0 = int(wk.min())
    span = int(wk.max()) - wk0 + 1
    cell, inv = np.unique((model * span + (wk - wk0)) * n + other, return_inverse=True)
    cell_w, cell_y = np.bincount(inv, ww, len(cell)), np.bincount(inv, yy, len(cell))
    cell_theta = theta[cell % n]
    group, g = np.unique(cell // n, return_inverse=True)
    group_model, group_week = group // span, group % span + wk0

    def score(t):
        s = _sigmoid(t[g] - cell_theta)
        return (np.bincount(g, cell_y - cell_w * s, len(group)),
                -np.bincount(g, cell_w * s * (1.0 - s), len(group)))

    centre = theta[group_model]
    lo, hi = centre - _ROOT_BRACKET, centre + _ROOT_BRACKET
    if np.any(score(lo)[0] <= 0) or np.any(score(hi)[0] >= 0):
        raise ArenaScoreFitError("weekly strength outside the root bracket")
    t = centre.copy()
    for _ in range(200):
        f, df = score(t)
        lo, hi = np.where(f > 0, t, lo), np.where(f < 0, t, hi)
        with np.errstate(divide="ignore", invalid="ignore"):
            nxt = t - f / df
        nxt = np.where((nxt > lo) & (nxt < hi), nxt, 0.5 * (lo + hi))
        done = np.max(np.abs(nxt - t)) < 1e-12
        t = nxt
        if done:
            return group_model, group_week, t - centre
    raise ArenaScoreFitError("weekly strengths did not converge")


def _linkage(weeks: int, rho: float) -> float:
    k = np.arange(1, weeks)
    return float(1.0 + 2.0 * np.sum((1.0 - k / weeks) * rho ** k))


def _pooled_floor(group_model, group_week, resid, n):
    """Pooled sigma_week and lag-1 correlation, half-width per model, pool threshold and size."""
    weeks = np.bincount(group_model, minlength=n)
    for threshold in (LONG_RECORD_WEEKS, *POOL_FALLBACK_WEEKS):
        pool = weeks >= threshold
        if pool.any():
            break
    else:
        return None, None, None, None, 0
    dev = resid - (np.bincount(group_model, resid, n) / np.maximum(weeks, 1))[group_model]
    in_pool = pool[group_model]
    sum_sq = float(np.sum(dev[in_pool] ** 2))
    sigma = math.sqrt(sum_sq / float(np.sum(weeks[pool] - 1)))
    adjacent = ((group_model[1:] == group_model[:-1]) & (group_week[1:] == group_week[:-1] + 1)
                & in_pool[1:])
    cross = float(np.sum(dev[:-1][adjacent] * dev[1:][adjacent]))
    rho = max(cross / sum_sq, 0.0) if sum_sq > 0 else 0.0
    half = np.array([_Z95 * sigma * math.sqrt(_linkage(int(w), rho) / w) for w in weeks])
    return sigma, rho, half, threshold, int(pool.sum())


def _block_counts(starts: np.ndarray, circle: int) -> np.ndarray:
    """How many drawn blocks cover each day of the circle, per refit; starts is (refits, blocks)."""
    days = (starts[:, :, None] + np.arange(BLOCK_DAYS)) % circle
    days = days + circle * np.arange(len(starts))[:, None, None]
    return np.bincount(days.ravel(), minlength=circle * len(starts)).reshape(len(starts), circle)


def _bootstrap(rows, pairs, theta, anchor, n, n_boot, seed, n_days):
    """Anchored draws per refit (NaN where a model is absent) and diagnostics."""
    row_pair, row_day, row_w, row_wy = rows
    pair_i, pair_j = pairs
    circle = BLOCK_DAYS * -(-n_days // BLOCK_DAYS)
    starts = np.random.default_rng(seed).integers(0, circle, size=(n_boot, circle // BLOCK_DAYS))
    counts = _block_counts(starts, circle)
    draws = np.full((n_boot, n), np.nan)
    split = unanchored = 0
    max_grad = 0.0
    for b in range(n_boot):
        c = counts[b][row_day]
        wb = np.bincount(row_pair, c * row_w, len(pair_i))
        yb = np.bincount(row_pair, c * row_wy, len(pair_i))
        keep = wb > 0
        i, j = pair_i[keep], pair_j[keep]
        refit, _, g = _newton(i, j, wb[keep], yb[keep], n, theta)
        max_grad = max(max_grad, g)
        present = (np.bincount(i, minlength=n) + np.bincount(j, minlength=n)) > 0
        split += len(np.unique(_components(n, i, j)[present])) > 1
        used = anchor & present
        if not used.any():
            unanchored += 1
            continue
        draws[b, present] = (refit - refit[used].mean() + theta[used].mean())[present]
    return draws, dict(circle_days=int(circle), refits_split=int(split),
                       refits_unanchored=int(unanchored), refit_max_grad=float(max_grad))


def _rank_ranges(lower: np.ndarray, upper: np.ndarray):
    """Best and worst rank within the set the bounds belong to."""
    best = 1 + (lower[None, :] > upper[:, None]).sum(axis=1)
    worst = (upper[None, :] >= lower[:, None]).sum(axis=1)
    return best, worst


def rate_scope(
    pairs: PairSums,
    models: Iterable[Any],
    covered: Mapping[Any, Coverage],
    beta: float = 1.0,
    n_boot: int = N_BOOT,
    seed: int = SEED,
    scope: str = "",
) -> ScopeRating:
    """Ratings, intervals and rank ranges of one scope and metric, one row per model by id.

    models:  every model with a kept row in the scope
    covered: coverage per model, from `coverage()`
    scope:   label for log messages
    """
    if beta <= 0:
        raise ValueError("beta must be positive")
    if pairs.beta != beta:
        raise ValueError("pair sums were built with a different beta")
    model_list = sorted({m.item() if hasattr(m, "item") else m for m in models})
    ids = np.unique(np.concatenate([pairs.i, pairs.j]))
    if not set(ids.tolist()) <= set(model_list):
        raise ValueError("pairs refer to models that are not in `models`")
    if any(m not in covered for m in model_list):
        raise ValueError("`covered` needs an entry for every model")

    n_all = len(ids)
    ii, jj = np.searchsorted(ids, pairs.i), np.searchsorted(ids, pairs.j)
    week = (pairs.day + 3) // 7
    evidence = np.bincount(ii, pairs.w, n_all) + np.bincount(jj, pairs.w, n_all)
    weeks_all = _weeks_played(ii, jj, week, n_all) if n_all else np.zeros(0, np.int64)

    # the largest connected group is fitted: most models, then most evidence
    fitted = np.zeros(n_all, dtype=bool)
    if n_all:
        pair_key, pair_of_row = np.unique(ii * n_all + jj, return_inverse=True)
        pair_i, pair_j = pair_key // n_all, pair_key % n_all
        pair_w = np.bincount(pair_of_row, pairs.w, len(pair_key))
        pair_wy = np.bincount(pair_of_row, pairs.wy, len(pair_key))
        label = _components(n_all, pair_i, pair_j)
        groups = np.unique(label)
        size = np.bincount(label, minlength=n_all)[groups]
        mass = np.bincount(label, evidence, n_all)[groups]
        fitted = label == groups[np.lexsort((mass, size))[-1]]
    fitted_ids = ids[fitted].tolist()
    fitted_set = set(fitted_ids)
    left_out = [m for m in model_list if m not in fitted_set]
    if left_out:
        logger.warning("Arena Score scope %s: %d model(s) not connected to the largest group, "
                       "left out: %s", scope, len(left_out), left_out)

    n = len(fitted_ids)
    theta = np.zeros(n)
    lo_boot, hi_boot = np.full(n, np.nan), np.full(n, np.nan)
    n_present = np.zeros(n, dtype=np.int64)
    sigma = rho = half = pool_weeks = None
    pool_size = anchor_size = anchor_weeks = fit_iterations = 0
    fit_max_grad = 0.0
    diag: Dict[str, Any] = dict(circle_days=0, refits_split=0, refits_unanchored=0, refit_max_grad=0.0)
    if n:
        new = np.cumsum(fitted) - 1
        pair_in = fitted[pair_i]
        fi, fj = new[pair_i[pair_in]], new[pair_j[pair_in]]
        theta, fit_iterations, fit_max_grad = _newton(
            fi, fj, pair_w[pair_in], pair_wy[pair_in], n, np.zeros(n))
        theta = theta - theta.mean()

        row_in = fitted[ii]
        ri, rj, rweek = new[ii[row_in]], new[jj[row_in]], week[row_in]
        rw, rwy, rday = pairs.w[row_in], pairs.wy[row_in], pairs.day[row_in]
        group_model, group_week, resid = _week_strengths(ri, rj, rweek, rw, rwy, theta, n)
        sigma, rho, half, pool_weeks, pool_size = _pooled_floor(group_model, group_week, resid, n)

        weeks = weeks_all[fitted]
        anchor_weeks = min(LONG_RECORD_WEEKS, int(weeks.max()))
        anchor = weeks >= anchor_weeks
        anchor_size = int(anchor.sum())
        first = pairs.first_day if pairs.first_day is not None else int(pairs.day.min())
        last = pairs.last_day if pairs.last_day is not None else int(pairs.day.max())
        if rday.min() < first or rday.max() > last:
            raise ValueError("pair sums lie outside the day range of the scope")
        row_pair = (np.cumsum(pair_in) - 1)[pair_of_row[row_in]]
        draws, diag = _bootstrap((row_pair, rday - first, rw, rwy), (fi, fj), theta, anchor, n,
                                 n_boot, seed, last - first + 1)
        n_present = np.sum(~np.isnan(draws), axis=0)
        enough = n_present >= MIN_PRESENT_REFITS
        if enough.any():
            lo_boot[enough], hi_boot[enough] = np.nanpercentile(draws[:, enough], _CI_PERCENTILES, axis=0)

    # intervals are widened to the floor; without a floor (no sigma) there is no interval
    ci_lo = ci_hi = None
    if half is not None:
        ci_lo, ci_hi = np.fmin(lo_boot, theta - half), np.fmax(hi_boot, theta + half)
    fit_weeks = weeks_all[fitted]
    fit_covered = np.array([covered[m].covered for m in fitted_ids], dtype=bool)
    ranked = (fit_weeks >= PROVISIONAL_WEEKS) & fit_covered if half is not None else np.zeros(n, bool)
    best, worst = np.zeros(n, np.int64), np.zeros(n, np.int64)
    if ranked.any():
        best[ranked], worst[ranked] = _rank_ranges(ci_lo[ranked], ci_hi[ranked])

    def display(x: float) -> float:
        return float(1000.0 + POINTS_PER_UNIT * x / beta)

    slot = {m: k for k, m in enumerate(fitted_ids)}
    position = {m: k for k, m in enumerate(ids.tolist())}
    rows: List[ModelRating] = []
    for m in model_list:
        k = position.get(m)
        common = dict(model_id=m, coverage=covered[m].value, low_coverage=not covered[m].covered,
                      weeks=int(weeks_all[k]) if k is not None else 0,
                      evidence=float(evidence[k]) if k is not None else 0.0)
        if m not in slot:
            rows.append(ModelRating(
                **common, theta=None, score=None, ci_lower=None, ci_upper=None, boot_lower=None,
                boot_upper=None, boot_refits=0, floor=None, provisional=False,
                unranked_disconnected=True, rank_best=None, rank_worst=None))
            continue
        s = slot[m]
        has_boot = not np.isnan(lo_boot[s])
        rows.append(ModelRating(
            **common,
            theta=float(theta[s] / beta),
            score=display(theta[s]),
            ci_lower=None if ci_lo is None else display(ci_lo[s]),
            ci_upper=None if ci_hi is None else display(ci_hi[s]),
            boot_lower=display(lo_boot[s]) if has_boot else None,
            boot_upper=display(hi_boot[s]) if has_boot else None,
            boot_refits=int(n_present[s]),
            floor=None if half is None else float(POINTS_PER_UNIT * half[s] / beta),
            provisional=bool(half is None or fit_weeks[s] < PROVISIONAL_WEEKS),
            unranked_disconnected=False,
            rank_best=int(best[s]) if ranked[s] else None,
            rank_worst=int(worst[s]) if ranked[s] else None,
        ))

    method = dict(
        seed=seed, n_boot=n_boot, block_days=BLOCK_DAYS, beta=beta, tau=TAU,
        provisional_weeks=PROVISIONAL_WEEKS, long_record_weeks=LONG_RECORD_WEEKS,
        sigma_week=None if sigma is None else sigma / beta,
        sigma_week_points=None if sigma is None else POINTS_PER_UNIT * sigma / beta,
        rho=rho, pool_size=pool_size, pool_min_weeks=pool_weeks,
        anchor_size=anchor_size, anchor_min_weeks=anchor_weeks or None,
        n_models=len(model_list), n_fitted=n, n_pairs=pairs.n_pairs,
        skipped_points=pairs.skipped_points, skipped_zero=len(pairs.skipped_zero),
        fit_iterations=fit_iterations, fit_max_grad=fit_max_grad, **diag,
    )
    return ScopeRating(models=rows, method=method)
