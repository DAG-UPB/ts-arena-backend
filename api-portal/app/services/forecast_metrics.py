"""
Pure (no DB, no I/O) probabilistic forecast metric functions.

Implements **Scaled Quantile Loss (SQL)** as defined by fev-bench
(arXiv:2509.26468, Sec. 3.2), adapted to the arena scaling convention:

    ρ_q(y, ŷ)  = 2 · |(y − ŷ) · (1(y ≤ ŷ) − q)|          # quantile loss (fev form)
    a          = mean_t |y_t − naive|                     # SAME denominator as arena MASE
    SQL        = mean_q [ mean_t ρ_q(y_t, ŷ_t^{(q)}) / a ] # mean over quantiles of mean over time

The scale ``a`` is the MAE of the flat last-context-value naive over the evaluated
timestamps — identical to the arena MASE denominator (``mae_naive``), so SQL and MASE are
directly comparable on the platform. This is a deliberate deviation from fev's seasonal
scaling. Consequence: arena SQL values are not numerically comparable to
published fev-bench SQL, but the ranking methodology is.

Key identity (regression anchor): a degenerate distribution where all nine deciles equal the
point forecast scores exactly the same as the arena MASE of that point forecast, because the
deciles are symmetric about 0.5 and ``mean_q |1(y≤ŷ) − q| = 0.5``.

``forecasts.scores.mase`` is a relative MAE against persistence, not Hyndman & Koehler MASE;
``context_scale`` and ``compute_score_fields`` compute the latter for ``forecasts.forecast_scores``.

These functions are intentionally free of any database or framework dependency so they can be
unit-tested in isolation and reused by the scoring service.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

# The nine deciles the platform scores on.
QUANTILE_LEVELS: Tuple[float, ...] = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)

# Canonical wire-format key for a quantile level, e.g. "q_0.1" … "q_0.9".
_QUANTILE_KEY_RE = re.compile(r"^q_0\.[1-9]$")

# Bare decile key, e.g. "0.1" … "0.9". Several submitters (the statistical baselines
# AutoETS/AutoARIMA/Prophet/MLForecast, and the platform's own `sql_per_quantile` JSONB)
# use this form. Accepting it costs nothing and is the difference between scoring a model's
# real distribution and silently scoring a degenerate one — see backend-69.
_BARE_QUANTILE_KEY_RE = re.compile(r"^0\.[1-9]$")


def canonical_quantile_key(key: object) -> Optional[str]:
    """Return the canonical ``q_0.x`` form of a quantile key, or ``None`` if it isn't one.

    Accepts both supported forms and normalises to one:

        "q_0.1" -> "q_0.1"      (already canonical)
        "0.1"   -> "q_0.1"      (bare decile)
        anything else           -> None

    Both forms are held to the same strictness — only the nine deciles ``0.1``…``0.9``
    written exactly that way qualify. ``"0.10"``, ``"q_0.15"``, ``"median"`` do not.
    """
    if not isinstance(key, str):
        return None
    if _QUANTILE_KEY_RE.match(key):
        return key
    if _BARE_QUANTILE_KEY_RE.match(key):
        return f"q_{key}"
    return None


def quantile_loss(y_true: np.ndarray, q_pred: np.ndarray, level: float) -> np.ndarray:
    """Elementwise quantile (pinball) loss in the fev formulation.

    ``ρ_q(y, ŷ) = 2 · |(y − ŷ) · (1(y ≤ ŷ) − q)|``

    Args:
        y_true: actual values.
        q_pred: predicted values for quantile ``level``.
        level: quantile level in (0, 1).

    Returns:
        Elementwise loss array (same shape as inputs).
    """
    y_true = np.asarray(y_true, dtype=float)
    q_pred = np.asarray(q_pred, dtype=float)
    diff = y_true - q_pred
    indicator = (y_true <= q_pred).astype(float)
    return 2.0 * np.abs(diff * (indicator - level))


def naive_scale(y_true: np.ndarray, naive_value: float) -> Optional[float]:
    """Arena MASE denominator: mean |y − naive| over the evaluated timestamps.

    Returns ``None`` when the scale is zero (undefined SQL), mirroring the MASE
    ``mae_naive == 0`` edge case so the caller can store NULL.
    """
    y_true = np.asarray(y_true, dtype=float)
    scale = float(np.mean(np.abs(y_true - naive_value)))
    return scale if scale > 0 else None


def sql_score(
    y_true: np.ndarray,
    quantile_forecasts: Dict[float, np.ndarray],
    scale: Optional[float],
) -> Tuple[Optional[float], Dict[float, float]]:
    """Scaled Quantile Loss over the provided quantile levels.

    Args:
        y_true: actual values, shape (T,).
        quantile_forecasts: mapping ``level -> predicted array`` (each shape (T,)).
            Only the levels present are scored (partial sets are allowed).
        scale: the SQL scale ``a`` (arena ``mae_naive``); ``None``/0 -> undefined.

    Returns:
        ``(overall_sql, per_level)`` where ``overall_sql`` is the mean over levels of the
        per-level scaled mean loss, and ``per_level`` maps ``level -> scaled mean loss``.
        Returns ``(None, {})`` when the scale is undefined or no quantiles are given.
    """
    if scale is None or scale == 0 or not quantile_forecasts:
        return None, {}

    y_true = np.asarray(y_true, dtype=float)
    per_level: Dict[float, float] = {}
    for level in sorted(quantile_forecasts):
        losses = quantile_loss(y_true, quantile_forecasts[level], level)
        per_level[level] = float(np.mean(losses) / scale)

    overall = float(np.mean(list(per_level.values())))
    return overall, per_level


# ---------------------------------------------------------------------------
# Helpers for turning stored `probabilistic_values` JSON into scorable arrays.
# ---------------------------------------------------------------------------

def parse_probabilistic_values(pv: Optional[Dict[str, object]]) -> Dict[float, float]:
    """Parse a stored ``probabilistic_values`` dict into ``{level: value}``.

    Tolerant: both key forms (``q_0.1`` and bare ``0.1``) are accepted; keys that are
    neither, and non-finite values, are dropped. Returns ``{}`` for ``None``/empty/
    point-only forecasts.

    Reading both forms matters historically: millions of stored rows predate key
    normalisation on upload and carry the bare form. Dropping them here did not surface as
    an error — it silently routed those models into the degenerate branch of
    ``assemble_quantile_forecasts``, scoring them as if they had submitted no distribution
    at all (backend-69).

    When a dict somehow carries both forms for the same level, the canonical ``q_0.x``
    entry wins; the bare duplicate is ignored rather than racing on dict order.
    """
    if not pv:
        return {}
    out: Dict[float, float] = {}
    for key, raw in pv.items():
        canonical = canonical_quantile_key(key)
        if canonical is None:
            continue
        level = float(canonical[2:])
        # Canonical key already recorded for this level -> never let a bare duplicate
        # overwrite it (dict iteration order is otherwise the only tiebreak).
        if level in out and key != canonical:
            continue
        try:
            val = float(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if not np.isfinite(val):
            continue
        out[level] = val
    return out


def clean_probabilistic_values(
    pv: Optional[Dict[str, object]]
) -> Tuple[Optional[Dict[str, float]], List[str]]:
    """Validate/normalise a single point's ``probabilistic_values`` for upload.

    Keeps the nine deciles in **either** accepted key form and stores them under the
    canonical ``q_0.x`` key, so everything written from here on is one format regardless of
    what the submitter sent. Keys that are not deciles, and non-finite values, are dropped.
    Returns the cleaned dict and the list of dropped keys (as submitted). ``None`` or ``{}``
    pass through unchanged (point-only forecasts).

    Normalising rather than rejecting is deliberate: the bare form was previously dropped
    here, which destroyed the quantiles of every submitter using it at write time
    (backend-69). Where both forms carry the same level, the canonical one wins and the
    bare duplicate is reported as dropped.
    """
    if pv is None:
        return None, []
    if not isinstance(pv, dict) or not pv:
        return {}, []
    cleaned: Dict[str, float] = {}
    dropped: List[str] = []
    for key, raw in pv.items():
        canonical = canonical_quantile_key(key)
        ok = canonical is not None
        if ok and canonical in cleaned and key != canonical:
            # Canonical entry already present for this level — keep it, drop the duplicate.
            ok = False
        if ok:
            try:
                val = float(raw)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                ok = False
            else:
                ok = bool(np.isfinite(val))
        if ok:
            cleaned[canonical] = val  # type: ignore[index]
        else:
            dropped.append(str(key))
    return cleaned, dropped


def repair_point_quantiles(
    pv: Optional[Dict[str, float]]
) -> Tuple[Optional[Dict[str, float]], bool]:
    """Isotonic repair of one point's quantiles: sort values ascending across levels.

    Assumes keys are already cleaned (``q_0.1``…``q_0.9``). Returns the repaired dict and
    whether any crossing was fixed. No-op for fewer than two levels.
    """
    if not pv or len(pv) < 2:
        return pv, False
    levels = sorted(pv, key=lambda k: float(k[2:]))
    values = [pv[k] for k in levels]
    sorted_values = sorted(values)
    if values == sorted_values:
        return pv, False
    return {k: sorted_values[i] for i, k in enumerate(levels)}, True


def repair_crossing(
    quantile_forecasts: Dict[float, np.ndarray]
) -> Tuple[Dict[float, np.ndarray], int]:
    """Enforce monotonicity in level per timestamp (isotonic sort across levels).

    Returns the repaired mapping and the number of timestamps that had at least one
    crossing (values out of order across levels before the sort).
    """
    if not quantile_forecasts:
        return {}, 0
    levels = sorted(quantile_forecasts)
    mat = np.vstack([np.asarray(quantile_forecasts[l], dtype=float) for l in levels])
    sorted_mat = np.sort(mat, axis=0)
    crossing_count = int(np.sum(np.any(mat != sorted_mat, axis=0)))
    repaired = {level: sorted_mat[i] for i, level in enumerate(levels)}
    return repaired, crossing_count


_LEVELS = np.array(QUANTILE_LEVELS)
_LEVEL_ROW = {level: row for row, level in enumerate(QUANTILE_LEVELS)}


def _complete_deciles(column: np.ndarray) -> Tuple[np.ndarray, bool]:
    """Fill the missing deciles (NaN) of one timestamp that carries at least two.

    The submitted values are sorted across levels first. A missing decile between two submitted
    levels is interpolated linearly in the level, one beyond the outermost submitted levels is
    extrapolated along the line through the two outermost. Returns the nine values and whether
    the submitted ones had to be sorted.
    """
    known = ~np.isnan(column)
    levels, submitted = _LEVELS[known], column[known]
    values = np.sort(submitted)
    filled = np.interp(_LEVELS, levels, values)
    below, above = _LEVELS < levels[0], _LEVELS > levels[-1]
    filled[below] = values[0] + (_LEVELS[below] - levels[0]) * (values[1] - values[0]) / (levels[1] - levels[0])
    filled[above] = values[-1] + (_LEVELS[above] - levels[-1]) * (values[-1] - values[-2]) / (levels[-1] - levels[-2])
    return filled, bool(np.any(values != submitted))


def assemble_quantile_forecasts(
    predicted_values: np.ndarray,
    probabilistic_values: List[Optional[Dict[str, object]]],
) -> Tuple[Dict[float, np.ndarray], bool, int, int]:
    """Build the nine per-decile forecast arrays for a (model, series) evaluation.

    Given the aligned point forecasts and their per-timestamp ``probabilistic_values``,
    returns ``(quantile_forecasts, has_quantiles, levels_count, crossing_count)``:

    - A timestamp with at least two valid deciles is completed to all nine (see
      ``_complete_deciles``). The pinball loss is proper, so in expectation a filled-in decile
      cannot beat the model's own: sending fewer deciles does not score better.
    - A timestamp with fewer than two deciles takes the point forecast for each missing one.
    - ``has_quantiles``: whether any timestamp carried at least two valid deciles. Otherwise
      the row is point-only: a degenerate distribution, all nine deciles set to the point
      forecast, which scores identically to the arena MASE of the point forecast.
    - ``levels_count``: the number of distinct levels submitted (union across timestamps), so
      partial submissions stay visible.
    - Values are sorted across levels per timestamp to guarantee monotonicity;
      ``crossing_count`` is the number of timestamps that needed it.
    """
    predicted_values = np.asarray(predicted_values, dtype=float)
    n = len(predicted_values)
    parsed = [parse_probabilistic_values(pv) for pv in probabilistic_values]
    levels_count = len({level for p in parsed for level in p})

    if not any(len(p) >= 2 for p in parsed):
        forecasts = {level: predicted_values.copy() for level in QUANTILE_LEVELS}
        return forecasts, False, levels_count, 0

    grid = np.full((len(QUANTILE_LEVELS), n), np.nan)
    for t, p in enumerate(parsed):
        for level, value in p.items():
            grid[_LEVEL_ROW[level], t] = value

    submitted = np.sum(~np.isnan(grid), axis=0)
    crossing_count = 0
    for t in np.flatnonzero((submitted >= 2) & (submitted < len(QUANTILE_LEVELS))):
        grid[:, t], crossed = _complete_deciles(grid[:, t])
        crossing_count += crossed

    few = submitted < 2
    grid[:, few] = np.where(np.isnan(grid[:, few]), predicted_values[few], grid[:, few])

    forecasts, repaired = repair_crossing({level: grid[row] for row, level in enumerate(QUANTILE_LEVELS)})
    return forecasts, True, levels_count, crossing_count + repaired


def compute_sql_fields(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    probabilistic_values: List[Optional[Dict[str, object]]],
    mae_naive: float,
) -> Dict[str, object]:
    """Compute the 5 stored SQL columns for one (model, series) evaluation.

    This is the single source of truth for turning aligned evaluation arrays plus the
    shared MASE/SQL scale (``mae_naive``) into the columns persisted on
    ``forecasts.scores``: ``sql_score, sql_per_quantile, has_quantiles,
    quantile_levels_count, quantile_crossing_count``. Both the live scorer
    (``ScoreEvaluationService._calculate_score_for_model_series``) and the historical
    backfill script (``app/scripts/backfill_sql_scores.py``) call this function so their
    results are byte-identical by construction.

    Args:
        y_true: actual values, shape (T,).
        y_pred: point forecasts, shape (T,).
        probabilistic_values: per-timestamp ``probabilistic_values`` dicts (or None),
            aligned with ``y_true``/``y_pred``.
        mae_naive: the SQL scale (the naive MAE, or the context scale from
            ``compute_score_fields``). ``0`` means the SQL scale is undefined ->
            ``sql_score``/``sql_per_quantile`` come back ``None``.

    Returns:
        Dict with keys ``sql_score, sql_per_quantile, has_quantiles,
        quantile_levels_count, quantile_crossing_count``.
    """
    quantile_forecasts, has_quantiles, levels_count, crossing_count = (
        assemble_quantile_forecasts(y_pred, probabilistic_values)
    )
    sql_scale = mae_naive if mae_naive > 0 else None
    sql_overall, sql_per_level = sql_score(y_true, quantile_forecasts, sql_scale)
    # JSONB keys as level strings ("0.1" … "0.9")
    sql_per_quantile = (
        {f"{level:.1f}": value for level, value in sql_per_level.items()}
        if sql_per_level else None
    )
    return {
        "sql_score": sql_overall,
        "sql_per_quantile": sql_per_quantile,
        "has_quantiles": has_quantiles,
        "quantile_levels_count": levels_count,
        "quantile_crossing_count": crossing_count,
    }


# Seasonal naive lag m per bucket: one day of steps, one week for daily data.
SEASONAL_LAG_BY_BUCKET: Dict[timedelta, int] = {
    timedelta(minutes=15): 96,
    timedelta(hours=1): 24,
    timedelta(days=1): 7,
}

MASE_METHOD = "hk2006_context_daily"


def seasonal_lag(bucket: timedelta) -> int:
    return SEASONAL_LAG_BY_BUCKET[bucket]


def context_scale(
    timestamps: Sequence[datetime],
    values: Sequence[Optional[float]],
    frequency: timedelta,
    m: int,
) -> Tuple[Optional[float], int, int, int]:
    """In-sample naive scale ``mean |y_t − y_{t−m}|`` of a context, lagged by timestamp so gaps are skipped.

    Returns ``(scale, m, n_points, n_pairs)``; the scale is None when there are ``<= m`` points.
    """
    series: Dict[datetime, float] = {}
    for ts, value in zip(timestamps, values):
        if value is None:
            continue
        value = float(value)
        if np.isfinite(value):
            series[ts] = value

    n_points = len(series)
    if n_points <= m:
        return None, m, n_points, 0
    lag = m * frequency

    diffs = [abs(value - series[ts - lag]) for ts, value in series.items() if ts - lag in series]
    if not diffs:
        return None, m, n_points, 0
    return float(np.mean(diffs)), m, n_points, len(diffs)


def mase_scale_defined(scale: Optional[float]) -> bool:
    return scale is not None and bool(np.isfinite(scale)) and scale > 0


def compute_score_fields(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    probabilistic_values: List[Optional[Dict[str, object]]],
    scale: Optional[float],
    naive_value: Optional[float],
) -> Dict[str, object]:
    """Scores of one (model, series); ``mase``/``sql_score`` are None for an undefined scale. Inputs must be finite."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    errors = y_true - y_pred
    mae = float(np.mean(np.abs(errors)))
    defined = mase_scale_defined(scale)
    sql_fields = compute_sql_fields(y_true, y_pred, probabilistic_values, scale if defined else 0.0)
    return {
        "mae": mae,
        "rmse": float(np.sqrt(np.mean(errors ** 2))),
        "naive_mae": (
            float(np.mean(np.abs(y_true - naive_value))) if naive_value is not None else None
        ),
        "n_points": int(len(y_true)),
        "scale": scale,
        "mase": mae / scale if defined else None,
        **sql_fields,
    }
