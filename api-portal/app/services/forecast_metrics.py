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

These functions are intentionally free of any database or framework dependency so they can be
unit-tested in isolation and reused by the scoring service.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import numpy as np

# The nine deciles the platform scores on.
QUANTILE_LEVELS: Tuple[float, ...] = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)

# Wire-format key for a quantile level, e.g. "q_0.1" … "q_0.9".
_QUANTILE_KEY_RE = re.compile(r"^q_0\.[1-9]$")


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

    Tolerant: keys not matching ``q_0.1``…``q_0.9`` and non-finite values are dropped.
    Returns ``{}`` for ``None``/empty/point-only forecasts.
    """
    if not pv:
        return {}
    out: Dict[float, float] = {}
    for key, raw in pv.items():
        if not isinstance(key, str) or not _QUANTILE_KEY_RE.match(key):
            continue
        try:
            val = float(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if not np.isfinite(val):
            continue
        out[float(key[2:])] = val
    return out


def clean_probabilistic_values(
    pv: Optional[Dict[str, object]]
) -> Tuple[Optional[Dict[str, float]], List[str]]:
    """Validate/filter a single point's ``probabilistic_values`` for upload.

    Keeps only keys matching ``q_0.1``…``q_0.9`` with finite float values; returns the
    cleaned dict (string keys preserved for storage) and the list of dropped keys. ``None``
    or ``{}`` pass through unchanged (point-only forecasts).
    """
    if pv is None:
        return None, []
    if not isinstance(pv, dict) or not pv:
        return {}, []
    cleaned: Dict[str, float] = {}
    dropped: List[str] = []
    for key, raw in pv.items():
        ok = isinstance(key, str) and bool(_QUANTILE_KEY_RE.match(key))
        if ok:
            try:
                val = float(raw)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                ok = False
            else:
                ok = bool(np.isfinite(val))
        if ok:
            cleaned[key] = val
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


def assemble_quantile_forecasts(
    predicted_values: np.ndarray,
    probabilistic_values: List[Optional[Dict[str, object]]],
) -> Tuple[Dict[float, np.ndarray], bool, int, int]:
    """Build per-level forecast arrays for a (model, series) evaluation.

    Given the aligned point forecasts and their per-timestamp ``probabilistic_values``,
    returns ``(quantile_forecasts, has_quantiles, levels_count, crossing_count)``:

    - ``has_quantiles``: whether any timestamp carried at least one valid quantile.
    - Point-only (no quantiles anywhere): a degenerate distribution — all nine deciles set
      to the point forecast (``levels_count == 0``). This scores identically to the arena
      MASE of the point forecast.
    - Partial / full quantile sets: only the levels actually submitted (union across
      timestamps) are scored; a level missing at a given timestamp falls back to that
      timestamp's point forecast. Values are sorted across levels per timestamp to guarantee
      monotonicity, and the number of timestamps repaired is returned as ``crossing_count``.
    """
    predicted_values = np.asarray(predicted_values, dtype=float)
    n = len(predicted_values)
    parsed = [parse_probabilistic_values(pv) for pv in probabilistic_values]
    submitted_levels = sorted({level for p in parsed for level in p})

    if not submitted_levels:
        # Degenerate: all nine deciles == point forecast.
        forecasts = {level: predicted_values.copy() for level in QUANTILE_LEVELS}
        return forecasts, False, 0, 0

    forecasts: Dict[float, np.ndarray] = {}
    for level in submitted_levels:
        col = np.array(
            [parsed[t].get(level, predicted_values[t]) for t in range(n)],
            dtype=float,
        )
        forecasts[level] = col

    forecasts, crossing_count = repair_crossing(forecasts)
    return forecasts, True, len(submitted_levels), crossing_count


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
        mae_naive: the MAE of the flat last-context-value naive over the evaluated
            timestamps (same denominator as arena MASE). ``0`` means the SQL scale is
            undefined -> ``sql_score``/``sql_per_quantile`` come back ``None``.

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
