"""Python-side reproduction of the evaluation join, shared by every scoring caller.

`forecasts.forecasts` is a ~200M-row TimescaleDB hypertable. A query filtered only on
`(round_id, model_id, series_id)` carries no `ts` predicate, so the planner cannot prune
chunks and pays full planning cost on every call — measured on dev at **180 ms planning vs
12 ms execution** for a single (model, series) lookup. Issued once per (model, series) pair,
that is the entire cost of an evaluation cycle: a 33-model × 18-series round means ~600 such
queries.

The fix is to fetch per *round* and per *series* instead, then do the join in Python. That
requires reproducing what the SQL join did, exactly:

    JOIN <actuals> ON forecast.series_id = actual.series_id
                   AND date_trunc('minute', forecast.ts) = date_trunc('minute', actual.ts)

These helpers are that reproduction. They were first written for the historical SQL backfill
(backend-65) and are shared from here so the live scorer
(`ScoreEvaluationService`) and the backfill cannot drift apart — the same requirement that
makes `compute_sql_fields` a single shared function.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def truncate_to_minute(ts: datetime) -> datetime:
    """Python-side equivalent of SQL `date_trunc('minute', ts)` (both sides are UTC,
    tz-aware, straight from the DB — no tz handling needed beyond dropping seconds)."""
    return ts.replace(second=0, microsecond=0)


def group_actuals_by_minute(actual_rows: List[Dict[str, Any]]) -> Dict[datetime, List[float]]:
    """Group a series' actuals ({'ts', 'value'} rows) by minute-truncated ts.

    A list per key (not a single value) so `align_evaluation_data` can faithfully
    reproduce what a SQL inner join would do if more than one actual row truncates to
    the same minute (shouldn't happen for bucketed data, but a join would emit one
    output row per match, not silently keep only the first).
    """
    grouped: Dict[datetime, List[float]] = {}
    for row in actual_rows:
        key = truncate_to_minute(row["ts"])
        grouped.setdefault(key, []).append(row["value"])
    return grouped


def align_evaluation_data(
    forecast_rows: List[Dict[str, Any]],
    actuals_by_minute: Dict[datetime, List[float]],
) -> List[Dict[str, Any]]:
    """Python-side reproduction of the SQL inner join described in the module docstring.

    Forecast rows are matched against `actuals_by_minute` (built by
    `group_actuals_by_minute`) via minute-truncated ts. If more than one actual maps to the
    same truncated minute, a true SQL join would emit one output row per (forecast, actual)
    pair — this does too (does NOT just keep the first).

    Args:
        forecast_rows: forecast rows for ONE (model, series), each with 'ts',
            'predicted_value', and optionally 'probabilistic_values' — ordered by ts
            (as `get_round_forecasts` returns them).
        actuals_by_minute: minute-truncated ts -> list of actual values, from
            `group_actuals_by_minute`.

    Returns:
        List of dicts (ts, predicted_value, probabilistic_values, actual_value) — the
        same shape `get_evaluation_data_by_resolution` returns — ordered by forecast ts
        (ties from a duplicate-actual match ordered by match order within that minute).
    """
    aligned: List[Dict[str, Any]] = []
    for row in forecast_rows:
        matches = actuals_by_minute.get(truncate_to_minute(row["ts"]))
        if not matches:
            continue
        for actual_value in matches:
            aligned.append({
                "ts": row["ts"],
                "predicted_value": row["predicted_value"],
                "probabilistic_values": row.get("probabilistic_values"),
                "actual_value": actual_value,
            })
    return aligned
