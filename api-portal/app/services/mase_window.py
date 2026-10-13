"""Displayed MASE and SQL: geometric mean over a common window, per ranking scope."""
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from fractions import Fraction
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.forecasts.forecast_scores_repository import ForecastScoresRepository
from app.services.forecast_metrics import MASE_METHOD

WINDOW_DAYS = 30
MIN_PARTICIPATION = Fraction(4, 5)

Scope = Tuple[str, Optional[str]]


@dataclass(frozen=True)
class WindowScore:
    scope_type: str
    scope_id: Optional[str]
    model_id: int
    metric: str
    value: Optional[float]
    participation: float
    model_tasks: int
    scope_tasks: int


def window_bounds(ranking_date: date) -> Tuple[datetime, datetime]:
    """Rounds ended in the `WINDOW_DAYS` full days before the ranking date (UTC)."""
    end = datetime.combine(ranking_date, time.min, tzinfo=timezone.utc)
    return end - timedelta(days=WINDOW_DAYS), end


def _scopes(definition: Dict[str, Any]) -> List[Scope]:
    scopes: List[Scope] = [("global", None), ("definition", str(definition["definition_id"]))]
    if definition["frequency"] is not None and definition["horizon"] is not None:
        scopes.append(("frequency_horizon", f"{definition['frequency']}::{definition['horizon']}"))
    return scopes


def aggregate_window(
    definitions: Iterable[Dict[str, Any]], models: Iterable[Dict[str, Any]]
) -> List[WindowScore]:
    """Roll per-definition aggregates up to the global, definition and frequency-horizon scopes.

    A task belongs to one round and so to one definition; counts and log sums therefore add up.
    """
    scope_tasks: Dict[Scope, int] = {}
    scopes_of: Dict[int, List[Scope]] = {}
    for d in definitions:
        scopes_of[d["definition_id"]] = _scopes(d)
        for scope in scopes_of[d["definition_id"]]:
            scope_tasks[scope] = scope_tasks.get(scope, 0) + d["tasks"]

    sums: Dict[Tuple[Scope, int, str], List[float]] = {}
    for m in models:
        per_metric = {
            "mase": (m["tasks"], m["mase_positive"], m["mase_log_sum"]),
            "sql": (m["sql_tasks"], m["sql_positive"], m["sql_log_sum"]),
        }
        for scope in scopes_of[m["definition_id"]]:
            for metric, (n, n_positive, log_sum) in per_metric.items():
                if not n:
                    continue
                acc = sums.setdefault((scope, m["model_id"], metric), [0, 0, 0.0])
                acc[0] += n
                acc[1] += n_positive
                acc[2] += log_sum or 0.0

    scores = []
    for ((scope_type, scope_id), model_id, metric), (n, n_positive, log_sum) in sums.items():
        total = scope_tasks[(scope_type, scope_id)]
        shown = n >= MIN_PARTICIPATION * total and n_positive > 0
        scores.append(WindowScore(
            scope_type=scope_type,
            scope_id=scope_id,
            model_id=model_id,
            metric=metric,
            value=math.exp(log_sum / n_positive) if shown else None,
            participation=n / total,
            model_tasks=n,
            scope_tasks=total,
        ))
    return scores


async def compute_window_scores(session: AsyncSession, ranking_date: date) -> List[WindowScore]:
    start, end = window_bounds(ranking_date)
    definitions, models = await ForecastScoresRepository(session).window_aggregates(start, end, MASE_METHOD)
    return aggregate_window(definitions, models)
