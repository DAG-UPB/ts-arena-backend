"""Displayed MASE/SQL over the 30-day window: participation rule, geometric mean, scope roll-up."""
import math
from datetime import date, datetime, timezone

import pytest

from app.services.mase_window import aggregate_window, window_bounds

HOURLY = ("01:00:00", "1 day")
QUARTER_HOURLY = ("00:15:00", "1 day")


def _aggregates(rows, definitions):
    """What `window_aggregates` returns for task rows (definition, round, series, model, mase, sql)."""
    tasks, models = {}, {}
    for definition, round_id, series_id, model_id, mase, sql in rows:
        tasks.setdefault(definition, set()).add((round_id, series_id))
        m = models.setdefault((definition, model_id), {
            "definition_id": definition, "model_id": model_id, "tasks": 0,
            "mase_positive": 0, "mase_log_sum": None,
            "sql_tasks": 0, "sql_positive": 0, "sql_log_sum": None,
        })
        m["tasks"] += 1
        for metric, value in (("mase", mase), ("sql", sql)):
            if value is None:
                continue
            if metric == "sql":
                m["sql_tasks"] += 1
            if value > 0:
                m[f"{metric}_positive"] += 1
                m[f"{metric}_log_sum"] = (m[f"{metric}_log_sum"] or 0.0) + math.log(value)
    defs = [
        {"definition_id": d, "frequency": definitions[d][0], "horizon": definitions[d][1], "tasks": len(t)}
        for d, t in tasks.items()
    ]
    return defs, list(models.values())


def _score(scores, scope_type, scope_id, model_id, metric="mase"):
    found = [s for s in scores if (s.scope_type, s.scope_id, s.model_id, s.metric)
             == (scope_type, scope_id, model_id, metric)]
    assert len(found) <= 1
    return found[0] if found else None


def test_value_is_the_geometric_mean():
    rows = [(1, 10, 1, 7, 0.5, None), (1, 10, 2, 7, 2.0, None), (1, 11, 1, 7, 4.0, None)]
    scores = aggregate_window(*_aggregates(rows, {1: HOURLY}))
    s = _score(scores, "definition", "1", 7)
    assert s.value == pytest.approx(4.0 ** (1 / 3))
    assert (s.model_tasks, s.scope_tasks, s.participation) == (3, 3, 1.0)


def test_participation_threshold_is_80_percent_inclusive():
    rows = [(1, r, 1, 1, 1.0, None) for r in range(5)]
    rows += [(1, r, 1, 2, 1.0, None) for r in range(4)]
    rows += [(1, r, 1, 3, 1.0, None) for r in range(3)]
    scores = aggregate_window(*_aggregates(rows, {1: HOURLY}))
    assert _score(scores, "global", None, 2).value == pytest.approx(1.0)
    hidden = _score(scores, "global", None, 3)
    assert hidden.value is None
    assert (hidden.participation, hidden.model_tasks, hidden.scope_tasks) == (0.6, 3, 5)


def test_tasks_nobody_was_scored_on_do_not_count():
    rows = [(1, 10, 1, 1, 1.0, None), (1, 11, 1, 1, 1.0, None)]
    scores = aggregate_window(*_aggregates(rows, {1: HOURLY}))
    assert _score(scores, "global", None, 1).scope_tasks == 2


def test_zero_mase_counts_for_participation_but_not_the_mean():
    rows = [(1, 10, 1, 1, 0.0, None), (1, 10, 2, 1, 2.0, None),
            (1, 11, 1, 1, 8.0, None), (1, 11, 1, 2, 0.0, None), (1, 10, 1, 2, 0.0, None)]
    scores = aggregate_window(*_aggregates(rows, {1: HOURLY}))
    s = _score(scores, "global", None, 1)
    assert (s.model_tasks, s.participation) == (3, 1.0)
    assert s.value == pytest.approx(4.0)
    exact = _score(scores, "global", None, 2)
    assert exact.value is None and exact.model_tasks == 2


def test_scopes_roll_up_by_definition():
    definitions = {1: HOURLY, 2: HOURLY, 3: QUARTER_HOURLY, 4: (None, "1 day")}
    rows = [(1, 10, 1, 1, 0.5, None), (2, 20, 1, 1, 2.0, None), (2, 20, 2, 1, 1.0, None),
            (3, 30, 1, 1, 4.0, None), (4, 40, 1, 1, 1.0, None)]
    scores = aggregate_window(*_aggregates(rows, definitions))

    fh = _score(scores, "frequency_horizon", "01:00:00::1 day", 1)
    assert (fh.scope_tasks, fh.value) == (3, pytest.approx(1.0))
    assert _score(scores, "frequency_horizon", "00:15:00::1 day", 1).value == pytest.approx(4.0)
    assert {s.scope_id for s in scores if s.scope_type == "frequency_horizon"} == {
        "01:00:00::1 day", "00:15:00::1 day"}
    glob = _score(scores, "global", None, 1)
    assert (glob.scope_tasks, glob.value) == (5, pytest.approx(4.0 ** (1 / 5)))
    assert _score(scores, "definition", "2", 1).value == pytest.approx(math.sqrt(2.0))


def test_sql_counts_only_rows_with_quantiles_against_all_tasks():
    rows = [(1, r, 1, 1, 1.0, 0.5) for r in range(5)]
    rows += [(1, r, 1, 2, 1.0, 0.5 if r < 2 else None) for r in range(5)]
    rows += [(1, r, 1, 3, 1.0, None) for r in range(5)]
    scores = aggregate_window(*_aggregates(rows, {1: HOURLY}))
    assert _score(scores, "global", None, 1, "sql").value == pytest.approx(0.5)
    partly = _score(scores, "global", None, 2, "sql")
    assert (partly.value, partly.participation, partly.scope_tasks) == (None, 0.4, 5)
    assert _score(scores, "global", None, 2, "mase").value == pytest.approx(1.0)
    assert _score(scores, "global", None, 3, "sql") is None


def test_empty_window():
    assert aggregate_window([], []) == []


def test_window_is_the_30_full_days_before_the_ranking_date():
    start, end = window_bounds(date(2026, 10, 13))
    assert start == datetime(2026, 9, 13, tzinfo=timezone.utc)
    assert end == datetime(2026, 10, 13, tzinfo=timezone.utc)
