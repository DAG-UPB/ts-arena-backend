"""Unit tests for the SQL backfill script's compute path and data-source fallback.

Part 1 exercises `recompute_sql_for_model_series` — the script's pure, no-DB function —
against the same golden scenarios as `tests/test_forecast_metrics.py` and
`tests/test_scoring_sql_integration.py`: a real-quantile case, a point-only degenerate
case (sql == mase identity), a crossing-repair case, and an a==0 -> NULL case.

`recompute_sql_for_model_series` calls `app.services.forecast_metrics.compute_sql_fields`,
the SAME function `ScoreEvaluationService._calculate_score_for_model_series` calls for the
live path — so a green test here is proof the backfill script produces byte-identical
results to live scoring, not just similar ones.

Part 2 exercises the aggregate/raw fallback decision: `decide_sql_source` (pure) and
`resolve_evaluation_source` (async orchestration, driven with stubbed caches standing in
for the DB-backed `_SeriesCache`/`_SeriesActualsCache`). The decision/guard semantics
(`decide_sql_source`, `_source_sufficient`) are unchanged from before the batching
rework — these tests just confirm the new signature still drives them correctly.

Part 3 exercises `align_evaluation_data` / `group_actuals_by_minute` — the Python-side
join that replaced the per-row SQL join (`get_evaluation_data_by_resolution`'s
`date_trunc('minute', ...)` equality), covering minute truncation, inner-join semantics
(non-matching rows dropped), ordering, and the duplicate-actual case. The bucketing SQL
itself can't be unit-tested offline (requires a real TimescaleDB with `time_bucket`), so
`ForecastRepository.get_series_actuals_raw_bucketed` /
`TimeSeriesRepository.get_raw_bucketed_value_at` are deliberately kept simple and
reviewable instead.
"""
import asyncio
from datetime import datetime, timezone

import numpy as np
import pytest

from app.scripts.backfill_sql_scores import (
    BackfillSummary,
    align_evaluation_data,
    decide_sql_source,
    group_actuals_by_minute,
    recompute_sql_for_model_series,
    resolve_evaluation_source,
)


# --- golden scenarios ---------------------------------------------------------------

def _quantile_model_rows():
    y_true = [10.0, 12.0, 11.0]
    y_pred = [10.5, 11.5, 11.0]
    rows = []
    for yt, yp in zip(y_true, y_pred):
        pv = {f"q_0.{i}": round(yp - 1.0 + 0.25 * (i - 1), 3) for i in range(1, 10)}
        pv["q_0.5"] = yp  # median consistency
        rows.append({"actual_value": yt, "predicted_value": yp, "probabilistic_values": pv})
    return rows


def test_real_quantile_case():
    fields = recompute_sql_for_model_series(_quantile_model_rows(), naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 9
    assert fields["quantile_crossing_count"] == 0
    assert fields["sql_score"] is not None and fields["sql_score"] >= 0
    assert set(fields["sql_per_quantile"]) == {f"0.{i}" for i in range(1, 10)}
    assert fields["mae_naive"] == np.mean(np.abs(np.array([10.0, 12.0, 11.0]) - 9.0))


def test_point_only_degenerate_equals_mase():
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None},
        {"actual_value": 12.0, "predicted_value": 11.5, "probabilistic_values": {}},
        {"actual_value": 11.0, "predicted_value": 11.0, "probabilistic_values": None},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    y_true = np.array([10.0, 12.0, 11.0])
    y_pred = np.array([10.5, 11.5, 11.0])
    mae_naive = float(np.mean(np.abs(y_true - 9.0)))
    mase = float(np.mean(np.abs(y_true - y_pred))) / mae_naive

    assert fields["has_quantiles"] is False
    assert fields["quantile_levels_count"] == 0
    # Degenerate distribution => SQL == MASE (shared denominator identity).
    assert fields["sql_score"] == pytest.approx(mase, rel=1e-9)


def test_crossing_repair_case():
    # q_0.8 > q_0.9 at the first timestamp -> one crossing repaired at scoring.
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.0,
         "probabilistic_values": {"q_0.1": 8.0, "q_0.5": 10.0, "q_0.8": 13.0, "q_0.9": 11.0}},
        {"actual_value": 12.0, "predicted_value": 12.0,
         "probabilistic_values": {"q_0.1": 10.0, "q_0.5": 12.0, "q_0.8": 13.0, "q_0.9": 14.0}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    assert fields["has_quantiles"] is True
    assert fields["quantile_levels_count"] == 4
    assert fields["quantile_crossing_count"] == 1
    assert fields["sql_score"] is not None


def test_a_zero_yields_null_sql():
    # naive == actuals everywhere and perfect point => mae_naive == 0 => scale None => NULL SQL
    rows = [
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
        {"actual_value": 9.0, "predicted_value": 9.0, "probabilistic_values": {}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    assert fields["mae_naive"] == 0.0
    assert fields["sql_score"] is None
    assert fields["sql_per_quantile"] is None


# --- BackfillSummary bookkeeping ------------------------------------------------------

def test_summary_records_degenerate_row_and_detects_no_mismatch_when_identical():
    summary = BackfillSummary()
    fields = recompute_sql_for_model_series(
        [
            {"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None},
            {"actual_value": 12.0, "predicted_value": 11.5, "probabilistic_values": None},
        ],
        naive_value=9.0,
    )
    summary.record_row(
        round_id=1, model_id=2, series_id=3, fields=fields, stored_mase=fields["sql_score"],
        source="aggregate", no_expected_count=False,
    )

    assert summary.rows_updated == 1
    assert summary.rows_degenerate == 1
    assert summary.rows_real_quantiles == 0
    assert summary.rows_from_aggregate == 1
    assert summary.rows_from_raw == 0
    assert summary.rows_no_expected_count == 0
    assert summary.mismatches == []


def test_summary_flags_identity_mismatch():
    summary = BackfillSummary()
    fields = recompute_sql_for_model_series(
        [{"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None}],
        naive_value=9.0,
    )
    # Simulate stored mase drifting from the freshly-computed value (data changed since
    # the row was originally scored).
    stale_mase = fields["sql_score"] + 1.0
    summary.record_row(
        round_id=1, model_id=2, series_id=3, fields=fields, stored_mase=stale_mase,
        source="aggregate", no_expected_count=False,
    )

    assert len(summary.mismatches) == 1
    assert summary.mismatches[0] == (1, 2, 3, fields["sql_score"], stale_mase)


def test_summary_tracks_crossing_by_model():
    summary = BackfillSummary()
    rows = [
        {"actual_value": 10.0, "predicted_value": 10.0,
         "probabilistic_values": {"q_0.1": 8.0, "q_0.5": 10.0, "q_0.8": 13.0, "q_0.9": 11.0}},
    ]
    fields = recompute_sql_for_model_series(rows, naive_value=9.0)
    summary.record_row(
        round_id=1, model_id=42, series_id=3, fields=fields, stored_mase=None,
        source="aggregate", no_expected_count=False,
    )

    assert summary.crossing_by_model == {42: 1}


def test_summary_tracks_raw_source_and_no_expected_count():
    summary = BackfillSummary()
    fields = recompute_sql_for_model_series(
        [{"actual_value": 10.0, "predicted_value": 10.5, "probabilistic_values": None}],
        naive_value=9.0,
    )
    summary.record_row(
        round_id=1, model_id=2, series_id=3, fields=fields, stored_mase=None,
        source="raw", no_expected_count=True,
    )

    assert summary.rows_from_raw == 1
    assert summary.rows_from_aggregate == 0
    assert summary.rows_no_expected_count == 1


def test_summary_records_coverage_mismatch():
    summary = BackfillSummary()
    summary.record_coverage_mismatch(
        round_id=1, model_id=2, series_id=3, evaluated_count=5, aggregate_count=3, raw_count=4
    )

    assert summary.rows_coverage_mismatch == 1
    assert summary.coverage_mismatches == [(1, 2, 3, 5, 3, 4)]
    # A coverage mismatch is never counted as a written row.
    assert summary.rows_updated == 0


# --- decide_sql_source: the fallback decision table -----------------------------------

def test_decide_prefers_aggregate_when_it_matches_expected_count():
    source, no_expected_count = decide_sql_source(
        evaluated_count=5, aggregate=(10.0, 5), raw=(10.0, 5)
    )
    assert source == "aggregate"
    assert no_expected_count is False


def test_decide_falls_back_to_raw_when_aggregate_misses_count():
    source, no_expected_count = decide_sql_source(
        evaluated_count=5, aggregate=(10.0, 3), raw=(10.0, 5)
    )
    assert source == "raw"
    assert no_expected_count is False


def test_decide_falls_back_to_raw_when_aggregate_naive_unresolved():
    source, no_expected_count = decide_sql_source(
        evaluated_count=5, aggregate=(None, 0), raw=(9.5, 5)
    )
    assert source == "raw"
    assert no_expected_count is False


def test_decide_coverage_mismatch_when_both_sources_miss_expected_count():
    source, no_expected_count = decide_sql_source(
        evaluated_count=5, aggregate=(10.0, 3), raw=(9.5, 4)
    )
    assert source is None
    assert no_expected_count is False


def test_decide_no_expected_count_prefers_aggregate_when_both_have_data():
    # evaluated_count is None (pre-column row) -> "resolves + non-empty" is the guard.
    source, no_expected_count = decide_sql_source(
        evaluated_count=None, aggregate=(10.0, 4), raw=(9.5, 4)
    )
    assert source == "aggregate"
    assert no_expected_count is True


def test_decide_no_expected_count_falls_back_to_raw_when_aggregate_empty():
    source, no_expected_count = decide_sql_source(
        evaluated_count=None, aggregate=(None, 0), raw=(9.5, 4)
    )
    assert source == "raw"
    assert no_expected_count is True


def test_decide_coverage_mismatch_when_no_expected_count_and_both_empty():
    source, no_expected_count = decide_sql_source(
        evaluated_count=None, aggregate=(None, 0), raw=(None, 0)
    )
    assert source is None
    assert no_expected_count is False


# --- align_evaluation_data / group_actuals_by_minute: the Python-side SQL-join replica -

def _ts(second=0, minute=0, hour=0):
    return datetime(2026, 1, 1, hour, minute, second, tzinfo=timezone.utc)


def test_group_actuals_by_minute_truncates_seconds():
    actuals = [{"ts": _ts(second=17), "value": 1.5}, {"ts": _ts(minute=1, second=45), "value": 2.5}]
    grouped = group_actuals_by_minute(actuals)
    assert grouped == {_ts(): [1.5], _ts(minute=1): [2.5]}


def test_align_evaluation_data_matches_on_truncated_minute():
    forecast_rows = [{"ts": _ts(second=5), "predicted_value": 1.0, "probabilistic_values": {"q_0.5": 1.0}}]
    actuals_by_minute = group_actuals_by_minute([{"ts": _ts(second=0), "value": 1.2}])

    aligned = align_evaluation_data(forecast_rows, actuals_by_minute)

    assert aligned == [{
        "ts": _ts(second=5), "predicted_value": 1.0,
        "probabilistic_values": {"q_0.5": 1.0}, "actual_value": 1.2,
    }]


def test_align_evaluation_data_is_inner_join_drops_unmatched():
    forecast_rows = [
        {"ts": _ts(minute=0), "predicted_value": 1.0, "probabilistic_values": None},
        {"ts": _ts(minute=1), "predicted_value": 2.0, "probabilistic_values": None},  # no actual
    ]
    actuals_by_minute = group_actuals_by_minute([{"ts": _ts(minute=0), "value": 1.5}])

    aligned = align_evaluation_data(forecast_rows, actuals_by_minute)

    assert len(aligned) == 1
    assert aligned[0]["ts"] == _ts(minute=0)


def test_align_evaluation_data_preserves_forecast_order():
    forecast_rows = [
        {"ts": _ts(minute=m), "predicted_value": float(m), "probabilistic_values": None}
        for m in range(5)
    ]
    actuals_by_minute = group_actuals_by_minute(
        [{"ts": _ts(minute=m), "value": float(m) + 0.1} for m in reversed(range(5))]
    )

    aligned = align_evaluation_data(forecast_rows, actuals_by_minute)

    assert [row["ts"] for row in aligned] == [_ts(minute=m) for m in range(5)]


def test_align_evaluation_data_duplicate_actual_emits_one_row_per_match():
    # A true SQL inner join multiplies rows when more than one actual matches the same
    # truncated minute — replicate that faithfully, don't silently keep just the first.
    forecast_rows = [{"ts": _ts(second=10), "predicted_value": 1.0, "probabilistic_values": None}]
    actuals_by_minute = {_ts(): [1.1, 1.2]}

    aligned = align_evaluation_data(forecast_rows, actuals_by_minute)

    assert len(aligned) == 2
    assert [row["actual_value"] for row in aligned] == [1.1, 1.2]
    assert all(row["ts"] == _ts(second=10) for row in aligned)


def test_align_evaluation_data_empty_actuals_yields_empty_list():
    forecast_rows = [{"ts": _ts(), "predicted_value": 1.0, "probabilistic_values": None}]
    assert align_evaluation_data(forecast_rows, {}) == []


# --- resolve_evaluation_source: async orchestration with stubbed caches ---------------

class _StubSeriesCache:
    """Stands in for `_SeriesCache`: canned naive values, call-counted for laziness checks."""

    def __init__(self, agg_naive, raw_naive):
        self._agg_naive = agg_naive
        self._raw_naive = raw_naive
        self.raw_naive_calls = 0

    async def aggregate_naive(self, series_id):
        return self._agg_naive

    async def raw_naive(self, series_id):
        self.raw_naive_calls += 1
        return self._raw_naive


class _StubActualsCache:
    """Stands in for `_SeriesActualsCache`: canned actuals-by-minute dicts for each
    source, call-counted for laziness checks."""

    def __init__(self, agg_actuals, raw_actuals):
        self._agg_actuals = agg_actuals
        self._raw_actuals = raw_actuals
        self.raw_calls = 0

    async def aggregate(self, series_id):
        return self._agg_actuals

    async def raw(self, series_id):
        self.raw_calls += 1
        return self._raw_actuals


def _forecast_rows(n):
    return [
        {"ts": _ts(minute=i), "predicted_value": 1.0, "probabilistic_values": None}
        for i in range(n)
    ]


def _actuals_for(n):
    return group_actuals_by_minute([{"ts": _ts(minute=i), "value": 1.0} for i in range(n)])


def test_resolve_evaluation_source_aggregate_match_never_touches_raw():
    forecast_rows = _forecast_rows(5)
    naive_cache = _StubSeriesCache(agg_naive=10.0, raw_naive=10.0)
    actuals_cache = _StubActualsCache(agg_actuals=_actuals_for(5), raw_actuals=_actuals_for(5))

    source, no_expected_count, naive_value, eval_data, agg_count, raw_count = asyncio.run(
        resolve_evaluation_source(forecast_rows, naive_cache, actuals_cache, series_id=3, evaluated_count=5)
    )

    assert source == "aggregate"
    assert no_expected_count is False
    assert naive_value == 10.0
    assert len(eval_data) == 5
    assert agg_count == 5 and raw_count == 0
    # Laziness: the aggregate alone settled it, so the raw fallback was never queried.
    assert actuals_cache.raw_calls == 0
    assert naive_cache.raw_naive_calls == 0


def test_resolve_evaluation_source_falls_back_to_raw_on_aggregate_miss():
    forecast_rows = _forecast_rows(5)
    naive_cache = _StubSeriesCache(agg_naive=10.0, raw_naive=9.5)
    # Aggregate only has actuals for 3 of the 5 forecast minutes -> joined count misses.
    actuals_cache = _StubActualsCache(agg_actuals=_actuals_for(3), raw_actuals=_actuals_for(5))

    source, no_expected_count, naive_value, eval_data, agg_count, raw_count = asyncio.run(
        resolve_evaluation_source(forecast_rows, naive_cache, actuals_cache, series_id=3, evaluated_count=5)
    )

    assert source == "raw"
    assert no_expected_count is False
    assert naive_value == 9.5
    assert len(eval_data) == 5
    assert agg_count == 3 and raw_count == 5
    assert actuals_cache.raw_calls == 1
    assert naive_cache.raw_naive_calls == 1


def test_resolve_evaluation_source_coverage_mismatch_when_both_fail():
    forecast_rows = _forecast_rows(5)
    naive_cache = _StubSeriesCache(agg_naive=10.0, raw_naive=9.5)
    actuals_cache = _StubActualsCache(agg_actuals=_actuals_for(3), raw_actuals=_actuals_for(4))

    source, no_expected_count, naive_value, eval_data, agg_count, raw_count = asyncio.run(
        resolve_evaluation_source(forecast_rows, naive_cache, actuals_cache, series_id=3, evaluated_count=5)
    )

    assert source is None
    assert no_expected_count is False
    assert naive_value is None
    assert eval_data == []
    assert agg_count == 3 and raw_count == 4
