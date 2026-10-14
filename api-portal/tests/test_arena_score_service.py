"""Arena Score service: rows to pair sums and coverage tables, scope assembly, 30-day columns, group writes.

No database: the repository is faked or driven through a fake session that records statements.
The fixture is the running example of the rating core (3 models, 5 rounds 28 days apart).
"""
import asyncio
import json
import math
from contextlib import asynccontextmanager
from datetime import date

import numpy as np
import pytest

from app.database.forecasts import arena_scores_repository as repository
from app.database.forecasts.arena_scores_repository import ArenaScoresRepository
from app.services import arena_score as arena
from app.services import arena_score_service as service
from app.services.mase_window import WindowScore

TODAY = date(2026, 10, 14)
A, B, C, D = 1, 2, 3, 4
LOAD, BIKES = 1, 2
HOURLY = ("01:00:00", "1 day")
QUARTER_HOURLY = ("00:15:00", "1 day")
POINTS = 24
MORNING = 7 * 3600.0

# (round, series, {model: MAE})
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
TOY_SCORES = [18, 7, 39]  # A, B, C against a naive forecast at 1.2 x A's MAE


def day_of(rnd):
    return 28 * (rnd - 1)


def make_rows(tasks, round_offset=0, quantiles=(), n_points=POINTS, naive=1.2, scale=1.0):
    """Column arrays as `kept_rows` returns them. Models in `quantiles` have an SQL score (0.8 x MAE).

    The naive MAE of a task is `naive` times the MAE of its first model; None leaves it NULL.
    """
    rows = []
    for rnd, series, errs in tasks:
        naive_mae = None if naive is None else naive * next(iter(errs.values()))
        for model, mae in errs.items():
            sql = 0.8 * mae if model in quantiles else None
            rows.append((rnd + round_offset, series, model, day_of(rnd), day_of(rnd) * 86400.0 + MORNING,
                         mae, sql, n_points, naive_mae, scale))
    return {name: np.array([row[k] for row in rows], dtype=dtype)
            for k, (name, dtype) in enumerate(repository.ROW_COLUMNS)}


def definition(definition_id, frequency_horizon=HOURLY):
    return {"definition_id": definition_id, "frequency": frequency_horizon[0], "horizon": frequency_horizon[1]}


def rated(sums, key=("global", None, "point"), window=None):
    rows = service.rate_group(TODAY, key, sums, window or {})
    return {row["model_id"]: row for row in rows}


# ---------------------------------------------------------------- rows to sums


def test_summarize_definition_builds_pair_sums_and_coverage_tables():
    sums = service.summarize_definition(7, make_rows(TOY))
    assert list(sums) == ["point"]
    point = sums["point"]
    assert point.pairs.n_pairs == 42  # 18 between the models, one per model and task with the naive forecast
    assert service.NAIVE_MODEL_ID in point.pairs.i
    assert (point.pairs.first_day, point.pairs.last_day) == (0, 112)
    assert point.rounds == [(7, r, day_of(r) * 86400.0 + MORNING, 2) for r in range(1, 6)]
    assert point.model_tasks == sorted(
        [(7, r, m, 2) for r in range(1, 6) for m in (A, B)] + [(7, r, C, 2) for r in (4, 5)])


def test_probabilistic_sums_hold_only_rows_with_quantiles():
    sums = service.summarize_definition(7, make_rows(TOY, quantiles=(A, B)))
    assert sorted(sums) == ["point", "probabilistic"]
    prob = sums["probabilistic"]
    assert {m for _, _, m, _ in prob.model_tasks} == {A, B}
    assert prob.pairs.n_pairs == 30 and {*prob.pairs.i, *prob.pairs.j} == {A, B, service.NAIVE_MODEL_ID}
    assert {r for _, r, _, _ in prob.rounds} == {1, 2, 3, 4, 5}
    assert all(tasks == 2 for _, _, _, tasks in prob.rounds)
    assert sums["point"].pairs.n_pairs == 42


def test_errors_that_do_not_count_are_left_out():
    rows = make_rows(TOY)
    rows["mae"][:2] = np.nan  # round 1, series LOAD: neither row has a valid error
    sums = service.summarize_definition(7, rows)["point"]
    assert sums.pairs.n_pairs == 39
    assert sums.rounds[0] == (7, 1, MORNING, 1)  # only the BIKES task of round 1 is left
    assert service.summarize_definition(7, make_rows([])) == {}


def test_a_single_model_task_is_a_pair_with_the_naive_forecast():
    sums = service.summarize_definition(7, make_rows(TOY + [(6, LOAD, {A: 100}), (7, LOAD, {D: 80})]))["point"]
    assert sums.pairs.n_pairs == 44
    assert (7, 6, day_of(6) * 86400.0 + MORNING, 1) in sums.rounds
    by_model = rated(sums)
    assert by_model[A]["tasks_played"] == 11 and by_model[A]["rounds_played"] == 6
    lonely = by_model[D]
    assert lonely["score"] == pytest.approx(100 * math.log(1.2), abs=1e-4)  # its only task: vs naive at 1.2 x its MAE
    assert not lonely["unranked_disconnected"] and lonely["provisional"] and lonely["weeks_played"] == 1
    assert (lonely["tasks_played"], lonely["rounds_played"]) == (1, 1)
    assert service.NAIVE_MODEL_ID not in by_model


def test_the_naive_row_comes_from_the_row_with_the_most_points():
    rows = make_rows([(1, LOAD, {A: 10.0, B: 8.0})])
    rows["n_points"] = np.array([24, 12])
    rows["naive_mae"] = np.array([12.0, 6.0])
    r, s, e, n, d = service.naive_rows(rows["round_id"], rows["series_id"], rows["naive_mae"],
                                       rows["n_points"], rows["day"])
    assert (r.tolist(), s.tolist(), e.tolist(), n.tolist(), d.tolist()) == ([1], [LOAD], [12.0], [24], [0])
    sums = service.summarize_definition(7, rows)["point"]
    assert sums.pairs.n_pairs == 1 and sums.pairs.skipped_points == 2  # B's 12 points pair with nobody


def test_the_naive_sql_is_its_mae_over_the_scale():
    sums = service.summarize_definition(7, make_rows([(1, LOAD, {A: 10.0})], quantiles=(A,), naive=1.0, scale=4.0))
    for metric, naive_err in (("point", 10.0), ("probabilistic", 2.5)):
        pairs = sums[metric].pairs
        assert (pairs.i.tolist(), pairs.j.tolist()) == ([service.NAIVE_MODEL_ID], [A])
        err_a = 10.0 if metric == "point" else 8.0
        assert pairs.wy.tolist() == pytest.approx([err_a / (naive_err + err_a)])


def test_a_scope_without_naive_errors_fails():
    sums = service.summarize_definition(7, make_rows(TOY, naive=None))["point"]
    assert sums.pairs.n_pairs == 18
    with pytest.raises(arena.ArenaScoreFitError):
        rated(sums)


# ---------------------------------------------------------------- scopes


def test_two_definitions_fold_into_the_running_example():
    load = [t for t in TOY if t[1] == LOAD]
    bikes = [t for t in TOY if t[1] == BIKES]
    aggregates = {}
    for d, tasks, offset in ((definition(1), load, 0), (definition(2), bikes, 100)):
        sums = service.summarize_definition(d["definition_id"], make_rows(tasks, offset))
        service.fold_definition(aggregates, d, sums)
    assert sorted(aggregates) == [("frequency_horizon", "01:00:00::1 day", "point"), ("global", None, "point")]

    single = rated(service.summarize_definition(1, make_rows(TOY))["point"])
    for key, sums in aggregates.items():
        by_model = rated(sums, key)
        assert [round(by_model[m]["score"]) for m in (A, B, C)] == TOY_SCORES
        assert [by_model[m]["weeks_played"] for m in (A, B, C)] == [5, 5, 2]
        assert [by_model[m]["tasks_played"] for m in (A, B, C)] == [10, 10, 4]
        assert [by_model[m]["rounds_played"] for m in (A, B, C)] == [10, 10, 4]
        assert [by_model[m]["coverage"] for m in (A, B, C)] == [1.0, 1.0, 1.0]
        assert by_model[C]["provisional"] and (by_model[C]["rank_best"], by_model[C]["rank_worst"]) == (None, None)
        assert (by_model[A]["rank_best"], by_model[A]["rank_worst"]) == (1, 2)
        for m in (A, B, C):
            assert by_model[m]["score"] == pytest.approx(single[m]["score"], abs=1e-9)
            assert by_model[m]["ci_lower"] == pytest.approx(single[m]["ci_lower"], abs=1e-9)


def test_a_definition_without_frequency_or_horizon_is_in_global_and_its_own_scope_only():
    aggregates = {}
    for d, offset in ((definition(1), 0), (definition(2), 100), (definition(3, (None, "1 day")), 200),
                      (definition(4, QUARTER_HOURLY), 300)):
        service.fold_definition(aggregates, d, service.summarize_definition(
            d["definition_id"], make_rows(TOY, offset, quantiles=(A, B))))

    assert sorted(aggregates) == [
        ("frequency_horizon", "00:15:00::1 day", "point"),
        ("frequency_horizon", "00:15:00::1 day", "probabilistic"),
        ("frequency_horizon", "01:00:00::1 day", "point"),
        ("frequency_horizon", "01:00:00::1 day", "probabilistic"),
        ("global", None, "point"),
        ("global", None, "probabilistic"),
    ]
    definitions_of = {key: {d for d, _, _, _ in s.rounds} for key, s in aggregates.items()}
    assert definitions_of[("global", None, "point")] == {1, 2, 3, 4}
    assert definitions_of[("frequency_horizon", "01:00:00::1 day", "point")] == {1, 2}
    assert definitions_of[("frequency_horizon", "00:15:00::1 day", "probabilistic")] == {4}


def test_global_pair_sums_are_the_sum_of_the_definitions():
    aggregates = {}
    parts = []
    for d, offset in ((definition(1), 0), (definition(2), 100)):
        sums = service.summarize_definition(d["definition_id"], make_rows(TOY, offset))
        parts.append(sums["point"])
        service.fold_definition(aggregates, d, sums)
    glob = aggregates[("global", None, "point")].pairs
    assert glob.n_pairs == sum(p.pairs.n_pairs for p in parts) == 84
    assert glob.w.sum() == pytest.approx(sum(p.pairs.w.sum() for p in parts))
    assert len(glob.day) == len(parts[0].pairs.day)  # the same days and model pairs
    assert glob.w == pytest.approx(2 * parts[0].pairs.w)


def test_a_model_missing_from_one_definition_or_late_there_is_low_coverage_where_they_are_joined():
    first = [(r, LOAD, {A: 100.0, B: 90.0 + 5 * r, C: 80.0 + 7 * r, D: 120.0 - 9 * r}) for r in range(1, 5)]
    second = [(r, LOAD, {A: 105.0, B: 95.0 + 3 * r, **({D: 110.0} if r == 4 else {})}) for r in range(1, 5)]
    aggregates, sums = {}, {}
    for d, tasks, offset in ((definition(1), first, 0), (definition(2), second, 100)):
        sums[d["definition_id"]] = service.summarize_definition(d["definition_id"], make_rows(tasks, offset))
        service.fold_definition(aggregates, d, sums[d["definition_id"]])

    # C never plays definition 2: 0 of its 4 tasks since joining. D joins it at the last round: 1 of 4.
    for key in (("global", None, "point"), ("frequency_horizon", "01:00:00::1 day", "point")):
        joint = rated(aggregates[key], key)
        assert [joint[m]["coverage"] for m in (A, B, C, D)] == [1.0, 1.0, 0.0, 0.25]
        assert [joint[m]["low_coverage"] for m in (A, B, C, D)] == [False, False, True, True]
        assert [(joint[m]["rank_best"], joint[m]["rank_worst"]) for m in (C, D)] == [(None, None)] * 2
        assert all(joint[m]["score"] is not None for m in (C, D))  # still fitted, only unranked

    # in its own definition each of them covers everything since joining
    one = rated(sums[1]["point"], ("definition", "1", "point"))
    two = rated(sums[2]["point"], ("definition", "2", "point"))
    assert sorted(one) == [A, B, C, D] and sorted(two) == [A, B, D]
    assert [one[m]["coverage"] for m in (A, B, C, D)] == [1.0] * 4 and not any(r["low_coverage"] for r in one.values())
    assert [two[m]["coverage"] for m in (A, B, D)] == [1.0] * 3 and not any(r["low_coverage"] for r in two.values())


# ---------------------------------------------------------------- rows

def test_rows_carry_the_stored_columns_and_a_json_method():
    sums = service.summarize_definition(7, make_rows(TOY))["point"]
    rows = rated(sums, ("definition", "7", "point"))
    assert set(rows[A]) == set(repository.SCORE_COLUMNS)
    assert (rows[A]["calculation_date"], rows[A]["scope_type"], rows[A]["scope_id"], rows[A]["metric"]) == (
        TODAY, "definition", "7", "point")
    assert json.loads(json.dumps(rows[A]["method"])) == rows[A]["method"]
    assert rows[A]["method"]["n_pairs"] == 42 and rows[A]["method"]["seed"] == arena.SEED
    assert rows[A]["method"]["zero_point"] == "reference"
    assert rows[A]["method"] is rows[B]["method"]
    for row in rows.values():
        for column in ("score", "ci_lower", "ci_upper"):
            assert isinstance(row[column], float) and math.isfinite(row[column])
        assert all(isinstance(row[c], int) for c in ("tasks_played", "rounds_played", "weeks_played", "model_id"))
        assert all(isinstance(row[c], bool) for c in ("provisional", "low_coverage", "unranked_disconnected"))


def test_window_columns_are_stored_on_the_rows_of_both_metrics():
    scores = [
        WindowScore("global", None, A, "mase", 0.9, 1.0, 10, 10),
        WindowScore("global", None, A, "sql", 0.7, 0.9, 9, 10),
        WindowScore("global", None, B, "mase", None, 0.6, 6, 10),
        WindowScore("definition", "7", A, "sql", None, 0.5, 5, 10),
        WindowScore("frequency_horizon", "01:00:00::1 day", A, "mase", 0.8, 0.95, 19, 20),
        WindowScore("frequency_horizon", "01:00:00::1 day", A, "sql", 0.6, 0.9, 18, 20),
    ]
    window = service.window_columns(scores)
    assert window[("global", None, A)] == {
        "mase_30d": 0.9, "participation_30d": 1.0, "sql_30d": 0.7, "sql_participation_30d": 0.9, "tasks_30d": 10}
    assert window[("global", None, B)] == {"mase_30d": None, "participation_30d": 0.6, "tasks_30d": 10}

    sums = service.summarize_definition(7, make_rows(TOY, quantiles=(A, B, C)))
    for metric in ("point", "probabilistic"):
        rows = rated(sums[metric], ("global", None, metric), window)
        assert (rows[A]["mase_30d"], rows[A]["participation_30d"]) == (0.9, 1.0)
        assert (rows[A]["sql_30d"], rows[A]["sql_participation_30d"], rows[A]["tasks_30d"]) == (0.7, 0.9, 10)
        assert (rows[B]["mase_30d"], rows[B]["participation_30d"], rows[B]["tasks_30d"]) == (None, 0.6, 10)
        assert (rows[B]["sql_30d"], rows[B]["sql_participation_30d"]) == (None, None)
        assert [rows[C][c] for c in service.WINDOW_COLUMNS] == [None] * 5

    own_scope = rated(sums["point"], ("definition", "7", "point"), window)
    assert own_scope[A]["sql_participation_30d"] == 0.5 and own_scope[A]["mase_30d"] is None
    assert own_scope[B]["tasks_30d"] is None

    key = ("frequency_horizon", "01:00:00::1 day", "point")
    horizon = rated(sums["point"], key, window)
    assert [horizon[A][c] for c in service.WINDOW_COLUMNS] == [0.8, 0.95, 0.6, 0.9, 20]
    assert [horizon[B][c] for c in service.WINDOW_COLUMNS] == [None] * 5


# ---------------------------------------------------------------- run


class FakeSession:
    def __init__(self):
        self.commits = self.rollbacks = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


class FakeRepository:
    """Serves the same running example for every definition; records the groups written."""

    definitions_served = []
    written = []

    def __init__(self, session):
        pass

    @asynccontextmanager
    async def read_transaction(self):
        yield

    async def definitions(self):
        return self.definitions_served

    async def kept_rows(self, definition_id):
        if definition_id == 3:
            return make_rows([])
        return make_rows(TOY, round_offset=100 * definition_id, quantiles=(A, B))

    async def replace_scores(self, calculation_date, scope_type, scope_id, metric, rows):
        self.written.append((scope_type, scope_id, metric, len(rows), rows))
        return len(rows)


@pytest.fixture
def fake_repository(monkeypatch):
    FakeRepository.definitions_served = [definition(1), definition(2), definition(3), definition(4, QUARTER_HOURLY)]
    FakeRepository.written = []
    monkeypatch.setattr(service, "ArenaScoresRepository", FakeRepository)

    async def window(session, ranking_date):
        assert ranking_date == TODAY
        return [WindowScore("global", None, A, "mase", 0.9, 1.0, 10, 10),
                WindowScore("definition", "99", A, "mase", 0.9, 1.0, 10, 10)]

    monkeypatch.setattr(service, "compute_window_scores", window)
    return FakeRepository


def test_run_writes_every_scope_and_metric(fake_repository):
    session = FakeSession()
    summary = asyncio.run(service.compute_and_store_arena_scores(session, TODAY))

    written = {(t, s, m): n for t, s, m, n, _ in fake_repository.written}
    assert set(written) == (
        {("definition", str(d), m) for d in (1, 2, 4) for m in ("point", "probabilistic")}
        | {("frequency_horizon", fh, m) for fh in ("01:00:00::1 day", "00:15:00::1 day")
           for m in ("point", "probabilistic")}
        | {("global", None, "point"), ("global", None, "probabilistic")}
    )
    assert written[("global", None, "point")] == 3 and written[("global", None, "probabilistic")] == 2
    assert [t for t, _, _, _, _ in fake_repository.written][:3] == ["definition"] * 3  # global and frequency-horizon last
    assert summary["failures"] == [] and (summary["scopes"], summary["groups"]) == (6, 12)
    assert summary["rows"] == {
        "definition": {"point": 9, "probabilistic": 6},
        "frequency_horizon": {"point": 6, "probabilistic": 4},
        "global": {"point": 3, "probabilistic": 2},
    }
    assert session.commits == 12 and session.rollbacks == 0
    assert summary["window_unmatched"] == 1  # a 30-day entry of a scope without rows


def test_a_failing_group_writes_nothing_and_the_others_go_on(fake_repository, monkeypatch, caplog):
    real = arena.rate_scope

    def flaky(pairs, models, covered, **kwargs):
        if kwargs["scope"] == "definition 2 [point]":
            raise arena.ArenaScoreFitError("no convergence in 1000 iterations")
        return real(pairs, models, covered, **kwargs)

    monkeypatch.setattr(arena, "rate_scope", flaky)
    session = FakeSession()
    with caplog.at_level("ERROR"):
        summary = asyncio.run(service.compute_and_store_arena_scores(session, TODAY))

    written = {(t, s, m) for t, s, m, _, _ in fake_repository.written}
    assert ("definition", "2", "point") not in written
    assert ("definition", "2", "probabilistic") in written and ("global", None, "point") in written
    assert len(written) == 11
    assert summary["failures"] == [{
        "scope_type": "definition", "scope_id": "2", "metric": "point",
        "error": "ArenaScoreFitError: no convergence in 1000 iterations"}]
    assert (summary["groups"], session.commits, session.rollbacks) == (12, 11, 1)
    assert "definition 2 [point]" in caplog.text


def test_a_failing_write_rolls_back_and_the_others_go_on(fake_repository, monkeypatch):
    original = FakeRepository.replace_scores

    async def broken(self, calculation_date, scope_type, scope_id, metric, rows):
        if (scope_type, metric) == ("frequency_horizon", "point"):
            raise RuntimeError("deadlock detected")
        return await original(self, calculation_date, scope_type, scope_id, metric, rows)

    monkeypatch.setattr(FakeRepository, "replace_scores", broken)
    session = FakeSession()
    summary = asyncio.run(service.compute_and_store_arena_scores(session, TODAY))
    assert sorted((f["scope_type"], f["scope_id"], f["metric"]) for f in summary["failures"]) == [
        ("frequency_horizon", "00:15:00::1 day", "point"), ("frequency_horizon", "01:00:00::1 day", "point")]
    assert (session.commits, session.rollbacks) == (10, 2)
    assert summary["rows"]["frequency_horizon"] == {"probabilistic": 4}


# ---------------------------------------------------------------- repository


class RecordingSession:
    def __init__(self):
        self.statements = []  # (sql text, params)
        self.commits = self.rollbacks = 0

    async def execute(self, query, params=None):
        self.statements.append((str(query), params))

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


def _score_row(model_id):
    return {**dict.fromkeys(repository.SCORE_COLUMNS), "model_id": model_id, "method": {"seed": 0}}


def test_group_delete_precedes_the_inserts_and_nothing_is_committed_by_the_repository():
    session = RecordingSession()
    written = asyncio.run(ArenaScoresRepository(session).replace_scores(
        TODAY, "global", None, "point", [_score_row(1), _score_row(2)]))
    assert written == 2

    delete, params = session.statements[0]
    assert "DELETE FROM forecasts.daily_arena_scores" in delete and "model_id" not in delete
    assert params == {"calculation_date": TODAY, "scope_type": "global", "scope_id": None, "metric": "point"}
    insert, rows = session.statements[1]
    assert "INSERT INTO forecasts.daily_arena_scores" in insert and ":method" in insert
    assert [r["model_id"] for r in rows] == [1, 2]
    assert len(session.statements) == 2 and session.commits == 0


def test_an_empty_group_is_left_untouched():
    session = RecordingSession()
    assert asyncio.run(ArenaScoresRepository(session).replace_scores(TODAY, "global", None, "point", [])) == 0
    assert session.statements == []


def test_read_transaction_limits_parallel_workers_and_ends_with_a_rollback():
    session = RecordingSession()

    async def read(fail):
        async with ArenaScoresRepository(session).read_transaction():
            if fail:
                raise RuntimeError("boom")

    asyncio.run(read(False))
    assert "SET LOCAL max_parallel_workers_per_gather = 0" in session.statements[0][0]
    assert session.rollbacks == 1
    with pytest.raises(RuntimeError):
        asyncio.run(read(True))
    assert session.rollbacks == 2


class StreamedResult:
    def __init__(self, partitions):
        self._partitions = partitions

    async def partitions(self, size):
        assert size > 0
        for rows in self._partitions:
            yield rows


class StreamingSession:
    def __init__(self, partitions):
        self.partitions = partitions
        self.params = None

    async def stream(self, query, params=None):
        self.params = params
        return StreamedResult(self.partitions)


def test_kept_rows_become_one_array_per_column_with_nan_for_a_missing_error():
    partitions = [
        [(1, 10, 5, 100, 8.64e6, 2.5, None, 24, 3.5, 1.5), (1, 10, 6, 100, 8.64e6, 3.0, 2.4, 24, 3.5, 1.5)],
        [(2, 10, 5, 101, 8.7264e6, None, 1.0, 12, None, None)],
    ]
    session = StreamingSession(partitions)
    rows = asyncio.run(ArenaScoresRepository(session).kept_rows(7))
    assert session.params == {"definition_id": 7}
    assert list(rows) == [name for name, _ in repository.ROW_COLUMNS]
    assert {name: rows[name].dtype for name in rows} == dict(repository.ROW_COLUMNS)
    assert rows["round_id"].tolist() == [1, 1, 2] and rows["n_points"].tolist() == [24, 24, 12]
    assert np.isnan(rows["sql_score"]).tolist() == [True, False, False]
    assert np.isnan(rows["mae"]).tolist() == [False, False, True]
    assert np.isnan(rows["naive_mae"]).tolist() == np.isnan(rows["scale"]).tolist() == [False, False, True]

    empty = asyncio.run(ArenaScoresRepository(StreamingSession([])).kept_rows(7))
    assert all(len(a) == 0 for a in empty.values()) and empty["day"].dtype == np.int64


def test_kept_rows_query_applies_the_row_filter_of_the_ranking():
    sql = str(repository._KEPT_ROWS)
    for fragment in (
        "f.final_evaluation", "NOT COALESCE(r.is_cancelled, FALSE)", "f.evaluation_status IN ('complete', 'partial')",
        "ds.is_excluded", "f.has_quantiles AND f.sql_score >= 0", "< 'Infinity'::float8", "r.definition_id = :definition_id",
        "AT TIME ZONE 'UTC'", "f.naive_mae", "f.scale",
        "JOIN models.model_info m ON m.id = f.model_id",
        "m.publishing_date IS NULL OR (r.registration_start AT TIME ZONE 'UTC')::date >= m.publishing_date",
    ):
        assert fragment in sql
