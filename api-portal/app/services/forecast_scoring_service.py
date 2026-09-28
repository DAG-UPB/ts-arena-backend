"""
Scores forecasts into `forecasts.forecast_scores`, on a schedule of its own.

Per (round, model, series) it stores the model's MAE and RMSE over the evaluated timestamps,
MASE and SQL against the (round, series) context scale in `forecasts.series_scale`
(Hyndman & Koehler 2006: the in-sample naive error of the context as served, the same for
every model), and `naive_mae`, the persistence forecast's MAE on the same timestamps, so that
`mae / naive_mae` is the relative MAE the arena score stores as `mase`.

This runs next to `ScoreEvaluationService`, which keeps writing `forecasts.scores` until
every reader has moved here. Nothing in this module reads `forecasts.scores` or imports the
arena scorer, so that one can be switched off and deleted without touching this. Which
points count and when a score is final follow the arena scorer's rules, copied below.

A run of `periodic_forecast_scoring_job`:

1. `capture_served_scales`: rounds whose context was just served get their scales from
   `challenges.context_data`, the only time that exact context is available.
2. `rounds_to_score` and `score_round`: active and completed rounds that are not final yet,
   one transaction each. Scales the capture missed are rebuilt from SCD2.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.challenges.challenge_repository import ChallengeRoundRepository
from app.database.forecasts.forecast_scores_repository import ForecastScoresRepository
from app.database.forecasts.repository import ForecastRepository
from app.services.evaluation_alignment import align_evaluation_data, group_actuals_by_minute
from app.services.forecast_metrics import MASE_METHOD, compute_score_fields, mase_scale_defined
from app.services.series_scale_service import SeriesScaleService

logger = logging.getLogger(__name__)


# --- The arena scorer's rules ------------------------------------------------------------
# Copied from ScoreEvaluationService rather than imported, so that it can be deleted.

FREQUENCY_TO_RESOLUTION: Dict[timedelta, str] = {
    timedelta(minutes=15): "15min",
    timedelta(hours=1): "1h",
    timedelta(days=1): "1d",
}

EVALUATION_TIMEOUT = timedelta(days=1)  # Grace period after round end for ground truth
MIN_COVERAGE_FOR_FINAL = 0.95           # Coverage a pair needs after it to keep its score

# A forecast and an actual join when their minute-truncated timestamps are equal, so padding
# the actuals fetch by one minute keeps the ts-bounded query lossless.
_ACTUALS_TS_MARGIN = timedelta(minutes=1)


# --- This service's own settings ----------------------------------------------------------

# Rounds that ended longer ago are left to the backfill. Well above the grace period, so a
# job that was down for a few days still catches up on its own.
SCORING_LOOKBACK = timedelta(days=7)

# How far back the capture looks for rounds whose context has been served.
CAPTURE_LOOKBACK = timedelta(hours=6)


def _as_utc(value: datetime) -> datetime:
    """Normalise to UTC so naive DB timestamps compare against tz-aware ones."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _finite(value: Any) -> bool:
    try:
        return value is not None and bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def timedelta_to_resolution(frequency: Optional[timedelta]) -> str:
    """Resolution view ("15min", "1h", "1d") of a round frequency; "1h" when unknown."""
    resolution = FREQUENCY_TO_RESOLUTION.get(frequency) if frequency is not None else None
    if resolution is None:
        logger.warning(f"Unknown frequency {frequency}, defaulting to '1h' resolution")
        return "1h"
    return resolution


def series_forecast_windows(
    context_edges: Dict[int, Optional[datetime]],
    frequency: Optional[timedelta],
    horizon: Optional[timedelta],
) -> Dict[int, Tuple[datetime, datetime]]:
    """`series_id -> (first_ts, last_ts)`: the timestamps a forecast for that series may carry.

    `context_edge + frequency` through `context_edge + horizon`, per series, the range uploads
    are validated against. Points outside it are not scored. Series without a context edge
    are left out, and their points are not filtered.
    """
    if not frequency or not horizon:
        return {}
    return {
        series_id: (_as_utc(edge) + frequency, _as_utc(edge) + horizon)
        for series_id, edge in context_edges.items()
        if edge is not None
    }


def evaluation_timeout_passed(round_end_time: Optional[datetime]) -> bool:
    """Has the grace period for ground truth after the round's end elapsed?

    A round without an end time never times out.
    """
    if round_end_time is None:
        return False
    return datetime.now(timezone.utc) > _as_utc(round_end_time) + EVALUATION_TIMEOUT


def coverage_verdict(data_coverage: float, timeout_passed: bool) -> Tuple[str, bool]:
    """`(evaluation_status, final_evaluation)` of a pair with at least one evaluated point.

    Complete coverage is final at once. After the grace period a pair is final either way:
    with at least `MIN_COVERAGE_FOR_FINAL` it is a valid `partial` score, below that it is
    `insufficient_data` and carries no score. Before it, a partial pair waits for more data.
    """
    if data_coverage >= 1.0:
        return "complete", True
    if timeout_passed:
        if data_coverage < MIN_COVERAGE_FOR_FINAL:
            return "insufficient_data", True
        return "partial", True
    return "partial", False


def build_score_row(
    round_id: int,
    model_id: int,
    series_id: int,
    forecast_count: int,
    evaluation_data: List[Dict[str, Any]],
    scale_row: Optional[Dict[str, Any]],
    timeout_passed: bool,
) -> Dict[str, Any]:
    """One `forecasts.forecast_scores` row for a pair with in-window forecasts.

    `evaluation_data` are the pair's forecasts joined to the actuals. A point whose forecast
    or actual is not a finite number is not evaluated, so it counts against coverage like a
    missing one (the arena scorer stores NaN or Infinity instead). `scale_row` is the
    (round, series) row of `forecasts.series_scale`, the same for every model; when it is
    missing, NULL or 0 the pair gets `undefined_scale` and no MASE, for every model alike.
    """
    points = [
        p for p in evaluation_data
        if _finite(p["predicted_value"]) and _finite(p["actual_value"])
    ]
    row: Dict[str, Any] = {
        "round_id": round_id,
        "model_id": model_id,
        "series_id": series_id,
        "mae": None,
        "rmse": None,
        "naive_mae": None,
        "n_points": len(points),
        "scale": scale_row["scale"] if scale_row else None,
        "mase": None,
        "sql_score": None,
        "sql_per_quantile": None,
        "has_quantiles": None,
        "quantile_levels_count": None,
        "quantile_crossing_count": None,
        "forecast_count": forecast_count,
        "data_coverage": 0.0,
        "final_evaluation": timeout_passed,
        "evaluation_status": "no_overlap",
        "error_message": "No overlapping timestamps between forecasts and actuals",
        "method": MASE_METHOD,
    }
    if not points:
        if evaluation_data:
            row["error_message"] = "No overlapping timestamp has a finite forecast and actual"
        return row

    data_coverage = len(points) / forecast_count
    status, final = coverage_verdict(data_coverage, timeout_passed)
    row.update(
        data_coverage=data_coverage,
        evaluation_status=status,
        final_evaluation=final,
        error_message=None,
    )
    if status == "insufficient_data":
        row["error_message"] = (
            f"Timeout: only {data_coverage:.1%} coverage "
            f"(min {MIN_COVERAGE_FOR_FINAL:.0%} required)"
        )
        return row

    row.update(compute_score_fields(
        np.array([p["actual_value"] for p in points], dtype=float),
        np.array([p["predicted_value"] for p in points], dtype=float),
        [p.get("probabilistic_values") for p in points],
        row["scale"],
        scale_row.get("last_value") if scale_row else None,
    ))
    if not mase_scale_defined(row["scale"]):
        row["evaluation_status"] = "undefined_scale"
        row["error_message"] = (
            "No context as of round creation to scale by" if scale_row is None
            else "Context has no variation at lag m (scale 0) or no lag pair"
        )
    return row


class _SeriesActualsCache:
    """Per-series actuals for one round, keyed by minute-truncated ts, one query per series.

    Every model on a series is scored against the same actuals. The ts window is the widest
    range any model forecast for the series, padded by `_ACTUALS_TS_MARGIN`. With
    `raw_fallback` (backfill only), a series the continuous aggregate has nothing for is read
    from raw data, bucketed alike: dev restores lack old aggregate periods.
    """

    def __init__(
        self,
        forecast_repo: ForecastRepository,
        resolution: str,
        bounds: Dict[int, Tuple[datetime, datetime]],
        raw_fallback: bool = False,
    ):
        self._forecast_repo = forecast_repo
        self._resolution = resolution
        self._bounds = bounds
        self._raw_fallback = raw_fallback
        self._by_minute: Dict[int, Dict[datetime, List[float]]] = {}

    async def by_minute(self, series_id: int) -> Dict[datetime, List[float]]:
        if series_id not in self._by_minute:
            rows: List[Dict[str, Any]] = []
            bounds = self._bounds.get(series_id)
            if bounds is not None:
                lo, hi = bounds[0] - _ACTUALS_TS_MARGIN, bounds[1] + _ACTUALS_TS_MARGIN
                rows = await self._forecast_repo.get_series_actuals_aggregate(
                    series_id, self._resolution, lo, hi
                )
                if not rows and self._raw_fallback:
                    rows = await self._forecast_repo.get_series_actuals_raw_bucketed(
                        series_id, self._resolution, lo, hi
                    )
            self._by_minute[series_id] = group_actuals_by_minute(rows)
        return self._by_minute[series_id]


@dataclass
class _RoundInputs:
    round_info: Any
    resolution: str
    forecasts_by_pair: Dict[Tuple[int, int], List[Dict[str, Any]]]
    actuals: _SeriesActualsCache


class ForecastScoringService:
    def __init__(self, db_session: AsyncSession, raw_actuals_fallback: bool = False):
        self.db_session = db_session
        self.round_repo = ChallengeRoundRepository(db_session)
        self.forecast_repo = ForecastRepository(db_session)
        self.scale_service = SeriesScaleService(db_session)
        self.raw_actuals_fallback = raw_actuals_fallback

    @property
    def repo(self) -> ForecastScoresRepository:
        return self.scale_service.repo

    async def tables_exist(self) -> bool:
        return await self.repo.tables_exist()

    async def capture_served_scales(self) -> int:
        """Store the scales of rounds whose context was just served. Commits per round.

        Returns the number of (round, series) scales stored. A round that fails is rolled
        back and logged; its scales are rebuilt from SCD2 when it is scored.
        """
        stored = 0
        for round_id, frequency in await self.repo.rounds_awaiting_capture(CAPTURE_LOOKBACK):
            try:
                n = await self.scale_service.store_served_scales(
                    round_id, timedelta_to_resolution(frequency)
                )
                await self.db_session.commit()
                stored += n
                logger.info(f"Round {round_id}: stored the MASE scale of {n} series as served")
            except Exception as e:
                await self.db_session.rollback()
                logger.exception(f"Round {round_id}: served MASE scales not stored: {e}")
        return stored

    async def rounds_to_score(self) -> List[int]:
        return await self.repo.rounds_to_score(SCORING_LOOKBACK)

    async def score_round(self, round_id: int) -> Optional[int]:
        """Score one round into `forecasts.forecast_scores`, in one transaction.

        Returns the number of rows written, or None when another process holds the round.
        """
        try:
            if not await self.repo.try_lock_round(round_id):
                await self.db_session.rollback()
                logger.info(f"Round {round_id} is being scored by another process; skipped")
                return None
            rows = await self.compute_round(round_id)
            written = await self.repo.upsert_forecast_scores(rows) if rows else 0
            await self.db_session.commit()
            logger.info(f"Round {round_id}: {written} forecast score row(s) written")
            return written
        except Exception:
            await self.db_session.rollback()
            raise

    async def compute_round(self, round_id: int) -> Optional[List[Dict[str, Any]]]:
        """`forecasts.forecast_scores` rows for one round, without writing them.

        Stores any scale it has to rebuild (not committed). None when the round has no
        in-window forecasts.
        """
        inputs = await self._load_round_inputs(round_id)
        if inputs is None:
            return None
        scales = await self.scale_service.ensure_scales(
            round_id, inputs.resolution, inputs.round_info.created_at
        )
        timeout_passed = evaluation_timeout_passed(inputs.round_info.end_time)

        rows = []
        for (model_id, series_id), forecast_rows in sorted(inputs.forecasts_by_pair.items()):
            args = (round_id, model_id, series_id, len(forecast_rows))
            try:
                evaluation_data = align_evaluation_data(
                    forecast_rows, await inputs.actuals.by_minute(series_id)
                )
                rows.append(build_score_row(
                    *args, evaluation_data, scales.get(series_id), timeout_passed
                ))
            except Exception as e:
                logger.exception(
                    f"Scoring failed for round {round_id}, model {model_id}, "
                    f"series {series_id}: {e}"
                )
                row = build_score_row(*args, [], scales.get(series_id), timeout_passed)
                row.update(evaluation_status="error", error_message=str(e)[:500])
                rows.append(row)
        return rows

    async def _load_round_inputs(self, round_id: int) -> Optional[_RoundInputs]:
        """The round's in-window forecasts, grouped by pair, and its actuals source."""
        round_info = await self.round_repo.get_by_id(round_id)
        if round_info is None:
            logger.warning(f"Round {round_id} not found")
            return None
        resolution = timedelta_to_resolution(round_info.frequency)

        windows = series_forecast_windows(
            await self.round_repo.get_series_context_edges(round_id),
            round_info.frequency,
            round_info.horizon,
        )
        forecasts_by_pair: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        dropped = 0
        for row in await self.forecast_repo.get_round_forecasts(round_id):
            window = windows.get(row["series_id"])
            if window is not None and not window[0] <= _as_utc(row["ts"]) <= window[1]:
                dropped += 1
                continue
            forecasts_by_pair.setdefault((row["model_id"], row["series_id"]), []).append(row)
        if dropped:
            logger.warning(f"Round {round_id}: {dropped} out-of-window forecast point(s) not scored")
        if not forecasts_by_pair:
            logger.info(f"Round {round_id}: no forecasts to score")
            return None

        bounds: Dict[int, Tuple[datetime, datetime]] = {}
        for (_, series_id), rows in forecasts_by_pair.items():
            timestamps = [_as_utc(r["ts"]) for r in rows]
            lo, hi = min(timestamps), max(timestamps)
            if series_id in bounds:
                lo, hi = min(lo, bounds[series_id][0]), max(hi, bounds[series_id][1])
            bounds[series_id] = (lo, hi)

        return _RoundInputs(
            round_info=round_info,
            resolution=resolution,
            forecasts_by_pair=forecasts_by_pair,
            actuals=_SeriesActualsCache(
                self.forecast_repo, resolution, bounds, raw_fallback=self.raw_actuals_fallback
            ),
        )
