import logging
from typing import Dict, List, Optional, Tuple
import uuid
from statistics import mean

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.repositories.challenge_repository import ChallengeRepository
from app.database.repositories.forecast_repository import ForecastRepository
from app.database.repositories.statistics_repository import StatisticsRepository

logger = logging.getLogger(__name__)


class StatisticsService:
    """Service to compute and persist statistics (MASE) for challenges.

    The service will:
    - For each challenge_data (series) of a challenge collect last context value,
      test data and forecasts per model.
    - Compute the MASE for each model on that series using a naive forecast
      that always predicts the last context value.
    - Persist series-level MASEs into `model_challenge_series_scores` and
      upsert aggregated model-level scores into `model_challenge_scores`.
    """

    def __init__(self, db_session: AsyncSession):
        self.challenge_repo = ChallengeRepository(db_session)
        self.forecast_repo = ForecastRepository(db_session)
        self.statistics_repo = StatisticsRepository(db_session)

    async def compute_and_persist_statistics_for_challenge(
        self, challenge_id: uuid.UUID
    ) -> Dict[str, Optional[float]]:
        """Compute MASE for all models in the given challenge and persist results.

        Returns a mapping model_id (str) -> average_mase (float|None).
        """
        # 1) Load all challenge_data rows (one per series selected for the challenge)
        cds = await self.challenge_repo.get_challenge_data(challenge_id)
        if not cds:
            logger.info(f"No challenge_data found for challenge {challenge_id}")
            return {}

        # Keep per-model per-series mase values
        model_series_mases: Dict[str, List[Optional[float]]] = {}

        # Iterate series (challenge_data)
        for cd in cds:
            series_id = cd.series_id
            challenge_data_id = cd.id

            # last value of context (naive forecast baseline)
            last_ctx_point = await self.challenge_repo.get_latest_context_data_point(challenge_id, series_id)
            if not last_ctx_point:
                logger.debug(f"No last context point for challenge_data {challenge_data_id}, skipping series")
                continue
            last_ctx_value = last_ctx_point.value

            # test data
            test_points = await self.challenge_repo.get_test_data(challenge_id, series_id)
            if not test_points:
                logger.debug(f"No test data for challenge_data {challenge_data_id}, skipping series")
                continue
            # Map timestamps -> test value for quick lookup
            test_map = {tp.ts: tp.value for tp in test_points}

            # forecasts metadata for this series (filter by challenge_data_id)
            all_forecasts_for_series = await self.forecast_repo.get_all_forecasts_for_series(series_id)
            relevant_forecasts = [f for f in all_forecasts_for_series if getattr(f, "challenge_data_id", None) == challenge_data_id]

            # group forecasts by model_id
            forecasts_by_model: Dict[str, List] = {}
            for fmeta in relevant_forecasts:
                forecasts_by_model.setdefault(str(fmeta.model_id), []).append(fmeta)

            # For each model, try to compute MASE on this series. If multiple forecasts per model exist
            # for the same challenge_data, we compute MASE per forecast and average them.
            for model_id, fmetas in forecasts_by_model.items():
                series_mases: List[float] = []
                for fmeta in fmetas:
                    # get forecast data points
                    fdata = await self.forecast_repo.get_forecast_data(fmeta.forecast_id)
                    # fdata: list of dicts with keys timestamp, value
                    # align forecast points with test timestamps
                    paired: List[Tuple[float, float]] = []  # (test_value, forecast_value)
                    for row in fdata:
                        ts = row.get("ts")
                        fv = row.get("value")
                        if fv is None:
                            continue
                        if ts in test_map:
                            paired.append((test_map[ts], fv))

                    if not paired:
                        logger.debug(f"No overlapping timestamps between forecast {fmeta.forecast_id} and test data; skipping")
                        continue

                    # compute MAE_model and MAE_naive (naive uses last_ctx_value for all timestamps)
                    errors_model = [abs(tv - fv) for tv, fv in paired]
                    errors_naive = [abs(tv - last_ctx_value) for tv, _ in paired]

                    mae_model = mean(errors_model) if errors_model else None
                    mae_naive = mean(errors_naive) if errors_naive else None

                    if mae_model is None or mae_naive in (None, 0):
                        # Can't compute a meaningful MASE; store null-like indicator
                        mase_val: Optional[float] = None
                    else:
                        mase_val = mae_model / mae_naive

                    if mase_val is not None:
                        series_mases.append(mase_val)

                    # persist per-series score (may be None -> stored as NULL)
                    try:
                        await self.statistics_repo.add_series_score(
                            challenge_id=challenge_id,
                            challenge_data_id=challenge_data_id,
                            model_id=uuid.UUID(model_id),
                            mase=mase_val,
                        )
                    except Exception as e:
                        logger.exception(f"Failed to persist series score for model {model_id}, challenge_data {challenge_data_id}: {e}")

                # average across multiple forecasts from same model for this series
                if series_mases:
                    avg_series_mase = mean(series_mases)
                    model_series_mases.setdefault(model_id, []).append(avg_series_mase)
                else:
                    # keep a placeholder None so aggregated average ignores it (DB avg ignores NULLs)
                    model_series_mases.setdefault(model_id, []).append(None)

        # After processing all series, compute aggregated per-model average MASE and upsert into model_challenge_scores
        aggregated: Dict[str, Optional[float]] = {}
        for model_id, mase_list in model_series_mases.items():
            # filter out None entries
            numeric = [m for m in mase_list if m is not None]
            if not numeric:
                avg_mase = None
            else:
                avg_mase = mean(numeric)

            aggregated[model_id] = avg_mase

            # call repository to upsert challenge-level score (this will compute avg again server-side if preferred)
            try:
                await self.statistics_repo.compute_and_upsert_model_challenge_score(challenge_id, uuid.UUID(model_id))
            except Exception:
                logger.exception(f"Failed to upsert aggregated model_challenge_score for model {model_id} and challenge {challenge_id}")

        return aggregated

    async def get_completed_challenges_missing_statistics(self) -> List[uuid.UUID]:
        """Return challenge_ids for challenges with status 'completed' that
        currently have no entries in `model_challenge_scores`.

        Delegates the DB-side comparison to `StatisticsRepository.get_completed_challenges_without_statistics`.
        """
        return await self.statistics_repo.get_completed_challenges_without_statistics()
