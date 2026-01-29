from typing import List, Dict, Any, Optional, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, func, desc

from .models import Forecast, ChallengeScore
from app.database.data_portal.time_series import (
    TimeSeriesDataModel,
    TimeSeriesData15minModel,
    TimeSeriesData1hModel,
    TimeSeriesData1dModel
)

# Maps resolution strings to the appropriate Continuous Aggregate Model for evaluation
EVALUATION_RESOLUTION_MAP: Dict[str, Type] = {
    "15min": TimeSeriesData15minModel,
    "15 minutes": TimeSeriesData15minModel,
    "1h": TimeSeriesData1hModel,
    "1 hour": TimeSeriesData1hModel,
    "1d": TimeSeriesData1dModel,
    "1 day": TimeSeriesData1dModel,
    "raw": TimeSeriesDataModel,
}


class ForecastRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def bulk_create_forecasts(
        self, 
        round_id: int,
        model_id: int,
        series_id: int,
        forecast_data: List[Dict[str, Any]]
    ) -> int:
        """
        Bulk insert forecasts for a specific challenge round, model, and series.
        Uses INSERT ... ON CONFLICT DO NOTHING to handle duplicates gracefully.
        
        Args:
            round_id: Challenge Round ID
            model_id: Model ID
            series_id: Underlying time series ID (resolved from challenge_series_name)
            forecast_data: List of dicts with 'timestamp', 'value', 'probabilistic_values'
        
        Returns:
            Number of rows inserted
        """
        if not forecast_data:
            return 0
        
        # Prepare data for bulk insert
        mappings = [
            {
                "round_id": round_id,
                "model_id": model_id,
                "series_id": series_id,
                "ts": dp["ts"],
                "predicted_value": dp["value"],
                "probabilistic_values": dp.get("probabilistic_values"),
            }
            for dp in forecast_data
        ]
        
        # Use PostgreSQL INSERT ... ON CONFLICT DO NOTHING
        stmt = insert(Forecast).values(mappings)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["round_id", "model_id", "series_id", "ts"]
        )
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    async def get_ids_needing_evaluation(self) -> List[int]:
        """
        Get all round_ids that need score evaluation.
        Includes rounds with no scores yet OR rounds with non-finalized scores.
        
        Returns:
            List of round_ids
        """
        from sqlalchemy import text
        
        query = text("""
            SELECT DISTINCT c.id as round_id
            FROM challenges.v_rounds_with_status c
            LEFT JOIN forecasts.scores cs ON c.id = cs.round_id
            WHERE c.computed_status IN ('active', 'completed')
              AND (cs.id IS NULL OR cs.final_evaluation = FALSE)
            ORDER BY c.id
        """)
        
        result = await self.session.execute(query)
        return [row.round_id for row in result]

    async def mark_scores_final(self, round_id: int) -> int:
        """
        Set final_evaluation=True for all scores of a given round.
        
        Returns:
            Number of rows updated
        """
        from sqlalchemy import update
        
        stmt = update(ChallengeScore).where(
            ChallengeScore.round_id == round_id
        ).values(final_evaluation=True)
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    async def get_round_participants(self, round_id: int) -> List[int]:
        """
        Get unique model_ids that have submitted forecasts for a round.
        
        Returns:
            List of model_ids
        """
        result = await self.session.execute(
            select(Forecast.model_id)
            .where(Forecast.round_id == round_id)
            .distinct()
        )
        return [row[0] for row in result]

    async def get_round_series_ids(self, round_id: int) -> List[int]:
        """
        Get unique series_ids for a round.
        
        Returns:
            List of series_ids
        """
        result = await self.session.execute(
            select(Forecast.series_id)
            .where(Forecast.round_id == round_id)
            .distinct()
        )
        return [row[0] for row in result]

    async def get_forecast_stats(
        self,
        round_id: int,
        model_id: int,
        series_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get stats (min_ts, max_ts, count) of forecasts for a specific model/series.
        
        Returns:
            Dict with 'min_ts', 'max_ts', 'count' or None if no forecasts found.
        """
        result = await self.session.execute(
            select(
                func.min(Forecast.ts).label("min_ts"),
                func.max(Forecast.ts).label("max_ts"),
                func.count(Forecast.id).label("count")
            ).where(
                and_(
                    Forecast.round_id == round_id,
                    Forecast.model_id == model_id,
                    Forecast.series_id == series_id
                )
            )
        )
        row = result.first()
        if row and row.count > 0:
            return {
                "min_ts": row.min_ts,
                "max_ts": row.max_ts,
                "count": row.count
            }
        return None

    async def check_existing_forecasts(
        self,
        round_id: int,
        model_id: int,
        series_id: int
    ) -> int:
        """
        Count existing forecasts for a round/model/series combination.
        
        Returns:
            Number of existing forecast records
        """
        result = await self.session.execute(
            select(func.count(Forecast.id))
            .where(
                and_(
                    Forecast.round_id == round_id,
                    Forecast.model_id == model_id,
                    Forecast.series_id == series_id
                )
            )
        )
        return result.scalar_one()

    async def get_forecasts_by_round_and_model(
        self, 
        round_id: int, 
        model_id: int,
        series_id: Optional[int] = None
    ) -> List[Forecast]:
        """
        Retrieve forecasts for a specific model in a round.
        Optionally filter by series_id.
        """
        conditions = [
            Forecast.round_id == round_id,
            Forecast.model_id == model_id
        ]
        
        if series_id is not None:
            conditions.append(Forecast.series_id == series_id)
        
        result = await self.session.execute(
            select(Forecast)
            .where(and_(*conditions))
            .order_by(Forecast.series_id, Forecast.ts)
        )
        return result.scalars().all()

    async def get_evaluation_data(
        self,
        round_id: int,
        model_id: int,
        series_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get aligned forecast and actual data for evaluation.
        Performs an INNER JOIN between forecasts and time_series_data on series_id and ts.
        """
        # Join on series_id and timestamps truncated to the minute to ignore second/microsecond diffs
        # This makes the evaluation robust against ingestion delays or precision differences
        stmt = (
            select(
                Forecast.ts,
                Forecast.predicted_value,
                TimeSeriesDataModel.value.label("actual_value")
            )
            .join(
                TimeSeriesDataModel,
                and_(
                    Forecast.series_id == TimeSeriesDataModel.series_id,
                    func.date_trunc('minute', Forecast.ts) == func.date_trunc('minute', TimeSeriesDataModel.ts)
                )
            )
            .where(
                and_(
                    Forecast.round_id == round_id,
                    Forecast.model_id == model_id,
                    Forecast.series_id == series_id
                )
            )
            .order_by(Forecast.ts)
        )
        
        result = await self.session.execute(stmt)
        return [
            {
                "ts": row.ts, 
                "predicted_value": row.predicted_value, 
                "actual_value": row.actual_value
            } 
            for row in result
        ]

    async def get_evaluation_data_by_resolution(
        self,
        round_id: int,
        model_id: int,
        series_id: int,
        resolution: str = "1h"
    ) -> List[Dict[str, Any]]:
        """
        Get aligned forecast and actual data for evaluation.
        Reads actual values from the appropriate continuous aggregate view.
        
        Args:
            round_id: Round ID
            model_id: Model ID
            series_id: Time series ID
            resolution: Target resolution ("15min", "1h", "1d", "raw")
            
        Returns:
            List of dicts with 'ts', 'predicted_value', 'actual_value'
        """
        # Select the appropriate model based on resolution
        model = EVALUATION_RESOLUTION_MAP.get(resolution, TimeSeriesData1hModel)
        
        # Join on series_id and timestamps truncated to the minute
        stmt = (
            select(
                Forecast.ts,
                Forecast.predicted_value,
                model.value.label("actual_value")
            )
            .join(
                model,
                and_(
                    Forecast.series_id == model.series_id,
                    func.date_trunc('minute', Forecast.ts) == func.date_trunc('minute', model.ts)
                )
            )
            .where(
                and_(
                    Forecast.round_id == round_id,
                    Forecast.model_id == model_id,
                    Forecast.series_id == series_id
                )
            )
            .order_by(Forecast.ts)
        )
        
        result = await self.session.execute(stmt)
        return [
            {
                "ts": row.ts, 
                "predicted_value": row.predicted_value, 
                "actual_value": row.actual_value
            } 
            for row in result
        ]

    async def delete_forecasts(
        self,
        round_id: int,
        model_id: int,
        series_id: Optional[int] = None
    ) -> int:
        """
        Delete forecasts for a round/model combination.
        Optionally filter by series_id.
        
        Returns:
            Number of deleted rows
        """
        from sqlalchemy import delete as sql_delete
        
        conditions = [
            Forecast.round_id == round_id,
            Forecast.model_id == model_id
        ]
        
        if series_id is not None:
            conditions.append(Forecast.series_id == series_id)
        
        stmt = sql_delete(Forecast).where(and_(*conditions))
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    # === Challenge Scores ===
    
    async def create_or_update_score(
        self, 
        score_data: Dict[str, Any]
    ) -> ChallengeScore:
        """
        Create or update a challenge score using UPSERT.
        """
        stmt = insert(ChallengeScore).values(**score_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["round_id", "model_id", "series_id"],
            set_={
                "mase": stmt.excluded.mase,
                "rmse": stmt.excluded.rmse,
                "forecast_count": stmt.excluded.forecast_count,
                "actual_count": stmt.excluded.actual_count,
                "evaluated_count": stmt.excluded.evaluated_count,
                "data_coverage": stmt.excluded.data_coverage,
                "evaluation_status": stmt.excluded.evaluation_status,
                "error_message": stmt.excluded.error_message,
                "final_evaluation": stmt.excluded.final_evaluation,
                "calculated_at": func.now()
            }
        )
        
        await self.session.execute(stmt)
        await self.session.commit()
        
        # Retrieve the inserted/updated record
        result = await self.session.execute(
            select(ChallengeScore)
            .where(
                and_(
                    ChallengeScore.round_id == score_data["round_id"],
                    ChallengeScore.model_id == score_data["model_id"],
                    ChallengeScore.series_id == score_data["series_id"]
                )
            )
        )
        return result.scalar_one()

    async def get_scores_by_round(self, round_id: int) -> List[ChallengeScore]:
        """
        Retrieve all scores for a given round, including model information.
        """
        result = await self.session.execute(
            select(ChallengeScore)
            .where(ChallengeScore.round_id == round_id)
            .options(selectinload(ChallengeScore.model))
            .order_by(ChallengeScore.mase.asc())
        )
        return result.scalars().all()

    async def bulk_insert_scores(self, scores_data: List[Dict[str, Any]]) -> int:
        """
        Bulk insert scores.
        """
        if not scores_data:
            return 0
        
        stmt = insert(ChallengeScore).values(scores_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["round_id", "model_id", "series_id"],
            set_={
                "mase": stmt.excluded.mase,
                "rmse": stmt.excluded.rmse,
                "forecast_count": stmt.excluded.forecast_count,
                "actual_count": stmt.excluded.actual_count,
                "evaluated_count": stmt.excluded.evaluated_count,
                "data_coverage": stmt.excluded.data_coverage,
                "evaluation_status": stmt.excluded.evaluation_status,
                "error_message": stmt.excluded.error_message,
                "final_evaluation": stmt.excluded.final_evaluation,
                "calculated_at": func.now()
            }
        )
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    async def check_all_scores_complete(self, round_id: int) -> bool:
        """
        Check if all scores for a round have 100% data coverage.
        
        Returns:
            True if all scores have evaluation_status = 'complete'
        """
        from sqlalchemy import text
        
        query = text("""
            SELECT 
                COUNT(*) as total_scores,
                COUNT(*) FILTER (WHERE evaluation_status = 'complete') as complete_scores
            FROM forecasts.scores
            WHERE round_id = :round_id
        """)
        
        result = await self.session.execute(query, {"round_id": round_id})
        row = result.fetchone()
        
        if not row or row.total_scores == 0:
            return False
        
        return row.complete_scores == row.total_scores
