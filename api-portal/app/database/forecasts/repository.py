from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, func, desc

from .models import Forecast, ChallengeScore
from app.database.data_portal.time_series import TimeSeriesDataModel


class ForecastRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def bulk_create_forecasts(
        self, 
        challenge_id: int,
        model_id: int,
        series_id: int,
        forecast_data: List[Dict[str, Any]]
    ) -> int:
        """
    Bulk insert forecasts for a specific challenge, model, and series.
        Uses INSERT ... ON CONFLICT DO NOTHING to handle duplicates gracefully.
        
        Args:
            challenge_id: Challenge ID
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
                "challenge_id": challenge_id,
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
            index_elements=["challenge_id", "model_id", "series_id", "ts"]
        )
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    async def get_challenges_needing_evaluation(self) -> List[Dict[str, Any]]:
        """
        Get all challenges that need score evaluation.
        Includes challenges with no scores yet OR challenges with non-finalized scores.
        
        Returns:
            List of dicts with challenge_id and status
        """
        from sqlalchemy import text
        
        query = text("""
            SELECT DISTINCT c.id as challenge_id, c.status
            FROM challenges.v_challenges_with_status c
            LEFT JOIN forecasts.challenge_scores cs ON c.id = cs.challenge_id
            WHERE c.status IN ('active', 'completed')
              AND (cs.id IS NULL OR cs.final_evaluation = FALSE)
            ORDER BY c.id
        """)
        
        result = await self.session.execute(query)
        return [{"challenge_id": row.challenge_id, "status": row.status} for row in result]

    async def mark_challenge_scores_final(self, challenge_id: int) -> int:
        """
        Set final_evaluation=True for all scores of a given challenge.
        
        Returns:
            Number of rows updated
        """
        from sqlalchemy import update
        
        stmt = update(ChallengeScore).where(
            ChallengeScore.challenge_id == challenge_id
        ).values(final_evaluation=True)
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    async def get_challenge_participants(self, challenge_id: int) -> List[int]:
        """
        Get unique model_ids that have submitted forecasts for a challenge.
        
        Returns:
            List of model_ids
        """
        result = await self.session.execute(
            select(Forecast.model_id)
            .where(Forecast.challenge_id == challenge_id)
            .distinct()
        )
        return [row[0] for row in result]

    async def get_challenge_series_ids(self, challenge_id: int) -> List[int]:
        """
        Get unique series_ids for a challenge.
        
        Returns:
            List of series_ids
        """
        result = await self.session.execute(
            select(Forecast.series_id)
            .where(Forecast.challenge_id == challenge_id)
            .distinct()
        )
        return [row[0] for row in result]

    async def get_forecast_stats(
        self,
        challenge_id: int,
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
                    Forecast.challenge_id == challenge_id,
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
        challenge_id: int,
        model_id: int,
        series_id: int
    ) -> int:
        """
        Count existing forecasts for a challenge/model/series combination.
        
        Returns:
            Number of existing forecast records
        """
        result = await self.session.execute(
            select(func.count(Forecast.id))
            .where(
                and_(
                    Forecast.challenge_id == challenge_id,
                    Forecast.model_id == model_id,
                    Forecast.series_id == series_id
                )
            )
        )
        return result.scalar_one()

    async def get_forecasts_by_challenge_and_model(
        self, 
        challenge_id: int, 
        model_id: int,
        series_id: Optional[int] = None
    ) -> List[Forecast]:
        """
        Retrieve forecasts for a specific model in a challenge.
        Optionally filter by series_id.
        """
        conditions = [
            Forecast.challenge_id == challenge_id,
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
        challenge_id: int,
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
                    Forecast.challenge_id == challenge_id,
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
        challenge_id: int,
        model_id: int,
        series_id: Optional[int] = None
    ) -> int:
        """
        Delete forecasts for a challenge/model combination.
        Optionally filter by series_id.
        
        Returns:
            Number of deleted rows
        """
        from sqlalchemy import delete as sql_delete
        
        conditions = [
            Forecast.challenge_id == challenge_id,
            Forecast.model_id == model_id
        ]
        
        if series_id is not None:
            conditions.append(Forecast.series_id == series_id)
        
        stmt = sql_delete(Forecast).where(and_(*conditions))
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        return result.rowcount if result.rowcount else 0

    # === Challenge Scores ===
    
    async def create_or_update_challenge_score(
        self, 
        score_data: Dict[str, Any]
    ) -> ChallengeScore:
        """
        Create or update a challenge score using UPSERT.
        """
        stmt = insert(ChallengeScore).values(**score_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["challenge_id", "model_id", "series_id"],
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
                    ChallengeScore.challenge_id == score_data["challenge_id"],
                    ChallengeScore.model_id == score_data["model_id"],
                    ChallengeScore.series_id == score_data["series_id"]
                )
            )
        )
        return result.scalar_one()

    async def get_scores_by_challenge(self, challenge_id: int) -> List[ChallengeScore]:
        """
        Retrieve all scores for a given challenge, including model information.
        """
        result = await self.session.execute(
            select(ChallengeScore)
            .where(ChallengeScore.challenge_id == challenge_id)
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
            index_elements=["challenge_id", "model_id", "series_id"],
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

    async def check_all_scores_complete(self, challenge_id: int) -> bool:
        """
        Check if all scores for a challenge have 100% data coverage.
        
        Returns:
            True if all scores have evaluation_status = 'complete'
        """
        from sqlalchemy import text
        
        query = text("""
            SELECT 
                COUNT(*) as total_scores,
                COUNT(*) FILTER (WHERE evaluation_status = 'complete') as complete_scores
            FROM forecasts.challenge_scores
            WHERE challenge_id = :challenge_id
        """)
        
        result = await self.session.execute(query, {"challenge_id": challenge_id})
        row = result.fetchone()
        
        if not row or row.total_scores == 0:
            return False
        
        return row.complete_scores == row.total_scores
