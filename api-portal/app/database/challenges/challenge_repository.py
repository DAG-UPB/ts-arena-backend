from typing import List, Optional, Any, Dict
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database.challenges.challenge import Challenge, ChallengeContextData, ChallengeSeriesPseudo, VChallengeWithStatus, ChallengeParticipant
from app.database.data_portal.time_series import TimeSeriesModel

logger = logging.getLogger(__name__)


class ChallengeRepository:
    """Repository for challenge-related operations."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_challenge(self, **kwargs: Dict[str, Any]) -> Challenge:
        """
        Creates a new challenge in the database.
        """
        challenge = Challenge(**kwargs)
        self.session.add(challenge)
        await self.session.commit()
        await self.session.refresh(challenge)
        return challenge

    async def upsert_challenge(self, **kwargs: Dict[str, Any]) -> Challenge:
        """
        Creates a new challenge or returns existing one if name already exists.
        This provides database-level idempotency for challenge creation.
        """
        name = kwargs.get("name")
        if name and isinstance(name, str):
            existing = await self.get_challenge_by_name(name)
            if existing:
                logger.info(f"Challenge '{name}' already exists (ID: {existing.id}). Returning existing challenge (upsert).")
                return existing
        
        # If no name provided or doesn't exist, create new challenge
        logger.info(f"Creating new challenge: '{name}'")
        return await self.create_challenge(**kwargs)

    async def get_challenge_by_id(self, challenge_id: int) -> Optional[Challenge]:
        """Retrieves a challenge by its ID."""
        return await self.session.get(Challenge, challenge_id)

    async def get_by_id(self, challenge_id: int) -> Optional[Challenge]:
        """Alias for get_challenge_by_id for consistency."""
        return await self.get_challenge_by_id(challenge_id)

    async def get_challenge_by_name(self, name: str) -> Optional[Challenge]:
        """Retrieves a challenge by its name."""
        result = await self.session.execute(
            select(Challenge).where(Challenge.name == name)
        )
        return result.scalar_one_or_none()

    async def list_challenges(
        self, statuses: Optional[List[str]] = None
    ) -> List[VChallengeWithStatus]:
        """
        Lists challenges from the v_challenges_with_status view, optionally filtered by status.
        Results are ordered by creation date descending.
        """
        query = select(VChallengeWithStatus)
        if statuses:
            query = query.where(VChallengeWithStatus.status.in_(statuses))
        
        query = query.order_by(VChallengeWithStatus.created_at.desc())

        result = await self.session.execute(query)
        return result.scalars().all()


    async def update_preparation_params(
        self, 
        challenge_id: int, 
        preparation_params: Dict[str, Any]
    ) -> Optional[Challenge]:
        """
        Updates the preparation_params for a challenge.
        """
        challenge = await self.get_challenge_by_id(challenge_id)
        if not challenge:
            return None
        
        challenge.preparation_params = preparation_params
        await self.session.commit()
        await self.session.refresh(challenge)
        return challenge

    async def get_context_data(
        self,
        challenge_id: int,
        series_id: Optional[int] = None,
    ) -> List[ChallengeContextData]:
        """
        Retrieves context data for a given challenge, optionally filtered by series_id.
        """
        query = select(ChallengeContextData).where(
            ChallengeContextData.challenge_id == challenge_id
        )

        if series_id:
            query = query.where(ChallengeContextData.series_id == series_id)

        query = query.order_by(ChallengeContextData.ts)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_context_data_bulk(
        self,
        challenge_id: int,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves all context data for a given challenge, grouped by series_id.
        Returns a dictionary where keys are challenge_series_names and values are dicts containing
        'frequency' and 'data' (list of timestamped data points).
        """
        # Join context data with pseudo table to get challenge_series_name and time_series to get frequency
        query = (
            select(
                ChallengeSeriesPseudo.challenge_series_name,
                TimeSeriesModel.frequency,
                ChallengeContextData.ts,
                ChallengeContextData.value,
            )
            .join(
                ChallengeSeriesPseudo,
                (ChallengeSeriesPseudo.challenge_id == ChallengeContextData.challenge_id)
                & (ChallengeSeriesPseudo.series_id == ChallengeContextData.series_id)
            )
            .join(
                TimeSeriesModel,
                TimeSeriesModel.series_id == ChallengeContextData.series_id
            )
            .where(ChallengeContextData.challenge_id == challenge_id)
            .order_by(ChallengeSeriesPseudo.challenge_series_name, ChallengeContextData.ts)
        )

        result = await self.session.execute(query)

        # Group results by challenge_series_name
        grouped_data: Dict[str, Dict[str, Any]] = {}
        for row in result.mappings():
            key = row["challenge_series_name"]
            if key not in grouped_data:
                grouped_data[key] = {
                    "frequency": row["frequency"],
                    "data": []
                }

            grouped_data[key]["data"].append({"ts": row["ts"], "value": row["value"]})

        return grouped_data

    async def upsert_challenge_series_pseudo(self, entries: List[Dict[str, Any]]) -> None:
        """
        Inserts or updates challenge_series_pseudo rows.
        Each entry expects keys: challenge_id, series_id, challenge_series_name,
        and optionally: min_ts, max_ts, value_avg, value_std.
        On conflict (challenge_id, series_id) updates all fields.
        """
        # SQLAlchemy ORM bulk upsert varies; keep it simple: try insert and on conflict update using core.
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(ChallengeSeriesPseudo.__table__).values(entries)
        
        # Update all fields on conflict
        update_dict = {
            "challenge_series_name": stmt.excluded.challenge_series_name,
            "min_ts": stmt.excluded.min_ts,
            "max_ts": stmt.excluded.max_ts,
            "value_avg": stmt.excluded.value_avg,
            "value_std": stmt.excluded.value_std
        }
        
        do_update_stmt = stmt.on_conflict_do_update(
            index_elements=[ChallengeSeriesPseudo.challenge_id, ChallengeSeriesPseudo.series_id],
            set_=update_dict
        )
        await self.session.execute(do_update_stmt)
        # Don't commit here; leave transaction management to caller

    async def get_challenge_participants(self, challenge_id: int) -> List[Dict[str, Any]]:
        """
        Retrieves all participants for a given challenge.
        """
        query = select(ChallengeParticipant).where(ChallengeParticipant.challenge_id == challenge_id)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_challenge_series_ids(self, challenge_id: int) -> List[int]:
        """
        Retrieves all series_ids associated with a challenge.
        """
        query = select(ChallengeSeriesPseudo.series_id).where(ChallengeSeriesPseudo.challenge_id == challenge_id)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_challenge_series_pseudo(self, challenge_id: int, series_id: int) -> Optional[ChallengeSeriesPseudo]:
        """
        Retrieves ChallengeSeriesPseudo entry for a specific challenge and series.
        """
        query = select(ChallengeSeriesPseudo).where(
            ChallengeSeriesPseudo.challenge_id == challenge_id,
            ChallengeSeriesPseudo.series_id == series_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()