from typing import List, Optional, Any, Dict
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database.challenges.challenge import (
    ChallengeDefinition, 
    ChallengeDefinitionSeriesScd2,
    ChallengeRound, 
    ChallengeContextData, 
    ChallengeSeriesPseudo, 
    VChallengeRoundWithStatus, 
    ChallengeParticipant
)
from app.database.data_portal.time_series import TimeSeriesModel

logger = logging.getLogger(__name__)


class ChallengeDefinitionRepository:
    """Repository for challenge definition operations."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_definition(self, **kwargs: Dict[str, Any]) -> ChallengeDefinition:
        """
        Creates or updates a challenge definition based on schedule_id.
        """
        schedule_id = kwargs.get("schedule_id")
        if schedule_id:
            existing = await self.get_by_schedule_id(schedule_id)
            if existing:
                # Update existing definition
                for key, value in kwargs.items():
                    if hasattr(existing, key) and key != "id":
                        setattr(existing, key, value)
                await self.session.commit()
                await self.session.refresh(existing)
                return existing
        
        # Create new definition
        definition = ChallengeDefinition(**kwargs)
        self.session.add(definition)
        await self.session.commit()
        await self.session.refresh(definition)
        return definition

    async def get_by_id(self, definition_id: int) -> Optional[ChallengeDefinition]:
        """Retrieves a challenge definition by its ID."""
        return await self.session.get(ChallengeDefinition, definition_id)

    async def get_by_schedule_id(self, schedule_id: str) -> Optional[ChallengeDefinition]:
        """Retrieves a challenge definition by its schedule_id."""
        result = await self.session.execute(
            select(ChallengeDefinition).where(ChallengeDefinition.schedule_id == schedule_id)
        )
        return result.scalar_one_or_none()

    async def list_active(self) -> List[ChallengeDefinition]:
        """Lists all active challenge definitions."""
        result = await self.session.execute(
            select(ChallengeDefinition)
            .where(ChallengeDefinition.is_active == True)
            .order_by(ChallengeDefinition.name)
        )
        return result.scalars().all()

    async def list_all(self) -> List[ChallengeDefinition]:
        """Lists all challenge definitions."""
        result = await self.session.execute(
            select(ChallengeDefinition).order_by(ChallengeDefinition.name)
        )
        return result.scalars().all()

    async def upsert_series_assignment(
        self,
        definition_id: int,
        series_id: int,
        is_required: bool = True
    ) -> ChallengeDefinitionSeriesScd2:
        """
        Adds or updates a series assignment (SCD2 style).
        If series already current, does nothing. If not, creates new current record.
        """
        # Check if already current
        result = await self.session.execute(
            select(ChallengeDefinitionSeriesScd2).where(
                ChallengeDefinitionSeriesScd2.definition_id == definition_id,
                ChallengeDefinitionSeriesScd2.series_id == series_id,
                ChallengeDefinitionSeriesScd2.is_current == True
            )
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            if existing.is_required == is_required:
                return existing  # No change needed
            # Close existing and create new
            from datetime import datetime, timezone
            existing.valid_to = datetime.now(timezone.utc)
            existing.is_current = False
        
        # Create new current assignment
        new_assignment = ChallengeDefinitionSeriesScd2(
            definition_id=definition_id,
            series_id=series_id,
            is_required=is_required
        )
        self.session.add(new_assignment)
        await self.session.commit()
        await self.session.refresh(new_assignment)
        return new_assignment

    async def remove_series_assignment(self, definition_id: int, series_id: int) -> bool:
        """
        Closes the current series assignment (SCD2 style).
        Returns True if assignment was found and closed.
        """
        from datetime import datetime, timezone
        result = await self.session.execute(
            select(ChallengeDefinitionSeriesScd2).where(
                ChallengeDefinitionSeriesScd2.definition_id == definition_id,
                ChallengeDefinitionSeriesScd2.series_id == series_id,
                ChallengeDefinitionSeriesScd2.is_current == True
            )
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            existing.valid_to = datetime.now(timezone.utc)
            existing.is_current = False
            await self.session.commit()
            return True
        return False

    async def get_current_series_ids(self, definition_id: int) -> List[int]:
        """Gets all currently assigned series_ids for a definition."""
        result = await self.session.execute(
            select(ChallengeDefinitionSeriesScd2.series_id).where(
                ChallengeDefinitionSeriesScd2.definition_id == definition_id,
                ChallengeDefinitionSeriesScd2.is_current == True
            )
        )
        return result.scalars().all()


class ChallengeRoundRepository:
    """Repository for challenge round operations."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_round(self, **kwargs: Dict[str, Any]) -> ChallengeRound:
        """Creates a new challenge round in the database."""
        round_obj = ChallengeRound(**kwargs)
        self.session.add(round_obj)
        await self.session.commit()
        await self.session.refresh(round_obj)
        return round_obj

    async def upsert_round(self, **kwargs: Dict[str, Any]) -> ChallengeRound:
        """
        Creates a new round or returns existing one if name already exists.
        This provides database-level idempotency for round creation.
        """
        name = kwargs.get("name")
        if name and isinstance(name, str):
            existing = await self.get_by_name(name)
            if existing:
                logger.info(f"Round '{name}' already exists (ID: {existing.id}). Returning existing round.")
                return existing
        
        logger.info(f"Creating new round: '{name}'")
        return await self.create_round(**kwargs)

    async def get_by_id(self, round_id: int) -> Optional[ChallengeRound]:
        """Retrieves a challenge round by its ID."""
        return await self.session.get(ChallengeRound, round_id)

    async def get_by_name(self, name: str) -> Optional[ChallengeRound]:
        """Retrieves a challenge round by its name."""
        result = await self.session.execute(
            select(ChallengeRound).where(ChallengeRound.name == name)
        )
        return result.scalar_one_or_none()

    async def list_rounds(
        self, 
        statuses: Optional[List[str]] = None,
        definition_id: Optional[int] = None
    ) -> List[VChallengeRoundWithStatus]:
        """
        Lists challenge rounds from the view, optionally filtered by status or definition.
        Results are ordered by creation date descending.
        """
        query = select(VChallengeRoundWithStatus)
        if statuses:
            query = query.where(VChallengeRoundWithStatus.status.in_(statuses))
        if definition_id:
            query = query.where(VChallengeRoundWithStatus.definition_id == definition_id)
        
        query = query.order_by(VChallengeRoundWithStatus.created_at.desc())

        result = await self.session.execute(query)
        return result.scalars().all()

    async def update_status(self, round_id: int, status: str) -> Optional[ChallengeRound]:
        """Updates the status of a challenge round."""
        round_obj = await self.get_by_id(round_id)
        if not round_obj:
            return None
        
        round_obj.status = status
        await self.session.commit()
        await self.session.refresh(round_obj)
        return round_obj



    async def get_context_data(
        self,
        round_id: int,
        series_id: Optional[int] = None,
    ) -> List[ChallengeContextData]:
        """Retrieves context data for a given round, optionally filtered by series_id."""
        query = select(ChallengeContextData).where(
            ChallengeContextData.round_id == round_id
        )

        if series_id:
            query = query.where(ChallengeContextData.series_id == series_id)

        query = query.order_by(ChallengeContextData.ts)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_context_data_bulk(
        self,
        round_id: int,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves all context data for a given round, grouped by series_id.
        Returns a dictionary where keys are challenge_series_names and values are dicts containing
        'frequency' and 'data' (list of timestamped data points).
        """
        query = (
            select(
                ChallengeSeriesPseudo.challenge_series_name,
                TimeSeriesModel.frequency,
                ChallengeContextData.ts,
                ChallengeContextData.value,
            )
            .join(
                ChallengeSeriesPseudo,
                (ChallengeSeriesPseudo.round_id == ChallengeContextData.round_id)
                & (ChallengeSeriesPseudo.series_id == ChallengeContextData.series_id)
            )
            .join(
                TimeSeriesModel,
                TimeSeriesModel.series_id == ChallengeContextData.series_id
            )
            .where(ChallengeContextData.round_id == round_id)
            .order_by(ChallengeSeriesPseudo.challenge_series_name, ChallengeContextData.ts)
        )

        result = await self.session.execute(query)

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

    async def upsert_series_pseudo(self, entries: List[Dict[str, Any]]) -> None:
        """
        Inserts or updates challenge_series_pseudo rows.
        """
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(ChallengeSeriesPseudo.__table__).values(entries)
        
        update_dict = {
            "challenge_series_name": stmt.excluded.challenge_series_name,
            "min_ts": stmt.excluded.min_ts,
            "max_ts": stmt.excluded.max_ts,
            "value_avg": stmt.excluded.value_avg,
            "value_std": stmt.excluded.value_std
        }
        
        do_update_stmt = stmt.on_conflict_do_update(
            index_elements=[ChallengeSeriesPseudo.round_id, ChallengeSeriesPseudo.series_id],
            set_=update_dict
        )
        await self.session.execute(do_update_stmt)

    async def get_participants(self, round_id: int) -> List[ChallengeParticipant]:
        """Retrieves all participants for a given round."""
        query = select(ChallengeParticipant).where(ChallengeParticipant.round_id == round_id)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_series_ids(self, round_id: int) -> List[int]:
        """Retrieves all series_ids associated with a round."""
        query = select(ChallengeSeriesPseudo.series_id).where(ChallengeSeriesPseudo.round_id == round_id)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_series_pseudo(self, round_id: int, series_id: int) -> Optional[ChallengeSeriesPseudo]:
        """Retrieves ChallengeSeriesPseudo entry for a specific round and series."""
        query = select(ChallengeSeriesPseudo).where(
            ChallengeSeriesPseudo.round_id == round_id,
            ChallengeSeriesPseudo.series_id == series_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

