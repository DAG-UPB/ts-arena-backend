from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any, Dict
import random
import logging
import hashlib
import isodate

from app.schemas.challenge import (
    ChallengeRoundCreate, ChallengeRoundFull, ChallengeRoundResponse, 
    ChallengeDefinitionResponse, ChallengeContextData, ContextDataPoint,
    ChallengeRoundData
)
from app.database.challenges.challenge_repository import (
    ChallengeDefinitionRepository, ChallengeRoundRepository
)
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.forecasts.repository import ForecastRepository

logger = logging.getLogger(__name__)


class ChallengeService:
    
    # Mapping from challenge frequency to resolution view
    FREQUENCY_TO_RESOLUTION = {
        "15 minutes": "15min",
        "1 hour": "1h",
        "PT1H": "1h",
        "1 day": "1d",
        "P1D": "1d",
        "15 minutes": "15min",
        "PT15M": "15min",
    }
    
    def __init__(self, db_session, scheduler=None):
        self.definition_repository = ChallengeDefinitionRepository(db_session)
        self.round_repository = ChallengeRoundRepository(db_session)
        self.time_series_repository = TimeSeriesRepository(db_session)
        self.forecast_repository = ForecastRepository(db_session)
        self.db_session = db_session
        self.scheduler = scheduler
        

    
    def _frequency_to_resolution(self, frequency: str) -> str:
        """Maps challenge frequency to view resolution."""
        resolution = self.FREQUENCY_TO_RESOLUTION.get(frequency)
        if not resolution:
            logger.warning(f"Unknown frequency '{frequency}', defaulting to '1h'")
            return "1h"
        return resolution

    # ==========================================================
    # Definition operations
    # ==========================================================
    
    async def sync_definition_from_yaml(
        self, 
        schedule_id: str, 
        schedule_config: Dict[str, Any]
    ) -> int:
        """
        Syncs a challenge definition from YAML config to database.
        Returns the definition ID.
        
        Also syncs the series assignments using unique_id from YAML.
        """
        params = schedule_config.get("params", {})
        
        def parse_duration(duration_str: str) -> timedelta:
            parts = duration_str.split()
            value = int(parts[0])
            unit = parts[1].lower()
            if "minute" in unit:
                return timedelta(minutes=value)
            if "hour" in unit:
                return timedelta(hours=value)
            if "day" in unit:
                return timedelta(days=value)
            raise ValueError(f"Unsupported duration unit: {unit}")
        
        # Parse all duration fields
        frequency = parse_duration(params["frequency"])
        horizon = parse_duration(params["forecast_horizon"])
        # announce_lead removed
        registration_duration = parse_duration(params.get("registration_duration", "1 hour"))
        evaluation_delay = parse_duration(params.get("evaluation_delay", "0 hours")) if params.get("evaluation_delay") else None
        
        definition = await self.definition_repository.upsert_definition(
            schedule_id=schedule_id,
            name=params.get("description", schedule_id),
            description=params.get("description"),
            domains=[params.get("domain")] if params.get("domain") else params.get("domains", []),
            subdomains=[params.get("subdomain")] if params.get("subdomain") else params.get("subdomains", []),
            categories=[params.get("category")] if params.get("category") else params.get("categories", []),
            subcategories=[params.get("subcategory")] if params.get("subcategory") else params.get("subcategories", []),
            context_length=params["context_length"],
            horizon=horizon,
            frequency=frequency,
            cron_schedule=schedule_config.get("cron"),
            n_time_series=params["n_time_series"],
            # announce_lead is removed
            registration_duration=registration_duration,
            evaluation_delay=evaluation_delay,
            is_active=True,
            run_on_startup=schedule_config.get("run_on_startup", False),
        )
        
        # Sync series assignments using unique_ids from YAML
        required_unique_ids = params.get("required_time_series", [])
        if required_unique_ids:
            for unique_id in required_unique_ids:
                # Look up series_id by unique_id
                series = await self.time_series_repository.get_time_series_by_unique_id(unique_id)
                if series:
                    await self.definition_repository.upsert_series_assignment(
                        definition_id=definition.id,
                        series_id=series.series_id,
                        is_required=True
                    )
                else:
                    logger.warning(f"Time series with unique_id '{unique_id}' not found for definition '{schedule_id}'")
        
        logger.info(f"Synced definition '{schedule_id}' (ID: {definition.id}) with {len(required_unique_ids)} required series")
        return definition.id
    
    async def get_definition(self, definition_id: int) -> Optional[ChallengeDefinitionResponse]:
        """Get a single challenge definition by ID."""
        definition = await self.definition_repository.get_by_id(definition_id)
        if definition:
            return ChallengeDefinitionResponse.model_validate(definition, from_attributes=True)
        return None
    
    async def list_definitions(self, active_only: bool = True) -> List[ChallengeDefinitionResponse]:
        """List challenge definitions."""
        if active_only:
            definitions = await self.definition_repository.list_active()
        else:
            definitions = await self.definition_repository.list_all()
        return [
            ChallengeDefinitionResponse.model_validate(d, from_attributes=True) 
            for d in definitions
        ]

    # ==========================================================
    # Round operations  
    # ==========================================================
    
    async def create_round_from_definition(
        self, 
        definition_id: int
    ) -> ChallengeRoundFull:
        """
        Creates a new challenge round from a definition.
        """
        definition = await self.definition_repository.get_by_id(definition_id)
        if not definition:
            raise ValueError(f"Definition {definition_id} not found")
        
        now = datetime.now(timezone.utc)
        name = f"{definition.name} - {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        
        # Calculate timing windows
        # registration_start is now (immediate registration after creation)
        registration_start = now
        registration_end = registration_start + (definition.registration_duration or timedelta(hours=1))
        start_time = registration_end
        end_time = start_time + definition.horizon
        
        # Get required series_ids for this definition
        required_series_ids = await self.definition_repository.get_current_series_ids(definition_id)
        
        round_obj = await self.round_repository.upsert_round(
            definition_id=definition_id,
            name=name,
            description=definition.description,
            context_length=definition.context_length,
            horizon=definition.horizon,
            frequency=definition.frequency,
            registration_start=registration_start,
            registration_end=registration_end,
            start_time=start_time,
            end_time=end_time,
            status="registration",
        )

        # Schedule data preparation job
        await self._schedule_round_preparation(
            round_id=round_obj.id,
            registration_start=registration_start
        )
        
        return ChallengeRoundFull.model_validate(round_obj, from_attributes=True)



    async def _schedule_round_preparation(
        self,
        round_id: int,
        registration_start: datetime
    ) -> None:
        """Schedules a one-time job to prepare challenge context data."""
        if self.scheduler:
            job_id = f"prepare_round_{round_id}"
            await self.scheduler.schedule_challenge_preparation(
                job_id=job_id,
                round_id=round_id,
                run_at=registration_start
            )
            logger.info(f"Scheduled preparation job '{job_id}' for round {round_id} at {registration_start}")
        else:
            logger.warning(f"Scheduler not available, cannot schedule preparation for round {round_id}")

    async def prepare_round_context_data(self, round_id: int) -> None:
        """
        Prepares context data for a challenge round.
        This is called by the scheduler at registration_start.
        """
        try:
            round_obj = await self.round_repository.get_by_id(round_id)
            if not round_obj:
                raise ValueError(f"Round {round_id} not found")
            
            if not round_obj.definition_id:
                raise ValueError(f"Round {round_id} has no definition_id")

            definition = await self.definition_repository.get_by_id(round_obj.definition_id)
            if not definition:
                raise ValueError(f"Definition {round_obj.definition_id} not found")

            # Get required series
            required_series_ids = await self.definition_repository.get_current_series_ids(definition.id)

            await self._prepare_context_data(
                round_id=round_id,
                domains=definition.domains or [],
                subdomains=definition.subdomains or [],
                categories=definition.categories or [],
                subcategories=definition.subcategories or [],
                frequency=isodate.duration_isoformat(definition.frequency),
                required_series_ids=required_series_ids,
                n_time_series=definition.n_time_series,
                context_length=definition.context_length,
                before_time=round_obj.start_time
            )

        except Exception as e:
            logger.error(f"Error preparing context data for round {round_id}: {e}")
            await self.round_repository.update_status(round_id, "cancelled")

    async def _prepare_context_data(
        self,
        round_id: int,
        domains: List[str],
        subdomains: List[str],
        categories: List[str],
        subcategories: List[str],
        frequency: str,
        required_series_ids: List[int],
        n_time_series: int,
        context_length: int,
        before_time: datetime
    ) -> None:
        """
        Selects time series and copies their context data to the round.
        """
        try:
            selected_series_ids = list(required_series_ids)
            
            # Add random series if needed
            remaining_needed = n_time_series - len(selected_series_ids)
            
            if remaining_needed > 0:
                available_series_ids = await self.time_series_repository.filter_time_series_with_recent_data(
                    domains=domains,
                    subdomains=subdomains,
                    categories=categories,
                    subcategories=subcategories,
                    frequency=frequency,
                    only_with_recent_data=True
                )
                
                available_series_ids = [
                    sid for sid in available_series_ids 
                    if sid not in selected_series_ids
                ]
                
                if len(available_series_ids) < remaining_needed:
                    logger.warning(f"Not enough series available. Needed: {remaining_needed}, Available: {len(available_series_ids)}")
                    remaining_needed = len(available_series_ids)
                
                if available_series_ids:
                    additional_series = random.sample(available_series_ids, remaining_needed)
                    logger.info(f"Randomly selected {len(additional_series)} additional series")
                    selected_series_ids.extend(additional_series)
            
            logger.info(f"Total selected series for round {round_id}: {len(selected_series_ids)}")
            
            if selected_series_ids:
                series_mapping = {}
                pseudo_entries = []
                
                for series_id in selected_series_ids:
                    ts_metadata = await self.time_series_repository.get_time_series_by_id(series_id)
                    if ts_metadata:
                        series_name = ts_metadata.unique_id or ts_metadata.name or f"series_{series_id}"
                        
                        # Required series keep their name, random series get hashed names
                        if series_id in required_series_ids:
                            challenge_series_name = series_name
                        else:
                            digest = hashlib.sha1(f"{round_id}:{series_id}".encode("utf-8")).hexdigest()[:12]
                            challenge_series_name = f"series_{digest}"
                        
                        series_mapping[series_id] = series_name
                        pseudo_entries.append({
                            "round_id": round_id,
                            "series_id": series_id,
                            "challenge_series_name": challenge_series_name,
                        })
                    else:
                        logger.warning(f"Time series {series_id} not found")
                
                resolution = self._frequency_to_resolution(frequency)
                logger.info(f"Copying {context_length} context points for {len(series_mapping)} series (resolution: {resolution})")
                
                copy_result = await self.time_series_repository.copy_bulk_to_challenge_by_resolution(
                    series_mapping=series_mapping,
                    round_id=round_id,
                    n=context_length,
                    resolution=resolution,
                    before_time=before_time
                )
                
                total_copied = sum(copy_result.values())
                logger.info(f"Copied {total_copied} total context points to round {round_id}")

                # Calculate statistics
                for entry in pseudo_entries:
                    series_id = entry["series_id"]
                    stats = await self.time_series_repository.calculate_context_data_stats(
                        round_id=round_id,
                        series_id=series_id
                    )
                    if stats:
                        entry["min_ts"] = stats["min_ts"]
                        entry["max_ts"] = stats["max_ts"]
                        entry["value_avg"] = stats["value_avg"]
                        entry["value_std"] = stats["value_std"]
                    else:
                        entry["min_ts"] = None
                        entry["max_ts"] = None
                        entry["value_avg"] = None
                        entry["value_std"] = None

                if pseudo_entries:
                    await self.round_repository.upsert_series_pseudo(pseudo_entries)
                
                await self.db_session.commit()
            else:
                logger.warning(f"No time series selected for round {round_id}")
                
        except Exception as e:
            logger.error(f"Error preparing context data: {e}")
            await self.db_session.rollback()
            raise

    # ==========================================================
    # Query operations
    # ==========================================================

    async def get_context_data_bulk(self, round_id: int) -> List[ChallengeContextData]:
        """Returns all stored context data points for a round."""
        raw = await self.round_repository.get_context_data_bulk(round_id)
        return [
            ChallengeContextData(
                challenge_series_name=series_name,
                frequency=series_data.get("frequency"),
                data=[ContextDataPoint(**point) for point in series_data["data"]]
            )
            for series_name, series_data in raw.items()
        ]

    async def get_round_data(self, round_id: int) -> ChallengeRoundData:
        """Returns complete round data (Context, Actuals, Forecasts)."""
        # Ensure round exists
        round_obj = await self.round_repository.get_by_id(round_id)
        if not round_obj:
            raise ValueError(f"Round {round_id} not found")
            
        raw_data = await self.round_repository.get_round_complete_data(round_id)
        return ChallengeRoundData(**raw_data)

    async def get_round(self, round_id: int) -> Optional[ChallengeRoundResponse]:
        """Get a single challenge round by ID with definition info."""
        # Use the view for extra definition info
        rounds = await self.round_repository.list_rounds(statuses=None)
        for r in rounds:
            if r.id == round_id:
                return ChallengeRoundResponse(
                    id=r.id,
                    name=r.name,
                    description=r.description,
                    context_length=r.context_length,
                    horizon=r.horizon,
                    frequency=r.frequency,
                    registration_start=r.registration_start,
                    registration_end=r.registration_end,
                    start_time=r.start_time,
                    end_time=r.end_time,
                    status=r.status if r.status == "cancelled" else r.computed_status,
                    definition_id=r.definition_id,
                    definition_name=r.definition_name,
                    definition_domains=r.definition_domains,
                    definition_subdomains=r.definition_subdomains,
                    definition_categories=r.definition_categories,
                    definition_subcategories=r.definition_subcategories,
                    created_at=r.created_at,
                )
        return None

    async def list_rounds(
        self,
        statuses: Optional[List[str]] = None,
        definition_id: Optional[int] = None
    ) -> List[ChallengeRoundResponse]:
        """Lists challenge rounds with definition info."""
        rounds = await self.round_repository.list_rounds(
            statuses=statuses,
            definition_id=definition_id
        )
        return [
            ChallengeRoundResponse(
                id=r.id,
                name=r.name,
                description=r.description,
                context_length=r.context_length,
                horizon=r.horizon,
                frequency=r.frequency,
                registration_start=r.registration_start,
                registration_end=r.registration_end,
                start_time=r.start_time,
                end_time=r.end_time,
                status=r.status if r.status == "cancelled" else r.computed_status,
                definition_id=r.definition_id,
                definition_name=r.definition_name,
                definition_domains=r.definition_domains,
                definition_subdomains=r.definition_subdomains,
                definition_categories=r.definition_categories,
                definition_subcategories=r.definition_subcategories,
                created_at=r.created_at,
            )
            for r in rounds
        ]

    async def generate_naive_forecast_template(
        self,
        round_id: int
    ) -> Dict[str, Any]:
        """
        Generates a naive forecast template for a round.
        
        Uses persistence (last known value) as the prediction method.
        Returns a structure matching ForecastUploadRequest for direct upload.
        """
        # Get round details
        round_obj = await self.round_repository.get_by_id(round_id)
        if not round_obj:
            raise ValueError(f"Round {round_id} not found")
        
        # Get context data
        context_data = await self.get_context_data_bulk(round_id)
        if not context_data:
            raise ValueError(f"No context data available for round {round_id}")
        
        # Calculate forecast timestamps based on horizon and frequency
        # Forecast starts at registration_end (= start_time of actual forecast period)
        forecast_start = round_obj.start_time
        forecast_end = round_obj.end_time
        frequency = round_obj.frequency
        
        if not frequency:
            raise ValueError(f"Round {round_id} has no frequency defined")
        
        # Generate timestamps
        forecast_timestamps = []
        current_ts = forecast_start
        while current_ts < forecast_end:
            forecast_timestamps.append(current_ts)
            current_ts += frequency
        
        # Build naive forecast for each series
        forecasts_list = []
        for series_data in context_data:
            # Get last known value (naive persistence)
            if not series_data.data:
                continue
            
            last_value = series_data.data[-1].value
            
            forecasts = [
                {"ts": ts, "value": last_value}
                for ts in forecast_timestamps
            ]
            
            forecasts_list.append({
                "challenge_series_name": series_data.challenge_series_name,
                "forecasts": forecasts
            })
        
        # Return structure matching ForecastUploadRequest
        return {
            "round_id": round_id,
            "model_name": "Naive",
            "forecasts": forecasts_list
        }

