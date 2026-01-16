from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any, Dict
import uuid
import random
import logging
from app.schemas.challenge import (
    ChallengeCreate, Challenge, ChallengeStatus, ChallengeContextData, ContextDataPoint
)
from app.database.challenges.challenge_repository import ChallengeRepository
from app.database.data_portal.time_series_repository import TimeSeriesRepository
from app.database.forecasts.repository import ForecastRepository
import hashlib
import numpy as np
from sklearn.metrics import mean_squared_error

logger = logging.getLogger(__name__)

class ChallengeService:
    
    # Mapping from challenge frequency to resolution view
    FREQUENCY_TO_RESOLUTION = {
        "15 minutes": "15min",
        "1 hour": "1h",
        "1 day": "1d",
    }
    
    def __init__(self, db_session, scheduler=None):
        self.repository: ChallengeRepository = ChallengeRepository(db_session)
        self.time_series_repository: TimeSeriesRepository = TimeSeriesRepository(db_session)
        self.forecast_repository: ForecastRepository = ForecastRepository(db_session)
        self.db_session = db_session
        self.scheduler = scheduler
    
    def _frequency_to_resolution(self, frequency: str) -> str:
        """Maps challenge frequency to view resolution."""
        resolution = self.FREQUENCY_TO_RESOLUTION.get(frequency)
        if not resolution:
            logger.warning(f"Unknown frequency '{frequency}', defaulting to '1h'")
            return "1h"
        return resolution

    async def create_challenge(
        self,
        challenge_data: ChallengeCreate,
    ) -> Challenge:
        """
        Creates an initial challenge entry in the database
        """
        challenge_create_dict = challenge_data.model_dump()
        # Enum to string for the DB
        created = await self.repository.create_challenge(**challenge_create_dict)
        return Challenge.model_validate(created, from_attributes=True)

    async def create_challenge_from_schedule(
        self, schedule_params: Dict[str, Any]
    ) -> Challenge:
        """
        Creates a new challenge based on a schedule's parameters.
        Implements upsert logic: if a challenge with the same name exists, it returns the existing one.
        """
        now = datetime.now(timezone.utc)
        # Use seconds for better uniqueness (was only minutes before)
        name = f"{schedule_params['description']} - {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"

        # Add logging to inspect the incoming schedule_params
        logger.info(f"Creating challenge '{name}'")
        logger.info(f"schedule_params received: {schedule_params}")
        logger.info(f"schedule_params keys: {list(schedule_params.keys())}")

        # Calculate timings from string durations like "7 days" or "55 minutes"
        announce_lead_str = schedule_params.get("announce_lead", "1 minuteute")
        registration_duration_str = schedule_params.get("registration_duration", "1 hour")
        forecast_horizon_str = schedule_params["forecast_horizon"]
        context_length = schedule_params["context_length"]

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

        announce_lead = parse_duration(announce_lead_str)
        registration_duration = parse_duration(registration_duration_str)
        horizon_delta = parse_duration(forecast_horizon_str)
        
        # Parse challenge frequency for the new dedicated column
        frequency_str = schedule_params["frequency"]
        frequency_delta = parse_duration(frequency_str)
        
        # context_length is the number of data points (integer)
        # No conversion needed - store directly as integer

        registration_start = now + announce_lead
        registration_end = registration_start + registration_duration
        start_time = registration_end
        end_time = start_time + horizon_delta

        challenge_data = ChallengeCreate(
            name=name,
            description=schedule_params["description"],
            context_length=context_length,
            horizon=horizon_delta,
            frequency=frequency_delta,
            registration_start=registration_start,
            registration_end=registration_end,
            start_time=start_time,
            end_time=end_time,
        )

        # Use upsert to handle idempotency at repository level
        challenge_dict = challenge_data.model_dump()
        challenge_db = await self.repository.upsert_challenge(**challenge_dict)
        challenge = Challenge.model_validate(challenge_db, from_attributes=True)
        
        # Store preparation parameters in challenge metadata for later use
        # This will be used by the preparation job at registration_start
        preparation_params = {
            "domain": schedule_params.get("domain"),
            "subdomain": schedule_params.get("subdomain"),
            "frequency": schedule_params["frequency"],
            "required_time_series": schedule_params.get("required_time_series", []),
            "n_time_series": schedule_params["n_time_series"],
            "context_length": context_length,
            "before_time": start_time.isoformat(),
        }
        
        # Save preparation params to database
        await self.repository.update_preparation_params(
            challenge_id=challenge.id,
            preparation_params=preparation_params
        )
        
        # Schedule data preparation job for registration_start
        await self._schedule_challenge_preparation(
            challenge_id=challenge.id,
            registration_start=registration_start,
            preparation_params=preparation_params
        )
        
        # Note: Score calculation is now handled by periodic_challenge_scores_evaluation_job
        # which runs every 10 minutes and evaluates all active/completed challenges
        
        return challenge

    async def _schedule_challenge_preparation(
        self,
        challenge_id: int,
        registration_start: datetime,
        preparation_params: Dict[str, Any]
    ) -> None:
        """
        Schedules a one-time job to prepare challenge context data.
        The job will execute at registration_start.
        """
        if self.scheduler:
            job_id = f"prepare_challenge_{challenge_id}"
            await self.scheduler.schedule_challenge_preparation(
                job_id=job_id,
                challenge_id=challenge_id,
                run_at=registration_start,
                preparation_params=preparation_params
            )
            logger.info(f"Scheduled preparation job '{job_id}' for challenge {challenge_id} at {registration_start}")
        else:
            logger.warning(f"Scheduler not available, cannot schedule preparation for challenge {challenge_id}")

    async def prepare_challenge_context_data(self, challenge_id: int) -> None:
        """
        Prepares context data for a challenge by selecting and copying time series data.
        This should be called at registration_start to ensure fresh data.
        
        Status transitions:
        - ANNOUNCED → PREPARING (at start of method)
        - PREPARING → REGISTRATION (on success)
        - PREPARING → ERROR (on failure)
        """
        try:
            # Fetch challenge to get preparation params
            challenge = await self.repository.get_challenge_by_id(challenge_id)
            if not challenge:
                raise ValueError(f"Challenge {challenge_id} not found")
            
            if not challenge.preparation_params:
                raise ValueError(f"Challenge {challenge_id} has no preparation_params")
                        
            # Execute preparation
            await self._execute_context_data_preparation(
                challenge_id=challenge_id,
                preparation_params=challenge.preparation_params
            )
            
            logger.info(f"Successfully prepared context data for challenge {challenge_id}")
            
        except Exception as e:
            logger.error(f"Error preparing challenge context data for challenge {challenge_id}: {e}")

    async def _execute_context_data_preparation(
        self,
        challenge_id: int,
        preparation_params: Dict[str, Any]
    ) -> None:
        """
        Internal method that executes the actual context data preparation.
        This is called by prepare_challenge_context_data_job.
        
        Args:
            challenge_id: ID of the challenge
            preparation_params: Dict containing domain, subdomain, frequency, 
                              required_time_series, n_time_series, context_length, before_time
        """
        # Extract parameters
        domain = preparation_params.get("domain")
        subdomain = preparation_params.get("subdomain")
        frequency = preparation_params["frequency"]
        required_time_series = preparation_params.get("required_time_series", [])
        n_time_series = preparation_params["n_time_series"]
        context_length = preparation_params["context_length"]
        before_time_str = preparation_params["before_time"]
        before_time = datetime.fromisoformat(before_time_str)
        
        # Call the existing preparation logic
        await self._prepare_challenge_context_data(
            challenge_id=challenge_id,
            domain=domain,
            subdomain=subdomain,
            frequency=frequency,
            required_time_series=required_time_series,
            n_time_series=n_time_series,
            context_length=context_length,
            before_time=before_time
        )

    async def _prepare_challenge_context_data(
        self,
        challenge_id: int,
        domain: Optional[str],
        subdomain: Optional[str],
        frequency: str,
        required_time_series: List[int],
        n_time_series: int,
        context_length: int,
        before_time: datetime
    ) -> None:
        """
        Selects time series for a challenge and copies their context data.
        
        Business Logic:
        1. Start with required_time_series if specified
        2. If more series needed, randomly select additional ones based on:
           - domain: if "mixed", select from all; otherwise filter by domain
           - subdomain: filter by subdomain if specified
           - frequency: must match the specified frequency
           - Only select series with recent data (via v_data_availability view)
        3. Copy last N points (context_length) for each selected series
        
        Args:
            challenge_id: ID of the challenge
            domain: Domain filter ("mixed" = no filter, specific domain = filter by it)
            subdomain: Subdomain filter (optional, "mixed" = no filter)
            frequency: Required frequency (e.g., "1 hour")
            required_time_series: List of series_ids that must be included
            n_time_series: Total number of time series to include
            context_length: Number of historical data points to copy
            before_time: Cutoff time for context data (exclusive)
        """
        try:
            selected_series_ids = []
            
            # Step 1: Add required time series
            if required_time_series:
                logger.info(f"Adding {len(required_time_series)} required time series: {required_time_series}")
                selected_series_ids.extend(required_time_series)
            
            # Step 2: Check if we need more time series
            remaining_needed = n_time_series - len(selected_series_ids)
            
            if remaining_needed > 0:
                # Build filter criteria for random selection using v_data_availability view
                filter_domain = domain  # "mixed" will be handled by repository
                filter_category = subdomain 
                filter_subcategory = None   # Could be extended if needed
                
                logger.info(f"Filtering time series with recent data: domain={filter_domain}, "
                           f"subdomain={filter_category}, frequency={frequency}")
                
                # Get filtered time series IDs with recent data availability check
                available_series_ids = await self.time_series_repository.filter_time_series_with_recent_data(
                    domain=filter_domain,
                    category=filter_category,
                    subcategory=filter_subcategory,
                    frequency=frequency,
                    only_with_recent_data=True  # Only select series with recent data
                )
                
                # Remove already selected series
                available_series_ids = [
                    sid for sid in available_series_ids 
                    if sid not in selected_series_ids
                ]
                
                if len(available_series_ids) < remaining_needed:
                    logger.warning(
                        f"Not enough time series with recent data available. "
                        f"Needed: {remaining_needed}, Available: {len(available_series_ids)}"
                    )
                    remaining_needed = len(available_series_ids)
                
                # Randomly select additional series
                if available_series_ids:
                    additional_series = random.sample(available_series_ids, remaining_needed)
                    logger.info(f"Randomly selected {len(additional_series)} additional time series with recent data: {additional_series}")
                    selected_series_ids.extend(additional_series)
            
            logger.info(f"Total selected time series for challenge {challenge_id}: {selected_series_ids}")
            
            # Step 3: Copy context data for all selected series
            if selected_series_ids:
                # Build series mapping: series_id -> series_name for challenge
                series_mapping = {}
                # Prepare entries for challenge_series_pseudo
                pseudo_entries = []
                for series_id in selected_series_ids:
                    ts_metadata = await self.time_series_repository.get_time_series_by_id(series_id)
                    if ts_metadata:
                        # Use endpoint_prefix or name as series identifier
                        series_name = ts_metadata.endpoint_prefix or ts_metadata.name or f"series_{series_id}"
                        # Determine challenge_series_name: klarname for required, hash for random
                        if required_time_series and series_id in required_time_series:
                            challenge_series_name = series_name
                        else:
                            # Deterministic short hash from challenge_id + series_id
                            digest = hashlib.sha1(f"{challenge_id}:{series_id}".encode("utf-8")).hexdigest()[:12]
                            challenge_series_name = f"series_{digest}"
                        series_mapping[series_id] = series_name
                        pseudo_entries.append({
                            "challenge_id": challenge_id,
                            "series_id": series_id,
                            "challenge_series_name": challenge_series_name,
                        })
                    else:
                        logger.warning(f"Time series {series_id} not found in metadata")
                
                # Copy last N points for all selected series FROM THE APPROPRIATE RESOLUTION VIEW
                resolution = self._frequency_to_resolution(frequency)
                logger.info(f"Copying {context_length} context points for {len(series_mapping)} series (resolution: {resolution})")
                
                # Validate all series for this resolution and log warnings
                for series_id in selected_series_ids:
                    is_valid = await self.time_series_repository.validate_series_for_resolution(
                        series_id, resolution
                    )
                    if not is_valid:
                        logger.warning(f"Series {series_id} may not be available for resolution {resolution} (native frequency > target)")
                
                copy_result = await self.time_series_repository.copy_bulk_to_challenge_by_resolution(
                    series_mapping=series_mapping,
                    challenge_id=challenge_id,
                    n=context_length,
                    resolution=resolution,
                    before_time=before_time
                )
                
                total_copied = sum(copy_result.values())
                logger.info(f"Successfully copied {total_copied} total context points to challenge {challenge_id} (resolution: {resolution})")

                # Calculate statistics for each series and add to pseudo_entries
                logger.info(f"Calculating statistics for {len(pseudo_entries)} series")
                for entry in pseudo_entries:
                    series_id = entry["series_id"]
                    stats = await self.time_series_repository.calculate_context_data_stats(
                        challenge_id=challenge_id,
                        series_id=series_id
                    )
                    if stats:
                        entry["min_ts"] = stats["min_ts"]
                        entry["max_ts"] = stats["max_ts"]
                        entry["value_avg"] = stats["value_avg"]
                        entry["value_std"] = stats["value_std"]
                        
                        # Format statistics for logging
                        avg_str = f"{stats['value_avg']:.2f}" if stats['value_avg'] is not None else "N/A"
                        std_str = f"{stats['value_std']:.2f}" if stats['value_std'] is not None else "N/A"
                        logger.debug(
                            f"Series {series_id}: range=[{stats['min_ts']} - {stats['max_ts']}], "
                            f"avg={avg_str}, std={std_str}"
                        )
                    else:
                        logger.warning(f"Could not calculate stats for series {series_id}")
                        # Set None values explicitly
                        entry["min_ts"] = None
                        entry["max_ts"] = None
                        entry["value_avg"] = None
                        entry["value_std"] = None

                # Persist mapping between challenge and selected series names
                if pseudo_entries:
                    await self.repository.upsert_challenge_series_pseudo(pseudo_entries)
                
                # Commit the transaction
                await self.db_session.commit()
            else:
                logger.warning(f"No time series selected for challenge {challenge_id}")
                
        except Exception as e:
            logger.error(f"Error preparing challenge context data: {e}")
            await self.db_session.rollback()
            raise

    # ==========================================================================
    # Read-Only Operations
    # ==========================================================================



    async def get_context_data_bulk(
        self, challenge_id: int
    ) -> List[ChallengeContextData]:
        """
        Returns all stored context data points for a challenge
        as Pydantic models, grouped by challenge_series_name.
        """
        raw = await self.repository.get_context_data_bulk(challenge_id)
        # raw: Dict[str, Dict[str, Any]] where each value has "frequency" and "data"
        return [
            ChallengeContextData(
                challenge_series_name=series_name,
                frequency=series_data.get("frequency"),
                data=[ContextDataPoint(**point) for point in series_data["data"]]
            )
            for series_name, series_data in raw.items()
        ]

    async def get_challenge(self, challenge_id: int) -> Optional[Challenge]:
        """
        Get a single challenge by its ID.
        """
        challenge = await self.repository.get_challenge_by_id(challenge_id)
        if challenge:
            return Challenge.model_validate(challenge, from_attributes=True)
        return None

    async def get_open_challenges_sorted(self) -> List[Challenge]:
        """
        Get all open/announced challenges sorted by registration_start ascending.
        """
        challenges = await self.repository.list_challenges(
            statuses=["announced", "registration", "active"]
        )
        # Sort by registration_start
        sorted_challenges = sorted(
            challenges, 
            key=lambda c: c.registration_start if c.registration_start else datetime.max.replace(tzinfo=timezone.utc)
        )
        return [
            Challenge.model_validate(c, from_attributes=True) for c in sorted_challenges
        ]

    async def list_challenges(
        self,
        statuses: Optional[List[str]] = None,
    ) -> List[Challenge]:
        """
        Lists all challenges, optionally filtered by a list of statuses.
        """
        challenges = await self.repository.list_challenges(statuses=statuses)
        return [
            Challenge.model_validate(c, from_attributes=True) for c in challenges
        ]