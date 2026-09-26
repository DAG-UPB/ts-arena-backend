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
from app.scheduler.schedule_validation import parse_duration
from app.services.series_scale_service import SeriesScaleService

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

        # Parse all duration fields
        # (shared with the registration-window validator so the two cannot drift)
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
        yaml_series_ids = set()
        
        if required_unique_ids:
            for unique_id in required_unique_ids:
                # Look up series_id by unique_id
                series = await self.time_series_repository.get_time_series_by_unique_id(unique_id)
                if series:
                    yaml_series_ids.add(series.series_id)
                    await self.definition_repository.upsert_series_assignment(
                        definition_id=definition.id,
                        series_id=series.series_id,
                        is_required=True
                    )
                else:
                    logger.warning(f"Time series with unique_id '{unique_id}' not found for definition '{schedule_id}'")
        
        # Close out series that are no longer in the YAML
        closed_count = await self.definition_repository.close_out_removed_series(
            definition_id=definition.id,
            active_series_ids=list(yaml_series_ids)
        )
        if closed_count > 0:
            logger.info(f"Closed {closed_count} series assignments no longer in YAML for definition '{schedule_id}'")
        
        logger.info(f"Synced definition '{schedule_id}' (ID: {definition.id}) with {len(yaml_series_ids)} required series")
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
                frequency_timedelta=definition.frequency,
                horizon=definition.horizon,
                required_series_ids=required_series_ids,
                n_time_series=definition.n_time_series,
                context_length=definition.context_length,
            )

        except Exception as e:
            logger.error(f"Error preparing context data for round {round_id}: {e}")
            await self.round_repository.cancel_round(round_id)

    async def _prepare_context_data(
        self,
        round_id: int,
        domains: List[str],
        subdomains: List[str],
        categories: List[str],
        subcategories: List[str],
        frequency: str,
        frequency_timedelta: timedelta,
        horizon: timedelta,
        required_series_ids: List[int],
        n_time_series: int,
        context_length: int,
    ) -> None:
        """
        Selects time series and copies their context data to the round.
        
        Logic:
        - If required_series_ids is provided and non-empty: use ONLY those series
        - If required_series_ids is empty: select n_time_series random series
        - Context data is copied up to the maximum available timestamp (no cutoff)
        - Round's start_time and end_time are updated based on: max_context_ts + frequency,
          where max_context_ts is the GLOBAL max across the round's series. Both fields are
          informative only — they do not define where any series' forecast starts. See
          wiki/30_Notes/round-time-fields-and-forecast-anchoring.md.
        """
        try:
            # Simple logic: use required series OR random series, never mix
            if required_series_ids:
                selected_series_ids = list(required_series_ids)
                use_hashed_names = False  # Required series keep their real names
                logger.info(f"Using {len(selected_series_ids)} required series for round {round_id}")
            else:
                # No required series defined - select random series
                available_series_ids = await self.time_series_repository.filter_time_series_with_recent_data(
                    domains=domains,
                    subdomains=subdomains,
                    categories=categories,
                    subcategories=subcategories,
                    frequency=frequency,
                    only_with_recent_data=True
                )
                
                if len(available_series_ids) < n_time_series:
                    logger.warning(f"Not enough series available. Needed: {n_time_series}, Available: {len(available_series_ids)}")
                    selected_series_ids = available_series_ids
                else:
                    selected_series_ids = random.sample(available_series_ids, n_time_series)
                
                use_hashed_names = True  # Random series get hashed names
                logger.info(f"Randomly selected {len(selected_series_ids)} series for round {round_id}")
            
            if not selected_series_ids:
                logger.warning(f"No time series selected for round {round_id}")
                return
            
            # Build series mapping and pseudo entries
            series_mapping = {}
            pseudo_entries = []
            
            for series_id in selected_series_ids:
                ts_metadata = await self.time_series_repository.get_time_series_by_id(series_id)
                if not ts_metadata:
                    logger.warning(f"Time series {series_id} not found")
                    continue
                    
                series_name = ts_metadata.unique_id or ts_metadata.name or f"series_{series_id}"
                
                if use_hashed_names:
                    digest = hashlib.sha1(f"{round_id}:{series_id}".encode("utf-8")).hexdigest()[:12]
                    challenge_series_name = f"series_{digest}"
                else:
                    challenge_series_name = series_name
                
                series_mapping[series_id] = series_name
                pseudo_entries.append({
                    "round_id": round_id,
                    "series_id": series_id,
                    "challenge_series_name": challenge_series_name,
                })
            
            if not series_mapping:
                logger.warning(f"No valid time series found for round {round_id}")
                return
            
            resolution = self._frequency_to_resolution(frequency)
            logger.info(f"Copying {context_length} context points for {len(series_mapping)} series (resolution: {resolution})")
            
            # Copy context data WITHOUT before_time cutoff - gets all available data up to max
            # timestamp.
            #
            # "All available data" means as far as the publisher has released, which for a
            # publish-ahead source is in the future (backend-87). That is only true because the
            # read unions the aggregate with a live tail over raw
            # (`_read_aggregate_with_live_tail`); the aggregate alone stops at its watermark,
            # which `end_offset` holds at ~now. Before that union existed this call silently
            # meant "everything up to now", which for SMARD day-ahead prices put the whole
            # forecast window inside already-published data.
            copy_result = await self.time_series_repository.copy_bulk_to_challenge_by_resolution(
                series_mapping=series_mapping,
                round_id=round_id,
                n=context_length,
                resolution=resolution,
                before_time=None  # No cutoff - include all available data
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
                await self._store_served_mase_scales(round_id, resolution)

            # Determine the global max timestamp across all series in context
            # This becomes the basis for forecast_start = max_ts + 1 frequency step
            all_max_ts = [
                entry["max_ts"] for entry in pseudo_entries 
                if entry.get("max_ts") is not None
            ]
            
            if all_max_ts:
                # The publication edge, not "now": for a source that publishes ahead of
                # delivery this is genuinely in the future, which is what puts the forecast
                # window after the data that already exists (backend-87). The read side has
                # already dropped the still-filling bucket, so this cannot be a partial one.
                global_max_ts = max(all_max_ts)
                new_start_time = global_max_ts + frequency_timedelta
                new_end_time = new_start_time + horizon

                # NOTE: `start_time` / `end_time` are INFORMATIVE ONLY. This is the one
                # place they are written, and writing them from the GLOBAL max is the
                # origin of a recurring misreading — that they mark where a forecast
                # begins. They do not. The first forecast timestamp is per series
                # (`series_pseudo.max_ts + frequency`) and differs per series, because the
                # providers publish with a small lag that varies slightly between them. On
                # a round where nothing lags these values coincide, which is what keeps the
                # misreading alive: it is correct on most rounds and on most series.
                # Anchor on `get_series_context_edges`, never on this.
                # See wiki/30_Notes/round-time-fields-and-forecast-anchoring.md.
                # Update round's start_time and end_time
                await self.round_repository.update_round_times(
                    round_id=round_id,
                    start_time=new_start_time,
                    end_time=new_end_time
                )
                logger.info(
                    f"Updated round {round_id} forecast window: "
                    f"max_context_ts={global_max_ts}, "
                    f"start_time={new_start_time}, end_time={new_end_time}"
                )
            else:
                logger.warning(f"No max_ts found for round {round_id}, cannot update forecast times")
            
            await self.db_session.commit()
                
        except Exception as e:
            logger.error(f"Error preparing context data: {e}")
            await self.db_session.rollback()
            raise

    # ==========================================================
    # Query operations
    # ==========================================================

    async def _store_served_mase_scales(self, round_id: int, resolution: str) -> None:
        """Record each series' MASE scale from the context exactly as served.

        This is the only point at which the served context is guaranteed to be stored
        (`context_data` is not kept). Best-effort: it runs in a savepoint, so a failure here
        never costs the round its context. A missing scale is rebuilt at scoring time.
        """
        scale_service = SeriesScaleService(self.db_session)
        try:
            if not await scale_service.repo.tables_exist():
                return
            async with self.db_session.begin_nested():
                stored = await scale_service.store_served_scales(round_id, resolution)
            logger.info(f"Stored MASE scales for {stored} series of round {round_id}")
        except Exception as e:
            logger.warning(f"Could not store MASE scales for round {round_id}: {e}")

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
                    status=r.computed_status,
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
                status=r.computed_status,
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

    # Deciles the platform scores on (backend-13/backend-64). Kept here rather than imported
    # so the template cannot silently fall out of step with `canonical_quantile_key`.
    NAIVE_QUANTILE_LEVELS = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)

    @staticmethod
    def _naive_quantile_offsets(values: List[float]) -> Dict[str, float]:
        """Offsets from the persistence point forecast, one per canonical decile.

        Derived from the empirical quantiles of the context's first differences: if the
        series has moved by `d` between steps historically, a one-step-ahead persistence
        forecast is wrong by about `d`. Deterministic — no sampling — so the same round
        always yields the same template, which matters for a payload participants diff
        against their own output.

        Degenerate input (fewer than two points, or a flat series) yields a zero offset at
        every level. That is a valid, monotone, non-crossing set of quantiles; it simply
        expresses no uncertainty.
        """
        levels = ChallengeService.NAIVE_QUANTILE_LEVELS
        if len(values) < 2:
            return {f"q_{level}": 0.0 for level in levels}

        diffs = sorted(values[i + 1] - values[i] for i in range(len(values) - 1))

        offsets: Dict[str, float] = {}
        for level in levels:
            # Linear interpolation between order statistics (numpy's default method),
            # written out to keep this dependency-free.
            position = level * (len(diffs) - 1)
            lower_index = int(position)
            upper_index = min(lower_index + 1, len(diffs) - 1)
            weight = position - lower_index
            offsets[f"q_{level}"] = (
                diffs[lower_index] * (1 - weight) + diffs[upper_index] * weight
            )

        # Empirical quantiles of a sorted sample are already non-decreasing; assert the
        # invariant the upload path would otherwise silently repair.
        ordered = [offsets[f"q_{level}"] for level in levels]
        assert ordered == sorted(ordered), "naive quantile offsets must not cross"
        return offsets

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
        
        frequency = round_obj.frequency
        if not frequency:
            raise ValueError(f"Round {round_id} has no frequency defined")

        # How many points a forecast must carry. Same derivation the upload path uses to
        # validate the count, so the template cannot produce a payload of the wrong length.
        if not round_obj.horizon:
            raise ValueError(f"Round {round_id} has no horizon defined")
        step_count = int(round_obj.horizon.total_seconds() / frequency.total_seconds())

        # Build naive forecast for each series.
        #
        # Anchoring is PER SERIES (backend-95). The template used to walk a single global
        # window, `rounds.start_time` -> `end_time`, for every series at once.
        #
        # `rounds.start_time` anchors nothing. It is an informative field; the first
        # forecast timestamp is `that series' own last context ts + frequency`, and it
        # differs per series because the providers publish with a small lag that varies
        # slightly between them. The lag is usually harmless — median zero on most
        # definitions — but one step off invalidates every timestamp in the submission.
        # Measured on prod over 2026-09-08..09-22, 65 % of definition 2's series and 42 %
        # of definition 3's sat more than one step behind the round-wide value. For every
        # one of those the template emitted timestamps that do not exist for that series —
        # which `ForecastService._expected_forecast_timestamps` rejects, since it validates
        # `series_pseudo.max_ts + k * frequency` per series.
        # See wiki/30_Notes/round-time-fields-and-forecast-anchoring.md.
        #
        # `max(...)` over the points rather than `data[-1]` so this does not depend on the
        # repository's ORDER BY (cf. ts-arena #20).
        forecasts_list = []
        for series_data in context_data:
            # Get last known value (naive persistence)
            if not series_data.data:
                continue

            last_point = max(series_data.data, key=lambda point: point.ts)
            last_value = last_point.value
            forecast_timestamps = [
                last_point.ts + k * frequency for k in range(1, step_count + 1)
            ]

            # Persistence has no spread of its own, so the band comes from the dispersion of
            # the context's own step-to-step changes. It is a weak forecast on purpose — the
            # point is that the template demonstrates the *shape* the platform expects,
            # including the nine canonical quantile keys the SQL leaderboard requires
            # (backend-64). A point-only template could never reach that board.
            quantile_offsets = self._naive_quantile_offsets(
                [point.value for point in series_data.data]
            )

            forecasts = [
                {
                    "ts": ts,
                    "value": last_value,
                    "probabilistic_values": {
                        key: last_value + offset for key, offset in quantile_offsets.items()
                    },
                }
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

