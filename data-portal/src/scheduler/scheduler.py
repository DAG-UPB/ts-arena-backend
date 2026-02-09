"""Data Portal Scheduler - Main scheduler for automated data collection"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.triggers.interval import IntervalTrigger
from src.config import Config
from src.scheduler.plugin_loader import PluginLoader
from src.scheduler.frequency_parser import parse_frequency, get_interval_seconds
from src.database import SessionLocal
from src.repositories.time_series_repository import TimeSeriesDataRepository
from src.plugins.base_plugin import BasePlugin, MultiSeriesPlugin
from src.services.imputation_service import ImputationService, parse_frequency_to_timedelta

logger = logging.getLogger(__name__)


class DataPortalScheduler:
    """
    Main scheduler for data portal service.
    Manages scheduled jobs for fetching data from plugins and writing to database.
    """
    
    def __init__(self):
        self.plugin_loader = PluginLoader(Config.PLUGIN_CONFIG_PATH)
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.plugins: Dict[str, BasePlugin] = {}
        self.multi_series_plugins: Dict[str, MultiSeriesPlugin] = {}
        self.max_concurrent_jobs = 10  # Global limit for parallel jobs
        self.job_semaphore = asyncio.Semaphore(self.max_concurrent_jobs)
        
    async def initialize(self):
        """Initialize scheduler and load plugins"""
        logger.info("Initializing Data Portal Scheduler...")
        
        # Load single-series plugins
        self.plugins = self.plugin_loader.load_plugins()
        if not self.plugins:
            logger.warning("No single-series plugins loaded.")
        
        # Load multi-series plugins
        self.multi_series_plugins = self.plugin_loader.load_multi_series_plugins()
        if not self.multi_series_plugins:
            logger.info("No multi-series plugins loaded.")
        
        total_plugins = len(self.plugins) + len(self.multi_series_plugins)
        if total_plugins == 0:
            logger.warning("No plugins loaded. Scheduler will run but no jobs will be scheduled.")
        
        jobstores = {
            'default': MemoryJobStore()
        }
        
        executors = {
            'default': AsyncIOExecutor()
        }
        
        job_defaults = {
            'coalesce': True,  # Combine missed runs into one
            'max_instances': 1,  # Prevent concurrent runs of same job
            'misfire_grace_time': 300  # Allow 5 minutes grace for missed jobs
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=Config.SCHEDULER_TIMEZONE
        )
        
        logger.info("Scheduler configured successfully")
    
    async def start(self):
        """Start the scheduler and register all plugin jobs"""
        if not self.scheduler:
            raise RuntimeError("Scheduler not initialized. Call initialize() first.")
        
        logger.info("Starting scheduler...")
        
        # Register single-series plugin jobs
        for unique_id, plugin in self.plugins.items():
            try:
                await self._register_plugin_job(unique_id, plugin)
            except Exception as e:
                logger.error(f"Failed to register job for {unique_id}: {e}", exc_info=True)
        
        # Register multi-series plugin jobs
        for group_id, plugin in self.multi_series_plugins.items():
            try:
                await self._register_multi_series_job(group_id, plugin)
            except Exception as e:
                logger.error(f"Failed to register multi-series job for {group_id}: {e}", exc_info=True)
        
        self.scheduler.start()
        logger.info(f"Scheduler started with {len(self.scheduler.get_jobs())} jobs")
        
        logger.info("Triggering initial data fetch for all plugins...")
        await self._run_initial_fetch()
    
    async def _run_initial_fetch(self):
        """Run initial data fetch for all plugins on startup in batches"""
        # Combine single-series and multi-series plugins for initial fetch
        single_items = list(self.plugins.items())
        multi_items = list(self.multi_series_plugins.items())
        
        batch_size = 5  # Reduced to prevent DB/CPU overload during initial fetch
        total_successful = 0
        total_failed = 0
        
        # Process single-series plugins
        logger.info(f"Running initial fetch for {len(single_items)} single-series plugins in batches of {batch_size}")
        
        for i in range(0, len(single_items), batch_size):
            batch = single_items[i:i + batch_size]
            batch_tasks = []
            
            logger.info(f"Processing single-series batch {i//batch_size + 1}/{(len(single_items) + batch_size - 1)//batch_size}")
            
            for unique_id, plugin in batch:
                logger.info(f"Scheduling initial fetch for {unique_id}...")
                task = asyncio.create_task(
                    self._fetch_and_store_data(unique_id, plugin)
                )
                batch_tasks.append(task)
            
            if batch_tasks:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                batch_successful = sum(1 for r in results if not isinstance(r, Exception))
                batch_failed = len(results) - batch_successful
                total_successful += batch_successful
                total_failed += batch_failed
                
                logger.info(f"Batch completed: {batch_successful} successful, {batch_failed} failed")
                
                # Longer delay between batches to let DB recover
                if i + batch_size < len(single_items):
                    await asyncio.sleep(2.0)
        
        # Process multi-series plugins (sequentially since each makes one API call for multiple series)
        logger.info(f"Running initial fetch for {len(multi_items)} multi-series plugins")
        
        for group_id, plugin in multi_items:
            try:
                logger.info(f"Scheduling initial fetch for multi-series group {group_id}...")
                await self._fetch_and_store_multi_series_data(group_id, plugin)
                total_successful += 1
            except Exception as e:
                logger.error(f"Failed initial fetch for multi-series group {group_id}: {e}", exc_info=True)
                total_failed += 1
            await asyncio.sleep(0.3)  # Small delay between groups
        
        logger.info(
            f"Initial data fetch completed: {total_successful} successful, {total_failed} failed"
        )
    
    async def _register_plugin_job(self, unique_id: str, plugin: BasePlugin):
        """Register a scheduled job for a plugin"""
        metadata = plugin.get_metadata()
        update_frequency = metadata.update_frequency
        
        try:
            interval_params = parse_frequency(update_frequency)
        except ValueError as e:
            logger.error(f"Invalid frequency '{update_frequency}' for {unique_id}: {e}")
            return
        
        trigger = IntervalTrigger(**interval_params)
        
        job_id = f"fetch_{unique_id}"
        self.scheduler.add_job(
            self._fetch_and_store_data,
            trigger=trigger,
            id=job_id,
            name=f"Fetch data: {metadata.name}",
            args=[unique_id, plugin],
            replace_existing=True
        )
        
        logger.info(
            f"Registered job '{job_id}' with interval {interval_params} "
            f"for {metadata.name}"
        )
    
    async def _register_multi_series_job(self, group_id: str, plugin: MultiSeriesPlugin):
        """Register a scheduled job for a multi-series plugin"""
        schedule = plugin.schedule
        
        try:
            interval_params = parse_frequency(schedule)
        except ValueError as e:
            logger.error(f"Invalid schedule '{schedule}' for multi-series group {group_id}: {e}")
            return
        
        trigger = IntervalTrigger(**interval_params)
        
        job_id = f"fetch_multi_{group_id}"
        series_count = len(plugin.get_series_definitions())
        
        self.scheduler.add_job(
            self._fetch_and_store_multi_series_data,
            trigger=trigger,
            id=job_id,
            name=f"Fetch multi-series: {group_id} ({series_count} series)",
            args=[group_id, plugin],
            replace_existing=True
        )
        
        logger.info(
            f"Registered multi-series job '{job_id}' with interval {interval_params} "
            f"for group {group_id} ({series_count} time series)"
        )
    
    async def _fetch_and_store_data(self, unique_id: str, plugin: BasePlugin):
        """
        Fetch data from plugin and store in database.
        This is the main job function that gets executed on schedule.
        Uses semaphore to limit concurrent jobs and prevent DB connection exhaustion.
        """
        metadata = plugin.get_metadata()
        job_start = datetime.now()
        
        logger.info(f"[{unique_id}] Starting data fetch job...")
        
        # Use semaphore to limit concurrent jobs
        async with self.job_semaphore:
            active_jobs = self.max_concurrent_jobs - self.job_semaphore._value
            logger.info(f"[{unique_id}] Acquired job semaphore (active jobs: {active_jobs}/{self.max_concurrent_jobs})")
            
            # Log pool status periodically (every 10th job)
            if active_jobs % 10 == 0:
                from src.database import log_pool_status
                log_pool_status()
            
            try:
                # Get database session using async context manager
                async with SessionLocal() as session:
                    repo = TimeSeriesDataRepository(session)
                    
                    # Get or create series_id
                    series_id = await repo.get_or_create_series_id(
                        name=metadata.name,
                        unique_id=unique_id,
                        description=metadata.description,
                        frequency=metadata.frequency,
                        unit=getattr(metadata, 'unit', ''),
                        domain=getattr(metadata, 'domain', ''),
                        category=getattr(metadata, 'category', ''),
                        subcategory=getattr(metadata, 'subcategory', ''),
                        imputation_policy=getattr(metadata, 'imputation_policy', None),
                        update_frequency=metadata.update_frequency
                    )
                    
                    # Determine start date to fetch - always get last 1000 values based on frequency
                    # No end_date is provided as APIs may operate in different timezones
                    # and should return data up to the latest available
                    interval_seconds = get_interval_seconds(metadata.update_frequency)
                    start_date = (datetime.now() - timedelta(seconds=1000 * interval_seconds)).isoformat()
                    
                    logger.info(f"[{unique_id}] Fetching data from {start_date} to latest available")
                    
                    # Fetch data from plugin with retry logic (no end_date)
                    data = await self._fetch_with_retry(plugin, start_date, unique_id)
                    
                    if not data or 'data' not in data:
                        logger.warning(f"[{unique_id}] No data returned from plugin")
                        return
                    
                    data_points = data['data']
                    
                    if not data_points:
                        logger.info(f"[{unique_id}] No new data points to store")
                        return
                    
                    # Apply imputation to fill gaps
                    imputation_service = ImputationService()
                    frequency_td = parse_frequency_to_timedelta(metadata.frequency)
                    imputed_data, n_interpolated, n_null = imputation_service.impute_gaps(
                        data_points, frequency_td
                    )
                    
                    if n_interpolated > 0 or n_null > 0:
                        logger.info(
                            f"[{unique_id}] Imputation: {n_interpolated} interpolated, "
                            f"{n_null} NULL markers added"
                        )
                    
                    # Store data in operational table (without quality_code)
                    # Filter out NULL values for operational table
                    operational_data = [
                        {'ts': p['ts'], 'value': p['value']} 
                        for p in imputed_data if p.get('value') is not None
                    ]
                    rows_affected = await repo.upsert_data_points(series_id, operational_data)
                    
                    # Store in SCD2 table with quality_code
                    from src.repositories.time_series_scd2_repository import TimeSeriesDataSCD2Repository
                    scd2_repo = TimeSeriesDataSCD2Repository(session)
                    scd2_stats = await scd2_repo.upsert_data_points(series_id, imputed_data)
                    
                    duration = (datetime.now() - job_start).total_seconds()
                    logger.info(
                        f"[{unique_id}] Job completed successfully in {duration:.2f}s. "
                        f"Stored {rows_affected} data points. "
                        f"SCD2: {scd2_stats['inserted']} new, {scd2_stats['updated']} updated, "
                        f"{scd2_stats['unchanged']} unchanged."
                    )
                    
                    # Update timezone if detected
                    detected_timezone = plugin.get_detected_timezone()
                    if detected_timezone:
                        await repo.update_series_timezone(series_id, detected_timezone)
                        logger.info(f"[{unique_id}] Updated timezone to {detected_timezone}")
                
            except Exception as e:
                duration = (datetime.now() - job_start).total_seconds()
                logger.error(
                    f"[{unique_id}] Job failed after {duration:.2f}s: {e}",
                    exc_info=True
                )
    
    async def _fetch_and_store_multi_series_data(self, group_id: str, plugin: MultiSeriesPlugin):
        """
        Fetch data from multi-series plugin and store in database.
        Makes ONE API call and stores data for multiple time series.
        """
        job_start = datetime.now()
        series_definitions = plugin.get_series_definitions()
        
        logger.info(f"[{group_id}] Starting multi-series data fetch for {len(series_definitions)} series...")
        
        # Use semaphore to limit concurrent jobs
        async with self.job_semaphore:
            active_jobs = self.max_concurrent_jobs - self.job_semaphore._value
            logger.info(f"[{group_id}] Acquired job semaphore (active jobs: {active_jobs}/{self.max_concurrent_jobs})")
            
            try:
                # Get database session using async context manager
                async with SessionLocal() as session:
                    repo = TimeSeriesDataRepository(session)
                    imputation_service = ImputationService()
                    
                    # Determine start date based on the smallest update frequency
                    min_interval = min(
                        get_interval_seconds(s.update_frequency or '15 minutes') 
                        for s in series_definitions
                    )
                    # Use fixed 24-hour lookback for multi-series plugins
                    # (prevents hitting Fingrid API pagination limits)
                    start_date = (datetime.now() - timedelta(hours=24)).isoformat()
                    
                    logger.info(f"[{group_id}] Fetching data from {start_date} to latest available")
                    
                    # Fetch data from plugin with retry logic (ONE API call)
                    data = await self._fetch_multi_with_retry(plugin, start_date, group_id)
                    
                    if not data:
                        logger.warning(f"[{group_id}] No data returned from multi-series plugin")
                        return
                    
                    # Process each time series from the response
                    total_rows = 0
                    total_interpolated = 0
                    total_null = 0
                    
                    for series_def in series_definitions:
                        unique_id = series_def.unique_id
                        series_data = data.get(unique_id, [])
                        
                        if not series_data:
                            logger.debug(f"[{group_id}] No data for series {unique_id}")
                            continue
                        
                        # Get or create series_id
                        series_id = await repo.get_or_create_series_id(
                            name=series_def.name,
                            unique_id=unique_id,
                            description=series_def.description,
                            frequency=series_def.frequency,
                            unit=series_def.unit,
                            domain=series_def.domain,
                            category=series_def.category,
                            subcategory=series_def.subcategory or '',
                            imputation_policy=series_def.imputation_policy,
                            update_frequency=series_def.update_frequency or '15 minutes'
                        )
                        
                        # Apply imputation to fill gaps
                        frequency_td = parse_frequency_to_timedelta(series_def.frequency)
                        imputed_data, n_interpolated, n_null = imputation_service.impute_gaps(
                            series_data, frequency_td
                        )
                        total_interpolated += n_interpolated
                        total_null += n_null
                        
                        # Store data in operational table (without quality_code, filter NULL values)
                        operational_data = [
                            {'ts': p['ts'], 'value': p['value']} 
                            for p in imputed_data if p.get('value') is not None
                        ]
                        rows_affected = await repo.upsert_data_points(series_id, operational_data)
                        total_rows += rows_affected
                        
                        # Store in SCD2 table with quality_code
                        from src.repositories.time_series_scd2_repository import TimeSeriesDataSCD2Repository
                        scd2_repo = TimeSeriesDataSCD2Repository(session)
                        await scd2_repo.upsert_data_points(series_id, imputed_data)
                        
                        logger.debug(f"[{group_id}] Stored {rows_affected} points for {unique_id}")
                        
                        # Update timezone if detected
                        detected_tz = plugin.get_detected_timezone(unique_id)
                        if detected_tz:
                            await repo.update_series_timezone(series_id, detected_tz)
                    
                    duration = (datetime.now() - job_start).total_seconds()
                    imputation_msg = ""
                    if total_interpolated > 0 or total_null > 0:
                        imputation_msg = f" Imputation: {total_interpolated} interpolated, {total_null} NULL markers."
                    logger.info(
                        f"[{group_id}] Multi-series job completed in {duration:.2f}s. "
                        f"Stored {total_rows} total data points across {len(series_definitions)} series.{imputation_msg}"
                    )
                
            except Exception as e:
                duration = (datetime.now() - job_start).total_seconds()
                logger.error(
                    f"[{group_id}] Multi-series job failed after {duration:.2f}s: {e}",
                    exc_info=True
                )
    
    async def _fetch_multi_with_retry(
        self, 
        plugin: MultiSeriesPlugin, 
        start_date: str, 
        group_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch data from multi-series plugin with exponential backoff retry logic.
        """
        max_retries = Config.MAX_RETRIES
        retry_delay = Config.RETRY_DELAY_SECONDS
        
        for attempt in range(max_retries):
            try:
                data = await plugin.get_historical_data_multi(start_date)
                return data
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(
                        f"[{group_id}] Multi-series fetch attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"[{group_id}] All {max_retries} multi-series fetch attempts failed"
                    )
                    raise
    
    async def _fetch_with_retry(
        self, 
        plugin: BasePlugin, 
        start_date: str, 
        unique_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch data from plugin with exponential backoff retry logic.
        No end_date is provided - plugins should return data up to the latest available.
        """
        max_retries = Config.MAX_RETRIES
        retry_delay = Config.RETRY_DELAY_SECONDS
        
        for attempt in range(max_retries):
            try:
                data = await plugin.get_historical_data(start_date)
                return data
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"[{unique_id}] Fetch attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"[{unique_id}] All {max_retries} fetch attempts failed"
                    )
                    raise
    
    async def shutdown(self):
        """Gracefully shutdown the scheduler"""
        if self.scheduler:
            logger.info("Shutting down scheduler...")
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler shutdown complete")
    
    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self.scheduler is not None and self.scheduler.running
    
    def get_job_count(self) -> int:
        """Get number of scheduled jobs"""
        if self.scheduler:
            return len(self.scheduler.get_jobs())
        return 0
    
    def get_job_info(self) -> list:
        """Get information about all scheduled jobs"""
        if not self.scheduler:
            return []
        
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        return jobs
