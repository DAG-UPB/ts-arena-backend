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
from src.plugins.base_plugin import BasePlugin

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
        self.max_concurrent_jobs = 10  # Global limit for parallel jobs
        self.job_semaphore = asyncio.Semaphore(self.max_concurrent_jobs)
        
    async def initialize(self):
        """Initialize scheduler and load plugins"""
        logger.info("Initializing Data Portal Scheduler...")
        
        self.plugins = self.plugin_loader.load_plugins()
        if not self.plugins:
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
        
        for endpoint_prefix, plugin in self.plugins.items():
            try:
                await self._register_plugin_job(endpoint_prefix, plugin)
            except Exception as e:
                logger.error(f"Failed to register job for {endpoint_prefix}: {e}", exc_info=True)
        
        self.scheduler.start()
        logger.info(f"Scheduler started with {len(self.scheduler.get_jobs())} jobs")
        
        logger.info("Triggering initial data fetch for all plugins...")
        await self._run_initial_fetch()
    
    async def _run_initial_fetch(self):
        """Run initial data fetch for all plugins on startup in batches"""
        plugin_items = list(self.plugins.items())
        batch_size = 5  # Reduced to prevent "too many clients" errors
        total_successful = 0
        total_failed = 0
        
        logger.info(f"Running initial fetch for {len(plugin_items)} plugins in batches of {batch_size}")
        
        for i in range(0, len(plugin_items), batch_size):
            batch = plugin_items[i:i + batch_size]
            batch_tasks = []
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(plugin_items) + batch_size - 1)//batch_size}")
            
            for endpoint_prefix, plugin in batch:
                logger.info(f"Scheduling initial fetch for {endpoint_prefix}...")
                task = asyncio.create_task(
                    self._fetch_and_store_data(endpoint_prefix, plugin)
                )
                batch_tasks.append(task)
            
            if batch_tasks:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                batch_successful = sum(1 for r in results if not isinstance(r, Exception))
                batch_failed = len(results) - batch_successful
                total_successful += batch_successful
                total_failed += batch_failed
                
                logger.info(f"Batch completed: {batch_successful} successful, {batch_failed} failed")
                
                # Small delay between batches to let DB recover
                if i + batch_size < len(plugin_items):
                    await asyncio.sleep(2)
        
        logger.info(
            f"Initial data fetch completed: {total_successful} successful, {total_failed} failed"
        )
    
    async def _register_plugin_job(self, endpoint_prefix: str, plugin: BasePlugin):
        """Register a scheduled job for a plugin"""
        metadata = plugin.get_metadata()
        update_frequency = metadata.update_frequency
        
        try:
            interval_params = parse_frequency(update_frequency)
        except ValueError as e:
            logger.error(f"Invalid frequency '{update_frequency}' for {endpoint_prefix}: {e}")
            return
        
        trigger = IntervalTrigger(**interval_params)
        
        job_id = f"fetch_{endpoint_prefix}"
        self.scheduler.add_job(
            self._fetch_and_store_data,
            trigger=trigger,
            id=job_id,
            name=f"Fetch data: {metadata.name}",
            args=[endpoint_prefix, plugin],
            replace_existing=True
        )
        
        logger.info(
            f"Registered job '{job_id}' with interval {interval_params} "
            f"for {metadata.name}"
        )
    
    async def _fetch_and_store_data(self, endpoint_prefix: str, plugin: BasePlugin):
        """
        Fetch data from plugin and store in database.
        This is the main job function that gets executed on schedule.
        Uses semaphore to limit concurrent jobs and prevent DB connection exhaustion.
        """
        metadata = plugin.get_metadata()
        job_start = datetime.now()
        
        logger.info(f"[{endpoint_prefix}] Starting data fetch job...")
        
        # Use semaphore to limit concurrent jobs
        async with self.job_semaphore:
            active_jobs = self.max_concurrent_jobs - self.job_semaphore._value
            logger.info(f"[{endpoint_prefix}] Acquired job semaphore (active jobs: {active_jobs}/{self.max_concurrent_jobs})")
            
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
                        endpoint_prefix=endpoint_prefix,
                        description=metadata.description,
                        frequency=metadata.frequency,
                        unit=getattr(metadata, 'unit', ''),
                        domain=getattr(metadata, 'domain', ''),
                        category=getattr(metadata, 'category', ''),
                        subcategory=getattr(metadata, 'subcategory', ''),
                        update_frequency=metadata.update_frequency
                    )
                    
                    # Determine date range to fetch - always get last 1000 values based on frequency
                    interval_seconds = get_interval_seconds(metadata.update_frequency)
                    start_date = (datetime.now() - timedelta(seconds=1000 * interval_seconds)).isoformat()
                    end_date = datetime.now().isoformat()
                    
                    logger.info(f"[{endpoint_prefix}] Fetching last 1000 values from {start_date} to {end_date}")
                    
                    # Fetch data from plugin with retry logic
                    data = await self._fetch_with_retry(plugin, start_date, end_date, endpoint_prefix)
                    
                    if not data or 'data' not in data:
                        logger.warning(f"[{endpoint_prefix}] No data returned from plugin")
                        return
                    
                    data_points = data['data']
                    
                    if not data_points:
                        logger.info(f"[{endpoint_prefix}] No new data points to store")
                        return
                    
                    # Store data in both regular and SCD2 tables
                    rows_affected = await repo.upsert_data_points(series_id, data_points)
                    
                    # Also write to SCD2 table for history tracking
                    from src.repositories.time_series_scd2_repository import TimeSeriesDataSCD2Repository
                    scd2_repo = TimeSeriesDataSCD2Repository(session)
                    scd2_stats = await scd2_repo.upsert_data_points(series_id, data_points)
                    
                    duration = (datetime.now() - job_start).total_seconds()
                    logger.info(
                        f"[{endpoint_prefix}] Job completed successfully in {duration:.2f}s. "
                        f"Stored {rows_affected} data points. "
                        f"SCD2: {scd2_stats['inserted']} new, {scd2_stats['updated']} updated, "
                        f"{scd2_stats['unchanged']} unchanged."
                    )
                
            except Exception as e:
                duration = (datetime.now() - job_start).total_seconds()
                logger.error(
                    f"[{endpoint_prefix}] Job failed after {duration:.2f}s: {e}",
                    exc_info=True
                )
    
    async def _fetch_with_retry(
        self, 
        plugin: BasePlugin, 
        start_date: str, 
        end_date: str,
        endpoint_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch data from plugin with exponential backoff retry logic.
        """
        max_retries = Config.MAX_RETRIES
        retry_delay = Config.RETRY_DELAY_SECONDS
        
        for attempt in range(max_retries):
            try:
                data = await plugin.get_historical_data(start_date, end_date)
                return data
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"[{endpoint_prefix}] Fetch attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"[{endpoint_prefix}] All {max_retries} fetch attempts failed"
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
