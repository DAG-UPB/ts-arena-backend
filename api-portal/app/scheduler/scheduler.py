from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from contextlib import AsyncExitStack
from apscheduler import AsyncScheduler, CoalescePolicy
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from app.scheduler.jobs import (
    create_challenge_from_schedule_job,
    prepare_challenge_context_data_job,
    periodic_challenge_scores_evaluation_job
)
import yaml
from pathlib import Path

class ChallengeScheduler:
    """Wraps APScheduler v4 AsyncScheduler and challenge job scheduling with auto-recovery."""

    def __init__(
        self,
        database_url: str,
        *,
        logger: Optional[logging.Logger] = None,
        max_restart_attempts: int = 5,
        restart_delay: float = 5.0,
    ) -> None:
        data_store = SQLAlchemyDataStore(engine_or_url=database_url)
        self.scheduler = AsyncScheduler(data_store=data_store)
        self._started = False
        self._exit_stack: AsyncExitStack | None = None
        self.logger = logger or logging.getLogger("challenge-scheduler")
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._max_restart_attempts = max_restart_attempts
        self._restart_delay = restart_delay
        self._restart_count = 0
        self._config_path: Optional[str] = None

    async def start(self) -> None:
        """Start the scheduler with automatic recovery monitoring."""
        if not self._started:
            try:
                self._exit_stack = AsyncExitStack()
                await self._exit_stack.enter_async_context(self.scheduler)
                await self.scheduler.start_in_background()
                self._started = True
                self._shutdown_event.clear()
                self.logger.info("Scheduler started in background.")
                
                # Schedule the periodic challenge scores evaluation job (every 10 minutes)
                # Called after _started is set to True to avoid recursion
                await self.schedule_periodic_scores_evaluation()
                
                # Start the monitoring task for auto-recovery
                if self._monitor_task is None or self._monitor_task.done():
                    self._monitor_task = asyncio.create_task(self._monitor_scheduler())
                    self.logger.info("Scheduler monitoring task started.")
            except Exception as e:
                self.logger.error(f"Failed to start scheduler: {e}", exc_info=True)
                self._started = False
                raise

    async def shutdown(self) -> None:
        """Gracefully shutdown the scheduler and monitoring task."""
        self._shutdown_event.set()
        
        # Cancel monitoring task
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(self._monitor_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.logger.warning("Monitor task cancellation timed out or was cancelled.")
        
        if self._started:
            try:
                await asyncio.wait_for(self.scheduler.stop(), timeout=5.0)
                await asyncio.wait_for(self.scheduler.cleanup(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("Scheduler shutdown timed out.")
            except Exception as e:
                self.logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)
            finally:
                if self._exit_stack:
                    try:
                        await self._exit_stack.aclose()
                    except Exception as e:
                        self.logger.error(f"Error closing exit stack: {e}", exc_info=True)
                self._started = False
                self.logger.info("Scheduler shut down.")

    async def load_recurring_schedules(self, config_path: str) -> None:
        """
        Loads recurring challenge creation schedules from a YAML file and registers cron jobs.
        """
        self._config_path = config_path  # Store for potential restart
        await self._ensure_started()
        path = Path(config_path)
        if not path.exists():
            self.logger.warning(f"Challenge schedule config not found: {config_path}")
            return

        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.exception(f"Error loading schedule config from {config_path}: {e}")
            return

        schedules = data.get("schedules", [])
        self.logger.info(f"Found {len(schedules)} recurring schedules in {config_path}")

        for schedule_config in schedules:
            schedule_id = schedule_config.get("id")
            cron_expression = schedule_config.get("cron")
            params = schedule_config.get("params", {})
            run_on_startup = schedule_config.get("run_on_startup", False)

            if not schedule_id or not cron_expression:
                self.logger.warning(f"Skipping invalid schedule entry: {schedule_config}")
                continue

            try:
                # Create a deep copy of params to avoid shared references between jobs
                import copy
                params_copy = copy.deepcopy(params)
                
                # Log the parameters being used for this schedule
                self.logger.info(f"Schedule '{schedule_id}' params: {params_copy}")
                
                # Upsert cron job - scheduler is accessed via global reference in job
                await self.scheduler.add_schedule(
                    func_or_task_id=create_challenge_from_schedule_job,
                    trigger=CronTrigger.from_crontab(cron_expression, timezone=timezone.utc),
                    id=schedule_id,
                    args=[params_copy],
                    coalesce=CoalescePolicy.latest,
                    misfire_grace_time=600,
                )
                self.logger.info(f"Upserted cron schedule '{schedule_id}' with cron '{cron_expression}'")

                if run_on_startup:
                    self.logger.info(f"Executing '{schedule_id}' on startup.")
                    # Running the job directly for startup execution with a fresh copy
                    await create_challenge_from_schedule_job(copy.deepcopy(params))

            except Exception as e:
                self.logger.exception(f"Failed to add or run schedule '{schedule_id}': {e}")

    async def schedule_challenge_preparation(
        self,
        job_id: str,
        challenge_id: int,
        run_at: datetime,
        preparation_params: dict[str, Any]
    ) -> None:
        """
        Schedules a one-time job to prepare challenge context data.
        """
        await self._ensure_started()
        
        try:
            # Ensure run_at is timezone-aware (UTC)
            if run_at.tzinfo is None:
                run_at = run_at.replace(tzinfo=timezone.utc)
            
            await self.scheduler.add_schedule(
                func_or_task_id=prepare_challenge_context_data_job,
                trigger=DateTrigger(run_at),
                id=job_id,
                args=[challenge_id, preparation_params],
                coalesce=CoalescePolicy.latest,
                misfire_grace_time=300,  # 5 minute grace period
            )
            self.logger.info(
                f"Scheduled challenge preparation job '{job_id}' "
                f"for challenge {challenge_id} at {run_at}"
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to schedule preparation job for challenge {challenge_id}: {e}"
            )
            raise


    async def schedule_periodic_scores_evaluation(self) -> None:
        """
        Schedules the periodic challenge scores evaluation job.
        Runs at fixed times every 10 minutes (e.g., 12:00, 12:10, 12:20, 12:30, etc.)
        to evaluate scores for active/completed challenges.
        
        Uses fixed minute intervals (0, 10, 20, 30, 40, 50) so the job always runs
        at the same times regardless of when the service starts.
        """
        # Note: _ensure_started() is not called here to avoid recursion
        # This method is only called from start() after the scheduler is already started
        
        try:
            # Run at fixed minute marks: :00, :10, :20, :30, :40, :50
            # This ensures consistent execution times (e.g., 12:00, 12:10, 12:20)
            # regardless of service restart time
            await self.scheduler.add_schedule(
                func_or_task_id=periodic_challenge_scores_evaluation_job,
                trigger=CronTrigger(minute="0,10,20,30,40,50"),
                id="periodic_challenge_scores_evaluation",
                coalesce=CoalescePolicy.latest,
                misfire_grace_time=300,  # 5 minute grace period
            )
            self.logger.info(
                "Scheduled periodic challenge scores evaluation job "
                "(runs at :00, :10, :20, :30, :40, :50 of every hour)"
            )
        except Exception as e:
            self.logger.exception(f"Failed to schedule periodic scores evaluation job: {e}")
            raise

    async def _ensure_started(self) -> None:
        if not self._started:
            await self.start()
    
    async def _monitor_scheduler(self) -> None:
        """
        Monitors the scheduler and automatically restarts it if it crashes.
        Runs in the background as a separate task.
        """
        self.logger.info("Scheduler monitoring started.")
        
        while not self._shutdown_event.is_set():
            try:
                # Wait a bit before checking
                await asyncio.sleep(10)
                
                if self._shutdown_event.is_set():
                    break
                
                # Check if scheduler is still running
                if self._started:
                    try:
                        # Try to access scheduler state to verify it's alive
                        _ = self.scheduler.state
                    except Exception as e:
                        self.logger.error(
                            f"Scheduler health check failed: {e}. Attempting restart...",
                            exc_info=True
                        )
                        await self._attempt_restart()
                
            except asyncio.CancelledError:
                self.logger.info("Scheduler monitoring cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error in scheduler monitor: {e}", exc_info=True)
                await asyncio.sleep(5)
        
        self.logger.info("Scheduler monitoring stopped.")
    
    async def _attempt_restart(self) -> None:
        """
        Attempts to restart the scheduler after a crash.
        """
        if self._restart_count >= self._max_restart_attempts:
            self.logger.error(
                f"Maximum restart attempts ({self._max_restart_attempts}) reached. "
                "Giving up on scheduler restart."
            )
            return
        
        self._restart_count += 1
        self.logger.warning(
            f"Attempting scheduler restart {self._restart_count}/{self._max_restart_attempts}..."
        )
        
        try:
            # Clean up current state
            self._started = False
            if self._exit_stack:
                try:
                    await asyncio.wait_for(self._exit_stack.aclose(), timeout=5.0)
                except Exception as e:
                    self.logger.error(f"Error during cleanup before restart: {e}")
            
            # Wait before restarting
            await asyncio.sleep(self._restart_delay)
            
            # Restart scheduler
            self._exit_stack = AsyncExitStack()
            await self._exit_stack.enter_async_context(self.scheduler)
            await self.scheduler.start_in_background()
            self._started = True
            
            self.logger.info("Scheduler restarted successfully.")
            
            # Reschedule periodic evaluation
            await self.schedule_periodic_scores_evaluation()
            
            # Reload config if available
            if self._config_path:
                try:
                    await self.load_recurring_schedules(self._config_path)
                except Exception as e:
                    self.logger.error(f"Failed to reload schedules after restart: {e}")
            
            # Reset restart counter on successful restart
            self._restart_count = 0
            
        except Exception as e:
            self.logger.error(
                f"Failed to restart scheduler (attempt {self._restart_count}): {e}",
                exc_info=True
            )
            # Will retry on next monitoring cycle
