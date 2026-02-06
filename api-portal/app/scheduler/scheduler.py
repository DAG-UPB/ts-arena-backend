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
    create_round_from_definition_job,
    prepare_round_context_data_job,
    periodic_challenge_scores_evaluation_job,
    periodic_elo_ranking_calculation_job,
    startup_elo_check_job
)
import yaml
from pathlib import Path

class ChallengeScheduler:
    """Wraps APScheduler v4 AsyncScheduler and challenge job scheduling with auto-recovery.
    
    Note: APScheduler v4 alpha has a known bug where exceptions can cause the scheduler
    to crash internally. This wrapper implements robust crash detection by tracking the
    background task and recreating the scheduler instance when needed.
    """

    def __init__(
        self,
        database_url: str,
        *,
        logger: Optional[logging.Logger] = None,
        max_restart_attempts: int = 5,
        restart_delay: float = 5.0,
    ) -> None:
        self._database_url = database_url  # Store for recreation after crash
        self.scheduler = self._create_scheduler()
        self._started = False
        self._exit_stack: AsyncExitStack | None = None
        self.logger = logger or logging.getLogger("challenge-scheduler")
        self._monitor_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None  # Track the scheduler background task
        self._shutdown_event = asyncio.Event()
        self._max_restart_attempts = max_restart_attempts
        self._restart_delay = restart_delay
        self._restart_count = 0
        self._config_path: Optional[str] = None
    
    def _create_scheduler(self) -> AsyncScheduler:
        """Create a new AsyncScheduler instance with the configured data store."""
        data_store = SQLAlchemyDataStore(engine_or_url=self._database_url)
        return AsyncScheduler(data_store=data_store)

    async def start(self) -> None:
        """Start the scheduler with automatic recovery monitoring."""
        if not self._started:
            try:
                self._exit_stack = AsyncExitStack()
                await self._exit_stack.enter_async_context(self.scheduler)
                
                # Start scheduler in a tracked task instead of using start_in_background()
                # This allows us to detect when the scheduler crashes internally
                self._scheduler_task = asyncio.create_task(
                    self._run_scheduler_with_crash_handling(),
                    name="scheduler-runner"
                )
                
                self._started = True
                self._shutdown_event.clear()
                self.logger.info("Scheduler started in background.")
                
                # Schedule the periodic challenge scores evaluation job (every 10 minutes)
                # Called after _started is set to True to avoid recursion
                await self.schedule_periodic_scores_evaluation()
                
                # Schedule the ELO ranking calculation job (4x daily)
                await self.schedule_periodic_elo_calculation()
                
                # Run startup ELO check in background (don't block startup!)
                # This allows the application to become healthy before calculation starts
                asyncio.create_task(
                    self._delayed_startup_elo_check(),
                    name="startup-elo-check"
                )
                
                # Start the monitoring task for auto-recovery
                if self._monitor_task is None or self._monitor_task.done():
                    self._monitor_task = asyncio.create_task(
                        self._monitor_scheduler(),
                        name="scheduler-monitor"
                    )
                    self.logger.info("Scheduler monitoring task started.")
            except Exception as e:
                self.logger.error(f"Failed to start scheduler: {e}", exc_info=True)
                self._started = False
                raise

    async def _run_scheduler_with_crash_handling(self) -> None:
        """Run the scheduler and log any crashes."""
        try:
            await self.scheduler.run_until_stopped()
        except Exception as e:
            self.logger.error(f"Scheduler run_until_stopped crashed: {e}", exc_info=True)
            raise  # Re-raise so the task shows as failed

    async def _delayed_startup_elo_check(self) -> None:
        """
        Run startup ELO check with a delay to avoid blocking application startup.
        
        This runs in a background task so the application can become healthy
        and respond to requests while ELO calculations run.
        """
        try:
            # Small delay to let the application fully start
            await asyncio.sleep(5)
            self.logger.info("Starting background ELO check...")
            await startup_elo_check_job()
        except Exception as e:
            self.logger.error(f"Background ELO check failed: {e}", exc_info=True)
            # Don't re-raise - this is a background task

    async def shutdown(self) -> None:
        """Gracefully shutdown the scheduler and monitoring task."""
        self.logger.info("Initiating scheduler shutdown...")
        self._shutdown_event.set()
        
        # Cancel monitoring task first
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._monitor_task), timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.logger.debug("Monitor task cancellation completed.")
        
        if self._started:
            self._started = False  # Prevent restart attempts during shutdown
            try:
                # Stop the scheduler task
                if self._scheduler_task and not self._scheduler_task.done():
                    try:
                        await asyncio.wait_for(self.scheduler.stop(), timeout=5.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("Scheduler stop timed out.")
                    except asyncio.CancelledError:
                        self.logger.debug("Scheduler stop was cancelled.")
                    except Exception as e:
                        self.logger.error(f"Error stopping scheduler: {e}")
                    
                    # Wait for the task to complete after stop
                    try:
                        await asyncio.wait_for(self._scheduler_task, timeout=3.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        self.logger.debug("Scheduler task completed.")
                    except Exception:
                        pass  # Task may have crashed, that's fine during shutdown
                
                # Cleanup resources
                try:
                    await asyncio.wait_for(self.scheduler.cleanup(), timeout=5.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Scheduler cleanup timed out.")
                except asyncio.CancelledError:
                    self.logger.debug("Scheduler cleanup was cancelled.")
                except Exception as e:
                    self.logger.error(f"Error during scheduler cleanup: {e}")
            finally:
                if self._exit_stack:
                    try:
                        await asyncio.wait_for(self._exit_stack.aclose(), timeout=3.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("Exit stack close timed out.")
                    except asyncio.CancelledError:
                        self.logger.debug("Exit stack close was cancelled.")
                    except (Exception, BaseExceptionGroup) as e:
                        # APScheduler v4 alpha can raise ExceptionGroups during cleanup
                        self.logger.warning(f"Exit stack close raised: {type(e).__name__}: {e}")
                self.logger.info("Scheduler shut down.")

    async def load_recurring_schedules(self, config_path: str) -> None:
        """
        Loads recurring challenge creation schedules from a YAML file and registers cron jobs.
        Also syncs definitions to the database for the challenge definitions table.
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

        # Import here to avoid circular imports
        from app.database.connection import SessionLocal
        from app.services.challenge_service import ChallengeService

        for schedule_config in schedules:
            schedule_id = schedule_config.get("id")
            cron_expression = schedule_config.get("cron")
            run_on_startup = schedule_config.get("run_on_startup", False)

            if not schedule_id or not cron_expression:
                self.logger.warning(f"Skipping invalid schedule entry: {schedule_config}")
                continue

            try:
                # Sync definition to database
                definition_id = None
                async with SessionLocal() as session:
                    challenge_service = ChallengeService(session, scheduler=self)
                    definition_id = await challenge_service.sync_definition_from_yaml(schedule_id, schedule_config)
                
                if definition_id is None:
                    self.logger.error(f"Failed to sync definition for {schedule_id}")
                    continue
                
                # Upsert cron job
                await self.scheduler.add_schedule(
                    func_or_task_id=create_round_from_definition_job,
                    trigger=CronTrigger.from_crontab(cron_expression, timezone=timezone.utc),
                    id=schedule_id,
                    args=[definition_id],
                    coalesce=CoalescePolicy.latest,
                    misfire_grace_time=600,
                )
                self.logger.info(f"Upserted cron schedule '{schedule_id}' with cron '{cron_expression}'")

                if run_on_startup:
                    self.logger.info(f"Executing '{schedule_id}' on startup.")
                    await create_round_from_definition_job(definition_id)

            except Exception as e:
                self.logger.exception(f"Failed to add or run schedule '{schedule_id}': {e}")

    async def schedule_challenge_preparation(
        self,
        job_id: str,
        round_id: int,
        run_at: datetime
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
                func_or_task_id=prepare_round_context_data_job,
                trigger=DateTrigger(run_at),
                id=job_id,
                args=[round_id],
                coalesce=CoalescePolicy.latest,
                misfire_grace_time=300,  # 5 minute grace period
            )
            self.logger.info(
                f"Scheduled challenge preparation job '{job_id}' "
                f"for challenge {round_id} at {run_at}"
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to schedule preparation job for challenge {round_id}: {e}"
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
                max_running_jobs=3, 
            )
            self.logger.info(
                "Scheduled periodic challenge scores evaluation job "
                "(runs at :00, :10, :20, :30, :40, :50 of every hour)"
            )
        except Exception as e:
            self.logger.exception(f"Failed to schedule periodic scores evaluation job: {e}")
            raise

    async def schedule_periodic_elo_calculation(self) -> None:
        """
        Schedules the periodic ELO ranking calculation job.
        Runs 4x daily at 00:00, 06:00, 12:00, 18:00 UTC to calculate
        bootstrapped ELO ratings for all models.
        """
        # Note: _ensure_started() is not called here to avoid recursion
        # This method is only called from start() after the scheduler is already started
        
        try:
            # Run 4x daily at fixed hours: 00:00, 06:00, 12:00, 18:00 UTC
            await self.scheduler.add_schedule(
                func_or_task_id=periodic_elo_ranking_calculation_job,
                trigger=CronTrigger(hour="0,6,12,18", minute="0"),
                id="periodic_elo_ranking_calculation",
                coalesce=CoalescePolicy.latest,
                misfire_grace_time=3600,  # 1 hour grace period
                max_running_jobs=1,  # Only one ELO calculation at a time
            )
            self.logger.info(
                "Scheduled periodic ELO ranking calculation job "
                "(runs at 00:00, 06:00, 12:00, 18:00 UTC)"
            )
        except Exception as e:
            self.logger.exception(f"Failed to schedule periodic ELO calculation job: {e}")
            raise

    async def _ensure_started(self) -> None:
        if not self._started:
            await self.start()
    
    async def _monitor_scheduler(self) -> None:
        """
        Monitors the scheduler and automatically restarts it if it crashes.
        Runs in the background as a separate task.
        
        This now properly detects crashes by checking if the scheduler task has
        completed unexpectedly, rather than just checking scheduler.state.
        """
        self.logger.info("Scheduler monitoring started.")
        
        while not self._shutdown_event.is_set():
            try:
                # Wait a bit before checking
                await asyncio.sleep(10)
                
                if self._shutdown_event.is_set():
                    break
                
                # Check if scheduler task has crashed (completed unexpectedly)
                if self._started and self._scheduler_task:
                    if self._scheduler_task.done():
                        # Scheduler task completed - this means it crashed
                        try:
                            # This will raise the exception that caused the crash
                            self._scheduler_task.result()
                            # If we get here, the task completed normally (shouldn't happen)
                            self.logger.warning("Scheduler task exited unexpectedly without error")
                        except asyncio.CancelledError:
                            # Task was cancelled, likely during shutdown
                            if not self._shutdown_event.is_set():
                                self.logger.warning("Scheduler task was cancelled unexpectedly")
                        except Exception as e:
                            self.logger.error(
                                f"Scheduler task crashed with error: {e}",
                                exc_info=True
                            )
                        
                        # Attempt restart if not shutting down
                        if not self._shutdown_event.is_set():
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
        
        Important: APScheduler v4 alpha requires creating a new AsyncScheduler instance
        after a crash because the internal state becomes corrupted.
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
            
            # Cancel scheduler task if still running
            if self._scheduler_task and not self._scheduler_task.done():
                self._scheduler_task.cancel()
                try:
                    await asyncio.wait_for(self._scheduler_task, timeout=3.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            # Close exit stack to cleanup old scheduler resources
            if self._exit_stack:
                try:
                    await asyncio.wait_for(self._exit_stack.aclose(), timeout=5.0)
                except (Exception, BaseExceptionGroup) as e:
                    self.logger.warning(f"Error during cleanup before restart: {type(e).__name__}: {e}")
            
            # Wait before restarting
            await asyncio.sleep(self._restart_delay)
            
            # Create a NEW scheduler instance (required after crash in APScheduler v4 alpha)
            self.scheduler = self._create_scheduler()
            self.logger.info("Created new scheduler instance for restart.")
            
            # Start the new scheduler
            self._exit_stack = AsyncExitStack()
            await self._exit_stack.enter_async_context(self.scheduler)
            
            # Start scheduler in a tracked task
            self._scheduler_task = asyncio.create_task(
                self._run_scheduler_with_crash_handling(),
                name="scheduler-runner"
            )
            
            self._started = True
            self.logger.info("Scheduler restarted successfully.")
            
            # Reschedule periodic evaluation
            await self.schedule_periodic_scores_evaluation()
            
            # Reschedule ELO calculation
            await self.schedule_periodic_elo_calculation()
            
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
