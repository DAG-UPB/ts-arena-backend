"""
Data Portal Service - Main Entry Point

This service automatically fetches time series data from configured sources
and writes them to the TimescaleDB database according to their update schedules.

Pure data collection worker - scheduler loop only.
"""

import asyncio
import logging
import os
import signal
from typing import Optional

# No TTY here, so a progress bar is not a progress bar -- tqdm's carriage returns land in
# the container log as a fresh line per refresh. `gridstatus` wraps its per-day download
# loops in tqdm, which was writing ~55 lines per 17-minute window, none of them readable
# and none of them going through logging at all (ts-arena-15). setdefault, so
# TQDM_DISABLE stays an operator knob. Must precede any import that pulls in tqdm.
os.environ.setdefault("TQDM_DISABLE", "1")

from src.config import Config  # noqa: E402
from src.logging_setup import configure_logging  # noqa: E402
from src.scheduler.scheduler import DataPortalScheduler  # noqa: E402

# One format across every backend service, and timestamps in UTC rather than local
# container time -- the scheduler runs on Config.SCHEDULER_TIMEZONE, so an unmarked
# local timestamp could not be lined up against a job's own schedule. This also
# replaces `getattr(logging, Config.LOG_LEVEL)`, which had no default and so raised
# AttributeError on any typo in the env var.
configure_logging("data-portal", level=Config.LOG_LEVEL)

logger = logging.getLogger(__name__)

scheduler: Optional[DataPortalScheduler] = None
shutdown_event = asyncio.Event()


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()


async def main():
    """Main entry point for the data collection worker"""
    global scheduler
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("Starting Data Portal Service...")
        scheduler = DataPortalScheduler()
        await scheduler.initialize()
        await scheduler.start()
        
        logger.info("Data Portal Service started successfully - collecting data according to schedule")
        
        await shutdown_event.wait()
        
    except Exception as e:
        logger.error(f"Error during operation: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down Data Portal Service...")
        if scheduler:
            await scheduler.shutdown()
        logger.info("Data Portal Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
