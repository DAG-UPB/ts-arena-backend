"""
Data Portal Service - Main Entry Point

This service automatically fetches time series data from configured sources
and writes them to the TimescaleDB database according to their update schedules.

Pure data collection worker - scheduler loop only.
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from src.config import Config
from src.scheduler.scheduler import DataPortalScheduler

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

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
