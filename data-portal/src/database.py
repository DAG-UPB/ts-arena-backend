"""Database connection management for data-portal service"""

import logging
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import event
from src.config import Config

logger = logging.getLogger(__name__)

Base = declarative_base()

engine = create_async_engine(
    Config.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,  # Reduced to prevent "too many clients" errors
    max_overflow=20,  # Reduced to prevent "too many clients" errors
    echo=False,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Async database session dependency"""
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_db_session() -> AsyncSession:
    """Returns a new database session for use in scheduled jobs"""
    return SessionLocal()


def log_pool_status():
    """Log current connection pool status for monitoring"""
    pool = engine.pool
    logger.info(
        f"DB Pool Status - Size: {pool.size()}, "
        f"Checked out: {pool.checkedout()}, "
        f"Overflow: {pool.overflow()}, "
        f"Checked in: {pool.checkedin()}"
    )
