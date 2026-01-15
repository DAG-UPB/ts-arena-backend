from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import Config

# Declarative base for models
Base = declarative_base()

# IMPORTANT: Import all ORM models so that the tables are registered in MetaData
# (especially for string-based ForeignKeys like 'challenges.challenge_id').
# This import has no runtime costs, but ensures the order.
from app.database import models  # noqa: F401

# Create asynchronous database engine
engine = create_async_engine(
    Config.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=40,
    pool_recycle=1800,  # Recycle connections every 30 minutes
    echo=getattr(Config, 'DB_ECHO_LOG', False),
)

# Asynchrone Session-Factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Asynchronous database session dependency"""
    async with SessionLocal() as session:
        yield session
