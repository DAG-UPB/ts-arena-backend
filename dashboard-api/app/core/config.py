from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    # API Security
    API_KEY: str
    
    # Database (existing TimescaleDB)
    DATABASE_URL: str
    
    # CORS (for arena-app access)
    # Note: In production (Coolify), set CORS_ORIGINS env var to actual domain URLs.
    # The Docker-internal hostname "arena-app" is not valid in standalone deployment.
    CORS_ORIGINS: list[str] = [
        "http://localhost:8501",
        "https://huggingface.co",
    ]
    
    # App
    DEBUG: bool = False
    API_TITLE: str = "TS-Arena Dashboard API"
    API_VERSION: str = "1.0.0"

    # Database connection pool, per uvicorn worker.
    # Budget: DB_POOL_MAX * UVICORN_WORKERS connections against the server's
    # max_connections (100 in prod), which is shared with api-portal and
    # data-portal. The default 8 x 4 workers = 32 leaves ample headroom.
    DB_POOL_MIN: int = 1
    DB_POOL_MAX: int = 8
    
    class Config:
        env_file = ".env"


settings = Settings()
