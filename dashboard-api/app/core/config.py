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
    
    class Config:
        env_file = ".env"


settings = Settings()
