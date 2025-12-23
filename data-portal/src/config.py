"""Configuration management for data-portal service"""

import os
from typing import Optional


def read_secret_file(file_path: str) -> Optional[str]:
    """Reads a secret value from a file (for Docker Secrets)"""
    try:
        with open(file_path, 'r') as f:
            return f.read().strip()
    except (FileNotFoundError, IOError):
        return None


def get_env_or_secret(env_var: str, secret_file_var: Optional[str] = None) -> Optional[str]:
    """Gets a value from environment variable or Docker secret file"""
    # First try environment variable
    value = os.getenv(env_var)
    if value:
        return value
    
    # If not found, try Docker secret file
    if secret_file_var:
        secret_file_path = os.getenv(secret_file_var)
        if secret_file_path:
            return read_secret_file(secret_file_path)
    
    return None


class Config:
    """Data portal configuration"""
    
    # Database Configuration
    DATABASE_URL = get_env_or_secret("DATABASE_URL", "DATABASE_URL_FILE") or ""
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Scheduler Configuration
    SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "UTC")
    SCHEDULER_JOBSTORE_URL = get_env_or_secret("DATABASE_URL", "DATABASE_URL_FILE") or ""
    
    # Plugin Configuration
    PLUGIN_CONFIG_PATH = os.getenv("PLUGIN_CONFIG_PATH", "src/plugins/configs/sources.yaml")
    
    # Retry Configuration
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    
    # Health Check Configuration
    HEALTHCHECK_INTERVAL_SECONDS = int(os.getenv("HEALTHCHECK_INTERVAL_SECONDS", "300"))
