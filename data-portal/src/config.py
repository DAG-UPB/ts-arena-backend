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
    
    DATABASE_URL = get_env_or_secret("DATABASE_URL", "DATABASE_URL_FILE") or ""
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "UTC")
    SCHEDULER_JOBSTORE_URL = get_env_or_secret("DATABASE_URL", "DATABASE_URL_FILE") or ""
    
    PLUGIN_CONFIG_PATH = os.getenv("PLUGIN_CONFIG_PATH", "src/plugins/configs/sources.yaml")
    
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    
    HEALTHCHECK_INTERVAL_SECONDS = int(os.getenv("HEALTHCHECK_INTERVAL_SECONDS", "300"))
    
    # Imputation settings
    ENABLE_IMPUTATION = os.getenv("ENABLE_IMPUTATION", "true").lower() == "true"
    MAX_GAP_FACTOR = int(os.getenv("MAX_GAP_FACTOR", "6"))  # Gaps > 6x frequency get NULL marker

