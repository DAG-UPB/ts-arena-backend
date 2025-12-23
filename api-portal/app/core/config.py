# app/core/config.py

import os
import json
from typing import Optional

def read_secret_file(file_path: str) -> Optional[str]:
    """Reads a secret value from a file (for Docker Secrets)"""
    try:
        with open(file_path, 'r') as f:
            return f.read().strip()
    except (FileNotFoundError, IOError):
        return None

def get_env_or_secret(env_var: str, secret_file_var: str = None) -> Optional[str]:
    """Gets a value from environment variable or Docker secret file"""
    # First try to read from environment variable
    value = os.getenv(env_var)
    if value:
        return value
    
    # If not present, try Docker secret file
    if secret_file_var:
        secret_file_path = os.getenv(secret_file_var)
        if secret_file_path:
            return read_secret_file(secret_file_path)
    
    return None

class Config:
    # Database Configuration
    DATABASE_URL = get_env_or_secret("DATABASE_URL", "DATABASE_URL_FILE") or ""
    
    # API Configuration
    API_VERSION = "1.0.0"
    API_KEY = get_env_or_secret("API_KEY", "API_KEY_FILE")    
    # Plugin Configuration
    PLUGIN_DIR = os.getenv("PLUGIN_DIR", "app/plugins/data_sources")
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Forecast Configuration
    FORECAST_HORIZON_OPTIONS = ["60m", "12h", "24h", "7d", "30d"]
    TIME_GRANULARITY_OPTIONS = ["1m", "15m", "1h", "1d", "1w"]
    
    # Scheduler Configuration
    CHALLENGE_SCHEDULE_FILE = os.getenv("CHALLENGE_SCHEDULE_FILE", "app/configs/challenge_schedules.yaml")
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validates the configuration and returns True if all required values are present"""
        required_vars = [
            ("DATABASE_URL", cls.DATABASE_URL),
            ("API_KEY", cls.API_KEY),
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            print(f"❌ Missing required configuration variables: {', '.join(missing_vars)}")
            return False
        
        print("✅ Configuration validated")
        return True