"""Parse update frequency strings to cron-like intervals"""

import re
from typing import Dict, Any


def parse_frequency(frequency: str) -> Dict[str, Any]:
    """
    Parse frequency string like '1 hour', '30 minutes', '1 day' into APScheduler interval parameters.
    
    Args:
        frequency: String like '1 hour', '30 minutes', '1 day', '7 days', '1 week'
        
    Returns:
        Dict with interval parameters for APScheduler, e.g., {'hours': 1}, {'minutes': 30}
    """
    frequency = frequency.lower().strip()
    
    # Match patterns like "1 hour", "30 minutes", "1 day", "30 seconds"
    match = re.match(r'^(\d+)\s*(second|minute|hour|day|week)s?$', frequency)
    
    if not match:
        raise ValueError(f"Invalid frequency format: {frequency}. Expected format: '<number> <unit>' (e.g., '1 hour', '30 minutes')")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Convert to APScheduler interval parameters
    if unit == 'second':
        return {'seconds': value}
    elif unit == 'minute':
        return {'minutes': value}
    elif unit == 'hour':
        return {'hours': value}
    elif unit == 'day':
        return {'days': value}
    elif unit == 'week':
        return {'weeks': value}
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def get_interval_seconds(frequency: str) -> int:
    """
    Convert frequency string to total seconds.
    
    Args:
        frequency: String like '1 hour', '30 minutes', '1 day'
        
    Returns:
        Total seconds as integer
    """
    params = parse_frequency(frequency)
    
    if 'seconds' in params:
        return params['seconds']
    elif 'minutes' in params:
        return params['minutes'] * 60
    elif 'hours' in params:
        return params['hours'] * 3600
    elif 'days' in params:
        return params['days'] * 86400
    elif 'weeks' in params:
        return params['weeks'] * 604800
    else:
        return 3600  # default to 1 hour
