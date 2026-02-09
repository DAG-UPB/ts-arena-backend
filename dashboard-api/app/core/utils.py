"""Utility functions for the Dashboard API."""

from typing import List, Optional
from datetime import timedelta
import isodate


def serialize_timedelta_to_iso8601(value: Optional[timedelta]) -> Optional[str]:
    """
    Convert timedelta to ISO 8601 duration format (PostgreSQL interval format).
    
    Args:
        value: timedelta object or None
        
    Returns:
        ISO 8601 duration string like 'P1D' (1 day), 'PT1H' (1 hour), 'PT15M' (15 minutes), or None
    """
    if value is None:
        return None
    
    return isodate.duration_isoformat(value)

def parse_iso8601_to_interval_list(
    iso_strings: List[str]
) -> List[str]:
    """
    Converts list of ISO 8601 duration strings to PostgreSQL INTERVAL strings.
    
    Args:
        iso_strings: List of ISO 8601 strings (e.g. ["PT1H", "P1D"])
    
    Returns:
        List of PostgreSQL INTERVAL strings (e.g. ["3600 seconds", "86400 seconds"])
    
    Raises:
        ValueError: If a string cannot be parsed
    
    Examples:
        >>> parse_iso8601_to_interval_list(["PT1H", "P1D"])
        ["3600 seconds", "86400 seconds"]
    """
    intervals = []
    for iso_str in iso_strings:
        try:
            duration = isodate.parse_duration(iso_str)
            # Convert to timedelta if necessary
            if isinstance(duration, timedelta):
                seconds = int(duration.total_seconds())
            else:
                # isodate.Duration (more complex formats with months/years)
                # Approximation necessary
                seconds = int(duration.totimedelta(start=None).total_seconds())
            intervals.append(f"{seconds} seconds")
        except Exception as e:
            raise ValueError(f"Invalid ISO 8601 duration '{iso_str}': {e}")
    
    return intervals


def parse_comma_separated(value: Optional[str]) -> Optional[List[str]]:
    """
    Parses comma-separated strings into lists.
    
    Args:
        value: Comma-separated string or None
    
    Returns:
        List of trimmed strings or None
    
    Examples:
        >>> parse_comma_separated("Energy, Finance")
        ["Energy", "Finance"]
        >>> parse_comma_separated(None)
        None
    """
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]
