"""Imputation service for handling missing values in time series data"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from src.config import Config

logger = logging.getLogger(__name__)

# Quality code constants
QUALITY_ORIGINAL = 0  # Original data from source
QUALITY_IMPUTED = 1   # Imputed/interpolated value


class ImputationService:
    """
    Service for detecting and filling gaps in time series data.
    
    Supports modular imputation methods:
    - linear: Linear interpolation between known values
    - (future) locf: Last observation carried forward
    - (future) mean: Fill with series mean
    
    Gaps larger than MAX_GAP_FACTOR * frequency are marked with NULL values
    instead of being interpolated.
    """
    
    def __init__(
        self,
        enabled: bool = None,
        max_gap_factor: int = None,
        method: str = 'linear'
    ):
        """
        Initialize imputation service.
        
        Args:
            enabled: Whether imputation is enabled (defaults to Config.ENABLE_IMPUTATION)
            max_gap_factor: Maximum gap size as multiple of frequency (defaults to Config.MAX_GAP_FACTOR)
            method: Imputation method ('linear' for now)
        """
        self.enabled = enabled if enabled is not None else Config.ENABLE_IMPUTATION
        self.max_gap_factor = max_gap_factor if max_gap_factor is not None else Config.MAX_GAP_FACTOR
        self.method = method
        
        logger.info(
            f"ImputationService initialized: enabled={self.enabled}, "
            f"max_gap_factor={self.max_gap_factor}, method={self.method}"
        )
    
    def impute_gaps(
        self,
        data_points: List[Dict[str, Any]],
        frequency: timedelta
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Detect and fill gaps in time series data.
        
        Args:
            data_points: List of dicts with 'ts' and 'value' keys
            frequency: Expected frequency of the time series
            
        Returns:
            Tuple of:
            - Extended list including imputed data points with quality_code
            - Count of interpolated values
            - Count of NULL gap markers
        """
        if not self.enabled:
            # Add quality_code=0 to all original points and return
            return self._add_quality_codes(data_points, QUALITY_ORIGINAL), 0, 0
        
        if not data_points or len(data_points) < 2:
            return self._add_quality_codes(data_points, QUALITY_ORIGINAL), 0, 0
        
        # Parse and sort data points by timestamp
        parsed_points = self._parse_and_sort(data_points)
        
        if not parsed_points:
            return [], 0, 0
        
        # Detect gaps and fill them
        result = []
        interpolated_count = 0
        null_marker_count = 0
        
        for i, point in enumerate(parsed_points):
            # Add the original point with quality_code=0
            result.append({
                'ts': point['ts'],
                'value': point['value'],
                'quality_code': QUALITY_ORIGINAL
            })
            
            # Check for gap before next point
            if i < len(parsed_points) - 1:
                next_point = parsed_points[i + 1]
                gap_points, n_interpolated, n_null = self._fill_gap(
                    point, next_point, frequency
                )
                result.extend(gap_points)
                interpolated_count += n_interpolated
                null_marker_count += n_null
        
        logger.debug(
            f"Imputation complete: {len(data_points)} original, "
            f"{interpolated_count} interpolated, {null_marker_count} NULL markers"
        )
        
        return result, interpolated_count, null_marker_count
    
    def _parse_and_sort(
        self, 
        data_points: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse timestamps and sort data points chronologically."""
        parsed = []
        for point in data_points:
            ts = point.get('ts')
            value = point.get('value')
            
            if ts is None or value is None:
                continue
            
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            
            parsed.append({
                'ts': ts,
                'value': float(value)
            })
        
        return sorted(parsed, key=lambda x: x['ts'])
    
    def _fill_gap(
        self,
        start_point: Dict[str, Any],
        end_point: Dict[str, Any],
        frequency: timedelta
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Fill a gap between two data points.
        
        Returns:
            Tuple of:
            - List of imputed points (empty if no gap)
            - Count of interpolated values
            - Count of NULL markers
        """
        start_ts = start_point['ts']
        end_ts = end_point['ts']
        gap_duration = end_ts - start_ts
        
        # Check if there's actually a gap (more than 1 frequency interval)
        tolerance = frequency * 1.5  # Allow some tolerance
        if gap_duration <= tolerance:
            return [], 0, 0
        
        # Calculate number of missing points
        n_missing = int(gap_duration / frequency) - 1
        
        if n_missing <= 0:
            return [], 0, 0
        
        # Check if gap is too large for interpolation
        max_gap_duration = frequency * self.max_gap_factor
        is_large_gap = gap_duration > max_gap_duration
        
        result = []
        interpolated_count = 0
        null_marker_count = 0
        
        for i in range(1, n_missing + 1):
            imputed_ts = start_ts + (frequency * i)
            
            if is_large_gap:
                # Large gap: insert NULL marker
                result.append({
                    'ts': imputed_ts,
                    'value': None,
                    'quality_code': QUALITY_IMPUTED
                })
                null_marker_count += 1
            else:
                # Small gap: interpolate
                imputed_value = self._interpolate(
                    start_point['value'],
                    end_point['value'],
                    i,
                    n_missing + 1
                )
                result.append({
                    'ts': imputed_ts,
                    'value': imputed_value,
                    'quality_code': QUALITY_IMPUTED
                })
                interpolated_count += 1
        
        if result:
            gap_type = "NULL markers" if is_large_gap else "interpolated"
            logger.debug(
                f"Filled gap from {start_ts} to {end_ts} with {len(result)} {gap_type} points"
            )
        
        return result, interpolated_count, null_marker_count
    
    def _interpolate(
        self,
        start_value: float,
        end_value: float,
        step: int,
        total_steps: int
    ) -> float:
        """
        Linear interpolation between two values.
        
        Args:
            start_value: Value at start of gap
            end_value: Value at end of gap
            step: Current step (1-indexed)
            total_steps: Total number of steps (including endpoints)
            
        Returns:
            Interpolated value at the given step
        """
        if self.method == 'linear':
            return start_value + (end_value - start_value) * (step / total_steps)
        else:
            raise ValueError(f"Unknown imputation method: {self.method}")
    
    def _add_quality_codes(
        self,
        data_points: List[Dict[str, Any]],
        quality_code: int
    ) -> List[Dict[str, Any]]:
        """Add quality_code to all data points."""
        result = []
        for point in data_points:
            result.append({
                **point,
                'quality_code': quality_code
            })
        return result


def parse_frequency_to_timedelta(frequency: Any) -> timedelta:
    """
    Parse a frequency string or timedelta into a timedelta object.
    
    Supports:
    - timedelta objects (passed through)
    - PostgreSQL INTERVAL strings: '1 hour', '15 minutes', '1 day'
    - ISO 8601 durations: 'PT1H', 'PT15M', 'P1D'
    
    Args:
        frequency: Frequency as string or timedelta
        
    Returns:
        timedelta object
    """
    import re
    import isodate
    
    if isinstance(frequency, timedelta):
        return frequency
    
    if not isinstance(frequency, str):
        raise ValueError(f"Invalid frequency type: {type(frequency)}")
    
    frequency = frequency.strip()
    
    # Try ISO 8601 format first
    if frequency.startswith('P'):
        try:
            duration = isodate.parse_duration(frequency)
            if not isinstance(duration, timedelta):
                duration = duration.totimedelta(start=datetime.now())
            return duration
        except Exception:
            pass
    
    # Try PostgreSQL INTERVAL format
    match = re.match(r'^(\d+)\s*(second|minute|hour|day|week)s?$', frequency.lower())
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'second':
            return timedelta(seconds=value)
        elif unit == 'minute':
            return timedelta(minutes=value)
        elif unit == 'hour':
            return timedelta(hours=value)
        elif unit == 'day':
            return timedelta(days=value)
        elif unit == 'week':
            return timedelta(weeks=value)
    
    raise ValueError(f"Could not parse frequency: {frequency}")
