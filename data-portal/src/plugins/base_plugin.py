"""Base plugin interface for data sources"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime


class TimeSeriesMetadata:
    """Metadata for a time series data source"""
    
    def __init__(
        self,
        endpoint_prefix: str,
        name: str,
        description: str,
        frequency: str,
        unit: str,
        domain: str,
        subdomain: str,
        update_frequency: str
    ):
        """
        Initialize time series metadata.
        
        Args:
            endpoint_prefix: Unique prefix for API endpoint
            name: Display name of the time series
            description: Description of the data source
            frequency: Data frequency as PostgreSQL INTERVAL string (e.g., '1 hour', '15 minutes', '1 day')
                      This will be stored as INTERVAL type in the database
            unit: Unit of measurement (e.g., 'MWh', '°C')
            domain: Domain category (e.g., 'energy', 'weather')
            subdomain: Subdomain/category (e.g., 'generation', 'temperature')
            update_frequency: How often the data source is updated (PostgreSQL INTERVAL string)
        """
        self.endpoint_prefix = endpoint_prefix
        self.name = name
        self.description = description
        self.frequency = frequency
        self.unit = unit
        self.domain = domain
        self.subdomain = subdomain
        self.update_frequency = update_frequency


class BasePlugin(ABC):
    """Base interface for all data source plugins"""

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        self._meta = metadata
        self._defaults = default_params

    def get_metadata(self) -> TimeSeriesMetadata:
        """Returns metadata for this data source"""
        return self._meta
    
    def get_endpoint_prefix(self) -> str:
        """Returns the endpoint prefix for this data source"""
        return self._meta.endpoint_prefix
    
    def get_update_frequency(self) -> str:
        """Returns the update frequency for this data source"""
        return self._meta.update_frequency
    
    @abstractmethod
    async def get_historical_data(
        self, 
        start_date: str, 
        end_date: str, 
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch historical data from the data source
        
        Args:
            start_date: Start date in ISO format
            end_date: End date in ISO format
            metrics: Optional list of metrics to fetch
            
        Returns:
            Dict with the following format:
            {
                "data": [
                    {"ts": "2025-08-05T14:00:00Z", "value": 22.5},
                    {"ts": "2025-08-05T14:05:00Z", "value": 22.7},
                    ...
                ]
            }
        """
        pass
