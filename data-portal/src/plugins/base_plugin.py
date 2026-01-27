"""Base plugin interface for data sources"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class TimeSeriesDefinition:
    """Definition of a single time series within a request group"""
    unique_id: str
    name: str
    description: str
    frequency: str
    unit: str
    domain: str
    category: str
    subcategory: Optional[str] = None
    update_frequency: Optional[str] = None
    extract_filter: Dict[str, Any] = field(default_factory=dict)


class TimeSeriesMetadata:
    """Metadata for a time series data source"""
    
    def __init__(
        self,
        unique_id: str,
        name: str,
        description: str,
        frequency: str,
        unit: str,
        domain: str,
        category: str,
        subcategory: Optional[str],
        update_frequency: str
    ):
        """
        Initialize time series metadata.
        
        Args:
            unique_id: Unique prefix for API endpoint
            name: Display name of the time series
            description: Description of the data source
            frequency: Data frequency as PostgreSQL INTERVAL string (e.g., '1 hour', '15 minutes', '1 day')
                      This will be stored as INTERVAL type in the database
            unit: Unit of measurement (e.g., 'MWh', '°C')
            domain: Domain category (e.g., 'energy', 'weather')
            category: Category (e.g., 'generation', 'temperature')
            subcategory: Subcategory (e.g., 'nuclear', 'wind')
            update_frequency: How often the data source is updated (PostgreSQL INTERVAL string)
        """
        self.unique_id = unique_id
        self.name = name
        self.description = description
        self.frequency = frequency
        self.unit = unit
        self.domain = domain
        self.category = category
        self.subcategory = subcategory
        self.update_frequency = update_frequency


class BasePlugin(ABC):
    """Base interface for all data source plugins"""

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        self._meta = metadata
        self._defaults = default_params

    def get_metadata(self) -> TimeSeriesMetadata:
        """Returns metadata for this data source"""
        return self._meta
    
    def get_unique_id(self) -> str:
        """Returns the unique id for this data source"""
        return self._meta.unique_id
    
    def get_update_frequency(self) -> str:
        """Returns the update frequency for this data source"""
        return self._meta.update_frequency
    
    @abstractmethod
    async def get_historical_data(
        self, 
        start_date: str, 
        end_date: Optional[str] = None, 
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch historical data from the data source
        
        Args:
            start_date: Start date in ISO format
            end_date: Optional end date in ISO format. If not provided, 
                      the API should return data up to the latest available.
                      This is preferred as APIs may operate in different timezones.
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


class MultiSeriesPlugin(ABC):
    """
    Base interface for plugins that return multiple time series from a single API call.
    
    Use this when an API returns data for multiple time series in one response,
    to avoid making redundant API calls for each series.
    """

    def __init__(
        self, 
        group_id: str,
        request_params: Dict[str, Any], 
        series_definitions: List[TimeSeriesDefinition],
        schedule: str
    ):
        """
        Initialize multi-series plugin.
        
        Args:
            group_id: Unique identifier for this request group
            request_params: Common parameters for the API request
            series_definitions: List of time series definitions to extract from response
            schedule: Update frequency for this group (PostgreSQL INTERVAL string)
        """
        self._group_id = group_id
        self._request_params = request_params
        self._series_definitions = series_definitions
        self._schedule = schedule
    
    @property
    def group_id(self) -> str:
        """Returns the group ID for this plugin"""
        return self._group_id
    
    @property
    def schedule(self) -> str:
        """Returns the update schedule for this plugin"""
        return self._schedule
    
    @property
    def request_params(self) -> Dict[str, Any]:
        """Returns the common request parameters"""
        return self._request_params
    
    def get_series_definitions(self) -> List[TimeSeriesDefinition]:
        """Returns all time series definitions for this group"""
        return self._series_definitions
    
    def get_unique_ides(self) -> List[str]:
        """Returns list of all unique ides in this group"""
        return [s.unique_id for s in self._series_definitions]
    
    @abstractmethod
    async def get_historical_data_multi(
        self, 
        start_date: str, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch historical data for all time series in this group with a single API call.
        
        Args:
            start_date: Start date in ISO format
            end_date: Optional end date in ISO format. If not provided, 
                      the API should return data up to the latest available.
            
        Returns:
            Dict mapping unique_id to list of data points:
            {
                "series-1": [
                    {"ts": "2025-08-05T14:00:00Z", "value": 22.5},
                    {"ts": "2025-08-05T14:05:00Z", "value": 22.7},
                    ...
                ],
                "series-2": [
                    {"ts": "2025-08-05T14:00:00Z", "value": 100.0},
                    ...
                ]
            }
        """
        pass
