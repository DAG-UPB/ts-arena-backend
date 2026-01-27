"""Fingrid Multi-Series Plugin - Fetches multiple time series with batched API calls"""

import asyncio
import logging
import os
from typing import Dict, Any, List, Optional
import requests
from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)


class FingridMultiApiClient:
    """
    Helper client for Fingrid API that can fetch multiple datasets efficiently.
    Uses rate limiting to stay under API limits (10 calls/minute).
    """
    BASE_URL = "https://data.fingrid.fi/api/datasets"
    
    # Global state to share across instances for rate limiting
    last_call_time = 0.0
    _lock = asyncio.Lock()

    def __init__(self, api_key: str, page_size: int = 20000):
        self.api_key = api_key
        self.page_size = page_size
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": self.api_key})

    async def _wait_for_rate_limit(self):
        """Simple async rate limiter to stay under 10 calls/min."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = 6.5 - (now - FingridMultiApiClient.last_call_time)
            if wait_time > 0:
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            FingridMultiApiClient.last_call_time = asyncio.get_event_loop().time()

    async def fetch_dataset(self, dataset_id: int, start_time: str, end_time: str) -> List[Dict]:
        """
        Fetch data for a single dataset with pagination.
        
        Args:
            dataset_id: Fingrid dataset ID
            start_time: ISO format start time
            end_time: ISO format end time
            
        Returns:
            List of data points with 'ts' and 'value' keys
        """
        all_data = []
        page = 1
        
        while True:
            await self._wait_for_rate_limit()
            
            params = {
                "startTime": start_time,
                "endTime": end_time,
                "format": "json",
                "oneRowPerTimePeriod": "true",
                "page": page,
                "pageSize": self.page_size,
                "locale": "en",
                "sortBy": "startTime",
                "sortOrder": "asc"
            }
            
            url = f"{self.BASE_URL}/{dataset_id}/data"
            logger.info(f"Fingrid Multi: Fetching page {page} for dataset {dataset_id}")
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: self.session.get(url, params=params)
            )
            
            if response.status_code != 200:
                logger.error(f"Fingrid API error {response.status_code} for dataset {dataset_id}: {response.text}")
                break

            data_json = response.json()
            items = data_json.get("data", [])
            
            if not items:
                break
                
            # Normalize data - find the value key dynamically
            exclude_keys = {"startTime", "endTime"}
            first_item = items[0]
            value_key = next((k for k in first_item.keys() if k not in exclude_keys), None)
            
            for item in items:
                value = item.get(value_key) if value_key else None
                all_data.append({
                    "ts": item.get("endTime"),
                    "value": float(value) if value is not None else None
                })
            
            if len(items) < self.page_size:
                break
            page += 1

        logger.info(f"Fingrid Multi: Fetched {len(all_data)} points for dataset {dataset_id}")
        return all_data


class FingridMultiSeriesPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Fingrid data.
    
    Fetches multiple Fingrid datasets efficiently by:
    1. Respecting rate limits across all datasets
    2. Sharing the same HTTP session
    3. Processing all series in one scheduled job
    """

    def __init__(
        self, 
        group_id: str,
        request_params: Dict[str, Any], 
        series_definitions: List[TimeSeriesDefinition],
        schedule: str
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)
        
        self.api_key = os.getenv("API_KEY_SOURCE_FINGRID")
        if not self.api_key:
            logger.error("API_KEY_SOURCE_FINGRID not found in environment variables")
        
        page_size = request_params.get("page_size", 20000)
        self.client = FingridMultiApiClient(self.api_key or "", page_size)

    async def get_historical_data_multi(
        self, 
        start_date: str, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch historical data for all configured Fingrid datasets.
        
        Makes one API call per dataset but shares rate limiting and session.
        
        Args:
            start_date: Start date in ISO format
            end_date: Optional end date. If not provided, uses current time + 2 days.
            
        Returns:
            Dict mapping unique_id to list of data points
        """
        import pandas as pd
        
        # Fingrid requires end_date, fallback to current time + 2 days if not provided
        if end_date is None:
            end_date = (pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=2)).isoformat()
        
        # At this point end_date is guaranteed to be a string
        end_date_str: str = str(end_date)
        
        # Ensure proper ISO format with Z suffix
        if not start_date.endswith('Z') and '+' not in start_date:
            start_date = start_date.replace('+00:00', 'Z')
            if not start_date.endswith('Z'):
                start_date = start_date + 'Z'
        
        if not end_date_str.endswith('Z') and '+' not in end_date_str:
            end_date_str = end_date_str.replace('+00:00', 'Z')
            if not end_date_str.endswith('Z'):
                end_date_str = end_date_str + 'Z'
        
        result: Dict[str, List[Dict[str, Any]]] = {}
        
        for series_def in self._series_definitions:
            unique_id = series_def.unique_id
            dataset_id = series_def.extract_filter.get("dataset_id")
            
            if not dataset_id:
                logger.warning(f"No dataset_id in extract_filter for {unique_id}")
                result[unique_id] = []
                continue
            
            try:
                data = await self.client.fetch_dataset(
                    dataset_id=int(dataset_id),
                    start_time=start_date,
                    end_time=end_date_str
                )
                result[unique_id] = data
            except Exception as e:
                logger.error(f"Failed to fetch dataset {dataset_id} for {unique_id}: {e}")
                result[unique_id] = []
        
        total_points = sum(len(v) for v in result.values())
        logger.info(
            f"Fingrid Multi: Completed fetch for {len(result)} series, "
            f"{total_points} total data points"
        )
        
        return result
