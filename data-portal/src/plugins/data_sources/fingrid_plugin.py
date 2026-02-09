"""Fingrid Data Portal Plugin"""

import asyncio
import logging
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata

logger = logging.getLogger(__name__)

class FingridApiClient:
    """Helper client for Fingrid API with rate limiting logic."""
    BASE_URL = "https://data.fingrid.fi/api/datasets"
    
    # Global state to share across instances for rate limiting (10 per minute = every 6s)
    last_call_time = 0.0
    _lock = asyncio.Lock()

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": self.api_key})

    async def _wait_for_rate_limit(self):
        """Simple async rate limiter to stay under 10 calls/min."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = 6.5 - (now - FingridApiClient.last_call_time)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            FingridApiClient.last_call_time = asyncio.get_event_loop().time()

    async def fetch_data(self, dataset_id: int, start_time: str, end_time: str) -> List[Dict]:
        all_data = []
        page = 1
        page_size = 20000 
        
        while True:
            await self._wait_for_rate_limit()
            
            params = {
                "startTime": start_time,
                "endTime": end_time,
                "format": "json",
                "oneRowPerTimePeriod": "true",
                "page": page,
                "pageSize": page_size,
                "locale": "en",
                "sortBy": "startTime",
                "sortOrder": "asc"
            }
            
            url = f"{self.BASE_URL}/{dataset_id}/data"
            logger.info(f"Fingrid: Fetching page {page} for dataset {dataset_id}")
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: self.session.get(url, params=params))
            
            if response.status_code != 200:
                logger.error(f"Fingrid API error {response.status_code}: {response.text}")
                break

            data_json = response.json()
            items = data_json.get("data", [])
            
            if not items:
                break
                
            # Normalize data
            exclude_keys = {"startTime", "endTime"}
            first_item = items[0]
            value_key = next((k for k in first_item.keys() if k not in exclude_keys), None)
            
            for item in items:
                all_data.append({
                    "ts": item.get("endTime"),
                    "value": float(item.get(value_key)) if value_key and item.get(value_key) is not None else None
                })
            
            if len(items) < page_size:
                break
            page += 1

        return all_data

class FingridDataSourcePlugin(BasePlugin):
    """Plugin for historical time series data from Fingrid"""

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        super().__init__(metadata, default_params)
        self.api_key = os.getenv("API_KEY_SOURCE_FINGRID")
        if not self.api_key:
            logger.error("API_KEY_SOURCE_FINGRID not found in environment variables")
        self.client = FingridApiClient(self.api_key or "")

    def get_detected_timezone(self) -> Optional[str]:
        return "Europe/Helsinki"

    async def get_historical_data(
        self, 
        start_date: str, 
        end_date: Optional[str] = None, 
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch data from Fingrid using provided API client.
        
        Note: Fingrid API requires an end_date. If not provided,
        current time will be used as fallback.
        """
        dataset_id = self._defaults.get("dataset_id")
        if not dataset_id:
            return {"data": []}

        # Fingrid requires end_date, fallback to current time + 2 days if not provided
        if not end_date:
            end_date = (pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=2)).isoformat()

        # Convert dates to ISO with 'Z' as expected by Fingrid
        # start_date/end_date from repo already ISO, but ensure format
        try:
            data = await self.client.fetch_data(dataset_id, start_date, end_date)
            return {"data": data}
        except Exception as e:
            logger.error(f"Error fetching Fingrid data: {e}")
            return {"data": []}
