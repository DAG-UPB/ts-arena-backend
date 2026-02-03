"""Fingrid Multi-Series Plugin - Fetches multiple time series with batched API calls"""

import asyncio
import logging
import os
from typing import Dict, Any, List, Optional
from collections import defaultdict
import requests
from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)


class FingridMultiApiClient:
    """
    Helper client for Fingrid API that fetches multiple datasets in a single request.
    Uses the /api/data endpoint which supports querying multiple datasets at once.
    """
    BASE_URL = "https://data.fingrid.fi/api/data"
    
    def __init__(self, api_key: str, page_size: int = 20000):
        self.api_key = api_key
        self.page_size = page_size
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": self.api_key})

    async def fetch_multiple_datasets(
        self, 
        dataset_ids: List[int], 
        start_time: str, 
        end_time: str
    ) -> Dict[int, List[Dict]]:
        """
        Fetch data for multiple datasets in a single batched API call.
        
        Args:
            dataset_ids: List of Fingrid dataset IDs to fetch
            start_time: ISO format start time
            end_time: ISO format end time
            
        Returns:
            Dict mapping dataset_id to list of data points with 'ts' and 'value' keys
        """
        if not dataset_ids:
            return {}
        
        # Initialize result dict for each dataset
        result: Dict[int, List[Dict]] = defaultdict(list)
        
        # Build comma-separated datasets string
        datasets_param = ",".join(str(d) for d in dataset_ids)
        
        page = 1
        total_fetched = 0
        
        while True:
            params = {
                "datasets": datasets_param,
                "startTime": start_time,
                "endTime": end_time,
                "format": "json",
                "oneRowPerTimePeriod": "false",  # Get datasetId in each row
                "page": page,
                "pageSize": self.page_size,
                "locale": "en",
                "sortBy": "startTime",
                "sortOrder": "asc"
            }
            
            logger.info(
                f"Fingrid Multi: Fetching page {page} for {len(dataset_ids)} datasets "
                f"({datasets_param[:50]}{'...' if len(datasets_param) > 50 else ''})"
            )
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: self.session.get(self.BASE_URL, params=params)
            )
            
            if response.status_code != 200:
                logger.error(
                    f"Fingrid API error {response.status_code}: {response.text[:500]}"
                )
                break

            data_json = response.json()
            items = data_json.get("data", [])
            
            if not items:
                break
            
            # Group items by datasetId
            for item in items:
                dataset_id = item.get("datasetId")
                if dataset_id is not None:
                    result[dataset_id].append({
                        "ts": item.get("endTime"),
                        "value": float(item["value"]) if item.get("value") is not None else None
                    })
            
            total_fetched += len(items)
            
            # Check pagination
            pagination = data_json.get("pagination", {})
            if pagination.get("nextPage") is None:
                break
            page += 1

        logger.info(
            f"Fingrid Multi: Fetched {total_fetched} total data points "
            f"across {len(result)} datasets"
        )
        return dict(result)


class FingridMultiSeriesPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Fingrid data.
    
    Fetches multiple Fingrid datasets efficiently by:
    1. Batching all dataset IDs into a single API call
    2. Using the /api/data endpoint with datasets parameter
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
        
        # Build mapping from dataset_id to unique_id for quick lookup
        self._dataset_to_unique_id: Dict[int, str] = {}
        for series_def in self._series_definitions:
            dataset_id = series_def.extract_filter.get("dataset_id")
            if dataset_id is not None:
                self._dataset_to_unique_id[int(dataset_id)] = series_def.unique_id

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "Europe/Helsinki"

    async def get_historical_data_multi(
        self, 
        start_date: str, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch historical data for all configured Fingrid datasets in a single API call.
        
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
        
        # Collect all dataset IDs
        dataset_ids = list(self._dataset_to_unique_id.keys())
        
        if not dataset_ids:
            logger.warning("No dataset IDs configured for Fingrid Multi plugin")
            return {series_def.unique_id: [] for series_def in self._series_definitions}
        
        # Fetch all datasets in one batched call
        dataset_data = await self.client.fetch_multiple_datasets(
            dataset_ids=dataset_ids,
            start_time=start_date,
            end_time=end_date_str
        )
        
        # Map results back to unique_ids
        result: Dict[str, List[Dict[str, Any]]] = {}
        for series_def in self._series_definitions:
            unique_id = series_def.unique_id
            dataset_id = series_def.extract_filter.get("dataset_id")
            
            if dataset_id is not None:
                result[unique_id] = dataset_data.get(int(dataset_id), [])
            else:
                logger.warning(f"No dataset_id in extract_filter for {unique_id}")
                result[unique_id] = []
        
        total_points = sum(len(v) for v in result.values())
        logger.info(
            f"Fingrid Multi: Completed fetch for {len(result)} series, "
            f"{total_points} total data points"
        )
        
        return result
