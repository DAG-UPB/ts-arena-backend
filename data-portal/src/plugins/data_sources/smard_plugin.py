"""SMARD Data Portal Plugin"""

from datetime import datetime
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata


class SmardDataPortal:
    """Client for SMARD API"""
    
    def __init__(self):
        pass

    def get_timestamps(self, filter: str, region: str) -> Optional[List[int]]:
        """Fetch available timestamps from SMARD API"""
        base_url = "https://www.smard.de/app/chart_data/{filter}/{region}/index_hour.json"
        url = base_url.format(filter=filter, region=region)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("timestamps")
        return None

    def construct_url(self, filter: str, region: str, resolution: str, timestamp: int) -> str:
        """Construct URL for data download"""
        base_url = (
            "https://www.smard.de/app/chart_data/{filter}/{region}/"
            "{filterCopy}_{regionCopy}_{resolution}_{timestamp}.json"
        )
        return base_url.format(
            filter=filter,
            region=region,
            filterCopy=filter,
            regionCopy=region,
            resolution=resolution,
            timestamp=timestamp,
        )

    def download_historical_data(
        self,
        filter: str = "410",
        region: str = "DE",
        resolution: str = "hour",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Download raw historical data from SMARD API"""
        timestamps = self.get_timestamps(filter, region)
        if timestamps is None:
            return {"error": "Failed to fetch timestamps"}

        pd_ts = sorted(pd.to_datetime(timestamps, unit="ms"))
        target = []
        if start_date:
            if not isinstance(start_date, pd.Timestamp):
                start_date = pd.Timestamp(start_date)
            start_date = start_date.tz_localize(
                None) if start_date.tzinfo else start_date
            after = [ts for ts in pd_ts if ts >= start_date]
            before = [ts for ts in pd_ts if ts < start_date]
            sel = []
            if before:
                sel.append(max(before))
            sel.extend(after)
            target = sorted({int(ts.timestamp()) * 1000 for ts in sel})
        else:
            target = sorted({int(ts.timestamp()) * 1000 for ts in pd_ts})[-1:]

        all_series = []
        seen = set()
        for ts in target:
            url = self.construct_url(filter, region, resolution, ts)
            resp = requests.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for point in data.get("series", []):
                    if point[0] not in seen:
                        all_series.append(point)
                        seen.add(point[0])
        all_series.sort(key=lambda x: x[0])
        return {"series": all_series}

    def get_processed_history(
        self, 
        filter_value: str = "410", 
        region: str = "DE", 
        resolution: str = "hour", 
        start_date: Optional[datetime] = None, 
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Load and process complete history as list of dicts with 'timestamp' and 'value'.
        All filtering, conversion and cleaning happens here.
        """
        if start_date is None:
            start_date = pd.Timestamp.now() - pd.Timedelta(days=7)
        else:
            # start date is always in utc, we convert to Europe/Berlin timezone and remove timezone info
            start_date = pd.Timestamp(start_date).tz_localize("UTC").tz_convert(
                "Europe/Berlin").tz_localize(None) if isinstance(start_date, pd.Timestamp) else pd.Timestamp(start_date)
        
        raw_data = self.download_historical_data(
            filter=filter_value,
            region=region,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date
        )
        if "error" in raw_data or not raw_data.get("series"):
            return []
        
        values = raw_data["series"]
        history = [
            {
                "ts": pd.to_datetime(val[0], unit="ms"),
                "value": val[1]
            }
            for val in values
        ]
        
        # Filter None values and sort by timestamp
        history = [entry for entry in history if entry["value"] is not None]
        history.sort(key=lambda x: x["ts"])

        # Optional: Filter by start and end date
        if start_date is not None:
            history = [
                entry for entry in history if entry["ts"] >= start_date]
        if end_date is not None:
            history = [
                entry for entry in history if entry["ts"] <= end_date]
        
        # Convert timestamps to ISO format
        # Handle DST transitions: ambiguous times are marked as True for first occurrence (DST)
        # and False for second occurrence (standard time)
        for i, entry in enumerate(history):
            try:
                entry["ts"] = entry["ts"].tz_localize("Europe/Berlin").isoformat()
            except Exception as e:
                # If ambiguous, check if we've seen this timestamp before
                if "AmbiguousTimeError" in str(type(e).__name__):
                    # Check if there's a previous entry with the same naive timestamp
                    is_first_occurrence = True
                    for j in range(i):
                        if history[j]["ts"] == entry["ts"] if isinstance(history[j]["ts"], str) else False:
                            is_first_occurrence = False
                            break
                    # First occurrence: DST (True), Second occurrence: Standard time (False)
                    entry["ts"] = entry["ts"].tz_localize(
                        "Europe/Berlin", ambiguous=is_first_occurrence).isoformat()
                else:
                    raise
        
        return history


class SmardDataSourcePlugin(BasePlugin):
    """Plugin for historical time series data from SMARD"""

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        super().__init__(metadata, default_params)
        self.portal = SmardDataPortal()
        # Get default parameters from config
        self.filter = self._defaults.get("filter", "410")
        self.region = self._defaults.get("region", "DE")
        self.resolution = self._defaults.get("resolution", "hour")

    async def get_historical_data(
        self,
        start_date: str,
        end_date: str,
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Fetch historical data from SMARD API"""
        # Convert date strings to Timestamp objects and remove timezone information
        start_dt = pd.Timestamp(start_date).tz_localize(
            None) if start_date else None
        end_dt = pd.Timestamp(end_date).tz_localize(None) if end_date else None
        
        # Call processed history
        processed = self.portal.get_processed_history(
            filter_value=self.filter,
            region=self.region,
            resolution=self.resolution,
            start_date=start_dt,
            end_date=end_dt,
        )
        
        # Return processed data in standardized format
        return {"data": processed}
