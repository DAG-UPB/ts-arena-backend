"""SMARD Data Portal Plugin"""

import asyncio
from datetime import datetime
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata


def _as_utc(value) -> pd.Timestamp:
    """Normalise any timestamp-like value to a tz-aware UTC Timestamp.

    Everything inside this plugin is compared and stored as a UTC *instant*. A naive
    value is assumed to already be UTC, which is what every caller passes.
    """
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


class SmardDataPortal:
    """Client for SMARD API"""
    
    def __init__(self):
        pass

    def get_timestamps(self, filter: str, region: str) -> Optional[List[int]]:
        """Fetch available timestamps from SMARD API"""
        base_url = "https://www.smard.de/app/chart_data/{filter}/{region}/index_hour.json"
        url = base_url.format(filter=filter, region=region)
        response = requests.get(url, timeout=30)
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

        pd_ts = sorted(pd.to_datetime(timestamps, unit="ms", utc=True))
        target = []
        if start_date:
            start_date = _as_utc(start_date)
            after = [ts for ts in pd_ts if ts >= start_date]
            before = [ts for ts in pd_ts if ts < start_date]
            sel = []
            if before:
                sel.append(max(before))
            sel.extend(after)
            target = sorted({int(ts.timestamp()) * 1000 for ts in sel})
        else:
            target = sorted({int(ts.timestamp()) * 1000 for ts in pd_ts})[-1:]

        target = target[-5:]
        all_series = []
        seen = set()
        for ts in target:
            url = self.construct_url(filter, region, resolution, ts)
            resp = requests.get(url, timeout=30)
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
            start_date = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
        else:
            start_date = _as_utc(start_date)
        if end_date is not None:
            end_date = _as_utc(end_date)

        
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
                # SMARD timestamps are epoch milliseconds, i.e. UTC instants. Keep them
                # as instants; only the rendering below is Europe/Berlin. Re-labelling the
                # UTC wall clock as local time shifts every point by the Berlin offset.
                "ts": pd.to_datetime(val[0], unit="ms", utc=True),
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
        
        # Render in Europe/Berlin. The offset is carried explicitly, so both DST
        # transitions are unambiguous and need no ambiguous/nonexistent handling: the
        # instant never changes, only how it is written.
        for entry in history:
            entry["ts"] = entry["ts"].tz_convert("Europe/Berlin").isoformat()

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

    def get_detected_timezone(self) -> Optional[str]:
        return "Europe/Berlin"

    async def get_historical_data(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch historical data from SMARD API.
        
        SMARD API does not require an end_date - it returns data up to the latest available.
        """
        # Normalise to UTC instants; end_date is optional, SMARD returns up to the
        # latest available when it is not given.
        start_dt = _as_utc(start_date) if start_date else None
        end_dt = _as_utc(end_date) if end_date else None
        
        # Run blocking HTTP calls in a thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        processed = await loop.run_in_executor(
            None,
            lambda: self.portal.get_processed_history(
                filter_value=self.filter,
                region=self.region,
                resolution=self.resolution,
                start_date=start_dt,
                end_date=end_dt,
            ),
        )
        
        # Return processed data in standardized format
        return {"data": processed}
