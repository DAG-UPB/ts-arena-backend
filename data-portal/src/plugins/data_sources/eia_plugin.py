"""EIA (U.S. Energy Information Administration) Data Plugin"""

import requests
import pandas as pd
import os
from typing import Dict, Any, List, Optional
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata


class EIADataPortal:
    """Client for EIA API"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.eia.gov/v2/electricity/rto/"):
        self.api_key = api_key
        self.base_url = base_url

    def query_data(
        self,
        frequency: str,
        start: str,
        facet_args: Dict[str, List[str]],
        end: Optional[str] = None,
        sub_id: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Query time series data from EIA API with pagination support.
        """
        endpoint = f"{self.base_url}{sub_id}/data/"

        # Build query params according to swagger spec
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "start": start,
            "data[]": "value",   # request the "value" field
            "length": 5000,      # API default max page size
            "offset": 0,
        }
        
        # Only add end parameter if provided
        if end:
            params["end"] = end

        # Add facets (facets[<id>][])
        for key, values in facet_args.items():
            for value in values:
                params[f"facets[{key}][]"] = value

        rows = []
        while True:
            response = requests.get(endpoint, params=params)
            if response.status_code != 200:
                print(f"❌ Request failed: {response.status_code} for {response.url}")
                break

            data = response.json()
            response_data = data.get("response", {}).get("data", [])
            rows.extend(response_data)

            total = data.get("response", {}).get("total", len(rows))
            # Ensure total is integer
            total = int(total) if total is not None else len(rows)
            if params["offset"] + params["length"] >= total:
                break

            params["offset"] += params["length"]

        return rows

    def get_processed_history(
        self,
        frequency: str,
        start_date: str,
        facet_args: Dict[str, List[str]],
        end_date: Optional[str] = None,
        sub_id: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Standardize output to: [{'ts': ..., 'value': ...}]
        """
        raw_data = self.query_data(frequency, start_date, facet_args=facet_args, end=end_date, sub_id=sub_id)

        history = []
        for row in raw_data:
            ts = row.get("period")
            val = row.get("value")
            if ts and val is not None:
                history.append({"ts": pd.to_datetime(ts).isoformat(), "value": val})

        # cleanup
        history = [entry for entry in history if entry["value"] is not None]
        history.sort(key=lambda x: x["ts"])
        return history


class EIADataSourcePlugin(BasePlugin):
    """Plugin for historical time series from EIA (Electricity Data)"""

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        super().__init__(metadata, default_params)

        api_key = os.getenv("API_KEY_SOURCE_EIA")
        if not api_key:
            raise ValueError("Environment variable API_KEY_SOURCE_EIA is not set!")

        self.portal = EIADataPortal(api_key=api_key)

        # Default parameters from config
        self.frequency = self._defaults.get("frequency", "hourly")
        self.facet_args = self._defaults.get("facet_args", {})
        self.sub_id = self._defaults.get("sub_id", "")
        self.detected_timezone: Optional[str] = None

    def get_detected_timezone(self) -> Optional[str]:
        return self.detected_timezone

    async def get_historical_data(
        self, 
        start_date: str, 
        end_date: Optional[str] = None, 
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch historical data from EIA API.
        
        EIA API does not require an end_date - it returns data up to the latest available.
        """
        # EIA expects format YYYY-MM-DDTHH
        start_dt = pd.Timestamp(start_date)
        # end_date is optional, EIA will return up to latest if not provided
        end_date_str = pd.Timestamp(end_date).strftime("%Y-%m-%dT%H") if end_date else None

        processed = self.portal.get_processed_history(
            frequency=self.frequency,
            start_date=start_dt.strftime("%Y-%m-%dT%H"),
            end_date=end_date_str,
            facet_args=self.facet_args,
            sub_id=self.sub_id,
        )

        # Detect timezone from first data point
        if processed and len(processed) > 0 and "ts" in processed[0]:
            try:
                ts_str = processed[0]["ts"]
                ts = pd.Timestamp(ts_str)
                if ts.tz:
                    self.detected_timezone = str(ts.tz)
            except Exception:
                pass

        return {"data": processed}