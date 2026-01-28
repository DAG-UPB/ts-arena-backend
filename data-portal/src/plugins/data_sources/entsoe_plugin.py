"""ENTSO-E Transparency Platform Plugin"""

from datetime import datetime
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata


class EntsoeApiClient:
    """
    Helper class for communication with ENTSO-E Transparency Platform API.
    """
    BASE_URL = "https://transparency.entsoe.eu/api"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def fetch_timeseries(
        self, 
        document_type: str, 
        process_type: str, 
        period_start: str, 
        period_end: str, 
        **params
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch time series data from ENTSO-E API.
        
        Args:
            document_type: Document type (e.g., "A65" for load, "A44" for generation)
            process_type: Process type (e.g., "A16" for day-ahead)
            period_start: Start time in format YYYYMMDDHHMM
            period_end: End time in format YYYYMMDDHHMM
            params: Additional optional parameters (e.g., "outBiddingZone_Domain")
            
        Returns:
            List of dicts with 'timestamp' and 'value'.
        """
        url = f"{self.BASE_URL}?securityToken={self.api_key}"
        payload = {
            "documentType": document_type,
            "processType": process_type,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
        payload.update(params)
        response = requests.get(url, params=payload)
        if response.status_code != 200:
            return None
        
        # ENTSO-E API returns XML, so we need xmltodict
        try:
            import xmltodict
        except ImportError:
            raise ImportError("xmltodict package is required for ENTSO-E API parsing.")
        
        data = xmltodict.parse(response.text)
        
        # Extract time series points (simplified assumption, may need adjustment based on API response)
        try:
            timeseries = data["Publication_MarketDocument"]["TimeSeries"]
            if not isinstance(timeseries, list):
                timeseries = [timeseries]
            result = []
            for ts in timeseries:
                period = ts["Period"]
                start = period["timeInterval"]["start"]
                resolution = period["resolution"]
                points = period["Point"]
                if not isinstance(points, list):
                    points = [points]
                
                # Calculate timestamp for each point
                start_dt = pd.Timestamp(start)
                for idx, point in enumerate(points):
                    # ENTSO-E: Resolution e.g., PT60M, PT15M
                    if resolution == "PT60M":
                        ts_dt = start_dt + pd.Timedelta(hours=idx)
                    elif resolution == "PT15M":
                        ts_dt = start_dt + pd.Timedelta(minutes=15*idx)
                    else:
                        ts_dt = start_dt + pd.Timedelta(minutes=idx)  # fallback
                    value = float(point["quantity"])
                    result.append({
                        "ts": ts_dt.isoformat(),
                        "value": value
                    })
            return result
        except Exception:
            return None


class EntsoeDataSourcePlugin(BasePlugin):
    """
    Plugin for historical time series data from ENTSO-E
    """

    def __init__(self, metadata: TimeSeriesMetadata, default_params: Dict[str, Any]):
        super().__init__(metadata, default_params)
        self.api_key = self._defaults.get("api_key", "")
        self.document_type = self._defaults.get("document_type", "A65")  # e.g., load
        self.process_type = self._defaults.get("process_type", "A16")    # e.g., day-ahead
        self.process_type = self._defaults.get("process_type", "A16")    # e.g., day-ahead
        self.domain = self._defaults.get("outBiddingZone_Domain", "10Y1001A1001A83F")  # DE
        self.client = EntsoeApiClient(self.api_key)
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
        Fetch historical data from ENTSO-E API.
        
        Note: ENTSO-E API requires an end_date. If not provided, 
        current time will be used as fallback.
        """
        # ENTSO-E expects time in format YYYYMMDDHHMM
        start_dt = pd.Timestamp(start_date)
        # ENTSO-E requires end_date, fallback to current time if not provided
        end_dt = pd.Timestamp(end_date) if end_date else pd.Timestamp.now(tz='UTC')
        period_start = start_dt.strftime("%Y%m%d%H%M")
        period_end = end_dt.strftime("%Y%m%d%H%M")
        
        # Fetch time series
        data = self.client.fetch_timeseries(
            document_type=self.document_type,
            process_type=self.process_type,
            period_start=period_start,
            period_end=period_end,
            outBiddingZone_Domain=self.domain
        )
        
        # Detect timezone from first data point
        if data and len(data) > 0 and "ts" in data[0]:
            try:
                ts_str = data[0]["ts"]
                ts = pd.Timestamp(ts_str)
                if ts.tz:
                    self.detected_timezone = str(ts.tz)
            except Exception:
                pass
        
        if data is None:
            return {"data": [], "error": "Failed to fetch or parse ENTSO-E data."}
        
        # Optional: Filter by metrics (if supported)
        if metrics:
            # ENTSO-E usually returns only one metric per request
            # Here you could filter by 'quantity' or other fields if available
            pass
        
        return {"data": data}
