"""GridStatus Data Portal Plugin

Multi-Series plugin for fetching data from US/Canadian electricity grid ISOs.
Uses MultiSeriesPlugin pattern for efficient batch data fetching.

Supported ISOs: CAISO, MISO, NYISO, PJM, ISONE, SPP, IESO
Supported datasets: load, fuel_mix, lmp (availability varies by ISO)

One API call returns multiple time series (e.g., fuel_mix returns Solar, Wind, Nuclear, etc.)
which are then extracted into separate time series based on extract_filter configuration.

YAML Configuration Example (add to sources.yaml under 'request_groups'):
```yaml
request_groups:
  gridstatus-caiso-fuel-mix:
    module: src.plugins.data_sources.gridstatus_plugin
    class: GridStatusMultiSeriesPlugin
    schedule: 5 minutes
    request_params:
      iso: CAISO
      dataset: fuel_mix
    timeseries:
      - unique_id: gridstatus-caiso-solar
        extract_filter:
          value_column: Solar
        metadata:
          name: GRIDSTATUS-CAISO-Solar-5min
          description: 'California ISO solar generation'
          frequency: 5 minutes
          unit: MW
          domain: energy
          category: generation
      - unique_id: gridstatus-caiso-wind
        extract_filter:
          value_column: Wind
        metadata:
          name: GRIDSTATUS-CAISO-Wind-5min
          description: 'California ISO wind generation'
          frequency: 5 minutes
          unit: MW
          domain: energy
          category: generation
```
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import gridstatus

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)


class GridStatusApiClient:
    """Helper client for gridstatus with async support and rate limiting."""
    
    _lock = asyncio.Lock()
    last_call_time = 0.0
    
    ISO_CLASSES = {
        "CAISO": gridstatus.CAISO,
        "MISO": gridstatus.MISO,
        "NYISO": gridstatus.NYISO,
        "PJM": gridstatus.PJM,
        "ISONE": gridstatus.ISONE,
        "SPP": gridstatus.SPP,
        "IESO": gridstatus.IESO,
    }
    
    def __init__(self, iso_name: str, api_key: Optional[str] = None):
        self.iso_name = iso_name
        iso_class = self.ISO_CLASSES.get(iso_name)
        if not iso_class:
            raise ValueError(f"Unsupported ISO: {iso_name}. Supported: {list(self.ISO_CLASSES.keys())}")
        
        # Initialize ISO client, passing api_key if provided and supported (e.g. for PJM)
        if iso_name == "PJM" and api_key:
             self.iso = iso_class(api_key=api_key)
        else:
             self.iso = iso_class()
    
    async def _wait_for_rate_limit(self):
        """Simple async rate limiter to avoid overwhelming the APIs."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = 1.0 - (now - GridStatusApiClient.last_call_time)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            GridStatusApiClient.last_call_time = asyncio.get_event_loop().time()
    
    async def fetch_dataframe(
        self, 
        dataset: str, 
        start_time: str, 
        end_time: str, 
        market: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch raw DataFrame from the ISO.
        
        Args:
            dataset: Dataset type ('load', 'fuel_mix', 'lmp')
            start_time: Start time ISO string
            end_time: End time ISO string
            market: Optional market type for LMP (default: REAL_TIME_5_MIN)
        """
        await self._wait_for_rate_limit()
        
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time) if end_time else None
        
        try:
            loop = asyncio.get_event_loop()
            
            if dataset == "load":
                if self.iso_name == "MISO":
                    # MISO requires day-by-day fetching
                    df = await self._fetch_miso_daily(self.iso.get_load, start_dt, end_dt, loop)
                else:
                    df = await loop.run_in_executor(
                        None, 
                        lambda: self.iso.get_load(start=start_dt, end=end_dt)
                    )
            elif dataset == "fuel_mix":
                if self.iso_name == "MISO":
                    # MISO requires day-by-day fetching
                    df = await self._fetch_miso_daily(self.iso.get_fuel_mix, start_dt, end_dt, loop)
                else:
                    df = await loop.run_in_executor(
                        None, 
                        lambda: self.iso.get_fuel_mix(start=start_dt, end=end_dt)
                    )
            elif dataset == "lmp":
                lmp_market = market or "REAL_TIME_5_MIN"
                df = await loop.run_in_executor(
                    None, 
                    lambda: self.iso.get_lmp(start=start_dt, end=end_dt, market=lmp_market)
                )
            else:
                logger.error(f"Unknown dataset: {dataset}")
                return None
            
            return df


        except Exception as e:
            logger.error(f"Error fetching {dataset} from {self.iso_name}: {e}")
            return None

    async def _fetch_miso_daily(self, method, start_dt, end_dt, loop):
        """Helper to fetch MISO data day by day."""
        # Normalize to dates
        start_date = start_dt.normalize()
        end_date = end_dt.normalize() if end_dt else pd.Timestamp.now().normalize()
        
        # MISO fetch includes the end date day, so we need to be careful with ranges?
        # pd.date_range includes end by default.
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        tasks = []
        for date in date_range:
            # Capture date in lambda default arg
            tasks.append(
                loop.run_in_executor(None, lambda d=date: method(date=d))
            )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        dfs = []
        for r in results:
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)
            elif isinstance(r, Exception):
                logger.warning(f"Error fetching MISO data for a day: {r}")
        
        if not dfs:
            return None
            
        return pd.concat(dfs).sort_index() if not dfs[0].empty else None


class GridStatusMultiSeriesPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for gridstatus data.
    
    Fetches data once per request group and extracts multiple time series
    from the response based on extract_filter configuration.
    
    Configure via request_groups in sources.yaml:
        request_params:
          - iso: ISO name (CAISO, MISO, NYISO)
          - dataset: Dataset type (load, fuel_mix, lmp)
          - market: (Optional) Market type for LMP (default: REAL_TIME_5_MIN)
        
        extract_filter (per timeseries):
          - value_column: Column name to extract (e.g., 'Solar', 'Wind', 'Load')
    """
    
    def __init__(
        self, 
        group_id: str,
        request_params: Dict[str, Any], 
        series_definitions: List[TimeSeriesDefinition],
        schedule: str
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)
        
        self.iso_name = request_params.get("iso", "CAISO")
        self.dataset = request_params.get("dataset", "load")
        self.market = request_params.get("market", None)
        self.api_key = request_params.get("api_key", None)
        self.detected_timezones: Dict[str, str] = {}
        
        try:
            self.client = GridStatusApiClient(self.iso_name, api_key=self.api_key)
        except ValueError as e:
            logger.error(f"Failed to initialize GridStatus client: {e}")
            logger.error(f"Failed to initialize GridStatus client: {e}")
            self.client = None
    
    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return self.detected_timezones.get(unique_id)
    
    async def get_historical_data_multi(
        self, 
        start_date: str, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch data for ALL time series in this group with a single API call.
        
        Returns:
            Dict mapping unique_id -> list of data points
        """
        result: Dict[str, List[Dict[str, Any]]] = {}
        
        # Initialize empty results for all series
        for series_def in self._series_definitions:
            result[series_def.unique_id] = []
        
        if not self.client:
            logger.error("GridStatus client not initialized")
            return result
        
        # Ensure end_date is a string
        end_date_str: str = end_date if end_date else (
            pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=1)
        ).isoformat()
        
        try:
            # Single API call for all series
            df = await self.client.fetch_dataframe(
                self.dataset, 
                start_date, 
                end_date_str, 
                self.market
            )
            
            if df is None or df.empty:
                logger.warning(f"No data returned for {self._group_id}")
                return result
            
            # Determine timestamp column
            if 'Interval End' in df.columns:
                ts_col = 'Interval End'
            elif 'Time' in df.columns:
                ts_col = 'Time'
            elif 'Interval Start' in df.columns:
                ts_col = 'Interval Start'
            elif 'Interval Start' in df.columns:
                ts_col = 'Interval Start'
            else:
                ts_col = df.columns[0]
            
            # Detect timezone from the timestamp column
            try:
                # Check if column is timezone-aware
                if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
                    # If it's already datetime, accessing .dt.tz should work if it's aware
                    # However, read_csv or similar might not preserve it unless parse_dates used
                    # But gridstatus returns live objects usually.
                    
                    # If it's not datetime, we might need to convert?
                    # But let's assume it returned valid types.
                    
                    # Note: df[ts_col].dt.tz returns the timezone object or None
                    tz = getattr(df[ts_col].dt, 'tz', None)
                    if tz:
                        tz_str = str(tz)
                        # Map pytz/dateutil to string
                        for series_def in self._series_definitions:
                            self.detected_timezones[series_def.unique_id] = tz_str
            except Exception as e:
                logger.warning(f"Failed to detect timezone for {self._group_id}: {e}")
            
            # Extract data for each series based on extract_filter
            for series_def in self._series_definitions:
                value_column = series_def.extract_filter.get("value_column")
                
                if not value_column:
                    logger.error(
                        f"No value_column specified in extract_filter for {series_def.unique_id}"
                    )
                    continue
                
                if value_column not in df.columns:
                    logger.error(
                        f"Column '{value_column}' not found for {series_def.unique_id}. "
                        f"Available: {list(df.columns)}"
                    )
                    continue
                
                # Extract data points
                data_points = []
                for _, row in df.iterrows():
                    ts = row[ts_col]
                    if pd.notna(ts):
                        val = row[value_column]
                        data_points.append({
                            "ts": pd.to_datetime(ts).isoformat(),
                            "value": float(val) if pd.notna(val) else None
                        })
                
                result[series_def.unique_id] = data_points
                logger.info(
                    f"Extracted {len(data_points)} points for {series_def.unique_id}"
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching data for {self._group_id}: {e}")
            return result
