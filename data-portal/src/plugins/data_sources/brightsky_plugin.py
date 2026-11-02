"""Bright Sky plugin: DWD MOSMIX weather forecasts per station.

Source: Deutscher Wetterdienst (DWD), served by Bright Sky (https://brightsky.dev).
Only forecast records are kept; past forecasts are not available, so the history of
these series is what this plugin has archived.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.brightsky.dev"
DEFAULT_HORIZON_HOURS = 48


def forecast_points(payload: Dict[str, Any], fields: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Per field, the points of all forecast records in a /weather response."""
    forecast_sources = {
        s["id"] for s in payload.get("sources", []) if s.get("observation_type") == "forecast"
    }
    points: Dict[str, List[Dict[str, Any]]] = {f: [] for f in fields}
    for record in payload.get("weather", []):
        if record.get("source_id") not in forecast_sources:
            continue
        for f in fields:
            value = record.get(f)
            if value is not None:
                points[f].append({"ts": record["timestamp"], "value": float(value)})
    return points


class BrightSkyForecastPlugin(MultiSeriesPlugin):
    """One request per station; each series is one field of one station."""

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)
        self.base_url = request_params.get("base_url", DEFAULT_BASE_URL).rstrip("/")
        self.horizon_hours = int(request_params.get("horizon_hours", DEFAULT_HORIZON_HOURS))
        self.session = requests.Session()

    def _fetch_station(self, wmo_station_id: str, start: datetime, end: datetime) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/weather",
            params={
                "wmo_station_id": wmo_station_id,
                "date": start.isoformat(),
                "last_date": end.isoformat(),
                "tz": "UTC",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _fetch_all(self, now: datetime) -> Dict[str, List[Dict[str, Any]]]:
        start = now.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=self.horizon_hours)

        by_station: Dict[str, List[TimeSeriesDefinition]] = defaultdict(list)
        for series in self._series_definitions:
            by_station[str(series.extract_filter["wmo_station_id"])].append(series)

        result: Dict[str, List[Dict[str, Any]]] = {}
        for station, series_list in by_station.items():
            fields = [s.extract_filter["field"] for s in series_list]
            try:
                points = forecast_points(self._fetch_station(station, start, end), fields)
            except Exception as e:
                logger.warning(f"[{self._group_id}] Station {station} failed: {e}")
                continue
            for series in series_list:
                result[series.unique_id] = points[series.extract_filter["field"]]
        if by_station and not result:
            raise RuntimeError("all stations failed")
        return result

    async def get_historical_data_multi(
        self, start_date: str, end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        # start_date is ignored: Bright Sky serves only the latest forecast run.
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_all, datetime.now(timezone.utc))
