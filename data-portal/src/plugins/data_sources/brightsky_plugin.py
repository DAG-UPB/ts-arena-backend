"""Bright Sky plugin: DWD station weather, observations or MOSMIX forecasts.

Source: Deutscher Wetterdienst (DWD), served by Bright Sky (https://brightsky.dev).
`kind: observation` stores measured values up to now. `kind: forecast` stores the latest
forecast from now to `horizon_hours`; past forecasts are not available, so the history of
those series is what this plugin has archived.
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
KINDS = ("forecast", "observation")


def station_points(
    payload: Dict[str, Any], fields: List[str], kind: str = "forecast"
) -> Dict[str, List[Dict[str, Any]]]:
    """Per field, the points of all records of one kind in a /weather response.

    A field that Bright Sky filled from another source (another station or kind) is dropped.
    """
    wanted_sources = {
        s["id"]
        for s in payload.get("sources", [])
        if (s.get("observation_type") == "forecast") == (kind == "forecast")
    }
    points: Dict[str, List[Dict[str, Any]]] = {f: [] for f in fields}
    for record in payload.get("weather", []):
        if record.get("source_id") not in wanted_sources:
            continue
        fallbacks = record.get("fallback_source_ids") or {}
        for f in fields:
            value = record.get(f)
            if value is not None and f not in fallbacks:
                points[f].append({"ts": record["timestamp"], "value": float(value)})
    return points


def _as_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


class BrightSkyPlugin(MultiSeriesPlugin):
    """One request per station; each series is one field of one station."""

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)
        self.kind = request_params.get("kind", "forecast")
        if self.kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {self.kind!r}")
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
                "units": "dwd",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _window(self, now: datetime, start_date: Optional[str]) -> tuple[datetime, datetime]:
        hour = now.replace(minute=0, second=0, microsecond=0)
        if self.kind == "forecast":
            return hour, hour + timedelta(hours=self.horizon_hours)
        start = _as_utc(start_date) if start_date else now - timedelta(hours=24)
        return start.replace(minute=0, second=0, microsecond=0), hour

    def _fetch_all(
        self, now: datetime, start_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        start, end = self._window(now, start_date)

        by_station: Dict[str, List[TimeSeriesDefinition]] = defaultdict(list)
        for series in self._series_definitions:
            by_station[str(series.extract_filter["wmo_station_id"])].append(series)

        result: Dict[str, List[Dict[str, Any]]] = {}
        for station, series_list in by_station.items():
            fields = [s.extract_filter["field"] for s in series_list]
            try:
                payload = self._fetch_station(station, start, end)
            except Exception as e:
                logger.warning(f"[{self._group_id}] Station {station} failed: {e}")
                continue
            points = station_points(payload, fields, self.kind)
            for series in series_list:
                result[series.unique_id] = points[series.extract_filter["field"]]
        if by_station and not result:
            raise RuntimeError("all stations failed")
        return result

    async def get_historical_data_multi(
        self, start_date: str, end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        # Forecasts ignore start_date: Bright Sky serves only the latest run.
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._fetch_all, datetime.now(timezone.utc), start_date
        )
