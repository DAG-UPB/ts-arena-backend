"""CityBikes Multi-Series Plugin — Live bike-sharing availability per city

Fetches current station data from the CityBikes API v2 and aggregates
to network (city) level. No historical API exists — data is collected
by polling every 5 minutes.

API documentation: https://api.citybik.es/v2/
One request per network: GET /v2/networks/{network_id}?fields=network.stations

Each station returns:
    free_bikes  — available bikes (all types)
    empty_slots — empty docks
    extra.ebike / extra.normal_bikes — e-bike / normal bike split (if available)

Series selection via extract_filter:
    metric: "free_bikes"   — total available bikes across all stations
    metric: "ebikes"       — total available e-bikes (from extra field)
    metric: "normal_bikes" — total available non-electric bikes (from extra field)

Data license: CityBikes / AGPL-3.0
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

API_BASE = "https://api.citybik.es/v2/networks"


class CityBikesPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for CityBikes bike-sharing data.

    One HTTP GET per network fetches all stations; the plugin sums
    station-level values to produce network-level totals for each
    configured metric.
    """

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)

        self._network_id: str = request_params.get("network_id", "")
        if not self._network_id:
            logger.error(f"[{group_id}] network_id not set in request_params")

        # Map metric name → unique_id
        self._metric_to_uid: Dict[str, str] = {}
        for series_def in self._series_definitions:
            metric = series_def.extract_filter.get("metric", "")
            if metric:
                self._metric_to_uid[metric] = series_def.unique_id

        logger.info(
            f"[{group_id}] CityBikes plugin initialised: "
            f"network={self._network_id}, metrics={list(self._metric_to_uid.keys())}"
        )

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        # CityBikes API returns UTC timestamps; we store in UTC
        return "UTC"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch current bike availability from the CityBikes API.

        No historical data available — always returns the current snapshot.
        The scheduler accumulates these over time.
        """
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in self._metric_to_uid.values()
        }

        if not self._network_id:
            return result

        stations = await self._fetch_stations()
        if stations is None:
            return result

        self._aggregate_and_fill(stations, result)
        return result

    async def _fetch_stations(self) -> Optional[List[Dict[str, Any]]]:
        """Download station data for the configured network."""
        url = f"{API_BASE}/{self._network_id}"
        params = {"fields": "network.stations"}
        timeout = httpx.Timeout(30.0)

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("network", {}).get("stations", [])
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"[{self._group_id}] CityBikes HTTP error "
                f"{exc.response.status_code} for {self._network_id}: {exc}"
            )
        except httpx.RequestError as exc:
            logger.error(
                f"[{self._group_id}] CityBikes request failed "
                f"for {self._network_id}: {exc}"
            )
        return None

    def _aggregate_and_fill(
        self,
        stations: List[Dict[str, Any]],
        result: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        """Sum station-level values to network totals and populate result."""
        ts_str = datetime.now(timezone.utc).isoformat()

        totals: Dict[str, int] = {
            "free_bikes": 0,
            "empty_slots": 0,
            "ebikes": 0,
            "normal_bikes": 0,
        }

        for station in stations:
            totals["free_bikes"] += station.get("free_bikes", 0) or 0
            totals["empty_slots"] += station.get("empty_slots", 0) or 0
            extra = station.get("extra") or {}
            totals["ebikes"] += (
                extra.get("ebike", 0) or extra.get("ebikes", 0) or 0
            )
            totals["normal_bikes"] += extra.get("normal_bikes", 0) or 0

        for metric, uid in self._metric_to_uid.items():
            value = totals.get(metric)
            if value is not None:
                result[uid].append({"ts": ts_str, "value": float(value)})

        logger.info(
            f"[{self._group_id}] {self._network_id}: "
            f"{len(stations)} stations, "
            f"free_bikes={totals['free_bikes']}, "
            f"ebikes={totals['ebikes']}, "
            f"empty_slots={totals['empty_slots']}"
        )
