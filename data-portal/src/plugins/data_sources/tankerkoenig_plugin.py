"""Tankerkoenig Multi-Series Plugin - German retail fuel prices at 5-min resolution

Fetches current station prices from the Tankerkoenig list.php API across a grid
of coordinates covering Germany, deduplicates stations, and computes national
and regional (city-level) averages for E5, E10, and Diesel every 5 minutes.

Historical backfill uses the Tankerkoenig daily price CSV archive:
  https://creativecommons.tankerkoenig.de/history/prices/YYYY/MM/YYYY-MM-DD-prices.csv
Each CSV contains per-station price change events. The plugin takes the last
known price per station per day and computes daily averages.
"""

import asyncio
import io
import logging
import math
import os
from typing import Dict, Any, List, Optional, Set, Tuple

import pandas as pd
import requests

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

LIST_URL = "https://creativecommons.tankerkoenig.de/json/list.php"
HISTORY_CSV_URL = (
    "https://creativecommons.tankerkoenig.de/history/prices"
    "/{year}/{month:02d}/{year}-{month:02d}-{day:02d}-prices.csv"
)
FUEL_COLUMNS = ["e5", "e10", "diesel"]

# Grid of coordinates covering Germany (25 km radius per point).
# Major cities + fill points for even geographic coverage.
# Germany bounding box: ~47.3-55.1°N, ~5.9-15.0°E
GERMANY_GRID = [
    # Northern Germany
    (54.8, 9.5),   # Flensburg
    (54.3, 13.1),  # Stralsund
    (53.9, 10.7),  # Luebeck
    (53.55, 10.0), # Hamburg
    (53.5, 8.1),   # Bremerhaven
    (53.1, 8.8),   # Bremen
    (53.1, 12.0),  # Pritzwalk
    (52.7, 7.3),   # Meppen
    # Central-North
    (52.5, 13.4),  # Berlin
    (52.4, 9.7),   # Hanover
    (52.3, 11.6),  # Magdeburg
    (52.0, 8.5),   # Bielefeld
    (51.9, 7.6),   # Muenster
    (51.7, 14.3),  # Cottbus
    # Central
    (51.5, 7.0),   # Essen/Ruhr
    (51.3, 9.5),   # Kassel
    (51.3, 12.4),  # Leipzig
    (51.05, 13.7), # Dresden
    (50.9, 7.0),   # Cologne
    (50.7, 11.0),  # Jena
    (50.6, 8.7),   # Giessen
    # Central-South
    (50.1, 8.7),   # Frankfurt
    (50.0, 12.1),  # Hof
    (49.9, 6.9),   # Trier
    (49.5, 11.1),  # Nuremberg
    (49.5, 8.5),   # Mannheim
    (49.2, 7.0),   # Saarbruecken
    # Southern Germany
    (48.8, 9.2),   # Stuttgart
    (48.8, 13.0),  # Passau area
    (48.4, 10.9),  # Augsburg
    (48.1, 11.6),  # Munich
    (48.0, 7.8),   # Freiburg
    (47.7, 10.3),  # Kempten
    (47.6, 9.5),   # Lindau/Konstanz
]

# City regions: (lat, lng, radius_km)
CITY_REGIONS: Dict[str, Tuple[float, float, float]] = {
    "hamburg":   (53.55, 10.0, 30),
    "berlin":    (52.52, 13.41, 30),
    "munich":    (48.14, 11.58, 30),
    "paderborn": (51.72, 8.75, 25),
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class TankerkoenigPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for German retail fuel prices (Tankerkoenig/MTS-K).

    Queries the list.php endpoint across a grid of ~35 coordinates covering
    Germany, deduplicates stations by ID, and computes national and regional
    average prices for each fuel type.

    For historical backfill, downloads daily price CSVs from the Tankerkoenig
    archive and computes daily averages per region.
    """

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)

        self.api_key = os.getenv("API_KEY_TANKERKOENIG", "")
        if not self.api_key:
            logger.error("API_KEY_TANKERKOENIG not set — plugin will not work")

        self.session = requests.Session()

        # Build mapping: (region, fuel_type) → unique_id
        self._filter_to_unique_id: Dict[Tuple[str, str], str] = {}
        for series_def in self._series_definitions:
            fuel_type = series_def.extract_filter.get("fuel_type")
            region = series_def.extract_filter.get("region", "national")
            if fuel_type:
                self._filter_to_unique_id[(region, fuel_type)] = series_def.unique_id

        # Cached station coordinates: station_uuid → (lat, lng)
        self._station_coords: Dict[str, Tuple[float, float]] = {}

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "Europe/Berlin"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in self._filter_to_unique_id.values()
        }

        if not self.api_key:
            logger.error("Tankerkoenig: No API key configured, skipping fetch")
            return result

        today = pd.Timestamp.now(tz="Europe/Berlin").normalize()
        start = pd.Timestamp(start_date, tz="Europe/Berlin")
        end = pd.Timestamp(end_date, tz="Europe/Berlin") if end_date else today

        # Historical backfill: download daily CSVs for past days
        has_city_series = any(
            r != "national" for r, _ in self._filter_to_unique_id
        )
        if start.normalize() < today:
            if has_city_series:
                await self._load_station_coords()
            await self._backfill_from_csv(result, start, min(end, today - pd.Timedelta(days=1)))

        # Live fetch for today (current prices via list.php)
        if end.normalize() >= today:
            await self._fetch_live_prices(result)

        return result

    async def _fetch_live_prices(
        self, result: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Fetch current prices via list.php and compute averages."""
        stations = await self._fetch_all_stations()
        if not stations:
            logger.warning("Tankerkoenig: No stations collected for live prices")
            return

        ts_str = pd.Timestamp.now(tz="Europe/Berlin").floor("5min").isoformat()

        # National averages
        for fuel in FUEL_COLUMNS:
            prices = [
                s[fuel] for s in stations.values()
                if s.get(fuel) is not None and 0 < s[fuel] < 5
            ]
            if not prices:
                continue

            avg = round(sum(prices) / len(prices), 4)
            uid = self._filter_to_unique_id.get(("national", fuel))
            if uid:
                result[uid].append({"ts": ts_str, "value": avg})
                logger.info(
                    f"Tankerkoenig live national {fuel}: {avg:.4f} EUR/L "
                    f"(from {len(prices)} stations)"
                )

        # City-level averages
        for city, (clat, clng, radius) in CITY_REGIONS.items():
            city_stations = [
                s for s in stations.values()
                if s.get("lat") is not None and s.get("lng") is not None
                and _haversine_km(clat, clng, s["lat"], s["lng"]) <= radius
            ]

            for fuel in FUEL_COLUMNS:
                prices = [
                    s[fuel] for s in city_stations
                    if s.get(fuel) is not None and 0 < s[fuel] < 5
                ]
                if not prices:
                    continue

                avg = round(sum(prices) / len(prices), 4)
                uid = self._filter_to_unique_id.get((city, fuel))
                if uid:
                    result[uid].append({"ts": ts_str, "value": avg})
                    logger.info(
                        f"Tankerkoenig live {city} {fuel}: {avg:.4f} EUR/L "
                        f"(from {len(prices)} stations)"
                    )

    async def _load_station_coords(self) -> None:
        """Load station coordinates from list.php (needed for city-level CSV filtering)."""
        if self._station_coords:
            return  # already cached

        stations = await self._fetch_all_stations()
        for sid, s in stations.items():
            lat, lng = s.get("lat"), s.get("lng")
            if lat is not None and lng is not None:
                self._station_coords[sid] = (lat, lng)

        logger.info(
            f"Tankerkoenig: Cached coordinates for {len(self._station_coords)} stations"
        )

    async def _backfill_from_csv(
        self,
        result: Dict[str, List[Dict[str, Any]]],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        """Download daily price CSVs and compute averages for each day."""
        loop = asyncio.get_event_loop()

        # Pre-compute city station sets for filtering
        city_station_sets: Dict[str, Set[str]] = {}
        for city, (clat, clng, radius) in CITY_REGIONS.items():
            city_station_sets[city] = {
                sid for sid, (slat, slng) in self._station_coords.items()
                if _haversine_km(clat, clng, slat, slng) <= radius
            }

        days = pd.date_range(start.normalize(), end.normalize(), freq="D")
        logger.info(
            f"Tankerkoenig CSV backfill: {len(days)} days "
            f"({start.date()} to {end.date()})"
        )

        for day in days:
            url = HISTORY_CSV_URL.format(
                year=day.year, month=day.month, day=day.day
            )

            try:
                response = await loop.run_in_executor(
                    None,
                    lambda url=url: self.session.get(
                        url,
                        params={"apikey": self.api_key},
                        timeout=60,
                    ),
                )
            except requests.RequestException as exc:
                logger.warning(f"Tankerkoenig CSV: Failed for {day.date()}: {exc}")
                continue

            if response.status_code == 404:
                logger.debug(f"Tankerkoenig CSV: No data for {day.date()} (404)")
                continue
            if response.status_code != 200:
                logger.warning(
                    f"Tankerkoenig CSV: HTTP {response.status_code} for {day.date()}"
                )
                continue

            try:
                df = pd.read_csv(
                    io.StringIO(response.text),
                    dtype={"station_uuid": str},
                )
            except Exception as exc:
                logger.warning(f"Tankerkoenig CSV: Parse error for {day.date()}: {exc}")
                continue

            if df.empty:
                continue

            # Keep last price per station (CSV is chronologically ordered)
            df_last = df.groupby("station_uuid").last().reset_index()

            # Timestamp at noon Berlin time for the day
            ts_str = day.tz_localize("Europe/Berlin").replace(hour=12).isoformat()

            # National averages
            for fuel in FUEL_COLUMNS:
                if fuel not in df_last.columns:
                    continue
                prices = pd.to_numeric(df_last[fuel], errors="coerce")
                valid = prices[(prices > 0) & (prices < 5)].dropna()
                if valid.empty:
                    continue

                avg = round(float(valid.mean()), 4)
                uid = self._filter_to_unique_id.get(("national", fuel))
                if uid:
                    result[uid].append({"ts": ts_str, "value": avg})

            # City-level averages
            for city, station_ids in city_station_sets.items():
                if not station_ids:
                    continue
                df_city = df_last[df_last["station_uuid"].isin(station_ids)]
                if df_city.empty:
                    continue

                for fuel in FUEL_COLUMNS:
                    if fuel not in df_city.columns:
                        continue
                    prices = pd.to_numeric(df_city[fuel], errors="coerce")
                    valid = prices[(prices > 0) & (prices < 5)].dropna()
                    if valid.empty:
                        continue

                    avg = round(float(valid.mean()), 4)
                    uid = self._filter_to_unique_id.get((city, fuel))
                    if uid:
                        result[uid].append({"ts": ts_str, "value": avg})

            logger.info(
                f"Tankerkoenig CSV: {day.date()} — "
                f"{len(df_last)} stations processed"
            )

            # Small delay between CSV downloads
            await asyncio.sleep(0.2)

        total = sum(len(v) for v in result.values())
        logger.info(f"Tankerkoenig CSV backfill complete: {total} data points")

    async def _fetch_all_stations(self) -> Dict[str, Dict[str, Any]]:
        """Query list.php across the Germany grid and deduplicate stations."""
        all_stations: Dict[str, Dict[str, Any]] = {}
        loop = asyncio.get_event_loop()

        for lat, lng in GERMANY_GRID:
            try:
                response = await loop.run_in_executor(
                    None,
                    lambda lat=lat, lng=lng: self.session.get(
                        LIST_URL,
                        params={
                            "lat": lat,
                            "lng": lng,
                            "rad": 25,
                            "sort": "dist",
                            "type": "all",
                            "apikey": self.api_key,
                        },
                        timeout=30,
                    ),
                )
            except requests.RequestException as exc:
                logger.warning(f"Tankerkoenig: Request failed for ({lat},{lng}): {exc}")
                continue

            if response.status_code != 200:
                logger.warning(
                    f"Tankerkoenig: HTTP {response.status_code} for ({lat},{lng})"
                )
                continue

            data = response.json()
            if not data.get("ok"):
                logger.warning(
                    f"Tankerkoenig: API error for ({lat},{lng}): "
                    f"{data.get('message', 'unknown')}"
                )
                continue

            for station in data.get("stations", []):
                sid = station.get("id")
                if sid and sid not in all_stations:
                    all_stations[sid] = station

            # Rate-limit courtesy: 500ms between requests to avoid 503s
            await asyncio.sleep(0.5)

        logger.info(
            f"Tankerkoenig: Collected {len(all_stations)} unique stations "
            f"from {len(GERMANY_GRID)} grid points"
        )
        return all_stations
