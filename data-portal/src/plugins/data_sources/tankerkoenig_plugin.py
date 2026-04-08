"""Tankerkoenig Multi-Series Plugin - German retail fuel prices at 10-min resolution

Uses one representative gas station per city as a proxy for local fuel prices.
Station IDs are configured in sources.yaml via extract_filter.station_id.
Ongoing price polling uses prices.php with all station IDs in a single API call.

Per Tankerkoenig terms of use:
- Max 1 request per 5 minutes for automated systems
- Avoid round-time queries (add random offset)
- Use prices.php for batch price queries (up to 10 stations per call)

Historical backfill uses the Tankerkoenig authenticated Git data repository:
  https://data.tankerkoenig.de/tankerkoenig-organization/tankerkoenig-data/
Authentication via Basic Auth (USERNAME_TANKERKOENIG / API_KEY_TANKERKOENIG).

The raw price data is event-based (only entries when a price changes).
We resample to regular 10-minute intervals: if a real price change falls
within a bin, we average the carry-in price with the new price(s);
otherwise we forward-fill the last known price.

Data license: Creative Commons BY-NC-SA 4.0
https://creativecommons.org/licenses/by-nc-sa/4.0/
"""

import asyncio
import io
import logging
import os
import random
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import requests

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

PRICES_URL = "https://creativecommons.tankerkoenig.de/json/prices.php"

# New authenticated Git repository for historical data
HISTORY_CSV_BASE = (
    "https://data.tankerkoenig.de/tankerkoenig-organization/"
    "tankerkoenig-data/raw/branch/master"
)
HISTORY_PRICES_URL = (
    HISTORY_CSV_BASE
    + "/prices/{year}/{month:02d}/{year}-{month:02d}-{day:02d}-prices.csv"
)
HISTORY_STATIONS_URL = (
    HISTORY_CSV_BASE
    + "/stations/{year}/{month:02d}/{year}-{month:02d}-{day:02d}-stations.csv"
)

RESAMPLE_FREQ = "10min"
FUEL_COLUMNS = ["e5", "e10", "diesel"]


class TankerkoenigPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for German retail fuel prices (Tankerkoenig/MTS-K).

    Each city series references a fixed station_id in its extract_filter.
    Every 10 minutes a single prices.php call fetches all station prices.
    National average = mean of the city station prices.
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
        self.username = os.getenv("USERNAME_TANKERKOENIG", "")
        if not self.api_key:
            logger.error("API_KEY_TANKERKOENIG not set — plugin will not work")
        if not self.username:
            logger.warning(
                "USERNAME_TANKERKOENIG not set — historical data download "
                "from authenticated repository will not work"
            )

        self.session = requests.Session()

        # Build mappings from series definitions
        # (region, fuel_type) -> unique_id
        self._filter_to_unique_id: Dict[Tuple[str, str], str] = {}
        # station_id -> city name (for city series that have a station_id)
        self._station_to_city: Dict[str, str] = {}
        # All unique station IDs to query
        self._station_ids: List[str] = []

        seen_stations = set()
        for series_def in self._series_definitions:
            fuel_type = series_def.extract_filter.get("fuel_type")
            region = series_def.extract_filter.get("region", "national")
            station_id = series_def.extract_filter.get("station_id")

            if fuel_type:
                self._filter_to_unique_id[(region, fuel_type)] = series_def.unique_id

            if station_id and station_id not in seen_stations:
                seen_stations.add(station_id)
                self._station_ids.append(station_id)
                if region != "national":
                    self._station_to_city[station_id] = region

        logger.info(
            f"Tankerkoenig: {len(self._station_ids)} stations configured, "
            f"{len(self._filter_to_unique_id)} series"
        )

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "Europe/Berlin"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch historical + live fuel prices.

        Strategy:
        1. **Initial load** (start far in the past): downloads daily CSV
           files for the full requested range. At 10-min resolution,
           ~7 days = 1008 data points which is enough context.
        2. **Daily reconciliation** (scheduled runs): *always* re-downloads
           yesterday's CSV even if start_date is only 24h ago. This
           ensures the event-based CSV data (every price change recorded)
           overwrites the less accurate live-polled snapshots. Since
           the DB uses upsert, duplicate timestamps are harmless.
        3. **Today**: uses the live prices.php API for the current snapshot.
        """
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in self._filter_to_unique_id.values()
        }

        if not self.api_key:
            logger.error("Tankerkoenig: No API key configured, skipping fetch")
            return result

        today = pd.Timestamp.now(tz="Europe/Berlin").normalize()
        yesterday = today - pd.Timedelta(days=1)
        start = pd.Timestamp(start_date, tz="Europe/Berlin")
        end = pd.Timestamp(end_date, tz="Europe/Berlin") if end_date else today

        # Historical backfill via daily CSVs (available up to yesterday).
        # Even for short lookbacks (e.g. scheduled 24h window), we always
        # include yesterday for daily reconciliation of live data.
        csv_start = min(start.normalize(), yesterday)
        if csv_start < today:
            await self._backfill_from_csv(
                result, csv_start, min(end, yesterday)
            )

        # Live fetch for today via prices.php
        if end.normalize() >= today:
            await self._fetch_live_prices(result)

        return result

    async def _fetch_live_prices(
        self, result: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Fetch current prices for all configured stations in one prices.php call."""
        if not self._station_ids:
            logger.warning("Tankerkoenig: No station IDs configured, skipping")
            return

        # Random jitter to avoid round-time queries
        jitter = random.uniform(5, 30)
        logger.info(f"Tankerkoenig: Waiting {jitter:.0f}s jitter before price query")
        await asyncio.sleep(jitter)

        ids_param = ",".join(self._station_ids)
        loop = asyncio.get_event_loop()

        try:
            response = await loop.run_in_executor(
                None,
                lambda: self.session.get(
                    PRICES_URL,
                    params={"ids": ids_param, "apikey": self.api_key},
                    timeout=30,
                ),
            )
        except requests.RequestException as exc:
            logger.error(f"Tankerkoenig: prices.php request failed: {exc}")
            return

        if response.status_code != 200:
            logger.error(f"Tankerkoenig: prices.php HTTP {response.status_code}")
            return

        data = response.json()
        if not data.get("ok"):
            logger.error(f"Tankerkoenig: prices.php error: {data.get('message')}")
            return

        prices_data = data.get("prices", {})
        ts_str = pd.Timestamp.now(tz="Europe/Berlin").floor("10min").isoformat()

        # Collect city prices for national average
        all_fuel_prices: Dict[str, List[float]] = {f: [] for f in FUEL_COLUMNS}

        for sid in self._station_ids:
            station_info = prices_data.get(sid, {})
            city = self._station_to_city.get(sid)

            if station_info.get("status") != "open":
                logger.info(f"Tankerkoenig: Station {sid} ({city or 'extra'}) closed")
                continue

            for fuel in FUEL_COLUMNS:
                price = station_info.get(fuel)
                if price is None or price is False or not (0 < price < 5):
                    continue

                price = round(price, 4)
                all_fuel_prices[fuel].append(price)

                # City-level series
                if city:
                    uid = self._filter_to_unique_id.get((city, fuel))
                    if uid:
                        result[uid].append({"ts": ts_str, "value": price})

            if city:
                logger.info(
                    f"Tankerkoenig live {city}: "
                    f"e5={station_info.get('e5', '-')} "
                    f"e10={station_info.get('e10', '-')} "
                    f"diesel={station_info.get('diesel', '-')}"
                )

        # National average = mean of all station prices
        for fuel in FUEL_COLUMNS:
            prices = all_fuel_prices[fuel]
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

    async def _backfill_from_csv(
        self,
        result: Dict[str, List[Dict[str, Any]]],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        """Download daily price CSVs and resample to 10-min intervals.

        The raw CSV data is event-based: rows are only created when ANY fuel
        price changes for a station.  This means each fuel column contains
        many "echo" rows where its value didn't actually change.

        For each 10-minute bin we compute:
          - If no actual price change falls inside the bin: last known price.
          - If one or more actual price changes fall inside the bin: simple
            (unweighted) average of the carry-in price and the new prices.

        City series: resampled price of the configured station.
        National series: mean of all stations' resampled prices per interval.

        Uses the authenticated Git data repository:
          https://data.tankerkoenig.de/tankerkoenig-organization/tankerkoenig-data/
        """
        loop = asyncio.get_event_loop()

        if not self.username:
            logger.error(
                "Tankerkoenig CSV: USERNAME_TANKERKOENIG not set, "
                "cannot download from authenticated repository"
            )
            return

        days = pd.date_range(start.normalize(), end.normalize(), freq="D")
        logger.info(
            f"Tankerkoenig CSV backfill: {len(days)} days "
            f"({start.date()} to {end.date()})"
        )

        # Track last known price per (station_uuid, fuel) across days
        # to fill the midnight-to-first-event gap via forward-fill.
        carry_over: Dict[str, Dict[str, float]] = {}

        for day in days:
            url = HISTORY_PRICES_URL.format(
                year=day.year, month=day.month, day=day.day
            )

            try:
                response = await loop.run_in_executor(
                    None,
                    lambda url=url: self.session.get(
                        url,
                        auth=(self.username, self.api_key),
                        timeout=120,
                    ),
                )
            except requests.RequestException as exc:
                logger.warning(f"Tankerkoenig CSV: Failed for {day.date()}: {exc}")
                continue

            if response.status_code == 404:
                logger.debug(f"Tankerkoenig CSV: No data for {day.date()} (404)")
                continue
            if response.status_code == 401:
                logger.error(
                    f"Tankerkoenig CSV: Authentication failed (401) for {day.date()}. "
                    "Check USERNAME_TANKERKOENIG and API_KEY_TANKERKOENIG."
                )
                return  # stop trying — creds are wrong
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

            # ── Filter to configured stations only ────────────────────
            # The full CSV has ~14K stations (~35MB). We only need our
            # configured ones, so filter early to save memory & compute.
            df = df[df["station_uuid"].isin(self._station_ids)]
            if df.empty:
                logger.debug(
                    f"Tankerkoenig CSV: {day.date()} — none of our "
                    f"{len(self._station_ids)} stations found in CSV"
                )
                continue

            df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Europe/Berlin")

            # Replace 0.000 with NaN (0 means "no price available")
            for fuel in FUEL_COLUMNS:
                if fuel in df.columns:
                    df[fuel] = pd.to_numeric(df[fuel], errors="coerce")
                    df.loc[df[fuel] <= 0, fuel] = pd.NA

            # Build 10-min time index for the day
            day_start = day.tz_localize("Europe/Berlin") if day.tzinfo is None else day.tz_convert("Europe/Berlin")
            day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(
                minutes=10
            )
            time_index = pd.date_range(day_start, day_end, freq=RESAMPLE_FREQ)

            # ── National average at 10-min resolution ─────────────────
            # Group by station, resample each, then average across stations
            national_prices: Dict[str, List[pd.Series]] = {
                f: [] for f in FUEL_COLUMNS
            }

            station_groups = df.groupby("station_uuid")
            for sid, sdf in station_groups:
                sdf = sdf.set_index("date").sort_index()
                if sid not in carry_over:
                    carry_over[sid] = {}
                for fuel in FUEL_COLUMNS:
                    if fuel not in sdf.columns:
                        continue
                    fuel_series = sdf[fuel].dropna()
                    prev = carry_over.get(sid, {}).get(fuel)

                    resampled = _resample_fuel_series(
                        fuel_series, time_index, prev, day_start
                    )

                    if resampled is None:
                        continue

                    # Update carry-over with last known price
                    last_valid = resampled.last_valid_index()
                    if last_valid is not None:
                        carry_over[sid][fuel] = float(
                            resampled[last_valid]
                        )

                    national_prices[fuel].append(resampled)

                    # City-level series for configured stations
                    if sid in self._station_to_city:
                        city = self._station_to_city[sid]
                        uid = self._filter_to_unique_id.get((city, fuel))
                        if uid:
                            for ts, val in resampled.dropna().items():
                                result[uid].append({
                                    "ts": ts.isoformat(),
                                    "value": round(float(val), 4),
                                })

            # Compute national average per time step
            for fuel in FUEL_COLUMNS:
                if not national_prices[fuel]:
                    continue
                stacked = pd.concat(national_prices[fuel], axis=1)
                avg_series = stacked.mean(axis=1).dropna()
                uid = self._filter_to_unique_id.get(("national", fuel))
                if uid:
                    for ts, val in avg_series.items():
                        result[uid].append({
                            "ts": ts.isoformat(),
                            "value": round(float(val), 4),
                        })

            n_stations = df["station_uuid"].nunique()
            n_points = len(time_index)
            logger.info(
                f"Tankerkoenig CSV: {day.date()} — {n_stations} stations, "
                f"{n_points} time steps per station"
            )

            # small delay to avoid hammering the server
            await asyncio.sleep(0.5)

        total = sum(len(v) for v in result.values())
        logger.info(f"Tankerkoenig CSV backfill complete: {total} data points")


def _resample_fuel_series(
    series: pd.Series,
    time_index: pd.DatetimeIndex,
    prev_price: Optional[float],
    day_start: pd.Timestamp,
) -> Optional[pd.Series]:
    """Resample a single fuel's event series to 10-min chunks.

    Raw Tankerkoenig data emits a row when ANY fuel column changes,
    so each individual fuel column contains many "echo" rows where
    its value didn't actually change.  We first strip consecutive
    duplicates, then for each 10-minute bin:

      - If no actual price change falls inside the bin: use the
        last known (carried-forward) price.
      - If one or more actual price changes fall inside the bin:
        compute the simple (unweighted) average of the carry-in
        price and all new prices.

    Args:
        series: Non-null price values for one fuel, sorted by time.
                May contain echo rows (consecutive duplicates).
        time_index: The regular 10-min grid for the day.
        prev_price: Last known price from the previous day (carry-over).
        day_start: Start of the calendar day (tz-aware).

    Returns:
        A pd.Series indexed by time_index, or None if no data at all.
    """
    if series.empty and prev_price is None:
        return None

    # ── Deduplicate: keep only actual price changes ──────────────────
    # Strip consecutive duplicates so that only real price changes are
    # treated as events for averaging purposes.
    if not series.empty:
        changes_only = series[series != series.shift()]
    else:
        changes_only = series

    freq = pd.Timedelta(minutes=10)
    result_values = []

    # Build a forward-filled "last known price" series for lookups.
    # Include prev_price as a seed just before day_start so ffill works.
    if prev_price is not None and (changes_only.empty or changes_only.index[0] > day_start):
        seed = pd.Series([prev_price], index=[day_start - pd.Timedelta(seconds=1)])
        full_series = pd.concat([seed, changes_only])
    else:
        full_series = changes_only

    # Create a forward-filled version at union of events + grid
    combined_idx = full_series.index.union(time_index)
    ffilled = full_series.reindex(combined_idx).ffill()

    for t in time_index:
        bin_start = t
        bin_end = t + freq

        # Price valid at (or just before) this bin's start = carry-in price
        carry_in = ffilled.get(bin_start)

        # Actual price changes strictly inside the bin
        events_in_bin = changes_only[
            (changes_only.index > bin_start) & (changes_only.index < bin_end)
        ]

        if events_in_bin.empty:
            # No price change during this chunk — use the carry-in price
            result_values.append(carry_in)
        else:
            # Average: carry-in price + all new prices in the bin
            prices = [carry_in] + events_in_bin.tolist()
            # Remove None/NaN
            prices = [p for p in prices if p is not None and pd.notna(p)]
            if prices:
                result_values.append(sum(prices) / len(prices))
            else:
                result_values.append(None)

    return pd.Series(result_values, index=time_index, dtype=float)

