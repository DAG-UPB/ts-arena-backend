"""Münster Parking (PLS) Multi-Series Plugin

Fetches real-time parking garage occupancy from the City of Münster's
Parkleitsystem (PLS) XML feed. No historical API exists — data is
collected by polling every 5 minutes.

Data source:
    https://www.stadt-muenster.de/ms/tiefbauamt/pls/PLS-INet.xml
Open Data page:
    https://opendata.stadt-muenster.de/dataset/parkleitsystem-parkhausbelegung-aktuell

XML structure (plain, no namespaces):
    <parkhaeuser>
      <parkhaus>
        <bezeichnung>Parkhaus Cineplex</bezeichnung>
        <gesamt>590</gesamt>
        <frei>485</frei>
        <status>frei</status>
        <zeitstempel>08.04.2026 14:33</zeitstempel>
      </parkhaus>
      ...
    </parkhaeuser>

Series selection via extract_filter:
    garage_id: "total"           → sum of <frei> across all garages
    garage_id: "<bezeichnung>"   → <frei> for one specific garage

License: CC-BY 4.0 — Stadt Münster / Tiefbauamt
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

PLS_URL = "https://www.stadt-muenster.de/ms/tiefbauamt/pls/PLS-INet.xml"

TOTAL_GARAGE_ID = "total"


class MuensterParkingPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Münster parking garage occupancy (PLS feed).

    One HTTP GET fetches the XML feed and populates all configured series.
    Series are identified by extract_filter.garage_id:
    - "total" → sum of free spaces across all garages
    - any other string → free spaces for that specific garage
      (matched against <bezeichnung>, case-insensitive)
    """

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)

        self._garage_to_uid: Dict[str, str] = {}
        self._has_total = False
        self._total_uid: Optional[str] = None

        for series_def in self._series_definitions:
            garage_id = series_def.extract_filter.get("garage_id", "").strip()
            if not garage_id:
                logger.warning(
                    f"[{group_id}] Series {series_def.unique_id} has no garage_id "
                    "in extract_filter — skipping"
                )
                continue
            if garage_id.lower() == TOTAL_GARAGE_ID:
                self._has_total = True
                self._total_uid = series_def.unique_id
            else:
                self._garage_to_uid[garage_id.lower()] = series_def.unique_id

        logger.info(
            f"[{group_id}] Münster Parking plugin initialised: "
            f"total={'yes' if self._has_total else 'no'}, "
            f"individual garages={len(self._garage_to_uid)}"
        )

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "Europe/Berlin"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch current parking occupancy from the PLS XML feed.

        No historical data available — always returns the current snapshot.
        The scheduler accumulates these over time.
        """
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in (
                list(self._garage_to_uid.values())
                + ([self._total_uid] if self._total_uid else [])
            )
        }

        xml_text = await self._fetch_xml()
        if xml_text is None:
            return result

        try:
            self._parse_and_fill(xml_text, result)
        except Exception as exc:
            logger.error(f"[{self._group_id}] Failed to parse PLS XML: {exc}", exc_info=True)

        return result

    async def _fetch_xml(self) -> Optional[str]:
        """Download the PLS XML feed."""
        timeout = httpx.Timeout(30.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(PLS_URL)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"[{self._group_id}] PLS HTTP error {exc.response.status_code}: {exc}"
            )
        except httpx.RequestError as exc:
            logger.error(f"[{self._group_id}] PLS request failed: {exc}")
        return None

    def _parse_and_fill(
        self,
        xml_text: str,
        result: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        """Parse PLS XML and populate result dict.

        XML structure:
            <parkhaeuser>
              <parkhaus>
                <bezeichnung>Name</bezeichnung>
                <gesamt>total_capacity</gesamt>
                <frei>free_spaces</frei>
                <status>frei|besetzt|geschlossen</status>
                <zeitstempel>DD.MM.YYYY HH:MM</zeitstempel>
              </parkhaus>
              ...
            </parkhaeuser>
        """
        root = ET.fromstring(xml_text)
        facilities = root.findall("parkhaus")

        total_free = 0
        total_count = 0

        for parkhaus in facilities:
            name_el = parkhaus.find("bezeichnung")
            frei_el = parkhaus.find("frei")
            status_el = parkhaus.find("status")
            ts_el = parkhaus.find("zeitstempel")

            if frei_el is None or frei_el.text is None:
                continue

            try:
                free_spaces = int(frei_el.text.strip())
            except ValueError:
                continue

            name = name_el.text.strip() if name_el is not None and name_el.text else "unknown"
            status = status_el.text.strip().lower() if status_el is not None and status_el.text else ""

            # Parse timestamp: "DD.MM.YYYY HH:MM" in Europe/Berlin
            ts_str = self._parse_zeitstempel(ts_el)

            total_free += free_spaces
            total_count += 1

            # Match individual garage series
            if name.lower() in self._garage_to_uid:
                uid = self._garage_to_uid[name.lower()]
                result[uid].append({"ts": ts_str, "value": float(free_spaces)})
                logger.debug(
                    f"[{self._group_id}] {name}: {free_spaces} free ({status}) @ {ts_str}"
                )

        if self._has_total and self._total_uid and total_count > 0:
            ts_now = datetime.now(timezone.utc).isoformat()
            result[self._total_uid].append(
                {"ts": ts_now, "value": float(total_free)}
            )
            logger.info(
                f"[{self._group_id}] Total free spaces: {total_free} "
                f"(across {total_count} garages)"
            )
        elif self._has_total and total_count == 0:
            logger.warning(
                f"[{self._group_id}] No <parkhaus> elements found in PLS XML"
            )

    @staticmethod
    def _parse_zeitstempel(ts_el: Optional[ET.Element]) -> str:
        """Parse 'DD.MM.YYYY HH:MM' timestamp to ISO-8601 UTC string."""
        if ts_el is not None and ts_el.text:
            try:
                from zoneinfo import ZoneInfo
                dt = datetime.strptime(ts_el.text.strip(), "%d.%m.%Y %H:%M")
                dt = dt.replace(tzinfo=ZoneInfo("Europe/Berlin"))
                return dt.astimezone(timezone.utc).isoformat()
            except (ValueError, KeyError):
                pass
        return datetime.now(timezone.utc).isoformat()
