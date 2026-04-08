"""Münster Parking (PLS) Multi-Series Plugin

Fetches real-time parking garage occupancy from the City of Münster's
Parkleitsystem (PLS) XML feed. No historical API exists — data is
collected by polling every 5 minutes.

Data source:
    https://www.stadt-muenster.de/ms/tiefbauamt/pls/PLS-INet.xml
Open Data page:
    https://opendata.stadt-muenster.de/dataset/parkleitsystem-parkhausbelegung-aktuell

Each parking garage in the XML has a <ParkingFacility> element with:
    <ShortDescription>  — short name / ID used to match series
    <TotalCapacity>     — total number of spaces
    <VacantSpaces>      — currently available (free) spaces

Series selection via extract_filter:
    garage_id: "total"           → sum of VacantSpaces across all open garages
    garage_id: "<ShortDescription>" → VacantSpaces for one specific garage

License: CC-BY 4.0 — Stadt Münster / Tiefbauamt
"""

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

PLS_URL = "https://www.stadt-muenster.de/ms/tiefbauamt/pls/PLS-INet.xml"

# Namespaces used in the PLS XML
_NS = {
    "d2": "http://datex2.eu/schema/2/2_0",
}

TOTAL_GARAGE_ID = "total"


class MuensterParkingPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Münster parking garage occupancy (PLS feed).

    One HTTP GET fetches the XML feed and populates all configured series
    from a single response. Series are identified by extract_filter.garage_id:
    - "total" → sum of available spaces across all open garages
    - any other string → available spaces for that specific garage
      (matched against the <shortDescription> element, case-insensitive)
    """

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ):
        super().__init__(group_id, request_params, series_definitions, schedule)

        # Map garage_id (lower) → unique_id for fast lookup
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

        Since the PLS does not expose historical data, this method always
        returns the current snapshot (one data point per series per call).
        The scheduler accumulates these snapshots over time.

        start_date and end_date are accepted for interface compatibility
        but are not used.
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
        """Download the PLS XML feed. Returns raw text or None on error."""
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
        """Parse PLS XML and populate result with data points.

        The PLS XML is a DATEX II publication. We look for
        <parkingFacilityStatus> elements and extract:
          - facilityName / shortDescription → garage identifier
          - vacantParkingSpaces → available spaces
          - parkingFacilityStatusTime → timestamp (fallback: now)

        Falls back to a simple tag scan when DATEX II namespaces are absent.
        """
        root = ET.fromstring(xml_text)

        # Try namespace-aware parse first, then plain tag scan
        facilities = root.findall(".//d2:parkingFacilityStatus", _NS)
        if not facilities:
            # Fall back to namespace-stripped scan
            facilities = root.findall(".//{*}parkingFacilityStatus")
        if not facilities:
            # Last resort: look for any ParkingFacility or similar element
            facilities = root.findall(".//{*}ParkingFacility")

        total_vacant = 0
        total_count = 0

        for facility in facilities:
            name = self._get_text(facility, "shortDescription") or \
                   self._get_text(facility, "facilityName") or \
                   self._get_text(facility, "ShortDescription") or \
                   self._get_text(facility, "Name")

            vacant_str = (
                self._get_text(facility, "vacantParkingSpaces")
                or self._get_text(facility, "VacantSpaces")
                or self._get_text(facility, "freeCapacity")
            )

            if vacant_str is None:
                continue

            try:
                vacant = int(vacant_str)
            except ValueError:
                logger.debug(
                    f"[{self._group_id}] Non-integer vacantParkingSpaces "
                    f"for {name!r}: {vacant_str!r}"
                )
                continue

            # Timestamp: prefer the feed's own timestamp, fall back to now
            ts_raw = (
                self._get_text(facility, "parkingFacilityStatusTime")
                or self._get_text(facility, "lastUpdated")
            )
            ts = self._parse_ts(ts_raw)

            total_vacant += vacant
            total_count += 1

            if name and name.lower() in self._garage_to_uid:
                uid = self._garage_to_uid[name.lower()]
                result[uid].append({"ts": ts, "value": float(vacant)})
                logger.debug(
                    f"[{self._group_id}] Garage {name!r}: {vacant} free spaces @ {ts}"
                )

        if self._has_total and self._total_uid and total_count > 0:
            ts_now = datetime.now(timezone.utc).isoformat()
            result[self._total_uid].append(
                {"ts": ts_now, "value": float(total_vacant)}
            )
            logger.info(
                f"[{self._group_id}] Total free spaces: {total_vacant} "
                f"(across {total_count} garages)"
            )
        elif self._has_total and total_count == 0:
            logger.warning(
                f"[{self._group_id}] No facility data found in PLS XML — "
                "check XML structure / namespace"
            )

    @staticmethod
    def _get_text(element: ET.Element, tag: str) -> Optional[str]:
        """Return text of first child with local name matching tag (namespace-agnostic)."""
        # Exact match
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        # Namespace-wildcard match
        child = element.find(f"{{*}}{tag}")
        if child is not None and child.text:
            return child.text.strip()
        return None

    @staticmethod
    def _parse_ts(ts_raw: Optional[str]) -> str:
        """Parse an ISO-8601 timestamp string or return current UTC time."""
        if ts_raw:
            try:
                dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                return dt.astimezone(timezone.utc).isoformat()
            except ValueError:
                pass
        return datetime.now(timezone.utc).isoformat()
