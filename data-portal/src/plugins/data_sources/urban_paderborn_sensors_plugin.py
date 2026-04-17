"""Urban Paderborn Sensor Plugin — Teuto.net Data Hub

Generic sensor-style plugin for point measurements from the Paderborn Urban
Data Hub.  Used for sensors whose value is a direct measurement (e.g. water
temperature) — no class filter, no direction split, no aggregation.

API endpoint:
    https://export.data-hub.teuto.net/geojson?project=<project>
    &query=<metric>[<lookback>]

One HTTP call per (project, metric) returns all sensors for that project.
Each GeoJSON feature corresponds to one physical sensor (one `devid`) and
carries a `values` array of (timestamp, value) pairs.  The plugin picks out
the sensors listed in `series_definitions` by `devid`.

Series unique_id scheme:
    urban_paderborn.<location-slug>.<metric-short>

Example:
    urban_paderborn.padersee.water_temperature

License: CC-BY 4.0 — Stadt Paderborn / Urban Data Hub
"""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

BASE_URL = "https://export.data-hub.teuto.net/geojson"


def _build_query_url(project: str, metric: str, lookback: str) -> str:
    """
    Build the GeoJSON export URL for a simple range query without label filters.

    The server expects a literal `[<lookback>]` suffix (square brackets must
    not be percent-encoded).
    """
    params = f"project={quote(project, safe='')}&query={metric}[{lookback}]"
    return f"{BASE_URL}?{params}"


class UrbanPaderbornSensorsPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Urban Paderborn point-measurement sensors.

    Makes one HTTP call per (project, metric) and distributes the response to
    the configured series by `devid`.  No aggregation is applied — raw
    (timestamp, value) pairs are stored directly (upserts are handled by the
    data-portal scheduler).

    request_params (from sources.yaml):
        project (str): Data Hub project ID.
        metric (str):  Metric name, e.g. "water_temperature_degrees_celsius".
        lookback (str): PromQL range, e.g. "1h".
        ssl_verify (bool): Whether to verify TLS certificate (default True).
        timeout_seconds (float): HTTP timeout (default 60).

    Each TimeSeriesDefinition.extract_filter must contain:
        devid (str): Sensor devid from the Data Hub.
    """

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ) -> None:
        super().__init__(group_id, request_params, series_definitions, schedule)

        self._project: str = request_params["project"]
        self._metric: str = request_params["metric"]
        self._lookback: str = request_params.get("lookback", "1h")
        self._ssl_verify: bool = request_params.get("ssl_verify", True)
        self._timeout: float = float(request_params.get("timeout_seconds", 60.0))

        # devid -> unique_id (one sensor per configured series)
        self._devid_to_uid: Dict[str, str] = {}
        for sd in self._series_definitions:
            devid = sd.extract_filter.get("devid")
            if not devid:
                logger.warning(
                    "[%s] Series %s has no devid in extract_filter — skipping",
                    group_id, sd.unique_id,
                )
                continue
            self._devid_to_uid[devid] = sd.unique_id

        logger.info(
            "[%s] UrbanPaderbornSensors init: project=%s metric=%s sensors=%d lookback=%s",
            group_id, self._project, self._metric,
            len(self._devid_to_uid), self._lookback,
        )

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "UTC"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch raw sensor measurements for all configured series in one HTTP call.
        """
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in self._devid_to_uid.values()
        }

        url = _build_query_url(self._project, self._metric, self._lookback)
        logger.debug("[%s] GET %s", self._group_id, url)

        async with httpx.AsyncClient(
            verify=self._ssl_verify,
            timeout=httpx.Timeout(self._timeout),
        ) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[%s] HTTP %d for %s/%s: %s",
                    self._group_id, exc.response.status_code,
                    self._project, self._metric, exc,
                )
                return result
            except httpx.RequestError as exc:
                logger.error(
                    "[%s] Request error for %s/%s: %s",
                    self._group_id, self._project, self._metric, exc,
                )
                return result

        try:
            geojson = response.json()
        except Exception as exc:
            logger.error(
                "[%s] JSON parse error for %s/%s: %s",
                self._group_id, self._project, self._metric, exc,
            )
            return result

        features = geojson.get("features", [])
        logger.debug(
            "[%s] %s/%s: %d features returned",
            self._group_id, self._project, self._metric, len(features),
        )

        for feature in features:
            props = feature.get("properties", {})
            devid = props.get("devid", "")
            uid = self._devid_to_uid.get(devid)
            if uid is None:
                continue  # sensor not in our allowlist

            raw_values = props.get("values", [])
            for entry in raw_values:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    ts_str, val = entry
                    try:
                        result[uid].append({"ts": str(ts_str), "value": float(val)})
                    except (TypeError, ValueError):
                        pass

        total_points = sum(len(v) for v in result.values())
        logger.info(
            "[%s] Fetch complete: %d series, %d total points (lookback=%s)",
            self._group_id, len(result), total_points, self._lookback,
        )
        return result
