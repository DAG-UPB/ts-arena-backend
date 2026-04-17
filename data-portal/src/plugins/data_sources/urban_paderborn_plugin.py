"""Urban Paderborn Traffic Plugin — Teuto.net Data Hub

Fetches vehicle traffic data from the Urban Data Hub Paderborn (teuto.net)
via its Prometheus-compatible GeoJSON export API.

API endpoint:
    https://export.data-hub.teuto.net/geojson?project=paderborn.spb-a66-topo
    &query=<metric>{devid="<id>",class=~"<regex>"}[<lookback>]

Key design decisions:
- Direction is NOT included in the URL query filter.  The API returns both
  directions (0 and 1) per class; the plugin splits client-side on the
  `direction` property.
- Multi-metric stacking is NOT supported by the API (duplicate `query=` param
  returns HTTP 400).  One HTTP call is made per metric per devid.
- Aggregation (avg, count) is computed client-side from the raw time-series
  values returned in the `values` list of each feature.
- Only aggregate rows are stored — raw per-vehicle rows are never persisted.

Series unique_id scheme:
    traffic_<devid>_dir<N>_<class_group>_<metric>_<aggregate>_5min

Example:
    traffic_topo004932_dir0_motorvehicles_speed_kilometers_per_hour_avg_5min

License: CC-BY 4.0 — Stadt Paderborn / Urban Data Hub
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote

import httpx

from src.plugins.base_plugin import MultiSeriesPlugin, TimeSeriesDefinition

logger = logging.getLogger(__name__)

BASE_URL = "https://export.data-hub.teuto.net/geojson"


def _build_query_url(
    project: str,
    devid: str,
    class_regex: str,
    metric: str,
    lookback: str,
) -> str:
    """
    Build the GeoJSON export URL using PromQL inline label selectors.

    The server parses PromQL: metric{label="value",...}[range].
    Curly braces, equals, tilde, pipe and double-quotes must be URL-encoded.
    Square brackets in [range] must be left unencoded (server rejects %5B%5D).

    Args:
        project: e.g. "paderborn.spb-a66-topo"
        devid: e.g. "topo004932"
        class_regex: e.g. "car|bus|truck"
        metric: e.g. "speed_kilometers_per_hour"
        lookback: e.g. "1h" or "96h"
    """
    selector = (
        f'{metric}'
        f'{{devid="{devid}",class=~"{class_regex}"}}'
        f'[{lookback}]'
    )
    # URL-encode everything except the square brackets (server requires literal [])
    # We encode the curly-brace selector portion only
    encoded_selector = (
        metric
        + quote(f'{{devid="{devid}",class=~"{class_regex}"}}', safe="")
        + f"[{lookback}]"
    )
    params = f"project={quote(project, safe='')}&query={encoded_selector}"
    return f"{BASE_URL}?{params}"


def _bucket_ts(ts_str: str, bucket_seconds: int = 300) -> str:
    """Round an ISO-8601 timestamp down to the nearest bucket boundary (default 5 min)."""
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    epoch = int(dt.timestamp())
    bucketed = (epoch // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(bucketed, tz=timezone.utc).isoformat()


def _compute_aggregates(
    values: List[Tuple[str, float]],
    aggregates: List[str],
    bucket_seconds: int = 300,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Compute per-bucket aggregates from a flat list of (timestamp, value) pairs.

    Args:
        values: List of (ISO-8601 timestamp, float) tuples.
        aggregates: List of aggregate names to compute, e.g. ["avg", "count"].
        bucket_seconds: Bucket width in seconds (default 300 = 5 min).

    Returns:
        Dict mapping bucket_ts (ISO-8601 str) → {agg_name: value}.
    """
    buckets: Dict[str, List[float]] = defaultdict(list)
    for ts_str, val in values:
        if val is None:
            continue
        bts = _bucket_ts(ts_str, bucket_seconds)
        buckets[bts].append(float(val))

    result: Dict[str, Dict[str, Optional[float]]] = {}
    for bts, vals in buckets.items():
        row: Dict[str, Optional[float]] = {}
        for agg in aggregates:
            if agg == "avg":
                row["avg"] = sum(vals) / len(vals) if vals else None
            elif agg == "count":
                row["count"] = float(len(vals))
            elif agg == "min":
                row["min"] = min(vals) if vals else None
            elif agg == "max":
                row["max"] = max(vals) if vals else None
            elif agg == "sum":
                row["sum"] = sum(vals) if vals else None
            elif agg == "stddev":
                if len(vals) > 1:
                    mean = sum(vals) / len(vals)
                    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
                    row["stddev"] = variance ** 0.5
                else:
                    row["stddev"] = 0.0
            else:
                logger.warning("Unknown aggregate: %s", agg)
        result[bts] = row
    return result


class UrbanPaderbornPlugin(MultiSeriesPlugin):
    """
    Multi-series plugin for Urban Paderborn traffic data.

    Makes one HTTP call per configured (devid, metric) combination, then
    splits the response by direction client-side and computes aggregates
    per 5-minute bucket.

    request_params (from sources.yaml):
        project (str): Data Hub project ID.
        lookback (str): PromQL range, e.g. "1h" or "96h".
        ssl_verify (bool): Whether to verify TLS certificate (default True).

    Series unique_id encodes:
        traffic_{devid}_dir{N}_{class_group}_{metric}_{aggregate}_5min
    """

    BUCKET_SECONDS = 300  # 5-minute aggregation buckets

    def __init__(
        self,
        group_id: str,
        request_params: Dict[str, Any],
        series_definitions: List[TimeSeriesDefinition],
        schedule: str,
    ) -> None:
        super().__init__(group_id, request_params, series_definitions, schedule)

        self._project: str = request_params.get("project", "paderborn.spb-a66-topo")
        self._lookback: str = request_params.get("lookback", "1h")
        self._ssl_verify: bool = request_params.get("ssl_verify", True)
        self._timeout: float = float(request_params.get("timeout_seconds", 60.0))

        # Parse series definitions into a lookup structure.
        # Each series_def.extract_filter contains:
        #   devid, direction (int), class_group_regex, metric, aggregate
        self._uid_to_filter: Dict[str, Dict[str, Any]] = {
            sd.unique_id: sd.extract_filter for sd in self._series_definitions
        }

        # Build the set of (devid, class_group_name, class_regex, metric) fetch jobs
        # to avoid duplicate HTTP calls for the same (devid, metric, class_group).
        fetch_jobs: Dict[Tuple[str, str, str, str], List[str]] = defaultdict(list)
        for uid, filt in self._uid_to_filter.items():
            key = (
                filt["devid"],
                filt["class_group"],
                filt["class_regex"],
                filt["metric"],
            )
            fetch_jobs[key].append(uid)
        self._fetch_jobs = dict(fetch_jobs)

        logger.info(
            "[%s] UrbanPaderborn plugin init: %d series, %d HTTP fetch jobs, lookback=%s",
            group_id,
            len(self._series_definitions),
            len(self._fetch_jobs),
            self._lookback,
        )

    def get_detected_timezone(self, unique_id: str) -> Optional[str]:
        return "UTC"

    async def get_historical_data_multi(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch and aggregate traffic data for all configured series.

        Makes one HTTP call per (devid, class_group, metric) combination,
        then distributes aggregated values to the matching series by direction
        and aggregate type.

        Returns:
            Dict mapping unique_id → list of {"ts": str, "value": float}.
        """
        result: Dict[str, List[Dict[str, Any]]] = {
            uid: [] for uid in self._uid_to_filter
        }

        async with httpx.AsyncClient(
            verify=self._ssl_verify,
            timeout=httpx.Timeout(self._timeout),
        ) as client:
            for (devid, class_group, class_regex, metric), uids in self._fetch_jobs.items():
                url = _build_query_url(
                    project=self._project,
                    devid=devid,
                    class_regex=class_regex,
                    metric=metric,
                    lookback=self._lookback,
                )
                logger.debug("[%s] GET %s", self._group_id, url)

                try:
                    response = await client.get(url)
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "[%s] HTTP %d for %s/%s: %s",
                        self._group_id, exc.response.status_code,
                        devid, metric, exc,
                    )
                    continue
                except httpx.RequestError as exc:
                    logger.error(
                        "[%s] Request error for %s/%s: %s",
                        self._group_id, devid, metric, exc,
                    )
                    continue

                try:
                    geojson = response.json()
                except Exception as exc:
                    logger.error(
                        "[%s] JSON parse error for %s/%s: %s",
                        self._group_id, devid, metric, exc,
                    )
                    continue

                features = geojson.get("features", [])
                logger.debug(
                    "[%s] %s/%s/%s: %d features returned",
                    self._group_id, devid, class_group, metric, len(features),
                )

                # Group raw values by direction
                # direction_values: {direction_int -> [(ts, value), ...]}
                direction_values: Dict[int, List[Tuple[str, float]]] = defaultdict(list)

                for feature in features:
                    props = feature.get("properties", {})
                    feat_metric = props.get("__name__", "")
                    feat_devid = props.get("devid", "")
                    feat_class = props.get("class", "")

                    # Client-side filtering
                    if feat_metric != metric:
                        continue
                    if feat_devid != devid:
                        continue
                    if not re.match(f"^({class_regex})$", feat_class):
                        continue

                    try:
                        direction = int(props.get("direction", -1))
                    except (TypeError, ValueError):
                        direction = -1

                    raw_values = props.get("values", [])
                    for entry in raw_values:
                        if isinstance(entry, (list, tuple)) and len(entry) == 2:
                            ts_str, val = entry
                            try:
                                direction_values[direction].append(
                                    (str(ts_str), float(val))
                                )
                            except (TypeError, ValueError):
                                pass

                # Compute aggregates per direction, then fill matching series
                for direction, raw_vals in direction_values.items():
                    # Determine which aggregates are needed for this direction
                    needed_aggs: set = set()
                    for uid in uids:
                        filt = self._uid_to_filter[uid]
                        if int(filt["direction"]) == direction:
                            needed_aggs.add(filt["aggregate"])

                    if not needed_aggs:
                        continue

                    agg_buckets = _compute_aggregates(
                        raw_vals,
                        list(needed_aggs),
                        self.BUCKET_SECONDS,
                    )

                    for uid in uids:
                        filt = self._uid_to_filter[uid]
                        if int(filt["direction"]) != direction:
                            continue
                        agg_name = filt["aggregate"]
                        for bts, agg_row in agg_buckets.items():
                            val = agg_row.get(agg_name)
                            if val is not None:
                                result[uid].append({"ts": bts, "value": val})

        total_points = sum(len(v) for v in result.values())
        logger.info(
            "[%s] Fetch complete: %d series, %d total data points (lookback=%s)",
            self._group_id, len(result), total_points, self._lookback,
        )
        return result
