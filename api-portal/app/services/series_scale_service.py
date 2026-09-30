"""The MASE scale of each (round, series), stored once in `forecasts.series_scale`.

Taken from the served context during registration, otherwise rebuilt from SCD2 as of round creation.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.data_portal.time_series_repository import RESOLUTION_TO_BUCKET_INTERVAL
from app.database.forecasts.forecast_scores_repository import ForecastScoresRepository
from app.services.forecast_metrics import MASE_SEASONAL_LAG, context_scale

logger = logging.getLogger(__name__)

SOURCE_SERVED = "context_data"
SOURCE_REBUILT = "scd2_as_of_round_creation"


def resolution_bucket(resolution: str) -> timedelta:
    return RESOLUTION_TO_BUCKET_INTERVAL[resolution]


def _last_value(points: List[Tuple[datetime, Optional[float]]]) -> Optional[float]:
    finite = [(ts, float(v)) for ts, v in points if v is not None and np.isfinite(float(v))]
    return max(finite)[1] if finite else None


def build_scale_rows(
    round_id: int,
    context_rows: Iterable[Dict[str, Any]],
    windows: Dict[int, Tuple[datetime, datetime]],
    bucket: timedelta,
    source: str,
    m: int = MASE_SEASONAL_LAG,
) -> List[Dict[str, Any]]:
    """One row per series with context points in its window; a series without any gets none, so it is retried."""
    by_series: Dict[int, List[Tuple[datetime, Optional[float]]]] = defaultdict(list)
    for row in context_rows:
        window = windows.get(row["series_id"])
        if window is not None and window[0] <= row["ts"] <= window[1]:
            by_series[row["series_id"]].append((row["ts"], row["value"]))

    rows = []
    for series_id, (start, end) in sorted(windows.items()):
        points = by_series.get(series_id)
        if not points:
            continue
        scale, m_used, n_points, n_pairs = context_scale(
            [ts for ts, _ in points], [v for _, v in points], bucket, m
        )
        if n_points == 0:
            continue
        rows.append({
            "round_id": round_id,
            "series_id": series_id,
            "m": m_used,
            "scale": scale,
            "last_value": _last_value(points),
            "n_points": n_points,
            "n_pairs": n_pairs,
            "context_start": start,
            "context_end": end,
            "source": source,
        })
    return rows


class SeriesScaleService:
    def __init__(self, db_session: AsyncSession):
        self.repo = ForecastScoresRepository(db_session)

    async def store_served_scales(self, round_id: int, resolution: str) -> int:
        """Store the scales of the context as served. Does not commit."""
        windows = await self.repo.get_context_windows(round_id)
        if not windows:
            return 0
        rows = build_scale_rows(
            round_id,
            await self.repo.read_served_context(round_id),
            windows,
            resolution_bucket(resolution),
            SOURCE_SERVED,
        )
        await self.repo.insert_scales(rows)
        return len(rows)

    async def ensure_scales(
        self, round_id: int, resolution: str, round_created_at: datetime
    ) -> Dict[int, Dict[str, Any]]:
        """`series_id -> scale row`, rebuilding missing ones from SCD2. Does not commit."""
        scales = await self.repo.get_scales(round_id)
        windows = await self.repo.get_context_windows(round_id)
        missing = {sid: w for sid, w in windows.items() if sid not in scales}
        if not missing:
            return scales

        lo = min(start for start, _ in missing.values())
        hi = max(end for _, end in missing.values())
        rows = build_scale_rows(
            round_id,
            await self.repo.read_context_as_of(
                round_id, resolution_bucket(resolution), round_created_at, lo, hi
            ),
            missing,
            resolution_bucket(resolution),
            SOURCE_REBUILT,
        )
        await self.repo.insert_scales(rows)
        unresolved = len(missing) - len(rows)
        if unresolved:
            logger.warning(
                f"Round {round_id}: no context as of round creation for {unresolved} series; "
                f"their MASE scale stays undefined"
            )
        scales.update({row["series_id"]: row for row in rows})
        return scales
