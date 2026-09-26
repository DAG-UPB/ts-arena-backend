"""
The MASE scale of each (round, series): the in-sample naive error of the context as served.

Computed once and stored in `forecasts.series_scale`, so every model on a series is scored
against the same number, whenever and however often the round is scored.

Two sources, one definition:

- At round creation, from the context just written to `challenges.context_data`, i.e.
  exactly what participants download.
- Otherwise (rounds created before this existed, or a missed round-creation write), rebuilt
  from `data_portal.time_series_data_scd2` as of `rounds.created_at` (see
  `SeriesScaleRepository.read_context_as_of`). `context_data` is never read then: it is a
  serving cache for registration and is not kept.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.data_portal.time_series_repository import RESOLUTION_TO_BUCKET_INTERVAL
from app.database.forecasts.series_scale_repository import SeriesScaleRepository
from app.services.forecast_metrics import MASE_SEASONAL_LAG, context_scale

logger = logging.getLogger(__name__)

SOURCE_SERVED = "context_data"
SOURCE_REBUILT = "scd2_as_of_round_creation"


def resolution_bucket(resolution: str) -> timedelta:
    """Bucket width of a resolution view ("15min", "1h", "1d"): the spacing of the context."""
    return RESOLUTION_TO_BUCKET_INTERVAL[resolution]


def build_scale_rows(
    round_id: int,
    context_rows: Iterable[Dict[str, Any]],
    windows: Dict[int, Tuple[datetime, datetime]],
    bucket: timedelta,
    source: str,
    m: int = MASE_SEASONAL_LAG,
) -> List[Dict[str, Any]]:
    """One `forecasts.series_scale` row per series that has context points in its window.

    A series with no point at all gets no row: that means the source had nothing, which is a
    data problem, not a property of the series, and a stored row would never be revisited.
    A series with points but no lag pair does get a row, with `scale = NULL`.
    """
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
            "n_points": n_points,
            "n_pairs": n_pairs,
            "context_start": start,
            "context_end": end,
            "source": source,
        })
    return rows


class SeriesScaleService:
    def __init__(self, db_session: AsyncSession):
        self.repo = SeriesScaleRepository(db_session)

    async def store_served_scales(self, round_id: int, resolution: str) -> int:
        """At round creation: compute the scales from the context just served. Does not commit."""
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
        """`series_id -> scale row` for a round, rebuilding any that are missing. Does not commit.

        A series that is still missing afterwards had no context in SCD2 as of round
        creation; callers treat its scale as undefined.
        """
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
