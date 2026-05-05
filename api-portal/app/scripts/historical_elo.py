"""
Reconstruct historical month-end ELO snapshots into forecasts.daily_rankings.

For each target date, ELO is computed using only rounds whose
registration_start <= date (via score_cutoff_date), so each snapshot
reflects the platform state as it stood on that day.

Run inside the running api-portal container:
    docker compose exec api-portal python -m app.scripts.historical_elo

Optional: pass dates as ISO strings to override the defaults:
    docker compose exec api-portal python -m app.scripts.historical_elo \
        2025-12-31 2026-01-31
"""
import asyncio
import logging
import sys
from datetime import date

from app.database.connection import SessionLocal
from app.services.elo_ranking_service import EloRankingService

DEFAULT_DATES = [
    date(2025, 12, 31),
    date(2026, 1, 31),
    date(2026, 2, 28),
    date(2026, 3, 31),
    date(2026, 4, 30),
]


def _parse_dates(argv: list[str]) -> list[date]:
    if not argv:
        return DEFAULT_DATES
    return [date.fromisoformat(a) for a in argv]


async def main(target_dates: list[date]) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("historical-elo")
    logger.info("Reconstructing %d snapshot(s): %s", len(target_dates), target_dates)

    for d in target_dates:
        logger.info("=== Computing snapshot for %s ===", d)
        async with SessionLocal() as session:
            svc = EloRankingService(session)
            results = await svc.calculate_and_store_all_ratings(
                n_bootstraps=500,
                calculation_date=d,
            )
            logger.info(
                "Done %s — global=%d, definitions=%d, freq_horizon=%d, %dms",
                d,
                len(results.get("global", [])),
                len(results.get("per_definition", [])),
                len(results.get("per_frequency_horizon", [])),
                results.get("total_duration_ms", 0),
            )


if __name__ == "__main__":
    asyncio.run(main(_parse_dates(sys.argv[1:])))
