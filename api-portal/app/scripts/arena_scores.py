"""
Compute and store the Arena Score for one date in forecasts.daily_arena_scores.

Run inside the running api-portal container:
    docker compose exec api-portal python -m app.scripts.arena_scores

The date defaults to today (UTC); a date that already has rows is replaced:
    docker compose exec api-portal python -m app.scripts.arena_scores --date 2026-10-14
"""
import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from app.database.connection import SessionLocal
from app.services.arena_score_service import compute_and_store_arena_scores


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute and store the Arena Score.")
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="Calculation date, YYYY-MM-DD (default: today, UTC).")
    return parser.parse_args(argv)


async def main(calculation_date: date) -> Dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    async with SessionLocal() as session:
        return await compute_and_store_arena_scores(session, calculation_date)


if __name__ == "__main__":
    args = _parse_args()
    summary = asyncio.run(main(args.date or datetime.now(timezone.utc).date()))
    sys.exit(1 if summary["failures"] else 0)
