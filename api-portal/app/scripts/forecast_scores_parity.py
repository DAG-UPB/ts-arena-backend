"""Read-only comparison of `forecasts.forecast_scores` with the arena scores in `forecasts.scores`.

    python -m app.scripts.forecast_scores_parity [flags]

    --days N      rounds that ended within the last N days (default 14)
    --round-id X  only round X
"""
from __future__ import annotations

import argparse
import asyncio
import math
from collections import Counter
from datetime import timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.database.connection import SessionLocal
from app.services.forecast_scoring_service import EVALUATION_TIMEOUT

TOLERANCE = 1e-9

_QUERY = """
    WITH rounds AS (
        SELECT r.id
        FROM challenges.rounds r
        WHERE r.end_time > now() - CAST(:window AS interval)
          AND r.end_time < now() - CAST(:grace AS interval)
          AND (CAST(:round_id AS integer) IS NULL OR r.id = CAST(:round_id AS integer))
    ),
    old AS (
        SELECT s.round_id, s.model_id, s.series_id, s.mase, s.rmse, s.evaluated_count,
               s.evaluation_status
        FROM forecasts.scores s
        JOIN rounds ON rounds.id = s.round_id
        WHERE s.final_evaluation
    ),
    new AS (
        SELECT f.round_id, f.model_id, f.series_id, f.mae, f.rmse, f.naive_mae, f.n_points,
               f.evaluation_status, f.final_evaluation
        FROM forecasts.forecast_scores f
        JOIN rounds ON rounds.id = f.round_id
    )
    SELECT COALESCE(o.round_id, n.round_id) AS round_id,
           COALESCE(o.model_id, n.model_id) AS model_id,
           COALESCE(o.series_id, n.series_id) AS series_id,
           o.round_id IS NOT NULL AS in_old,
           n.round_id IS NOT NULL AS in_new,
           o.mase AS old_mase, o.rmse AS old_rmse, o.evaluated_count AS old_points,
           o.evaluation_status AS old_status,
           n.mae, n.rmse AS new_rmse, n.naive_mae, n.n_points AS new_points,
           n.evaluation_status AS new_status, n.final_evaluation AS new_final
    FROM old o
    FULL OUTER JOIN new n
      ON n.round_id = o.round_id AND n.model_id = o.model_id AND n.series_id = o.series_id
"""


def _comparable(value: Optional[float]) -> bool:
    return value is not None and math.isfinite(value)


def _same(value: float, reference: float) -> bool:
    return abs(value - reference) <= TOLERANCE * max(1.0, abs(reference))


def summarise(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    both = [r for r in rows if r["in_old"] and r["in_new"]]
    report: Dict[str, Any] = {
        "rounds": len({r["round_id"] for r in rows}),
        "pairs_both": len(both),
        "only_old": sum(1 for r in rows if r["in_old"] and not r["in_new"]),
        "only_new": sum(1 for r in rows if r["in_new"] and not r["in_old"]),
        "new_not_final": sum(1 for r in rows if r["in_new"] and not r["new_final"]),
        "status_agree": sum(1 for r in both if r["old_status"] == r["new_status"]),
        "status_pairs": Counter(
            (r["old_status"], r["new_status"]) for r in both if r["old_status"] != r["new_status"]
        ),
        "points_agree": sum(1 for r in both if r["old_points"] == r["new_points"]),
        "rmse_compared": 0,
        "rmse_exact": 0,
        "rmse_mismatch_by_series": Counter(),
        "relmae_compared": 0,
        "relmae_exact": 0,
        "relmae_mismatch_by_series": Counter(),
    }
    for r in both:
        if _comparable(r["old_rmse"]) and r["new_rmse"] is not None:
            report["rmse_compared"] += 1
            if _same(r["new_rmse"], r["old_rmse"]):
                report["rmse_exact"] += 1
            else:
                report["rmse_mismatch_by_series"][r["series_id"]] += 1
        old, mae, naive = r["old_mase"], r["mae"], r["naive_mae"]
        if not _comparable(old) or mae is None or not naive:
            continue
        report["relmae_compared"] += 1
        if _same(mae / naive, old):
            report["relmae_exact"] += 1
        else:
            report["relmae_mismatch_by_series"][r["series_id"]] += 1
    return report


def format_report(report: Dict[str, Any]) -> str:
    def share(n: int, d: int) -> str:
        return f"{n}/{d} ({n / d:.1%})" if d else "0/0"

    lines = [
        "",
        "=" * 72,
        "forecast_scores vs forecasts.scores (read-only)",
        "=" * 72,
        f"Rounds:                   {report['rounds']}",
        f"Pairs in both:            {report['pairs_both']}",
        f"Final only in scores:     {report['only_old']}",
        f"Only in forecast_scores:  {report['only_new']}",
        f"Not final in forecast_scores: {report['new_not_final']}",
        f"Status agrees:            {share(report['status_agree'], report['pairs_both'])}",
    ]
    lines += [
        f"    {old} -> {new}: {n}" for (old, new), n in report["status_pairs"].most_common(10)
    ]
    lines += [
        f"Evaluated points agree:   {share(report['points_agree'], report['pairs_both'])}",
        f"RMSE agrees:              {share(report['rmse_exact'], report['rmse_compared'])}",
    ]
    lines += [
        f"    series {sid}: {n} mismatch(es)"
        for sid, n in report["rmse_mismatch_by_series"].most_common(10)
    ]
    lines += [
        f"mae / naive_mae == mase:  {share(report['relmae_exact'], report['relmae_compared'])}"
        " (informative)",
    ]
    lines += [
        f"    series {sid}: {n} mismatch(es)"
        for sid, n in report["relmae_mismatch_by_series"].most_common(10)
    ]
    lines.append("=" * 72)
    return "\n".join(lines)


async def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description="Compare forecast_scores with forecasts.scores.")
    parser.add_argument("--days", type=int, default=14, help="Rounds that ended within this many days.")
    parser.add_argument("--round-id", type=int, default=None, help="Only this round.")
    args = parser.parse_args(argv)

    async with SessionLocal() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await session.execute(
            text(_QUERY),
            {"window": timedelta(days=args.days), "grace": EVALUATION_TIMEOUT, "round_id": args.round_id},
        )
        rows = [dict(row._mapping) for row in result]

    report = summarise(rows)
    print(format_report(report))
    return report


if __name__ == "__main__":
    asyncio.run(main())
