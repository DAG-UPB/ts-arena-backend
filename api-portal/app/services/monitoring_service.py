# app/services/monitoring_service.py

"""Participation and leaderboard-membership monitoring (backend #92).

Two checks, both reading data the platform already stores.

**1. Per-model participation drop.** On 2026-09-02 one participant-operated runner
submitting four models stopped uploading and nobody noticed for nineteen days. Nothing
caught it: registration only happens inside the upload handler, so a runner that never
calls us leaves no trace at all — no request, no log line, no failed forecast. The only
evidence is a *missing* `challenges.participants` row, which is exactly what this check
looks for. A round-level "at least N forecasts arrived" aggregate (backend #53) cannot
see it: 34-35 of 38 models still submitted on every affected round.

**2. Ranking membership diff.** On 2026-09-11 nine models left the global leaderboard as a
side effect of backend #87's round cancellation, and again nobody was told. The diff runs
on every `daily_rankings` write and reports both directions — an *intended* removal
(backend #64 dropped ten models on 2026-08-24) should announce itself too.

Three design decisions worth keeping, all forced by the data:

* **Cancelled rounds still count.** backend #87 cancelled every `smard_dam` round between
  2026-08-01 and 2026-09-10. Filtering `is_cancelled` would blank the baseline exactly
  where the incident lives. Whether we later voided a round for scoring says nothing about
  whether the participant's runner showed up during its registration window — and that is
  the only question this check asks.
* **Per (model, definition), not per model.** The 2026-09 outage was staggered: the runner
  died on the 24h/15min definitions days before it stopped on the 72h/1h ones. Averaged
  across a model's definitions the drop stays above any usable threshold.
* **Windows are counted in rounds, not days.** Definitions differ in cadence, and a run of
  cancelled or missing rounds would silently shorten a day-based window. "The last 7
  rounds of this definition" means the same thing on every definition.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.alerts import Alert, send_alert_async

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        logger.warning("%s is not an integer; falling back to %s", name, default)
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except ValueError:
        logger.warning("%s is not a number; falling back to %s", name, default)
        return default


@dataclass(frozen=True)
class ParticipationThresholds:
    """Tuning for the participation check.

    The defaults are measured, not guessed. Over 2026-07-25 to 2026-08-31, among the 577
    (model, definition) pairs with a baseline registration rate of at least 0.5, the
    longest run of consecutive missed rounds was 0 for 392 pairs, 1 for 115, 2 for 21 and
    3 for 23 — only 21 pairs ever exceeded 4. `silence_streak = 5` therefore sits in the
    tail of normal flakiness rather than in the middle of it. An earlier cut using a
    3-round window and a 0.5 drop ratio produced 30+ findings a day of pure noise.
    """

    recent_n: int = 7           # rounds in the recent window
    baseline_n: int = 28        # rounds in the trailing baseline, immediately before those
    min_recent_rounds: int = 4  # too few recent rounds to judge -> skip the pair
    min_baseline_rounds: int = 14
    min_baseline_rate: float = 0.5  # only models that were genuinely participating
    drop_ratio: float = 0.34    # recent rate at or below this share of baseline -> alert
    silence_streak: int = 5     # consecutive missed rounds -> alert regardless of rate

    @classmethod
    def from_env(cls) -> "ParticipationThresholds":
        return cls(
            recent_n=_env_int("PARTICIPATION_RECENT_N", cls.recent_n),
            baseline_n=_env_int("PARTICIPATION_BASELINE_N", cls.baseline_n),
            min_recent_rounds=_env_int("PARTICIPATION_MIN_RECENT_ROUNDS", cls.min_recent_rounds),
            min_baseline_rounds=_env_int("PARTICIPATION_MIN_BASELINE_ROUNDS", cls.min_baseline_rounds),
            min_baseline_rate=_env_float("PARTICIPATION_MIN_BASELINE_RATE", cls.min_baseline_rate),
            drop_ratio=_env_float("PARTICIPATION_DROP_RATIO", cls.drop_ratio),
            silence_streak=_env_int("PARTICIPATION_SILENCE_STREAK", cls.silence_streak),
        )


@dataclass(frozen=True)
class ParticipationFinding:
    """One (model, definition) pair whose participation fell away."""

    model_id: int
    model_name: str
    definition_id: int
    definition_name: str
    miss_streak: int
    recent_reg: int
    recent_rounds: int
    baseline_reg: int
    baseline_rounds: int

    @property
    def recent_rate(self) -> float:
        return self.recent_reg / self.recent_rounds if self.recent_rounds else 0.0

    @property
    def baseline_rate(self) -> float:
        return self.baseline_reg / self.baseline_rounds if self.baseline_rounds else 0.0

    def is_silent(self, thresholds: ParticipationThresholds) -> bool:
        return self.miss_streak >= thresholds.silence_streak

    def describe(self, thresholds: ParticipationThresholds) -> str:
        if self.is_silent(thresholds):
            lead = f"silent for the last {self.miss_streak} rounds"
        else:
            lead = f"{self.recent_reg}/{self.recent_rounds} recent rounds"
        return (
            f"{self.definition_name}: {lead} "
            f"({self.recent_rate:.0%} now vs {self.baseline_rate:.0%} baseline)"
        )


@dataclass(frozen=True)
class RankingMembershipDiff:
    """Change in `global` leaderboard membership between two calculation dates."""

    metric: str
    calculation_date: date
    previous_date: Optional[date]
    entered: Sequence[str]
    left: Sequence[str]

    @property
    def changed(self) -> bool:
        return bool(self.entered) or bool(self.left)


# The participation query. One pass over the rounds of every definition, windowed by
# round ordinal rather than by date, then left-joined against participants per model.
#
# `eligible` deliberately does not filter `is_cancelled` (see the module docstring) but
# does require the registration window to have closed at `as_of` — an open window is not
# a missed round, it is a round nobody could have registered for yet.
_PARTICIPATION_SQL = text(
    """
    WITH eligible AS (
        SELECT r.id,
               r.definition_id,
               row_number() OVER (PARTITION BY r.definition_id
                                  ORDER BY r.registration_start DESC) AS rn_desc
        FROM challenges.rounds r
        WHERE r.registration_end IS NOT NULL
          AND r.registration_end <= :as_of
    ),
    windowed AS (
        SELECT id, definition_id, rn_desc, (rn_desc <= :recent_n) AS is_recent
        FROM eligible
        WHERE rn_desc <= :recent_n + :baseline_n
    ),
    -- Only models that registered at least once somewhere in the window: a model that
    -- never took part in a definition is not "missing" from it.
    active AS (
        SELECT DISTINCT p.model_id, w.definition_id
        FROM windowed w
        JOIN challenges.participants p ON p.round_id = w.id
    ),
    grid AS (
        SELECT a.model_id, w.definition_id, w.rn_desc, w.is_recent,
               (p.model_id IS NOT NULL) AS present
        FROM active a
        JOIN windowed w ON w.definition_id = a.definition_id
        LEFT JOIN challenges.participants p
               ON p.round_id = w.id AND p.model_id = a.model_id
    ),
    agg AS (
        SELECT model_id,
               definition_id,
               count(*) FILTER (WHERE is_recent)                 AS recent_rounds,
               count(*) FILTER (WHERE is_recent AND present)      AS recent_reg,
               count(*) FILTER (WHERE NOT is_recent)              AS baseline_rounds,
               count(*) FILTER (WHERE NOT is_recent AND present)  AS baseline_reg,
               -- Trailing consecutive misses: the newest round is rn_desc = 1, so the
               -- distance to the most recent round the model did register for is the
               -- length of the current silence. Never present -> the whole window.
               coalesce(min(rn_desc) FILTER (WHERE present), max(rn_desc) + 1) - 1
                   AS miss_streak
        FROM grid
        GROUP BY model_id, definition_id
    )
    SELECT a.model_id,
           mi.name AS model_name,
           a.definition_id,
           coalesce(d.name, d.schedule_id) AS definition_name,
           a.miss_streak,
           a.recent_reg,
           a.recent_rounds,
           a.baseline_reg,
           a.baseline_rounds
    FROM agg a
    JOIN models.model_info mi ON mi.id = a.model_id
    JOIN challenges.definitions d ON d.id = a.definition_id
    WHERE a.baseline_rounds >= :min_baseline_rounds
      AND a.recent_rounds >= :min_recent_rounds
      AND a.baseline_reg::numeric / a.baseline_rounds >= :min_baseline_rate
      AND (
            a.miss_streak >= :silence_streak
         OR a.recent_reg::numeric / a.recent_rounds
            <= :drop_ratio * (a.baseline_reg::numeric / a.baseline_rounds)
          )
    ORDER BY a.miss_streak DESC, mi.name, definition_name
    """
)


_PREVIOUS_RANKING_DATE_SQL = text(
    """
    SELECT max(calculation_date)
    FROM forecasts.daily_rankings
    WHERE scope_type = 'global'
      AND metric = :metric
      AND calculation_date < :calculation_date
    """
)


_RANKING_MEMBERSHIP_SQL = text(
    """
    WITH cur_m AS (
        SELECT model_id
        FROM forecasts.daily_rankings
        WHERE scope_type = 'global'
          AND metric = :metric
          AND calculation_date = :calculation_date
    ),
    prev_m AS (
        SELECT model_id
        FROM forecasts.daily_rankings
        WHERE scope_type = 'global'
          AND metric = :metric
          AND calculation_date = :previous_date
    )
    SELECT (c.model_id IS NULL) AS has_left,
           mi.name AS model_name
    FROM cur_m c
    FULL OUTER JOIN prev_m p USING (model_id)
    JOIN models.model_info mi ON mi.id = coalesce(c.model_id, p.model_id)
    WHERE c.model_id IS NULL OR p.model_id IS NULL
    ORDER BY has_left, model_name
    """
)


class MonitoringService:
    """Runs the backend #92 checks and routes their findings to the alert channel."""

    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    # --- Check 1: per-model participation drop -----------------------------------

    async def check_participation_drop(
        self,
        as_of: Optional[datetime] = None,
        thresholds: Optional[ParticipationThresholds] = None,
    ) -> List[ParticipationFinding]:
        """Find (model, definition) pairs whose participation has fallen away.

        `as_of` exists so the check can be replayed against a past date — that is how the
        2026-09-02 incident was verified — and defaults to now.
        """
        thresholds = thresholds or ParticipationThresholds.from_env()
        as_of = as_of or datetime.now(timezone.utc)

        result = await self.db.execute(
            _PARTICIPATION_SQL,
            {
                "as_of": as_of,
                "recent_n": thresholds.recent_n,
                "baseline_n": thresholds.baseline_n,
                "min_recent_rounds": thresholds.min_recent_rounds,
                "min_baseline_rounds": thresholds.min_baseline_rounds,
                "min_baseline_rate": thresholds.min_baseline_rate,
                "drop_ratio": thresholds.drop_ratio,
                "silence_streak": thresholds.silence_streak,
            },
        )

        return [
            ParticipationFinding(
                model_id=row.model_id,
                model_name=row.model_name,
                definition_id=row.definition_id,
                definition_name=row.definition_name,
                miss_streak=row.miss_streak,
                recent_reg=row.recent_reg,
                recent_rounds=row.recent_rounds,
                baseline_reg=row.baseline_reg,
                baseline_rounds=row.baseline_rounds,
            )
            for row in result.all()
        ]

    # --- Check 2: global ranking membership diff ----------------------------------

    async def diff_global_ranking_membership(
        self, metric: str, calculation_date: date
    ) -> RankingMembershipDiff:
        """Diff `global` membership for `metric` against the previous calculation date."""
        previous_date = await self.db.scalar(
            _PREVIOUS_RANKING_DATE_SQL,
            {"metric": metric, "calculation_date": calculation_date},
        )
        if previous_date is None:
            # The very first calculation for this metric. Announcing the whole
            # leaderboard as new arrivals would be noise, not news.
            logger.info(
                "No global ranking before %s for metric '%s'; nothing to diff against.",
                calculation_date,
                metric,
            )
            return RankingMembershipDiff(
                metric=metric,
                calculation_date=calculation_date,
                previous_date=None,
                entered=[],
                left=[],
            )

        result = await self.db.execute(
            _RANKING_MEMBERSHIP_SQL,
            {
                "metric": metric,
                "calculation_date": calculation_date,
                "previous_date": previous_date,
            },
        )
        rows = result.all()

        return RankingMembershipDiff(
            metric=metric,
            calculation_date=calculation_date,
            previous_date=previous_date,
            entered=[r.model_name for r in rows if not r.has_left],
            left=[r.model_name for r in rows if r.has_left],
        )


def build_participation_alert(
    findings: Sequence[ParticipationFinding],
    thresholds: ParticipationThresholds,
    as_of: datetime,
) -> Alert:
    """Render findings as one digest grouped by model.

    One message per run, not one per finding: a dead runner takes out every definition it
    was submitting to at once, and sixteen near-identical alerts would bury the signal it
    is meant to carry.
    """
    by_model: Dict[str, List[ParticipationFinding]] = {}
    for finding in findings:
        by_model.setdefault(finding.model_name, []).append(finding)

    silent_models = [
        name
        for name, items in by_model.items()
        if any(f.is_silent(thresholds) for f in items)
    ]

    title = (
        f"Participation drop: {len(findings)} model/challenge pair(s) across "
        f"{len(by_model)} model(s) as of {as_of:%Y-%m-%d %H:%M} UTC"
    )
    lines: List[str] = []
    if silent_models:
        lines.append(
            f"Gone quiet entirely on at least one challenge: {', '.join(sorted(silent_models))}"
        )
        lines.append("")

    for model_name in sorted(by_model):
        items = sorted(by_model[model_name], key=lambda f: -f.miss_streak)
        lines.append(f"{model_name}:")
        lines.extend(f"  - {item.describe(thresholds)}" for item in items)

    lines.append("")
    lines.append(
        f"Window: last {thresholds.recent_n} rounds vs the {thresholds.baseline_n} before them, "
        f"per challenge definition."
    )
    return Alert(title=title, lines=lines)


def build_ranking_membership_alert(diff: RankingMembershipDiff) -> Alert:
    """Render a membership change. Intended changes are reported too, by design."""
    title = (
        f"Global leaderboard membership changed [{diff.metric}]: "
        f"{len(diff.left)} left, {len(diff.entered)} entered "
        f"({diff.previous_date} -> {diff.calculation_date})"
    )
    lines: List[str] = []
    if diff.left:
        lines.append(f"Left ({len(diff.left)}): {', '.join(diff.left)}")
    if diff.entered:
        lines.append(f"Entered ({len(diff.entered)}): {', '.join(diff.entered)}")
    lines.append("")
    lines.append(
        "A deliberate change (an eligibility rule, a cancelled batch of rounds) shows up "
        "here too — confirm it was intended."
    )
    return Alert(title=title, lines=lines)


async def alert_on_ranking_membership_change(
    session: AsyncSession, metric: str, calculation_date: date
) -> Optional[RankingMembershipDiff]:
    """Run check 2 and alert if membership moved. Never raises.

    Called from the ranking write path, where a monitoring failure must not be allowed to
    fail the ranking calculation that triggered it.
    """
    try:
        service = MonitoringService(session)
        diff = await service.diff_global_ranking_membership(metric, calculation_date)
        if not diff.changed:
            logger.info(
                "Global ranking membership [%s] unchanged vs %s.", metric, diff.previous_date
            )
            return diff
        await send_alert_async(build_ranking_membership_alert(diff))
        return diff
    except Exception as exc:  # noqa: BLE001 - monitoring must never break the caller
        logger.error(
            "Ranking membership diff failed for metric '%s' on %s: %s",
            metric,
            calculation_date,
            exc,
            exc_info=True,
        )
        return None
