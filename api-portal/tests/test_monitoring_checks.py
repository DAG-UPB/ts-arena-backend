"""Unit tests for the backend #92 monitoring checks.

Covers the parts that decide *whether* an alert goes out and *what it says* — the
thresholds, the digest format and the never-raise contract. The two SQL queries are
exercised against the dev database in the issue's replay, not here; these tests run
against fake sessions, no DB.
"""
import asyncio
import logging
from datetime import date, datetime, timezone

import pytest

from app.core.alerts import Alert, MAX_ALERT_CHARS, send_alert
from app.services.monitoring_service import (
    MonitoringService,
    ParticipationFinding,
    ParticipationThresholds,
    RankingMembershipDiff,
    alert_on_ranking_membership_change,
    build_participation_alert,
    build_ranking_membership_alert,
)


# --- alert channel ---------------------------------------------------------------


def test_alert_renders_title_and_body():
    alert = Alert(title="Something moved", lines=["a", "b"], source="api-portal")
    assert alert.render() == "[api-portal] Something moved\na\nb"


def test_alert_render_truncates_oversized_body():
    alert = Alert(title="Flood", lines=["x" * 10_000])
    rendered = alert.render()
    assert len(rendered) == MAX_ALERT_CHARS
    assert rendered.endswith("...")


def test_send_alert_without_webhook_logs_and_reports_undelivered(monkeypatch, caplog):
    """No ALERT_WEBHOOK_URL (the state until ts-arena #7 lands) must still surface the text."""
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    with caplog.at_level(logging.WARNING):
        delivered = send_alert(Alert(title="Model left", lines=["MLForecast"]))

    assert delivered is False
    assert any("MLForecast" in record.getMessage() for record in caplog.records)


def test_send_alert_never_raises_when_delivery_fails(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://alerts.invalid/hook")

    def _boom(url, payload, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr("app.core.alerts._post", _boom)
    assert send_alert(Alert(title="t")) is False


def test_send_alert_posts_rendered_text(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://alerts.invalid/hook")
    sent = {}

    def _capture(url, payload, timeout):
        sent["url"] = url
        sent["payload"] = payload

    monkeypatch.setattr("app.core.alerts._post", _capture)
    assert send_alert(Alert(title="Model left", lines=["MLForecast"])) is True
    assert sent["url"] == "http://alerts.invalid/hook"
    assert "MLForecast" in sent["payload"]["text"]


# --- participation thresholds ----------------------------------------------------


def _finding(**overrides) -> ParticipationFinding:
    base = dict(
        model_id=12,
        model_name="MLForecast",
        definition_id=1,
        definition_name="smard_dam_challenge_24h_15min",
        miss_streak=0,
        recent_reg=7,
        recent_rounds=7,
        baseline_reg=22,
        baseline_rounds=28,
    )
    base.update(overrides)
    return ParticipationFinding(**base)


def test_rates_are_computed_from_counts():
    finding = _finding(recent_reg=1, recent_rounds=7, baseline_reg=21, baseline_rounds=28)
    assert finding.recent_rate == pytest.approx(1 / 7)
    assert finding.baseline_rate == pytest.approx(0.75)


def test_rates_do_not_divide_by_zero():
    finding = _finding(recent_reg=0, recent_rounds=0, baseline_reg=0, baseline_rounds=0)
    assert finding.recent_rate == 0.0
    assert finding.baseline_rate == 0.0


def test_silence_rule_uses_the_streak_threshold():
    thresholds = ParticipationThresholds(silence_streak=5)
    assert _finding(miss_streak=5).is_silent(thresholds) is True
    assert _finding(miss_streak=4).is_silent(thresholds) is False


def test_describe_distinguishes_silence_from_a_partial_drop():
    thresholds = ParticipationThresholds(silence_streak=5)
    silent = _finding(miss_streak=7, recent_reg=0, recent_rounds=7).describe(thresholds)
    dropped = _finding(miss_streak=2, recent_reg=1, recent_rounds=7).describe(thresholds)

    assert "silent for the last 7 rounds" in silent
    assert "1/7 recent rounds" in dropped
    # Both carry the comparison against the model's own baseline.
    assert "79% baseline" in silent and "79% baseline" in dropped


def test_thresholds_from_env_override_defaults(monkeypatch):
    monkeypatch.setenv("PARTICIPATION_SILENCE_STREAK", "3")
    monkeypatch.setenv("PARTICIPATION_DROP_RATIO", "0.2")
    thresholds = ParticipationThresholds.from_env()
    assert thresholds.silence_streak == 3
    assert thresholds.drop_ratio == pytest.approx(0.2)
    assert thresholds.recent_n == ParticipationThresholds.recent_n  # untouched


def test_thresholds_from_env_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("PARTICIPATION_SILENCE_STREAK", "not-a-number")
    assert ParticipationThresholds.from_env().silence_streak == (
        ParticipationThresholds.silence_streak
    )


# --- participation digest --------------------------------------------------------


def test_participation_digest_groups_by_model():
    """A dead runner hits every definition at once; the digest must read as one outage."""
    thresholds = ParticipationThresholds(silence_streak=5)
    findings = [
        _finding(model_name="MLForecast", definition_name="smard_dam", miss_streak=7,
                 recent_reg=0, recent_rounds=7),
        _finding(model_name="MLForecast", definition_name="fingrid_24h", miss_streak=6,
                 recent_reg=0, recent_rounds=7),
        _finding(model_name="Prophet", definition_name="smard_dam", miss_streak=7,
                 recent_reg=0, recent_rounds=7),
    ]
    alert = build_participation_alert(
        findings, thresholds, datetime(2026, 9, 8, 4, 30, tzinfo=timezone.utc)
    )
    text = alert.render()

    assert "3 model/challenge pair(s) across 2 model(s)" in alert.title
    assert "2026-09-08 04:30 UTC" in alert.title
    assert "Gone quiet entirely on at least one challenge: MLForecast, Prophet" in text
    # One heading per model, definitions nested underneath it.
    assert text.count("MLForecast:") == 1
    assert "  - smard_dam:" in text
    assert "  - fingrid_24h:" in text


def test_participation_digest_reports_the_window_used():
    thresholds = ParticipationThresholds(recent_n=7, baseline_n=28)
    alert = build_participation_alert(
        [_finding(miss_streak=6)], thresholds, datetime(2026, 9, 8, tzinfo=timezone.utc)
    )
    assert "last 7 rounds vs the 28 before them" in alert.render()


# --- ranking membership ----------------------------------------------------------


def test_membership_diff_reports_no_change_when_both_sides_empty():
    diff = RankingMembershipDiff(
        metric="mase", calculation_date=date(2026, 9, 11),
        previous_date=date(2026, 9, 10), entered=[], left=[],
    )
    assert diff.changed is False


def test_membership_alert_names_models_and_direction():
    diff = RankingMembershipDiff(
        metric="mase",
        calculation_date=date(2026, 9, 11),
        previous_date=date(2026, 9, 10),
        entered=["Chronos-2"],
        left=["AutoARIMA", "AutoETS", "MLForecast"],
    )
    alert = build_ranking_membership_alert(diff)
    text = alert.render()

    assert "[mase]" in alert.title
    assert "3 left, 1 entered" in alert.title
    assert "2026-09-10 -> 2026-09-11" in alert.title
    assert "Left (3): AutoARIMA, AutoETS, MLForecast" in text
    assert "Entered (1): Chronos-2" in text
    # Intended changes are reported too — the alert says so, so nobody treats it as a bug.
    assert "deliberate change" in text


class _FakeResultRow:
    def __init__(self, has_left, model_name):
        self.has_left = has_left
        self.model_name = model_name


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    """Answers the two queries the diff makes: previous date, then the membership rows."""

    def __init__(self, previous_date, rows):
        self._previous_date = previous_date
        self._rows = rows

    async def scalar(self, query, params=None):
        return self._previous_date

    async def execute(self, query, params=None):
        return _FakeResult(self._rows)


def test_first_ever_calculation_does_not_announce_the_whole_leaderboard():
    service = MonitoringService(_FakeSession(previous_date=None, rows=[]))
    diff = asyncio.run(service.diff_global_ranking_membership("mase", date(2026, 9, 11)))

    assert diff.previous_date is None
    assert diff.changed is False


def test_diff_splits_rows_into_entered_and_left():
    rows = [
        _FakeResultRow(has_left=False, model_name="Chronos-2"),
        _FakeResultRow(has_left=True, model_name="MLForecast"),
    ]
    service = MonitoringService(_FakeSession(previous_date=date(2026, 9, 10), rows=rows))
    diff = asyncio.run(service.diff_global_ranking_membership("sql", date(2026, 9, 11)))

    assert diff.entered == ["Chronos-2"]
    assert diff.left == ["MLForecast"]
    assert diff.changed is True


# --- the never-break-the-caller contract -----------------------------------------


class _ExplodingSession:
    async def scalar(self, query, params=None):
        raise RuntimeError("database is on fire")


def test_membership_check_swallows_its_own_failure():
    """It is wired into the ELO write path; a monitoring bug must not fail the ranking."""
    result = asyncio.run(
        alert_on_ranking_membership_change(
            _ExplodingSession(), "mase", date(2026, 9, 11)
        )
    )
    assert result is None


def test_membership_check_is_silent_when_membership_is_unchanged(monkeypatch):
    sent = []

    async def _capture(alert):
        sent.append(alert)
        return True

    monkeypatch.setattr("app.services.monitoring_service.send_alert_async", _capture)
    session = _FakeSession(previous_date=date(2026, 9, 10), rows=[])
    diff = asyncio.run(alert_on_ranking_membership_change(session, "mase", date(2026, 9, 11)))

    assert diff is not None and diff.changed is False
    assert sent == []


def test_membership_check_alerts_when_membership_moved(monkeypatch):
    sent = []

    async def _capture(alert):
        sent.append(alert)
        return True

    monkeypatch.setattr("app.services.monitoring_service.send_alert_async", _capture)
    session = _FakeSession(
        previous_date=date(2026, 9, 10),
        rows=[_FakeResultRow(has_left=True, model_name="MLForecast")],
    )
    diff = asyncio.run(alert_on_ranking_membership_change(session, "mase", date(2026, 9, 11)))

    assert diff is not None and diff.changed is True
    assert len(sent) == 1
    assert "MLForecast" in sent[0].render()
