"""Forecasts stay hidden from the round-data routes until registration has closed.

`GET /challenge/rounds/{id}/data` and the monthly export both read through
`get_round_complete_data`. Serving forecasts while a round still accepts submissions
would let a participant copy the others before `registration_end`.
"""
from pathlib import Path

from app.database.challenges.challenge_repository import ChallengeRoundRepository

SOURCE = Path(ChallengeRoundRepository.__module__.replace(".", "/") + ".py")


def _query() -> str:
    source = (Path(__file__).resolve().parents[1] / SOURCE).read_text()
    body = source.split("async def get_round_complete_data", 1)[1]
    body = body.split("result = await self.session.execute(sql", 1)[0]
    return " ".join(body.split())


def _forecast_subquery(sql: str) -> str:
    return sql.split("as actuals,", 1)[1].split("as forecasts", 1)[0]


def test_round_info_carries_registration_end():
    round_info = _query().split("WITH round_info AS (", 1)[1].split("FROM challenges.rounds r", 1)[0]
    assert "r.registration_end" in round_info


def test_forecasts_are_gated_on_registration_end():
    forecasts = _forecast_subquery(_query())
    assert "ri.registration_end <= NOW()" in forecasts


def test_context_and_actuals_are_not_gated():
    sql = _query()
    before_forecasts = sql.split("as actuals,", 1)[0]
    assert "registration_end <= NOW()" not in before_forecasts
