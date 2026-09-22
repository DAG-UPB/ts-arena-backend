"""ts-arena #20 subtask D: pin the context-data ordering external clients depend on.

`GET /challenge/rounds/{id}/context-data` is served from `get_context_data_bulk`. Every
copy of `ts-arena-participation_example` in the field reads the series' last context point
to anchor its forecast, and the reference model service takes it as `series[-1]` — array
order. Nothing in the API contract promised that order; it was an incidental `ORDER BY`.

Dropping or reordering that clause would shift every external participant's forecast at
once, silently. This test makes that a build failure instead.
"""
import re
from pathlib import Path

from sqlalchemy.dialects import postgresql

from app.database.challenges.challenge_repository import ChallengeRoundRepository

SOURCE = Path(ChallengeRoundRepository.__module__.replace(".", "/") + ".py")


def _compiled_query() -> str:
    """The SELECT `get_context_data_bulk` builds, as SQL.

    Taken from the method's own source so the assertion tracks the real query rather than a
    copy of it that could drift.
    """
    source = (Path(__file__).resolve().parents[1] / SOURCE).read_text()
    body = source.split("async def get_context_data_bulk", 1)[1]
    body = body.split("result = await self.session.execute(query)", 1)[0]
    return body


def test_context_data_is_ordered_by_timestamp_within_a_series():
    body = _compiled_query()
    order_by = re.search(r"\.order_by\(([^)]*)\)", body)
    assert order_by, "get_context_data_bulk must keep an explicit ORDER BY"

    clause = " ".join(order_by.group(1).split())
    assert "ChallengeContextData.ts" in clause, (
        "context points must stay ordered by ts within a series: external participant "
        "clients anchor their forecast on the last element in array order (ts-arena #20)"
    )
    # ts must be the last key, i.e. the within-series sort, not a leading one.
    keys = [k.strip() for k in clause.split(",") if k.strip()]
    assert keys[-1] == "ChallengeContextData.ts", (
        f"ts must be the final ORDER BY key so it sorts within each series, got {keys}"
    )


def test_ordering_is_ascending():
    """Descending order would put the OLDEST point last and invert every anchor."""
    body = _compiled_query()
    order_by = re.search(r"\.order_by\(([^)]*)\)", body)
    assert order_by, "get_context_data_bulk must keep an explicit ORDER BY"
    clause = " ".join(order_by.group(1).split())
    assert "desc" not in clause.lower(), "context ordering must stay ascending"
