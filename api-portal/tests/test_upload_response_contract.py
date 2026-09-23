"""backend-94: the upload response has to let a participant verify what we stored.

Before this, a client could not tell a whole upload from a partial one: `success` is
"anything landed", `forecasts_inserted` was a single count with no point/probabilistic
split, the resolved `model_id` was never returned, and `errors` mixed genuine rejections
with advisories. These tests pin the contract that fixes that.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.schemas.forecast import (
    ForecastDataPoint,
    ForecastSeriesUpload,
    ForecastUploadRequest,
)
from app.services.forecast_service import ForecastService

NOW = datetime.now(timezone.utc)
FREQ = timedelta(hours=1)
HORIZON = timedelta(hours=3)
MODEL_ID = 4711
FULL_Q = {f"q_0.{i}": float(i) for i in range(1, 10)}


def _service(inserted=(3, 3), series_id=7):
    """A ForecastService with every collaborator stubbed.

    `bulk_create_forecasts` returns (rows, of-which-probabilistic) — the tuple backend-94
    introduced so the probabilistic count comes from what the INSERT actually stored.
    """
    svc = ForecastService(db_session=AsyncMock())
    svc.model_repo.get_by_name_and_user = AsyncMock(
        return_value=SimpleNamespace(id=MODEL_ID, name="TestModel")
    )
    svc.challenge_repo.get_by_id = AsyncMock(
        return_value=SimpleNamespace(
            registration_start=(NOW - timedelta(hours=1)).replace(tzinfo=None),
            registration_end=(NOW + timedelta(hours=1)).replace(tzinfo=None),
            horizon=HORIZON,
            frequency=FREQ,
        )
    )
    # No context edge -> timestamp validation is skipped, keeping these tests about the
    # response contract rather than about backend-87's window rules.
    svc.challenge_repo.get_series_context_edges = AsyncMock(return_value={})
    svc.forecast_repo.bulk_create_forecasts = AsyncMock(return_value=inserted)
    svc._auto_register_participant = AsyncMock()
    svc._create_initial_score_entry = AsyncMock()
    svc._resolve_series_id = AsyncMock(return_value=series_id)
    return svc


def _request(points, series_name="series_a"):
    return ForecastUploadRequest(
        round_id=1,
        model_name="TestModel",
        forecasts=[ForecastSeriesUpload(challenge_series_name=series_name, forecasts=points)],
    )


def _points(n=3, pv=None):
    return [
        ForecastDataPoint(ts=NOW + i * FREQ, value=float(i), probabilistic_values=pv)
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_response_carries_the_resolved_model_id():
    """The id the readback endpoint needs, returned by the call that resolved it."""
    resp = await _service().upload_forecasts(_request(_points(pv=FULL_Q)), user_id=1)
    assert resp.model_id == MODEL_ID


@pytest.mark.asyncio
async def test_probabilistic_points_counted_separately():
    resp = await _service(inserted=(3, 3)).upload_forecasts(
        _request(_points(pv=FULL_Q)), user_id=1
    )
    assert resp.points_inserted == 3
    assert resp.probabilistic_points_inserted == 3
    assert resp.forecasts_inserted == 3  # legacy field unchanged


@pytest.mark.asyncio
async def test_point_only_upload_reports_zero_probabilistic():
    """The case the reporting participant could not distinguish from success."""
    resp = await _service(inserted=(3, 0)).upload_forecasts(_request(_points()), user_id=1)
    assert resp.success is True
    assert resp.points_inserted == 3
    assert resp.probabilistic_points_inserted == 0


@pytest.mark.asyncio
async def test_unknown_series_is_a_hard_error_not_a_warning():
    svc = _service()
    svc._resolve_series_id = AsyncMock(return_value=None)
    resp = await svc.upload_forecasts(_request(_points(pv=FULL_Q)), user_id=1)
    assert resp.warnings == []
    assert len(resp.errors) == 1
    assert "Unknown challenge_series_name" in resp.errors[0]
    assert resp.success is False


@pytest.mark.asyncio
async def test_dropped_keys_are_a_warning_and_still_in_errors():
    """Advisory: the point was accepted, some keys were not. Must not read as a rejection,
    but must stay in `errors` so pre-backend-94 clients see it exactly as before."""
    resp = await _service().upload_forecasts(
        _request(_points(pv={"q_0.1": 1.0, "median": 2.0})), user_id=1
    )
    assert len(resp.warnings) == 1
    assert "dropped unrecognised" in resp.warnings[0]
    assert resp.warnings[0] in resp.errors
    # nothing fatal happened
    assert set(resp.errors) - set(resp.warnings) == set()
    assert resp.success is True


@pytest.mark.asyncio
async def test_repaired_crossings_are_a_warning():
    crossed = {"q_0.1": 9.0, "q_0.2": 8.0, "q_0.3": 7.0, "q_0.4": 6.0, "q_0.5": 5.0,
               "q_0.6": 4.0, "q_0.7": 3.0, "q_0.8": 2.0, "q_0.9": 1.0}
    resp = await _service().upload_forecasts(_request(_points(pv=crossed)), user_id=1)
    assert any("repaired quantile crossings" in w for w in resp.warnings)
    assert set(resp.errors) - set(resp.warnings) == set()


@pytest.mark.asyncio
async def test_wrong_point_count_is_fatal_and_separable():
    """A client must be able to fail on this without also failing on advisories."""
    resp = await _service().upload_forecasts(_request(_points(n=2, pv=FULL_Q)), user_id=1)
    fatal = set(resp.errors) - set(resp.warnings)
    assert len(fatal) == 1
    assert "Invalid forecast count" in fatal.pop()


@pytest.mark.asyncio
async def test_message_reports_quantiles_and_both_severities():
    svc = _service()
    resp = await svc.upload_forecasts(
        _request(_points(pv={"q_0.1": 1.0, "nonsense": 2.0})), user_id=1
    )
    assert "with quantiles" in resp.message
    assert "warning(s)" in resp.message
    assert "error(s)" not in resp.message


# --- backend-97: the per-series timestamp window, exercised through the upload itself ---
#
# The helpers above stub the context edge away. These tests give each series a real edge, so
# the check that decides whether a participant's upload lands is covered end to end.

EDGE = NOW - FREQ  # last context point; an honest forecast starts one step later, at NOW


def _windowed_service(edges, names_to_ids, inserted=(3, 0)):
    svc = _service(inserted=inserted)
    svc.challenge_repo.get_series_context_edges = AsyncMock(return_value=edges)
    svc._resolve_series_id = AsyncMock(side_effect=lambda _round, name: names_to_ids.get(name))
    return svc


def _shifted_points(steps):
    """Three points starting `steps` frequency steps away from the honest first point."""
    return [
        ForecastDataPoint(ts=NOW + (i + steps) * FREQ, value=float(i)) for i in range(3)
    ]


@pytest.mark.asyncio
async def test_window_after_the_series_edge_is_accepted():
    svc = _windowed_service({7: EDGE}, {"series_a": 7})
    resp = await svc.upload_forecasts(_request(_points()), user_id=1)
    assert resp.success is True
    assert set(resp.errors) - set(resp.warnings) == set()
    svc.forecast_repo.bulk_create_forecasts.assert_awaited_once()


@pytest.mark.asyncio
async def test_lagging_series_is_accepted_on_its_own_edge():
    """A series one step behind the round's other series starts one step earlier. Anchoring
    on the round-wide `start_time` would reject it; the per-series edge does not."""
    svc = _windowed_service({7: EDGE - FREQ}, {"series_a": 7})
    resp = await svc.upload_forecasts(_request(_shifted_points(-1)), user_id=1)
    assert resp.success is True
    assert set(resp.errors) - set(resp.warnings) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("steps", [-1, 1], ids=["backdated_into_context", "shifted_later"])
async def test_window_off_the_series_edge_is_rejected_with_the_accepted_range(steps):
    svc = _windowed_service({7: EDGE}, {"series_a": 7})
    resp = await svc.upload_forecasts(_request(_shifted_points(steps)), user_id=1)
    assert resp.success is False
    svc.forecast_repo.bulk_create_forecasts.assert_not_awaited()
    fatal = set(resp.errors) - set(resp.warnings)
    assert len(fatal) == 1
    msg = fatal.pop()
    assert "Invalid forecast timestamps for series 'series_a'" in msg
    # The message names the range that would have been accepted, so the fix is actionable.
    assert f"from {NOW.isoformat()} to {(NOW + 2 * FREQ).isoformat()}" in msg


@pytest.mark.asyncio
async def test_duplicate_timestamp_masking_a_missing_one_is_rejected():
    """Right count, wrong set: the count check alone would let this through."""
    svc = _windowed_service({7: EDGE}, {"series_a": 7})
    points = [ForecastDataPoint(ts=NOW + i * FREQ, value=0.0) for i in (0, 1, 1)]
    resp = await svc.upload_forecasts(_request(points), user_id=1)
    assert resp.success is False
    assert any("Invalid forecast timestamps" in e for e in resp.errors)


@pytest.mark.asyncio
async def test_one_bad_series_does_not_sink_the_rest_of_the_upload():
    svc = _windowed_service({7: EDGE, 8: EDGE}, {"series_a": 7, "series_b": 8})
    req = ForecastUploadRequest(
        round_id=1,
        model_name="TestModel",
        forecasts=[
            ForecastSeriesUpload(challenge_series_name="series_a", forecasts=_points()),
            ForecastSeriesUpload(challenge_series_name="series_b", forecasts=_shifted_points(1)),
        ],
    )
    resp = await svc.upload_forecasts(req, user_id=1)
    assert resp.success is True
    fatal = set(resp.errors) - set(resp.warnings)
    assert len(fatal) == 1 and "'series_b'" in fatal.pop()
    svc.forecast_repo.bulk_create_forecasts.assert_awaited_once()
