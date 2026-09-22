"""backend-95: the naive template must be a payload the upload endpoint accepts.

`Participation.md` tells newcomers to POST this template straight back as an end-to-end
smoke test. That is only true if its timestamps match what
`ForecastService._expected_forecast_timestamps` will demand, per series, and if it carries
the quantiles the SQL leaderboard gates on.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.schemas.forecast import ForecastUploadRequest
from app.services.challenge_service import ChallengeService
from app.services.forecast_service import ForecastService

FREQ = timedelta(hours=1)
HORIZON = timedelta(hours=3)
BASE = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)


def _ctx(name, last_ts, values):
    """A context series whose points are deliberately NOT in timestamp order."""
    points = [
        SimpleNamespace(ts=last_ts - (len(values) - 1 - i) * FREQ, value=v)
        for i, v in enumerate(values)
    ]
    return SimpleNamespace(
        challenge_series_name=name,
        frequency=FREQ,
        data=list(reversed(points)),
    )


def _service(context_series, start_time):
    svc = ChallengeService(db_session=AsyncMock())
    svc.round_repository.get_by_id = AsyncMock(
        return_value=SimpleNamespace(
            id=1, start_time=start_time, end_time=start_time + HORIZON,
            frequency=FREQ, horizon=HORIZON,
        )
    )
    svc.get_context_data_bulk = AsyncMock(return_value=context_series)
    return svc


@pytest.mark.asyncio
async def test_lagging_series_anchors_on_its_own_context_edge():
    """The defect: `start_time` is the round-wide max, so a lagging series got timestamps
    that do not exist for it. Measured on prod, most series on definitions 2 and 3 lag."""
    on_time = _ctx("on_time", BASE, [1.0, 2.0, 3.0])
    lagging = _ctx("lagging", BASE - 6 * FREQ, [10.0, 11.0, 12.0])
    template = await _service([on_time, lagging], start_time=BASE + FREQ).generate_naive_forecast_template(1)

    by_name = {s["challenge_series_name"]: s for s in template["forecasts"]}
    assert [f["ts"] for f in by_name["on_time"]["forecasts"]] == [
        BASE + FREQ, BASE + 2 * FREQ, BASE + 3 * FREQ
    ]
    assert [f["ts"] for f in by_name["lagging"]["forecasts"]] == [
        BASE - 5 * FREQ, BASE - 4 * FREQ, BASE - 3 * FREQ
    ]


@pytest.mark.asyncio
async def test_template_timestamps_equal_what_the_validator_expects():
    """The contract, asserted against the validator itself rather than a restatement."""
    series = _ctx("s", BASE, [1.0, 2.0, 3.0, 5.0])
    template = await _service([series], start_time=BASE + FREQ).generate_naive_forecast_template(1)

    expected = ForecastService._expected_forecast_timestamps(
        context_edge=BASE, frequency=FREQ, count=int(HORIZON / FREQ)
    )
    assert [f["ts"] for f in template["forecasts"][0]["forecasts"]] == expected


@pytest.mark.asyncio
async def test_point_count_matches_horizon_over_frequency():
    series = _ctx("s", BASE, [1.0, 2.0])
    template = await _service([series], start_time=BASE + FREQ).generate_naive_forecast_template(1)
    assert len(template["forecasts"][0]["forecasts"]) == int(HORIZON / FREQ) == 3


@pytest.mark.asyncio
async def test_template_carries_all_nine_canonical_quantiles():
    series = _ctx("s", BASE, [1.0, 3.0, 2.0, 6.0, 4.0])
    template = await _service([series], start_time=BASE + FREQ).generate_naive_forecast_template(1)

    for point in template["forecasts"][0]["forecasts"]:
        pv = point["probabilistic_values"]
        assert set(pv) == {f"q_0.{i}" for i in range(1, 10)}
        levels = [pv[f"q_0.{i}"] for i in range(1, 10)]
        assert levels == sorted(levels), "quantiles must not cross"


@pytest.mark.asyncio
async def test_template_survives_upload_request_validation():
    """It is declared as a ForecastUploadRequest; it has to actually be one, with its
    quantile keys surviving the tolerant cleaner unchanged."""
    series = _ctx("s", BASE, [1.0, 3.0, 2.0, 6.0])
    template = await _service([series], start_time=BASE + FREQ).generate_naive_forecast_template(1)

    request = ForecastUploadRequest(**template)
    point = request.forecasts[0].forecasts[0]
    assert set(point.probabilistic_values) == {f"q_0.{i}" for i in range(1, 10)}
    assert point.dropped_probabilistic_keys == []


@pytest.mark.asyncio
async def test_flat_series_yields_zero_width_band_not_a_crash():
    series = _ctx("flat", BASE, [7.0, 7.0, 7.0])
    template = await _service([series], start_time=BASE + FREQ).generate_naive_forecast_template(1)
    pv = template["forecasts"][0]["forecasts"][0]["probabilistic_values"]
    assert set(pv.values()) == {7.0}


def test_quantile_offsets_are_deterministic():
    values = [1.0, 4.0, 2.0, 9.0, 3.0]
    assert ChallengeService._naive_quantile_offsets(values) == \
           ChallengeService._naive_quantile_offsets(values)


def test_quantile_offsets_single_point_is_degenerate_not_an_error():
    assert set(ChallengeService._naive_quantile_offsets([5.0]).values()) == {0.0}
