from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
from api.core import DIParams
from api.endpoints.historical import get_historical_vehicles
from consts import HISTORICAL_V_INDEX
from fastapi import HTTPException, Request


@pytest.mark.anyio("asyncio")
@pytest.mark.parametrize("minutes", [60, 75, 90])
async def test_historical_vehicles_accepts_up_to_90_minutes(minutes: int) -> None:
    redis = MagicMock()
    redis.zrangebyscore = AsyncMock(return_value=[])
    start = datetime(2026, 11, 26, tzinfo=UTC)
    end = start + timedelta(minutes=minutes)

    response = await get_historical_vehicles(
        request=Request({"type": "http"}),
        commons=cast(DIParams, MagicMock(r_client=redis)),
        start=start,
        end=end,
    )

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"snapshots": []}
    redis.zrangebyscore.assert_awaited_once_with(
        HISTORICAL_V_INDEX, min=start.timestamp(), max=f"({end.timestamp()}"
    )


@pytest.mark.anyio("asyncio")
@pytest.mark.parametrize(
    "duration",
    [
        timedelta(minutes=90, microseconds=1),
        timedelta(hours=2),
        timedelta(0),
        timedelta(minutes=-1),
    ],
)
async def test_historical_vehicles_rejects_invalid_ranges(duration: timedelta) -> None:
    redis = MagicMock()
    redis.zrangebyscore = AsyncMock()
    start = datetime(2026, 11, 26, tzinfo=UTC)

    with pytest.raises(HTTPException) as error:
        await get_historical_vehicles(
            request=Request({"type": "http"}),
            commons=cast(DIParams, MagicMock(r_client=redis)),
            start=start,
            end=start + duration,
        )

    assert error.value.status_code == 400
    assert error.value.detail == (
        "start must precede end and the range cannot exceed 90 minutes"
    )
    redis.zrangebyscore.assert_not_awaited()


def test_historical_vehicles_documents_90_minute_limit() -> None:
    from api_server import create_app

    operation = create_app().openapi()["paths"]["/historical/vehicles"]["get"]

    assert "Request at most 90 minutes per call" in operation["description"]
