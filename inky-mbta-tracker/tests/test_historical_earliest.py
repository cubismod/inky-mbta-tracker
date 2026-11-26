from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
from api.core import DIParams
from api.endpoints.historical import get_earliest_historical_vehicle_timestamp
from api.services.historical import fetch_earliest_historical_vehicle_timestamp
from consts import HISTORICAL_V_DATA
from fastapi import HTTPException, Request
from redis.asyncio import Redis
from redis.exceptions import RedisError


@pytest.mark.anyio("asyncio")
@pytest.mark.parametrize(
    "fields,expected",
    [
        ([b"10000.0", b"999.125", b"2000.0"], 999.125),
        (["10000.0", "999.125", "2000.0"], 999.125),
        ([b"0.0"], 0.0),
        ([], None),
    ],
)
async def test_earliest_uses_retained_hash_timestamps(
    fields: list[str | bytes], expected: float | None
) -> None:
    redis = MagicMock()
    redis.hkeys = AsyncMock(return_value=fields)
    # Expired snapshots may still have entries in the time index.
    redis.zrange = AsyncMock(return_value=[b"1.0"])

    result = await fetch_earliest_historical_vehicle_timestamp(cast(Redis, redis))

    assert result.earliest_timestamp == (
        datetime.fromtimestamp(expected, tz=UTC) if expected is not None else None
    )
    redis.hkeys.assert_awaited_once_with(HISTORICAL_V_DATA)
    redis.zrange.assert_not_awaited()
    redis.hgetall.assert_not_called()


@pytest.mark.anyio("asyncio")
@pytest.mark.parametrize(
    "fields,expected",
    [
        ([b"3600.125"], "1970-01-01T01:00:00.125000Z"),
        ([], None),
    ],
)
async def test_earliest_endpoint_json(
    fields: list[bytes], expected: str | None
) -> None:
    redis = MagicMock()
    redis.hkeys = AsyncMock(return_value=fields)
    response = await get_earliest_historical_vehicle_timestamp(
        request=Request({"type": "http"}),
        commons=cast(DIParams, MagicMock(r_client=redis)),
    )

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"earliest_timestamp": expected}
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.anyio("asyncio")
async def test_earliest_endpoint_redis_failure() -> None:
    redis = MagicMock()
    redis.hkeys = AsyncMock(side_effect=RedisError("unavailable"))
    with pytest.raises(HTTPException) as error:
        await get_earliest_historical_vehicle_timestamp(
            request=Request({"type": "http"}),
            commons=cast(DIParams, MagicMock(r_client=redis)),
        )

    assert error.value.status_code == 500
    assert error.value.detail == "Internal server error"


def test_earliest_route_registered_in_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IMT_RATE_LIMITING_ENABLED", "false")
    from api_server import create_app

    operation = create_app().openapi()["paths"]["/historical/vehicles/earliest"]["get"]

    assert "parameters" not in operation
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/HistoricalVehiclesEarliestResponse"
    }
