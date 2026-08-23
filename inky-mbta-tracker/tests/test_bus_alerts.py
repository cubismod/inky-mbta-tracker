import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from config import Config
from mbta_client import MBTAApi
from mbta_responses import ActivePeriod, AlertAttributes, AlertResource, InformedEntity
from redis.asyncio.client import Redis as RedisClient
from shared_types.shared_types import TaskType


def make_alert_resource(
    alert_id: str,
    routes: list[str],
    updated_at: str = "2026-06-27T12:00:00-04:00",
) -> AlertResource:
    return AlertResource(
        type="alert",
        id=alert_id,
        attributes=AlertAttributes(
            cause="UNKNOWN_CAUSE",
            created_at="2026-06-27T12:00:00-04:00",
            header="Bus delay",
            short_header="Delay",
            updated_at=updated_at,
            active_period=[ActivePeriod(start=None, end=None)],
            informed_entity=[
                InformedEntity(route=route, route_type=3) for route in routes
            ],
            severity=5,
        ),
    )


def test_mbtapi_stores_route_type() -> None:
    api = MBTAApi(
        cast(RedisClient, MagicMock()),
        watcher_type=TaskType.ALERTS,
        route_type=3,
    )
    assert api.route_type == 3


@pytest.mark.anyio("asyncio")
async def test_save_alert_memberships_writes_registry_for_route_type() -> None:
    r_client = AsyncMock()
    api = MBTAApi(
        cast(RedisClient, r_client),
        watcher_type=TaskType.ALERTS,
        route_type=3,
    )
    alert = make_alert_resource("A1", routes=["77", "73"])

    await api._save_alert_memberships(alert)

    assert any(
        call.args == ("alerts:route:77", "A1") for call in r_client.sadd.await_args_list
    )
    assert any(
        call.args == ("alerts:route:73", "A1") for call in r_client.sadd.await_args_list
    )
    assert any(
        call.args == ("alerts:route_type:3", "77")
        for call in r_client.sadd.await_args_list
    )
    assert any(
        call.args == ("alerts:route_type:3", "73")
        for call in r_client.sadd.await_args_list
    )


@pytest.mark.anyio("asyncio")
async def test_alerts_reset_clears_registry_backed_route_sets() -> None:
    r_client = AsyncMock()
    r_client.hexists.return_value = False
    r_client.smembers.return_value = {b"77", b"73"}
    api = MBTAApi(
        cast(RedisClient, r_client),
        watcher_type=TaskType.ALERTS,
        route_type=3,
    )
    reset_body = json.dumps(
        [
            {
                "type": "alert",
                "id": "A1",
                "attributes": make_alert_resource("A1", ["77"]).attributes.model_dump(),
            }
        ]
    )
    tg = MagicMock()

    await api.parse_live_api_response(
        reset_body, "reset", 0, MagicMock(closed=False), tg, Config()
    )

    assert any(
        call.args == ("alerts:route:77",) for call in r_client.delete.await_args_list
    )
    assert any(
        call.args == ("alerts:route:73",) for call in r_client.delete.await_args_list
    )


@pytest.mark.anyio("asyncio")
async def test_process_alert_item_skips_webhook_for_route_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_alert_event = AsyncMock()
    monkeypatch.setattr("mbta_client.process_alert_event", process_alert_event)
    r_client = AsyncMock()
    api = MBTAApi(
        cast(RedisClient, r_client),
        watcher_type=TaskType.ALERTS,
        route_type=3,
    )
    alert = make_alert_resource("A1", routes=["77"])

    await api._process_alert_item(alert, "add", Config())

    process_alert_event.assert_not_awaited()
