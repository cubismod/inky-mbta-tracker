import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from alert_stream import handle_alert_stream_event
from config import Config


def _alert_json(alert_id: str = "alert-1") -> str:
    return json.dumps(
        {
            "type": "alert",
            "id": alert_id,
            "attributes": {
                "cause": "UNKNOWN_CAUSE",
                "created_at": "2026-01-01T12:00:00+00:00",
                "header": "Test alert",
                "short_header": "Test",
                "updated_at": "2026-01-01T12:05:00+00:00",
                "active_period": [],
                "informed_entity": [
                    {"route": "Red"},
                    {"trip": "trip-1"},
                ],
                "severity": 7,
            },
        }
    )


@pytest.mark.anyio("asyncio")
async def test_handle_alert_stream_add_stores_alert_and_memberships() -> None:
    r_client = AsyncMock()
    tg = AsyncMock()
    call_order = Mock()

    with (
        patch("alert_stream.write_cache", new=AsyncMock()) as mock_write_cache,
        patch("alert_stream.process_alert_event", new=AsyncMock()) as mock_process,
    ):
        call_order.attach_mock(mock_write_cache, "write_cache")
        call_order.attach_mock(mock_process, "process_alert_event")

        await handle_alert_stream_event(
            _alert_json(),
            "add",
            r_client,
            Config(stops=[]),
            tg,
            route="Red",
        )

    assert call_order.mock_calls[0][0] == "write_cache"
    assert call_order.mock_calls[1][0] == "process_alert_event"
    r_client.sadd.assert_any_await("alerts:route:Red", "alert-1")
    r_client.sadd.assert_any_await("alerts:trip:trip-1", "alert-1")
    r_client.expire.assert_awaited_once_with("alerts:trip:trip-1", 604800)


@pytest.mark.anyio("asyncio")
async def test_handle_alert_stream_reset_clears_route_and_processes_each_alert() -> None:
    r_client = AsyncMock()
    tg = AsyncMock()
    alerts = f"[{_alert_json('alert-a')}, {_alert_json('alert-b')}]"

    with (
        patch("alert_stream.write_cache", new=AsyncMock()) as mock_write_cache,
        patch("alert_stream.process_alert_event", new=AsyncMock()) as mock_process,
    ):
        await handle_alert_stream_event(
            alerts,
            "reset",
            r_client,
            Config(stops=[]),
            tg,
            route="Orange",
        )

    r_client.delete.assert_awaited_once_with("alerts:route:Orange")
    assert mock_process.await_count == 2
    assert mock_write_cache.await_count == 2
    r_client.sadd.assert_any_await("alerts:route:Orange", "alert-a")
    r_client.sadd.assert_any_await("alerts:route:Orange", "alert-b")


@pytest.mark.anyio("asyncio")
async def test_handle_alert_stream_remove_deletes_cached_alert_memberships() -> None:
    r_client = AsyncMock()
    tg = AsyncMock()

    with (
        patch("alert_stream.get_cache", new=AsyncMock(return_value=_alert_json())),
        patch("alert_stream.delete_webhook", new=AsyncMock()) as mock_delete_webhook,
    ):
        await handle_alert_stream_event(
            json.dumps({"type": "alert", "id": "alert-1"}),
            "remove",
            r_client,
            Config(stops=[]),
            tg,
        )

    mock_delete_webhook.assert_awaited_once_with("alert-1", r_client)
    r_client.srem.assert_any_await("alerts:route:Red", "alert-1")
    r_client.srem.assert_any_await("alerts:trip:trip-1", "alert-1")
    r_client.expire.assert_awaited_once_with("alerts:trip:trip-1", 604800)
    r_client.delete.assert_awaited_once_with("alert:alert-1")
