import json
from typing import Any, cast

import pytest
from aiohttp import ClientSession
from api.services.alerts import AlertsResult, fetch_alerts_with_retry
from config import Config
from fastapi.routing import APIRoute
from geojson_utils import collect_alerts
from redis.asyncio import Redis


def make_alert(
    alert_id: str,
    updated_at: str,
    created_at: str | None = "2026-01-01T00:00:00-04:00",
    header: str = "Service delay",
    **extra_attrs: Any,
) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        "timeframe": None,
        "image_alternative_text": None,
        "cause": "UNKNOWN_CAUSE",
        "image": None,
        "created_at": created_at,
        "banner": None,
        "header": header,
        "url": None,
        "short_header": "Delay",
        "effect": "DELAY",
        "updated_at": updated_at,
        "effect_name": None,
        "active_period": [{"start": None, "end": None}],
        "informed_entity": [{"route": "Red"}],
        "severity": 5,
    }
    attrs.update(extra_attrs)
    return {"type": "alert", "id": alert_id, "attributes": attrs}


class FakeSession:
    closed = False


def patch_batches(
    monkeypatch: pytest.MonkeyPatch, batches: list[list[dict[str, Any]]]
) -> list[str]:
    calls: list[str] = []

    async def fake_batch(
        routes_str: str, session: ClientSession, r_client: Redis
    ) -> list[dict[str, Any]] | None:
        calls.append(routes_str)
        idx = len(calls) - 1
        return batches[idx] if idx < len(batches) else []

    monkeypatch.setattr("geojson_utils.light_get_alerts_batch", fake_batch)
    return calls


@pytest.mark.anyio("asyncio")
async def test_collect_alerts_returns_serialized_body_and_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older = make_alert("A1", updated_at="2026-01-01T00:00:00-04:00")
    newer = make_alert("A2", updated_at="2026-06-27T12:00:00-04:00")
    duplicate = make_alert("A1", updated_at="2026-01-01T00:00:00-04:00")

    # Both routes fit in a single batch (batch_size == 10).
    calls = patch_batches(monkeypatch, [[older, newer, duplicate]])

    count, body = await collect_alerts(
        Config(vehicles_by_route=["Red", "Blue"]),
        cast(ClientSession, FakeSession()),
        cast(Redis, object()),
    )

    assert calls == ["Red,Blue"]
    assert count == 2
    data = json.loads(body)
    assert list(data.keys()) == ["data"]
    ids = [a["id"] for a in data["data"]]
    # duplicate A1 collapsed; newer A2 sorted first by updated_at desc
    assert ids == ["A2", "A1"]


@pytest.mark.anyio("asyncio")
async def test_collect_alerts_preserves_raw_mbta_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alert = make_alert(
        "A1",
        updated_at="2026-06-27T12:00:00-04:00",
        service_effect_text="Unknown to the pydantic model",
    )

    patch_batches(monkeypatch, [[alert]])

    _, body = await collect_alerts(
        Config(vehicles_by_route=["Red"]),
        cast(ClientSession, FakeSession()),
        cast(Redis, object()),
    )

    payload = json.loads(body)
    assert payload["data"][0]["attributes"]["service_effect_text"] == (
        "Unknown to the pydantic model"
    )


@pytest.mark.anyio("asyncio")
async def test_collect_alerts_no_routes_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_batches(monkeypatch, [])

    count, body = await collect_alerts(
        Config(vehicles_by_route=None),
        cast(ClientSession, FakeSession()),
        cast(Redis, object()),
    )

    assert count == 0
    assert json.loads(body) == {"data": []}


@pytest.mark.anyio("asyncio")
async def test_fetch_alerts_with_retry_returns_alerts_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alert = make_alert("A1", updated_at="2026-06-27T12:00:00-04:00")
    patch_batches(monkeypatch, [[alert]])

    result = await fetch_alerts_with_retry(
        Config(vehicles_by_route=["Red"]),
        cast(ClientSession, FakeSession()),
        cast(Redis, object()),
    )

    assert isinstance(result, AlertsResult)
    assert result.count == 1
    assert json.loads(result.body)["data"][0]["id"] == "A1"


def test_api_server_registers_alerts_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IMT_RATE_LIMITING_ENABLED", "false")

    from api_server import create_app

    app = create_app()

    assert any(
        route.path == "/alerts" for route in app.routes if isinstance(route, APIRoute)
    )
