from typing import Any
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlencode

import orjson
import pytest
from api.endpoints.historical import router
from fastapi import FastAPI
from redis.exceptions import RedisError
from starlette.types import Message


async def _get(app: FastAPI, params: dict[str, str]) -> tuple[int, Any]:
    messages: list[Message] = []

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        messages.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/historical/vehicles",
            "raw_path": b"/historical/vehicles",
            "query_string": urlencode(params).encode(),
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        },
        receive,
        send,
    )
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        m.get("body", b"") for m in messages if m["type"] == "http.response.body"
    )
    return status, orjson.loads(body)


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    monkeypatch.setattr("api.core.load_config", lambda: object())
    app = FastAPI()
    app.state.session = MagicMock()
    app.state.r_client = MagicMock(zrangebyscore=AsyncMock(return_value=[]))
    app.include_router(router)
    return app


@pytest.mark.anyio
@pytest.mark.parametrize(
    "start,end,detail",
    [
        (
            "2026-01-01T00:00:00",
            "2026-01-01T01:00:00Z",
            "start must include a timezone",
        ),
        ("2026-01-01T00:00:00Z", "2026-01-01T01:00:00", "end must include a timezone"),
        ("2026-01-01T01:00:00Z", "2026-01-01T00:00:00Z", None),
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", None),
        ("2026-01-01T00:00:00Z", "2026-01-01T01:00:01Z", None),
    ],
)
async def test_invalid_range_returns_bad_request(
    app: FastAPI, start: str, end: str, detail: str | None
) -> None:
    status, body = await _get(app, {"start": start, "end": end})

    assert status == 400
    assert body == {
        "detail": detail
        or "start must precede end and the range cannot exceed one hour"
    }
    app.state.r_client.zrangebyscore.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"start": "invalid", "end": "2026-01-01T01:00:00Z"},
    ],
)
async def test_invalid_query_returns_validation_response(
    app: FastAPI, params: dict[str, str]
) -> None:
    status, body = await _get(app, params)

    assert status == 422
    assert isinstance(body["detail"], list)
    app.state.r_client.zrangebyscore.assert_not_awaited()


@pytest.mark.anyio
async def test_valid_range_returns_snapshots(app: FastAPI) -> None:
    status, body = await _get(
        app, {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z"}
    )

    assert status == 200
    assert body == {"snapshots": []}
    app.state.r_client.zrangebyscore.assert_awaited_once()


@pytest.mark.anyio
async def test_redis_failure_returns_server_error(app: FastAPI) -> None:
    app.state.r_client.zrangebyscore.side_effect = RedisError("unavailable")
    status, body = await _get(
        app, {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z"}
    )

    assert status == 500
    assert body == {"detail": "Internal server error"}
