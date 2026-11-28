import types
from unittest.mock import AsyncMock

import pytest
from api.core import DIParams, get_di
from fastapi import HTTPException


class _FakeApp:
    def __init__(self, session: object, r_client: object) -> None:
        self.state = types.SimpleNamespace(session=session, r_client=r_client)


class _FakeRequest:
    def __init__(self, session: object, r_client: object) -> None:
        self.app = _FakeApp(session, r_client)


@pytest.mark.anyio("asyncio")
async def test_get_di_reuses_shared_redis_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = AsyncMock()
    request = _FakeRequest(session=object(), r_client=shared)
    monkeypatch.setattr("api.core.load_config", lambda: object())

    seen = None
    async for di in get_di(request):  # type: ignore[arg-type]
        seen = di.r_client

    assert seen is shared


@pytest.mark.anyio("asyncio")
@pytest.mark.parametrize(
    "error",
    [
        HTTPException(status_code=400, detail="Invalid request"),
        ValueError("unexpected failure"),
        ExceptionGroup("multiple failures", [ValueError("first"), TypeError("second")]),
    ],
)
async def test_diparams_preserves_request_errors(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    monkeypatch.setattr("api.core.load_config", lambda: object())
    di = DIParams(object(), AsyncMock())  # type: ignore[arg-type]

    with pytest.raises(type(error)) as caught:
        async with di:
            raise error

    assert caught.value is error
    assert di.tg is None


@pytest.mark.anyio("asyncio")
async def test_diparams_uses_provided_redis_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = AsyncMock()
    monkeypatch.setattr("api.core.load_config", lambda: object())

    seen = None
    async with DIParams(object(), shared) as di:  # type: ignore[arg-type]
        seen = di.r_client

    assert seen is shared
