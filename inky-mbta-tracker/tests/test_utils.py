import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from utils import get_vehicles_data


class DummyTG:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def start_soon(self, fn: Any, *args: Any) -> None:  # type: ignore[no-untyped-def]
        self.calls.append((fn, args))


@pytest.mark.anyio("asyncio")
async def test_get_vehicles_data_cache_hit() -> None:
    mock_redis = AsyncMock()
    cached = {"type": "FeatureCollection", "features": []}
    mock_redis.get.return_value = json.dumps(cached)

    tg = DummyTG()
    res = await get_vehicles_data(mock_redis, tg)  # type: ignore[arg-type]

    assert res == cached
    # Should not schedule a setex when using cached response
    assert tg.calls == []


@pytest.mark.anyio("asyncio")
async def test_get_vehicles_data_cache_miss_schedules_setex() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.setex = AsyncMock()

    with (
        patch("geojson_utils.get_vehicle_features", return_value=[]) as mock_features,
        patch("utils.json") as mock_json,
        patch("consts.VEHICLES_CACHE_TTL", 321),
    ):
        # Keep json.dumps deterministic for assertion
        def dumps(obj: Any) -> str:
            return json.dumps(obj)

        mock_json.dumps.side_effect = dumps

        tg = DummyTG()
        res = await get_vehicles_data(mock_redis, tg)  # type: ignore[arg-type]

        assert res == {"type": "FeatureCollection", "features": []}
        mock_features.assert_called_once()
        # Ensure a setex was scheduled with the expected args
        assert len(tg.calls) == 1
        fn, args = tg.calls[0]
        assert fn is mock_redis.setex
        assert args[0] == "api:vehicles"
        assert args[1] == 321
        assert json.loads(args[2]) == res
