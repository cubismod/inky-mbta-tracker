from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from api.services.vehicle_counts import _classify_route, get_vehicle_route_counts
from config import Config
from redis.asyncio import Redis
from shared_types.shared_types import VehicleRedisSchema


def _vehicle(vid: str, route: str) -> VehicleRedisSchema:
    return VehicleRedisSchema(
        action="add",
        id=vid,
        current_status="IN_TRANSIT_TO",
        direction_id=0,
        latitude=42.0,
        longitude=-71.0,
        speed=15,
        bearing=0,
        stop=None,
        route=route,
        update_time=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _redis_for(vehicles: list[VehicleRedisSchema]) -> Any:
    redis = MagicMock()
    keys = {f"vehicle:{v.id}".encode() for v in vehicles}
    redis.smembers = AsyncMock(return_value=keys)
    pipeline = MagicMock()
    pipeline.get = AsyncMock()
    pipeline.execute = AsyncMock(
        return_value=[
            (v.model_dump_json().encode() if v is not None else None) for v in vehicles
        ]
    )
    redis.pipeline.return_value = pipeline
    redis.srem = AsyncMock()
    return redis


@pytest.mark.parametrize(
    "route,expected_line,expected_vtype",
    [
        ("Red", "RL", "heavy_rail"),
        ("Red-1", "RL", "heavy_rail"),
        ("Mattapan", "RL", "light_rail"),
        ("Green-B", "GL", "light_rail"),
        ("Blue", "BL", "heavy_rail"),
        ("Orange", "OL", "heavy_rail"),
        ("CR-Providence", "CR", "regional_rail"),
        ("Commuter Rail", "CR", "regional_rail"),
        ("SL1", "SL", "bus"),
        ("741", "SL", "bus"),
        ("749", "SL", "bus"),
    ],
)
def test_classify_route_buckets_known_lines(
    route: str, expected_line: str, expected_vtype: str
) -> None:
    assert _classify_route(route) == (expected_line, expected_vtype)


@pytest.mark.parametrize(
    "route",
    ["32", "112", "Amtrak NE Corridor", "X-Unknown", ""],
)
def test_classify_route_returns_none_for_untracked(route: str) -> None:
    assert _classify_route(route) == (None, None)


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_tallies_by_route() -> None:
    redis = _redis_for(
        [
            _vehicle("v1", "Red"),
            _vehicle("v2", "Green-B"),
            _vehicle("v3", "SL1"),
            _vehicle("v4", "32"),
        ]
    )

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis), Config(vehicles_by_route=["Red"])
    )

    assert counts.heavy_rail.RL == 1
    assert counts.heavy_rail.total == 1
    assert counts.light_rail.GL == 1
    assert counts.light_rail.total == 1
    assert counts.bus.SL == 1
    assert counts.bus.total == 1
    assert totals.RL == 1
    assert totals.GL == 1
    assert totals.SL == 1
    assert totals.total == 3


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_empty_pos_data_returns_zeros() -> None:
    redis = MagicMock()
    redis.smembers = AsyncMock(return_value=set())

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis), Config(vehicles_by_route=["Red"])
    )

    assert totals.total == 0
    for vtype in ("light_rail", "heavy_rail", "regional_rail", "bus"):
        assert getattr(counts, vtype).total == 0


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_prunes_stale_keys() -> None:
    payloads = {
        b"vehicle:stale": None,
        b"vehicle:live": _vehicle("live", "Red").model_dump_json().encode(),
    }
    redis = MagicMock()
    redis.smembers = AsyncMock(return_value=set(payloads))
    pipeline = MagicMock()
    recorded_gets: list[bytes] = []

    async def _record_get(vk: bytes) -> None:
        recorded_gets.append(vk)

    pipeline.get = AsyncMock(side_effect=_record_get)

    async def _execute() -> list[bytes | None]:
        return [payloads[vk] for vk in recorded_gets]

    pipeline.execute = AsyncMock(side_effect=_execute)
    redis.pipeline.return_value = pipeline
    redis.srem = AsyncMock()

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis), Config(vehicles_by_route=["Red"])
    )

    redis.srem.assert_awaited_once_with("pos-data", b"vehicle:stale")
    assert totals.RL == 1
    assert totals.total == 1


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_skips_invalid_payloads() -> None:
    redis = MagicMock()
    redis.smembers = AsyncMock(return_value={b"vehicle:bad", b"vehicle:good"})
    pipeline = MagicMock()
    pipeline.get = AsyncMock()
    pipeline.execute = AsyncMock(
        return_value=[
            b"not-json",
            _vehicle("good", "Orange").model_dump_json().encode(),
        ]
    )
    redis.pipeline.return_value = pipeline
    redis.srem = AsyncMock()

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis), Config(vehicles_by_route=["Orange"])
    )

    redis.srem.assert_not_awaited()
    assert totals.OL == 1
    assert counts.heavy_rail.OL == 1


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_excludes_frequent_buses_when_disabled() -> None:
    redis = _redis_for([_vehicle("a", "741"), _vehicle("b", "Red")])

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis),
        Config(vehicles_by_route=["Red"], frequent_bus_lines=["741"]),
        frequent_buses=False,
    )

    assert totals.SL == 0
    assert totals.RL == 1
    assert totals.total == 1


@pytest.mark.anyio("asyncio")
async def test_get_vehicle_route_counts_includes_only_frequent_buses_when_enabled() -> (
    None
):
    redis = _redis_for([_vehicle("a", "741"), _vehicle("b", "Red")])

    counts, totals = await get_vehicle_route_counts(
        cast(Redis, redis),
        Config(vehicles_by_route=["Red"], frequent_bus_lines=["741"]),
        frequent_buses=True,
    )

    assert totals.SL == 1
    assert totals.RL == 0
    assert totals.total == 1
