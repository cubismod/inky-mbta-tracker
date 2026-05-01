import json
from typing import Any

import pytest
from anyio import create_task_group
from api.services import vehicle_stream
from api.services.vehicle_stream import VehicleStreamManager
from geojson import Feature
from shared_types.shared_types import DiffApiResponse


class FakeDIParams:
    def __init__(self, session: object) -> None:
        self.session = session
        self.r_client = object()
        self.config = object()
        self.tg = object()

    async def __aenter__(self) -> "FakeDIParams":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        return None


def _sse_data(event: str) -> dict[str, Any]:
    assert event.startswith("data: ")
    return json.loads(event.removeprefix("data: ").strip())


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_fans_out_delta_work_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_count = 0
    diff_count = 0

    async def fake_get_vehicles_data(
        *_args: object, **_kwargs: object
    ) -> dict[str, Feature]:
        nonlocal fetch_count
        fetch_count += 1
        return {
            "vehicle-1": Feature(
                id="vehicle-1",
                geometry=None,
                properties={"fetch": fetch_count},
            )
        }

    def fake_calculate_diff(
        original: dict[str, Feature], new: dict[str, Feature]
    ) -> DiffApiResponse:
        nonlocal diff_count
        diff_count += 1
        return DiffApiResponse(updated=new, removed=set(original) - set(new))

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)
    monkeypatch.setattr(vehicle_stream, "calculate_diff", fake_calculate_diff)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(object(), tg, interval_seconds=999)
        async with (
            manager.subscribe("delta", frequent_buses=False) as first,
            manager.subscribe("delta", frequent_buses=False) as second,
        ):
            first_event = await first.receive()
            second_event = await second.receive()

            assert (
                _sse_data(first_event)["updated"]["vehicle-1"]["properties"]["fetch"]
                == 1
            )
            assert (
                _sse_data(second_event)["updated"]["vehicle-1"]["properties"]["fetch"]
                == 1
            )
            assert fetch_count == 1
            assert diff_count == 1

        await manager.aclose()
        tg.cancel_scope.cancel()


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_replays_delta_snapshot_to_late_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_count = 0

    async def fake_get_vehicles_data(
        *_args: object, **_kwargs: object
    ) -> dict[str, Feature]:
        nonlocal fetch_count
        fetch_count += 1
        return {
            "vehicle-1": Feature(
                id="vehicle-1",
                geometry=None,
                properties={"fetch": fetch_count},
            )
        }

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(object(), tg, interval_seconds=999)
        async with manager.subscribe("delta", frequent_buses=False) as first:
            first_event = await first.receive()
            assert (
                _sse_data(first_event)["updated"]["vehicle-1"]["properties"]["fetch"]
                == 1
            )

        async with manager.subscribe("delta", frequent_buses=False) as second:
            second_event = await second.receive()
            assert (
                _sse_data(second_event)["updated"]["vehicle-1"]["properties"]["fetch"]
                == 1
            )
            assert fetch_count == 1

        await manager.aclose()
        tg.cancel_scope.cancel()
