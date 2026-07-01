import json
from typing import Any

import pytest
from anyio import create_task_group, sleep
from api.services import vehicle_stream
from api.services.vehicle_stream import VehicleStreamManager
from geojson import Feature
from shared_types.shared_types import DiffApiResponse


class FakeDIParams:
    def __init__(self, session: object, r_client: object) -> None:
        self.session = session
        self.r_client = r_client
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


def _vehicle(vehicle_id: str, **properties: object) -> Feature:
    return Feature(id=vehicle_id, geometry=None, properties=properties)


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
            "vehicle-1": _vehicle("vehicle-1", fetch=fetch_count),
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
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=999
        )
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
            "vehicle-1": _vehicle("vehicle-1", fetch=fetch_count),
        }

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=999
        )
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


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_replays_full_snapshot_to_late_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_count = 0

    async def fake_get_vehicles_data(
        *_args: object, **_kwargs: object
    ) -> dict[str, Feature]:
        nonlocal fetch_count
        fetch_count += 1
        return {
            "vehicle-1": _vehicle("vehicle-1", fetch=fetch_count),
        }

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=999
        )
        async with manager.subscribe("full", frequent_buses=False) as first:
            first_event = await first.receive()
            assert _sse_data(first_event)["features"][0]["properties"]["fetch"] == 1

        async with manager.subscribe("full", frequent_buses=False) as second:
            second_event = await second.receive()
            assert _sse_data(second_event)["features"][0]["properties"]["fetch"] == 1
            assert fetch_count == 1

        await manager.aclose()
        tg.cancel_scope.cancel()


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_splits_frequent_bus_producers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[bool] = []

    async def fake_get_vehicles_data(
        _r_client: object,
        _config: object,
        _tg: object,
        frequent_buses: bool = False,
    ) -> dict[str, Feature]:
        calls.append(frequent_buses)
        vehicle_id = "frequent" if frequent_buses else "regular"
        return {
            vehicle_id: _vehicle(vehicle_id, frequent_buses=frequent_buses),
        }

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=999
        )
        async with (
            manager.subscribe("full", frequent_buses=False) as regular,
            manager.subscribe("full", frequent_buses=True) as frequent,
        ):
            regular_event = _sse_data(await regular.receive())
            frequent_event = _sse_data(await frequent.receive())

            assert regular_event["features"][0]["id"] == "regular"
            assert frequent_event["features"][0]["id"] == "frequent"
            assert calls.count(False) == 1
            assert calls.count(True) == 1

        await manager.aclose()
        tg.cancel_scope.cancel()


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_cleans_up_failed_replay() -> None:
    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), subscriber_buffer_size=0
        )
        state = await manager._state_for(False)
        state.latest_delta_snapshot_event = 'data: {"updated": {}, "removed": []}\n\n'

        with pytest.raises(RuntimeError, match="initial vehicle stream replay"):
            async with manager.subscribe("delta", frequent_buses=False):
                pass

        assert state.subscriber_count() == 0
        await manager.aclose()
        tg.cancel_scope.cancel()


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_resets_started_after_producer_setup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingDIParams:
        def __init__(self, session: object, r_client: object) -> None:
            self.session = session

        async def __aenter__(self) -> "FailingDIParams":
            raise RuntimeError("setup failed")

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object,
        ) -> None:
            return None

    monkeypatch.setattr(vehicle_stream, "DIParams", FailingDIParams)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=999
        )
        async with manager.subscribe("full", frequent_buses=False):
            state = await manager._state_for(False)
            for _ in range(10):
                async with state.lock:
                    started = state.started
                if not started:
                    break
                await sleep(0)

            async with state.lock:
                assert state.started is False

        await manager.aclose()
        tg.cancel_scope.cancel()


async def _wait_for_producer_exit(
    manager: VehicleStreamManager, frequent_buses: bool = False
) -> None:
    state = await manager._state_for(frequent_buses)
    for _ in range(200):
        async with state.lock:
            if not state.started:
                return
        await sleep(0.01)
    raise AssertionError("producer did not exit within timeout")


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_clears_replay_cache_when_producer_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_get_vehicles_data(
        *_args: object, **_kwargs: object
    ) -> dict[str, Feature]:
        return {"vehicle-1": _vehicle("vehicle-1", stale=True)}

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=1
        )
        async with manager.subscribe("full", frequent_buses=False) as sub:
            await sub.receive()

        await _wait_for_producer_exit(manager)

        state = await manager._state_for(False)
        async with state.lock:
            assert state.started is False
            assert state.latest_full_event is None
            assert state.latest_delta_snapshot_event is None
            assert state.last_raw_data == {}
            assert state.empty_snapshot_skips == 0

        await manager.aclose()
        tg.cancel_scope.cancel()


@pytest.mark.anyio("asyncio")
async def test_vehicle_stream_manager_no_stale_replay_for_new_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_data: dict[str, Feature] = {
        "vehicle-1": _vehicle("vehicle-1", generation="old"),
    }

    async def fake_get_vehicles_data(
        *_args: object, **_kwargs: object
    ) -> dict[str, Feature]:
        return fetch_data

    monkeypatch.setattr(vehicle_stream, "DIParams", FakeDIParams)
    monkeypatch.setattr(vehicle_stream, "get_vehicles_data", fake_get_vehicles_data)

    async with create_task_group() as tg:
        manager = VehicleStreamManager(
            object(), tg, r_client=object(), interval_seconds=1
        )

        async with manager.subscribe("full", frequent_buses=False) as first:
            first_event = await first.receive()
            assert _sse_data(first_event)["features"][0]["id"] == "vehicle-1"

        await _wait_for_producer_exit(manager)

        fetch_data = {"vehicle-2": _vehicle("vehicle-2", generation="fresh")}

        async with manager.subscribe("full", frequent_buses=False) as second:
            second_event = await second.receive()
            data = _sse_data(second_event)
            vehicle_ids = [f["id"] for f in data["features"]]
            assert "vehicle-2" in vehicle_ids
            assert "vehicle-1" not in vehicle_ids

        await manager.aclose()
        tg.cancel_scope.cancel()
