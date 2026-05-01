import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

from anyio import (
    BrokenResourceError,
    ClosedResourceError,
    Lock,
    WouldBlock,
    create_memory_object_stream,
    sleep,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from geojson import Feature, dumps
from shared_types.shared_types import DiffApiResponse
from utils import get_vehicles_data

from ..core import DIParams
from .vehicle_delta import calculate_diff

logger = logging.getLogger(__name__)

VehicleStreamMode = Literal["full", "delta"]


@dataclass
class _VehicleProducerState:
    frequent_buses: bool
    lock: Lock = field(default_factory=Lock)
    started: bool = False
    subscribers: dict[VehicleStreamMode, set[MemoryObjectSendStream[str]]] = field(
        default_factory=lambda: {
            "full": set(),
            "delta": set(),
        }
    )
    last_raw_data: dict[str, Feature] = field(default_factory=dict)
    latest_full_event: str | None = None
    latest_delta_snapshot_event: str | None = None

    def subscriber_count(self) -> int:
        return sum(len(subscribers) for subscribers in self.subscribers.values())


class VehicleStreamManager:
    def __init__(
        self,
        session: Any,
        task_group: TaskGroup,
        *,
        interval_seconds: int = 4,
        subscriber_buffer_size: int = 4,
    ) -> None:
        self._session = session
        self._task_group = task_group
        self._interval_seconds = interval_seconds
        self._subscriber_buffer_size = subscriber_buffer_size
        self._states: dict[bool, _VehicleProducerState] = {}
        self._states_lock = Lock()
        self._closed = False

    @asynccontextmanager
    async def subscribe(
        self, mode: VehicleStreamMode, frequent_buses: bool
    ) -> AsyncGenerator[MemoryObjectReceiveStream[str], None]:
        if self._closed:
            raise RuntimeError("Vehicle stream manager is closed")

        state = await self._state_for(frequent_buses)
        send_stream, receive_stream = create_memory_object_stream[str](
            self._subscriber_buffer_size
        )
        replay_failed = False

        async with state.lock:
            state.subscribers[mode].add(send_stream)
            replay = (
                state.latest_delta_snapshot_event
                if mode == "delta"
                else state.latest_full_event
            )
            if replay is not None:
                try:
                    send_stream.send_nowait(replay)
                except (WouldBlock, BrokenResourceError, ClosedResourceError):
                    state.subscribers[mode].discard(send_stream)
                    replay_failed = True
            if not replay_failed and not state.started:
                state.started = True
                self._task_group.start_soon(self._run_producer, state)

        if replay_failed:
            await send_stream.aclose()
            await receive_stream.aclose()
            raise RuntimeError("Unable to queue initial vehicle stream replay")

        try:
            async with receive_stream:
                yield receive_stream
        finally:
            async with state.lock:
                state.subscribers[mode].discard(send_stream)
            await send_stream.aclose()

    async def aclose(self) -> None:
        self._closed = True
        async with self._states_lock:
            states = list(self._states.values())
        for state in states:
            async with state.lock:
                subscribers = [
                    subscriber
                    for mode_subscribers in state.subscribers.values()
                    for subscriber in mode_subscribers
                ]
                for mode_subscribers in state.subscribers.values():
                    mode_subscribers.clear()
            for subscriber in subscribers:
                await subscriber.aclose()

    async def _state_for(self, frequent_buses: bool) -> _VehicleProducerState:
        async with self._states_lock:
            if frequent_buses not in self._states:
                self._states[frequent_buses] = _VehicleProducerState(
                    frequent_buses=frequent_buses
                )
            return self._states[frequent_buses]

    async def _run_producer(self, state: _VehicleProducerState) -> None:
        try:
            async with DIParams(self._session) as commons:
                while not self._closed:
                    async with state.lock:
                        if state.subscriber_count() == 0:
                            return

                    if not commons.tg:
                        await self._broadcast(state, "full", ": tg-unavailable\n\n")
                        await self._broadcast(state, "delta", ": tg-unavailable\n\n")
                        await sleep(self._interval_seconds)
                        continue

                    try:
                        raw_data = await get_vehicles_data(
                            commons.r_client,
                            commons.config,
                            commons.tg,
                            state.frequent_buses,
                        )
                        full_event = self._build_full_event(raw_data)
                        delta_response = calculate_diff(state.last_raw_data, raw_data)
                        delta_event = f"data: {delta_response.model_dump_json()}\n\n"
                        snapshot_response = DiffApiResponse(
                            updated=raw_data,
                            removed=set(),
                        )
                        snapshot_event = (
                            f"data: {snapshot_response.model_dump_json()}\n\n"
                        )

                        async with state.lock:
                            state.last_raw_data = raw_data
                            state.latest_full_event = full_event
                            state.latest_delta_snapshot_event = snapshot_event

                        await self._broadcast(state, "full", full_event)
                        await self._broadcast(state, "delta", delta_event)
                    except (
                        ConnectionError,
                        TimeoutError,
                        ValueError,
                        RuntimeError,
                        OSError,
                    ) as exc:
                        logger.error("Error producing SSE vehicles data", exc_info=exc)
                        await self._broadcast(
                            state, "full", ": error fetching data\n\n"
                        )
                        await self._broadcast(
                            state, "delta", ": error fetching data\n\n"
                        )

                    await sleep(self._interval_seconds)
        except Exception:
            logger.exception("Vehicle SSE producer exited unexpectedly")
        finally:
            async with state.lock:
                state.started = False

    async def _broadcast(
        self, state: _VehicleProducerState, mode: VehicleStreamMode, event: str
    ) -> None:
        stale_subscribers: list[MemoryObjectSendStream[str]] = []
        async with state.lock:
            subscribers = list(state.subscribers[mode])
            for subscriber in subscribers:
                try:
                    subscriber.send_nowait(event)
                except WouldBlock:
                    stale_subscribers.append(subscriber)
                except (BrokenResourceError, ClosedResourceError):
                    stale_subscribers.append(subscriber)

            for subscriber in stale_subscribers:
                state.subscribers[mode].discard(subscriber)

        for subscriber in stale_subscribers:
            await subscriber.aclose()

    def _build_full_event(self, raw_data: dict[str, Feature]) -> str:
        norm_data = self._normalize_features(raw_data)
        try:
            payload = {
                "type": "FeatureCollection",
                "features": list(norm_data.values()),
            }
            return f"data: {json.dumps(payload)}\n\n"
        except Exception:
            return f"data: {json.dumps(raw_data)}\n\n"

    def _normalize_features(self, features: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if not isinstance(features, dict):
            return out
        for key, value in features.items():
            try:
                if isinstance(value, Feature):
                    out[key] = json.loads(dumps(value, sort_keys=True))
                else:
                    out[key] = value
            except Exception:
                out[key] = value if isinstance(value, dict) else {"_repr": str(value)}
        return out
