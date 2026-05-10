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
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from prometheus import (
    vehicle_stream_active_producers,
    vehicle_stream_connected_clients,
    vehicle_stream_payload_vehicles,
    vehicle_stream_producer_iterations,
    vehicle_stream_stale_clients,
)
from shared_types.shared_types import DiffApiResponse
from utils import get_vehicles_data

from ..core import DIParams
from .vehicle_delta import calculate_diff

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

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
    empty_snapshot_skips: int = 0

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
        label = _bool_label(frequent_buses)
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
                    self._set_connected_client_metrics(state)
                    replay_failed = True
            if not replay_failed and not state.started:
                state.started = True
                self._task_group.start_soon(self._run_producer, state)
            if not replay_failed:
                self._set_connected_client_metrics(state)

        if replay_failed:
            vehicle_stream_stale_clients.labels(mode, label, "initial_replay").inc()
            await send_stream.aclose()
            await receive_stream.aclose()
            raise RuntimeError("Unable to queue initial vehicle stream replay")

        with tracer.start_as_current_span("api.vehicle_stream.subscribe") as span:
            add_transaction_ids_to_span(span)
            add_span_attributes(
                span,
                {
                    "vehicle_stream.mode": mode,
                    "vehicle_stream.frequent_buses": frequent_buses,
                    "vehicle_stream.clients.start": state.subscriber_count(),
                    "vehicle_stream.initial_replay": replay is not None,
                },
            )

            try:
                async with receive_stream:
                    yield receive_stream
                add_span_attributes(span, {"vehicle_stream.disconnect": "normal"})
            except Exception as exc:
                set_span_error(span, exc)
                add_span_attributes(span, {"vehicle_stream.disconnect": "error"})
                raise
            finally:
                async with state.lock:
                    state.subscribers[mode].discard(send_stream)
                    final_clients = state.subscriber_count()
                    self._set_connected_client_metrics(state)
                add_span_attributes(
                    span,
                    {
                        "vehicle_stream.clients.end": final_clients,
                        "vehicle_stream.closed": self._closed,
                    },
                )
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
                self._set_connected_client_metrics(state)
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
        label = _bool_label(state.frequent_buses)
        vehicle_stream_active_producers.labels(label).set(1)
        try:
            async with DIParams(self._session) as commons:
                while not self._closed:
                    async with state.lock:
                        if state.subscriber_count() == 0:
                            vehicle_stream_producer_iterations.labels(
                                label, "no_subscribers"
                            ).inc()
                            return

                    if not commons.tg:
                        full_sent, full_stale = await self._broadcast(
                            state, "full", ": tg-unavailable\n\n"
                        )
                        delta_sent, delta_stale = await self._broadcast(
                            state, "delta", ": tg-unavailable\n\n"
                        )
                        current_span = trace.get_current_span()
                        add_span_attributes(
                            current_span,
                            {
                                "vehicle_stream.broadcast.full.sent": full_sent,
                                "vehicle_stream.broadcast.delta.sent": delta_sent,
                                "vehicle_stream.broadcast.stale": full_stale
                                + delta_stale,
                                "error": True,
                                "error.type": "task_group_unavailable",
                            },
                        )
                        vehicle_stream_producer_iterations.labels(
                            label, "task_group_unavailable"
                        ).inc()
                        await sleep(self._interval_seconds)
                        continue

                    with tracer.start_as_current_span(
                        "api.vehicle_stream.produce"
                    ) as span:
                        try:
                            add_transaction_ids_to_span(span)
                            add_span_attributes(
                                span,
                                {
                                    "vehicle_stream.frequent_buses": (
                                        state.frequent_buses
                                    ),
                                    "vehicle_stream.clients": (
                                        state.subscriber_count()
                                    ),
                                },
                            )
                            raw_data = await get_vehicles_data(
                                commons.r_client,
                                commons.config,
                                commons.tg,
                                state.frequent_buses,
                            )
                            should_ignore_empty_snapshot = False
                            async with state.lock:
                                previous_count = len(state.last_raw_data)
                                if (
                                    not raw_data
                                    and previous_count > 0
                                    and state.empty_snapshot_skips < 3
                                ):
                                    state.empty_snapshot_skips += 1
                                    should_ignore_empty_snapshot = True
                                elif raw_data:
                                    state.empty_snapshot_skips = 0

                            if should_ignore_empty_snapshot:
                                add_span_attributes(
                                    span,
                                    {
                                        "vehicles.count": 0,
                                        "vehicles.previous_count": previous_count,
                                        "vehicles.empty_snapshot_skips": (
                                            state.empty_snapshot_skips
                                        ),
                                        "vehicle_stream.iteration.status": (
                                            "empty_snapshot_ignored"
                                        ),
                                    },
                                )
                                vehicle_stream_producer_iterations.labels(
                                    label, "empty_snapshot_ignored"
                                ).inc()
                                await sleep(self._interval_seconds)
                                continue

                            full_event = self._build_full_event(raw_data)
                            delta_response = calculate_diff(
                                state.last_raw_data, raw_data
                            )
                            delta_event = (
                                f"data: {delta_response.model_dump_json()}\n\n"
                            )
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
                                state.empty_snapshot_skips = 0

                            vehicle_stream_payload_vehicles.labels(label).set(
                                len(raw_data)
                            )
                            add_span_attributes(
                                span,
                                {
                                    "vehicles.count": len(raw_data),
                                    "diff.updated_count": len(delta_response.updated),
                                    "diff.removed_count": len(delta_response.removed),
                                    "vehicle_stream.payload.full.bytes": len(
                                        full_event.encode("utf-8")
                                    ),
                                    "vehicle_stream.payload.delta.bytes": len(
                                        delta_event.encode("utf-8")
                                    ),
                                },
                            )

                            full_sent, full_stale = await self._broadcast(
                                state, "full", full_event
                            )
                            delta_sent, delta_stale = await self._broadcast(
                                state, "delta", delta_event
                            )
                            add_span_attributes(
                                span,
                                {
                                    "vehicle_stream.broadcast.full.sent": full_sent,
                                    "vehicle_stream.broadcast.delta.sent": delta_sent,
                                    "vehicle_stream.broadcast.stale": full_stale
                                    + delta_stale,
                                    "vehicle_stream.iteration.status": "success",
                                },
                            )
                            vehicle_stream_producer_iterations.labels(
                                label, "success"
                            ).inc()
                        except (
                            ConnectionError,
                            TimeoutError,
                            ValueError,
                            RuntimeError,
                            OSError,
                        ) as exc:
                            logger.error(
                                "Error producing SSE vehicles data", exc_info=exc
                            )
                            set_span_error(span, exc)
                            full_sent, full_stale = await self._broadcast(
                                state, "full", ": error fetching data\n\n"
                            )
                            delta_sent, delta_stale = await self._broadcast(
                                state, "delta", ": error fetching data\n\n"
                            )
                            add_span_attributes(
                                span,
                                {
                                    "vehicle_stream.broadcast.full.sent": full_sent,
                                    "vehicle_stream.broadcast.delta.sent": delta_sent,
                                    "vehicle_stream.broadcast.stale": full_stale
                                    + delta_stale,
                                    "vehicle_stream.iteration.status": "error",
                                },
                            )
                            vehicle_stream_producer_iterations.labels(
                                label, "error"
                            ).inc()

                    await sleep(self._interval_seconds)
        except Exception as exc:
            set_span_error(trace.get_current_span(), exc)
            logger.exception("Vehicle SSE producer exited unexpectedly")
        finally:
            async with state.lock:
                state.started = False
            vehicle_stream_active_producers.labels(label).set(0)

    async def _broadcast(
        self, state: _VehicleProducerState, mode: VehicleStreamMode, event: str
    ) -> tuple[int, int]:
        stale_subscribers: list[tuple[MemoryObjectSendStream[str], str]] = []
        label = _bool_label(state.frequent_buses)
        async with state.lock:
            subscribers = list(state.subscribers[mode])
            for subscriber in subscribers:
                try:
                    subscriber.send_nowait(event)
                except WouldBlock:
                    stale_subscribers.append((subscriber, "backpressure"))
                except (BrokenResourceError, ClosedResourceError):
                    stale_subscribers.append((subscriber, "closed"))

            for subscriber, reason in stale_subscribers:
                state.subscribers[mode].discard(subscriber)
                vehicle_stream_stale_clients.labels(mode, label, reason).inc()
            self._set_connected_client_metrics(state)

        for subscriber, _reason in stale_subscribers:
            await subscriber.aclose()
        return len(subscribers) - len(stale_subscribers), len(stale_subscribers)

    def _set_connected_client_metrics(self, state: _VehicleProducerState) -> None:
        label = _bool_label(state.frequent_buses)
        for mode, subscribers in state.subscribers.items():
            vehicle_stream_connected_clients.labels(mode, label).set(len(subscribers))

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


def _bool_label(value: bool) -> str:
    return "true" if value else "false"
