import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import AsyncGenerator, Optional

from api.core import GET_DI, SSE_ENABLED
from api.limits import limiter
from config import StopSetup
from fastapi import APIRouter, Body, HTTPException, Query, Request
from mbta_client import MBTAApi
from pydantic import BaseModel, Field, ValidationError
from shared_types.shared_types import ScheduleEvent, TaskType
from starlette.responses import StreamingResponse

router = APIRouter()

logger = logging.getLogger(__name__)


@dataclass
class StopStreamConfig:
    stop_name: str
    route_filter: str
    direction_filter: int
    transit_time_min: int
    schedule_only: bool
    show_on_display: bool
    route_substring_filter: Optional[str]


class ScheduleStreamRequest(BaseModel):
    stops: list[StopSetup] = Field(..., description="Stops to stream schedules for")


class ScheduleStreamPayload(BaseModel):
    """Example payload shape for streamed schedule/prediction events."""

    events: list[ScheduleEvent] = Field(default_factory=list)
    stops: list[StopSetup] = Field(default_factory=list)


async def _resolve_stop_name(
    stop: StopSetup, commons: GET_DI
) -> tuple[str, Optional[str]]:
    """Resolve the display stop name (abbreviated) and any fetch error message."""
    try:
        direction = stop.direction_filter if stop.direction_filter != -1 else None
        async with MBTAApi(
            commons.r_client,
            stop_id=stop.stop_id,
            route=stop.route_filter or None,
            direction_filter=direction,
            schedule_only=stop.schedule_only,
            watcher_type=TaskType.SCHEDULES,
            show_on_display=stop.show_on_display,
            route_substring_filter=stop.route_substring_filter,
        ) as watcher:
            stop_data = await watcher.get_stop(
                commons.session, stop.stop_id, commons.tg
            )
            if stop_data and stop_data[0] and stop_data[0].data:
                name = stop_data[0].data.attributes.name or stop.stop_id
                return watcher.abbreviate(name), None
    except Exception as err:  # pragma: no cover - defensive logging
        logger.debug(
            "Failed to resolve stop name; falling back to stop_id", exc_info=err
        )
        return MBTAApi.abbreviate(stop.stop_id), str(err)
    return MBTAApi.abbreviate(stop.stop_id), None


def _filter_events(
    events: list[ScheduleEvent], configs: list[StopStreamConfig]
) -> list[ScheduleEvent]:
    """Filter schedule/prediction events by stop and StopSetup constraints."""
    now = datetime.now(UTC)
    filtered: list[ScheduleEvent] = []
    for event in events:
        if not event.show_on_display:
            continue
        if event.time < now:
            continue
        matching: Optional[StopStreamConfig] = None
        for cfg in configs:
            if event.stop.lower() == cfg.stop_name.lower():
                matching = cfg
                break
        if not matching:
            continue
        if matching.schedule_only and event.id.startswith("prediction"):
            continue
        if matching.route_filter and event.route_id != matching.route_filter:
            continue
        if matching.route_substring_filter and (
            matching.route_substring_filter not in event.route_id
        ):
            continue
        # Skip events that no longer meet the transit buffer
        try:
            if event.time - timedelta(minutes=matching.transit_time_min) <= now:
                continue
        except Exception:
            pass
        filtered.append(event)
    return sorted(filtered, key=lambda e: e.time)


async def _load_events(commons: GET_DI, lookahead_minutes: int) -> list[ScheduleEvent]:
    now_ts = int(datetime.now(UTC).timestamp())
    horizon_ts = int(
        (datetime.now(UTC) + timedelta(minutes=lookahead_minutes)).timestamp()
    )
    try:
        ids = await commons.r_client.zrange(
            "time", start=now_ts, end=horizon_ts, byscore=True, withscores=False
        )
        if not ids:
            return []
        raw_events = await commons.r_client.mget(ids)
        events: list[ScheduleEvent] = []
        for raw in raw_events:
            if not raw:
                continue
            try:
                events.append(ScheduleEvent.model_validate_json(raw, strict=False))
            except ValidationError:
                logger.debug("Skipping invalid schedule payload", exc_info=True)
        return events
    except Exception as err:  # pragma: no cover - log and surface as empty
        logger.error("Error loading schedule events from Redis", exc_info=err)
        return []


@router.get(
    "/schedules/stream",
    summary="Stream schedules and predictions",
    description=(
        "Server-sent events stream of upcoming schedules/predictions. "
        "Provide a JSON body containing stops (StopSetup objects). "
        "If the list is empty, configured stops are used."
    ),
    response_model=ScheduleStreamPayload,
    response_description="Streamed schedule/prediction events envelope",
)
@limiter.limit("60/minute")
async def stream_schedules(
    request: Request,
    commons: GET_DI,
    payload: ScheduleStreamRequest = Body(
        ..., description="Body with stops to stream (list of StopSetup)"
    ),
    lookahead_minutes: int = Query(120, ge=5, le=720),
) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    initial_stops: list[StopSetup] = payload.stops

    async def event_generator() -> AsyncGenerator[str, None]:
        from anyio import sleep

        yield ": stream-start\n\n"

        try:
            target_stops: list[StopSetup] = initial_stops
            if len(target_stops) == 0:
                target_stops = commons.config.stops

            stop_configs: list[StopStreamConfig] = []
            for stop in target_stops:
                resolved_name, _ = await _resolve_stop_name(stop, commons)
                stop_configs.append(
                    StopStreamConfig(
                        stop_name=resolved_name,
                        route_filter=stop.route_filter,
                        direction_filter=stop.direction_filter,
                        transit_time_min=stop.transit_time_min,
                        schedule_only=stop.schedule_only,
                        show_on_display=stop.show_on_display,
                        route_substring_filter=stop.route_substring_filter,
                    )
                )
        except HTTPException:
            raise
        except Exception as err:  # pragma: no cover - defensive
            logger.error("Failed preparing stop configs", exc_info=err)
            yield ": initialization-error\n\n"
            return

        last_payload = ""
        while True:
            if await request.is_disconnected():
                break
            events = await _load_events(commons, lookahead_minutes)
            filtered = _filter_events(events, stop_configs)
            payload_model = ScheduleStreamPayload(events=filtered, stops=target_stops)
            payload = payload_model.model_dump_json()
            if payload != last_payload:
                yield f"data: {payload}\n\n"
                last_payload = payload
            else:
                yield ": heartbeat\n\n"
            await sleep(4)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )
