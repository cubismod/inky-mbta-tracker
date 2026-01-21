import json
import logging
from datetime import UTC, datetime, timedelta
from typing import AsyncGenerator

from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import MemoryObjectSendStream
from api.core import SSE_ENABLED
from api.limits import limiter
from api.models import LiveSchedulesRequest
from api.services.connection_tracker import ConnectionTracker
from config import StopSetup
from fastapi import APIRouter, HTTPException, Request
from mbta_client_extended import watch_station
from schedule_tracker import ScheduleEvent, VehicleRedisSchema
from starlette.responses import StreamingResponse

router = APIRouter()

logger = logging.getLogger(__name__)


@router.post(
    "/schedules/stream",
    summary="Stream Live Schedules and Predictions",
    description=(
        "Server-sent events stream of live MBTA predictions and schedules for multiple stops. "
        "Accepts a list of stop configurations and streams real-time updates. "
        "Events are filtered to show only departures within the next 2 hours. "
        "Limited to 5 stops per request, 10 requests/minute, and 3 concurrent connections per IP."
    ),
)
@limiter.limit("10/minute")
async def stream_live_schedules(
    request: Request, schedule_request: LiveSchedulesRequest
) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    async def event_generator() -> AsyncGenerator[str, None]:
        from api.middleware.header_middleware import get_client_ip

        client_ip = get_client_ip(request)

        async with request.app.state.session as session:
            from api.core import DIParams

            async with DIParams(session) as commons:
                connection_tracker = ConnectionTracker(
                    commons.r_client, max_connections=3
                )

                if not await connection_tracker.can_connect(client_ip):
                    yield f"event: error\ndata: {json.dumps({'error': 'Too many concurrent connections. Maximum 3 connections per IP.'})}\n\n"
                    return

                connection_id = await connection_tracker.add_connection(client_ip)
                if not connection_id:
                    yield f"event: error\ndata: {json.dumps({'error': 'Failed to establish connection'})}\n\n"
                    return

                try:
                    yield ": stream-start\n\n"

                    send_stream, receive_stream = create_memory_object_stream[
                        ScheduleEvent | VehicleRedisSchema
                    ](max_buffer_size=1000)

                    async def handle_stop(
                        stop: StopSetup,
                        stream: MemoryObjectSendStream[
                            ScheduleEvent | VehicleRedisSchema
                        ],
                    ) -> None:
                        """Start watching a single stop and forward events to the aggregation stream."""
                        try:
                            direction_filter = None
                            if stop.direction_filter != -1:
                                direction_filter = stop.direction_filter

                            if commons.tg:
                                if stop.schedule_only:
                                    logger.info(
                                        f"Live schedules endpoint does not support schedule_only mode for stop {stop.stop_id}"
                                    )
                                    return

                                exp_time = datetime.now(UTC) + timedelta(hours=24)

                                await watch_station(
                                    commons.r_client,
                                    stop.stop_id,
                                    stop.route_filter if stop.route_filter else None,
                                    direction_filter,
                                    stream,
                                    stop.transit_time_min,
                                    exp_time,
                                    stop.show_on_display,
                                    commons.tg,
                                    commons.config,
                                    stop.route_substring_filter,
                                    commons.session,
                                )
                        except Exception as e:
                            logger.error(
                                f"Error watching stop {stop.stop_id}",
                                exc_info=e,
                            )

                    async def event_consumer() -> AsyncGenerator[str, None]:
                        """Consume events from all stops and yield as SSE."""
                        two_hours_from_now = datetime.now(UTC) + timedelta(hours=2)

                        async for event in receive_stream:
                            try:
                                if await request.is_disconnected():
                                    break

                                if isinstance(event, ScheduleEvent):
                                    if event.time > two_hours_from_now:
                                        continue

                                    event_data = event.model_dump_json()
                                    yield f"event: schedule\ndata: {event_data}\n\n"

                            except Exception as e:
                                logger.error("Error processing event", exc_info=e)
                                yield ": error processing event\n\n"

                    if commons.tg:
                        async with create_task_group() as tg:
                            for stop in schedule_request.stops:
                                tg.start_soon(handle_stop, stop, send_stream)

                            async for item in event_consumer():
                                yield item

                except Exception as e:
                    logger.error("Error in live schedules stream", exc_info=e)
                    yield f"event: error\ndata: {json.dumps({'error': 'Internal server error'})}\n\n"
                finally:
                    await connection_tracker.remove_connection(client_ip, connection_id)
                    logger.info(
                        f"Live schedules connection closed for {client_ip} (connection_id: {connection_id})"
                    )

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )
