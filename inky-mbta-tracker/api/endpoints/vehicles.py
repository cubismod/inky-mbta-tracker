import logging
from datetime import datetime
from typing import AsyncGenerator, Optional

import orjson
from api.middleware.cache_middleware import cache_ttl
from api.services.vehicle_counts import get_vehicle_route_counts
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from starlette.responses import StreamingResponse

from ..core import GET_DI, SSE_ENABLED
from ..limits import limiter
from ..models import VehiclesCountResponse

router = APIRouter()

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/vehicles",
    summary="Get Vehicle Positions",
    description=(
        "Get current vehicle positions as GeoJSON FeatureCollection. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
)
@cache_ttl(2)
@limiter.limit("70/minute")
async def get_vehicles(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> Response:
    with tracer.start_as_current_span("api.vehicles.get_vehicles") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "request.frequent_buses": frequent_buses,
                "api.endpoint": "vehicles",
                "response.format": "geojson",
            },
        )

        try:
            if commons.tg:
                span.set_attribute("cache.hit", False)
                features = await get_vehicle_features(
                    commons.r_client,
                    commons.config,
                    commons.tg,
                    frequent_buses,
                    session=commons.session,
                )
                span.set_attribute("vehicles.count", len(features))
                span.set_attribute("api.response.success", True)
                result = {"type": "FeatureCollection", "features": features}
                return Response(
                    content=orjson.dumps(result), media_type="application/json"
                )
            add_span_attributes(
                span,
                {
                    "api.response.success": False,
                    "error": True,
                    "error.type": "no_task_group",
                },
            )
            return Response(status_code=500)
        except (ConnectionError, TimeoutError) as exc:
            logger.error(
                "Error getting vehicles due to connection issue", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/vehicles/stream",
    summary="Stream Vehicle Positions",
    description="Server-sent events stream of vehicle positions",
)
@limiter.limit("70/minute")
async def get_vehicles_sse(
    request: Request, delta: bool = False, frequent_buses: bool = False
) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    async def event_generator() -> AsyncGenerator[str, None]:
        # send an initial comment to establish the stream quickly
        yield ": stream-start\n\n"
        manager = request.app.state.vehicle_stream_manager
        mode = "delta" if delta else "full"
        async with manager.subscribe(mode, frequent_buses) as events:
            async for event in events:
                if await request.is_disconnected():
                    break
                yield event

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    span = trace.get_current_span()
    add_span_attributes(
        span,
        {
            "api.endpoint": "vehicles_stream",
            "vehicle_stream.mode": "delta" if delta else "full",
            "vehicle_stream.frequent_buses": frequent_buses,
            "vehicle_stream.enabled": True,
        },
    )
    add_transaction_ids_to_span(span)
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )


@router.get(
    "/vehicles.json",
    summary="Get Vehicle Positions (JSON File)",
    description=("Get current vehicle positions as GeoJSON file."),
    response_class=RedirectResponse,
)
async def get_vehicles_json(request: Request):
    return RedirectResponse(url="/vehicles", status_code=302)


@router.get(
    "/vehicles/counts",
    summary="Get counts of vehicles by MBTA line and vehicle type",
    description="Return counts grouped by vehicle type (light rail, heavy rail, regional rail, bus) across main line groups (RL, GL, BL, OL, SL, CR)",
    response_model=Optional[VehiclesCountResponse],
)
@limiter.limit("70/minute")
@cache_ttl(30)
async def get_vehicles_counts(
    request: Request, commons: GET_DI
) -> Optional[VehiclesCountResponse]:
    with tracer.start_as_current_span("api.vehicles.get_vehicles_counts") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "vehicles_counts",
                "response.format": "json",
            },
        )

        try:
            if commons.tg:
                counts, totals_by_line = await get_vehicle_route_counts(
                    commons.r_client, commons.config
                )

                span.set_attribute("vehicles.total", totals_by_line.total)
                add_span_attributes(
                    span,
                    {
                        "vehicles.light_rail.total": counts.light_rail.total,
                        "vehicles.heavy_rail.total": counts.heavy_rail.total,
                        "vehicles.regional_rail.total": counts.regional_rail.total,
                        "vehicles.bus.total": counts.bus.total,
                        "api.response.success": True,
                    },
                )

                resp_model = VehiclesCountResponse(
                    success=True,
                    counts=counts,
                    totals_by_line=totals_by_line,
                    generated_at=datetime.now(),
                )

                return resp_model
            add_span_attributes(
                span,
                {
                    "api.response.success": False,
                    "error": True,
                    "error.type": "no_task_group",
                },
            )
            return None
        except (ConnectionError, TimeoutError) as exc:
            logger.error(
                "Error getting vehicle counts due to connection issue", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
