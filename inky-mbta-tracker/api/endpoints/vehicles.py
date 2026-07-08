import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Optional
from zlib_ng import zlib_ng

import orjson
from api.middleware.cache_middleware import cache_ttl
from api.services.vehicle_counts import get_vehicle_route_counts
from consts import VEHICLE_STREAM_KEY
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from shared_types.shared_types import DiffApiResponse
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
    description="Server-sent events stream of vehicle position deltas",
)
@limiter.limit("70/minute")
async def get_vehicles_sse(
    request: Request,
    commons: GET_DI,
    frequent_buses: bool = False,
    compress: bool = False,
) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")
    if not commons.tg:
        raise HTTPException(status_code=500, detail="Internal server error")

    tg = commons.tg
    r_client = commons.r_client
    config = commons.config
    session = commons.session
    channel = f"{VEHICLE_STREAM_KEY}:{'buses' if frequent_buses else 'rapid'}"

    async def _event_generator_str() -> AsyncGenerator[str, None]:
        yield ": stream-start\n\n"
        try:
            features = await get_vehicle_features(
                r_client,
                config,
                tg,
                frequent_buses,
                session=session,
            )
            if features:
                reset = DiffApiResponse(updated=features, removed=set())
                yield f"data: {reset.model_dump_json()}\n\n"
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error fetching initial vehicle snapshot", exc_info=exc)
            yield ": error initial-load\n\n"

        pubsub = r_client.pubsub()
        await pubsub.subscribe(channel)
        try:
            while not await request.is_disconnected():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None or message.get("type") != "message":
                    continue
                payload = message["data"]
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8")
                yield f"data: {payload}\n\n"
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    async def _event_generator_gz() -> AsyncGenerator[bytes, None]:
        compressor = zlib_ng.compressobj(wbits=zlib_ng.MAX_WBITS + 16)

        data = b": stream-start\n\n"
        compressed = compressor.compress(data)
        flushed = compressor.flush(zlib_ng.Z_SYNC_FLUSH)
        if compressed or flushed:
            yield compressed + flushed

        try:
            features = await get_vehicle_features(
                r_client,
                config,
                tg,
                frequent_buses,
                session=session,
            )
            if features:
                reset = DiffApiResponse(updated=features, removed=set())
                data = f"data: {reset.model_dump_json()}\n\n".encode("utf-8")
                compressed = compressor.compress(data)
                flushed = compressor.flush(zlib_ng.Z_SYNC_FLUSH)
                if compressed or flushed:
                    yield compressed + flushed
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error fetching initial vehicle snapshot", exc_info=exc)
            data = b": error initial-load\n\n"
            compressed = compressor.compress(data)
            flushed = compressor.flush(zlib_ng.Z_SYNC_FLUSH)
            if compressed or flushed:
                yield compressed + flushed

        pubsub = r_client.pubsub()
        await pubsub.subscribe(channel)
        try:
            while not await request.is_disconnected():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None or message.get("type") != "message":
                    continue
                payload: str | bytes = message["data"]
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8")
                data = f"data: {payload}\n\n".encode("utf-8")
                compressed = compressor.compress(data)
                flushed = compressor.flush(zlib_ng.Z_SYNC_FLUSH)
                if compressed or flushed:
                    yield compressed + flushed
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    if compress:
        headers["Content-Encoding"] = "gzip"

    span = trace.get_current_span()
    add_span_attributes(
        span,
        {
            "api.endpoint": "vehicles_stream",
            "vehicle_stream.frequent_buses": frequent_buses,
            "vehicle_stream.enabled": True,
            "vehicle_stream.compress": compress,
        },
    )
    add_transaction_ids_to_span(span)

    content = _event_generator_gz() if compress else _event_generator_str()
    return StreamingResponse(content, media_type="text/event-stream", headers=headers)


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
