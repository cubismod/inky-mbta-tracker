import json
import logging
from typing import AsyncGenerator

from consts import VEHICLES_CACHE_TTL
from fastapi import APIRouter, HTTPException, Request, Response
from geojson import FeatureCollection, dumps
from geojson_utils import get_vehicle_features
from starlette.responses import StreamingResponse
from utils import get_vehicles_data

from ..core import GET_DI, SSE_ENABLED, DIParams
from ..limits import limiter

router = APIRouter()

logger = logging.getLogger(__name__)


@router.get(
    "/vehicles",
    summary="Get Vehicle Positions",
    description=(
        "Get current vehicle positions as GeoJSON FeatureCollection. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
)
@limiter.limit("70/minute")
async def get_vehicles(request: Request, commons: GET_DI) -> Response:
    try:
        cache_key = "api:vehicles"
        cached_data = await commons.r_client.get(cache_key)
        if cached_data:
            return Response(content=cached_data, media_type="application/json")

        features = await get_vehicle_features(commons.r_client, commons.tg)
        result = {"type": "FeatureCollection", "features": features}
        await commons.r_client.setex(
            cache_key,
            VEHICLES_CACHE_TTL,
            json.dumps(result),
        )
        return Response(content=json.dumps(result), media_type="application/json")
    except (ConnectionError, TimeoutError):
        logger.error("Error getting vehicles due to connection issue", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/vehicles/stream",
    summary="Stream Vehicle Positions",
    description="Server-sent events stream of vehicle positions",
)
@limiter.limit("70/minute")
async def get_vehicles_sse(request: Request) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    async def event_generator() -> AsyncGenerator[str, None]:
        # light throttle to avoid tight loop pegging CPU
        from anyio import sleep

        # send an initial comment to establish the stream quickly
        yield ": stream-start\n\n"

        # Keep the dependency context open for the duration of the stream
        async with DIParams(request.app.state.session) as commons:
            while True:
                if await request.is_disconnected():
                    break
                if commons.tg:
                    try:
                        data = await get_vehicles_data(commons.r_client)
                        yield f"data: {json.dumps(data)}\n\n"
                    except (ConnectionError, TimeoutError, ValueError, RuntimeError, OSError) as e:
                        # These are expected transient/operational errors for SSE: network, timeout,
                        # JSON/serialization failures, or runtime issues. Do not break the stream.
                        logger.error("Error producing SSE vehicles data", exc_info=e)
                        # comment as heartbeat so client keeps connection
                        yield ": error fetching data\n\n"
                else:
                    # no task group available; emit heartbeat so clients keep connection
                    yield ": tg-unavailable\n\n"

                # throttle update frequency
                await sleep(1.0)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )


@router.get(
    "/vehicles.json",
    summary="Get Vehicle Positions (JSON File)",
    description=(
        "Get current vehicle positions as GeoJSON file. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
    response_class=Response,
)
@limiter.limit("70/minute")
async def get_vehicles_json(request: Request, commons: GET_DI) -> Response:
    try:
        cache_key = "api:vehicles:json"
        cached_data = await commons.r_client.get(cache_key)
        if cached_data:
            return Response(
                content=cached_data,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=vehicles.json"},
            )

        if commons.tg:
            features = await get_vehicle_features(commons.r_client)
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)
            await commons.r_client.setex(cache_key, VEHICLES_CACHE_TTL, geojson_str)

            return Response(
                content=geojson_str,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=vehicles.json"},
            )
        else:
            return Response(status_code=500)
    except (ConnectionError, TimeoutError):
        logger.error(
            "Error getting vehicles JSON due to connection issue", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
