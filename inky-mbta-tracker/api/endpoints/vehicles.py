import json
import logging
from typing import AsyncGenerator

from consts import VEHICLES_CACHE_TTL
from fastapi import APIRouter, HTTPException, Request, Response
from geojson import FeatureCollection, dumps
from geojson_utils import get_vehicle_features
from starlette.responses import StreamingResponse
from utils import get_vehicles_data

from ..core import REDIS_CLIENT, SSE_ENABLED
from ..limits import limiter

router = APIRouter()


@router.get(
    "/vehicles",
    summary="Get Vehicle Positions",
    description=(
        "Get current vehicle positions as GeoJSON FeatureCollection. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
)
@limiter.limit("70/minute")
async def get_vehicles(request: Request) -> Response:
    try:
        cache_key = "api:vehicles"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if cached_data:
            return Response(content=cached_data, media_type="application/json")

        features = await get_vehicle_features(REDIS_CLIENT)
        result = {"type": "FeatureCollection", "features": features}
        await REDIS_CLIENT.setex(cache_key, VEHICLES_CACHE_TTL, json.dumps(result))
        return Response(content=json.dumps(result), media_type="application/json")
    except (ConnectionError, TimeoutError):
        logging.error("Error getting vehicles due to connection issue", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/vehicles.sse",
    summary="Stream Vehicle Positions",
    description="Server-sent events stream of vehicle positions",
)
@limiter.limit("70/minute")
async def get_vehicles_sse(request: Request) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    async def event_generator() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                break
            data = await get_vehicles_data(REDIS_CLIENT)
            yield f"data: {json.dumps(data)}\n\n"

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
async def get_vehicles_json(request: Request) -> Response:
    try:
        cache_key = "api:vehicles:json"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if cached_data:
            return Response(
                content=cached_data,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=vehicles.json"},
            )

        features = await get_vehicle_features(REDIS_CLIENT)
        feature_collection = FeatureCollection(features)
        geojson_str = dumps(feature_collection, sort_keys=True)
        await REDIS_CLIENT.setex(cache_key, VEHICLES_CACHE_TTL, geojson_str)
        return Response(
            content=geojson_str,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=vehicles.json"},
        )
    except (ConnectionError, TimeoutError):
        logging.error(
            "Error getting vehicles JSON due to connection issue", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
