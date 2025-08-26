import json
import logging

from consts import SHAPES_CACHE_TTL
from fastapi import APIRouter, HTTPException, Request, Response
from geojson import FeatureCollection, dumps
from geojson_utils import get_shapes_features
from pydantic import ValidationError

from ..core import GET_DI
from ..limits import limiter

router = APIRouter()

logger = logging.getLogger(__name__)


@router.get(
    "/shapes",
    summary="Get Route Shapes",
    description="Get route shapes as GeoJSON FeatureCollection.",
)
@limiter.limit("70/minute")
async def get_shapes(request: Request, commons: GET_DI) -> Response:
    try:
        cache_key = "api:shapes"
        cached_data = await commons.r_client.get(cache_key)
        if cached_data:
            return Response(content=cached_data, media_type="application/json")
        if commons.tg:
            features = await get_shapes_features(
                commons.config, commons.r_client, commons.tg, commons.session
            )
            result = {"type": "FeatureCollection", "features": features}
            commons.tg.start_soon(
                commons.r_client.setex, cache_key, SHAPES_CACHE_TTL, json.dumps(result)
            )
            return Response(content=json.dumps(result), media_type="application/json")
        else:
            return Response()
    except (ConnectionError, TimeoutError):
        logger.error("Error getting shapes due to connection issue", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logger.error("Error getting shapes due to validation error", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/shapes.json",
    summary="Get Route Shapes (JSON File)",
    description="Get route shapes as GeoJSON file.",
    response_class=Response,
)
@limiter.limit("70/minute")
async def get_shapes_json(request: Request, commons: GET_DI) -> Response:
    try:
        cache_key = "api:shapes:json"
        cached_data = await commons.r_client.get(cache_key)
        if cached_data:
            return Response(
                content=cached_data,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=shapes.json"},
            )

        if commons.tg:
            features = await get_shapes_features(
                commons.config, commons.r_client, commons.tg, commons.session
            )
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)
            commons.tg.start_soon(
                commons.r_client.setex, cache_key, SHAPES_CACHE_TTL, geojson_str
            )
            return Response(
                content=geojson_str,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=shapes.json"},
            )
        else:
            return Response(status_code=500)
    except (ConnectionError, TimeoutError):
        logger.error("Error getting shapes JSON due to connection issue", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logger.error("Error getting shapes JSON due to validation error", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
