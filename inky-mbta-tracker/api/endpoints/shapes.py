import json
import logging

from api.middleware.cache_middleware import cache_ttl
from consts import WEEK
from fastapi import APIRouter, HTTPException, Request, Response
from geojson import FeatureCollection, dumps
from geojson_utils import get_shapes_features
from opentelemetry import trace
from otel_utils import add_transaction_ids_to_span
from pydantic import ValidationError

from ..core import GET_DI
from ..limits import limiter

router = APIRouter()

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/shapes",
    summary="Get Route Shapes",
    description="Get route shapes as GeoJSON FeatureCollection.",
)
@limiter.limit("70/minute")
@cache_ttl(2 * WEEK)
async def get_shapes(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> Response:
    with tracer.start_as_current_span("api.shapes.get_shapes") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            features = await get_shapes_features(
                commons.config,
                commons.r_client,
                commons.tg,
                commons.session,
                frequent_buses,
            )
            span.set_attribute("shapes.count", len(features))
            result = {"type": "FeatureCollection", "features": features}
            return Response(content=json.dumps(result), media_type="application/json")
        except (ConnectionError, TimeoutError):
            logger.error("Error getting shapes due to connection issue", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError:
            logger.error("Error getting shapes due to validation error", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/shapes.json",
    summary="Get Route Shapes (JSON File)",
    description="Get route shapes as GeoJSON file.",
    response_class=Response,
)
@limiter.limit("70/minute")
@cache_ttl(2 * WEEK)
async def get_shapes_json(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> Response:
    with tracer.start_as_current_span("api.shapes.get_shapes_json") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            features = await get_shapes_features(
                commons.config,
                commons.r_client,
                commons.tg,
                commons.session,
                frequent_buses,
            )
            span.set_attribute("shapes.count", len(features))
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)

            return Response(
                content=geojson_str,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=shapes.json"},
            )
        except (ConnectionError, TimeoutError):
            logger.error(
                "Error getting shapes JSON due to connection issue", exc_info=True
            )
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError:
            logger.error(
                "Error getting shapes JSON due to validation error", exc_info=True
            )
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=500, detail="Internal server error")
