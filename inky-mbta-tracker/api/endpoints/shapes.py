import json
import logging

from api.middleware.cache_middleware import cache_ttl
from consts import WEEK
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from geojson_utils import get_shapes_features
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
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
        add_span_attributes(
            span,
            {
                "request.frequent_buses": frequent_buses,
                "api.endpoint": "shapes",
                "response.format": "geojson",
            },
        )

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
            add_span_attributes(
                span,
                {
                    "api.response.success": True,
                    "response.feature_count": len(features),
                },
            )
            return Response(content=json.dumps(result), media_type="application/json")
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error getting shapes due to connection issue", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error("Error getting shapes due to validation error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/shapes.json",
    summary="Get Route Shapes (JSON File)",
    description="Get route shapes as GeoJSON file.",
    response_class=RedirectResponse,
)
@limiter.limit("70/minute")
@cache_ttl(2 * WEEK)
async def get_shapes_json(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> RedirectResponse:
    return RedirectResponse(url="/shapes.geojson", status_code=302)
