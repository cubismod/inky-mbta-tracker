import logging
import re

from api.core import GET_DI
from api.middleware.cache_middleware import cache_ttl
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from mbta_responses import Alerts
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from pydantic import ValidationError
from redis.exceptions import RedisError

from ..limits import limiter
from ..models import ErrorResponse
from ..services.alerts import fetch_alerts_with_retry, fetch_bus_alerts

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
ROUTE_ID_PATTERN = re.compile(r"^[0-9]+$")


@router.get(
    "/alerts",
    summary="Get MBTA Alerts",
    description=(
        "Get current MBTA alerts. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
    response_model=Alerts,
    responses={500: {"model": ErrorResponse, "description": "Internal server error"}},
)
@limiter.limit("100/minute")
@cache_ttl(60)
async def get_alerts(request: Request, commons: GET_DI) -> Response:
    with tracer.start_as_current_span("api.alerts.get_alerts") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "alerts",
                "response.format": "json",
            },
        )

        try:
            result = await fetch_alerts_with_retry(
                commons.config, commons.session, commons.r_client
            )

            span.set_attribute("alerts.count", result.count)
            add_span_attributes(
                span,
                {
                    "api.response.success": True,
                    "response.body.bytes": len(result.body.encode("utf-8")),
                },
            )
            return Response(content=result.body, media_type="application/json")
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error getting alerts due to connection issue", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting alerts due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error("Error getting alerts due to validation error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/alerts.json",
    summary="Get MBTA Alerts (JSON File)",
    description="Get current MBTA alerts as JSON file.",
    response_class=RedirectResponse,
)
@limiter.limit("100/minute")
async def get_alerts_json(request: Request) -> RedirectResponse:
    return RedirectResponse(url="/alerts", status_code=302)


@router.get(
    "/alerts/bus/{route_id}",
    summary="Get MBTA Bus Alerts for a Route",
    description="Get current MBTA alerts for a specific bus route.",
    response_model=Alerts,
    responses={500: {"model": ErrorResponse, "description": "Internal server error"}},
)
@limiter.limit("100/minute")
@cache_ttl(60)
async def get_bus_alerts(request: Request, route_id: str, commons: GET_DI) -> Response:
    if not ROUTE_ID_PATTERN.fullmatch(route_id):
        return Response(
            content='{"detail": "Invalid route_id"}',
            status_code=400,
            media_type="application/json",
        )
    with tracer.start_as_current_span("api.alerts.get_bus_alerts") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "alerts/bus",
                "route.id": route_id,
                "route.type": "bus",
                "response.format": "json",
            },
        )

        try:
            result = await fetch_bus_alerts(commons.r_client, route_id)

            span.set_attribute("alerts.count", result.count)
            add_span_attributes(
                span,
                {
                    "api.response.success": True,
                    "response.body.bytes": len(result.body.encode("utf-8")),
                },
            )
            return Response(content=result.body, media_type="application/json")
        except (ConnectionError, TimeoutError) as exc:
            logger.error(
                "Error getting bus alerts due to connection issue", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting bus alerts due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error(
                "Error getting bus alerts due to validation error", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/alerts/bus/{route_id}.json",
    summary="Get MBTA Bus Alerts for a Route (JSON File)",
    description="Get current MBTA alerts for a specific bus route as JSON file.",
    response_class=RedirectResponse,
)
@limiter.limit("100/minute")
async def get_bus_alerts_json(request: Request, route_id: str) -> Response:
    if not ROUTE_ID_PATTERN.fullmatch(route_id):
        return Response(
            content='{"detail": "Invalid route_id"}',
            status_code=400,
            media_type="application/json",
        )
    return RedirectResponse(url=f"/alerts/bus/{route_id}", status_code=302)
