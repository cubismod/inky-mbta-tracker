import logging

from aiohttp import ClientError
from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from api.models import ErrorResponse, RouteStopsResponse
from api.services.stops import StopsUpstreamError, fetch_route_stops
from consts import DAY
from fastapi import APIRouter, HTTPException, Request, Response
from mbta_client_extended import light_get_stop
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from pydantic import ValidationError
from redis import RedisError
from shared_types.shared_types import LightStop

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/stops/{route_id}",
    summary="Get MBTA stops for a route",
    description="Get stops for an MBTA route, cached for 30 days.",
    response_model=RouteStopsResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "MBTA API connection failed"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(30 * DAY)
async def get_route_stops(request: Request, commons: GET_DI, route_id: str) -> Response:
    with tracer.start_as_current_span("api.stops.get_route_stops") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {"api.endpoint": "stops", "response.format": "json", "route.id": route_id},
        )
        try:
            stops = await fetch_route_stops(commons.session, commons.r_client, route_id)
            add_span_attributes(
                span, {"api.response.success": True, "stops.count": len(stops.data)}
            )
            return Response(
                content=stops.model_dump_json(), media_type="application/json"
            )
        except StopsUpstreamError as exc:
            logger.error("Error getting route stops from MBTA", exc_info=exc)
            set_span_error(span, exc)
            raise HTTPException(
                status_code=exc.status_code, detail="MBTA API request failed"
            ) from exc
        except (ClientError, ConnectionError, TimeoutError) as exc:
            logger.error("Error connecting to MBTA for route stops", exc_info=exc)
            set_span_error(span, exc)
            raise HTTPException(
                status_code=502, detail="MBTA API connection failed"
            ) from exc
        except (ValidationError, RedisError) as exc:
            logger.error("Error getting route stops", exc_info=exc)
            set_span_error(span, exc)
            raise HTTPException(
                status_code=500, detail="Internal server error"
            ) from exc


@router.get(
    "/stop",
    summary="Get limited information about an MBTA stop",
    response_model=LightStop,
    responses={
        404: {"description": "Stop not found", "content": {}},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("10/minute")
@cache_ttl(20 * 60)
async def get_stop(request: Request, commons: GET_DI, id: str) -> Response:
    with tracer.start_as_current_span("api.stops.get_stop") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span, {"api.endpoint": "stop", "response.format": "json", "stop.id": id}
        )

        try:
            if commons.tg:
                light_stop = await light_get_stop(
                    commons.r_client, id, commons.tg, session=commons.session
                )
                if light_stop:
                    add_span_attributes(span, {"api.response.success": True})
                    return Response(
                        content=light_stop.model_dump_json(),
                        media_type="application/json",
                    )
                else:
                    add_span_attributes(span, {"api.response.success": False})
                    return Response(status_code=404)
            raise HTTPException(status_code=500, detail="Internal server error")
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error getting stop due to connection issue", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting stop due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error("Error getting stop due to validation error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")
