import logging
from typing import Annotated

from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from api.models import DeparturesResponse, ErrorResponse
from api.services.predictions import (
    MBTAUpstreamError,
    fetch_predictions,
    fetch_stop_departures,
)
from fastapi import APIRouter, HTTPException, Query, Request, Response
from mbta_responses import Predictions
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from pydantic import ValidationError
from redis.exceptions import RedisError

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/predictions",
    summary="Get MBTA Predictions",
    description=(
        "Get current MBTA predictions filtered by trip_id, latitude/longitude, or both."
    ),
    response_model=Predictions,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid filter combination"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(5)
async def get_predictions(
    request: Request,
    commons: GET_DI,
    trip_id: Annotated[
        str | None, Query(description="MBTA trip ID to pass as filter[trip].")
    ] = None,
    latitude: Annotated[
        float | None,
        Query(ge=-90, le=90, description="Latitude to pass as filter[latitude]."),
    ] = None,
    longitude: Annotated[
        float | None,
        Query(ge=-180, le=180, description="Longitude to pass as filter[longitude]."),
    ] = None,
    radius: Annotated[
        float | None,
        Query(gt=0, description="Radius to pass as filter[radius]."),
    ] = None,
) -> Response:
    with tracer.start_as_current_span("api.predictions.get_predictions") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "predictions",
                "response.format": "json",
                "trip.id": trip_id,
                "filter.latitude": latitude is not None,
                "filter.longitude": longitude is not None,
                "filter.radius": radius is not None,
            },
        )

        try:
            if not trip_id and latitude is None and longitude is None:
                raise HTTPException(
                    status_code=400,
                    detail="Specify trip_id or both latitude and longitude",
                )
            if (latitude is None) != (longitude is None):
                raise HTTPException(
                    status_code=400,
                    detail="latitude and longitude must be specified together",
                )
            if radius is not None and (latitude is None or longitude is None):
                raise HTTPException(
                    status_code=400,
                    detail="radius requires latitude and longitude",
                )

            result = await fetch_predictions(
                commons.session,
                commons.r_client,
                trip_id=trip_id,
                latitude=latitude,
                longitude=longitude,
                radius=radius,
            )
            add_span_attributes(
                span,
                {
                    "predictions.count": result.count,
                    "api.response.success": True,
                    "response.body.bytes": len(result.body.encode("utf-8")),
                },
            )
            return Response(content=result.body, media_type="application/json")
        except MBTAUpstreamError as exc:
            logger.error(
                "Error getting predictions due to MBTA upstream response",
                exc_info=True,
            )
            set_span_error(span, exc)
            add_span_attributes(
                span,
                {
                    "api.response.success": False,
                    "error.type": "upstream",
                    "http.status_code": exc.status_code,
                },
            )
            raise HTTPException(
                status_code=exc.status_code, detail="MBTA API request failed"
            )
        except (ConnectionError, TimeoutError) as exc:
            logger.error(
                "Error getting predictions due to connection issue", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting predictions due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error(
                "Error getting predictions due to validation error", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/predictions/departures",
    summary="Get upcoming departures for a stop",
    description=(
        "Simplified departures for an MBTA stop, filterable by route and "
        "direction. Enriched with alerting and bikes_allowed flags. Designed "
        "for transit displays."
    ),
    response_model=DeparturesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid filter combination"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(5)
async def get_stop_departures(
    request: Request,
    commons: GET_DI,
    stop: Annotated[
        str,
        Query(
            description="MBTA stop ID to pass as filter[stop]. Parent stations include their platforms."
        ),
    ],
    route: Annotated[
        str | None, Query(description="MBTA route ID to pass as filter[route].")
    ] = None,
    direction: Annotated[
        int | None,
        Query(ge=0, le=1, description="Direction ID to pass as filter[direction_id]."),
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=50, description="Max departures to return (page[limit]).")
    ] = 10,
) -> Response:
    with tracer.start_as_current_span("api.predictions.get_stop_departures") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "predictions.departures",
                "response.format": "json",
                "filter.stop": stop,
                "filter.route": route,
                "filter.direction_id": direction,
                "filter.limit": limit,
            },
        )

        try:
            result = await fetch_stop_departures(
                commons.session,
                commons.r_client,
                stop=stop,
                route=route,
                direction=direction,
                limit=limit,
            )
            add_span_attributes(
                span,
                {
                    "predictions.count": len(result.departures),
                    "api.response.success": True,
                },
            )
            return Response(
                content=result.model_dump_json(), media_type="application/json"
            )
        except MBTAUpstreamError as exc:
            logger.error(
                "Error getting departures due to MBTA upstream response",
                exc_info=True,
            )
            set_span_error(span, exc)
            add_span_attributes(
                span,
                {
                    "api.response.success": False,
                    "error.type": "upstream",
                    "http.status_code": exc.status_code,
                },
            )
            raise HTTPException(
                status_code=exc.status_code, detail="MBTA API request failed"
            )
        except (ConnectionError, TimeoutError) as exc:
            logger.error(
                "Error getting departures due to connection issue", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting departures due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error(
                "Error getting departures due to validation error", exc_info=True
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")
