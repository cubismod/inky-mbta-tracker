import logging
from typing import Annotated

from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from api.services.predictions import MBTAUpstreamError, fetch_predictions
from fastapi import APIRouter, HTTPException, Query, Request, Response
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
