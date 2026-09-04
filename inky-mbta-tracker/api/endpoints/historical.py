import logging

from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from api.models import (
    ErrorResponse,
    HistoricalVehicleCountsResponse,
    HistoricalVehicleSpeedsResponse,
    HistoricalVehiclesResponse,
)
from api.services.historical import (
    fetch_historical_snapshots,
    fetch_historical_vehicle_counts,
    fetch_historical_vehicle_speeds,
)
from fastapi import APIRouter, HTTPException, Request, Response
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from redis.exceptions import RedisError

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/historical/vehicles",
    summary="Get historical vehicle snapshots",
    description=(
        "Snapshot history of vehicle positions recorded by the worker, returned "
        "as a time series sorted by recording timestamp."
    ),
    response_model=HistoricalVehiclesResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(15 * 60)
async def get_historical_vehicles(request: Request, commons: GET_DI) -> Response:
    with tracer.start_as_current_span("api.historical.get_historical_vehicles") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "historical.vehicles",
                "response.format": "json",
            },
        )

        try:
            result = await fetch_historical_snapshots(commons.r_client)
            add_span_attributes(
                span,
                {
                    "historical.snapshots.count": len(result.snapshots),
                    "api.response.success": True,
                },
            )
            return Response(
                content=result.model_dump_json(), media_type="application/json"
            )
        except RedisError as exc:
            logger.error(
                "Error getting historical data due to Redis error", exc_info=exc
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/historical/vehicles/counts",
    summary="Get historical vehicle counts by MBTA line and vehicle type",
    description=(
        "Return vehicle counts grouped by vehicle type (light rail, heavy rail, "
        "regional rail, bus) across main line groups (RL, GL, BL, OL, SL, CR) for "
        "each historical snapshot, sorted by recording timestamp."
    ),
    response_model=HistoricalVehicleCountsResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(15 * 60)
async def get_historical_vehicle_counts(request: Request, commons: GET_DI) -> Response:
    with tracer.start_as_current_span(
        "api.historical.get_historical_vehicle_counts"
    ) as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "historical.vehicles.counts",
                "response.format": "json",
            },
        )

        try:
            result = await fetch_historical_vehicle_counts(commons.r_client)
            add_span_attributes(
                span,
                {
                    "historical.snapshots.count": len(result.snapshots),
                    "api.response.success": True,
                },
            )
            return Response(
                content=result.model_dump_json(), media_type="application/json"
            )
        except RedisError as exc:
            logger.error(
                "Error getting historical counts due to Redis error", exc_info=exc
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/historical/vehicles/speeds",
    summary="Get historical vehicle speed statistics by MBTA line",
    description=(
        "Return average, minimum, and maximum vehicle speeds per main line group "
        "(RL, GL, BL, OL, SL, CR) for each historical snapshot, sorted by "
        "recording timestamp. Stopped and speedless vehicles are excluded."
    ),
    response_model=HistoricalVehicleSpeedsResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("70/minute")
@cache_ttl(15 * 60)
async def get_historical_vehicle_speeds(request: Request, commons: GET_DI) -> Response:
    with tracer.start_as_current_span(
        "api.historical.get_historical_vehicle_speeds"
    ) as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "historical.vehicles.speeds",
                "response.format": "json",
            },
        )

        try:
            result = await fetch_historical_vehicle_speeds(commons.r_client)
            add_span_attributes(
                span,
                {
                    "historical.snapshots.count": len(result.snapshots),
                    "api.response.success": True,
                },
            )
            return Response(
                content=result.model_dump_json(), media_type="application/json"
            )
        except RedisError as exc:
            logger.error(
                "Error getting historical speeds due to Redis error", exc_info=exc
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
