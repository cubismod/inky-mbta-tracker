import logging
from datetime import datetime, timedelta

from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from api.models import (
    ErrorResponse,
    HistoricalVehicleCountsResponse,
    HistoricalVehiclesEarliestResponse,
    HistoricalVehiclesLatestResponse,
    HistoricalVehicleSpeedsResponse,
    HistoricalVehiclesResponse,
)
from api.services.historical import (
    fetch_earliest_historical_vehicle_timestamp,
    fetch_historical_snapshots,
    fetch_historical_vehicle_counts,
    fetch_historical_vehicle_speeds,
    fetch_latest_historical_vehicle_timestamp,
)
from fastapi import APIRouter, HTTPException, Query, Request, Response
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from redis.exceptions import RedisError

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/historical/vehicles/earliest",
    summary="Get the earliest available vehicle snapshot timestamp",
    description=(
        "Return the earliest vehicle snapshot still retained in Redis for use as "
        "a timelapse start time. earliest_timestamp is an ISO 8601 UTC timestamp "
        "with fractional seconds preserved, or null when no history is available. "
        "History can expire after this request; snapshot requests may return empty."
    ),
    response_model=HistoricalVehiclesEarliestResponse,
    responses={500: {"model": ErrorResponse, "description": "Internal server error"}},
)
@limiter.limit("70/minute")
async def get_earliest_historical_vehicle_timestamp(
    request: Request, commons: GET_DI
) -> Response:
    with tracer.start_as_current_span(
        "api.historical.get_earliest_historical_vehicle_timestamp"
    ) as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "historical.vehicles.earliest",
                "response.format": "json",
            },
        )
        try:
            result = await fetch_earliest_historical_vehicle_timestamp(commons.r_client)
            span.set_attribute("api.response.success", True)
            return Response(
                content=result.model_dump_json(),
                media_type="application/json",
                headers={"Cache-Control": "no-store"},
            )
        except RedisError as exc:
            logger.error(
                "Error getting earliest historical timestamp due to Redis error",
                exc_info=exc,
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/historical/vehicles/latest",
    summary="Get the latest available vehicle snapshot timestamp",
    description=(
        "Return the latest vehicle snapshot still retained in Redis for use as "
        "a timelapse upper bound. latest_timestamp is an ISO 8601 UTC timestamp "
        "with fractional seconds preserved, or null when no history is available. "
        "Snapshot range end times are exclusive; use an end after this timestamp."
    ),
    response_model=HistoricalVehiclesLatestResponse,
    responses={500: {"model": ErrorResponse, "description": "Internal server error"}},
)
@limiter.limit("70/minute")
async def get_latest_historical_vehicle_timestamp(
    request: Request, commons: GET_DI
) -> Response:
    with tracer.start_as_current_span(
        "api.historical.get_latest_historical_vehicle_timestamp"
    ) as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "api.endpoint": "historical.vehicles.latest",
                "response.format": "json",
            },
        )
        try:
            result = await fetch_latest_historical_vehicle_timestamp(commons.r_client)
            span.set_attribute("api.response.success", True)
            return Response(
                content=result.model_dump_json(),
                media_type="application/json",
                headers={"Cache-Control": "no-store"},
            )
        except RedisError as exc:
            logger.error(
                "Error getting latest historical timestamp due to Redis error",
                exc_info=exc,
            )
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/historical/vehicles",
    summary="Get historical vehicle snapshots",
    description=(
        "Snapshot history of vehicle positions recorded by the worker, returned "
        "as a time series sorted by recording timestamp. Request at most one hour "
        "per call; the end timestamp is exclusive."
    ),
    response_model=HistoricalVehiclesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid timestamp range"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
@limiter.limit("500/minute")
@cache_ttl(15 * 60)
async def get_historical_vehicles(
    request: Request,
    commons: GET_DI,
    start: datetime = Query(
        description="Inclusive start timestamp in ISO 8601 format with timezone."
    ),
    end: datetime = Query(
        description="Exclusive end timestamp in ISO 8601 format with timezone."
    ),
) -> Response:
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
            if start.tzinfo is None or start.utcoffset() is None:
                raise HTTPException(
                    status_code=400,
                    detail="start must include a timezone",
                )
            if end.tzinfo is None or end.utcoffset() is None:
                raise HTTPException(
                    status_code=400,
                    detail="end must include a timezone",
                )
            if end <= start or end - start > timedelta(hours=1):
                raise HTTPException(
                    status_code=400,
                    detail="start must precede end and the range cannot exceed one hour",
                )

            result = await fetch_historical_snapshots(commons.r_client, start, end)
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
        "regional rail, bus, ferry) across main line groups (RL, MT, GL, BL, OL, SL, CR, F) for "
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
        "(RL, MT, GL, BL, OL, SL, CR) for each historical snapshot, sorted by "
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
