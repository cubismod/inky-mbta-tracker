import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from functools import wraps
from queue import Queue
from typing import Any, Awaitable, Callable, List

import aiohttp
import uvicorn
from config import load_config
from consts import (
    ALERTS_CACHE_TTL,
    MBTA_V3_ENDPOINT,
    SHAPES_CACHE_TTL,
    VEHICLES_CACHE_TTL,
)
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from geojson import FeatureCollection, dumps
from geojson_utils import collect_alerts, get_shapes_features, get_vehicle_features
from mbta_client import determine_station_id
from mbta_responses import Alerts
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, ValidationError
from shared_types.shared_types import TaskType, TrackAssignment, TrackPrediction
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
from track_predictor.track_predictor import TrackPredictionStats, TrackPredictor
from utils import get_redis, get_vehicles_data, thread_runner
from vehicles_background_worker import State

# ============================================================================
# CONFIGURATION AND GLOBALS
# ============================================================================

# This is intended as a separate entrypoint to be run as a separate container
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# Global constants and instances
VEHICLES_QUEUE = Queue[State]()
REDIS_CLIENT = get_redis()
CONFIG = load_config()
TRACK_PREDICTOR = TrackPredictor()

# API timeout configurations
API_REQUEST_TIMEOUT = int(os.environ.get("IMT_API_REQUEST_TIMEOUT", "30"))  # seconds
TRACK_PREDICTION_TIMEOUT = int(
    os.environ.get("IMT_TRACK_PREDICTION_TIMEOUT", "15")
)  # seconds
RATE_LIMITING_ENABLED = os.getenv("IMT_RATE_LIMITING_ENABLED", "true").lower() == "true"
SSE_ENABLED = os.getenv("IMT_SSE_ENABLED", "true").lower() == "true"


# ============================================================================
# MIDDLEWARE AND UTILITIES
# ============================================================================


class HeaderLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log request headers for debugging and monitoring"""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        headers_to_log = {
            "user-agent": request.headers.get("user-agent"),
            "x-forwarded-for": request.headers.get("x-forwarded-for"),
            "x-real-ip": request.headers.get("x-real-ip"),
            "origin": request.headers.get("origin"),
            "authorization": "***"
            if request.headers.get("authorization")
            else None,  # Mask sensitive data
            "content-type": request.headers.get("content-type"),
            "accept": request.headers.get("accept"),
            "cf-connecting-ip": request.headers.get("cf-connecting-ip"),
            "cf-ipcountry": request.headers.get("cf-ipcountry"),
            "cf-ray": request.headers.get("cf-ray"),
            "cf-request-id": request.headers.get("cf-request-id"),
            "cf-request-priority": request.headers.get("cf-request-priority"),
        }

        # Filter out None values
        headers_to_log = {k: v for k, v in headers_to_log.items() if v is not None}

        logger.debug(
            f"{request.method} {request.url.path} from {request.client.host if request.client else 'unknown'} "
            f"- Headers: {headers_to_log}"
        )

        response = await call_next(request)
        logger.debug(f"Response status: {response.status_code}")
        return response


def get_client_ip(request: Request) -> str:
    """Get client IP, handling Cloudflare proxy headers"""
    # Check for Cloudflare's real IP header first
    if "CF-Connecting-IP" in request.headers:
        return request.headers["CF-Connecting-IP"]
    # Fallback to standard proxy headers
    if "X-Forwarded-For" in request.headers:
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    if "X-Real-IP" in request.headers:
        return request.headers["X-Real-IP"]
    # Default to remote address
    return get_remote_address(request)


class NoOpLimiter:
    """No-op limiter for when rate limiting is disabled"""

    def limit(self, rate: str) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            return wrapper

        return decorator


class TrafficMonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to queue TRAFFIC state messages for background vehicle data processing"""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # Skip queuing for health check endpoint (used by synthetic monitoring)
        if request.url.path != "/health" and request.url.path != "/metrics":
            # Queue a TRAFFIC message for the background worker
            try:
                VEHICLES_QUEUE.put_nowait(State.TRAFFIC)
                logger.debug("Queued TRAFFIC state for background worker")
            except Exception as e:
                # Queue might be full, log but don't fail the request
                logger.warning(f"Failed to queue TRAFFIC state: {e}")

        response = await call_next(request)
        return response


# ============================================================================
# FASTAPI APP SETUP AND CONFIGURATION
# ============================================================================

app = FastAPI(
    title="MBTA Transit Data API",
    description="API for MBTA transit data including track predictions, vehicle positions, alerts, and route shapes",
    version="2.0.0",
    docs_url="/",
    servers=[
        {"url": "https://imt.ryanwallace.cloud", "description": "Production"},
    ],
    swagger_ui_parameters={
        "defaultModelsExpandDepth": 1,
        "defaultModelExpandDepth": 1,
        "displayRequestDuration": True,
        "docExpansion": "none",
        "tryItOutEnabled": True,  # Keep enabled for track prediction endpoints
    },
)

# Configure rate limiting
if RATE_LIMITING_ENABLED:
    limiter = Limiter(key_func=get_client_ip)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore
else:
    limiter = NoOpLimiter()  # type: ignore

# Add middleware
app.add_middleware(TrafficMonitoringMiddleware)
app.add_middleware(HeaderLoggingMiddleware)

origins = [
    origin.strip()
    for origin in os.environ.get(
        "IMT_API_ORIGIN", "http://localhost:1313,http://localhost:8080"
    ).split(",")
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add instrumentation
instrumenter = Instrumentator().instrument(app).expose(app)


# ============================================================================
# PYDANTIC MODELS
# ============================================================================


class TrackPredictionResponse(BaseModel):
    success: bool
    prediction: TrackPrediction | str


class TrackPredictionStatsResponse(BaseModel):
    success: bool
    stats: TrackPredictionStats | str


class PredictionRequest(BaseModel):
    station_id: str
    route_id: str
    trip_id: str
    headsign: str
    direction_id: int
    scheduled_time: datetime


class ChainedPredictionsRequest(BaseModel):
    predictions: List[PredictionRequest]


class ChainedPredictionsResponse(BaseModel):
    results: List[TrackPredictionResponse]


# ============================================================================
# API ENDPOINTS
# ============================================================================


# Health Check
@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# Track Prediction Endpoints
@app.post("/predictions")
@limiter.limit("100/minute")
async def generate_track_prediction(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    prediction_request: PredictionRequest,
) -> TrackPredictionResponse:
    """
    Generate a track prediction based on historical data.
    """
    try:

        async def _generate_prediction() -> TrackPredictionResponse:
            station_id_resolved, has_track_predictions = determine_station_id(
                prediction_request.station_id
            )
            if not has_track_predictions:
                return TrackPredictionResponse(
                    success=False,
                    prediction="Predictions are not available for this station",
                )
            prediction = await TRACK_PREDICTOR.predict_track(
                station_id=station_id_resolved,
                route_id=prediction_request.route_id,
                trip_id=prediction_request.trip_id,
                headsign=prediction_request.headsign,
                direction_id=prediction_request.direction_id,
                scheduled_time=prediction_request.scheduled_time,
            )

            if prediction:
                return TrackPredictionResponse(
                    success=True,
                    prediction=prediction,
                )
            else:
                return TrackPredictionResponse(
                    success=False,
                    prediction="No prediction could be generated",
                )

        return await asyncio.wait_for(
            _generate_prediction(), timeout=TRACK_PREDICTION_TIMEOUT
        )

    except asyncio.TimeoutError:
        logging.error(
            f"Track prediction request timed out after {TRACK_PREDICTION_TIMEOUT} seconds"
        )
        return TrackPredictionResponse(
            success=False,
            prediction="Request timed out",
        )
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error generating prediction due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error generating prediction due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/chained-predictions")
@limiter.limit("50/minute")
async def generate_chained_track_predictions(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    chained_request: ChainedPredictionsRequest,
) -> ChainedPredictionsResponse:
    """
    Generate multiple track predictions in a single request.
    """

    async def process_single_prediction(
        pred_request: PredictionRequest,
    ) -> TrackPredictionResponse:
        try:
            station_id, has_track_predictions = determine_station_id(
                pred_request.station_id
            )
            if not has_track_predictions:
                return TrackPredictionResponse(
                    success=False,
                    prediction="Predictions are not available for this station",
                )

            prediction = await TRACK_PREDICTOR.predict_track(
                station_id=station_id,
                route_id=pred_request.route_id,
                trip_id=pred_request.trip_id,
                headsign=pred_request.headsign,
                direction_id=pred_request.direction_id,
                scheduled_time=pred_request.scheduled_time,
            )

            if prediction:
                return TrackPredictionResponse(
                    success=True,
                    prediction=prediction,
                )
            else:
                return TrackPredictionResponse(
                    success=False,
                    prediction="No prediction could be generated",
                )

        except (ConnectionError, TimeoutError) as e:
            logging.error(
                "Error generating chained prediction due to connection issue",
                exc_info=e,
            )
            return TrackPredictionResponse(
                success=False,
                prediction="Connection error occurred",
            )
        except ValidationError as e:
            logging.error(
                "Error generating chained prediction due to validation error",
                exc_info=e,
            )
            return TrackPredictionResponse(
                success=False,
                prediction="Validation error occurred",
            )
        except Exception as e:
            logging.error("Unexpected error generating chained prediction", exc_info=e)
            return TrackPredictionResponse(
                success=False,
                prediction="Unexpected error occurred",
            )

    tasks = [
        process_single_prediction(pred_request)
        for pred_request in chained_request.predictions
    ]
    results = await asyncio.gather(*tasks)

    return ChainedPredictionsResponse(
        results=list(results),
    )


@app.get("/stats/{station_id}/{route_id}")
@limiter.limit("50/minute")
async def get_prediction_stats(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    station_id: str,
    route_id: str,
) -> TrackPredictionStatsResponse:
    """
    Get prediction statistics for a station and route.
    """
    try:
        station_id, has_track_predictions = determine_station_id(station_id)
        if not has_track_predictions:
            return TrackPredictionStatsResponse(
                success=False,
                stats="Prediction stats are not available for this station",
            )
        stats = await TRACK_PREDICTOR.get_prediction_stats(station_id, route_id)

        if stats:
            return TrackPredictionStatsResponse(
                success=True,
                stats=stats,
            )
        else:
            return TrackPredictionStatsResponse(
                success=False,
                stats="No statistics available",
            )

    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting stats for {station_id}/{route_id} due to connection issue: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting stats for {station_id}/{route_id} due to validation error: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/historical/{station_id}/{route_id}")
@limiter.limit("50/minute")
async def get_historical_assignments(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    station_id: str,
    route_id: str,
    days: int = Query(30, description="Number of days to look back"),
) -> List[TrackAssignment]:
    """
    Get historical track assignments for analysis.
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        station_id, has_track_predictions = determine_station_id(station_id)
        if not has_track_predictions:
            return []
        assignments = await TRACK_PREDICTOR.get_historical_assignments(
            station_id, route_id, start_date, end_date
        )

        return [assignment for assignment in assignments]

    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting historical data for {station_id}/{route_id} due to connection issue: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting historical data for {station_id}/{route_id} due to validation error: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


# Vehicle Data Endpoints
@app.get(
    "/vehicles",
    summary="Get Vehicle Positions",
    description="Get current vehicle positions as GeoJSON FeatureCollection. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    response_description="GeoJSON FeatureCollection with vehicle positions",
    openapi_extra={
        "responses": {
            "200": {
                "description": "GeoJSON FeatureCollection with vehicle positions",
                "content": {
                    "application/json": {
                        "example": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "id": "vehicle-123",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [-71.0589, 42.3601],
                                    },
                                    "properties": {
                                        "route": "Red",
                                        "status": "IN_TRANSIT_TO",
                                        "marker-color": "#FA2D27",
                                        "speed": 25.0,
                                        "direction": 0,
                                    },
                                }
                            ],
                        }
                    }
                },
            }
        }
    },
)
@limiter.limit("70/minute")
async def get_vehicles(request: Request) -> dict:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get current vehicle positions as GeoJSON FeatureCollection. Also includes the next/current stop GeoJSON data.
    """
    try:
        return await asyncio.wait_for(
            get_vehicles_data(REDIS_CLIENT), timeout=API_REQUEST_TIMEOUT
        )

    except asyncio.TimeoutError:
        logging.error(
            f"Vehicle data request timed out after {API_REQUEST_TIMEOUT} seconds"
        )
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting vehicles due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting vehicles due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/vehicles/stream",
    summary="Stream Vehicle Positions (SSE)",
    description=(
        "Server-Sent Events stream of vehicle positions. Emits a new event whenever the"
        " vehicles cache updates. Disable via IMT_SSE_ENABLED=false."
    ),
    response_class=StreamingResponse,
)
@limiter.limit("20/minute")
async def stream_vehicles(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    poll_interval: float = Query(
        1.0, ge=0.1, le=10.0, description="Poll interval seconds"
    ),
    heartbeat_interval: float = Query(
        15.0, ge=1.0, le=120.0, description="Heartbeat interval seconds"
    ),
) -> StreamingResponse:
    """
    Stream vehicle updates as SSE. Sends the full FeatureCollection whenever the
    cached payload changes. Also sends periodic heartbeats to keep the connection alive.
    """
    if not SSE_ENABLED:
        raise HTTPException(status_code=404, detail="SSE is disabled")

    cache_key = "api:vehicles"

    async def event_generator() -> Any:
        last_payload: bytes | None = None
        last_heartbeat = datetime.now()

        # Send initial payload if available (or force-populate)
        try:
            data = await REDIS_CLIENT.get(cache_key)
            if not data:
                try:
                    # Populate cache if empty
                    initial = await get_vehicles_data(REDIS_CLIENT)
                    data = json.dumps(initial).encode("utf-8")
                except Exception:
                    data = None
            if data:
                last_payload = data
                yield f"event: vehicles\ndata: {data.decode('utf-8')}\n\n"
        except Exception as e:
            logger.error("Failed to send initial SSE payload", exc_info=e)

        while True:
            # Client disconnect check
            try:
                if await request.is_disconnected():
                    break
            except Exception:
                break

            try:
                current = await REDIS_CLIENT.get(cache_key)
                if current and current != last_payload:
                    last_payload = current
                    yield f"event: vehicles\ndata: {current.decode('utf-8')}\n\n"
            except Exception as e:
                logger.error("SSE loop error while reading cache", exc_info=e)

            # Heartbeat
            now = datetime.now()
            if (now - last_heartbeat).total_seconds() >= heartbeat_interval:
                last_heartbeat = now
                yield f": keep-alive {now.isoformat()}\n\n"

            await asyncio.sleep(poll_interval)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )


@app.get(
    "/vehicles.json",
    summary="Get Vehicle Positions (JSON File)",
    description="Get current vehicle positions as GeoJSON file. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    response_class=Response,
)
@limiter.limit("70/minute")
async def get_vehicles_json(request: Request) -> Response:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get current vehicle positions as GeoJSON file (for compatibility).
    """
    try:

        async def _get_vehicles_json_data() -> Response:
            cache_key = "api:vehicles:json"
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                    headers={
                        "Content-Disposition": "attachment; filename=vehicles.json"
                    },
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

        return await asyncio.wait_for(
            _get_vehicles_json_data(), timeout=API_REQUEST_TIMEOUT
        )

    except asyncio.TimeoutError:
        logging.error(
            f"Vehicle JSON request timed out after {API_REQUEST_TIMEOUT} seconds"
        )
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting vehicles JSON due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting vehicles JSON due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# Alerts Endpoints
@app.get(
    "/alerts",
    summary="Get MBTA Alerts",
    description="Get current MBTA alerts. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    openapi_extra={
        "responses": {
            "200": {
                "description": "MBTA alerts data",
                "content": {
                    "application/json": {
                        "example": {
                            "data": [
                                {
                                    "id": "alert-123",
                                    "type": "alert",
                                    "attributes": {
                                        "cause": "MAINTENANCE",
                                        "effect": "DELAY",
                                        "header": "Service Alert",
                                        "description": "Delays expected on Red Line",
                                    },
                                }
                            ]
                        }
                    }
                },
            }
        }
    },
)
@limiter.limit("100/minute")
async def get_alerts(request: Request) -> Response:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get current MBTA alerts.
    """
    try:

        async def _get_alerts_data() -> Response:
            cache_key = "api:alerts"
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                )

            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                alerts = await collect_alerts(CONFIG, session)

            alerts_data = Alerts(data=alerts)
            alerts_json = alerts_data.model_dump_json(exclude_unset=True)

            await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)

            return Response(
                content=alerts_json,
                media_type="application/json",
            )

        return await asyncio.wait_for(_get_alerts_data(), timeout=API_REQUEST_TIMEOUT)

    except asyncio.TimeoutError:
        logging.error(f"Alerts request timed out after {API_REQUEST_TIMEOUT} seconds")
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting alerts due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting alerts due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/alerts.json",
    summary="Get MBTA Alerts (JSON File)",
    description="Get current MBTA alerts as JSON file. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    response_class=Response,
)
@limiter.limit("100/minute")
async def get_alerts_json(request: Request) -> Response:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get current MBTA alerts as JSON file (for compatibility).
    """
    try:

        async def _get_alerts_json_data() -> Response:
            cache_key = "api:alerts:json"
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                    headers={"Content-Disposition": "attachment; filename=alerts.json"},
                )

            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                alerts = await collect_alerts(CONFIG, session)
            alerts_data = Alerts(data=alerts)
            alerts_json = alerts_data.model_dump_json(exclude_unset=True)

            await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)

            return Response(
                content=alerts_json,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=alerts.json"},
            )

        return await asyncio.wait_for(
            _get_alerts_json_data(), timeout=API_REQUEST_TIMEOUT
        )

    except asyncio.TimeoutError:
        logging.error(
            f"Alerts JSON request timed out after {API_REQUEST_TIMEOUT} seconds"
        )
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting alerts JSON due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting alerts JSON due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# Shapes Endpoints
@app.get(
    "/shapes",
    summary="Get Route Shapes",
    description="Get route shapes as GeoJSON FeatureCollection. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    openapi_extra={
        "responses": {
            "200": {
                "description": "GeoJSON FeatureCollection with route shapes",
                "content": {
                    "application/json": {
                        "example": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "LineString",
                                        "coordinates": [
                                            [-71.0589, 42.3601],
                                            [-71.0575, 42.3584],
                                        ],
                                    },
                                    "properties": {"route": "Red"},
                                }
                            ],
                        }
                    }
                },
            }
        }
    },
)
@limiter.limit("70/minute")
async def get_shapes(request: Request) -> Response:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get route shapes as GeoJSON FeatureCollection.
    """
    try:

        async def _get_shapes_data() -> Response:
            cache_key = "api:shapes"
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                return json.loads(cached_data)

            features = await get_shapes_features(CONFIG, REDIS_CLIENT)
            result = {"type": "FeatureCollection", "features": features}

            await REDIS_CLIENT.setex(cache_key, SHAPES_CACHE_TTL, json.dumps(result))

            return Response(
                content=json.dumps(result),
                media_type="application/json",
            )

        return await asyncio.wait_for(_get_shapes_data(), timeout=API_REQUEST_TIMEOUT)

    except asyncio.TimeoutError:
        logging.error(f"Shapes request timed out after {API_REQUEST_TIMEOUT} seconds")
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting shapes due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting shapes due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/shapes.json",
    summary="Get Route Shapes (JSON File)",
    description="Get route shapes as GeoJSON file. ⚠️ **WARNING: Do not use 'Try it out' - large response may crash browser!**",
    response_class=Response,
)
@limiter.limit("70/minute")
async def get_shapes_json(request: Request) -> Response:  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    """
    Get route shapes as GeoJSON file (for compatibility).
    """
    try:

        async def _get_shapes_json_data() -> Response:
            cache_key = "api:shapes:json"
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                    headers={"Content-Disposition": "attachment; filename=shapes.json"},
                )

            features = await get_shapes_features(CONFIG, REDIS_CLIENT)
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)

            await REDIS_CLIENT.setex(cache_key, SHAPES_CACHE_TTL, geojson_str)

            return Response(
                content=geojson_str,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=shapes.json"},
            )

        return await asyncio.wait_for(
            _get_shapes_json_data(), timeout=API_REQUEST_TIMEOUT
        )

    except asyncio.TimeoutError:
        logging.error(
            f"Shapes JSON request timed out after {API_REQUEST_TIMEOUT} seconds"
        )
        raise HTTPException(status_code=504, detail="Request timed out")
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting shapes JSON due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting shapes JSON due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# CACHE WARMING UTILITIES
# ============================================================================


async def warm_all_caches_internal() -> dict[str, bool]:
    """
    Internal utility to warm all API endpoint caches concurrently.
    Used for optimizing cache performance during startup or maintenance.

    Returns:
        Dictionary indicating success/failure for each cache type
    """
    results = {}

    async def warm_vehicles_cache() -> bool:
        try:
            features = await get_vehicle_features(REDIS_CLIENT)
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)

            # Warm both regular and JSON caches
            await asyncio.gather(
                REDIS_CLIENT.setex("api:vehicles", VEHICLES_CACHE_TTL, geojson_str),
                REDIS_CLIENT.setex(
                    "api:vehicles:json", VEHICLES_CACHE_TTL, geojson_str
                ),
            )
            return True
        except Exception as e:
            logging.error("Failed to warm vehicles cache", exc_info=e)
            return False

    async def warm_alerts_cache() -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                alerts = await collect_alerts(CONFIG, session)
                feature_collection = FeatureCollection(alerts)
                geojson_str = dumps(feature_collection, sort_keys=True)

                # Warm both regular and JSON caches
                await asyncio.gather(
                    REDIS_CLIENT.setex("api:alerts", ALERTS_CACHE_TTL, geojson_str),
                    REDIS_CLIENT.setex(
                        "api:alerts:json", ALERTS_CACHE_TTL, geojson_str
                    ),
                )
                return True
        except Exception as e:
            logging.error("Failed to warm alerts cache", exc_info=e)
            return False

    async def warm_shapes_cache() -> bool:
        try:
            features = await get_shapes_features(CONFIG, REDIS_CLIENT)
            feature_collection = FeatureCollection(features)
            geojson_str = dumps(feature_collection, sort_keys=True)

            # Warm both regular and JSON caches
            await asyncio.gather(
                REDIS_CLIENT.setex("api:shapes", SHAPES_CACHE_TTL, geojson_str),
                REDIS_CLIENT.setex("api:shapes:json", SHAPES_CACHE_TTL, geojson_str),
            )
            return True
        except Exception as e:
            logging.error("Failed to warm shapes cache", exc_info=e)
            return False

    # Warm all caches concurrently
    cache_tasks = [
        ("vehicles", warm_vehicles_cache()),
        ("alerts", warm_alerts_cache()),
        ("shapes", warm_shapes_cache()),
    ]

    cache_results = await asyncio.gather(
        *[task for _, task in cache_tasks], return_exceptions=True
    )

    for i, (cache_name, _) in enumerate(cache_tasks):
        result = cache_results[i]
        if isinstance(result, Exception):
            logging.error(f"Error warming {cache_name} cache", exc_info=result)
            results[cache_name] = False
        elif isinstance(result, bool):
            results[cache_name] = result
        else:
            results[cache_name] = False

    return results


# ============================================================================
# MAIN FUNCTION
# ============================================================================


def run_main() -> None:
    thr = threading.Thread(
        target=thread_runner,
        kwargs={
            "target": TaskType.VEHICLES_BACKGROUND_WORKER,
            "queue": None,
            "vehicles_queue": VEHICLES_QUEUE,
        },
        name="vehicles_background_worker",
    )
    thr.start()
    port = int(os.environ.get("IMT_API_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
