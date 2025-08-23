import asyncio
import json
import logging
import os
import random
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from queue import Full, Queue
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List

import aiohttp
import textdistance
import uvicorn
from ai_summarizer import (
    AISummarizer,
    JobPriority,
    SummarizationRequest,
    SummarizationResponse,
)
from config import Config, load_config
from consts import (
    ALERTS_CACHE_TTL,
    HOUR,
    MBTA_V3_ENDPOINT,
    MINUTE,
    SHAPES_CACHE_TTL,
    VEHICLES_CACHE_TTL,
)
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from geojson import FeatureCollection, dumps
from geojson_utils import collect_alerts, get_shapes_features, get_vehicle_features
from mbta_client import determine_station_id
from mbta_responses import AlertResource, Alerts
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, ValidationError
from redis.exceptions import RedisError
from shared_types.shared_types import TaskType, TrackAssignment, TrackPrediction
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
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
AI_SUMMARIZER = AISummarizer() if CONFIG.ollama.enabled else None

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


@retry(
    wait=wait_exponential_jitter(initial=2, jitter=5),
    stop=stop_after_attempt(3),
    retry=retry_if_not_exception_type((ValueError, ValidationError)),
    before_sleep=before_sleep_log(logger, logging.DEBUG),
    before=before_log(logger, logging.DEBUG),
)
async def fetch_alerts_with_retry(
    config: Config, session: aiohttp.ClientSession
) -> List[AlertResource]:
    """Fetch alerts with retry logic for rate limiting."""
    return await collect_alerts(config, session)


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
            except Full:
                # Queue might be full, log but don't fail the request
                logger.debug("Vehicles queue full; skipping TRAFFIC enqueue")

        response = await call_next(request)
        return response


# ============================================================================
# LIFESPAN EVENT HANDLER
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan event handler for startup and shutdown"""
    # Startup
    logger.info("Starting MBTA Transit Data API...")

    # Log file output configuration
    logger.debug(f"File output configuration - enabled: {CONFIG.file_output.enabled}")
    if CONFIG.file_output.enabled:
        logger.debug(f"File output directory: {CONFIG.file_output.output_directory}")
        logger.debug(f"File output filename: {CONFIG.file_output.filename}")
        logger.debug(
            f"File output include_timestamp: {CONFIG.file_output.include_timestamp}"
        )
        logger.debug(
            f"File output include_alert_count: {CONFIG.file_output.include_alert_count}"
        )
        logger.debug(
            f"File output include_model_info: {CONFIG.file_output.include_model_info}"
        )
    else:
        logger.debug("File output is disabled")

    # Start AI summarizer if enabled
    if AI_SUMMARIZER:
        try:
            await AI_SUMMARIZER.start()
            logger.info("AI summarizer started successfully")
            sleep_max = os.getenv("IMT_SUMMARY_SLEEP_MAX", "30")
            sleep_time = random.randint(0, int(sleep_max))

            # Immediately create a job for any existing alerts
            try:
                await asyncio.sleep(sleep_time)
                logger.info(
                    f"Sleeping for {sleep_time} seconds before creating initial AI summarization job"
                )

                logger.info(
                    "Creating initial AI summarization job for existing alerts..."
                )

                # Use retry logic for MBTA API call
                async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                    alerts = await fetch_alerts_with_retry(CONFIG, session)

                if alerts:
                    job_id = await AI_SUMMARIZER.queue_summary_job(
                        alerts,
                        priority=JobPriority.HIGH,  # High priority for startup
                        config={
                            "include_route_info": True,
                            "include_severity": True,
                        },
                    )
                    logger.info(
                        f"Initial startup: queued AI summary job {job_id} for {len(alerts)} alerts"
                    )
                else:
                    logger.info("No existing alerts to summarize on startup")
            except Exception as e:
                logger.warning(f"Failed to create initial AI summarization job: {e}")
                # Don't fail startup, just log the warning

            # Start background task for periodic AI summary refresh
            logger.info("Creating periodic AI summary refresh task...")
            background_task = asyncio.create_task(periodic_ai_summary_refresh())
            app.state.ai_summary_task = background_task
            logger.info(f"Started periodic AI summary refresh task: {background_task}")

            # Start background task for periodic individual alert summaries
            logger.info("Creating periodic individual alert summary task...")
            individual_task = asyncio.create_task(
                _periodic_individual_alert_summaries()
            )
            app.state.individual_summary_task = individual_task
            logger.info(
                f"Started periodic individual alert summary task: {individual_task}"
            )

            # Check if the tasks are actually running
            if background_task.done():
                logger.error("Periodic AI summary refresh task completed immediately!")
            else:
                logger.info("Periodic AI summary refresh task is running")

            if individual_task.done():
                logger.error(
                    "Periodic individual alert summary task completed immediately!"
                )
            else:
                logger.info("Periodic individual alert summary task is running")

        except Exception as e:
            logger.error(f"Failed to start AI summarizer: {e}", exc_info=True)
            # Don't fail startup, just log the error
    else:
        logger.info("AI summarizer not enabled")

    yield

    # Shutdown
    logger.info("Shutting down MBTA Transit Data API...")

    # Stop AI summarizer if enabled
    if AI_SUMMARIZER:
        try:
            # Cancel background task if it exists
            if hasattr(app.state, "ai_summary_task"):
                app.state.ai_summary_task.cancel()
                try:
                    await app.state.ai_summary_task
                except asyncio.CancelledError:
                    pass
                logger.info("Cancelled periodic AI summary refresh task")

            # Cancel individual alert summary task if it exists
            if hasattr(app.state, "individual_summary_task"):
                app.state.individual_summary_task.cancel()
                try:
                    await app.state.individual_summary_task
                except asyncio.CancelledError:
                    pass
                logger.info("Cancelled periodic individual alert summary task")

            await AI_SUMMARIZER.stop()
            await AI_SUMMARIZER.close()
            logger.info("AI summarizer stopped successfully")
        except Exception as e:
            logger.error(f"Failed to stop AI summarizer: {e}")


async def periodic_ai_summary_refresh() -> None:
    """Periodically refresh AI summaries for current alerts"""
    logger.info("Starting periodic AI summary refresh task")
    while True:
        try:
            sleep_time = random.randint(4 * MINUTE, 2 * HOUR)
            await asyncio.sleep(sleep_time)

            if AI_SUMMARIZER:
                try:
                    logger.info("Starting AI summary refresh cycle")

                    # Collect current alerts with retry logic
                    async with aiohttp.ClientSession(
                        base_url=MBTA_V3_ENDPOINT
                    ) as session:
                        alerts = await fetch_alerts_with_retry(CONFIG, session)

                    if alerts:
                        # Log some alert details for debugging
                        for i, alert in enumerate(alerts[:3]):  # Log first 3 alerts
                            logger.debug(
                                f"Alert {i + 1}: {alert.attributes.header} (ID: {alert.id})"
                            )

                        # Queue AI summary job with low priority for background refresh
                        job_id = await AI_SUMMARIZER.queue_summary_job(
                            alerts,
                            priority=JobPriority.LOW,
                            config={
                                "include_route_info": True,
                                "include_severity": True,
                            },
                        )
                        logger.info(
                            f"Periodic refresh: queued AI summary job {job_id} for {len(alerts)} alerts"
                        )
                    else:
                        logger.info(
                            "Periodic refresh: no alerts to summarize (this is normal if there are no active alerts)"
                        )

                except Exception as e:
                    logger.error(
                        f"Periodic AI summary refresh failed: {e}", exc_info=True
                    )
                    # Continue running even if this cycle fails

        except asyncio.CancelledError:
            logger.info("Periodic AI summary refresh task cancelled")
            break
        except Exception as e:
            logger.error(
                f"Unexpected error in periodic AI summary refresh: {e}", exc_info=True
            )
            # Wait a bit before retrying
            await asyncio.sleep(60)


async def _periodic_individual_alert_summaries() -> None:
    """Periodically generate individual alert summaries."""
    while True:
        try:
            logger.debug("Starting periodic individual alert summaries")

            # Fetch current alerts
            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                alerts = await fetch_alerts_with_retry(CONFIG, session)

            if not alerts:
                logger.warning("No alerts found for individual summaries")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
                continue

            # Queue individual summary jobs for each alert with 1-sentence limit
            if AI_SUMMARIZER:
                job_ids = AI_SUMMARIZER.queue_bulk_individual_summaries(
                    alerts, sentence_limit=1
                )
                logger.info(
                    f"Queued {len(job_ids)} individual alert summary jobs (1-sentence limit)"
                )

            sleep_time = random.randint(4 * MINUTE, 2 * HOUR)
            await asyncio.sleep(sleep_time)

        except Exception as e:
            logger.error(f"Error in periodic individual alert summaries: {e}")
            await asyncio.sleep(300)  # Wait 5 minutes before retrying


# ============================================================================
# FASTAPI APP SETUP AND CONFIGURATION
# ============================================================================

app = FastAPI(
    title="MBTA Transit Data API",
    description="API for MBTA transit data including track predictions, vehicle positions, alerts, and route shapes",
    version="2.0.0",
    docs_url="/",
    lifespan=lifespan,
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


class SummaryFormat(str, Enum):
    """Format options for AI summaries"""

    TEXT = "text"
    MARKDOWN = "markdown"
    JSON = "json"


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
        except (
            aiohttp.ClientError,
            RedisError,
            RuntimeError,
            ValueError,
            TypeError,
        ) as e:
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


def format_summary(summary: str, format_type: SummaryFormat) -> str:
    """Format a summary according to the requested format type."""
    if format_type == SummaryFormat.TEXT:
        return summary
    elif format_type == SummaryFormat.MARKDOWN:
        # Convert plain text to markdown format
        lines = summary.split("\n")
        formatted_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if line looks like a header (starts with common alert patterns)
            if any(
                line.lower().startswith(prefix)
                for prefix in [
                    "alert",
                    "route",
                    "effect",
                    "cause",
                    "severity",
                    "timeframe",
                ]
            ):
                formatted_lines.append(f"## {line}")
            elif line.endswith(":") and len(line) < 50:
                formatted_lines.append(f"**{line}**")
            else:
                formatted_lines.append(line)

        return "\n\n".join(formatted_lines)
    elif format_type == SummaryFormat.JSON:
        # Convert to structured JSON format
        try:
            # Try to parse as JSON first
            import json

            json.loads(summary)
            return summary  # Already JSON
        except (ValueError, TypeError):
            # Convert plain text to structured JSON
            lines = summary.split("\n")
            structured_data = {
                "summary": summary,
                "formatted_summary": {
                    "overview": lines[0] if lines else "",
                    "details": [line.strip() for line in lines[1:] if line.strip()],
                    "alert_count": len(
                        [line for line in lines if "alert" in line.lower()]
                    ),
                    "routes_affected": [
                        line for line in lines if "route" in line.lower()
                    ],
                    "effects": [
                        line
                        for line in lines
                        if "effect" in line.lower()
                        or "delay" in line.lower()
                        or "detour" in line.lower()
                    ],
                },
            }
            return json.dumps(structured_data, indent=2)
    else:
        return summary


def validate_ai_summary(
    summary: str, alerts: List[AlertResource], min_similarity: float = 0.105
) -> tuple[bool, float, str]:
    """
    Validate AI-generated summary using Levenshtein similarity.

    Args:
        summary: The AI-generated summary text
        alerts: List of input alerts
        min_similarity: Minimum similarity threshold (0.0 to 1.0)

    Returns:
        Tuple of (is_valid, similarity_score, reason)
    """
    try:
        if not alerts:
            return False, 0.0, "No alerts provided for validation"

        if not summary or len(summary.strip()) < 10:
            return False, 0.0, "Summary too short or empty"

        # Extract key information from alerts for comparison
        alert_texts = []
        for alert in alerts:
            # Combine header, effect, cause, and severity
            alert_text = f"{alert.attributes.header} "
            if alert.attributes.effect:
                alert_text += f"{alert.attributes.effect} "
            if alert.attributes.cause:
                alert_text += f"{alert.attributes.cause} "
            if alert.attributes.severity:
                alert_text += f"{alert.attributes.severity} "
            alert_texts.append(alert_text.strip())

        # Create a combined input text
        input_text = " ".join(alert_texts)

        # Calculate Levenshtein similarity
        similarity = textdistance.levenshtein.normalized_similarity(
            input_text.lower(), summary.lower()
        )

        # Additional validation checks
        is_valid = similarity >= min_similarity

        reason = ""
        if not is_valid:
            reason = f"Similarity {similarity:.3f} below threshold {min_similarity}"
        else:
            reason = f"Similarity {similarity:.3f} above threshold {min_similarity}"

        logger.debug(
            f"Summary validation - similarity: {similarity:.3f}, valid: {is_valid}, reason: {reason}"
        )

        return is_valid, similarity, reason

    except Exception as e:
        logger.error(f"Error validating AI summary: {e}", exc_info=True)
        return False, 0.0, f"Validation error: {str(e)}"


async def save_summary_to_file(
    summary: str, alerts: List[AlertResource], model_info: str, config: Config
) -> None:
    """Save AI summary to a local markdown file."""
    logger.debug(
        f"save_summary_to_file called - enabled: {config.file_output.enabled}, alerts: {len(alerts) if alerts else 0}"
    )

    if not config.file_output.enabled:
        logger.debug("File output is disabled, skipping file save")
        return

    # Validate summary if enabled
    if config.file_output.validate_summaries:
        is_valid, similarity, reason = validate_ai_summary(
            summary, alerts, config.file_output.min_similarity_threshold
        )

        if not is_valid:
            logger.warning(f"Summary validation failed: {reason}")
            logger.warning(
                f"Summary rejected - similarity {similarity:.3f} below threshold {config.file_output.min_similarity_threshold}"
            )
            return
        else:
            logger.info(f"Summary validation passed - similarity {similarity:.3f}")
    else:
        logger.debug("Summary validation disabled, proceeding with file save")

    try:
        import pathlib

        # Create output directory if it doesn't exist
        output_dir = pathlib.Path(config.file_output.output_directory)
        logger.debug(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create the full file path
        file_path = output_dir / config.file_output.filename
        logger.debug(f"Will save to file: {file_path}")

        # Check if directory exists and is writable
        if not output_dir.exists():
            logger.error(f"Output directory {output_dir} does not exist after mkdir")
            return

        if not output_dir.is_dir():
            logger.error(f"Output path {output_dir} is not a directory")
            return

        # Test write permissions
        try:
            test_file = output_dir / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
            logger.debug("Write permissions verified for output directory")
        except Exception as e:
            logger.error(f"Write permission test failed: {e}")
            return

        # Build markdown content
        markdown_content = []

        # Add header
        markdown_content.append("# MBTA Alerts Summary")
        markdown_content.append("")

        # Add metadata if configured
        if config.file_output.include_timestamp:
            markdown_content.append(
                f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            markdown_content.append("")

        if config.file_output.include_alert_count:
            markdown_content.append(f"**Active Alerts:** {len(alerts)}")
            markdown_content.append("")

        if config.file_output.include_model_info:
            markdown_content.append(f"**AI Model:** {model_info}")
            markdown_content.append("")

        # Add validation info if validation was performed
        if config.file_output.validate_summaries:
            is_valid, similarity, _ = validate_ai_summary(
                summary, alerts, config.file_output.min_similarity_threshold
            )
            markdown_content.append(
                f"**Validation:** PASSED (similarity: {similarity:.3f})"
            )
            markdown_content.append("")

        # Add summary content
        markdown_content.append("## Summary")
        markdown_content.append("")

        # Split summary into lines and format each line
        summary_lines = summary.split("\n")
        for line in summary_lines:
            line = line.strip()
            if not line:
                continue

            # Format headers and key information
            if any(
                line.lower().startswith(prefix)
                for prefix in [
                    "alert",
                    "route",
                    "effect",
                    "cause",
                    "severity",
                    "timeframe",
                    "overview",
                    "summary",
                ]
            ):
                markdown_content.append(f"### {line}")
            elif line.endswith(":") and len(line) < 50:
                markdown_content.append(f"**{line}**")
            else:
                markdown_content.append(line)

        markdown_content.append("")

        # Add alert details if available
        if alerts:
            markdown_content.append("## Alert Details")
            markdown_content.append("")

            for i, alert in enumerate(alerts, 1):
                markdown_content.append(f"### Alert {i}: {alert.attributes.header}")
                markdown_content.append("")

                if (
                    alert.attributes.short_header
                    and alert.attributes.short_header != alert.attributes.header
                ):
                    markdown_content.append(
                        f"**Short Header:** {alert.attributes.short_header}"
                    )
                    markdown_content.append("")

                if alert.attributes.effect:
                    markdown_content.append(f"**Effect:** {alert.attributes.effect}")

                if alert.attributes.severity:
                    markdown_content.append(
                        f"**Severity:** {alert.attributes.severity}"
                    )

                if alert.attributes.cause:
                    markdown_content.append(f"**Cause:** {alert.attributes.cause}")

                if alert.attributes.timeframe:
                    markdown_content.append(
                        f"**Timeframe:** {alert.attributes.timeframe}"
                    )

                markdown_content.append("")

        # Write to file
        logger.debug(f"Writing {len(markdown_content)} lines to {file_path}")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(markdown_content))

        logger.info(f"AI summary successfully saved to {file_path}")

        # Verify file was created
        if file_path.exists():
            file_size = file_path.stat().st_size
            logger.debug(f"File created successfully - size: {file_size} bytes")
        else:
            logger.error(f"File was not created at {file_path}")

    except Exception as e:
        logger.error(f"Failed to save summary to file: {e}", exc_info=True)
        # Don't fail the main operation if file saving fails


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
    except RedisError as e:
        logging.error(f"Error getting vehicles due to Redis error: {e}", exc_info=True)
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
    route: List[str] | None = Query(
        None, description="Filter by one or more routes (repeat param)"
    ),
) -> StreamingResponse:
    """
    Stream vehicle updates as SSE. Sends the full FeatureCollection whenever the
    cached payload changes. Also sends periodic heartbeats to keep the connection alive.
    """
    if not SSE_ENABLED:
        raise HTTPException(status_code=404, detail="SSE is disabled")

    cache_key = "api:vehicles"

    def filter_payload_bytes(payload: bytes) -> bytes:
        if not route:
            return payload
        try:
            obj = json.loads(payload)
            routes_set = {r.strip() for r in route if r and r.strip()}
            features = obj.get("features", [])

            def matches(r: str) -> bool:
                for wanted in routes_set:
                    if r == wanted or r.startswith(wanted):
                        return True
                return False

            obj["features"] = [
                f
                for f in features
                if matches(str(f.get("properties", {}).get("route", "")))
            ]
            return json.dumps(obj, separators=(",", ":")).encode("utf-8")
        except (ValueError, TypeError):
            return payload

    async def event_generator() -> Any:
        last_emitted: bytes | None = None
        last_heartbeat = datetime.now()

        # Send initial payload if available (or force-populate)
        try:
            data = await REDIS_CLIENT.get(cache_key)
            if not data:
                try:
                    # Populate cache if empty
                    initial = await get_vehicles_data(REDIS_CLIENT)
                    data = json.dumps(initial).encode("utf-8")
                except (
                    RedisError,
                    aiohttp.ClientError,
                    ValidationError,
                    ValueError,
                    TypeError,
                ):
                    data = None
            if data:
                filtered = filter_payload_bytes(data)
                last_emitted = filtered
                yield f"event: vehicles\ndata: {filtered.decode('utf-8')}\n\n"
        except RedisError as e:
            logger.error("Failed to send initial SSE payload (redis)", exc_info=e)

        while True:
            # Client disconnect check
            try:
                if await request.is_disconnected():
                    break
            except (RuntimeError, asyncio.CancelledError):
                break

            # Keep the background worker in TRAFFIC state while a client is connected
            try:
                VEHICLES_QUEUE.put_nowait(State.TRAFFIC)
            except Full:
                # Non-fatal; queue has pending work already
                pass

            try:
                current = await REDIS_CLIENT.get(cache_key)
                if current:
                    filtered = filter_payload_bytes(current)
                    if filtered != last_emitted:
                        last_emitted = filtered
                        yield f"event: vehicles\ndata: {filtered.decode('utf-8')}\n\n"
            except RedisError as e:
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
    except RedisError as e:
        logging.error(
            f"Error getting vehicles JSON due to Redis error: {e}", exc_info=True
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
                alerts = await fetch_alerts_with_retry(CONFIG, session)

            alerts_data = Alerts(data=alerts)
            alerts_json = alerts_data.model_dump_json(exclude_unset=True)

            await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)

            # Queue AI summary if enabled and there are alerts
            if AI_SUMMARIZER and alerts:
                try:
                    # Queue summary job with normal priority for regular refresh
                    job_id = await AI_SUMMARIZER.queue_summary_job(
                        alerts,
                        priority=JobPriority.NORMAL,
                        config={
                            "include_route_info": True,
                            "include_severity": True,
                        },
                    )
                    logger.info(
                        f"Queued AI summary job {job_id} for {len(alerts)} alerts during refresh"
                    )

                    # Save to local file if enabled (this will be updated when the summary job completes)
                    if CONFIG.file_output.enabled:
                        logger.debug(
                            "File output enabled - summary will be saved when AI job completes"
                        )

                except Exception as e:
                    logger.warning(
                        f"Failed to queue AI summary during alerts refresh: {e}"
                    )
                    # Don't fail the alerts request if AI summarization fails

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
    except RedisError as e:
        logging.error(f"Error getting alerts due to Redis error: {e}", exc_info=True)
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
                alerts = await fetch_alerts_with_retry(CONFIG, session)
            alerts_data = Alerts(data=alerts)
            alerts_json = alerts_data.model_dump_json(exclude_unset=True)

            await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)

            # Queue AI summary if enabled and there are alerts
            if AI_SUMMARIZER and alerts:
                try:
                    # Queue summary job with normal priority for regular refresh
                    job_id = await AI_SUMMARIZER.queue_summary_job(
                        alerts,
                        priority=JobPriority.NORMAL,
                        config={
                            "include_route_info": True,
                            "include_severity": True,
                        },
                    )
                    logger.info(
                        f"Queued AI summary job {job_id} for {len(alerts)} alerts during JSON refresh"
                    )

                    # Save to local file if enabled (this will be updated when the summary job completes)
                    if CONFIG.file_output.enabled:
                        logger.debug(
                            "File output enabled - summary will be saved when AI job completes"
                        )

                except Exception as e:
                    logger.warning(
                        f"Failed to queue AI summary during alerts JSON refresh: {e}"
                    )
                    # Don't fail the alerts request if AI summarization fails

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
    except RedisError as e:
        logging.error(
            f"Error getting alerts JSON due to Redis error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting alerts JSON due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# AI Summarizer Endpoints
@app.post(
    "/alerts/summarize",
    summary="Summarize MBTA Alerts with AI",
    description="Generate an AI-powered summary of current MBTA alerts using Ollama",
    response_model=SummarizationResponse,
)
@limiter.limit("20/minute")
async def summarize_alerts(
    request: SummarizationRequest, http_request: Request
) -> SummarizationResponse:
    """
    Generate an AI-powered summary of MBTA alerts.

    This endpoint uses Ollama to create concise, human-readable summaries
    of multiple alerts, identifying common themes and patterns.
    """
    if not AI_SUMMARIZER:
        raise HTTPException(
            status_code=503,
            detail="AI summarizer is not enabled. Please configure Ollama settings.",
        )

    try:
        # Get current alerts if none provided
        if not request.alerts:
            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                alerts = await fetch_alerts_with_retry(CONFIG, session)
                request.alerts = alerts

        # Generate summary
        summary_response = await AI_SUMMARIZER.summarize_alerts(request)

        # Apply formatting if requested
        if request.format != "text":
            summary_response.summary = format_summary(
                summary_response.summary, SummaryFormat(request.format)
            )

        # Cache the summary if we have alerts
        if request.alerts:
            cache_key = f"api:alerts:summary:{hash(str(request.alerts))}"
            cache_data = summary_response.model_dump_json()
            await REDIS_CLIENT.setex(cache_key, CONFIG.ollama.cache_ttl, cache_data)

        # Save to local file if enabled
        if CONFIG.file_output.enabled and request.alerts:
            logger.debug(
                f"File output enabled, saving summary for {len(request.alerts)} alerts"
            )
            await save_summary_to_file(
                summary_response.summary,
                request.alerts,
                AI_SUMMARIZER.config.model,
                CONFIG,
            )
        else:
            logger.debug(
                f"File output check - enabled: {CONFIG.file_output.enabled}, alerts: {len(request.alerts) if request.alerts else 0}"
            )

        return summary_response

    except Exception as e:
        logger.error(f"Failed to summarize alerts: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summary: {str(e)}"
        )


@app.get(
    "/alerts/summarize/health",
    summary="AI Summarizer Health Check",
    description="Check if the Ollama AI summarizer is healthy and available",
)
async def ai_summarizer_health() -> dict:
    """
    Check the health status of the AI summarizer.
    """
    if not AI_SUMMARIZER:
        return {
            "status": "disabled",
            "message": "AI summarizer is not enabled in configuration",
        }

    try:
        is_healthy = await AI_SUMMARIZER.health_check()
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "model": AI_SUMMARIZER.config.model,
            "endpoint": AI_SUMMARIZER.config.base_url,
            "enabled": True,
        }
    except Exception as e:
        logger.error(f"AI summarizer health check failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "enabled": True}


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
    except RedisError as e:
        logging.error(f"Error getting shapes due to Redis error: {e}", exc_info=True)
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
# AI SUMMARIZATION ENDPOINTS
# ============================================================================


@app.get(
    "/ai/test",
    summary="Test AI Summarization",
    description="Test endpoint to manually trigger AI summarization for debugging",
)
@limiter.limit("10/minute")
async def test_ai_summarization(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    format: SummaryFormat = Query(
        SummaryFormat.TEXT, description="Output format for the summary"
    ),
) -> dict:
    """Test AI summarization functionality."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")

    try:
        # Collect current alerts with retry logic
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)

        if not alerts:
            return {
                "status": "no_alerts",
                "message": "No active alerts to summarize",
                "alert_count": 0,
            }

        # Force generate a summary
        summary = await AI_SUMMARIZER.force_generate_summary(
            alerts[:3],  # Just use first 3 alerts for testing
            config={
                "include_route_info": True,
                "include_severity": True,
            },
        )

        # Apply formatting if requested
        if format != SummaryFormat.TEXT:
            summary = format_summary(summary, format)

        return {
            "status": "success",
            "message": "AI summary generated successfully",
            "alert_count": len(alerts[:3]),
            "summary": summary,
            "format": format,
            "model": AI_SUMMARIZER.config.model,
        }

    except Exception as e:
        logger.error(f"AI summarization test failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"AI summarization failed: {str(e)}"
        )


@app.get(
    "/ai/status",
    summary="AI Summarizer Status",
    description="Get the current status of the AI summarizer",
)
@limiter.limit("30/minute")
async def get_ai_status(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
) -> dict:
    """Get AI summarizer status."""
    if not AI_SUMMARIZER:
        return {"enabled": False, "message": "AI summarizer not enabled"}

    try:
        # Get basic status
        status = {
            "enabled": True,
            "model": AI_SUMMARIZER.config.model,
            "base_url": AI_SUMMARIZER.config.base_url,
            "timeout": AI_SUMMARIZER.config.timeout,
            "temperature": AI_SUMMARIZER.config.temperature,
        }

        # Get job queue status if available
        if hasattr(AI_SUMMARIZER, "job_queue"):
            queue = AI_SUMMARIZER.job_queue
            status.update(
                {
                    "queue_status": {
                        "pending_jobs": len(queue.pending_jobs),
                        "running_jobs": len(queue.running_jobs),
                        "completed_jobs": len(queue.completed_jobs),
                        "max_queue_size": queue.max_queue_size,
                        "max_concurrent_jobs": queue.max_concurrent_jobs,
                    }
                }
            )

        return status

    except Exception as e:
        logger.error(f"Failed to get AI status: {e}", exc_info=True)
        return {"enabled": True, "error": str(e)}


@app.get(
    "/ai/summaries",
    summary="Get AI Summarized Alerts",
    description="Get current alerts with AI-generated summaries",
)
@limiter.limit("30/minute")
async def get_ai_summaries(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
) -> Response:
    """
    Get current alerts with AI-generated summaries.
    """
    try:
        # Get alerts from cache
        cache_key = "api:alerts"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if not cached_data:
            raise HTTPException(status_code=504, detail="Alerts cache not warmed up")

        alerts_data = Alerts.model_validate_json(cached_data)
        alerts = alerts_data.data

        # Get summaries from cache
        summaries_cache_key = "api:alerts:summaries"
        cached_summaries = await REDIS_CLIENT.get(summaries_cache_key)
        if not cached_summaries:
            raise HTTPException(status_code=504, detail="Summaries cache not warmed up")

        # Parse summaries as JSON data, not as Alerts objects
        summaries_data = json.loads(cached_summaries)
        summaries = summaries_data.get("data", [])

        # Combine alerts and summaries
        combined_data = []
        for alert in alerts:
            alert_data: Dict[str, Any] = {
                "id": alert.id,
                "type": alert.type,
                "attributes": {
                    "header": alert.attributes.header,
                    "short_header": alert.attributes.short_header,
                    "effect": alert.attributes.effect,
                    "severity": alert.attributes.severity,
                    "cause": alert.attributes.cause,
                    "timeframe": alert.attributes.timeframe,
                    "created_at": alert.attributes.created_at,
                    "updated_at": alert.attributes.updated_at,
                },
            }

            # Add summary if available
            for summary in summaries:
                if summary.get("id") == alert.id:
                    # Create a new dict with the summary added
                    attributes = dict(alert_data["attributes"])
                    attributes["summary"] = summary.get("summary", "")
                    alert_data["attributes"] = attributes
                    break

            combined_data.append(alert_data)

        # Return combined data
        return Response(
            content=json.dumps({"data": combined_data}),
            media_type="application/json",
        )

    except HTTPException as e:
        raise e
    except (ConnectionError, TimeoutError) as e:
        logging.error(
            f"Error getting AI summaries due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except RedisError as e:
        logging.error(
            f"Error getting AI summaries due to Redis error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error getting AI summaries due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/ai/summaries/active",
    summary="Get AI Summaries for Active Alerts",
    description="Fetch current alerts and generate AI summaries for them on-demand",
)
@limiter.limit("20/minute")
async def get_active_alert_summaries(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    format: SummaryFormat = Query(
        SummaryFormat.TEXT, description="Output format for the summaries"
    ),
) -> dict:
    """Get AI summaries for all currently active alerts."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")

    try:
        # Collect current alerts from MBTA API with retry logic
        logger.info("Fetching active alerts for AI summarization...")
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)

        if not alerts:
            return {
                "status": "no_alerts",
                "message": "No active alerts to summarize",
                "alert_count": 0,
                "summaries": [],
            }

        logger.info(f"Found {len(alerts)} active alerts, generating AI summaries...")

        # Generate AI summaries for all alerts
        summaries = []
        for alert in alerts:
            try:
                # Generate summary for this alert
                summary = await AI_SUMMARIZER.force_generate_summary(
                    [alert],  # Single alert
                    config={
                        "include_route_info": True,
                        "include_severity": True,
                    },
                )

                # Validate individual summary if validation is enabled
                if CONFIG.file_output.validate_summaries:
                    is_valid, similarity, reason = validate_ai_summary(
                        summary, [alert], CONFIG.file_output.min_similarity_threshold
                    )

                    if not is_valid:
                        logger.warning(
                            f"Active alerts summary validation failed for alert {alert.id}: {reason}"
                        )
                        logger.warning(
                            f"Summary rejected - similarity {similarity:.3f} below threshold {CONFIG.file_output.min_similarity_threshold}"
                        )
                        # Add error entry instead of invalid summary
                        summaries.append(
                            {
                                "alert_id": alert.id,
                                "alert_header": alert.attributes.header,
                                "alert_short_header": alert.attributes.short_header,
                                "alert_effect": alert.attributes.effect,
                                "alert_severity": alert.attributes.severity,
                                "alert_cause": alert.attributes.cause,
                                "alert_timeframe": alert.attributes.timeframe,
                                "ai_summary": f"Summary validation failed: {reason}",
                                "format": format,
                                "model_used": AI_SUMMARIZER.config.model,
                                "generated_at": datetime.now().isoformat(),
                                "error": True,
                                "validation_failed": True,
                                "similarity_score": similarity,
                            }
                        )
                        continue
                    else:
                        logger.debug(
                            f"Active alerts summary validation passed for alert {alert.id} - similarity {similarity:.3f}"
                        )

                # Apply formatting if requested
                if format != SummaryFormat.TEXT:
                    summary = format_summary(summary, format)

                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": summary,
                        "format": format,
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                    }
                )

            except Exception as e:
                logger.warning(f"Failed to generate summary for alert {alert.id}: {e}")
                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": f"Error generating summary: {str(e)}",
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                        "error": True,
                    }
                )

        # Cache the summaries for future use
        try:
            cache_key = f"ai:summaries:active:{int(datetime.now().timestamp() / 300)}"  # 5-minute cache
            cache_data = {
                "alerts": summaries,
                "generated_at": datetime.now().isoformat(),
                "alert_count": len(alerts),
                "model_used": AI_SUMMARIZER.config.model,
            }
            await REDIS_CLIENT.setex(
                cache_key, 300, json.dumps(cache_data)
            )  # 5-minute TTL
            logger.info(f"Cached active alert summaries with key: {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to cache summaries: {e}")
            # Don't fail the request if caching fails

        # Save to local file if enabled
        if CONFIG.file_output.enabled and alerts:
            # Create a combined summary for the file
            combined_summary = "Active Alerts Summary\n\n"
            for summary_data in summaries:
                if not summary_data.get("error"):
                    combined_summary += f"## {summary_data['alert_header']}\n\n"
                    combined_summary += f"{summary_data['ai_summary']}\n\n"
                    if summary_data.get("alert_effect"):
                        combined_summary += (
                            f"**Effect:** {summary_data['alert_effect']}\n"
                        )
                    if summary_data.get("alert_severity"):
                        combined_summary += (
                            f"**Severity:** {summary_data['alert_severity']}\n"
                        )
                    if summary_data.get("alert_cause"):
                        combined_summary += (
                            f"**Cause:** {summary_data['alert_cause']}\n"
                        )
                    combined_summary += "\n"

            await save_summary_to_file(
                combined_summary, alerts, AI_SUMMARIZER.config.model, CONFIG
            )

        return {
            "status": "success",
            "message": f"Generated AI summaries for {len(alerts)} active alerts",
            "alert_count": len(alerts),
            "model_used": AI_SUMMARIZER.config.model,
            "generated_at": datetime.now().isoformat(),
            "summaries": summaries,
        }

    except Exception as e:
        logger.error(f"Failed to get active alert summaries: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summaries: {str(e)}"
        )


@app.get(
    "/ai/summaries/cached",
    summary="Get Cached AI Summaries",
    description="Get the most recently cached AI summaries for alerts",
)
@limiter.limit("30/minute")
async def get_cached_alert_summaries(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
) -> dict:
    """Get the most recently cached AI summaries for alerts."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")

    try:
        # Look for the most recent cached summaries
        cache_pattern = "ai:summaries:active:*"
        cache_keys = await REDIS_CLIENT.keys(cache_pattern)

        if not cache_keys:
            return {
                "status": "no_cache",
                "message": "No cached summaries found",
                "cached_summaries": None,
            }

        # Sort keys by timestamp (newest first)
        cache_keys.sort(reverse=True)
        latest_key = cache_keys[0]

        # Get the cached data
        cached_data = await REDIS_CLIENT.get(latest_key)
        if not cached_data:
            return {
                "status": "cache_expired",
                "message": "Cached summaries have expired",
                "cached_summaries": None,
            }

        # Parse the cached data
        summaries_data = json.loads(cached_data)

        return {
            "status": "success",
            "message": "Retrieved cached summaries",
            "cache_key": latest_key,
            "cached_summaries": summaries_data,
        }

    except Exception as e:
        logger.error(f"Failed to get cached summaries: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve cached summaries: {str(e)}"
        )


@app.get(
    "/ai/summaries/alert/{alert_id}",
    summary="Get AI Summary for Specific Alert",
    description="Generate or retrieve AI summary for a specific alert by ID",
)
@limiter.limit("30/minute")
async def get_alert_summary_by_id(
    request: Request,  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    alert_id: str,
    format: SummaryFormat = Query(
        SummaryFormat.TEXT, description="Output format for the summary"
    ),
) -> dict:
    """Get AI summary for a specific alert by ID."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")

    try:
        # First check if we have a cached summary for this alert
        cache_pattern = "ai:summaries:active:*"
        cache_keys = await REDIS_CLIENT.keys(cache_pattern)

        # Look through cached summaries for this specific alert
        for cache_key in sorted(cache_keys, reverse=True):
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                summaries_data = json.loads(cached_data)
                for summary in summaries_data.get("summaries", []):
                    if summary.get("alert_id") == alert_id:
                        # Apply formatting if requested
                        if format != SummaryFormat.TEXT:
                            summary["ai_summary"] = format_summary(
                                summary["ai_summary"], format
                            )
                            summary["format"] = format

                        return {
                            "status": "success",
                            "message": "Retrieved cached summary",
                            "cache_source": cache_key,
                            "summary": summary,
                        }

        # If no cached summary found, try to fetch the alert and generate a summary
        logger.info(
            f"No cached summary found for alert {alert_id}, fetching from MBTA API..."
        )

        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            # Try to get alerts and find the specific one with retry logic
            alerts = await fetch_alerts_with_retry(CONFIG, session)

            target_alert = None
            for alert in alerts:
                if alert.id == alert_id:
                    target_alert = alert
                    break

            if not target_alert:
                raise HTTPException(
                    status_code=404, detail=f"Alert {alert_id} not found"
                )

            # Generate summary for this specific alert
            summary = await AI_SUMMARIZER.force_generate_summary(
                [target_alert],
                config={
                    "include_route_info": True,
                    "include_severity": True,
                },
            )

            # Apply formatting if requested
            if format != SummaryFormat.TEXT:
                summary = format_summary(summary, format)

            summary_data = {
                "alert_id": target_alert.id,
                "alert_header": target_alert.attributes.header,
                "alert_short_header": target_alert.attributes.short_header,
                "alert_effect": target_alert.attributes.effect,
                "alert_severity": target_alert.attributes.severity,
                "alert_cause": target_alert.attributes.cause,
                "alert_timeframe": target_alert.attributes.timeframe,
                "ai_summary": summary,
                "format": format,
                "model_used": AI_SUMMARIZER.config.model,
                "generated_at": datetime.now().isoformat(),
                "cache_source": "generated_on_demand",
            }

            return {
                "status": "success",
                "message": "Generated summary on-demand",
                "summary": summary_data,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get summary for alert {alert_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summary: {str(e)}"
        )


@app.post("/ai/summaries/bulk")
@limiter.limit("10/minute")
async def generate_bulk_alert_summaries(
    request: Request,  # Required for rate limiting
    alert_ids: List[str],
    style: str = Query(
        default="comprehensive",
        description="Summary style - 'comprehensive' (detailed) or 'concise' (brief, 1-2 sentences per alert)",
    ),
    format: SummaryFormat = Query(
        default=SummaryFormat.TEXT, description="Output format for the summaries"
    ),
) -> dict:
    """Generate AI summaries for multiple specific alerts."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")

    if not alert_ids:
        raise HTTPException(status_code=400, detail="No alert IDs provided")

    if len(alert_ids) > 20:
        raise HTTPException(
            status_code=400, detail="Maximum 20 alerts allowed per request"
        )

    try:
        logger.info(f"Generating bulk summaries for {len(alert_ids)} alerts...")

        # Fetch all alerts from MBTA API with retry logic
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            all_alerts = await fetch_alerts_with_retry(CONFIG, session)

        # Find the requested alerts
        target_alerts = []
        for alert_id in alert_ids:
            for alert in all_alerts:
                if alert.id == alert_id:
                    target_alerts.append(alert)
                    break

        if not target_alerts:
            return {
                "status": "no_alerts",
                "message": "None of the requested alerts were found",
                "requested_count": len(alert_ids),
                "found_count": 0,
                "summaries": [],
            }

        logger.info(f"Found {len(target_alerts)} of {len(alert_ids)} requested alerts")

        # Generate summaries for each alert
        summaries = []
        for alert in target_alerts:
            try:
                summary = await AI_SUMMARIZER.force_generate_summary(
                    [alert],
                    config={
                        "include_route_info": True,
                        "include_severity": True,
                        "style": style,
                    },
                )

                # Validate individual summary if validation is enabled
                if CONFIG.file_output.validate_summaries:
                    is_valid, similarity, reason = validate_ai_summary(
                        summary, [alert], CONFIG.file_output.min_similarity_threshold
                    )

                    if not is_valid:
                        logger.warning(
                            f"Bulk summary validation failed for alert {alert.id}: {reason}"
                        )
                        logger.warning(
                            f"Summary rejected - similarity {similarity:.3f} below threshold {CONFIG.file_output.min_similarity_threshold}"
                        )
                        # Add error entry instead of invalid summary
                        summaries.append(
                            {
                                "alert_id": alert.id,
                                "alert_header": alert.attributes.header,
                                "alert_short_header": alert.attributes.short_header,
                                "alert_effect": alert.attributes.effect,
                                "alert_severity": alert.attributes.severity,
                                "alert_cause": alert.attributes.cause,
                                "alert_timeframe": alert.attributes.timeframe,
                                "ai_summary": f"Summary validation failed: {reason}",
                                "format": format,
                                "model_used": AI_SUMMARIZER.config.model,
                                "generated_at": datetime.now().isoformat(),
                                "error": True,
                                "validation_failed": True,
                                "similarity_score": similarity,
                            }
                        )
                        continue
                    else:
                        logger.debug(
                            f"Bulk summary validation passed for alert {alert.id} - similarity {similarity:.3f}"
                        )

                # Apply formatting if requested
                if format != SummaryFormat.TEXT:
                    summary = format_summary(summary, format)

                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": summary,
                        "format": format,
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                    }
                )

            except Exception as e:
                logger.warning(f"Failed to generate summary for alert {alert.id}: {e}")
                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": f"Error generating summary: {str(e)}",
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                        "error": True,
                    }
                )

        # Save to local file if enabled
        if CONFIG.file_output.enabled and target_alerts:
            # Create a combined summary for the file
            combined_summary = f"Bulk Alerts Summary ({style} style)\n\n"
            for summary_data in summaries:
                if not summary_data.get("error"):
                    combined_summary += f"## {summary_data['alert_header']}\n\n"
                    combined_summary += f"{summary_data['ai_summary']}\n\n"
                    if summary_data.get("alert_effect"):
                        combined_summary += (
                            f"**Effect:** {summary_data['alert_effect']}\n"
                        )
                    if summary_data.get("alert_severity"):
                        combined_summary += (
                            f"**Severity:** {summary_data['alert_severity']}\n"
                        )
                    if summary_data.get("alert_cause"):
                        combined_summary += (
                            f"**Cause:** {summary_data['alert_cause']}\n"
                        )
                    combined_summary += "\n"

            await save_summary_to_file(
                combined_summary, target_alerts, AI_SUMMARIZER.config.model, CONFIG
            )

        return {
            "status": "success",
            "message": f"Generated summaries for {len(target_alerts)} alerts",
            "requested_count": len(alert_ids),
            "found_count": len(target_alerts),
            "model_used": AI_SUMMARIZER.config.model,
            "generated_at": datetime.now().isoformat(),
            "summaries": summaries,
        }

    except Exception as e:
        logger.error(f"Failed to generate bulk summaries: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summaries: {str(e)}"
        )


@app.get("/ai/summaries/individual/{alert_id}")
async def get_individual_alert_summary(
    alert_id: str,
    sentence_limit: int = Query(
        default=2,
        ge=1,
        le=5,
        description="Maximum number of sentences for the summary (1 for ultra-concise, 2+ for standard concise)",
    ),
    format: SummaryFormat = Query(
        default=SummaryFormat.TEXT, description="Output format"
    ),
) -> Dict[str, Any]:
    """Get AI summary for a specific individual alert."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not available")

    try:
        # Check if we have a cached individual summary
        cache_key = f"ai_summary_individual:{alert_id}"
        cached_summary = await REDIS_CLIENT.get(cache_key)

        if cached_summary:
            summary_data = json.loads(cached_summary)
            logger.info(f"Returning cached individual summary for alert {alert_id}")

            # Format the summary if requested
            if format != SummaryFormat.TEXT:
                summary_data["summary"] = format_summary(
                    summary_data["summary"], format
                )

            return summary_data

        # If no cached summary, return a message indicating it needs to be generated
        return {
            "alert_id": alert_id,
            "summary": f"Individual summary not yet generated. Use the generate endpoint to create a {sentence_limit}-sentence summary.",
            "status": "pending",
            "message": f"Individual summaries are generated periodically. Check back later or use the generate endpoint for a {sentence_limit}-sentence summary.",
            "sentence_limit": sentence_limit,
        }

    except Exception as e:
        logger.error(f"Error fetching individual alert summary for {alert_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching individual summary: {str(e)}"
        )


@app.post("/ai/summaries/individual/generate")
async def generate_individual_alert_summary(
    alert_id: str,
    style: str = Query(
        default="concise",
        description="Summary style - 'concise' (1-2 sentences) or 'comprehensive' (detailed)",
    ),
    sentence_limit: int = Query(
        default=2,
        ge=1,
        le=5,
        description="Maximum number of sentences for the summary (1 for ultra-concise, 2+ for standard concise)",
    ),
    format: SummaryFormat = Query(
        default=SummaryFormat.TEXT, description="Output format"
    ),
) -> Dict[str, Any]:
    """Force generation of an individual alert summary."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not available")

    try:
        # Fetch the specific alert
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)

        # Find the specific alert
        target_alert = None
        for alert in alerts:
            if alert.id == alert_id:
                target_alert = alert
                break

        if not target_alert:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

        # Generate the individual summary
        summary = await AI_SUMMARIZER.summarize_individual_alert(
            target_alert, style=style, sentence_limit=sentence_limit
        )

        # Validate summary if validation is enabled
        if CONFIG.file_output.validate_summaries:
            is_valid, similarity, reason = validate_ai_summary(
                summary, [target_alert], CONFIG.file_output.min_similarity_threshold
            )

            if not is_valid:
                logger.warning(
                    f"Individual summary validation failed for alert {alert_id}: {reason}"
                )
                logger.warning(
                    f"Summary rejected - similarity {similarity:.3f} below threshold {CONFIG.file_output.min_similarity_threshold}"
                )
                raise HTTPException(
                    status_code=422,
                    detail=f"Generated summary failed validation: {reason}. Similarity: {similarity:.3f}",
                )
            else:
                logger.info(
                    f"Individual summary validation passed for alert {alert_id} - similarity {similarity:.3f}"
                )

        # Format the summary if requested
        if format != SummaryFormat.TEXT:
            summary = format_summary(summary, format)

        # Store in cache
        cache_key = f"ai_summary_individual:{alert_id}"
        summary_data = {
            "alert_id": alert_id,
            "summary": summary,
            "style": style,
            "sentence_limit": sentence_limit,
            "generated_at": datetime.now().isoformat(),
            "format": format.value,
        }

        await REDIS_CLIENT.setex(
            cache_key, 3600, json.dumps(summary_data)
        )  # 1 hour TTL

        # Save to local file if enabled
        if CONFIG.file_output.enabled and target_alert:
            # Create a summary for the file
            file_summary = "Individual Alert Summary\n\n"
            file_summary += f"## {target_alert.attributes.header}\n\n"
            file_summary += f"{summary}\n\n"
            if target_alert.attributes.effect:
                file_summary += f"**Effect:** {target_alert.attributes.effect}\n"
            if target_alert.attributes.severity:
                file_summary += f"**Severity:** {target_alert.attributes.severity}\n"
            if target_alert.attributes.cause:
                file_summary += f"**Cause:** {target_alert.attributes.cause}\n"
            if target_alert.attributes.timeframe:
                file_summary += f"**Timeframe:** {target_alert.attributes.timeframe}\n"
            file_summary += (
                f"\n**Style:** {style}\n**Sentence Limit:** {sentence_limit}\n"
            )

            await save_summary_to_file(
                file_summary, [target_alert], AI_SUMMARIZER.config.model, CONFIG
            )

        return summary_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating individual alert summary for {alert_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error generating individual summary: {str(e)}"
        )


@app.post("/ai/summaries/generate")
async def generate_ai_summary_with_style(
    alerts: List[AlertResource],
    style: str = Query(
        default="comprehensive",
        description="Summary style - 'comprehensive' (detailed) or 'concise' (brief, 1-2 sentences per alert)",
    ),
    format: SummaryFormat = Query(
        default=SummaryFormat.TEXT, description="Output format for the summary"
    ),
    include_route_info: bool = Query(
        default=True, description="Include route information"
    ),
    include_severity: bool = Query(
        default=True, description="Include severity information"
    ),
) -> Dict[str, Any]:
    """Generate AI summary with specified style and format."""
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not available")

    try:
        # Create summarization request with style
        request = SummarizationRequest(
            alerts=alerts,
            include_route_info=include_route_info,
            include_severity=include_severity,
            format=format.value,
            style=style,
        )

        # Generate summary
        response = await AI_SUMMARIZER.summarize_alerts(request)

        # Extract the summary text
        summary = response.summary

        # Format the summary if requested
        if format != SummaryFormat.TEXT:
            summary = format_summary(summary, format)

        # Save to local file if enabled
        if CONFIG.file_output.enabled and alerts:
            logger.debug(
                f"File output enabled, saving summary for {len(alerts)} alerts"
            )
            await save_summary_to_file(
                summary, alerts, AI_SUMMARIZER.config.model, CONFIG
            )
        else:
            logger.debug(
                f"File output check - enabled: {CONFIG.file_output.enabled}, alerts: {len(alerts) if alerts else 0}"
            )

        return {
            "summary": summary,
            "alert_count": len(alerts),
            "style": style,
            "format": format.value,
            "model_used": AI_SUMMARIZER.config.model,
            "generated_at": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error generating AI summary with style {style}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error generating summary: {str(e)}"
        )


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
        except (
            RedisError,
            aiohttp.ClientError,
            ValidationError,
            ValueError,
            TypeError,
        ) as e:
            logging.error("Failed to warm vehicles cache", exc_info=e)
            return False

    async def warm_alerts_cache() -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                alerts = await fetch_alerts_with_retry(CONFIG, session)
                feature_collection = FeatureCollection(alerts)
                geojson_str = dumps(feature_collection, sort_keys=True)

                # Warm both regular and JSON caches
                await asyncio.gather(
                    REDIS_CLIENT.setex("api:alerts", ALERTS_CACHE_TTL, geojson_str),
                    REDIS_CLIENT.setex(
                        "api:alerts:json", ALERTS_CACHE_TTL, geojson_str
                    ),
                )

                # Queue AI summary if enabled and there are alerts
                if AI_SUMMARIZER and alerts:
                    try:
                        # Queue summary job with low priority for background processing
                        job_id = await AI_SUMMARIZER.queue_summary_job(
                            alerts,
                            priority=JobPriority.LOW,
                            config={
                                "include_route_info": True,
                                "include_severity": True,
                            },
                        )
                        logger.info(
                            f"Queued AI summary job {job_id} for {len(alerts)} alerts during cache warming"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to queue AI summary during cache warming: {e}"
                        )
                        # Don't fail cache warming if AI summarization fails

                return True
        except (
            RedisError,
            aiohttp.ClientError,
            ValidationError,
            ValueError,
            TypeError,
        ) as e:
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
        except (
            RedisError,
            aiohttp.ClientError,
            ValidationError,
            ValueError,
            TypeError,
        ) as e:
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
