import logging
import os
from datetime import datetime, timedelta
from typing import Awaitable, Callable, List

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from mbta_client import determine_station_id
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, ValidationError
from shared_types.shared_types import TrackAssignment, TrackPrediction
from starlette.middleware.base import BaseHTTPMiddleware

from track_predictor.track_predictor import TrackPredictionStats, TrackPredictor

# This is intended as a separate entrypoint to be run as a separate container

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)

logger = logging.getLogger(__name__)


class HeaderLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log request headers for debugging and monitoring"""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        headers_to_log = {
            "user-agent": request.headers.get("user-agent"),
            "x-forwarded-for": request.headers.get("x-forwarded-for"),
            "x-real-ip": request.headers.get("x-real-ip"),
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


app = FastAPI(
    title="MBTA Track Prediction API",
    description="API for predicting commuter rail track assignments",
    version="1.0.0",
    docs_url="/",
)

# Add header logging middleware
app.add_middleware(HeaderLoggingMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=os.environ.get(
        "IMT_TRACK_API_ORIGIN_REGEX", "http://localhost:1313"
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

instrumenter = Instrumentator().instrument(app).expose(app)


# Initialize track predictor
track_predictor = TrackPredictor()


class TrackPredictionResponse(BaseModel):
    success: bool
    prediction: TrackPrediction | str


class TrackPredictionStatsResponse(BaseModel):
    success: bool
    stats: TrackPredictionStats | str


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/predictions")
async def generate_track_prediction(
    station_id: str,
    route_id: str,
    trip_id: str,
    headsign: str,
    direction_id: int,
    scheduled_time: datetime,
) -> TrackPredictionResponse:
    """
    Generate a track prediction based on historical data.
    """
    try:
        station_id, has_track_predictions = determine_station_id(station_id)
        if not has_track_predictions:
            return TrackPredictionResponse(
                success=False,
                prediction="Predictions are not available for this station",
            )
        prediction = await track_predictor.predict_track(
            station_id=station_id,
            route_id=route_id,
            trip_id=trip_id,
            headsign=headsign,
            direction_id=direction_id,
            scheduled_time=scheduled_time,
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
            f"Error generating prediction due to connection issue: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logging.error(
            f"Error generating prediction due to validation error: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stats/{station_id}/{route_id}")
async def get_prediction_stats(
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
        stats = await track_predictor.get_prediction_stats(station_id, route_id)

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
async def get_historical_assignments(
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
        assignments = await track_predictor.get_historical_assignments(
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


def run_main() -> None:
    port = int(os.environ.get("IMT_TRACK_API_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
