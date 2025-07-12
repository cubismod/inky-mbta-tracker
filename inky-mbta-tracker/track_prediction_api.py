import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List

import click
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mbta_client import determine_station_id
from pydantic import BaseModel
from shared_types.schema_versioner import schema_versioner
from shared_types.shared_types import TrackAssignment, TrackPrediction
from track_predictor import TrackPredictionStats, TrackPredictor

# This is intended as a separate entrypoint to be run as a separate container

origins = [
    "http://localhost:8080",
    "https://mbta.ryanwallace.cloud",
    "http://h.cubemoji.art:54478",
    os.environ.get("IMT_TRACK_API_ORIGIN", "http://localhost:1313"),
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
app = FastAPI(
    title="MBTA Track Prediction API",
    description="API for predicting commuter rail track assignments",
    version="1.0.0",
    docs_url="/",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        prediction = await track_predictor.predict_track(
            station_id=determine_station_id(station_id),
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

    except Exception as e:
        logging.error(f"Error generating prediction: {e}")
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
        stats = await track_predictor.get_prediction_stats(
            determine_station_id(station_id), route_id
        )

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

    except Exception as e:
        logging.error(f"Error getting stats for {station_id}/{route_id}: {e}")
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

        assignments = await track_predictor.get_historical_assignments(
            determine_station_id(station_id), route_id, start_date, end_date
        )

        return [assignment for assignment in assignments]

    except Exception as e:
        logging.error(f"Error getting historical data for {station_id}/{route_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# @app.get("/")
# async def root() -> dict:
#     """Root endpoint with API information"""
#     return {
#         "name": "MBTA Track Prediction API",
#         "version": "1.0.0",
#         "description": "API for predicting commuter rail track assignments",
#         "endpoints": {
#             "predictions": "/predictions/{station_id}",
#             "generate": "/predictions (POST)",
#             "stats": "/stats/{station_id}/{route_id}",
#             "historical": "/historical/{station_id}/{route_id}",
#             "health": "/health",
#         },
#     }


@click.command()
def run_main() -> None:
    port = int(os.environ.get("IMT_TRACK_API_PORT", "8080"))
    with asyncio.Runner() as runner:
        runner.run(schema_versioner())
        uvicorn.run(app, host="0.0.0.0", port=port)
