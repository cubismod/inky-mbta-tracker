import logging
import os
from datetime import datetime, timedelta
from typing import List

import click
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from track_predictor import TrackPredictor

# This is intended as a separate entrypoint to be run as a separate container

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

# Initialize track predictor
track_predictor = TrackPredictor()


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
) -> dict:
    """
    Generate a track prediction for a specific trip.

    Args:
        station_id: The MBTA station ID
        route_id: The route ID
        trip_id: The trip ID
        headsign: The trip headsign
        direction_id: The direction ID
        scheduled_time: The scheduled departure time

    Returns:
        Track prediction or error message
    """
    try:
        prediction = await track_predictor.predict_track(
            station_id=station_id,
            route_id=route_id,
            trip_id=trip_id,
            headsign=headsign,
            direction_id=direction_id,
            scheduled_time=scheduled_time,
        )

        if prediction:
            return {
                "success": True,
                "prediction": prediction.model_dump(),
            }
        else:
            return {
                "success": False,
                "message": "No prediction could be generated",
            }

    except Exception as e:
        logging.error(f"Error generating prediction: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stats/{station_id}/{route_id}")
async def get_prediction_stats(
    station_id: str,
    route_id: str,
) -> dict:
    """
    Get prediction statistics for a station and route.

    Args:
        station_id: The MBTA station ID
        route_id: The route ID

    Returns:
        Prediction statistics
    """
    try:
        stats = await track_predictor.get_prediction_stats(station_id, route_id)

        if stats:
            return {
                "success": True,
                "stats": stats.model_dump(),
            }
        else:
            return {
                "success": False,
                "message": "No statistics available",
            }

    except Exception as e:
        logging.error(f"Error getting stats for {station_id}/{route_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/historical/{station_id}/{route_id}")
async def get_historical_assignments(
    station_id: str,
    route_id: str,
    days: int = Query(30, description="Number of days to look back"),
) -> List[dict]:
    """
    Get historical track assignments for analysis.

    Args:
        station_id: The MBTA station ID
        route_id: The route ID
        days: Number of days to look back

    Returns:
        List of historical track assignments
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        assignments = await track_predictor.get_historical_assignments(
            station_id, route_id, start_date, end_date
        )

        return [assignment.model_dump() for assignment in assignments]

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
    uvicorn.run(app, host="0.0.0.0", port=port)
