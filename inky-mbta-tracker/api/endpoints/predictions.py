import logging
from datetime import datetime, timedelta
from typing import List

from api.core import GET_DI
from fastapi import APIRouter, HTTPException, Query, Request
from mbta_client import determine_station_id
from pydantic import ValidationError
from shared_types.shared_types import TrackAssignment

from ..limits import limiter
from ..models import (
    ChainedPredictionsRequest,
    ChainedPredictionsResponse,
    PredictionRequest,
    TrackPredictionResponse,
    TrackPredictionStatsResponse,
)

router = APIRouter()

logger = logging.getLogger(__name__)


@router.post("/predictions")
@limiter.limit("100/minute")
async def generate_track_prediction(
    request: Request, prediction_request: PredictionRequest, commons: GET_DI
) -> TrackPredictionResponse:
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
            if commons.tg:
                prediction = await commons.track_predictor.predict_track(
                    station_id=station_id_resolved,
                    route_id=prediction_request.route_id,
                    trip_id=prediction_request.trip_id,
                    headsign=prediction_request.headsign,
                    direction_id=prediction_request.direction_id,
                    scheduled_time=prediction_request.scheduled_time,
                    tg=commons.tg,
                )

                if prediction:
                    return TrackPredictionResponse(success=True, prediction=prediction)
            return TrackPredictionResponse(
                success=False, prediction="No prediction could be generated"
            )

        return await _generate_prediction()

    except TimeoutError:
        logger.error("Track prediction request timed out")
        return TrackPredictionResponse(success=False, prediction="Request timed out")
    except (ConnectionError, TimeoutError):
        logger.error("Connection error generating prediction", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logger.error("Validation error generating prediction", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/chained-predictions")
@limiter.limit("50/minute")
async def generate_chained_track_predictions(
    request: Request, chained_request: ChainedPredictionsRequest, commons: GET_DI
) -> ChainedPredictionsResponse:
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

            if commons.tg:
                prediction = await commons.track_predictor.predict_track(
                    station_id=station_id,
                    route_id=pred_request.route_id,
                    trip_id=pred_request.trip_id,
                    headsign=pred_request.headsign,
                    direction_id=pred_request.direction_id,
                    scheduled_time=pred_request.scheduled_time,
                    tg=commons.tg,
                )

                if prediction:
                    return TrackPredictionResponse(success=True, prediction=prediction)
            return TrackPredictionResponse(
                success=False, prediction="No prediction could be generated"
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Error generating chained prediction due to connection issue",
                exc_info=e,
            )
            return TrackPredictionResponse(
                success=False, prediction="Connection error occurred"
            )
        except ValidationError as e:
            logger.error(
                "Error generating chained prediction due to validation error",
                exc_info=e,
            )
            return TrackPredictionResponse(
                success=False, prediction="Validation error occurred"
            )
        except (RuntimeError, ValueError, TypeError) as e:
            logger.error("Unexpected error generating chained prediction", exc_info=e)
            return TrackPredictionResponse(
                success=False, prediction="Unexpected error occurred"
            )

    tasks = [
        await process_single_prediction(pred_request)
        for pred_request in chained_request.predictions
    ]

    return ChainedPredictionsResponse(results=tasks)


@router.get("/stats/{station_id}/{route_id}")
@limiter.limit("50/minute")
async def get_prediction_stats(
    request: Request, station_id: str, route_id: str, commons: GET_DI
) -> TrackPredictionStatsResponse:
    try:
        station_id, has_track_predictions = determine_station_id(station_id)
        if not has_track_predictions:
            return TrackPredictionStatsResponse(
                success=False,
                stats="Prediction stats are not available for this station",
            )
        stats = await commons.track_predictor.get_prediction_stats(station_id, route_id)
        if stats:
            return TrackPredictionStatsResponse(success=True, stats=stats)
        else:
            return TrackPredictionStatsResponse(
                success=False, stats="No stats available"
            )
    except (ConnectionError, TimeoutError):
        logger.error("Connection error getting prediction stats", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logger.error("Validation error getting prediction stats", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/historical/{station_id}/{route_id}")
@limiter.limit("50/minute")
async def get_historical_assignments(
    request: Request,
    station_id: str,
    route_id: str,
    commons: GET_DI,
    days: int = Query(30, description="Number of days to look back"),
) -> List[TrackAssignment]:
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        station_id, has_track_predictions = determine_station_id(station_id)
        if not has_track_predictions:
            return []
        assignments = await commons.track_predictor.get_historical_assignments(
            station_id, route_id, start_date, end_date
        )
        return [assignment for assignment in assignments]
    except (ConnectionError, TimeoutError):
        logger.error("Connection error getting historical data", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logger.error("Validation error getting historical data", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
