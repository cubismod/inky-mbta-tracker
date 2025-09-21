import logging
from datetime import datetime, timedelta
from typing import List

from api.core import CR_ROUTES, CR_STATIONS, GET_DI
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import ValidationError
from shared_types.shared_types import TrackAssignment

from ..limits import limiter
from ..models import (
    ChainedPredictionsRequest,
    ChainedPredictionsResponse,
    DatePredictionsRequest,
    DatePredictionsResponse,
    DepartureWithPrediction,
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
            station_id_resolved = commons.track_predictor.normalize_station(
                prediction_request.station_id
            )
            if not commons.track_predictor.supports_track_predictions(
                station_id_resolved
            ):
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
@limiter.limit("200/minute")
async def generate_chained_track_predictions(
    request: Request, chained_request: ChainedPredictionsRequest, commons: GET_DI
) -> ChainedPredictionsResponse:
    async def process_single_prediction(
        pred_request: PredictionRequest,
    ) -> TrackPredictionResponse:
        try:
            station_id = commons.track_predictor.normalize_station(
                pred_request.station_id
            )
            if not commons.track_predictor.supports_track_predictions(station_id):
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
        station_id = commons.track_predictor.normalize_station(station_id)
        if not commons.track_predictor.supports_track_predictions(station_id):
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
        station_id = commons.track_predictor.normalize_station(station_id)
        if not commons.track_predictor.supports_track_predictions(station_id):
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


@router.post("/predictions/date")
@limiter.limit("15/minute")
async def generate_track_predictions_for_date(
    request: Request, date_request: DatePredictionsRequest, commons: GET_DI
) -> DatePredictionsResponse:
    """
    Generate track predictions for all upcoming departures on a specific date.
    """
    departures_with_predictions: List[DepartureWithPrediction] = []
    try:
        for route in CR_ROUTES:
            # Fetch upcoming departures using the TrackPredictor method
            departures = await commons.track_predictor.fetch_upcoming_departures(
                session=commons.session,
                route_id=route,
                station_ids=CR_STATIONS,
                target_date=date_request.target_date,
                limit=25,
            )

            for departure in departures:
                prediction = None

                try:
                    # Only generate predictions for stations that support them
                    normalized_station = commons.track_predictor.normalize_station(
                        departure["station_id"]
                    )

                    if (
                        commons.tg
                        and commons.track_predictor.supports_track_predictions(
                            normalized_station
                        )
                        and departure["trip_id"]
                    ):
                        prediction = await commons.track_predictor.predict_track(
                            station_id=normalized_station,
                            route_id=departure["route_id"],
                            trip_id=departure["trip_id"],
                            headsign="",  # Will be fetched in predict_track method
                            direction_id=departure["direction_id"],
                            scheduled_time=datetime.fromisoformat(
                                departure["departure_time"]
                            ),
                            tg=commons.tg,
                        )

                except (ConnectionError, TimeoutError, ValidationError) as e:
                    logger.error(
                        f"Error generating prediction for departure {departure['trip_id']}",
                        exc_info=e,
                    )
                    # Continue processing other departures even if one fails

                departures_with_predictions.append(
                    DepartureWithPrediction(
                        departure_info=departure, prediction=prediction
                    )
                )

        return DatePredictionsResponse(
            success=True, departures=departures_with_predictions
        )

    except (ConnectionError, TimeoutError) as e:
        logger.error("Connection error generating date predictions", exc_info=e)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError as e:
        logger.error("Validation error generating date predictions", exc_info=e)
        raise HTTPException(status_code=400, detail="Invalid request parameters")
    except Exception as e:
        logger.error("Unexpected error generating date predictions", exc_info=e)
        raise HTTPException(status_code=500, detail="Internal server error")
