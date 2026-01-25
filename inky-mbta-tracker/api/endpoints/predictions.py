import logging
import os
from datetime import datetime, timedelta
from typing import List

from api.core import CR_ROUTES, CR_STATIONS, GET_DI
from api.middleware.cache_middleware import cache_ttl
from consts import HOUR, MINUTE
from fastapi import APIRouter, HTTPException, Query, Request
from opentelemetry import trace
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
tracer = trace.get_tracer(__name__)


@router.post("/predictions")
@limiter.limit("100/minute")
@cache_ttl(HOUR)
async def generate_track_prediction(
    request: Request, prediction_request: PredictionRequest, commons: GET_DI
) -> TrackPredictionResponse:
    with tracer.start_as_current_span(
        "api.predictions.generate_track_prediction",
        attributes={
            "station_id": prediction_request.station_id,
            "route_id": prediction_request.route_id,
            "trip_id": prediction_request.trip_id,
            "direction_id": prediction_request.direction_id,
        },
    ) as span:
        try:

            async def _generate_prediction() -> TrackPredictionResponse:
                with tracer.start_as_current_span("normalize_and_validate_station"):
                    station_id_resolved = commons.track_predictor.normalize_station(
                        prediction_request.station_id
                    )
                    span.set_attribute("station_id_resolved", station_id_resolved)
                    if not commons.track_predictor.supports_track_predictions(
                        station_id_resolved
                    ):
                        span.set_attribute("prediction.supported", False)
                        return TrackPredictionResponse(
                            success=False,
                            prediction="Predictions are not available for this station",
                        )
                    span.set_attribute("prediction.supported", True)

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
                        span.set_attribute("prediction.success", True)
                        span.set_attribute("prediction.result", str(prediction))
                        return TrackPredictionResponse(
                            success=True, prediction=prediction
                        )
                span.set_attribute("prediction.success", False)
                return TrackPredictionResponse(
                    success=False, prediction="No prediction could be generated"
                )

            return await _generate_prediction()

        except TimeoutError:
            logger.error("Track prediction request timed out")
            span.set_attribute("error", True)
            span.set_attribute("error.type", "timeout")
            return TrackPredictionResponse(
                success=False, prediction="Request timed out"
            )
        except (ConnectionError, TimeoutError):
            logger.error("Connection error generating prediction", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError:
            logger.error("Validation error generating prediction", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/chained-predictions")
@limiter.limit("200/minute")
@cache_ttl(HOUR)
async def generate_chained_track_predictions(
    request: Request, chained_request: ChainedPredictionsRequest, commons: GET_DI
) -> ChainedPredictionsResponse:
    with tracer.start_as_current_span(
        "api.predictions.generate_chained_track_predictions",
        attributes={"prediction_count": len(chained_request.predictions)},
    ) as span:

        async def process_single_prediction(
            pred_request: PredictionRequest,
        ) -> TrackPredictionResponse:
            with tracer.start_as_current_span(
                "process_single_chained_prediction",
                attributes={
                    "station_id": pred_request.station_id,
                    "route_id": pred_request.route_id,
                    "trip_id": pred_request.trip_id,
                },
            ):
                try:
                    station_id = commons.track_predictor.normalize_station(
                        pred_request.station_id
                    )
                    if not commons.track_predictor.supports_track_predictions(
                        station_id
                    ):
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
                            return TrackPredictionResponse(
                                success=True, prediction=prediction
                            )
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
                    logger.error(
                        "Unexpected error generating chained prediction", exc_info=e
                    )
                    return TrackPredictionResponse(
                        success=False, prediction="Unexpected error occurred"
                    )

        tasks = [
            await process_single_prediction(pred_request)
            for pred_request in chained_request.predictions
        ]

        successful = sum(1 for t in tasks if t.success)
        span.set_attribute("predictions.successful", successful)
        span.set_attribute("predictions.total", len(tasks))

        return ChainedPredictionsResponse(results=tasks)


@router.get("/stats/{station_id}/{route_id}")
@limiter.limit("50/minute")
@cache_ttl(HOUR)
async def get_prediction_stats(
    request: Request, station_id: str, route_id: str, commons: GET_DI
) -> TrackPredictionStatsResponse:
    with tracer.start_as_current_span(
        "api.predictions.get_prediction_stats",
        attributes={"station_id": station_id, "route_id": route_id},
    ) as span:
        try:
            station_id = commons.track_predictor.normalize_station(station_id)
            span.set_attribute("station_id_resolved", station_id)
            if not commons.track_predictor.supports_track_predictions(station_id):
                span.set_attribute("stats.supported", False)
                return TrackPredictionStatsResponse(
                    success=False,
                    stats="Prediction stats are not available for this station",
                )
            span.set_attribute("stats.supported", True)
            stats = await commons.track_predictor.get_prediction_stats(
                station_id, route_id
            )
            if stats:
                span.set_attribute("stats.success", True)
                return TrackPredictionStatsResponse(success=True, stats=stats)
            else:
                span.set_attribute("stats.success", False)
                return TrackPredictionStatsResponse(
                    success=False, stats="No stats available"
                )
        except (ConnectionError, TimeoutError):
            logger.error("Connection error getting prediction stats", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError:
            logger.error("Validation error getting prediction stats", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/historical/{station_id}/{route_id}")
@limiter.limit("50/minute")
@cache_ttl(5 * MINUTE)
async def get_historical_assignments(
    request: Request,
    station_id: str,
    route_id: str,
    commons: GET_DI,
    days: int = Query(30, description="Number of days to look back"),
) -> List[TrackAssignment]:
    with tracer.start_as_current_span(
        "api.predictions.get_historical_assignments",
        attributes={"station_id": station_id, "route_id": route_id, "days": days},
    ) as span:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            station_id = commons.track_predictor.normalize_station(station_id)
            span.set_attribute("station_id_resolved", station_id)
            if not commons.track_predictor.supports_track_predictions(station_id):
                span.set_attribute("historical.supported", False)
                return []
            span.set_attribute("historical.supported", True)
            assignments = await commons.track_predictor.get_historical_assignments(
                station_id, route_id, start_date, end_date
            )
            span.set_attribute("historical.count", len(assignments))
            return [assignment for assignment in assignments]
        except (ConnectionError, TimeoutError):
            logger.error("Connection error getting historical data", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError:
            logger.error("Validation error getting historical data", exc_info=True)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/predictions/date")
@limiter.limit("15/minute")
@cache_ttl(3 * HOUR)
async def generate_track_predictions_for_date(
    request: Request, date_request: DatePredictionsRequest, commons: GET_DI
) -> DatePredictionsResponse:
    """
    Generate track predictions for all upcoming departures on a specific date.
    """
    with tracer.start_as_current_span(
        "api.predictions.generate_track_predictions_for_date",
        attributes={"target_date": date_request.target_date.isoformat()},
    ) as span:
        departures_with_predictions: List[DepartureWithPrediction] = []
        if date_request.target_date.date() > datetime.now().date() + timedelta(
            days=7
        ) or date_request.target_date.date() < datetime.now().date() - timedelta(
            days=1
        ):
            span.set_attribute("date.valid", False)
            return DatePredictionsResponse(
                success=False,
                departures=[],
                note="Target date is invalid. Must be within the next 7 days or the past day.",
            )
        span.set_attribute("date.valid", True)
        try:
            for route in CR_ROUTES:
                with tracer.start_as_current_span(
                    "fetch_route_departures", attributes={"route": route}
                ):
                    # Fetch upcoming departures using the TrackPredictor method
                    departures = (
                        await commons.track_predictor.fetch_upcoming_departures(
                            session=commons.session,
                            route_id=route,
                            station_ids=CR_STATIONS,
                            target_date=date_request.target_date,
                            limit=int(os.getenv("IMT_CR_LIMIT", "10")),
                        )
                    )

                    for departure in departures:
                        prediction = None

                        try:
                            # Only generate predictions for stations that support them
                            normalized_station = (
                                commons.track_predictor.normalize_station(
                                    departure["station_id"]
                                )
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

            span.set_attribute("departures.total", len(departures_with_predictions))
            span.set_attribute(
                "departures.with_predictions",
                sum(1 for d in departures_with_predictions if d.prediction is not None),
            )
            return DatePredictionsResponse(
                success=True, departures=departures_with_predictions
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error("Connection error generating date predictions", exc_info=e)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as e:
            logger.error("Validation error generating date predictions", exc_info=e)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            raise HTTPException(status_code=400, detail="Invalid request parameters")
