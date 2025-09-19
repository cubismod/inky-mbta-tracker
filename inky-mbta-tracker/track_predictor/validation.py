"""Validation module for track prediction system.

This module handles validation of track predictions against actual assignments,
including accuracy tracking and metrics recording.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from anyio.abc import TaskGroup
from consts import INSTANCE_ID, WEEK
from prometheus import track_predictions_validated
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError
from redis_cache import check_cache, write_cache
from shared_types.shared_types import TrackPrediction

from .redis_ops import update_prediction_stats, update_track_accuracy

logger = logging.getLogger(__name__)


async def validate_prediction(
    redis: Redis,
    station_id: str,
    route_id: str,
    trip_id: str,
    scheduled_time: datetime,
    actual_track_number: Optional[str],
    tg: TaskGroup,
) -> None:
    """
    Validate a previous prediction against actual track assignment.

    Args:
        redis: Redis client
        station_id: The station ID
        route_id: The route ID
        trip_id: The trip ID
        scheduled_time: The scheduled departure time
        actual_track_number: The actual track number assigned
        tg: TaskGroup for background operations
    """
    try:
        # Check if this prediction has already been validated
        validation_key = f"track_validation:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
        if await check_cache(redis, validation_key):
            logger.debug(
                f"Prediction already validated for {station_id} {route_id} {trip_id} on {scheduled_time.date()}"
            )
            return

        key = f"track_prediction:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
        prediction_data = await check_cache(redis, key)

        if not prediction_data:
            return

        prediction = TrackPrediction.model_validate_json(prediction_data)

        # Check if prediction was correct
        is_correct = prediction.track_number == actual_track_number

        # Update statistics
        tg.start_soon(
            update_prediction_stats,
            redis,
            station_id,
            route_id,
            is_correct,
            prediction.confidence_score,
        )

        # Update per-track historical accuracy for the predicted track
        if prediction.track_number:
            tg.start_soon(
                update_track_accuracy,
                redis,
                station_id,
                route_id,
                prediction.track_number,
                is_correct,
            )

        # Record validation metric
        result = "correct" if is_correct else "incorrect"
        track_predictions_validated.labels(
            station_id=station_id,
            route_id=route_id,
            result=result,
            instance=INSTANCE_ID,
        ).inc()

        # Mark as validated to prevent duplicate processing
        tg.start_soon(
            write_cache,
            redis,
            validation_key,
            "validated",
            WEEK,
        )

        logger.info(
            f"Validated prediction for {station_id} {route_id}: {'CORRECT' if is_correct else 'INCORRECT'}"
        )

    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to validate prediction due to Redis connection issue",
            exc_info=e,
        )
    except ValidationError as e:
        logger.error(
            "Failed to validate prediction due to validation error",
            exc_info=e,
        )


async def check_validation_status(
    redis: Redis,
    station_id: str,
    route_id: str,
    trip_id: str,
    scheduled_time: datetime,
) -> bool:
    """
    Check if a prediction has already been validated.

    Args:
        redis: Redis client
        station_id: The station ID
        route_id: The route ID
        trip_id: The trip ID
        scheduled_time: The scheduled departure time

    Returns:
        True if already validated, False otherwise
    """
    try:
        validation_key = f"track_validation:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
        return bool(await check_cache(redis, validation_key))
    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to check validation status due to Redis connection issue",
            exc_info=e,
        )
        return False
