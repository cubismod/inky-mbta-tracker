"""Redis operations for track prediction caching and statistics.

This module handles all Redis-specific operations for the track prediction system,
including caching, accuracy tracking, and statistics management.
"""

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Optional

from consts import DAY, WEEK
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import should_trace_operation
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError
from redis_cache import check_cache, write_cache
from shared_types.shared_types import (
    TrackAssignment,
    TrackPrediction,
    TrackPredictionStats,
)

logger = logging.getLogger(__name__)


async def get_allowed_tracks(
    redis: Redis,
    station_id: str,
    route_id: str,
    assignments: Optional[list[TrackAssignment]] = None,
) -> set[str]:
    """Return a set of allowed track numbers for a station/route based on recent history.

    Cached in Redis for 1 day to avoid heavy scans. Falls back to empty set
    meaning "no restriction" if nothing found.
    """
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer and should_trace_operation("medium_volume"):
        with tracer.start_as_current_span(
            "track_predictor.get_allowed_tracks",
            attributes={"station_id": station_id, "route_id": route_id},
        ) as span:
            result = await _get_allowed_tracks_impl(
                redis, station_id, route_id, assignments, span
            )
            if span:
                span.set_attribute("allowed_tracks.count", len(result))
            return result
    else:
        return await _get_allowed_tracks_impl(
            redis, station_id, route_id, assignments, None
        )


async def _get_allowed_tracks_impl(
    redis: Redis,
    station_id: str,
    route_id: str,
    assignments: Optional[list[TrackAssignment]],
    span: Optional[Span],
) -> set[str]:
    cache_key = f"allowed_tracks:{station_id}:{route_id}"
    cached = await check_cache(redis, cache_key)
    if cached:
        if span:
            span.set_attribute("cache.hit", True)
        try:
            lst = json.loads(cached)
            return set(str(x) for x in lst)
        except (json.JSONDecodeError, TypeError):
            # cached value couldn't be parsed or isn't iterable as expected
            pass

    if span:
        span.set_attribute("cache.hit", False)

    # Build from historical assignments over the last 180 days
    try:
        if assignments is None:
            end = datetime.now(UTC)
            start = end - timedelta(days=180)
            assignments = await get_historical_assignments(
                redis, station_id, route_id, start, end
            )
        tracks = {a.track_number for a in assignments if a.track_number}
        await write_cache(redis, cache_key, json.dumps(sorted(tracks)), DAY)
        return set(tracks)
    except (ConnectionError, TimeoutError, ValueError) as e:
        # If we can't fetch history due to connectivity or parsing errors,
        # return an empty set (permissive fallback).
        logger.debug("Unable to build allowed tracks from history", exc_info=e)
        return set()


async def store_prediction(redis: Redis, prediction: TrackPrediction) -> None:
    """Store a prediction for later validation."""
    try:
        key = f"track_prediction:{prediction.station_id}:{prediction.route_id}:{prediction.trip_id}:{prediction.scheduled_time.date()}"
        await write_cache(
            redis,
            key,
            prediction.model_dump_json(),
            WEEK,  # Store for 1 week
        )
    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to store prediction due to Redis connection issue",
            exc_info=e,
        )
    except ValidationError as e:
        logger.error(
            "Failed to store prediction due to validation error",
            exc_info=e,
        )


async def get_prediction_accuracy(
    redis: Redis, station_id: str, route_id: str, track_number: str
) -> float:
    """Get historical prediction accuracy for a specific track."""
    try:
        # Get recent prediction validation results
        accuracy_key = f"track_accuracy:{station_id}:{route_id}:{track_number}"
        accuracy_data = await redis.get(accuracy_key)

        if accuracy_data:
            # Parse stored accuracy data (format: "correct:total")
            parts = accuracy_data.decode().split(":")
            if len(parts) == 2:
                correct = int(parts[0])
                total = int(parts[1])
                if total > 0:
                    return correct / total

        # Default accuracy for new tracks (slightly conservative)
        return 0.7

    except (ConnectionError, TimeoutError, ValueError) as e:
        logger.error("Failed to get prediction accuracy", exc_info=e)
        return 0.7  # Default accuracy


async def choose_top_allowed_track(
    redis: Redis,
    station_id: str,
    route_id: str,
    allowed: set[str],
    assignments: Optional[list[TrackAssignment]] = None,
) -> Optional[str]:
    """Choose the most frequent historical track among the allowed set.

    Returns the track string or None if unable to decide.
    """
    try:
        if assignments is None:
            end = datetime.now(UTC)
            start = end - timedelta(days=180)
            assignments = await get_historical_assignments(
                redis, station_id, route_id, start, end
            )
        counts: dict[str, int] = defaultdict(int)
        for a in assignments:
            if a.track_number and a.track_number in allowed:
                counts[a.track_number] += 1
        if not counts:
            return None
        # return most frequent; ties resolved by smallest track string
        best = max(sorted(counts.items(), key=lambda x: x[0]), key=lambda x: x[1])
        return best[0]
    except (ConnectionError, TimeoutError, ValueError) as e:
        # Errors fetching historical assignments or processing counts -> no decision
        logger.debug("Unable to choose top allowed track", exc_info=e)
        return None


async def update_track_accuracy(
    redis: Redis, station_id: str, route_id: str, track_number: str, is_correct: bool
) -> None:
    """Update per-track accuracy counters used by get_prediction_accuracy.

    Stored format is a simple "correct:total" string per station/route/track.
    """
    try:
        key = f"track_accuracy:{station_id}:{route_id}:{track_number}"
        raw = await redis.get(key)
        correct = 0
        total = 0
        if raw:
            try:
                parts = (
                    raw.decode().split(":")
                    if isinstance(raw, (bytes, bytearray))
                    else str(raw).split(":")
                )
                if len(parts) == 2:
                    correct = int(parts[0])
                    total = int(parts[1])
            except (ValueError, TypeError, AttributeError):
                # Malformed stored accuracy value; reset counters
                correct = 0
                total = 0
        total += 1
        if is_correct:
            correct += 1
        await write_cache(redis, key, f"{correct}:{total}", 30 * DAY)
    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to update per-track accuracy due to Redis connection issue",
            exc_info=e,
        )
    except OSError as e:
        # Limit to expected redis/network related errors; treat others as failures elsewhere
        logger.error("Failed to update per-track accuracy", exc_info=e)


async def update_prediction_stats(
    redis: Redis, station_id: str, route_id: str, is_correct: bool, confidence: float
) -> None:
    """Update prediction statistics."""
    try:
        stats_key = f"track_stats:{station_id}:{route_id}"
        stats_data = await check_cache(redis, stats_key)

        if stats_data:
            stats = TrackPredictionStats.model_validate_json(stats_data)
        else:
            stats = TrackPredictionStats(
                station_id=station_id,
                route_id=route_id,
                total_predictions=0,
                correct_predictions=0,
                accuracy_rate=0.0,
                last_updated=datetime.now(UTC),
                prediction_counts_by_track={},
                average_confidence=0.0,
            )

        # Update stats
        stats.total_predictions += 1
        if is_correct:
            stats.correct_predictions += 1
        stats.accuracy_rate = stats.correct_predictions / stats.total_predictions
        stats.last_updated = datetime.now(UTC)

        # Update average confidence
        old_total = (stats.total_predictions - 1) * stats.average_confidence
        stats.average_confidence = (old_total + confidence) / stats.total_predictions

        # Store updated stats
        await write_cache(
            redis,
            stats_key,
            stats.model_dump_json(),
            30 * DAY,  # Store for 30 days
        )

    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to update prediction stats due to Redis connection issue",
            exc_info=e,
        )
    except ValidationError as e:
        logger.error(
            "Failed to update prediction stats due to validation error",
            exc_info=e,
        )


async def get_prediction_stats(
    redis: Redis, station_id: str, route_id: str
) -> Optional[TrackPredictionStats]:
    """Get prediction statistics for a station and route."""
    try:
        stats_key = f"track_stats:{station_id}:{route_id}"
        stats_data = await check_cache(redis, stats_key)

        if stats_data:
            return TrackPredictionStats.model_validate_json(stats_data)
        return None

    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to get prediction stats due to Redis connection issue",
            exc_info=e,
        )
        return None
    except ValidationError as e:
        logger.error(
            "Failed to get prediction stats due to validation error",
            exc_info=e,
        )
        return None


async def get_historical_assignments(
    redis: Redis,
    station_id: str,
    route_id: str,
    start_date: datetime,
    end_date: datetime,
    include_related_routes: bool = False,
    route_families: Optional[dict[str, list[str]]] = None,
) -> list[TrackAssignment]:
    """Retrieve historical track assignments for a given station and route.

    Args:
        redis: Redis client
        station_id: The station to query
        route_id: The route to query
        start_date: Start date for the query
        end_date: End date for the query
        include_related_routes: Whether to include assignments from related routes
        route_families: Route family mappings for related routes

    Returns:
        List of historical track assignments
    """
    from prometheus import redis_commands

    try:
        routes_to_check = [route_id]
        if include_related_routes and route_families and route_id in route_families:
            routes_to_check.extend(route_families[route_id])

        async def fetch_route_assignments(
            current_route: str,
        ) -> list[TrackAssignment]:
            """Fetch assignments for a single route."""
            route_results: list[TrackAssignment] = []
            time_series_key = f"track_timeseries:{station_id}:{current_route}"

            # Get assignments within the time range
            assignments = await redis.zrangebyscore(
                time_series_key, start_date.timestamp(), end_date.timestamp()
            )
            redis_commands.labels("zrangebyscore").inc()

            if not assignments:
                return route_results

            async def fetch_assignment_data(
                assignment_key: bytes,
            ) -> Optional[TrackAssignment]:
                try:
                    assignment_data = await check_cache(redis, assignment_key.decode())
                    if assignment_data:
                        return TrackAssignment.model_validate_json(assignment_data)
                except ValidationError as e:
                    logger.error("Failed to parse assignment data", exc_info=e)
                return None

            for assignment_key in assignments:
                try:
                    result = await fetch_assignment_data(assignment_key)
                    if isinstance(result, TrackAssignment):
                        route_results.append(result)
                except (ConnectionError, TimeoutError) as e:
                    logger.error("Error fetching assignment data", exc_info=e)

            return route_results

        results: list[TrackAssignment] = []

        for route in routes_to_check:
            try:
                route_results = await fetch_route_assignments(route)
                results.extend(route_results)
            except (ConnectionError, TimeoutError) as e:
                logger.error("Error fetching route assignments", exc_info=e)

        return results

    except (ConnectionError, TimeoutError) as e:
        logger.error(
            "Failed to retrieve historical assignments due to Redis connection issue",
            exc_info=e,
        )
        return []
    except ValidationError as e:
        logger.error(
            "Failed to retrieve historical assignments due to validation error",
            exc_info=e,
        )
        return []
