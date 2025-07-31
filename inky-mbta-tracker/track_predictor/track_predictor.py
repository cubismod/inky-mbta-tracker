import logging
import math
import os
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
import shared_types.shared_types
import textdistance
from async_lru import alru_cache
from consts import DAY, INSTANCE_ID, MBTA_V3_ENDPOINT, MINUTE, WEEK, YEAR
from prometheus import (
    redis_commands,
    track_historical_assignments_stored,
    track_pattern_analysis_duration,
    track_prediction_confidence,
    track_predictions_cached,
    track_predictions_generated,
    track_predictions_validated,
)
from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis_cache import check_cache, write_cache
from shared_types.shared_types import (
    TrackAssignment,
    TrackPrediction,
    TrackPredictionStats,
)

logger = logging.getLogger(__name__)


class TrackPredictor:
    """
    Track prediction system for MBTA commuter rail trains.

    This system analyzes historical track assignments to predict future track
    assignments before they are officially announced by the MBTA.
    """

    def __init__(self) -> None:
        self.redis = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT", ""),
            port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
            password=os.environ.get("IMT_REDIS_PASSWORD", ""),
        )

    async def store_historical_assignment(self, assignment: TrackAssignment) -> None:
        """
        Store a historical track assignment for future analysis.

        Args:
            assignment: The track assignment data to store
        """
        try:
            # Store in Redis with a key that includes station, route, and timestamp
            key = f"track_history:{assignment.station_id}:{assignment.route_id}:{assignment.trip_id}:{assignment.scheduled_time.date()}"

            # check if the key already exists
            if await check_cache(self.redis, key):
                return

            # Store for 6 months for analysis
            await write_cache(
                self.redis,
                key,
                assignment.model_dump_json(),
                1 * YEAR,  # 1 year
            )

            # Also store in a time-series format for easier querying
            time_series_key = (
                f"track_timeseries:{assignment.station_id}:{assignment.route_id}"
            )
            await self.redis.zadd(
                time_series_key, {key: assignment.scheduled_time.timestamp()}
            )
            redis_commands.labels("zadd").inc()

            # Set expiration for time series (1 year)
            await self.redis.expire(time_series_key, 1 * YEAR)
            redis_commands.labels("expire").inc()

            track_historical_assignments_stored.labels(
                station_id=assignment.station_id,
                route_id=assignment.route_id,
                track_number=assignment.track_number or "unknown",
                instance=INSTANCE_ID,
            ).inc()

            logger.debug(
                f"Stored track assignment: {assignment.station_id} {assignment.route_id} -> {assignment.track_number}"
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to store track assignment due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to store track assignment due to validation error: {e}",
                exc_info=True,
            )

    async def get_historical_assignments(
        self, station_id: str, route_id: str, start_date: datetime, end_date: datetime
    ) -> List[TrackAssignment]:
        """
        Retrieve historical track assignments for a given station and route.

        Args:
            station_id: The station to query
            route_id: The route to query
            start_date: Start date for the query
            end_date: End date for the query

        Returns:
            List of historical track assignments
        """
        try:
            time_series_key = f"track_timeseries:{station_id}:{route_id}"

            # Get assignments within the time range
            assignments = await self.redis.zrangebyscore(
                time_series_key, start_date.timestamp(), end_date.timestamp()
            )
            redis_commands.labels("zrangebyscore").inc()

            results = []
            for assignment_key in assignments:
                assignment_data = await check_cache(self.redis, assignment_key.decode())
                if assignment_data:
                    try:
                        assignment = TrackAssignment.model_validate_json(
                            assignment_data
                        )
                        results.append(assignment)
                    except ValidationError as e:
                        logger.error(f"Failed to parse assignment data: {e}")

            return results

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to retrieve historical assignments due to Redis connection issue: {e}",
                exc_info=True,
            )
            return []
        except ValidationError as e:
            logger.error(
                f"Failed to retrieve historical assignments due to validation error: {e}",
                exc_info=True,
            )
            return []

    async def analyze_patterns(
        self,
        station_id: str,
        route_id: str,
        trip_headsign: str,
        direction_id: int,
        scheduled_time: datetime,
    ) -> Dict[str, float]:
        """
        Analyze historical patterns to determine track assignment probabilities.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_headsign: The trip headsign
            direction_id: The direction ID
            scheduled_time: The scheduled departure time

        Returns:
            Dictionary mapping track identifiers to probability scores
        """
        try:
            start_time = time.time()

            # Look back 6 months for better historical data coverage
            start_date = scheduled_time - timedelta(days=180)
            end_date = scheduled_time

            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date
            )

            if not assignments:
                logger.debug(
                    f"No historical assignments found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
                )
                return {}

            target_dow = scheduled_time.weekday()
            target_hour = scheduled_time.hour
            target_minute = scheduled_time.minute
            target_is_weekend = target_dow in [5, 6]
            target_is_holiday = await self._is_holiday(scheduled_time)

            # Enhanced pattern analysis with adaptive scoring
            pattern_scores: dict[str, float] = defaultdict(float)
            pattern_counts: dict[str, int] = defaultdict(int)

            for assignment in assignments:
                if not assignment.track_number:
                    continue

                # Calculate various similarity metrics
                headsign_similarity = textdistance.levenshtein.normalized_similarity(
                    trip_headsign, assignment.headsign
                )

                # Improved time matching with tighter windows
                time_diff_minutes = abs(
                    assignment.hour * 60
                    + assignment.minute
                    - target_hour * 60
                    - target_minute
                )

                # Service pattern similarity
                assignment_is_weekend = assignment.day_of_week in [5, 6]
                assignment_is_holiday = await self._is_holiday(
                    assignment.scheduled_time
                )

                # Calculate base score with weighted criteria
                base_score = 0.0

                # Exact match: headsign + time + direction + service pattern
                if (
                    headsign_similarity > 0.8
                    and assignment.direction_id == direction_id
                    and time_diff_minutes <= 15
                    and target_is_weekend == assignment_is_weekend
                ):
                    base_score = 15.0
                    logger.debug(f"Exact match found: {assignment.track_number}")

                # Strong headsign + direction + service pattern match
                elif (
                    headsign_similarity > 0.7
                    and assignment.direction_id == direction_id
                    and target_is_weekend == assignment_is_weekend
                ):
                    base_score = 10.0 * headsign_similarity
                    logger.debug(f"Strong headsign match: {assignment.track_number}")

                # Time-based patterns with tighter windows
                elif (
                    assignment.direction_id == direction_id
                    and time_diff_minutes <= 10
                    and target_is_weekend == assignment_is_weekend
                ):
                    base_score = 8.0
                    logger.debug(f"Precise time match: {assignment.track_number}")

                elif (
                    assignment.direction_id == direction_id and time_diff_minutes <= 20
                ):
                    base_score = 5.0
                    logger.debug(f"Time window match: {assignment.track_number}")

                # Direction + service pattern match
                elif (
                    assignment.direction_id == direction_id
                    and target_is_weekend == assignment_is_weekend
                ):
                    base_score = 4.0
                    logger.debug(
                        f"Direction + service pattern match: {assignment.track_number}"
                    )

                # Holiday pattern matching
                elif (
                    target_is_holiday
                    and assignment_is_holiday
                    and assignment.direction_id == direction_id
                ):
                    base_score = 6.0
                    logger.debug(f"Holiday pattern match: {assignment.track_number}")

                # Day of week pattern (weekday vs weekend distinction)
                elif assignment.day_of_week == target_dow:
                    base_score = 2.0
                    logger.debug(f"Day of week match: {assignment.track_number}")

                # Apply time decay with exponential weighting
                if base_score > 0:
                    days_old = (scheduled_time - assignment.scheduled_time).days
                    time_decay = max(0.1, 0.95 ** (days_old / 7.0))  # Weekly decay

                    # Data quality adjustment
                    quality_factor = self._calculate_data_quality(
                        assignment, scheduled_time
                    )

                    adjusted_score = base_score * time_decay * quality_factor
                    pattern_scores[assignment.track_number] += adjusted_score
                    pattern_counts[assignment.track_number] += 1

            # Enhanced confidence calculation with multi-factor scoring
            if pattern_scores:
                # Calculate enhanced confidence scores
                confidence_scores = {}
                total_raw_score = sum(pattern_scores.values())

                for track, raw_score in pattern_scores.items():
                    sample_count = pattern_counts[track]
                    base_confidence = raw_score / total_raw_score

                    # Sample size confidence (logarithmic scaling for diminishing returns)
                    sample_confidence = min(
                        1.0, math.log(sample_count + 1) / math.log(20)
                    )

                    # Historical accuracy for this specific track/route combination
                    track_accuracy = await self._get_prediction_accuracy(
                        station_id, route_id, track
                    )

                    # Recency factor - prefer recent successful predictions
                    recency_factor = await self._get_recent_success_rate(
                        station_id, route_id, track, scheduled_time
                    )

                    # Consistency factor - prefer tracks with consistent historical usage
                    consistency_factor = await self._calculate_track_consistency(
                        station_id, route_id, track, assignments
                    )

                    # Combined confidence with weighted factors
                    enhanced_confidence = (
                        0.35 * base_confidence
                        + 0.25 * sample_confidence
                        + 0.20 * track_accuracy
                        + 0.10 * recency_factor
                        + 0.10 * consistency_factor
                    )

                    confidence_scores[track] = enhanced_confidence

                # Normalize confidence scores
                total_confidence = sum(confidence_scores.values())
                if total_confidence > 0:
                    confidence_scores = {
                        track: conf / total_confidence
                        for track, conf in confidence_scores.items()
                    }

                logger.debug(
                    f"Enhanced pattern analysis for station_id={station_id}, route_id={route_id}, "
                    f"trip_id={trip_headsign}, direction_id={direction_id}"
                )
                logger.debug(f"Confidence scores: {confidence_scores}")

                # Record analysis duration
                duration = time.time() - start_time
                track_pattern_analysis_duration.labels(
                    station_id=station_id, route_id=route_id, instance=INSTANCE_ID
                ).set(duration)

                return confidence_scores

            logger.debug(
                f"No patterns found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
            )

            # Record analysis duration even for no patterns
            duration = time.time() - start_time
            track_pattern_analysis_duration.labels(
                station_id=station_id, route_id=route_id, instance=INSTANCE_ID
            ).set(duration)

            return {}

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to analyze patterns due to Redis connection issue: {e}",
                exc_info=True,
            )
            return {}
        except ValidationError as e:
            logger.error(
                f"Failed to analyze patterns due to validation error: {e}",
                exc_info=True,
            )
            return {}

    @alru_cache(ttl=30 * MINUTE)
    async def predict_track(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        headsign: str,
        direction_id: int,
        scheduled_time: datetime,
    ) -> Optional[TrackPrediction]:
        """
        Generate a track prediction for a given trip.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_id: The trip ID
            headsign: The trip headsign
            direction_id: The direction ID
            scheduled_time: The scheduled departure time

        Returns:
            TrackPrediction object or None if no prediction can be made
        """
        try:
            # Only predict for commuter rail
            if not route_id.startswith("CR"):
                return None

            # it makes more sense to get the headsign client-side using the exact trip_id due to API rate limits
            # Lazy import to avoid circular dependency
            import mbta_client

            async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
                async with mbta_client.MBTAApi(
                    watcher_type=shared_types.shared_types.TaskType.TRACK_PREDICTIONS
                ) as api:
                    new_hs = await api.get_headsign(trip_id, session)
                    if new_hs != "":
                        headsign = new_hs
                    stop_data = await api.get_stop(session, station_id)
                    if stop_data[0] and stop_data[0].data.attributes.platform_code:
                        # confirmed from the MBTA that this is the platform code
                        prediction = TrackPrediction(
                            station_id=station_id,
                            route_id=route_id,
                            trip_id=trip_id,
                            headsign=headsign,
                            direction_id=direction_id,
                            scheduled_time=scheduled_time,
                            track_number=stop_data[0].data.attributes.platform_code,
                            confidence_score=1.0,
                            prediction_method="platform_code",
                            historical_matches=0,
                            created_at=datetime.now(UTC),
                        )

                        track_predictions_generated.labels(
                            station_id=station_id,
                            route_id=route_id,
                            prediction_method="platform_code",
                            instance=INSTANCE_ID,
                        ).inc()

                        track_prediction_confidence.labels(
                            station_id=station_id,
                            route_id=route_id,
                            track_number=prediction.track_number,
                            instance=INSTANCE_ID,
                        ).set(1.0)

                        return prediction

            # Check if this is a cache hit
            cache_key = f"track_prediction:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
            cached_prediction = await check_cache(self.redis, cache_key)
            if cached_prediction:
                track_predictions_cached.labels(
                    station_id=station_id, route_id=route_id, instance=INSTANCE_ID
                ).inc()
                return TrackPrediction.model_validate_json(cached_prediction)

            # Analyze patterns
            patterns = await self.analyze_patterns(
                station_id, route_id, headsign, direction_id, scheduled_time
            )

            if not patterns:
                return None

            # Find the most likely track
            best_track = max(patterns.keys(), key=lambda x: patterns[x])
            confidence = patterns[best_track]

            # Dynamic confidence threshold based on historical accuracy
            overall_accuracy = await self._get_overall_accuracy(station_id, route_id)
            dynamic_threshold = max(0.25, min(0.5, overall_accuracy * 0.6))

            if confidence < dynamic_threshold:
                logger.debug(
                    f"No prediction made for station_id={station_id}, route_id={route_id}, trip_id={trip_id}, "
                    f"headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, "
                    f"confidence={confidence:.3f} < threshold={dynamic_threshold:.3f}"
                )
                return None

            # Determine prediction method
            method = "historical_pattern"
            if confidence >= 0.8:
                method = "high_confidence_pattern"
            elif confidence >= 0.6:
                method = "medium_confidence_pattern"
            else:
                method = "low_confidence_pattern"

            logger.debug(
                f"Predicting track for station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, method={method}"
            )
            # Count historical matches
            historical_matches = sum(
                1
                for _ in await self.get_historical_assignments(
                    station_id,
                    route_id,
                    scheduled_time - timedelta(days=30),
                    scheduled_time,
                )
            )
            logger.debug(f"Historical matches={historical_matches}")

            if not best_track.isdigit():
                logger.debug(f"Discarding non-digit track {best_track}")
                return None

            # Create prediction
            prediction = TrackPrediction(
                station_id=station_id,
                route_id=route_id,
                trip_id=trip_id,
                headsign=headsign,
                direction_id=direction_id,
                scheduled_time=scheduled_time,
                track_number=best_track,
                confidence_score=confidence,
                prediction_method=method,
                historical_matches=historical_matches,
                created_at=datetime.now(UTC),
            )

            # Record metrics
            track_predictions_generated.labels(
                station_id=station_id,
                route_id=route_id,
                prediction_method=method,
                instance=INSTANCE_ID,
            ).inc()

            track_prediction_confidence.labels(
                station_id=station_id,
                route_id=route_id,
                track_number=best_track,
                instance=INSTANCE_ID,
            ).set(confidence)

            # Store prediction for later validation
            await self._store_prediction(prediction)

            logger.debug(
                f"Predicted track={prediction.track_number}, station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, method={method}, historical_matches={historical_matches}"
            )

            return prediction

        except ValidationError as e:
            logger.error(f"Failed to predict track: {e}")
            return None

    async def _is_holiday(self, date: datetime) -> bool:
        """Check if a date is a holiday (simplified implementation)."""
        try:
            # Strip time components for comparison
            date_only = date.replace(hour=0, minute=0, second=0, microsecond=0)

            # Common US holidays that affect transit schedules
            holidays = [
                # New Year's Day
                datetime(date.year, 1, 1, tzinfo=date.tzinfo),
                # Independence Day
                datetime(date.year, 7, 4, tzinfo=date.tzinfo),
                # Christmas Day
                datetime(date.year, 12, 25, tzinfo=date.tzinfo),
            ]

            # Check if the date matches any holiday
            for holiday in holidays:
                if date_only.date() == holiday.date():
                    return True

            # Check for Thanksgiving (4th Thursday in November)
            if date.month == 11:
                # Find first Thursday in November
                first_day = datetime(date.year, 11, 1, tzinfo=date.tzinfo)
                days_until_thursday = (3 - first_day.weekday()) % 7
                first_thursday = first_day + timedelta(days=days_until_thursday)
                # Fourth Thursday is 3 weeks later
                thanksgiving = first_thursday + timedelta(weeks=3)
                if date_only.date() == thanksgiving.date():
                    return True

            return False
        except (ValueError, AttributeError):
            return False

    def _calculate_data_quality(
        self, assignment: TrackAssignment, target_time: datetime
    ) -> float:
        """Calculate data quality factor for an assignment."""
        quality = 1.0

        # Penalize missing or incomplete data
        if not assignment.track_number or not assignment.track_number.isdigit():
            quality *= 0.5

        # Prefer assignments with actual times over scheduled only
        if assignment.actual_time:
            time_diff = abs(
                (assignment.actual_time - assignment.scheduled_time).total_seconds()
            )
            if time_diff > 1800:  # 30 minutes delay
                quality *= 0.8
        else:
            quality *= 0.9  # Slight penalty for no actual time data

        # Boost quality for very recent data
        days_old = (target_time - assignment.scheduled_time).days
        if days_old <= 7:
            quality *= 1.2
        elif days_old <= 30:
            quality *= 1.1

        return min(quality, 1.5)  # Cap the boost

    async def _get_recent_success_rate(
        self, station_id: str, route_id: str, track: str, target_time: datetime
    ) -> float:
        """Get recent prediction success rate for this track."""
        try:
            # Look at predictions from the last 30 days
            recent_key = f"track_recent_success:{station_id}:{route_id}:{track}"
            recent_data = await self.redis.get(recent_key)

            if recent_data:
                parts = recent_data.decode().split(":")
                if len(parts) == 2:
                    correct = int(parts[0])
                    total = int(parts[1])
                    if total > 0:
                        return correct / total

            # Default to moderate success rate for unknown tracks
            return 0.75
        except (ConnectionError, TimeoutError, ValueError):
            return 0.75

    async def _calculate_track_consistency(
        self,
        station_id: str,
        route_id: str,
        track: str,
        assignments: List[TrackAssignment],
    ) -> float:
        """Calculate how consistently this track is used."""
        try:
            # Count assignments for this track vs. total assignments
            track_assignments = [a for a in assignments if a.track_number == track]
            if not track_assignments:
                return 0.0

            # Calculate consistency over different time periods
            time_buckets: dict[int, int] = defaultdict(int)
            total_buckets: dict[int, int] = defaultdict(int)

            for assignment in assignments:
                if not assignment.track_number:
                    continue

                # Create time bucket (hour of day)
                bucket = assignment.hour
                total_buckets[bucket] += 1
                if assignment.track_number == track:
                    time_buckets[bucket] += 1

            # Calculate average consistency across time buckets
            if not total_buckets:
                return 0.0

            consistency_scores = []
            for bucket, total in total_buckets.items():
                if total > 0:
                    consistency = time_buckets[bucket] / total
                    consistency_scores.append(consistency)

            return (
                sum(consistency_scores) / len(consistency_scores)
                if consistency_scores
                else 0.0
            )

        except Exception as e:
            logger.error(f"Error calculating track consistency: {e}")
            return 0.5

    async def _store_prediction(self, prediction: TrackPrediction) -> None:
        """Store a prediction for later validation."""
        try:
            key = f"track_prediction:{prediction.station_id}:{prediction.route_id}:{prediction.trip_id}:{prediction.scheduled_time.date()}"
            await write_cache(
                self.redis,
                key,
                prediction.model_dump_json(),
                WEEK,  # Store for 1 week
            )
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to store prediction due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to store prediction due to validation error: {e}",
                exc_info=True,
            )

    async def _get_prediction_accuracy(
        self, station_id: str, route_id: str, track_number: str
    ) -> float:
        """Get historical prediction accuracy for a specific track."""
        try:
            # Get recent prediction validation results
            accuracy_key = f"track_accuracy:{station_id}:{route_id}:{track_number}"
            accuracy_data = await self.redis.get(accuracy_key)

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
            logger.error(f"Failed to get prediction accuracy: {e}")
            return 0.7  # Default accuracy

    async def validate_prediction(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        scheduled_time: datetime,
        actual_track_number: Optional[str],
    ) -> None:
        """
        Enhanced validation of predictions with detailed accuracy tracking.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_id: The trip ID
            scheduled_time: The scheduled departure time
            actual_track_number: The actual track number assigned
        """
        try:
            key = f"track_prediction:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
            prediction_data = await check_cache(self.redis, key)

            if not prediction_data:
                return

            prediction = TrackPrediction.model_validate_json(prediction_data)

            # Check if prediction was correct
            is_correct = prediction.track_number == actual_track_number

            # Enhanced statistics update
            await self._update_prediction_stats(
                station_id,
                route_id,
                is_correct,
                prediction.confidence_score,
                prediction.track_number,
                prediction.prediction_method,
            )

            # Update recent success rate tracking
            if prediction.track_number:
                await self._update_recent_success_rate(
                    station_id, route_id, prediction.track_number, is_correct
                )

            # Update per-method accuracy tracking
            await self._update_method_accuracy(
                station_id, route_id, prediction.prediction_method, is_correct
            )

            # Record validation metric with additional labels
            result = "correct" if is_correct else "incorrect"
            track_predictions_validated.labels(
                station_id=station_id,
                route_id=route_id,
                result=result,
                instance=INSTANCE_ID,
            ).inc()

            confidence_level = (
                "high"
                if prediction.confidence_score >= 0.8
                else "medium"
                if prediction.confidence_score >= 0.5
                else "low"
            )
            logger.info(
                f"Validated prediction for {station_id} {route_id}: {'CORRECT' if is_correct else 'INCORRECT'} "
                f"(predicted: {prediction.track_number}, actual: {actual_track_number}, "
                f"confidence: {prediction.confidence_score:.2f} ({confidence_level}), "
                f"method: {prediction.prediction_method})"
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to validate prediction due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to validate prediction due to validation error: {e}",
                exc_info=True,
            )

    async def _update_prediction_stats(
        self,
        station_id: str,
        route_id: str,
        is_correct: bool,
        confidence: float,
        predicted_track: Optional[str] = None,
        method: Optional[str] = None,
    ) -> None:
        """Enhanced prediction statistics update."""
        try:
            stats_key = f"track_stats:{station_id}:{route_id}"
            stats_data = await check_cache(self.redis, stats_key)

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

            # Update overall stats
            stats.total_predictions += 1
            if is_correct:
                stats.correct_predictions += 1
            stats.accuracy_rate = stats.correct_predictions / stats.total_predictions
            stats.last_updated = datetime.now(UTC)

            # Update per-track predictions count
            if predicted_track:
                if predicted_track not in stats.prediction_counts_by_track:
                    stats.prediction_counts_by_track[predicted_track] = 0
                stats.prediction_counts_by_track[predicted_track] += 1

            # Update average confidence with exponential moving average
            alpha = 0.1  # Learning rate for confidence updates
            if stats.total_predictions == 1:
                stats.average_confidence = confidence
            else:
                stats.average_confidence = (
                    alpha * confidence + (1 - alpha) * stats.average_confidence
                )

            # Store updated stats
            await write_cache(
                self.redis,
                stats_key,
                stats.model_dump_json(),
                30 * DAY,
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to update prediction stats due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to update prediction stats due to validation error: {e}",
                exc_info=True,
            )

    async def _update_recent_success_rate(
        self, station_id: str, route_id: str, track: str, is_correct: bool
    ) -> None:
        """Update recent success rate for track-specific accuracy."""
        try:
            recent_key = f"track_recent_success:{station_id}:{route_id}:{track}"
            recent_data = await self.redis.get(recent_key)

            correct_count = 0
            total_count = 0

            if recent_data:
                parts = recent_data.decode().split(":")
                if len(parts) == 2:
                    correct_count = int(parts[0])
                    total_count = int(parts[1])

            # Update counts
            total_count += 1
            if is_correct:
                correct_count += 1

            # Store with 30-day expiration
            await self.redis.setex(
                recent_key, 30 * DAY, f"{correct_count}:{total_count}"
            )
            redis_commands.labels("setex").inc()

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to update recent success rate: {e}")

    async def _update_method_accuracy(
        self, station_id: str, route_id: str, method: str, is_correct: bool
    ) -> None:
        """Track accuracy by prediction method."""
        try:
            method_key = f"track_method_accuracy:{station_id}:{route_id}:{method}"
            method_data = await self.redis.get(method_key)

            correct_count = 0
            total_count = 0

            if method_data:
                parts = method_data.decode().split(":")
                if len(parts) == 2:
                    correct_count = int(parts[0])
                    total_count = int(parts[1])

            total_count += 1
            if is_correct:
                correct_count += 1

            # Store with 90-day expiration
            await self.redis.setex(
                method_key, 90 * DAY, f"{correct_count}:{total_count}"
            )
            redis_commands.labels("setex").inc()

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to update method accuracy: {e}")

    async def _get_overall_accuracy(self, station_id: str, route_id: str) -> float:
        """Get overall prediction accuracy for dynamic threshold calculation."""
        try:
            stats = await self.get_prediction_stats(station_id, route_id)
            if stats and stats.total_predictions > 5:
                return stats.accuracy_rate
            else:
                # Default to moderate accuracy for stations with limited data
                return 0.6
        except Exception as e:
            logger.error(f"Failed to get overall accuracy: {e}")
            return 0.6

    async def get_prediction_stats(
        self, station_id: str, route_id: str
    ) -> Optional[TrackPredictionStats]:
        """Get prediction statistics for a station and route."""
        try:
            stats_key = f"track_stats:{station_id}:{route_id}"
            stats_data = await check_cache(self.redis, stats_key)

            if stats_data:
                return TrackPredictionStats.model_validate_json(stats_data)
            return None

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to get prediction stats due to Redis connection issue: {e}",
                exc_info=True,
            )
            return None
        except ValidationError as e:
            logger.error(
                f"Failed to get prediction stats due to validation error: {e}",
                exc_info=True,
            )
            return None
