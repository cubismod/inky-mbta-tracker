import logging
import os
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
import mbta_client
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

            # Look back 3 months for historical data
            start_date = scheduled_time - timedelta(days=90)
            end_date = scheduled_time

            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date
            )

            if not assignments:
                logger.debug(
                    f"No historical assignments found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
                )
                return {}

            # Analyze patterns by different criteria
            patterns: dict[str, dict[str, int]] = {
                "exact_match": defaultdict(int),  # Same headsign, time, direction
                "headsign_match": defaultdict(int),  # Same headsign, direction
                "time_match": defaultdict(int),  # Same time window (±30 min)
                "direction_match": defaultdict(int),  # Same direction
                "day_of_week_match": defaultdict(int),  # Same day of week
            }

            target_dow = scheduled_time.weekday()
            target_hour = scheduled_time.hour
            target_minute = scheduled_time.minute

            for assignment in assignments:
                if not assignment.track_number:
                    continue

                if assignment.track_number:
                    headsign_similarity = (
                        textdistance.levenshtein.normalized_similarity(
                            trip_headsign, assignment.headsign
                        )
                    )
                    # Exact match (same headsign, time window, direction)
                    # we append the commuter rail line to the headsign when processing
                    # in mbta_client.py so we do a substring match here
                    if (
                        headsign_similarity > 0.7
                        and assignment.direction_id == direction_id
                        and abs(assignment.hour - target_hour) <= 1
                    ):
                        logger.debug(
                            f"Exact match found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, track_id={assignment.track_number}"
                        )
                        patterns["exact_match"][assignment.track_number] += 10

                    # Headsign match
                    if (
                        headsign_similarity > 0.7
                        and assignment.direction_id == direction_id
                    ):
                        logger.debug(
                            f"Headsign match found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, track_id={assignment.track_number}"
                        )
                        patterns["headsign_match"][assignment.track_number] += 5

                    # Time match (±30 minutes)
                    time_diff = abs(
                        assignment.hour * 60
                        + assignment.minute
                        - target_hour * 60
                        - target_minute
                    )
                    if time_diff <= 30:
                        logger.debug(
                            f"Time match found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, track_id={assignment.track_number}"
                        )
                        patterns["time_match"][assignment.track_number] += 3

                    # Direction match
                    if assignment.direction_id == direction_id:
                        logger.debug(
                            f"Direction match found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, track_id={assignment.track_number}"
                        )
                        patterns["direction_match"][assignment.track_number] += 2

                    # Day of week match
                    if assignment.day_of_week == target_dow:
                        logger.debug(
                            f"Day of week match found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, track_id={assignment.track_number}"
                        )
                        patterns["day_of_week_match"][assignment.track_number] += 1

            # Combine all patterns with weights and time decay
            combined_scores: dict[str, float] = defaultdict(float)
            sample_counts: dict[str, int] = defaultdict(int)

            # Apply time-based weighting to historical data
            for assignment in assignments:
                if not assignment.track_number:
                    continue

                # Time decay factor (recent data weighted more heavily)
                days_old = (scheduled_time - assignment.scheduled_time).days
                time_decay = max(0.1, 1.0 - (days_old / 90.0))  # 90-day decay

                # Calculate pattern matches with existing logic
                headsign_similarity = textdistance.levenshtein.normalized_similarity(
                    trip_headsign, assignment.headsign
                )

                base_score = 0
                if (
                    headsign_similarity > 0.7
                    and assignment.direction_id == direction_id
                    and abs(assignment.hour - target_hour) <= 1
                ):
                    base_score = 10
                elif (
                    headsign_similarity > 0.7
                    and assignment.direction_id == direction_id
                ):
                    base_score = 5
                elif (
                    abs(
                        assignment.hour * 60
                        + assignment.minute
                        - target_hour * 60
                        - target_minute
                    )
                    <= 30
                ):
                    base_score = 3
                elif assignment.direction_id == direction_id:
                    base_score = 2
                elif assignment.day_of_week == target_dow:
                    # Enhanced day-type pattern matching
                    is_weekend = target_dow in [5, 6]  # Saturday, Sunday
                    is_assignment_weekend = assignment.day_of_week in [5, 6]

                    if is_weekend == is_assignment_weekend:
                        base_score = 2  # Weekend/weekday pattern match
                    else:
                        base_score = 1  # Basic day match

                if base_score > 0:
                    combined_scores[assignment.track_number] += base_score * time_decay
                    sample_counts[assignment.track_number] += 1

            # Convert to probabilities with confidence adjustments
            if combined_scores:
                total_score = sum(combined_scores.values())
                total = {}
                for track, score in combined_scores.items():
                    base_prob = score / total_score
                    sample_size = sample_counts[track]

                    # Sample size confidence (more samples = higher confidence)
                    sample_confidence = min(1.0, sample_size / 10.0)

                    # Historical accuracy factor
                    accuracy = await self._get_prediction_accuracy(
                        station_id, route_id, track
                    )

                    # Combined confidence adjustment
                    confidence_factor = 0.3 + 0.4 * sample_confidence + 0.3 * accuracy
                    adjusted_prob = base_prob * confidence_factor
                    total[track] = adjusted_prob

                # Renormalize after confidence adjustments
                adj_total = sum(total.values())
                if adj_total > 0:
                    total = {track: prob / adj_total for track, prob in total.items()}
                logger.debug(
                    f"Calculated pattern for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}"
                )
                logger.debug(f"Total score={total_score}, total={total}")

                # Record analysis duration
                duration = time.time() - start_time
                track_pattern_analysis_duration.labels(
                    station_id=station_id, route_id=route_id, instance=INSTANCE_ID
                ).set(duration)

                return total

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

            # Only make predictions if confidence is above threshold
            if confidence < 0.3:  # 30% confidence threshold
                logger.debug(
                    f"No prediction made for station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}"
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
        Validate a previous prediction against actual track assignment.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_id: The trip ID
            scheduled_time: The scheduled departure time
            actual_platform_code: The actual platform code assigned
            actual_platform_name: The actual platform name assigned
        """
        try:
            key = f"track_prediction:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
            prediction_data = await check_cache(self.redis, key)

            if not prediction_data:
                return

            prediction = TrackPrediction.model_validate_json(prediction_data)

            # Check if prediction was correct
            is_correct = prediction.track_number == actual_track_number

            # Update statistics
            await self._update_prediction_stats(
                station_id, route_id, is_correct, prediction.confidence_score
            )

            # Record validation metric
            result = "correct" if is_correct else "incorrect"
            track_predictions_validated.labels(
                station_id=station_id,
                route_id=route_id,
                result=result,
                instance=INSTANCE_ID,
            ).inc()

            logger.info(
                f"Validated prediction for {station_id} {route_id}: {'CORRECT' if is_correct else 'INCORRECT'}"
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
        self, station_id: str, route_id: str, is_correct: bool, confidence: float
    ) -> None:
        """Update prediction statistics."""
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

            # Update stats
            stats.total_predictions += 1
            if is_correct:
                stats.correct_predictions += 1
            stats.accuracy_rate = stats.correct_predictions / stats.total_predictions
            stats.last_updated = datetime.now(UTC)

            # Update average confidence
            old_total = (stats.total_predictions - 1) * stats.average_confidence
            stats.average_confidence = (
                old_total + confidence
            ) / stats.total_predictions

            # Store updated stats
            await write_cache(
                self.redis,
                stats_key,
                stats.model_dump_json(),
                30 * DAY,  # Store for 30 days
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
