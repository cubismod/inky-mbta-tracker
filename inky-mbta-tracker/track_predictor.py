import logging
import os
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis_cache import check_cache, write_cache
from shared_types import TrackAssignment, TrackPrediction, TrackPredictionStats
from times_in_seconds import DAY, WEEK

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

            # Store for 6 months for analysis
            await write_cache(
                self.redis,
                key,
                assignment.model_dump_json(),
                6 * 30 * DAY,  # 6 months
            )

            # Also store in a time-series format for easier querying
            time_series_key = (
                f"track_timeseries:{assignment.station_id}:{assignment.route_id}"
            )
            await self.redis.zadd(
                time_series_key, {key: assignment.scheduled_time.timestamp()}
            )

            # Set expiration for time series (6 months)
            await self.redis.expire(time_series_key, 6 * 30 * DAY)

            logger.debug(
                f"Stored track assignment: {assignment.station_id} {assignment.route_id} -> {assignment.platform_code or assignment.platform_name}"
            )

        # TODO: narrow exception handling
        except Exception as e:
            logger.error(f"Failed to store track assignment: {e}")

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

        # TODO: narrow exception
        except Exception as e:
            logger.error(f"Failed to retrieve historical assignments: {e}")
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
            # Look back 3 months for historical data
            start_date = scheduled_time - timedelta(days=90)
            end_date = scheduled_time

            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date
            )

            if not assignments:
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
                if not (assignment.platform_code or assignment.platform_name):
                    continue

                track_id = assignment.platform_code or assignment.platform_name

                if track_id:
                    # Exact match (same headsign, time window, direction)
                    if (
                        assignment.headsign == trip_headsign
                        and assignment.direction_id == direction_id
                        and abs(assignment.hour - target_hour) <= 1
                    ):
                        patterns["exact_match"][track_id] += 10

                    # Headsign match
                    if (
                        assignment.headsign == trip_headsign
                        and assignment.direction_id == direction_id
                    ):
                        patterns["headsign_match"][track_id] += 5

                    # Time match (±30 minutes)
                    time_diff = abs(
                        assignment.hour * 60
                        + assignment.minute
                        - target_hour * 60
                        - target_minute
                    )
                    if time_diff <= 30:
                        patterns["time_match"][track_id] += 3

                    # Direction match
                    if assignment.direction_id == direction_id:
                        patterns["direction_match"][track_id] += 2

                    # Day of week match
                    if assignment.day_of_week == target_dow:
                        patterns["day_of_week_match"][track_id] += 1

            # Combine all patterns with weights
            combined_scores: dict[str, float] = defaultdict(float)
            for _, tracks in patterns.items():
                for track_id, score in tracks.items():
                    combined_scores[track_id] += score

            # Convert to probabilities
            if combined_scores:
                total_score = sum(combined_scores.values())
                total = {
                    track: score / total_score
                    for track, score in combined_scores.items()
                }
                logger.debug(
                    f"Calculated pattern for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}"
                )
                logger.debug(f"Total score={total_score}, total={total}")
                return total

            return {}

        # TODO: narrow exception
        except Exception as e:
            logger.error(f"Failed to analyze patterns: {e}")
            return {}

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
                return None

            # Determine prediction method
            method = "historical_pattern"
            if confidence >= 0.8:
                method = "high_confidence_pattern"
            elif confidence >= 0.6:
                method = "medium_confidence_pattern"
            else:
                method = "low_confidence_pattern"

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

            # Create prediction
            prediction = TrackPrediction(
                station_id=station_id,
                route_id=route_id,
                trip_id=trip_id,
                headsign=headsign,
                direction_id=direction_id,
                scheduled_time=scheduled_time,
                predicted_platform_code=best_track if best_track.isdigit() else None,
                predicted_platform_name=best_track
                if not best_track.isdigit()
                else None,
                confidence_score=confidence,
                prediction_method=method,
                historical_matches=historical_matches,
                created_at=datetime.now(UTC),
            )

            # Store prediction for later validation
            await self._store_prediction(prediction)

            return prediction

        # TODO: narrow exception
        except Exception as e:
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
        # TODO: narrow exception
        except Exception as e:
            logger.error(f"Failed to store prediction: {e}")

    async def validate_prediction(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        scheduled_time: datetime,
        actual_platform_code: Optional[str],
        actual_platform_name: Optional[str],
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
            predicted_track = (
                prediction.predicted_platform_code or prediction.predicted_platform_name
            )
            actual_track = actual_platform_code or actual_platform_name

            is_correct = predicted_track == actual_track

            # Update statistics
            await self._update_prediction_stats(
                station_id, route_id, is_correct, prediction.confidence_score
            )

            logger.info(
                f"Validated prediction for {station_id} {route_id}: {'CORRECT' if is_correct else 'INCORRECT'}"
            )

        except Exception as e:
            logger.error(f"Failed to validate prediction: {e}")

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

        # TODO: narrow exception
        except Exception as e:
            logger.error(f"Failed to update prediction stats: {e}")

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

        # TODO: narrow exception
        except Exception as e:
            logger.error(f"Failed to get prediction stats: {e}")
            return None
