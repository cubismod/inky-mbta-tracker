import logging
import os
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
import shared_types.shared_types
import textdistance
from async_lru import alru_cache
from consts import DAY, HOUR, INSTANCE_ID, MBTA_V3_ENDPOINT, MINUTE, WEEK, YEAR
from exceptions import RateLimitExceeded
from mbta_client import MBTAApi
from mbta_responses import Schedules
from metaphone import doublemetaphone
from prometheus import (
    mbta_api_requests,
    redis_commands,
    track_historical_assignments_stored,
    track_negative_cache_hits,
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
from tenacity import (
    before_sleep_log,
    retry,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)

# Route family mappings for cross-route pattern learning
ROUTE_FAMILIES = {
    "CR-Worcester": ["CR-Framingham"],
    "CR-Framingham": ["CR-Worcester"],
    "CR-Franklin": ["CR-Foxboro", "CR-Franklin"],
    "CR-Foxboro": ["CR-Franklin", "CR-Foxboro"],
    "CR-Providence": ["CR-Providence", "CR-Stoughton"],
    "CR-Stoughton": ["CR-Providence", "CR-Stoughton"],
    "CR-Needham": ["CR-Needham"],
    "CR-Fairmount": ["CR-Fairmount"],
    "CR-Fitchburg": ["CR-Fitchburg"],
    "CR-Lowell": ["CR-Lowell"],
    "CR-Haverhill": ["CR-Haverhill"],
    "CR-Newburyport": ["CR-Newburyport"],
    "CR-Rockport": ["CR-Rockport"],
    "CR-Kingston": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
    "CR-Plymouth": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
    "CR-Greenbush": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
}

# Service type patterns
EXPRESS_KEYWORDS = ["express", "limited", "direct"]
LOCAL_KEYWORDS = ["local", "all stops", "stopping"]

# Station-specific confidence thresholds
STATION_CONFIDENCE_THRESHOLDS = {
    "place-sstat": 0.25,  # South Station - high volume, lower threshold
    "place-north": 0.25,  # North Station - high volume, lower threshold
    "place-bbsta": 0.30,  # Back Bay - medium volume
    "default": 0.35,  # Smaller stations need higher confidence
}


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
        self,
        station_id: str,
        route_id: str,
        start_date: datetime,
        end_date: datetime,
        include_related_routes: bool = False,
    ) -> List[TrackAssignment]:
        """
        Retrieve historical track assignments for a given station and route.

        Args:
            station_id: The station to query
            route_id: The route to query
            start_date: Start date for the query
            end_date: End date for the query
            include_related_routes: Whether to include assignments from related routes

        Returns:
            List of historical track assignments
        """
        try:
            routes_to_check = [route_id]
            if include_related_routes and route_id in ROUTE_FAMILIES:
                routes_to_check.extend(ROUTE_FAMILIES[route_id])

            results = []
            for current_route in routes_to_check:
                time_series_key = f"track_timeseries:{station_id}:{current_route}"

                # Get assignments within the time range
                assignments = await self.redis.zrangebyscore(
                    time_series_key, start_date.timestamp(), end_date.timestamp()
                )
                redis_commands.labels("zrangebyscore").inc()

                for assignment_key in assignments:
                    assignment_data = await check_cache(
                        self.redis, assignment_key.decode()
                    )
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

    def _enhanced_headsign_similarity(self, headsign1: str, headsign2: str) -> float:
        """
        Enhanced headsign similarity using multiple matching algorithms.

        Returns a score between 0.0 and 1.0.
        """
        if not headsign1 or not headsign2:
            return 0.0

        # Normalize strings
        h1 = headsign1.lower().strip()
        h2 = headsign2.lower().strip()

        if h1 == h2:
            return 1.0

        # Multiple similarity measures with weights
        levenshtein_sim = textdistance.levenshtein.normalized_similarity(h1, h2)
        jaccard_sim = textdistance.jaccard.normalized_similarity(h1.split(), h2.split())

        # Phonetic similarity using metaphone
        try:
            metaphone1 = doublemetaphone(h1)
            metaphone2 = doublemetaphone(h2)
            phonetic_match = 0.0
            if metaphone1[0] and metaphone2[0] and metaphone1[0] == metaphone2[0]:
                phonetic_match = 0.8
            elif metaphone1[1] and metaphone2[1] and metaphone1[1] == metaphone2[1]:
                phonetic_match = 0.6
        except (AttributeError, TypeError, ValueError):
            phonetic_match = 0.0

        # Token-based similarity for destination matching
        token_sim = 0.0
        tokens1 = set(h1.split())
        tokens2 = set(h2.split())
        if tokens1 and tokens2:
            intersection = len(tokens1.intersection(tokens2))
            union = len(tokens1.union(tokens2))
            token_sim = intersection / union if union > 0 else 0.0

        # Weighted combination
        combined_score = (
            0.4 * levenshtein_sim
            + 0.25 * jaccard_sim
            + 0.2 * phonetic_match
            + 0.15 * token_sim
        )

        return min(1.0, combined_score)

    def _detect_service_type(self, headsign: str) -> str:
        """
        Detect service type from headsign (express, local, etc.).
        """
        headsign_lower = headsign.lower()

        for keyword in EXPRESS_KEYWORDS:
            if keyword in headsign_lower:
                return "express"

        for keyword in LOCAL_KEYWORDS:
            if keyword in headsign_lower:
                return "local"

        return "regular"

    def _is_weekend_service(self, day_of_week: int) -> bool:
        """
        Check if the day represents weekend service.
        """
        return day_of_week in [5, 6]  # Saturday, Sunday

    def _get_station_confidence_threshold(self, station_id: str) -> float:
        """
        Get station-specific confidence threshold.
        """
        return STATION_CONFIDENCE_THRESHOLDS.get(
            station_id, STATION_CONFIDENCE_THRESHOLDS["default"]
        )

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

            # Check negative cache for this pattern
            negative_cache_key = f"no_prediction:{station_id}:{route_id}:{scheduled_time.date()}:{scheduled_time.hour}"
            cached_result = await check_cache(self.redis, negative_cache_key)
            if cached_result:
                cache_reason = (
                    cached_result.decode()
                    if isinstance(cached_result, bytes)
                    else str(cached_result)
                )
                track_negative_cache_hits.labels(
                    station_id=station_id,
                    route_id=route_id,
                    cache_reason=cache_reason,
                    instance=INSTANCE_ID,
                ).inc()
                logger.debug(
                    f"Negative cache hit for station_id={station_id}, route_id={route_id}, hour={scheduled_time.hour}, reason={cache_reason}"
                )
                return {}

            # Look back 3 months for historical data
            start_date = scheduled_time - timedelta(days=90)
            end_date = scheduled_time

            # Get assignments from same route and related routes
            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date, include_related_routes=True
            )

            if not assignments:
                logger.debug(
                    f"No historical assignments found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
                )
                # Cache negative result for 1 hour to avoid repeated calculations
                await write_cache(
                    self.redis,
                    negative_cache_key,
                    "no_data",
                    1 * HOUR,  # Cache for 1 hour
                )
                return {}

            # Analyze patterns by different criteria with expanded windows
            patterns: dict[str, dict[str, int]] = {
                "exact_match": defaultdict(int),  # Same headsign, time, direction
                "headsign_match": defaultdict(int),  # Same headsign, direction
                "time_match_30": defaultdict(int),  # Same time window (±30 min)
                "time_match_60": defaultdict(int),  # Extended time window (±60 min)
                "time_match_120": defaultdict(int),  # Wide time window (±120 min)
                "direction_match": defaultdict(int),  # Same direction
                "day_of_week_match": defaultdict(int),  # Same day of week
                "service_type_match": defaultdict(int),  # Same service type
                "weekend_pattern_match": defaultdict(int),  # Weekend vs weekday pattern
            }

            target_dow = scheduled_time.weekday()
            target_hour = scheduled_time.hour
            target_minute = scheduled_time.minute
            target_service_type = self._detect_service_type(trip_headsign)
            target_is_weekend = self._is_weekend_service(target_dow)

            for assignment in assignments:
                if not assignment.track_number:
                    continue

                if assignment.track_number:
                    # Use enhanced headsign similarity
                    headsign_similarity = self._enhanced_headsign_similarity(
                        trip_headsign, assignment.headsign
                    )

                    assignment_service_type = self._detect_service_type(
                        assignment.headsign
                    )
                    assignment_is_weekend = self._is_weekend_service(
                        assignment.day_of_week
                    )

                    # Calculate time differences for multiple windows
                    time_diff_minutes = abs(
                        assignment.hour * 60
                        + assignment.minute
                        - target_hour * 60
                        - target_minute
                    )

                    # Exact match (enhanced similarity + time + direction)
                    if (
                        headsign_similarity > 0.6  # Lowered from 0.7
                        and assignment.direction_id == direction_id
                        and abs(assignment.hour - target_hour) <= 1
                    ):
                        score = 15 * headsign_similarity  # Graduated scoring
                        patterns["exact_match"][assignment.track_number] += int(score)
                        logger.debug(
                            f"Exact match found (score={score}, similarity={headsign_similarity:.2f})"
                        )

                    # Headsign match (lowered threshold)
                    if (
                        headsign_similarity > 0.5  # Lowered from 0.7
                        and assignment.direction_id == direction_id
                    ):
                        score = 8 * headsign_similarity  # Graduated scoring
                        patterns["headsign_match"][assignment.track_number] += int(
                            score
                        )
                        logger.debug(
                            f"Headsign match found (score={score}, similarity={headsign_similarity:.2f})"
                        )

                    # Multiple time windows with different weights
                    if time_diff_minutes <= 30:
                        patterns["time_match_30"][assignment.track_number] += 5
                    elif time_diff_minutes <= 60:
                        patterns["time_match_60"][assignment.track_number] += 3
                    elif time_diff_minutes <= 120:
                        patterns["time_match_120"][assignment.track_number] += 2

                    # Direction match
                    if assignment.direction_id == direction_id:
                        patterns["direction_match"][assignment.track_number] += 2

                    # Service type match (express vs local)
                    if target_service_type == assignment_service_type:
                        patterns["service_type_match"][assignment.track_number] += 3

                    # Weekend vs weekday pattern matching
                    if target_is_weekend == assignment_is_weekend:
                        patterns["weekend_pattern_match"][assignment.track_number] += 2

                    # Day of week match (reduced weight due to weekend pattern)
                    if assignment.day_of_week == target_dow:
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

                # Calculate enhanced pattern matches
                headsign_similarity = self._enhanced_headsign_similarity(
                    trip_headsign, assignment.headsign
                )

                assignment_service_type = self._detect_service_type(assignment.headsign)
                assignment_is_weekend = self._is_weekend_service(assignment.day_of_week)

                time_diff_minutes = abs(
                    assignment.hour * 60
                    + assignment.minute
                    - target_hour * 60
                    - target_minute
                )

                base_score = 0.0

                # Exact match with enhanced similarity
                if (
                    headsign_similarity > 0.6
                    and assignment.direction_id == direction_id
                    and abs(assignment.hour - target_hour) <= 1
                ):
                    base_score = 15 * headsign_similarity
                # Strong headsign match
                elif (
                    headsign_similarity > 0.5
                    and assignment.direction_id == direction_id
                ):
                    base_score = 8 * headsign_similarity
                # Extended time windows
                elif time_diff_minutes <= 30:
                    base_score = 5.0
                elif time_diff_minutes <= 60:
                    base_score = 3.0
                elif time_diff_minutes <= 120:
                    base_score = 2.0
                elif assignment.direction_id == direction_id:
                    base_score = 2.0
                elif target_is_weekend == assignment_is_weekend:
                    base_score = 2.0  # Weekend/weekday pattern match
                elif assignment.day_of_week == target_dow:
                    base_score = 1.0  # Basic day match

                # Bonus for service type match
                if target_service_type == assignment_service_type and base_score > 0:
                    base_score = base_score * 1.2  # 20% bonus

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

            # Cache negative result for 1 hour to avoid repeated calculations
            await write_cache(
                self.redis,
                negative_cache_key,
                "no_patterns",
                1 * HOUR,  # Cache for 1 hour
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

            # Check negative cache first to avoid redundant calculations
            negative_cache_key = f"no_prediction:{station_id}:{route_id}:{scheduled_time.date()}:{scheduled_time.hour}"
            cached_result = await check_cache(self.redis, negative_cache_key)
            if cached_result:
                cache_reason = (
                    cached_result.decode()
                    if isinstance(cached_result, bytes)
                    else str(cached_result)
                )
                track_negative_cache_hits.labels(
                    station_id=station_id,
                    route_id=route_id,
                    cache_reason=cache_reason,
                    instance=INSTANCE_ID,
                ).inc()
                logger.debug(
                    f"Negative cache hit for station_id={station_id}, route_id={route_id}, hour={scheduled_time.hour}, reason={cache_reason}"
                )
                return None

            # it makes more sense to get the headsign client-side using the exact trip_id due to API rate limits
            async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
                async with MBTAApi(
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

            # Use station-specific confidence threshold
            confidence_threshold = self._get_station_confidence_threshold(station_id)
            if confidence < confidence_threshold:
                logger.debug(
                    f"No prediction made for station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, threshold={confidence_threshold}"
                )

                # Cache negative result for low confidence predictions
                negative_cache_key = f"no_prediction:{station_id}:{route_id}:{scheduled_time.date()}:{scheduled_time.hour}"
                await write_cache(
                    self.redis,
                    negative_cache_key,
                    "low_confidence",
                    1 * HOUR,  # Cache for 1 hour
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

    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def fetch_upcoming_departures(
        self,
        session: aiohttp.ClientSession,
        route_id: str,
        station_ids: List[str],
        target_date: Optional[datetime] = None,
    ) -> List[dict]:
        """
        Fetch upcoming departures for specific stations on a commuter rail route.

        Args:
            session: HTTP session for API calls
            route_id: Commuter rail route ID (e.g., "CR-Worcester")
            station_ids: List of station IDs to fetch departures for
            target_date: Date to fetch departures for (defaults to today)

        Returns:
            List of departure data dictionaries with scheduled times
        """
        if target_date is None:
            target_date = datetime.now(UTC)

        date_str = target_date.date().isoformat()
        auth_token = os.environ.get("AUTH_TOKEN", "")
        upcoming_departures = []

        # Fetch schedules for all stations in a single API call
        stations_str = ",".join(station_ids)

        # Filter to only upcoming departures (next 4 hours)
        current_time = target_date.strftime("%H:%M")
        four_hours_later = (target_date + timedelta(hours=4)).strftime("%H:%M")

        endpoint = f"{MBTA_V3_ENDPOINT}/schedules?filter[stop]={stations_str}&filter[route]={route_id}&filter[date]={date_str}&sort=departure_time&include=trip&api_key={auth_token}&filter[min_time]={current_time}&filter[max_time]={four_hours_later}"

        try:
            async with session.get(endpoint) as response:
                if response.status == 429:
                    raise RateLimitExceeded()

                if response.status != 200:
                    logger.error(
                        f"Failed to fetch schedules for {stations_str} on {route_id}: HTTP {response.status}"
                    )
                    return []

                body = await response.text()
                mbta_api_requests.labels("schedules").inc()

                try:
                    schedules_data = Schedules.model_validate_json(body, strict=False)
                except ValidationError as e:
                    logger.error(
                        f"Unable to parse schedules for {stations_str} on {route_id}: {e}"
                    )
                    return []

                # Process each scheduled departure
                for schedule in schedules_data.data:
                    if not schedule.attributes.departure_time:
                        continue

                    # Parse departure time

                    # Get trip information from relationships
                    trip_id = ""
                    if (
                        hasattr(schedule, "relationships")
                        and hasattr(schedule.relationships, "trip")
                        and schedule.relationships.trip.data
                    ):
                        trip_id = schedule.relationships.trip.data.id

                    # Get station ID from relationships
                    station_id = ""
                    if (
                        hasattr(schedule, "relationships")
                        and hasattr(schedule.relationships, "stop")
                        and schedule.relationships.stop.data
                    ):
                        station_id = schedule.relationships.stop.data.id

                    departure_info = {
                        "trip_id": trip_id,
                        "station_id": station_id,
                        "route_id": route_id,
                        "direction_id": schedule.attributes.direction_id,
                        "departure_time": schedule.attributes.departure_time,
                    }

                    upcoming_departures.append(departure_info)

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.error(
                f"Error fetching schedules for {stations_str} on {route_id}: {e}"
            )
            return []

        logger.info(
            f"Found {len(upcoming_departures)} upcoming departures for route {route_id} across {len(station_ids)} stations"
        )
        return upcoming_departures

    async def precache_track_predictions(
        self,
        routes: Optional[List[str]] = None,
        target_stations: Optional[List[str]] = None,
    ) -> int:
        """
        Precache track predictions for upcoming departures.

        Args:
            routes: List of route IDs to precache (defaults to all CR routes)
            target_stations: List of station IDs to precache for (defaults to supported stations)

        Returns:
            Number of predictions cached
        """
        if routes is None:
            routes = [
                "CR-Worcester",
                "CR-Framingham",
                "CR-Franklin",
                "CR-Foxboro",
                "CR-Providence",
                "CR-Stoughton",
                "CR-Needham",
                "CR-Fairmount",
                "CR-Fitchburg",
                "CR-Lowell",
                "CR-Haverhill",
                "CR-Newburyport",
                "CR-Rockport",
                "CR-Kingston",
                "CR-Plymouth",
                "CR-Greenbush",
            ]

        if target_stations is None:
            target_stations = [
                "place-sstat",  # South Station
                "place-north",  # North Station
                "place-bbsta",  # Back Bay
                "place-rugg",  # Ruggles
            ]

        predictions_cached = 0
        current_time = datetime.now(UTC)

        logger.info(
            f"Starting precache for {len(routes)} routes and {len(target_stations)} stations"
        )

        try:
            async with aiohttp.ClientSession() as session:
                for route_id in routes:
                    logger.info(f"Precaching predictions for route {route_id}")

                    try:
                        # Fetch upcoming departures with actual scheduled times
                        upcoming_departures = await self.fetch_upcoming_departures(
                            session, route_id, target_stations, current_time
                        )

                        for departure_data in upcoming_departures:
                            trip_id = departure_data["trip_id"]
                            station_id = departure_data["station_id"]
                            direction_id = departure_data["direction_id"]
                            scheduled_time = departure_data["departure_time"]

                            if not trip_id:
                                continue  # Skip if no trip ID available

                            try:
                                # Check if we already have a cached prediction
                                cache_key = f"track_prediction:{station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
                                if await check_cache(self.redis, cache_key):
                                    continue  # Already cached

                                # Generate prediction using actual scheduled departure time
                                prediction = await self.predict_track(
                                    station_id=station_id,
                                    route_id=route_id,
                                    trip_id=trip_id,
                                    headsign="",  # Will be fetched in predict_track method
                                    direction_id=direction_id,
                                    scheduled_time=scheduled_time,
                                )

                                if prediction:
                                    predictions_cached += 1
                                    logger.debug(
                                        f"Precached prediction: {station_id} {route_id} {trip_id} @ {scheduled_time.strftime('%H:%M')} -> Track {prediction.track_number}"
                                    )

                            except (
                                ConnectionError,
                                TimeoutError,
                                ValidationError,
                                RateLimitExceeded,
                            ) as e:
                                logger.error(
                                    f"Error precaching prediction for {station_id} {route_id} {trip_id}: {e}"
                                )

                    except (
                        ConnectionError,
                        TimeoutError,
                        ValidationError,
                        RateLimitExceeded,
                    ) as e:
                        logger.error(f"Error processing route {route_id}: {e}")
                        continue

        except (ConnectionError, TimeoutError, aiohttp.ClientError) as e:
            logger.error(f"Error in precache_track_predictions: {e}")

        logger.info(f"Precached {predictions_cached} track predictions")
        return predictions_cached
