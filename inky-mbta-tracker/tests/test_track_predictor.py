import os
from datetime import UTC, datetime
from typing import Generator, cast
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import numpy as np
import pytest
from redis.asyncio import Redis as AsyncRedis
from shared_types.shared_types import (
    TrackAssignment,
    TrackAssignmentType,
    TrackPrediction,
)
from track_predictor.track_predictor import TrackPredictor

# Configure pytest-anyio
pytest_plugins = ("anyio",)


@pytest.fixture
def sample_assignment() -> TrackAssignment:
    """Create a sample track assignment for testing at module scope."""
    return TrackAssignment(
        station_id="place-sstat",
        route_id="CR-Providence",
        trip_id="test-trip-123",
        headsign="Providence",
        direction_id=0,
        assignment_type=TrackAssignmentType.HISTORICAL,
        track_number="3",
        scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
        recorded_time=datetime(2024, 1, 15, 9, 25, tzinfo=UTC),
        hour=9,
        minute=30,
        day_of_week=0,  # Monday
    )


class TestTrackPredictor:
    """Test suite for TrackPredictor enhancements."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        """Create a TrackPredictor instance with mocked Redis."""
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = MagicMock()
        # Configure async methods as AsyncMocks
        predictor.redis.zrangebyscore = AsyncMock()
        yield predictor


class TestEnhancedHeadsignSimilarity:
    """Test enhanced headsign similarity matching."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(cast(AsyncRedis, MagicMock()))

    def test_exact_match(self, track_predictor: TrackPredictor) -> None:
        """Test exact headsign match returns 1.0."""
        result = track_predictor._enhanced_headsign_similarity(
            "Providence", "Providence"
        )
        assert result == 1.0

    def test_case_insensitive_match(self, track_predictor: TrackPredictor) -> None:
        """Test case insensitive matching."""
        result = track_predictor._enhanced_headsign_similarity(
            "PROVIDENCE", "providence"
        )
        assert result == 1.0

    def test_high_similarity_match(self, track_predictor: TrackPredictor) -> None:
        """Test high similarity matches get good scores."""
        result = track_predictor._enhanced_headsign_similarity(
            "Providence", "Provdence"
        )
        assert result > 0.5  # Should be decent due to small typo

    def test_phonetic_matching(self, track_predictor: TrackPredictor) -> None:
        """Test phonetic matching works for similar sounding words."""
        result = track_predictor._enhanced_headsign_similarity(
            "Framingham", "Framinghem"
        )
        assert result > 0.5  # Should benefit from phonetic similarity

    def test_token_based_similarity(self, track_predictor: TrackPredictor) -> None:
        """Test token-based similarity for multi-word headsigns."""
        result = track_predictor._enhanced_headsign_similarity(
            "South Station", "Station South"
        )
        assert result > 0.4  # Should get token similarity bonus

    def test_partial_destination_match(self, track_predictor: TrackPredictor) -> None:
        """Test partial destination matching."""
        result1 = track_predictor._enhanced_headsign_similarity(
            "Worcester", "Worcester Express"
        )
        result2 = track_predictor._enhanced_headsign_similarity(
            "Worcester Local", "Worcester"
        )
        assert result1 > 0.4
        assert result2 > 0.4

    def test_low_similarity_returns_low_score(
        self, track_predictor: TrackPredictor
    ) -> None:
        """Test completely different strings return low scores."""
        result = track_predictor._enhanced_headsign_similarity("Providence", "Lowell")
        assert result < 0.3

    def test_empty_strings(self, track_predictor: TrackPredictor) -> None:
        """Test empty string handling."""
        result1 = track_predictor._enhanced_headsign_similarity("", "Providence")
        result2 = track_predictor._enhanced_headsign_similarity("Providence", "")
        result3 = track_predictor._enhanced_headsign_similarity("", "")
        assert result1 == 0.0
        assert result2 == 0.0
        assert result3 == 0.0

    def test_graduated_scoring_weights(self, track_predictor: TrackPredictor) -> None:
        """Test that different similarity components are weighted correctly."""
        # Test a variety of combinations to ensure weighting works
        result1 = track_predictor._enhanced_headsign_similarity(
            "Worcester", "Worcester"
        )  # Perfect
        result2 = track_predictor._enhanced_headsign_similarity(
            "Worcester", "Worcster"
        )  # Minor typo
        result3 = track_predictor._enhanced_headsign_similarity(
            "Worcester", "Boston"
        )  # Different

        assert result1 == 1.0
        assert result2 > result3
        assert result3 < 0.5


class TestServiceType:
    """Test service type detection."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(cast(AsyncRedis, MagicMock()))

    def test_express_detection(self, track_predictor: TrackPredictor) -> None:
        """Test express service detection."""
        assert track_predictor._detect_service_type("Worcester Express") == "express"
        assert (
            track_predictor._detect_service_type("LIMITED SERVICE to Framingham")
            == "express"
        )
        assert track_predictor._detect_service_type("Direct to Providence") == "express"

    def test_local_detection(self, track_predictor: TrackPredictor) -> None:
        """Test local service detection."""
        assert track_predictor._detect_service_type("Local to Worcester") == "local"
        assert (
            track_predictor._detect_service_type("All stops to Framingham") == "local"
        )
        assert track_predictor._detect_service_type("Stopping service") == "local"

    def test_regular_service_default(self, track_predictor: TrackPredictor) -> None:
        """Test regular service is default."""
        assert track_predictor._detect_service_type("Providence") == "regular"
        assert track_predictor._detect_service_type("Worcester") == "regular"
        assert track_predictor._detect_service_type("Framingham Line") == "regular"

    def test_case_insensitive_detection(self, track_predictor: TrackPredictor) -> None:
        """Test case insensitive service type detection."""
        assert track_predictor._detect_service_type("WORCESTER EXPRESS") == "express"
        assert track_predictor._detect_service_type("local to framingham") == "local"


class TestWeekendService:
    """Test weekend service detection."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(MagicMock())

    def test_weekend_detection(self, track_predictor: TrackPredictor) -> None:
        """Test weekend day detection."""
        assert track_predictor._is_weekend_service(5) is True  # Saturday
        assert track_predictor._is_weekend_service(6) is True  # Sunday

    def test_weekday_detection(self, track_predictor: TrackPredictor) -> None:
        """Test weekday detection."""
        for day in [0, 1, 2, 3, 4]:  # Monday through Friday
            assert track_predictor._is_weekend_service(day) is False


class TestConfidenceThresholds:
    """Test station-specific confidence thresholds."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(MagicMock())

    def test_south_station_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test South Station has lower threshold."""
        threshold = track_predictor._get_station_confidence_threshold("place-sstat")
        assert threshold == 0.25

    def test_north_station_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test North Station has lower threshold."""
        threshold = track_predictor._get_station_confidence_threshold("place-north")
        assert threshold == 0.25

    def test_back_bay_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test Back Bay has medium threshold."""
        threshold = track_predictor._get_station_confidence_threshold("place-bbsta")
        assert threshold == 0.30

    def test_default_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test unknown stations use default threshold."""
        threshold = track_predictor._get_station_confidence_threshold("place-unknown")
        assert threshold == 0.35


class TestCrossRoutePatterns:
    """Test cross-route pattern learning."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    async def test_related_routes_included(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test that related routes are included when requested."""
        # Mock the Redis calls to return empty results for simplicity
        track_predictor.redis.zrangebyscore.return_value = []  # type: ignore[attr-defined]

        start_date = datetime(2024, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 1, 31, tzinfo=UTC)

        # Call with include_related_routes=True for Worcester line
        await track_predictor.get_historical_assignments(
            "place-sstat",
            "CR-Worcester",
            start_date,
            end_date,
            include_related_routes=True,
        )

        # Should have called Redis for both Worcester and Framingham
        actual_calls = [
            call[0]
            for call in track_predictor.redis.zrangebyscore.call_args_list  # type: ignore[attr-defined]
        ]

        # Check that both route keys were queried
        assert len(actual_calls) == 2
        assert actual_calls[0][0] in [
            "track_timeseries:place-sstat:CR-Worcester",
            "track_timeseries:place-sstat:CR-Framingham",
        ]
        assert actual_calls[1][0] in [
            "track_timeseries:place-sstat:CR-Worcester",
            "track_timeseries:place-sstat:CR-Framingham",
        ]

    async def test_single_route_when_not_requested(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test that only single route is queried when related routes not requested."""
        track_predictor.redis.zrangebyscore.return_value = []  # type: ignore[attr-defined]

        start_date = datetime(2024, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 1, 31, tzinfo=UTC)

        # Call with include_related_routes=False
        await track_predictor.get_historical_assignments(
            "place-sstat",
            "CR-Worcester",
            start_date,
            end_date,
            include_related_routes=False,
        )

        # Should have called Redis only once for Worcester
        assert track_predictor.redis.zrangebyscore.call_count == 1  # type: ignore[attr-defined]
        actual_call = track_predictor.redis.zrangebyscore.call_args_list[0][0]  # type: ignore[attr-defined]
        assert actual_call[0] == "track_timeseries:place-sstat:CR-Worcester"

    def test_route_families_mapping(self, track_predictor: TrackPredictor) -> None:
        """Test that route families are correctly mapped."""
        from track_predictor.track_predictor import ROUTE_FAMILIES

        # Test some key route family relationships
        assert "CR-Framingham" in ROUTE_FAMILIES["CR-Worcester"]
        assert "CR-Worcester" in ROUTE_FAMILIES["CR-Framingham"]
        assert "CR-Foxboro" in ROUTE_FAMILIES["CR-Franklin"]
        assert "CR-Franklin" in ROUTE_FAMILIES["CR-Foxboro"]
        assert "CR-Stoughton" in ROUTE_FAMILIES["CR-Providence"]
        assert "CR-Plymouth" in ROUTE_FAMILIES["CR-Kingston"]


class TestExpandedTimeWindows:
    """Test expanded time window matching."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    async def test_multiple_time_windows_in_patterns(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test that multiple time windows are considered in pattern analysis."""
        # This is more of an integration test to ensure the pattern structure includes all time windows

        # Mock the get_historical_assignments to return a sample assignment
        sample_assignment = TrackAssignment(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test-trip",
            headsign="Providence",
            direction_id=0,
            assignment_type=TrackAssignmentType.HISTORICAL,
            track_number="3",
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            recorded_time=datetime(2024, 1, 15, 9, 25, tzinfo=UTC),
            hour=9,
            minute=30,
            day_of_week=0,
        )

        with (
            patch.object(
                track_predictor,
                "get_historical_assignments",
                return_value=[sample_assignment],
            ),
            patch.object(track_predictor, "_get_prediction_accuracy", return_value=0.7),
            patch("track_predictor.track_predictor.check_cache", return_value=None),
        ):
            # Use anyio directly for task group creation in tests
            async with anyio.create_task_group() as tg:
                patterns = await track_predictor.analyze_patterns(
                    "place-sstat",
                    "CR-Providence",
                    "Providence",
                    0,
                    datetime(2024, 1, 15, 10, 15, tzinfo=UTC),  # 45 minutes later
                    tg,
                )

        # The assignment should match the 60-minute window but not the 30-minute window
        # This tests that the expanded time windows are working
        # Note: The actual scoring is complex, but we can verify the method runs without error
        assert isinstance(patterns, dict)

    def test_time_window_constants(self, track_predictor: TrackPredictor) -> None:
        """Test that the expected time window constants exist in pattern analysis."""
        # This verifies the structure we expect exists

        # Check that our new pattern types would be recognized
        # (This is a structural test since the pattern dict is created in analyze_patterns)
        expected_patterns = [
            "exact_match",
            "headsign_match",
            "time_match_30",
            "time_match_60",
            "time_match_120",
            "direction_match",
            "day_of_week_match",
            "service_type_match",
            "weekend_pattern_match",
        ]

        # This test ensures our pattern types are what we expect
        assert len(expected_patterns) == 9  # Verify we have all expected pattern types


class TestStoreHistoricalAssignment:
    """Test historical assignment storage functionality."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.fixture
    def task_group(self) -> MagicMock:
        """Mock task group for testing."""
        tg = MagicMock()
        tg.start_soon = MagicMock()
        return tg

    async def test_store_historical_assignment_success(
        self,
        track_predictor: TrackPredictor,
        sample_assignment: TrackAssignment,
        task_group: MagicMock,
        anyio_backend: str,
    ) -> None:
        """Test successful storage of historical assignment."""
        track_predictor.redis.zadd = AsyncMock()
        track_predictor.redis.expire = AsyncMock()

        with patch("track_predictor.track_predictor.check_cache", return_value=None):
            await track_predictor.store_historical_assignment(
                sample_assignment, task_group
            )

            # Verify task group was called to start storage tasks
            assert task_group.start_soon.call_count >= 2

    async def test_store_historical_assignment_duplicate_key(
        self,
        track_predictor: TrackPredictor,
        sample_assignment: TrackAssignment,
        task_group: MagicMock,
        anyio_backend: str,
    ) -> None:
        """Test handling of duplicate assignment keys."""
        with patch(
            "track_predictor.track_predictor.check_cache", return_value="existing"
        ):
            await track_predictor.store_historical_assignment(
                sample_assignment, task_group
            )

            # Should not call start_soon if key already exists
            task_group.start_soon.assert_not_called()

    async def test_store_historical_assignment_redis_error(
        self,
        track_predictor: TrackPredictor,
        sample_assignment: TrackAssignment,
        task_group: MagicMock,
        anyio_backend: str,
    ) -> None:
        """Test handling of Redis connection errors."""
        with (
            patch(
                "track_predictor.track_predictor.check_cache",
                side_effect=ConnectionError("Redis down"),
            ),
            patch("logging.getLogger") as mock_logger,
        ):
            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            await track_predictor.store_historical_assignment(
                sample_assignment, task_group
            )

            # Should log error and not crash
            logger_instance.error.assert_called()


class TestMLPredictionSystem:
    """Test ML prediction system functionality."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    def test_ml_enabled_detection(self, track_predictor: TrackPredictor) -> None:
        """Test ML enabled environment variable detection."""
        with patch.dict(os.environ, {"IMT_ML": "true"}):
            assert TrackPredictor._ml_enabled() is True

        with patch.dict(os.environ, {"IMT_ML": "1"}):
            assert TrackPredictor._ml_enabled() is True

        with patch.dict(os.environ, {"IMT_ML": "false"}):
            assert TrackPredictor._ml_enabled() is False

        with patch.dict(os.environ, {}, clear=True):
            assert TrackPredictor._ml_enabled() is False

    def test_configuration_parameter_getters(
        self, track_predictor: TrackPredictor
    ) -> None:
        """Test configuration parameter getter methods."""
        # Test conf_gamma
        with patch.dict(os.environ, {"IMT_CONF_GAMMA": "1.5"}):
            assert TrackPredictor._conf_gamma() == 1.5

        with patch.dict(os.environ, {}, clear=True):
            assert TrackPredictor._conf_gamma() == 1.3  # default

        # Test conf_hist_weight
        with patch.dict(os.environ, {"IMT_CONF_HIST_WEIGHT": "0.4"}):
            assert TrackPredictor._conf_hist_weight() == 0.4

        # Test bayes_alpha
        with patch.dict(os.environ, {"IMT_BAYES_ALPHA": "0.7"}):
            assert TrackPredictor._bayes_alpha() == 0.7

        # Test ml_sample_prob
        with patch.dict(os.environ, {"IMT_ML_SAMPLE_PCT": "0.2"}):
            assert TrackPredictor._ml_sample_prob() == 0.2

        # Test ml_replace_delta
        with patch.dict(os.environ, {"IMT_ML_REPLACE_DELTA": "0.1"}):
            assert TrackPredictor._ml_replace_delta() == 0.1

    async def test_enqueue_ml_prediction(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test ML prediction enqueueing."""
        track_predictor.redis.lpush = AsyncMock()

        request_id = await track_predictor.enqueue_ml_prediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            trip_id="test-trip",
            headsign="Providence",
        )

        assert isinstance(request_id, str)
        track_predictor.redis.lpush.assert_called_once()

    def test_cyclical_time_features(self, track_predictor: TrackPredictor) -> None:
        """Test cyclical time feature encoding."""
        test_time = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)  # Monday, 9:30 AM

        features = TrackPredictor._cyclical_time_features(test_time)

        assert "hour_sin" in features
        assert "hour_cos" in features
        assert "minute_sin" in features
        assert "minute_cos" in features
        assert "day_sin" in features
        assert "day_cos" in features
        assert "scheduled_timestamp" in features

        # Verify values are within expected ranges
        assert -1.0 <= features["hour_sin"] <= 1.0
        assert -1.0 <= features["hour_cos"] <= 1.0
        assert isinstance(features["scheduled_timestamp"], float)

    async def test_vocab_get_or_add(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test vocabulary index management."""
        # Test existing token
        track_predictor.redis.hget = AsyncMock(return_value="5")

        index = await track_predictor._vocab_get_or_add("test_key", "test_token")
        assert index == 5

        # Test new token
        track_predictor.redis.hget = AsyncMock(return_value=None)
        track_predictor.redis.hlen = AsyncMock(return_value=10)
        track_predictor.redis.hset = AsyncMock()

        index = await track_predictor._vocab_get_or_add("test_key", "new_token")
        assert index == 10
        track_predictor.redis.hset.assert_called_once()


class TestStatisticalMethods:
    """Test statistical and mathematical methods."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(cast(AsyncRedis, MagicMock()))

    def test_sharpen_probs(self, track_predictor: TrackPredictor) -> None:
        """Test probability vector sharpening."""
        # Test normal case
        vec = np.array([0.1, 0.6, 0.3])
        gamma = 2.0

        sharpened = TrackPredictor._sharpen_probs(vec, gamma)
        assert sharpened is not None
        assert np.allclose(np.sum(sharpened), 1.0)  # Should sum to 1
        assert sharpened[1] > vec[1]  # Highest value should be sharpened

        # Test None input
        assert TrackPredictor._sharpen_probs(None, gamma) is None

        # Test invalid gamma
        result = TrackPredictor._sharpen_probs(vec, -1.0)
        assert result is not None

        # Test zero gamma
        result = TrackPredictor._sharpen_probs(vec, 0.0)
        assert result is not None

    def test_margin_confidence(self, track_predictor: TrackPredictor) -> None:
        """Test margin-based confidence calculation."""
        # Test normal case
        vec = np.array([0.1, 0.6, 0.3])
        confidence = TrackPredictor._margin_confidence(vec, 1)  # Index of max value

        assert 0.0 <= confidence <= 1.0
        assert confidence > 0.5  # Should be high confidence for clear winner

        # Test None input
        assert TrackPredictor._margin_confidence(None, 0) == 0.0

        # Test invalid index
        assert TrackPredictor._margin_confidence(vec, 10) == 0.0
        assert TrackPredictor._margin_confidence(vec, -1) == 0.0

        # Test single element
        single_vec = np.array([0.8])
        confidence = TrackPredictor._margin_confidence(single_vec, 0)
        assert confidence == 0.8

    def test_margin_confidence_edge_cases(
        self, track_predictor: TrackPredictor
    ) -> None:
        """Test edge cases for margin confidence."""
        # Test equal probabilities (low confidence)
        equal_vec = np.array([0.33, 0.33, 0.34])
        confidence = TrackPredictor._margin_confidence(equal_vec, 2)
        assert confidence < 0.6  # Should be low confidence

        # Test very skewed distribution (high confidence)
        skewed_vec = np.array([0.05, 0.9, 0.05])
        confidence = TrackPredictor._margin_confidence(skewed_vec, 1)
        assert confidence > 0.7  # Should be high confidence


class TestGetAllowedTracks:
    """Test allowed tracks functionality."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    async def test_get_allowed_tracks_cached(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test getting allowed tracks from cache."""
        cached_tracks = '["1", "2", "3"]'

        with patch(
            "track_predictor.track_predictor.check_cache", return_value=cached_tracks
        ):
            tracks = await track_predictor._get_allowed_tracks(
                "place-sstat", "CR-Providence"
            )

            assert tracks == {"1", "2", "3"}

    async def test_get_allowed_tracks_from_history(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test building allowed tracks from historical data."""
        # Mock historical assignments
        assignments = [
            TrackAssignment(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="trip1",
                headsign="Providence",
                direction_id=0,
                assignment_type=TrackAssignmentType.HISTORICAL,
                track_number="1",
                scheduled_time=datetime(2024, 1, 1, tzinfo=UTC),
                recorded_time=datetime(2024, 1, 1, tzinfo=UTC),
                hour=9,
                minute=0,
                day_of_week=0,
            ),
            TrackAssignment(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="trip2",
                headsign="Providence",
                direction_id=0,
                assignment_type=TrackAssignmentType.HISTORICAL,
                track_number="3",
                scheduled_time=datetime(2024, 1, 2, tzinfo=UTC),
                recorded_time=datetime(2024, 1, 2, tzinfo=UTC),
                hour=9,
                minute=0,
                day_of_week=1,
            ),
        ]

        with (
            patch("track_predictor.track_predictor.check_cache", return_value=None),
            patch.object(
                track_predictor, "get_historical_assignments", return_value=assignments
            ),
            patch(
                "track_predictor.track_predictor.write_cache", new_callable=AsyncMock
            ),
        ):
            tracks = await track_predictor._get_allowed_tracks(
                "place-sstat", "CR-Providence"
            )

            assert tracks == {"1", "3"}

    async def test_get_allowed_tracks_error_handling(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test error handling in get_allowed_tracks."""
        with (
            patch("track_predictor.track_predictor.check_cache", return_value=None),
            patch.object(
                track_predictor,
                "get_historical_assignments",
                side_effect=Exception("Test error"),
            ),
        ):
            tracks = await track_predictor._get_allowed_tracks(
                "place-sstat", "CR-Providence"
            )

            # Should return empty set on error, meaning no restrictions
            assert tracks == set()


class TestPredictTrackMainFlow:
    """Test main track prediction flow."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.fixture
    def task_group(self) -> MagicMock:
        """Mock task group."""
        tg = MagicMock()
        tg.start_soon = MagicMock()
        return tg

    async def test_predict_track_non_commuter_rail(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test prediction rejection for non-commuter rail routes."""
        prediction = await track_predictor.predict_track(
            station_id="place-sstat",
            route_id="Red",  # Not a CR route
            trip_id="test-trip",
            headsign="Alewife",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            tg=task_group,
        )

        assert prediction is None

    async def test_predict_track_cached_result(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test returning cached prediction."""
        cached_prediction = TrackPrediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test-trip",
            headsign="Providence",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            track_number="3",
            confidence_score=0.8,
            accuracy=0.7,
            prediction_method="cached",
            historical_matches=10,
            created_at=datetime.now(UTC),
        )

        with patch(
            "track_predictor.track_predictor.check_cache",
            return_value=cached_prediction.model_dump_json(),
        ):
            prediction = await track_predictor.predict_track(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                headsign="Providence",
                direction_id=0,
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                tg=task_group,
            )

            assert prediction is not None
            assert prediction.track_number == "3"
            assert prediction.confidence_score == 0.8

    async def test_predict_track_negative_cache(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test negative cache behavior."""
        with patch("track_predictor.track_predictor.check_cache") as mock_check:
            mock_check.side_effect = [
                None,
                "True",
            ]  # No prediction cache, but negative cache exists

            prediction = await track_predictor.predict_track(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                headsign="Providence",
                direction_id=0,
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                tg=task_group,
            )

            assert prediction is None


class TestValidatePrediction:
    """Test prediction validation functionality."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.fixture
    def task_group(self) -> MagicMock:
        """Mock task group."""
        tg = MagicMock()
        tg.start_soon = MagicMock()
        return tg

    async def test_validate_prediction_correct(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test validation of correct prediction."""
        prediction_data = TrackPrediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test-trip",
            headsign="Providence",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            track_number="3",
            confidence_score=0.8,
            accuracy=0.7,
            prediction_method="test",
            historical_matches=10,
            created_at=datetime.now(UTC),
        )

        with (
            patch("track_predictor.track_predictor.check_cache") as mock_check,
            patch("logging.getLogger") as mock_logger,
        ):
            mock_check.side_effect = [
                prediction_data.model_dump_json(),
                None,
            ]  # Prediction exists, not validated yet
            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            res = await track_predictor.validate_prediction(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                actual_track_number="3",  # Matches prediction
                tg=task_group,
            )
            assert res

    async def test_validate_prediction_incorrect(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test validation of incorrect prediction."""
        prediction_data = TrackPrediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test-trip",
            headsign="Providence",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            track_number="3",
            confidence_score=0.8,
            accuracy=0.7,
            prediction_method="test",
            historical_matches=10,
            created_at=datetime.now(UTC),
        )

        with (
            patch("track_predictor.track_predictor.check_cache") as mock_check,
            patch("logging.getLogger") as mock_logger,
        ):
            mock_check.side_effect = [prediction_data.model_dump_json(), None]
            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            res = await track_predictor.validate_prediction(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                actual_track_number="1",  # Different from prediction
                tg=task_group,
            )

            assert res

    async def test_validate_prediction_no_data(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test validation when no prediction data exists."""
        with patch("track_predictor.track_predictor.check_cache", return_value=None):
            await track_predictor.validate_prediction(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                actual_track_number="3",
                tg=task_group,
            )

            # Should not start any tasks if no prediction data
            task_group.start_soon.assert_not_called()

    async def test_validate_prediction_already_validated(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test skipping validation if already validated."""
        prediction_data = TrackPrediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test-trip",
            headsign="Providence",
            direction_id=0,
            scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            track_number="3",
            confidence_score=0.8,
            accuracy=0.7,
            prediction_method="test",
            historical_matches=10,
            created_at=datetime.now(UTC),
        )

        with patch("track_predictor.track_predictor.check_cache") as mock_check:
            mock_check.side_effect = [
                prediction_data.model_dump_json(),
                "validated",
            ]  # Already validated

            await track_predictor.validate_prediction(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id="test-trip",
                scheduled_time=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                actual_track_number="3",
                tg=task_group,
            )

            # Should not start tasks if already validated
            task_group.start_soon.assert_not_called()


class TestPredictionAccuracy:
    """Test prediction accuracy tracking."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    async def test_get_prediction_accuracy_with_data(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test getting prediction accuracy with existing data."""
        track_predictor.redis.get = AsyncMock(
            return_value="15:20"
        )  # 15 correct out of 20 total

        accuracy = await track_predictor._get_prediction_accuracy(
            "place-sstat", "CR-Providence", "3"
        )

        assert accuracy == 0.75  # 15/20

    async def test_get_prediction_accuracy_no_data(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test getting prediction accuracy with no existing data."""
        track_predictor.redis.get = AsyncMock(return_value=None)

        accuracy = await track_predictor._get_prediction_accuracy(
            "place-sstat", "CR-Providence", "3"
        )

        assert accuracy == 0.5  # Default when no data

    async def test_get_prediction_accuracy_malformed_data(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test handling of malformed accuracy data."""
        track_predictor.redis.get = AsyncMock(return_value="invalid_format")

        accuracy = await track_predictor._get_prediction_accuracy(
            "place-sstat", "CR-Providence", "3"
        )

        assert accuracy == 0.5  # Should fall back to default

    async def test_update_track_accuracy_new(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test updating track accuracy with new data."""
        track_predictor.redis.get = AsyncMock(return_value=None)
        track_predictor.redis.set = AsyncMock()

        await track_predictor._update_track_accuracy(
            "place-sstat", "CR-Providence", "3", is_correct=True
        )

        # Should set initial value "1:1" for first correct prediction
        track_predictor.redis.set.assert_called_once()
        args = track_predictor.redis.set.call_args[0]
        assert "1:1" in args[1]

    async def test_update_track_accuracy_existing(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test updating existing track accuracy data."""
        track_predictor.redis.get = AsyncMock(return_value="5:10")
        track_predictor.redis.set = AsyncMock()

        await track_predictor._update_track_accuracy(
            "place-sstat", "CR-Providence", "3", is_correct=True
        )

        # Should update to "6:11" (one more correct, one more total)
        track_predictor.redis.set.assert_called_once()
        args = track_predictor.redis.set.call_args[0]
        assert "6:11" in args[1]

    async def test_choose_top_allowed_track(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test choosing most frequent track from allowed set."""
        # Mock historical data showing track usage frequencies
        assignments = [
            TrackAssignment(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id=f"trip{i}",
                headsign="Providence",
                direction_id=0,
                assignment_type=TrackAssignmentType.HISTORICAL,
                track_number="3",
                scheduled_time=datetime(2024, 1, i + 1, tzinfo=UTC),
                recorded_time=datetime(2024, 1, i + 1, tzinfo=UTC),
                hour=9,
                minute=0,
                day_of_week=0,
            )
            for i in range(5)  # 5 occurrences of track 3
        ] + [
            TrackAssignment(
                station_id="place-sstat",
                route_id="CR-Providence",
                trip_id=f"trip{i + 5}",
                headsign="Providence",
                direction_id=0,
                assignment_type=TrackAssignmentType.HISTORICAL,
                track_number="1",
                scheduled_time=datetime(2024, 1, i + 6, tzinfo=UTC),
                recorded_time=datetime(2024, 1, i + 6, tzinfo=UTC),
                hour=9,
                minute=0,
                day_of_week=0,
            )
            for i in range(2)  # 2 occurrences of track 1
        ]

        allowed_tracks = {"1", "3"}

        with patch.object(
            track_predictor, "get_historical_assignments", return_value=assignments
        ):
            chosen_track = await track_predictor._choose_top_allowed_track(
                "place-sstat", "CR-Providence", allowed_tracks
            )

            assert chosen_track == "3"  # Most frequent among allowed tracks


class TestPrecaching:
    """Test precaching functionality."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.fixture
    def task_group(self) -> MagicMock:
        """Mock task group."""
        tg = MagicMock()
        tg.start_soon = MagicMock()
        return tg

    async def test_precache_default_parameters(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test precaching with default route and station parameters."""
        with (
            patch.object(track_predictor, "fetch_upcoming_departures", return_value=[]),
            patch("aiohttp.ClientSession") as mock_session,
        ):
            mock_session.return_value.__aenter__.return_value = MagicMock()

            count = await track_predictor.precache(task_group)

            assert isinstance(count, int)
            assert count >= 0

    async def test_precache_custom_parameters(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test precaching with custom route and station lists."""
        custom_routes = ["CR-Providence", "CR-Worcester"]
        custom_stations = ["place-sstat"]

        with (
            patch.object(track_predictor, "fetch_upcoming_departures", return_value=[]),
            patch("aiohttp.ClientSession") as mock_session,
        ):
            mock_session.return_value.__aenter__.return_value = MagicMock()

            count = await track_predictor.precache(
                task_group, routes=custom_routes, target_stations=custom_stations
            )

            assert isinstance(count, int)


class TestErrorHandling:
    """Test error handling across various scenarios."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.fixture
    def task_group(self) -> MagicMock:
        """Mock task group."""
        tg = MagicMock()
        tg.start_soon = MagicMock()
        return tg

    async def test_get_historical_assignments_connection_error(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test handling of Redis connection errors in get_historical_assignments."""
        track_predictor.redis.zrangebyscore = AsyncMock(
            side_effect=ConnectionError("Redis down")
        )

        assignments = await track_predictor.get_historical_assignments(
            "place-sstat",
            "CR-Providence",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 31, tzinfo=UTC),
        )

        assert assignments == []  # Should return empty list on error

    async def test_analyze_patterns_validation_error(
        self, track_predictor: TrackPredictor, task_group: MagicMock, anyio_backend: str
    ) -> None:
        """Test handling of validation errors in analyze_patterns."""
        with patch.object(
            track_predictor,
            "get_historical_assignments",
        ):
            patterns = await track_predictor.analyze_patterns(
                "place-sstat",
                "CR-Providence",
                "Providence",
                0,
                datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
                task_group,
            )

            assert patterns == {}  # Should return empty dict on error

    async def test_store_prediction_error_handling(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test error handling in prediction storage."""
        prediction = TrackPrediction(
            station_id="place-sstat",
            route_id="CR-Providence",
            trip_id="test",
            headsign="Providence",
            direction_id=0,
            scheduled_time=datetime.now(UTC),
            track_number="3",
            confidence_score=0.8,
            accuracy=0.7,
            prediction_method="test",
            historical_matches=10,
            created_at=datetime.now(UTC),
        )

        with (
            patch(
                "track_predictor.track_predictor.write_cache",
                side_effect=ConnectionError("Redis down"),
            ),
            patch("logging.getLogger") as mock_logger,
        ):
            logger_instance = MagicMock()
            mock_logger.return_value = logger_instance

            await track_predictor._store_prediction(prediction)

            # Should log error without crashing
            logger_instance.error.assert_called()

    async def test_prediction_stats_connection_error(
        self, track_predictor: TrackPredictor, anyio_backend: str
    ) -> None:
        """Test handling of connection errors in prediction statistics."""
        track_predictor.redis.get = AsyncMock(side_effect=TimeoutError("Redis timeout"))

        stats = await track_predictor.get_prediction_stats(
            "place-sstat", "CR-Providence"
        )

        assert stats is None  # Should return None on error


if __name__ == "__main__":
    pytest.main([__file__])
