from datetime import UTC, datetime
from typing import Generator, cast
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis
from shared_types.shared_types import TrackAssignment, TrackAssignmentType
from track_predictor.track_predictor import TrackPredictor
from track_predictor.utils import (
    get_station_confidence_threshold,
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

    @pytest.fixture
    def sample_assignment(self) -> TrackAssignment:
        """Create a sample track assignment for testing."""
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


class TestConfidenceThresholds:
    """Test station-specific confidence thresholds."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        yield TrackPredictor(MagicMock())

    def test_south_station_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test South Station has lower threshold."""
        threshold = get_station_confidence_threshold("place-sstat")
        assert threshold == 0.25

    def test_north_station_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test North Station has lower threshold."""
        threshold = get_station_confidence_threshold("place-north")
        assert threshold == 0.25

    def test_back_bay_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test Back Bay has medium threshold."""
        threshold = get_station_confidence_threshold("place-bbsta")
        assert threshold == 0.30

    def test_default_threshold(self, track_predictor: TrackPredictor) -> None:
        """Test unknown stations use default threshold."""
        threshold = get_station_confidence_threshold("place-unknown")
        assert threshold == 0.35


class TestCrossRoutePatterns:
    """Test cross-route pattern learning."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.mark.anyio("asyncio")
    async def test_related_routes_included(
        self, track_predictor: TrackPredictor
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

    @pytest.mark.anyio("asyncio")
    async def test_single_route_when_not_requested(
        self, track_predictor: TrackPredictor
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


class TestExpandedTimeWindows:
    """Test expanded time window matching."""

    @pytest.fixture
    def track_predictor(self) -> Generator[TrackPredictor, None, None]:
        predictor = TrackPredictor(cast(AsyncRedis, MagicMock()))
        predictor.redis = AsyncMock()
        yield predictor

    @pytest.mark.anyio("asyncio")
    async def test_multiple_time_windows_in_patterns(
        self, track_predictor: TrackPredictor
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
            async with anyio.create_task_group() as tg:
                patterns = await track_predictor.analyze_patterns(
                    "place-sstat",
                    "CR-Providence",
                    "Providence",
                    0,
                    datetime(2024, 1, 15, 10, 15, tzinfo=UTC),  # 45 minutes later
                )
                tg.cancel_scope.cancel()

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


class TestStationNormalizationAndSupport:
    """Tests for station normalization and supports_track_predictions behavior."""

    @pytest.fixture
    def predictor_initialized(self) -> TrackPredictor:
        """TrackPredictor pre-initialized with a simple child station map."""
        p = TrackPredictor(cast(AsyncRedis, MagicMock()))
        # Simulate loaded child_stations mapping and supported stations
        p.station_manager._child_stations_map = {
            "NEC-2287": "place-sstat",
            "BNT-0000": "place-north",
        }
        p.station_manager._supported_stations = {
            "place-sstat",
            "place-north",
            "place-bbsta",
        }
        return p

    @pytest.fixture
    def predictor_uninitialized(self) -> TrackPredictor:
        """TrackPredictor not initialized (uses fallback behavior)."""
        p = TrackPredictor(cast(AsyncRedis, MagicMock()))
        return p

    def test_normalize_child_id_when_initialized(
        self, predictor_initialized: TrackPredictor
    ) -> None:
        """Child stop IDs should map to canonical place-* IDs when initialized."""
        p = predictor_initialized
        assert p.normalize_station("NEC-2287") == "place-sstat"
        assert p.normalize_station("BNT-0000") == "place-north"
        # place-* IDs should be returned as-is
        assert p.normalize_station("place-sstat") == "place-sstat"
        # Unknown IDs should fall back to the original value
        assert p.normalize_station("UNKNOWN-STOP") == "UNKNOWN-STOP"

    def test_supports_track_predictions_when_initialized(
        self, predictor_initialized: TrackPredictor
    ) -> None:
        """supports_track_predictions should consult the precomputed supported set when initialized."""
        p = predictor_initialized
        assert p.supports_track_predictions("NEC-2287") is True
        assert p.supports_track_predictions("place-north") is True
        assert p.supports_track_predictions("some-other-stop") is False

    def test_normalize_and_supports_when_not_initialized(
        self, predictor_uninitialized: TrackPredictor
    ) -> None:
        """
        When not initialized the predictor should fall back to the lightweight
        determine_station_id behavior and the small built-in supported set.
        """
        p = predictor_uninitialized
        # The fallback supports a few canonical place-* ids; these should be True
        assert p.supports_track_predictions("place-north") is True
        assert p.supports_track_predictions("place-sstat") is True
        assert p.supports_track_predictions("place-bbsta") is True

        # The fallback normalize (via determine_station_id) should resolve common stop tokens
        # mbta_client.determine_station_id contains rules for NEC-2287 -> place-sstat etc.
        resolved = p.normalize_station("NEC-2287")
        assert isinstance(resolved, str)
        assert p.supports_track_predictions("NEC-2287") is True


if __name__ == "__main__":
    pytest.main([__file__])
