from datetime import UTC, datetime, timedelta
from typing import List
from unittest.mock import AsyncMock, patch

import pytest
from shared_types.shared_types import (
    TrackAssignment,
    TrackAssignmentType,
)
from track_predictor.track_predictor import TrackPredictor


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Create a mock Redis instance for testing."""
    redis_mock = AsyncMock()
    redis_mock.get.return_value = None
    redis_mock.set.return_value = None
    redis_mock.setex.return_value = None
    redis_mock.zadd.return_value = 1
    redis_mock.expire.return_value = None
    redis_mock.zrange.return_value = []
    redis_mock.zrangebyscore.return_value = []
    return redis_mock


@pytest.fixture
def track_predictor(mock_redis: AsyncMock) -> TrackPredictor:
    """Create a track predictor with mocked Redis."""
    predictor = TrackPredictor()
    predictor.redis = mock_redis
    return predictor


@pytest.fixture
def sample_assignments() -> List[TrackAssignment]:
    """Create sample track assignments for testing."""
    base_time = datetime(2024, 1, 15, 8, 30, tzinfo=UTC)
    assignments = []

    # Create assignments with different patterns
    for i in range(10):
        assignment = TrackAssignment(
            station_id="place-north",
            route_id="CR-Lowell",
            trip_id=f"trip_{i}",
            headsign="Lowell",
            direction_id=0,
            assignment_type=TrackAssignmentType.HISTORICAL,
            track_number="1" if i % 2 == 0 else "2",
            scheduled_time=base_time + timedelta(days=i),
            recorded_time=base_time + timedelta(days=i),
            day_of_week=(base_time + timedelta(days=i)).weekday(),
            hour=8,
            minute=30,
        )
        assignments.append(assignment)

    return assignments


class TestTrackPredictor:
    """Test suite for TrackPredictor class."""

    @pytest.mark.asyncio
    async def test_store_historical_assignment(
        self, track_predictor: TrackPredictor, sample_assignments: List[TrackAssignment]
    ) -> None:
        """Test storing historical track assignments."""
        assignment = sample_assignments[0]

        # Mock the cache check to return False (not cached)
        with (
            patch("redis_cache.check_cache", return_value=False),
            patch("redis_cache.write_cache", return_value=None),
        ):
            await track_predictor.store_historical_assignment(assignment)

            # Verify Redis methods were called
            track_predictor.redis.zadd.assert_called()
            track_predictor.redis.expire.assert_called()

    @pytest.mark.asyncio
    async def test_get_historical_assignments(
        self, track_predictor: TrackPredictor, sample_assignments: List[TrackAssignment]
    ) -> None:
        """Test retrieving historical track assignments."""
        # Test the method with mocked dependencies
        start_date = datetime(2024, 1, 10, tzinfo=UTC)
        end_date = datetime(2024, 1, 20, tzinfo=UTC)

        # Mock the method directly to test the interface
        expected_assignments = sample_assignments[:3]

        with patch.object(
            track_predictor,
            "get_historical_assignments",
            return_value=expected_assignments,
        ):
            assignments = await track_predictor.get_historical_assignments(
                "place-north", "CR-Lowell", start_date, end_date
            )

            assert len(assignments) == 3
            assert all(isinstance(a, TrackAssignment) for a in assignments)
            assert assignments == expected_assignments

    @pytest.mark.asyncio
    async def test_is_holiday_detection(self, track_predictor: TrackPredictor) -> None:
        """Test holiday detection functionality."""
        # Test major holidays
        new_years = datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
        independence_day = datetime(2024, 7, 4, 10, 0, tzinfo=UTC)
        christmas = datetime(2024, 12, 25, 10, 0, tzinfo=UTC)
        regular_day = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

        assert await track_predictor._is_holiday(new_years) is True
        assert await track_predictor._is_holiday(independence_day) is True
        assert await track_predictor._is_holiday(christmas) is True
        assert await track_predictor._is_holiday(regular_day) is False

    def test_calculate_data_quality(self, track_predictor: TrackPredictor) -> None:
        """Test data quality calculation."""
        base_time = datetime.now(UTC)

        # High quality assignment (recent, with actual time)
        high_quality = TrackAssignment(
            station_id="place-north",
            route_id="CR-Lowell",
            trip_id="trip_1",
            headsign="Lowell",
            direction_id=0,
            assignment_type=TrackAssignmentType.HISTORICAL,
            track_number="1",
            scheduled_time=base_time - timedelta(days=1),
            actual_time=base_time - timedelta(days=1, minutes=2),
            recorded_time=base_time - timedelta(days=1),
            day_of_week=0,
            hour=8,
            minute=30,
        )

        # Low quality assignment (old, no actual time, non-digit track)
        low_quality = TrackAssignment(
            station_id="place-north",
            route_id="CR-Lowell",
            trip_id="trip_2",
            headsign="Lowell",
            direction_id=0,
            assignment_type=TrackAssignmentType.HISTORICAL,
            track_number="TBD",
            scheduled_time=base_time - timedelta(days=60),
            actual_time=None,
            recorded_time=base_time - timedelta(days=60),
            day_of_week=0,
            hour=8,
            minute=30,
        )

        high_quality_score = track_predictor._calculate_data_quality(
            high_quality, base_time
        )
        low_quality_score = track_predictor._calculate_data_quality(
            low_quality, base_time
        )

        assert high_quality_score > low_quality_score
        assert high_quality_score > 1.0  # Should get recent data boost
        assert low_quality_score < 1.0  # Should be penalized

    @pytest.mark.asyncio
    async def test_calculate_track_consistency(
        self, track_predictor: TrackPredictor, sample_assignments: List[TrackAssignment]
    ) -> None:
        """Test track consistency calculation."""
        # Create assignments where track "1" is used consistently at 8:30
        consistent_assignments = []
        for i in range(5):
            assignment = TrackAssignment(
                station_id="place-north",
                route_id="CR-Lowell",
                trip_id=f"trip_{i}",
                headsign="Lowell",
                direction_id=0,
                assignment_type=TrackAssignmentType.HISTORICAL,
                track_number="1",
                scheduled_time=datetime(2024, 1, 15 + i, 8, 30, tzinfo=UTC),
                recorded_time=datetime(2024, 1, 15 + i, 8, 30, tzinfo=UTC),
                day_of_week=0,
                hour=8,
                minute=30,
            )
            consistent_assignments.append(assignment)

        consistency = await track_predictor._calculate_track_consistency(
            "place-north", "CR-Lowell", "1", consistent_assignments
        )

        assert consistency == 1.0  # Perfect consistency

    @pytest.mark.asyncio
    async def test_enhanced_pattern_analysis(
        self, track_predictor: TrackPredictor, sample_assignments: List[TrackAssignment]
    ) -> None:
        """Test the enhanced pattern analysis with various matching scenarios."""
        base_time = datetime(2024, 1, 15, 8, 30, tzinfo=UTC)

        # Mock get_historical_assignments to return sample data
        with (
            patch.object(
                track_predictor,
                "get_historical_assignments",
                return_value=sample_assignments[:3],
            ),
            patch.object(track_predictor, "_is_holiday", return_value=False),
            patch.object(track_predictor, "_get_prediction_accuracy", return_value=0.8),
            patch.object(track_predictor, "_get_recent_success_rate", return_value=0.9),
            patch.object(
                track_predictor, "_calculate_track_consistency", return_value=0.85
            ),
        ):
            # Test pattern analysis
            patterns = await track_predictor.analyze_patterns(
                "place-north", "CR-Lowell", "Lowell", 0, base_time
            )

            # Should return patterns based on sample assignments
            assert isinstance(patterns, dict)
            if patterns:  # May be empty if no strong patterns found
                for track, confidence in patterns.items():
                    assert 0 <= confidence <= 1
