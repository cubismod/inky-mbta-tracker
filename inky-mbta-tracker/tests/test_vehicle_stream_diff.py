from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from geojson import Feature, Point
from vehicle_stream_diff import VehicleStreamDiff, run_vehicle_stream_diff


def _make_feature(vehicle_id: str, lat: float = 0.0, lon: float = 0.0) -> Feature:
    return Feature(geometry=Point((lon, lat)), properties={"id": vehicle_id})


def _make_features(*ids: str) -> dict[str, Feature]:
    return {fid: _make_feature(fid) for fid in ids}


class TestCalculateDiff:
    def test_empty_original_returns_all_as_updated(self) -> None:
        new = _make_features("v1", "v2")
        result = VehicleStreamDiff._calculate_diff({}, new)

        assert result.updated == new
        assert result.removed == set()

    def test_no_changes_returns_empty_diff(self) -> None:
        original = _make_features("v1", "v2")
        result = VehicleStreamDiff._calculate_diff(original, original)

        assert result.updated == {}
        assert result.removed == set()

    def test_added_vehicles_returned_in_updated(self) -> None:
        original = _make_features("v1")
        new = _make_features("v1", "v2", "v3")
        result = VehicleStreamDiff._calculate_diff(original, new)

        assert set(result.updated.keys()) == {"v2", "v3"}
        assert result.removed == set()

    def test_removed_vehicles_returned_in_removed(self) -> None:
        original = _make_features("v1", "v2", "v3")
        new = _make_features("v1")
        result = VehicleStreamDiff._calculate_diff(original, new)

        assert result.updated == {}
        assert result.removed == {"v2", "v3"}

    def test_both_empty_returns_empty(self) -> None:
        result = VehicleStreamDiff._calculate_diff({}, {})

        assert result.updated == {}
        assert result.removed == set()

    def test_new_empty_no_removals_detected(self) -> None:
        original = _make_features("v1", "v2")
        result = VehicleStreamDiff._calculate_diff(original, {})

        assert result.updated == {}
        assert result.removed == {"v1", "v2"}

    def test_add_and_remove_in_same_diff(self) -> None:
        original = _make_features("v1", "v2")
        new = _make_features("v1", "v3")
        result = VehicleStreamDiff._calculate_diff(original, new)

        assert set(result.updated.keys()) == {"v3"}
        assert result.removed == {"v2"}


class TestSaveSnapshot:
    def test_saves_snapshot(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        features = _make_features("v1")
        vsd._save_snapshot(features)
        assert vsd.current_snapshot == features

    def test_overwrites_previous_snapshot(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        vsd._save_snapshot(_make_features("v1"))
        vsd._save_snapshot(_make_features("v2"))
        assert vsd.current_snapshot == _make_features("v2")


class TestWatch:
    @pytest.mark.anyio
    async def test_publishes_added_vehicles(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        features_a = _make_features("v1")
        features_b = _make_features("v1", "v2")

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return features_a
            if call_count == 2:
                return features_b
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch()

        publish_calls = r_client.publish.call_args_list
        assert len(publish_calls) >= 1
        assert publish_calls[0][0][0] == "vehicle_stream_diff:rapid"

    @pytest.mark.anyio
    async def test_publishes_to_buses_key_when_frequent(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_features("v1")
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch(frequent_buses=True)

        assert r_client.publish.call_args[0][0] == "vehicle_stream_diff:buses"

    @pytest.mark.anyio
    async def test_publishes_empty_after_sustained_empty(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                return {}
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch()

        publish_calls = r_client.publish.call_args_list
        assert len(publish_calls) == 1

    @pytest.mark.anyio
    async def test_handles_redis_error_gracefully(self) -> None:
        from redis.exceptions import RedisError

        r_client = AsyncMock()
        r_client.publish.side_effect = RedisError("connection lost")
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_features("v1")
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch()

    @pytest.mark.anyio
    async def test_saves_snapshot_after_each_iteration(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)

        features_a = _make_features("v1")
        features_b = _make_features("v1", "v2")

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return features_a
            if call_count == 2:
                return features_b
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch()

        assert vsd.current_snapshot == features_b

    @pytest.mark.anyio
    async def test_empty_count_resets_on_non_empty(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()
        vsd = VehicleStreamDiff(r_client, config, tg)
        vsd.empty_count = 5

        call_count = 0

        async def mock_get_vehicle_features(
            *_args: object, **_kwargs: object
        ) -> dict[str, Feature]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_features("v1")
            raise _StopIteration()

        with (
            patch(
                "vehicle_stream_diff.get_vehicle_features",
                side_effect=mock_get_vehicle_features,
            ),
            patch("vehicle_stream_diff.sleep", new_callable=AsyncMock),
            pytest.raises(_StopIteration),
        ):
            await vsd.watch()

        assert vsd.empty_count == 0


class TestRunVehicleStreamDiff:
    @pytest.mark.anyio
    async def test_creates_instance_and_calls_watch(self) -> None:
        r_client = AsyncMock()
        config = MagicMock()
        tg = MagicMock()

        with patch.object(
            VehicleStreamDiff, "watch", new_callable=AsyncMock
        ) as mock_watch:
            await run_vehicle_stream_diff(r_client, config, tg, frequent_buses=True)

            mock_watch.assert_awaited_once_with(True)


class _StopIteration(Exception):
    """Raised to break out of the infinite watch loop in tests."""
