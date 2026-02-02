import pytest
import pytest_asyncio
from api.services.connection_tracker import ConnectionTracker
from redis.asyncio import Redis


class MockRedis:
    """Mock Redis client with stateful SET operations."""

    def __init__(self) -> None:
        self.data: dict[str, set[str]] = {}

    async def scard(self, key: str) -> int:
        """Return the cardinality (size) of the set."""
        return len(self.data.get(key, set()))

    async def sadd(self, key: str, *values: str) -> int:
        """Add members to a set."""
        if key not in self.data:
            self.data[key] = set()
        before = len(self.data[key])
        self.data[key].update(values)
        after = len(self.data[key])
        return after - before

    async def srem(self, key: str, *values: str) -> int:
        """Remove members from a set."""
        if key not in self.data:
            return 0
        before = len(self.data[key])
        self.data[key].difference_update(values)
        after = len(self.data[key])
        return before - after

    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key (no-op in mock)."""
        return True

    async def delete(self, *keys: str) -> int:
        """Delete keys."""
        count = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                count += 1
        return count


@pytest_asyncio.fixture
async def redis_client() -> Redis:  # type: ignore[type-arg]
    """Create a mock Redis client for testing."""
    return MockRedis()  # type: ignore[return-value]


@pytest_asyncio.fixture
async def connection_tracker(redis_client: Redis) -> ConnectionTracker:  # type: ignore[type-arg]
    """Create a ConnectionTracker instance for testing."""
    return ConnectionTracker(redis_client, max_connections=3)


class TestConnectionTracker:
    """Test suite for ConnectionTracker service."""

    @pytest.mark.asyncio
    async def test_can_connect_when_under_limit(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that connections are allowed when under the limit."""
        ip = "192.168.1.1"
        assert await connection_tracker.can_connect(ip) is True

    @pytest.mark.asyncio
    async def test_add_connection_success(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test adding a connection successfully."""
        ip = "192.168.1.2"
        connection_id = await connection_tracker.add_connection(ip)
        assert connection_id is not None
        assert len(connection_id) > 0

    @pytest.mark.asyncio
    async def test_add_multiple_connections(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test adding multiple connections for the same IP."""
        ip = "192.168.1.3"

        # Add 3 connections (the max)
        conn_ids = []
        for _ in range(3):
            conn_id = await connection_tracker.add_connection(ip)
            assert conn_id is not None
            conn_ids.append(conn_id)

        # All connection IDs should be unique
        assert len(set(conn_ids)) == 3

        # Fourth connection should be rejected
        assert await connection_tracker.can_connect(ip) is False
        fourth_conn = await connection_tracker.add_connection(ip)
        assert fourth_conn is None

    @pytest.mark.asyncio
    async def test_remove_connection(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test removing a connection."""
        ip = "192.168.1.4"

        # Add a connection
        conn_id = await connection_tracker.add_connection(ip)
        assert conn_id is not None

        # Verify connection count
        count = await connection_tracker.get_connection_count(ip)
        assert count == 1

        # Remove the connection
        await connection_tracker.remove_connection(ip, conn_id)

        # Verify connection was removed
        count = await connection_tracker.get_connection_count(ip)
        assert count == 0

    @pytest.mark.asyncio
    async def test_connection_limit_enforcement(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that connection limit is properly enforced."""
        ip = "192.168.1.5"

        # Add connections up to the limit
        for i in range(3):
            conn_id = await connection_tracker.add_connection(ip)
            assert conn_id is not None, f"Failed to add connection {i + 1}"

        # Verify we're at the limit
        count = await connection_tracker.get_connection_count(ip)
        assert count == 3

        # Try to add one more (should fail)
        conn_id = await connection_tracker.add_connection(ip)
        assert conn_id is None

    @pytest.mark.asyncio
    async def test_remove_and_add_again(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that removing a connection frees up a slot."""
        ip = "192.168.1.6"

        # Fill up to limit
        conn_ids = []
        for _ in range(3):
            conn_id = await connection_tracker.add_connection(ip)
            conn_ids.append(conn_id)

        # Remove one connection
        await connection_tracker.remove_connection(ip, conn_ids[0])

        # Now we should be able to add one more
        new_conn_id = await connection_tracker.add_connection(ip)
        assert new_conn_id is not None

    @pytest.mark.asyncio
    async def test_different_ips_independent(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that different IPs have independent connection limits."""
        ip1 = "192.168.1.7"
        ip2 = "192.168.1.8"

        # Add 3 connections for ip1
        for _ in range(3):
            conn_id = await connection_tracker.add_connection(ip1)
            assert conn_id is not None

        # ip2 should still be able to connect
        assert await connection_tracker.can_connect(ip2) is True
        conn_id = await connection_tracker.add_connection(ip2)
        assert conn_id is not None

    @pytest.mark.asyncio
    async def test_get_connection_count_zero(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test getting connection count for IP with no connections."""
        ip = "192.168.1.9"
        count = await connection_tracker.get_connection_count(ip)
        assert count == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent_connection(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that removing a non-existent connection doesn't error."""
        ip = "192.168.1.10"
        fake_conn_id = "nonexistent-id"

        # Should not raise an error
        await connection_tracker.remove_connection(ip, fake_conn_id)

        # Count should still be zero
        count = await connection_tracker.get_connection_count(ip)
        assert count == 0

    @pytest.mark.asyncio
    async def test_custom_max_connections(
        self,
        redis_client: Redis,  # type: ignore[type-arg]
    ) -> None:
        """Test using a custom max_connections value."""
        tracker = ConnectionTracker(redis_client, max_connections=5)
        ip = "192.168.1.11"

        # Should be able to add 5 connections
        for i in range(5):
            conn_id = await tracker.add_connection(ip)
            assert conn_id is not None, f"Failed to add connection {i + 1}"

        # Sixth should fail
        conn_id = await tracker.add_connection(ip)
        assert conn_id is None

    @pytest.mark.asyncio
    async def test_connection_key_format(
        self, connection_tracker: ConnectionTracker
    ) -> None:
        """Test that connection keys are properly formatted."""
        ip = "192.168.1.12"
        expected_key = f"live_schedules:connections:{ip}"

        # The internal method should produce the correct key
        actual_key = connection_tracker._get_key(ip)
        assert actual_key == expected_key
