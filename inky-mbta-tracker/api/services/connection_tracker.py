import logging
import uuid
from typing import Optional

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class ConnectionTracker:
    """Tracks active SSE connections per IP address to enforce concurrent connection limits."""

    def __init__(self, redis_client: Redis, max_connections: int = 3):
        self.redis = redis_client
        self.max_connections = max_connections

    def _get_key(self, ip_address: str) -> str:
        return f"live_schedules:connections:{ip_address}"

    async def can_connect(self, ip_address: str) -> bool:
        """Check if the IP address can open a new connection."""
        try:
            key = self._get_key(ip_address)
            count = await self.redis.scard(key)  # type: ignore[misc]
            return count < self.max_connections
        except RedisError as e:
            logger.error(
                f"Error checking connection limit for {ip_address}", exc_info=e
            )
            return True

    async def add_connection(self, ip_address: str) -> Optional[str]:
        """
        Add a new connection for the IP address.

        Returns:
            Connection ID if successful, None if limit reached
        """
        try:
            if not await self.can_connect(ip_address):
                return None

            connection_id = str(uuid.uuid4())
            key = self._get_key(ip_address)
            await self.redis.sadd(key, connection_id)  # type: ignore[misc]
            await self.redis.expire(key, 3600)  # type: ignore[misc]
            logger.info(
                f"Added connection {connection_id} for {ip_address} "
                f"(current: {await self.redis.scard(key)})"  # type: ignore[misc]
            )
            return connection_id
        except RedisError as e:
            logger.error(f"Error adding connection for {ip_address}", exc_info=e)
            return str(uuid.uuid4())

    async def remove_connection(self, ip_address: str, connection_id: str) -> None:
        """Remove a connection for the IP address."""
        try:
            key = self._get_key(ip_address)
            await self.redis.srem(key, connection_id)  # type: ignore[misc]
            remaining = await self.redis.scard(key)  # type: ignore[misc]
            logger.info(
                f"Removed connection {connection_id} for {ip_address} "
                f"(remaining: {remaining})"
            )

            if remaining == 0:
                await self.redis.delete(key)  # type: ignore[misc]
        except RedisError as e:
            logger.error(
                f"Error removing connection {connection_id} for {ip_address}",
                exc_info=e,
            )

    async def get_connection_count(self, ip_address: str) -> int:
        """Get the current number of connections for an IP address."""
        try:
            key = self._get_key(ip_address)
            return await self.redis.scard(key)  # type: ignore[misc,return-value]
        except RedisError as e:
            logger.error(f"Error getting connection count for {ip_address}", exc_info=e)
            return 0
