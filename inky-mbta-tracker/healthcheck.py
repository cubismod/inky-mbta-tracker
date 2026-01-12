#!/usr/bin/env python3
"""
Healthcheck script for Docker container health monitoring.

This script checks Redis connectivity and verifies that the main process
is writing regular heartbeats. Exit code 0 indicates healthy, non-zero
indicates unhealthy.
"""

import logging
import os
import sys
from datetime import UTC, datetime

from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError

# Configure basic logging for healthcheck
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Redis key for heartbeat tracking
HEARTBEAT_KEY = "healthcheck:heartbeat"
# Maximum age of heartbeat in seconds before considering unhealthy
MAX_HEARTBEAT_AGE_SECONDS = 120


def check_health() -> bool:
    """
    Check application health via Redis heartbeat.

    Returns:
        True if healthy, False otherwise
    """
    try:
        # Connect to Redis using environment variables
        redis_client = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT", "localhost"),
            port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
            password=os.environ.get("IMT_REDIS_PASSWORD", ""),
            socket_connect_timeout=5,
            socket_timeout=5,
            decode_responses=True,
        )

        # Test Redis connectivity
        redis_client.ping()

        # Check heartbeat timestamp
        heartbeat_value = redis_client.get(HEARTBEAT_KEY)
        if not heartbeat_value:
            logger.error("No heartbeat found in Redis")
            return False

        # Ensure we have a string value (decode if needed)
        if isinstance(heartbeat_value, bytes):
            heartbeat_str = heartbeat_value.decode("utf-8")
        elif isinstance(heartbeat_value, str):
            heartbeat_str = heartbeat_value
        else:
            logger.error(f"Unexpected heartbeat value type: {type(heartbeat_value)}")
            return False

        # Parse timestamp and check age
        try:
            heartbeat_time = datetime.fromisoformat(heartbeat_str)
            now = datetime.now(UTC)
            age_seconds = (now - heartbeat_time).total_seconds()

            if age_seconds > MAX_HEARTBEAT_AGE_SECONDS:
                logger.error(
                    f"Heartbeat is too old: {age_seconds:.1f}s (max: {MAX_HEARTBEAT_AGE_SECONDS}s)"
                )
                return False

            logger.info(f"Heartbeat age: {age_seconds:.1f}s - healthy")
            return True

        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse heartbeat timestamp: {e}")
            return False

    except (ConnectionError, TimeoutError) as e:
        logger.error(f"Redis connection failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during healthcheck: {e}")
        return False


def main() -> int:
    """
    Main healthcheck entry point.

    Returns:
        0 if healthy, 1 if unhealthy
    """
    is_healthy = check_health()
    if is_healthy:
        print("healthy")
        return 0
    else:
        print("unhealthy")
        return 1


if __name__ == "__main__":
    sys.exit(main())
