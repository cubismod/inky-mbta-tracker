#!/usr/bin/env python3
"""
Healthcheck script for Docker container health monitoring.

This script checks Redis connectivity and verifies that the main process
is writing regular heartbeats. Exit code 0 indicates healthy, non-zero
indicates unhealthy.
"""

import json
import logging
import os
import sys
from datetime import UTC, datetime, time
from urllib.error import URLError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

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
VEHICLES_ENDPOINT_TIMEOUT_SECONDS = 5


def is_service_hours(now: datetime | None = None) -> bool:
    ny_tz = ZoneInfo("America/New_York")
    current = now.astimezone(ny_tz) if now else datetime.now(ny_tz)
    weekday = current.isoweekday()
    start = time(5, 0)
    end = time(0, 30)
    if weekday in (5, 6):
        end = time(2, 0)
    in_day_window = start <= current.time() <= time(23, 59, 59)
    in_overnight_window = current.time() <= end
    return in_day_window or in_overnight_window


def _extract_vehicle_count(payload: object) -> int | None:
    if not isinstance(payload, dict):
        return None

    # FeatureCollection may use a list (GeoJSON typical) or a dict mapping id->feature
    features = payload.get("features")
    if isinstance(features, list):
        return len(features)
    if isinstance(features, dict):
        return len(features)

    # Some payloads may use a top-level 'data' array or mapping
    data = payload.get("data")
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return len(data)

    return None


def check_vehicles_endpoint(url: str, now: datetime | None = None) -> bool:
    if not is_service_hours(now):
        logger.info("Skipping vehicles check outside service hours")
        return True

    try:
        with urlopen(url, timeout=VEHICLES_ENDPOINT_TIMEOUT_SECONDS) as response:
            raw = response.read()
    except URLError as e:
        logger.error(f"Vehicles endpoint request failed: {e}")
        return False

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Vehicles endpoint response is not JSON: {e}")
        return False

    count = _extract_vehicle_count(payload)
    if count is None:
        logger.error("Vehicles endpoint response missing vehicles list")
        return False
    if count <= 2:
        logger.error("Vehicles endpoint returned nearly empty vehicle list")
        return False
    return True


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

    # Determine behavior from environment variables (no CLI parsing)
    def env_flag(name: str, default: bool = False) -> bool:
        v = os.environ.get(name)
        if v is None:
            return default
        return v.lower() in ("1", "true", "yes", "on")

    check_vehicles = env_flag("IMT_HEALTHCHECK_CHECK_VEHICLES", False)

    is_healthy = check_health()
    if is_healthy and check_vehicles:
        vehicles_url = os.environ.get("IMT_VEHICLES_URL")
        if not vehicles_url:
            logger.error(
                "IMT_VEHICLES_URL is required when IMT_HEALTHCHECK_CHECK_VEHICLES is enabled"
            )
            is_healthy = False
        else:
            is_healthy = check_vehicles_endpoint(vehicles_url)
    if is_healthy:
        print("healthy")
        return 0
    else:
        print("unhealthy")
        return 1


if __name__ == "__main__":
    sys.exit(main())
