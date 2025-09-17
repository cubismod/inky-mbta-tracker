"""MBTA API client for track prediction system.

This module handles fetching departure data from the MBTA API,
including retry logic and error handling.
"""

import logging
import os
import random
from asyncio import CancelledError
from datetime import UTC, datetime
from typing import Any, List, Optional, TypedDict

import aiohttp
import anyio
from consts import MBTA_V3_ENDPOINT
from exceptions import RateLimitExceeded
from mbta_responses import Schedules
from prometheus import mbta_api_requests
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class DepartureInfo(TypedDict):
    """Typed representation of an upcoming departure returned from schedules."""

    trip_id: str
    station_id: str
    route_id: str
    direction_id: int
    departure_time: str


async def fetch_upcoming_departures(
    session: Any,
    route_id: str,
    station_ids: List[str],
    target_date: Optional[datetime] = None,
) -> List[DepartureInfo]:
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
    # Local retry/backoff implementation (replaces tenacity decorator).
    # This will attempt the schedule fetch a few times with exponential
    # backoff + jitter before giving up, while still propagating CancelledError.
    max_attempts = int(os.getenv("IMT_PRECACHE_MAX_ATTEMPTS", "5"))
    base_backoff = float(os.getenv("IMT_PRECACHE_BASE_BACKOFF", "1.0"))
    attempt = 0

    while True:
        try:
            if target_date is None:
                target_date = datetime.now(UTC)
            # Proceed with the normal fetch logic below on success
            break
        except CancelledError:
            # Preserve cancellation semantics
            raise
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error(
                    f"Exceeded max attempts ({max_attempts}) in fetch_upcoming_departures for route {route_id}",
                    exc_info=e,
                )
                return []
            # Exponential backoff with small random jitter
            backoff = min(60.0, base_backoff * (2 ** (attempt - 1)))
            jitter = random.random() * 0.5
            sleep_for = backoff + jitter
            logger.debug(
                f"fetch_upcoming_departures attempt {attempt}/{max_attempts} failed for route {route_id}; retrying in {sleep_for:.2f}s",
                exc_info=e,
            )
            await anyio.sleep(sleep_for)
            # loop and retry

    date_str = target_date.date().isoformat()
    auth_token = os.environ.get("AUTH_TOKEN", "")
    upcoming_departures: list[DepartureInfo] = []

    # Fetch schedules for all stations in a single API call
    stations_str = ",".join(station_ids)

    endpoint = f"{MBTA_V3_ENDPOINT}/schedules?filter[stop]={stations_str}&filter[route]={route_id}&filter[date]={date_str}&sort=departure_time&include=trip&api_key={auth_token}"

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
                    f"Unable to parse schedules for {stations_str} on {route_id}",
                    exc_info=e,
                )
                return []

            # Process each scheduled departure
            for schedule in schedules_data.data:
                if not schedule.attributes.departure_time:
                    continue

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

                departure_info: DepartureInfo = {
                    "trip_id": trip_id,
                    "station_id": station_id,
                    "route_id": route_id,
                    "direction_id": schedule.attributes.direction_id,
                    "departure_time": schedule.attributes.departure_time,
                }

                upcoming_departures.append(departure_info)

    except (aiohttp.ClientError, TimeoutError) as e:
        logger.error(
            f"Error fetching schedules for {stations_str} on {route_id}",
            exc_info=e,
        )
        return []

    logger.debug(
        f"Found {len(upcoming_departures)} upcoming departures for route {route_id} across {len(station_ids)} stations"
    )
    return upcoming_departures


def get_default_routes() -> List[str]:
    """Get the default list of commuter rail routes for precaching."""
    return [
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


def get_default_target_stations() -> List[str]:
    """Get the default list of stations for precaching."""
    return [
        "place-sstat",  # South Station
        "place-north",  # North Station
        "place-bbsta",  # Back Bay
        "place-rugg",  # Ruggles
    ]
