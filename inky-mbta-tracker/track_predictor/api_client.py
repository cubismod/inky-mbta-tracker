"""MBTA API client for track prediction system.

This module handles fetching departure data from the MBTA API,
including retry logic and error handling.
"""

import logging
from typing import List

logger = logging.getLogger(__name__)


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
