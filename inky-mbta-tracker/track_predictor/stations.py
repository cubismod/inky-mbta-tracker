"""Station management for track prediction system.

This module handles station ID normalization, child station mappings,
and determining which stations support track predictions.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Set

from anyio import open_file
from mbta_client import determine_station_id

logger = logging.getLogger(__name__)


class StationManager:
    """Manages station ID normalization and track prediction support."""

    def __init__(self) -> None:
        self._child_stations_map: Dict[str, str] = {}
        self._supported_stations: Set[str] = set()

    async def load_child_stations_map(self) -> Dict[str, str]:
        """Load the child stations JSON file and create a mapping from child -> parent."""
        try:
            child_stations_path = Path() / "child_stations.json"
            async with await open_file(child_stations_path) as f:
                content = await f.read()
                data = json.loads(content)

            # Create reverse mapping: child_id -> parent_station_id
            mapping = {}
            for parent_station, children in data.items():
                for child in children:
                    mapping[child["id"]] = parent_station
            return mapping
        except (json.JSONDecodeError, FileNotFoundError, PermissionError) as e:
            logger.warning("Failed to load child stations mapping", exc_info=e)
            return {}

    async def initialize(self) -> None:
        """Initialize the StationManager by loading child stations mapping."""
        self._child_stations_map = await self.load_child_stations_map()
        self._supported_stations = (
            set(self._child_stations_map.values())
            if self._child_stations_map
            else set()
        )

    def normalize_station(self, station_id: str) -> str:
        """
        Normalize station/stop identifiers to canonical 'place-...' station ids
        using the child_stations.json lookup table as the source of truth.
        """
        if not station_id:
            return station_id

        # First check if this ID is directly in our child stations mapping
        if station_id in self._child_stations_map:
            return self._child_stations_map[station_id]

        # If already a place-* ID, return as-is
        if station_id.startswith("place-"):
            return station_id

        try:
            resolved, has = determine_station_id(station_id)
            if resolved and has:
                return resolved
        except (TypeError, ValueError):
            # determine_station_id is simple but protect against unexpected input types
            pass

        return station_id

    def supports_track_predictions(self, station_id: str) -> bool:
        """
        Check if a station supports track predictions based on our child_stations.json mapping.
        Returns True if the normalized station ID is in our supported stations list.
        """
        normalized_station = self.normalize_station(station_id)
        return normalized_station in self._supported_stations

    @property
    def supported_stations(self) -> Set[str]:
        """Get the set of supported station IDs."""
        return self._supported_stations.copy()

    @property
    def child_stations_map(self) -> Dict[str, str]:
        """Get the child stations mapping."""
        return self._child_stations_map.copy()


# Convenience functions for backward compatibility
async def load_child_stations_map() -> Dict[str, str]:
    """Load the child stations JSON file and create a mapping from child -> parent."""
    manager = StationManager()
    return await manager.load_child_stations_map()


def normalize_station_id(station_id: str, child_stations_map: Dict[str, str]) -> str:
    """Normalize a station ID using the provided mapping."""
    if not station_id:
        return station_id

    # First check if this ID is directly in our child stations mapping
    if station_id in child_stations_map:
        return child_stations_map[station_id]

    # If already a place-* ID, return as-is
    if station_id.startswith("place-"):
        return station_id

    try:
        resolved, has = determine_station_id(station_id)
        if resolved and has:
            return resolved
    except (TypeError, ValueError):
        # determine_station_id is simple but protect against unexpected input types
        pass

    return station_id


def check_track_prediction_support(
    station_id: str, supported_stations: Set[str]
) -> bool:
    """Check if a station supports track predictions."""
    return station_id in supported_stations
