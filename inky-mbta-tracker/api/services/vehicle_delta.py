# in order to provide more efficient updates to real-time vehicle data, deltas are used
# to cut down on data transmission
# deltas work by transmitting a response consisting of updated and
# removed vehicles which are then handled appropriately by the API client

import logging

from deepdiff import DeepDiff
from geojson import Feature
from opentelemetry import trace
from shared_types.shared_types import DiffApiResponse

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def calculate_diff(
    original: dict[str, Feature], new: dict[str, Feature]
) -> DiffApiResponse:
    with tracer.start_as_current_span("api.services.calculate_vehicle_diff") as span:
        span.set_attribute("vehicles.original_count", len(original))
        span.set_attribute("vehicles.new_count", len(new))

        # Handle case where original is empty (first load)
        if not original:
            span.set_attribute("diff.first_load", True)
            return DiffApiResponse(updated=new, removed=set())

        span.set_attribute("diff.first_load", False)

        # Configure DeepDiff with minimal exclusions to start with
        diff_response = DeepDiff(
            original,
            new,
            verbose_level=0,
        )

        # Debug logging to understand what's happening
        logger.debug(f"DeepDiff keys: {list(diff_response.keys())}")
        logger.debug(f"Original vehicles: {len(original)}, New vehicles: {len(new)}")

        # Find vehicles that have been added or changed
        updated: dict[str, Feature] = {}

        # Handle new vehicles (added)
        if "dictionary_item_added" in diff_response:
            logger.debug(
                f"Added vehicles: {len(diff_response['dictionary_item_added'])}"
            )
            for key_path in diff_response["dictionary_item_added"]:
                # Extract the vehicle ID from the key path (e.g., "root['vehicle_id']")
                if "root['" in key_path and "']" in key_path:
                    vehicle_id = key_path.split("'")[1]
                    updated[vehicle_id] = new[vehicle_id]

        # Handle modified vehicles (changed)
        vehicle_ids_changed = set()

        if "values_changed" in diff_response:
            logger.debug(
                f"Changed vehicle fields: {len(diff_response['values_changed'])}"
            )
            for key_path in diff_response["values_changed"]:
                # Extract the vehicle ID from nested key paths
                if "root['" in key_path and "']" in key_path:
                    vehicle_id = key_path.split("'")[1]
                    vehicle_ids_changed.add(vehicle_id)

        # Handle type changes (e.g., stop_eta changing from None to string or vice versa)
        if "type_changes" in diff_response:
            logger.debug(
                f"Type changed vehicle fields: {len(diff_response['type_changes'])}"
            )
            for key_path in diff_response["type_changes"]:
                # Extract the vehicle ID from nested key paths
                if "root['" in key_path and "']" in key_path:
                    vehicle_id = key_path.split("'")[1]
                    vehicle_ids_changed.add(vehicle_id)

        for vehicle_id in vehicle_ids_changed:
            if vehicle_id in new:
                updated[vehicle_id] = new[vehicle_id]

        # Handle removed vehicles
        removed: set[str] = set()
        if "dictionary_item_removed" in diff_response:
            logger.debug(
                f"Removed vehicles: {len(diff_response['dictionary_item_removed'])}"
            )
            for key_path in diff_response["dictionary_item_removed"]:
                # Extract the vehicle ID from the key path
                if "root['" in key_path and "']" in key_path:
                    vehicle_id = key_path.split("'")[1]
                    removed.add(vehicle_id)

        span.set_attribute("diff.updated_count", len(updated))
        span.set_attribute("diff.removed_count", len(removed))

        logger.debug(
            f"Returning {len(updated)} updated, {len(removed)} removed vehicles"
        )
        return DiffApiResponse(updated=updated, removed=removed)
