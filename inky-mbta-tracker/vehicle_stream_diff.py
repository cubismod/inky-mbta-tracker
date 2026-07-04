import logging

from anyio import sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from config import Config
from deepdiff import DeepDiff
from geojson import Feature
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_transaction_ids_to_span
from redis.asyncio import Redis
from shared_types.shared_types import DiffApiResponse

SLEEP_DURATION = 2

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class VehicleStreamDiff:
    """
    Implements a differ for use with SSE real-time vehicle updates.
    """

    r_client: Redis
    send_stream: MemoryObjectSendStream[DiffApiResponse]
    config: Config
    tg: TaskGroup
    current_snapshot: dict[str, Feature] = {}
    empty_count = 0

    def __init__(
        self,
        r_client: Redis,
        send_stream: MemoryObjectSendStream[DiffApiResponse],
        config: Config,
        tg: TaskGroup,
    ) -> None:
        self.r_client = r_client
        self.send_stream = send_stream
        self.config = config
        self.tg = tg

    def _save_snapshot(self, features: dict[str, Feature]) -> None:
        self.current_snapshot = features

    async def get_reset(self, frequent_buses: bool = False) -> DiffApiResponse:
        features = await get_vehicle_features(
            self.r_client, self.config, self.tg, frequent_buses
        )
        return DiffApiResponse(updated=features, removed=set())

    @staticmethod
    def _calculate_diff(
        original: dict[str, Feature], new: dict[str, Feature]
    ) -> DiffApiResponse:
        with tracer.start_as_current_span(
            "api.services.calculate_vehicle_diff"
        ) as span:
            span.set_attribute("vehicles.original_count", len(original))
            span.set_attribute("vehicles.new_count", len(new))

            # Add transaction IDs to the span
            add_transaction_ids_to_span(span)

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
            logger.debug("DeepDiff keys: %s", list(diff_response.keys()))
            logger.debug(
                "Original vehicles: %s, New vehicles: %s", len(original), len(new)
            )

            # Find vehicles that have been added or changed
            updated: dict[str, Feature] = {}

            # Handle new vehicles (added)
            if "dictionary_item_added" in diff_response:
                logger.debug(
                    "Added vehicles: %s", len(diff_response["dictionary_item_added"])
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
                    "Changed vehicle fields: %s", len(diff_response["values_changed"])
                )
                for key_path in diff_response["values_changed"]:
                    # Extract the vehicle ID from nested key paths
                    if "root['" in key_path and "']" in key_path:
                        vehicle_id = key_path.split("'")[1]
                        vehicle_ids_changed.add(vehicle_id)

            # Handle type changes (e.g., stop_eta changing from None to string or vice versa)
            if "type_changes" in diff_response:
                logger.debug(
                    "Type changed vehicle fields: %s",
                    len(diff_response["type_changes"]),
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
                    "Removed vehicles: %s",
                    len(diff_response["dictionary_item_removed"]),
                )
                for key_path in diff_response["dictionary_item_removed"]:
                    # Extract the vehicle ID from the key path
                    if "root['" in key_path and "']" in key_path:
                        vehicle_id = key_path.split("'")[1]
                        removed.add(vehicle_id)

            span.set_attribute("diff.updated_count", len(updated))
            span.set_attribute("diff.removed_count", len(removed))

            logger.debug(
                "Returning %s updated, %s removed vehicles", len(updated), len(removed)
            )
            return DiffApiResponse(updated=updated, removed=removed)

    async def watch(self, frequent_buses: bool = False) -> None:
        while True:
            features = await get_vehicle_features(
                self.r_client, self.config, self.tg, frequent_buses
            )
            if len(features) == 0:
                if self.empty_count > 3:
                    await self.send_stream.send(
                        DiffApiResponse(updated={}, removed=set())
                    )
                    # note that this is intentionally added to the sleep at the end of the method
                    await sleep(5)
                self.empty_count += 1
            else:
                self.empty_count = 0
                await self.send_stream.send(
                    self._calculate_diff(self.current_snapshot, features)
                )
            await sleep(SLEEP_DURATION)
            self._save_snapshot(features)
