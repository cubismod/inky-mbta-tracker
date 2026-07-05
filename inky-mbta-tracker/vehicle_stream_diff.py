import logging

from anyio import sleep
from anyio.abc import TaskGroup
from config import Config
from consts import VEHICLE_STREAM_KEY
from deepdiff import DeepDiff
from geojson import Feature
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_current_span_attributes, add_transaction_ids_to_span
from redis.asyncio import Redis
from redis.exceptions import RedisError
from shared_types.shared_types import DiffApiResponse

SLEEP_DURATION = 2

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class VehicleStreamDiff:
    """
    Implements a differ for use with SSE real-time vehicle updates.

    Uses Redis pub/sub to send diff updates to connected clients.
    """

    r_client: Redis
    config: Config
    tg: TaskGroup
    current_snapshot: dict[str, Feature]
    empty_count: int

    def __init__(
        self,
        r_client: Redis,
        config: Config,
        tg: TaskGroup,
    ) -> None:
        self.r_client = r_client
        self.config = config
        self.tg = tg
        self.current_snapshot = {}
        self.empty_count = 0

    def _save_snapshot(self, features: dict[str, Feature]) -> None:
        self.current_snapshot = features

    @staticmethod
    def _calculate_diff(
        original: dict[str, Feature], new: dict[str, Feature]
    ) -> DiffApiResponse:
        with tracer.start_as_current_span(
            "api.services.calculate_vehicle_diff"
        ) as span:
            span.set_attribute("vehicles.original_count", len(original))
            span.set_attribute("vehicles.new_count", len(new))

            add_transaction_ids_to_span(span)

            if not original:
                span.set_attribute("diff.first_load", True)
                return DiffApiResponse(updated=new, removed=set())

            if not new:
                span.set_attribute("diff.all_removed", True)
                return DiffApiResponse(updated={}, removed=set(original.keys()))

            span.set_attribute("diff.first_load", False)

            diff_response = DeepDiff(
                original,
                new,
                verbose_level=0,
            )

            logger.debug("DeepDiff keys: %s", list(diff_response.keys()))
            logger.debug(
                "Original vehicles: %s, New vehicles: %s", len(original), len(new)
            )

            updated: dict[str, Feature] = {}

            if "dictionary_item_added" in diff_response:
                logger.debug(
                    "Added vehicles: %s", len(diff_response["dictionary_item_added"])
                )
                for key_path in diff_response["dictionary_item_added"]:
                    if "root['" in key_path and "']" in key_path:
                        vehicle_id = key_path.split("'")[1]
                        updated[vehicle_id] = new[vehicle_id]

            vehicle_ids_changed = set()

            if "values_changed" in diff_response:
                logger.debug(
                    "Changed vehicle fields: %s", len(diff_response["values_changed"])
                )
                for key_path in diff_response["values_changed"]:
                    if "root['" in key_path and "']" in key_path:
                        vehicle_id = key_path.split("'")[1]
                        vehicle_ids_changed.add(vehicle_id)

            if "type_changes" in diff_response:
                logger.debug(
                    "Type changed vehicle fields: %s",
                    len(diff_response["type_changes"]),
                )
                for key_path in diff_response["type_changes"]:
                    if "root['" in key_path and "']" in key_path:
                        vehicle_id = key_path.split("'")[1]
                        vehicle_ids_changed.add(vehicle_id)

            for vehicle_id in vehicle_ids_changed:
                if vehicle_id in new:
                    updated[vehicle_id] = new[vehicle_id]

            removed: set[str] = set()
            if "dictionary_item_removed" in diff_response:
                logger.debug(
                    "Removed vehicles: %s",
                    len(diff_response["dictionary_item_removed"]),
                )
                for key_path in diff_response["dictionary_item_removed"]:
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
        key = f"{VEHICLE_STREAM_KEY}:{'buses' if frequent_buses else 'rapid'}"
        while True:
            with tracer.start_as_current_span(
                "vehicle_stream_diff.watch_iteration"
            ) as span:
                span.set_attribute("vehicle_stream.frequent_buses", frequent_buses)
                span.set_attribute("vehicle_stream.empty_count", self.empty_count)
                add_transaction_ids_to_span(span)

                try:
                    features = await get_vehicle_features(
                        self.r_client, self.config, self.tg, frequent_buses
                    )
                except (RedisError, ConnectionError, TimeoutError):
                    logger.error(
                        "Failed to fetch vehicle features", exc_info=True
                    )
                    span.set_attribute("vehicle_stream.fetch_error", True)
                    await sleep(SLEEP_DURATION)
                    continue

                span.set_attribute("vehicles.current_count", len(features))

                try:
                    if len(features) == 0:
                        if self.empty_count > 3:
                            await self.r_client.publish(
                                key,
                                DiffApiResponse(
                                    updated={}, removed=set()
                                ).model_dump_json(),
                            )
                            span.set_attribute("vehicle_stream.published_empty", True)
                            await sleep(5)
                        self.empty_count += 1
                    else:
                        self.empty_count = 0
                        diff = self._calculate_diff(self.current_snapshot, features)
                        await self.r_client.publish(key, diff.model_dump_json())
                        span.set_attribute("vehicle_stream.published_diff", True)
                        span.set_attribute(
                            "vehicle_stream.diff_updated_count", len(diff.updated)
                        )
                        span.set_attribute(
                            "vehicle_stream.diff_removed_count", len(diff.removed)
                        )
                except RedisError:
                    span.set_attribute("vehicle_stream.redis_error", True)
                    logger.error(
                        "Unable to publish vehicle stream update", exc_info=True
                    )

                add_current_span_attributes({"vehicle_stream.iteration_complete": True})

            await sleep(SLEEP_DURATION)
            if features:
                self._save_snapshot(features)


async def run_vehicle_stream_diff(
    r_client: Redis, config: Config, tg: TaskGroup, frequent_buses: bool = False
) -> None:
    vse = VehicleStreamDiff(r_client, config, tg)
    await vse.watch(frequent_buses)
