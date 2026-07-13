import logging
from random import randint

from anyio import sleep
from anyio.abc import TaskGroup
from config import Config
from consts import VEHICLE_STREAM_KEY
from geojson import Feature
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_current_span_attributes, add_transaction_ids_to_span
from prometheus import vehicle_stream_pubsub
from redis.asyncio import Redis
from redis.exceptions import RedisError
from shared_types.shared_types import DiffApiResponse

SLEEP_DURATION = 3

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

            original_keys = set(original)
            new_keys = set(new)

            added_keys = new_keys - original_keys
            removed_keys = original_keys - new_keys
            common_keys = original_keys & new_keys

            updated: dict[str, Feature] = {k: new[k] for k in added_keys}
            updated.update({k: new[k] for k in common_keys if original[k] != new[k]})

            removed: set[str] = removed_keys

            logger.debug(
                "Vehicles: %d added, %d changed, %d removed",
                len(added_keys),
                len(updated) - len(added_keys),
                len(removed_keys),
            )

            span.set_attribute("diff.updated_count", len(updated))
            span.set_attribute("diff.removed_count", len(removed))

            logger.debug(
                "Returning %s updated, %s removed vehicles", len(updated), len(removed)
            )
            return DiffApiResponse(updated=updated, removed=removed)

    async def watch(self, frequent_buses: bool = False) -> None:
        key = f"{VEHICLE_STREAM_KEY}:{'buses' if frequent_buses else 'rapid'}"
        while True:
            try:
                subscribers = await self.r_client.pubsub_numsub(key)
            except RedisError:
                subscribers = None

            if subscribers is not None and subscribers[0][1] == 0:
                await sleep(SLEEP_DURATION)
                continue

            with tracer.start_as_current_span(
                "vehicle_stream_diff.watch_iteration"
            ) as span:
                span.set_attribute("vehicle_stream.frequent_buses", frequent_buses)
                span.set_attribute("vehicle_stream.empty_count", self.empty_count)
                add_transaction_ids_to_span(span)

                try:
                    features = await get_vehicle_features(
                        self.r_client,
                        self.config,
                        self.tg,
                        frequent_buses,
                        skip_cache=True,
                    )
                except (RedisError, ConnectionError, TimeoutError):
                    logger.error("Failed to fetch vehicle features", exc_info=True)
                    span.set_attribute("vehicle_stream.fetch_error", True)
                    await sleep(SLEEP_DURATION)
                    continue

                span.set_attribute("vehicles.current_count", len(features))

                try:
                    if len(features) == 0:
                        if self.empty_count > 3:
                            # we want to avoid serving up empty vehicle data as much as possible so wait a few times
                            # before actually returning empty data to prevent possible intermittent pipeline errors
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
                        if not diff.updated and not diff.removed:
                            continue

                        await self.r_client.publish(key, diff.model_dump_json())
                        [
                            vehicle_stream_pubsub.labels(event_type="publish").inc()
                            for _ in diff.updated
                        ]
                        [
                            vehicle_stream_pubsub.labels(event_type="publish").inc()
                            for _ in diff.removed
                        ]
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
            if features:
                self._save_snapshot(features)
            await sleep(randint(1, SLEEP_DURATION))


async def run_vehicle_stream_diff(
    r_client: Redis, config: Config, tg: TaskGroup, frequent_buses: bool = False
) -> None:
    vse = VehicleStreamDiff(r_client, config, tg)
    await vse.watch(frequent_buses)
