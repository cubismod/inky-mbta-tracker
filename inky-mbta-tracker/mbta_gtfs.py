import logging
from asyncio import CancelledError
from datetime import UTC, datetime
from typing import Any, Optional

import aiohttp
from anyio import sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from consts import MBTA_V3_ENDPOINT
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from mbta_client import MBTAApi, occupancy_status_human_readable
from pydantic import ValidationError
from redis.asyncio import Redis
from redis_cache import get_cache
from shared_types.shared_types import ScheduleEvent, VehicleRedisSchema
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential_jitter,
)
from utils import Config

GTFS_BASE_URL = "https://cdn.mbta.com/"
logger = logging.getLogger(__name__)


def _process_cariages(entity: dict[str, Any]) -> Optional[list[str]]:
    carriages: list[str] = []
    if "multi_carriage_details" in entity:
        for carriage in entity["multi_carriage_details"]:
            if "id" in carriage:
                carriages.append(carriage["id"])
        return carriages
    else:
        return None


def _get_nested_field(obj: dict[str, Any], field_path: str) -> Optional[Any]:
    for part in field_path.split("."):
        if not isinstance(obj, dict) or part not in obj:
            return None
        obj = obj[part]
    return obj


async def _should_replace_current_event(
    vehicle: VehicleRedisSchema, r_client: Redis, id: str
):
    existing_val = await get_cache(r_client, f"vehicle:{id}")
    if existing_val:
        try:
            parsed_val = VehicleRedisSchema.model_validate_json(existing_val)
            if round(vehicle.latitude, 6) != round(parsed_val.latitude, 6):
                return True
            if round(vehicle.longitude, 6) != round(parsed_val.longitude, 6):
                return True
            return False
        except ValidationError as err:
            logger.error(
                f"Failed to validate existing cache for vehicle {id}", exc_info=err
            )
            return True
    return True


async def _process_gtfs_event(
    vehicle_entity: dict[str, Any],
    r_client: Redis,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    session: aiohttp.ClientSession,
    mbta_session: aiohttp.ClientSession,
    tg: TaskGroup,
    route_id: str,
):
    field_map = [
        ("vehicle.trip.trip_id", "trip_id"),
        ("vehicle.trip.direction_id", "direction_id"),
        ("vehicle.position.bearing", "bearing"),
        ("vehicle.stop_id", "stop"),
        ("vehicle.occupancy_status", "occupancy_status"),
        ("vehicle.position.latitude", "latitude"),
        ("vehicle.position.longitude", "longitude"),
        ("id", "id"),
        ("vehicle.current_status", "current_status"),
    ]

    fields: dict[str, Any] = {}
    for field_path, var_name in field_map:
        value = _get_nested_field(vehicle_entity, field_path)
        if value is not None:
            fields[var_name] = value

    required_fields = ("id", "current_status", "direction_id", "latitude", "longitude")
    if not all(k in fields for k in required_fields):
        return

    if fields.get("occupancy_status"):
        fields["occupancy_status"] = occupancy_status_human_readable(
            fields["occupancy_status"]
        )

    carriages = _process_cariages(vehicle_entity)
    direction_id = fields.get("direction_id")

    if fields.get("trip_id") and direction_id:
        async with MBTAApi(r_client) as mbta_client:
            fields["headsign"] = await mbta_client.get_headsign(
                mbta_session, tg, fields["trip_id"], route_id, direction_id
            )

    vehicle = VehicleRedisSchema(
        action="update",
        id=fields["id"],
        current_status=fields["current_status"],
        direction_id=fields["direction_id"],
        latitude=fields["latitude"],
        longitude=fields["longitude"],
        speed=fields.get("speed"),
        bearing=fields.get("bearing"),
        stop=fields.get("stop"),
        route=route_id,
        update_time=datetime.now().astimezone(UTC),
        occupancy_status=fields.get("occupancy_status"),
        carriages=carriages,
        headsign=fields.get("headsign"),
        trip_id=fields.get("trip_id"),
        source="MBTA Real-Time GTFS",
    )
    if await _should_replace_current_event(vehicle, r_client, fields["id"]):
        await send_stream.send(vehicle)


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def gtfs_loop(
    r_client: Redis,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    tg: TaskGroup,
    config: Config,
):
    if config.vehicles_by_route:
        async with aiohttp.ClientSession(base_url=GTFS_BASE_URL) as session:
            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as mbta_session:
                vehicles_feed = gtfs_realtime_pb2.FeedMessage()  # type: ignore
                while True:
                    async with session.get("realtime/VehiclePositions.pb") as response:
                        if response.status == 200:
                            vehicles_feed.ParseFromString(await response.read())
                            for entity in vehicles_feed.entity:
                                entity_dict = MessageToDict(
                                    entity, preserving_proto_field_name=True
                                )
                                if "vehicle" in entity_dict:
                                    vehicle = entity_dict["vehicle"]
                                    if "trip" in vehicle:
                                        trip = vehicle["trip"]
                                        if "route_id" in trip:
                                            route_id = trip["route_id"]
                                            if route_id in config.vehicles_by_route or (
                                                config.frequent_bus_lines
                                                and route_id
                                                in config.frequent_bus_lines
                                            ):
                                                await _process_gtfs_event(
                                                    entity_dict,
                                                    r_client,
                                                    send_stream,
                                                    session,
                                                    mbta_session,
                                                    tg,
                                                    route_id,
                                                )
                    await sleep(5)
