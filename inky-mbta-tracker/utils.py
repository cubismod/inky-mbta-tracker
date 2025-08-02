import json
import os
from asyncio import Runner
from datetime import datetime
from queue import Queue
from typing import TYPE_CHECKING, Optional

from redis.asyncio import Redis

if TYPE_CHECKING:
    from schedule_tracker import ScheduleEvent, VehicleRedisSchema
    from shared_types.shared_types import TaskType
    from vehicles_background_worker import State


def get_redis() -> Redis:
    return Redis(
        host=os.environ.get("IMT_REDIS_ENDPOINT", ""),
        port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
        password=os.environ.get("IMT_REDIS_PASSWORD", ""),
    )


async def get_vehicles_data(r_client: Redis) -> dict:
    """Get vehicle data with caching"""
    from consts import VEHICLES_CACHE_TTL
    from geojson_utils import get_vehicle_features

    cache_key = "api:vehicles"
    cached_data = await r_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    features = await get_vehicle_features(r_client)
    result = {"type": "FeatureCollection", "features": features}

    await r_client.setex(cache_key, VEHICLES_CACHE_TTL, json.dumps(result))

    return result


def thread_runner(
    target: "TaskType",
    queue: "Queue[ScheduleEvent | VehicleRedisSchema] | None",
    transit_time_min: int = 0,
    stop_id: Optional[str] = None,
    route: Optional[str] = None,
    direction_filter: Optional[int] = None,
    expiration_time: Optional[datetime] = None,
    show_on_display: bool = True,
    route_substring_filter: Optional[str] = None,
    precache_routes: Optional[list[str]] = None,
    precache_stations: Optional[list[str]] = None,
    precache_interval_hours: int = 2,
    vehicles_queue: Optional["Queue[State]"] = None,
) -> None:
    from mbta_client import (
        precache_track_predictions_runner,
        watch_static_schedule,
        watch_station,
        watch_vehicles,
    )
    from shared_types.shared_types import TaskType
    from vehicles_background_worker import run_background_worker

    with Runner() as runner:
        match target:
            case TaskType.SCHEDULES:
                if stop_id and queue is not None:
                    runner.run(
                        watch_static_schedule(
                            stop_id,
                            route,
                            direction_filter,
                            queue,
                            transit_time_min,
                            show_on_display,
                            route_substring_filter,
                        )
                    )
            case TaskType.SCHEDULE_PREDICTIONS:
                if queue is not None:
                    runner.run(
                        watch_station(
                            stop_id or "place-sstat",
                            route,
                            direction_filter,
                            queue,
                            transit_time_min,
                            expiration_time,
                            show_on_display,
                            route_substring_filter,
                        )
                    )
            case TaskType.VEHICLES:
                if queue is not None:
                    runner.run(
                        watch_vehicles(
                            queue,
                            expiration_time,
                            route or "Red",
                        )
                    )
            case TaskType.TRACK_PREDICTIONS:
                runner.run(
                    precache_track_predictions_runner(
                        routes=precache_routes,
                        target_stations=precache_stations,
                        interval_hours=precache_interval_hours,
                    )
                )
            case TaskType.VEHICLES_BACKGROUND_WORKER:
                if vehicles_queue:
                    runner.run(run_background_worker(vehicles_queue))
