import json
import os
from queue import Queue
from typing import TYPE_CHECKING

from anyio import run
from redis.asyncio import Redis
from vehicles_background_worker import run_background_worker

if TYPE_CHECKING:
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


def bg_worker(queue: Queue[State]) -> None:
    run(
        run_background_worker,
        queue,
        backend="asyncio",
        backend_options={"use_uvloop": True},
    )
