import json
from typing import TYPE_CHECKING, List

from anyio.abc import TaskGroup
from geojson import Feature
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

if TYPE_CHECKING:
    pass


def get_redis(pool: ConnectionPool) -> Redis:
    return Redis().from_pool(pool)


async def get_vehicles_data(r_client: Redis, tg: TaskGroup) -> dict[str, List[Feature]]:
    """Get vehicle data with caching"""
    from consts import VEHICLES_CACHE_TTL
    from geojson_utils import get_vehicle_features

    cache_key = "api:vehicles"
    cached_data = await r_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    features = await get_vehicle_features(r_client, tg)
    result = {"type": "FeatureCollection", "features": features}

    tg.start_soon(r_client.setex, cache_key, VEHICLES_CACHE_TTL, json.dumps(result))

    return result
