from typing import TYPE_CHECKING

from geojson import Feature
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

if TYPE_CHECKING:
    pass


def get_redis(pool: ConnectionPool) -> Redis:
    return Redis().from_pool(pool)


async def get_vehicles_data(r_client: Redis) -> dict[str, Feature]:
    """Get vehicle data with caching"""
    from geojson_utils import get_vehicle_features

    features = await get_vehicle_features(r_client)

    return features
