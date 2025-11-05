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


def hex_color_to_int(hex_color: str, default: int = 0) -> int:
    """Convert a hex color string to an integer.

    Accepts strings like "#5865f2", "5865f2", "#abc", or "abc".
    For 3-digit shorthand (e.g. "abc") this expands to "aabbcc".
    The returned integer is the decimal representation of the RGB value (0xRRGGBB).
    Leading "#" is optional.

    On invalid input this returns the provided `default` value (defaults to 0).

    Example:
        >>> hex_color_to_int("#5865f2")
        5793266
    """
    s = hex_color.strip()
    if s.startswith("#"):
        s = s[1:]

    # Expand shorthand (e.g. "abc" -> "aabbcc")
    if len(s) == 3:
        s = "".join(ch * 2 for ch in s)

    if len(s) != 6:
        return default

    try:
        return int(s, 16)
    except ValueError:
        return default
