import logging
from datetime import datetime

import orjson
from anyio import sleep
from anyio.abc import TaskGroup
from config import Config
from consts import DAY
from geojson import Feature
from geojson_utils import get_vehicle_features
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

_RENDERED_PROPERTIES = ("route", "marker-symbol", "marker-color", "id", "marker-size")


# removes unnecessary fields that won't be rendered in the frontend
def _strip_response_fields(vehicles: dict[str, Feature]) -> dict[str, Feature]:
    return {
        vehicle_id: Feature(
            id=vehicle_id,
            geometry=vehicle.get("geometry"),
            properties={
                key: value
                for key, value in vehicle.get("properties", {}).items()
                if key in _RENDERED_PROPERTIES
            },
        )
        for vehicle_id, vehicle in vehicles.items()
    }


async def run(r_client: Redis, key: str, config: Config, tg: TaskGroup):
    await sleep(60)
    while True:
        try:
            vehicles = _strip_response_fields(
                await get_vehicle_features(r_client, config, tg)
            )
            ts_key = datetime.now().timestamp()
            await r_client.hsetex(
                key,
                str(ts_key),
                orjson.dumps(vehicles).decode("utf-8"),
                ex=2 * DAY,
            )  # pyright: ignore
            await sleep(45 * 60)
        except (ConnectionError, TimeoutError) as err:
            logger.error(f"Redis error: {err}")
