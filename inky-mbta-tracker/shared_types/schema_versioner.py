import logging
import os
from typing import Optional

from prometheus import redis_commands
from pydantic import BaseModel, ValidationError
from redis import ResponseError
from redis.asyncio.client import Redis
from redis_lock.asyncio import RedisLock
from tenacity import retry, stop_after_attempt, wait_exponential

import shared_types.class_hashes as class_hashes

logger = logging.getLogger(__name__)


class RedisSchema(BaseModel):
    id: str
    key_prefixes: list[str]
    hashes: set[str]


async def get_schema_version(redis: Redis, schema_key: str) -> Optional[RedisSchema]:
    try:
        schema_version = await redis.get(schema_key)
        redis_commands.labels("get").inc()
        if schema_version is None:
            return None
        return RedisSchema.model_validate_json(schema_version)
    except ResponseError as e:
        logger.error(f"Error getting schema version for {schema_key}: {e}")
        return None
    except ValidationError as e:
        logger.error(f"Error validating schema version for {schema_key}: {e}")
        return None


# this function is called each time an MBTAApi client is started and manages schema versioning by deleting keys associated with outdated schemas
# the tracker is able to gracefully recreate missing keys using the MBTA API
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=15))
async def schema_versioner() -> None:
    redis = Redis(
        host=os.environ.get("IMT_REDIS_ENDPOINT", ""),
        port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
        password=os.environ.get("IMT_REDIS_PASSWORD", ""),
    )
    schemas = [
        RedisSchema(
            id="schedule_event",
            key_prefixes=["schedule", "prediction", "time"],
            hashes={class_hashes.SCHEDULEEVENT_HASH},
        ),
        RedisSchema(
            id="vehicle_redis_schema",
            key_prefixes=["vehicle", "pos-data"],
            hashes={class_hashes.VEHICLEREDISSCHEMA_HASH},
        ),
        RedisSchema(
            id="track_assignment",
            key_prefixes=["track_history", "track_timeseries"],
            hashes={class_hashes.TRACKASSIGNMENT_HASH},
        ),
        RedisSchema(
            id="track_prediction",
            key_prefixes=["track_prediction"],
            hashes={class_hashes.TRACKPREDICTION_HASH},
        ),
        RedisSchema(
            id="track_prediction_stats",
            key_prefixes=["track_prediction_stats"],
            hashes={class_hashes.TRACKPREDICTIONSTATS_HASH},
        ),
        RedisSchema(
            id="stop",
            key_prefixes=["stop"],
            hashes={
                class_hashes.STOPRELATIONSHIP_HASH,
                class_hashes.STOPATTRIBUTES_HASH,
                class_hashes.STOPRESOURCE_HASH,
                class_hashes.STOP_HASH,
                class_hashes.STOPANDFACILITIES_HASH,
                class_hashes.FACILITYRELATIONSHIPS_HASH,
                class_hashes.FACILITYPROPERTY_HASH,
                class_hashes.FACILITYATTRIBUTES_HASH,
                class_hashes.FACILITYRESOURCE_HASH,
                class_hashes.FACILITY_HASH,
                class_hashes.FACILITIES_HASH,
            },
        ),
        RedisSchema(
            id="shape",
            key_prefixes=["shape"],
            hashes={
                class_hashes.SHAPES_HASH,
                class_hashes.SHAPERESOURCE_HASH,
                class_hashes.SHAPEATTRIBUTES_HASH,
            },
        ),
        RedisSchema(
            id="trip",
            key_prefixes=["trip"],
            hashes={
                class_hashes.TRIPS_HASH,
                class_hashes.TRIPRESOURCE_HASH,
                class_hashes.TRIPATTRIBUTES_HASH,
                class_hashes.TRIPGENERIC_HASH,
            },
        ),
    ]

    for schema in schemas:
        schema_key = f"schema:{schema.id}"
        schema_version = await get_schema_version(redis, schema_key)
        if schema_version is None or schema_version.hashes != schema.hashes:
            logger.warning(
                f"Schema version mismatch for {schema.id}. Removing keys associated with this schema."
            )
            try:
                async with RedisLock(
                    redis,
                    f"schema_versioner:{schema.id}",
                    blocking_timeout=30,
                    expire_timeout=120,
                ):
                    for key_prefix in schema.key_prefixes:
                        keys = await redis.keys(f"{key_prefix}*")
                        pl = redis.pipeline()
                        for key in keys:
                            pl.delete(key)
                        await pl.execute()
                    await redis.set(schema_key, schema.model_dump_json())
                    redis_commands.labels("set").inc()
                    logger.info(f"{schema.id} set to {schema.model_dump_json()}")
            except ResponseError:
                logger.error(
                    f"Unable to perform schema versioning for {schema.id}",
                    exc_info=True,
                )
                logger.fatal("Exiting due to schema versioning failure")
