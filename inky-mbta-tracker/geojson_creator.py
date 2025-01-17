import logging
import os
from asyncio import Runner, sleep

import geojson.geometry
from geojson import Feature, FeatureCollection, Point, dumps
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
from schedule_tracker import VehicleRedisSchema
from tenacity import before_sleep_log, retry, wait_exponential

logger = logging.getLogger("geojson")


def ret_color(vehicle: VehicleRedisSchema):
    if vehicle.route.startswith("Green"):
        return "#008150"
    if vehicle.route.startswith("Blue"):
        return "#2F5DA6"
    if vehicle.route.startswith("CR"):
        return "#7B388C"
    if vehicle.route.startswith("Red") or vehicle.route.startswith("Mattapan"):
        return "#FA2D27"
    if vehicle.route.startswith("Orange"):
        return "#FD8A03"


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
async def create_json():
    r = Redis(
        host=os.environ.get("IMT_REDIS_ENDPOINT"),
        port=os.environ.get("IMT_REDIS_PORT", "6379"),
        password=os.environ.get("IMT_REDIS_PASSWORD"),
    )

    while True:
        geojson.geometry.DEFAULT_PRECISION = 14
        try:
            vehicle_ids = await r.scan(0, match="vehicle*", count=300)
            features = list[Feature]()
            pl = r.pipeline()

            if vehicle_ids:
                for vehicle in vehicle_ids[1]:
                    dec_v = vehicle.decode("utf-8")
                    if dec_v:
                        await pl.get(vehicle)

                results = await pl.execute()
                for result in results:
                    vehicle_info = VehicleRedisSchema.model_validate_json(
                        strict=False, json_data=result
                    )
                    point = Point((vehicle_info.longitude, vehicle_info.latitude))
                    feature = Feature(
                        geometry=point,
                        id=vehicle_info.id,
                        properties={
                            "route": vehicle_info.route,
                            "status": vehicle_info.current_status,
                            "marker-size": "medium",
                            "marker-symbol": "rail",
                            "marker-color": ret_color(vehicle_info),
                            "speed": vehicle_info.speed,
                            "direction": vehicle_info.direction_id,
                            "id": vehicle_info.id,
                            "stop": vehicle_info.stop,
                        },
                    )
                    features.append(feature)
                with open(
                    os.environ.get("IMT_JSON_WRITE_FILE", "./imt-out.json"), "w"
                ) as file:
                    file.write(dumps(FeatureCollection(features=features)))

            await sleep(45)
        except ResponseError as err:
            logger.error("unable to run redis command", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate model", exc_info=err)


def run():
    with Runner() as runner:
        runner.run(create_json())
