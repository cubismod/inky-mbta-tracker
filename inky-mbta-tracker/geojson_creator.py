import logging
import os
from asyncio import Runner, sleep

import boto3
from config import Config
from geojson import (
    Feature,
    FeatureCollection,
    LineString,
    Point,
    dumps,
)
from mbta_client import get_shapes, light_get_stop, silver_line_lookup
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
from s3transfer import S3UploadFailedError
from schedule_tracker import VehicleRedisSchema
from tenacity import before_sleep_log, retry, wait_random_exponential

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
    if vehicle.route.startswith("74") or vehicle.route.startswith("SL"):
        return "#9A9C9D"
    elif vehicle.route.isdecimal():
        return "#907e00"


# create an upload a geojson file to S3
def create_and_upload_file(
    resource, file_name: str, bucket_name: str, features: list[Feature]
):
    with open(
        file_name,
        "w",
    ) as file:
        file.write(
            dumps(
                FeatureCollection(features),
                sort_keys=True,
            )
        )
        bucket = resource.Bucket(bucket_name)
        obj = bucket.Object(file_name)
        try:
            obj.upload_file(file_name)
            logger.info(f"Successfully uploaded file {file_name} to s3://{bucket_name}")
        except S3UploadFailedError as err:
            logger.error(
                f"Couldn't upload file {file_name} to {bucket.name}", exc_info=err
            )


@retry(
    wait=wait_random_exponential(multiplier=1, min=1),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
async def create_json(config: Config):
    r = Redis(
        host=os.environ.get("IMT_REDIS_ENDPOINT"),
        port=os.environ.get("IMT_REDIS_PORT", "6379"),
        password=os.environ.get("IMT_REDIS_PASSWORD"),
    )

    # set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables if you wish to use S3
    s3_bucket = os.environ.get("IMT_S3_BUCKET")

    prefix = os.environ.get("IMT_S3_PREFIX", "")

    lines = list()
    shapes = await get_shapes(config.vehicles_by_route, r)
    if shapes:
        for k, v in shapes.items():
            for line in v:
                if k.startswith("74") or k.startswith("75"):
                    k = silver_line_lookup(k)
                lines.append(
                    Feature(
                        geometry=LineString(coordinates=line),
                        properties={"route": k},
                    )
                )
    if s3_bucket:
        resource = boto3.resource("s3")
        create_and_upload_file(resource, f"{prefix}shapes.json", s3_bucket, lines)
        while True:
            try:
                features = dict[str, Feature]()
                pl = r.pipeline()

                for vehicle in await r.smembers("pos-data"):
                    dec_v = vehicle.decode("utf-8")
                    if dec_v:
                        await pl.get(vehicle)

                results = await pl.execute()
                for result in results:
                    if result:
                        vehicle_info = VehicleRedisSchema.model_validate_json(
                            strict=False, json_data=result
                        )
                        if vehicle_info.route:
                            point = Point(
                                (vehicle_info.longitude, vehicle_info.latitude)
                            )
                            stop = await light_get_stop(r, vehicle_info.stop)
                            route_icon = "rail"
                            if (
                                vehicle_info.route.startswith("7")
                                or vehicle_info.route.isdecimal()
                            ):
                                route_icon = "bus"
                            if vehicle_info.route.startswith(
                                "74"
                            ) or vehicle_info.route.startswith("75"):
                                vehicle_info.route = silver_line_lookup(
                                    vehicle_info.route
                                )
                            feature = Feature(
                                geometry=point,
                                id=vehicle_info.id,
                                properties={
                                    "route": vehicle_info.route,
                                    "status": vehicle_info.current_status,
                                    "marker-size": "medium",
                                    "marker-symbol": route_icon,
                                    "marker-color": ret_color(vehicle_info),
                                    "speed": vehicle_info.speed,
                                    "direction": vehicle_info.direction_id,
                                    "id": vehicle_info.id,
                                    "stop": stop[0],
                                },
                            )
                            features[f"v-{vehicle_info.id}"] = feature

                            if stop[0] and vehicle_info.stop:
                                stop_point = Point(stop[1])
                                stop_feature = Feature(
                                    geometry=stop_point,
                                    id=vehicle_info.stop,
                                    properties={
                                        "marker-size": "small",
                                        "marker-symbol": "building",
                                        "marker-color": ret_color(vehicle_info),
                                        "name": stop[0],
                                        "id": vehicle_info.stop,
                                    },
                                )
                                features[f"v-{vehicle_info.stop}"] = stop_feature
                vals = [v for _, v in features.items()]
                if len(vals) > 0:
                    create_and_upload_file(
                        resource, f"{prefix}vehicles.json", s3_bucket, vals
                    )
                await sleep(int(os.getenv("IMT_S3_REFRESH_TIME", "35")))
            except ResponseError as err:
                logger.error("unable to run redis command", exc_info=err)
            except ValidationError as err:
                logger.error("unable to validate model", exc_info=err)


def run(config: Config):
    with Runner() as runner:
        runner.run(create_json(config))
