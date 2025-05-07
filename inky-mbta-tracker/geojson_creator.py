import logging
import os
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from random import randint
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
import boto3
import humanize
from aiohttp import ClientSession
from config import Config
from consts import MBTA_V3_ENDPOINT
from geojson import (
    Feature,
    FeatureCollection,
    LineString,
    Point,
    dumps,
)
from mbta_client import (
    determine_station_id,
    get_shapes,
    light_get_alerts,
    light_get_stop,
    silver_line_lookup,
)
from mbta_responses import AlertResource, Alerts
from prometheus import redis_commands
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
from s3transfer import S3UploadFailedError
from schedule_tracker import VehicleRedisSchema
from tenacity import before_sleep_log, retry, wait_random_exponential
from track_predictor.track_predictor import TrackPredictor
from turfpy.measurement import bearing, distance

logger = logging.getLogger("geojson")


def ret_color(vehicle: VehicleRedisSchema) -> str:
    if vehicle.route.startswith("Amtrak"):
        return "#18567D"
    if vehicle.route.startswith("Amtrak"):
        return "#18567D"
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
        return "#FFFF00"
    return ""


def calculate_stop_eta(stop: Feature, vehicle: Feature, speed: float) -> str:
    dis = distance(stop, vehicle, "mi")
    # mi / mph = hr
    elapsed = dis / speed
    return humanize.naturaldelta(timedelta(hours=elapsed))


# create an upload a geojson file or list of alerts to S3
def create_and_upload_file(  # type: ignore
    resource,
    file_name: str,
    bucket_name: str,
    features: Optional[list[Feature]] = None,
    alerts: Optional[list[AlertResource]] = None,
) -> None:
    write_str = ""
    if features:
        write_str = dumps(
            FeatureCollection(features),
            sort_keys=True,
        )
    elif alerts:
        write_str = Alerts(data=alerts).model_dump_json(exclude_unset=True)
    if "geometry" in write_str or alerts:
        with open(
            file_name,
            "w",
        ) as file:
            file.write(write_str)
            bucket = resource.Bucket(bucket_name)
            obj = bucket.Object(file_name)
            try:
                obj.upload_file(file_name)
                logger.info(
                    f"Successfully uploaded file {file_name} to s3://{bucket_name}"
                )
            except S3UploadFailedError as err:
                logger.error(
                    f"Couldn't upload file {file_name} to {bucket.name}", exc_info=err
                )


def calculate_bearing(start: Point, end: Point) -> float:
    return bearing(Feature(geometry=start), Feature(geometry=end))


async def collect_alerts(config: Config, session: ClientSession) -> list[AlertResource]:
    alerts = dict[str, AlertResource]()
    if config.vehicles_by_route:
        for v in config.vehicles_by_route:
            await sleep(randint(1, 3))
            al = await light_get_alerts(v, session)
            if al:
                for a in al:
                    alerts[a.id] = a
    collected_alerts = list(alerts.values())
    collected_alerts.sort(
        key=lambda alert: alert.attributes.updated_at or alert.attributes.created_at,
        reverse=True,
    )
    return collected_alerts


@retry(
    wait=wait_random_exponential(multiplier=1, min=1),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
async def create_json(config: Config) -> None:
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        r = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT", ""),
            port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
            password=os.environ.get("IMT_REDIS_PASSWORD", ""),
        )
        # set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables if you wish to use S3
        s3_bucket = os.environ.get("IMT_S3_BUCKET")

        prefix = os.environ.get("IMT_S3_PREFIX", "")

        lines = list()
        if config.vehicles_by_route:
            shapes = await get_shapes(r, config.vehicles_by_route, session)
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
                create_and_upload_file(
                    resource, f"{prefix}shapes.json", s3_bucket, features=lines
                )
                next_alert_time = datetime.now().astimezone(UTC)
                while True:
                    logger.info("uploading geojson")
                    try:
                        features = dict[str, Feature]()
                        pl = r.pipeline()

                        # periodically clean up the set during overnight hours to avoid unnecessary redis calls
                        delete_all_pos_data = False
                        now = datetime.now().astimezone(ZoneInfo("US/Eastern"))
                        if await r.scard("pos-data") > 500 and 4 > now.hour > 2:
                            redis_commands.labels("scard").inc()
                            delete_all_pos_data = True

                        vehicles = await r.smembers("pos-data")
                        redis_commands.labels("smembers").inc()
                        for vehicle in vehicles:
                            if delete_all_pos_data:
                                await r.srem("pos-data", vehicle)
                                redis_commands.labels("srem").inc()
                            dec_v = vehicle.decode("utf-8")
                            if dec_v:
                                await pl.get(vehicle)
                                redis_commands.labels("get").inc()

                        results = await pl.execute()
                        for result in results:
                            if result:
                                vehicle_bearing = None
                                vehicle_info: VehicleRedisSchema = (
                                    VehicleRedisSchema.model_validate_json(
                                        strict=False, json_data=result
                                    )
                                )
                                if vehicle_info.route and vehicle_info.stop:
                                    point = Point(
                                        (vehicle_info.longitude, vehicle_info.latitude)
                                    )
                                    stop = None
                                    stop_id = None
                                    long = None
                                    lat = None
                                    route_icon = "rail"
                                    stop_eta = None

                                    if not vehicle_info.route.startswith("Amtrak"):
                                        stop = await light_get_stop(
                                            r, vehicle_info.stop, session
                                        )
                                        platform_prediction = None
                                        if stop:
                                            stop_id = stop.stop_id
                                            if stop.long and stop.lat:
                                                stop_point = Point(
                                                    (stop.long, stop.lat)
                                                )
                                                vehicle_bearing = calculate_bearing(
                                                    point, stop_point
                                                )
                                                long = stop.long
                                                lat = stop.lat
                                                if (
                                                    vehicle_info.speed
                                                    and vehicle_info.speed >= 10
                                                ):
                                                    stop_eta = calculate_stop_eta(
                                                        Feature(geometry=stop_point),
                                                        Feature(geometry=point),
                                                        vehicle_info.speed,
                                                    )
                                            station_id, has_track_predictions = (
                                                determine_station_id(stop_id)
                                            )
                                            if (
                                                vehicle_info.route.startswith("CR")
                                                and has_track_predictions
                                            ):
                                                track_predictor = TrackPredictor()
                                                prediction = await track_predictor.predict_track(
                                                    station_id=station_id,
                                                    route_id=vehicle_info.route,
                                                    trip_id=f"{vehicle_info.route}:{vehicle_info.id}",
                                                    headsign=vehicle_info.headsign,
                                                    direction_id=vehicle_info.direction_id,
                                                    scheduled_time=vehicle_info.update_time,
                                                )
                                                if prediction:
                                                    platform_prediction = f"{prediction.track_number} ({round(prediction.confidence_score * 100)}% confidence)"
                                    else:
                                        route_icon = "rail_amtrak"
                                        stop_id = vehicle_info.stop

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
                                            "stop": stop_id,
                                            "stop_eta": stop_eta,
                                            "stop-coordinates": (
                                                long,
                                                lat,
                                            ),
                                            "bearing": vehicle_bearing,
                                            "occupancy_status": vehicle_info.occupancy_status,
                                            # https://tc39.es/ecma262/multipage/numbers-and-dates.html#sec-date-time-string-format
                                            "approximate_speed": vehicle_info.approximate_speed,
                                            "update_time": vehicle_info.update_time.strftime(
                                                "%Y-%m-%dT%H:%M:%S.000Z"
                                            ),
                                            "platform_prediction": platform_prediction,
                                            "headsign": vehicle_info.headsign,
                                        },
                                    )
                                    features[f"v-{vehicle_info.id}"] = feature

                                    if (
                                        stop
                                        and stop.stop_id
                                        and vehicle_info.stop
                                        and stop.long
                                        and stop.lat
                                        and not vehicle_info.route.isdecimal()
                                    ):
                                        stop_point = Point((stop.long, stop.lat))
                                        stop_feature = Feature(
                                            geometry=stop_point,
                                            id=vehicle_info.stop,
                                            properties={
                                                "marker-size": "small",
                                                "marker-symbol": "building",
                                                "marker-color": ret_color(vehicle_info),
                                                "name": stop.stop_id,
                                                "id": vehicle_info.stop,
                                            },
                                        )
                                        features[f"v-{vehicle_info.stop}"] = (
                                            stop_feature
                                        )
                        vals = [v for _, v in features.items()]
                        create_and_upload_file(
                            resource, f"{prefix}vehicles.json", s3_bucket, vals
                        )
                        if datetime.now().astimezone() > next_alert_time:
                            logger.info("updating alerts")
                            create_and_upload_file(
                                resource,
                                f"{prefix}alerts.json",
                                alerts=await collect_alerts(config, session),
                                bucket_name=s3_bucket,
                            )
                            next_alert_time = datetime.now().astimezone(
                                UTC
                            ) + timedelta(minutes=4)
                        await sleep(int(os.getenv("IMT_S3_REFRESH_TIME", "35")))
                    except ResponseError as err:
                        logger.error("unable to run redis command", exc_info=err)
                    except ValidationError as err:
                        logger.error("unable to validate model", exc_info=err)


def run(config: Config) -> None:
    with Runner() as runner:
        runner.run(create_json(config))
