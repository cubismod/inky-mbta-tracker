import logging
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
import humanize
from aiohttp import ClientSession
from anyio import sleep
from anyio.abc import TaskGroup
from config import Config
from consts import MBTA_V3_ENDPOINT
from geojson import Feature, LineString, Point
from mbta_client import (
    get_shapes,
    light_get_stop,
    silver_line_lookup,
)
from mbta_responses import AlertResource
from prometheus import redis_commands
from pydantic import ValidationError
from redis.asyncio import Redis
from schedule_tracker import VehicleRedisSchema
from turfpy.measurement import bearing, distance

logger = logging.getLogger("geojson_utils")


async def light_get_alerts_batch(
    routes_str: str, session: ClientSession, r_client: Redis
) -> Optional[list[AlertResource]]:
    """Get alerts for multiple routes in a single API call"""
    from mbta_client import MBTA_AUTH
    from mbta_responses import Alerts

    endpoint = f"/alerts?filter[route]={routes_str}&api_key={MBTA_AUTH}&filter[lifecycle]=NEW,ONGOING,ONGOING_UPCOMING&filter[datetime]=NOW&filter[severity]=3,4,5,6,7,8,9,10"

    logger.debug(f"Fetching alerts from endpoint: {endpoint}")

    try:
        async with session.get(endpoint) as response:
            if response.status == 429:
                # we don't retry here because the client expects an immediate response
                logger.warning("Rate limit hit while fetching alerts")
                return None
            elif response.status != 200:
                logger.error(f"HTTP {response.status} while fetching alerts")
                return None

            data = await response.json()
            logger.debug(
                f"Raw response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}"
            )

            # Validate the response structure
            if not isinstance(data, dict):
                logger.error(f"MBTA API returned non-dict response: {type(data)}")
                return None

            if "data" not in data:
                logger.error(
                    f"MBTA API response missing 'data' key: {list(data.keys())}"
                )
                return None

            if not isinstance(data["data"], list):
                logger.error(f"MBTA API 'data' is not a list: {type(data['data'])}")
                return None

            alerts_response = Alerts.model_validate(data)
            alerts_count = len(alerts_response.data) if alerts_response.data else 0
            logger.debug(f"Parsed {alerts_count} alerts from response")
            return alerts_response.data if alerts_response.data else []
    except ValidationError as err:
        logger.error("Unable to validate alerts response", exc_info=err)
        return None
    except Exception as err:
        logger.error("Error fetching alerts", exc_info=err)
        return None


def lookup_vehicle_color(vehicle: VehicleRedisSchema) -> str:
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


def calculate_bearing(start: Point, end: Point) -> float:
    return bearing(Feature(geometry=start), Feature(geometry=end))


async def collect_alerts(
    config: Config, session: ClientSession, r_client: Redis
) -> list[AlertResource]:
    """Collect alerts for the routes specified in the config"""
    logger.debug(f"Collecting alerts for routes: {config.vehicles_by_route}")

    alerts = dict[str, AlertResource]()
    if config.vehicles_by_route:
        # Split routes into batches to avoid MBTA API limitations
        # MBTA API might have limits on query string length or number of routes
        batch_size = 10  # Process 10 routes at a time
        route_batches = [
            config.vehicles_by_route[i : i + batch_size]
            for i in range(0, len(config.vehicles_by_route), batch_size)
        ]

        logger.debug(
            f"Processing {len(config.vehicles_by_route)} routes in {len(route_batches)} batches"
        )

        for batch_num, route_batch in enumerate(route_batches, 1):
            routes_str = ",".join(route_batch)
            logger.debug(
                f"Fetching alerts for batch {batch_num}/{len(route_batches)}: {routes_str}"
            )

            al = await light_get_alerts_batch(routes_str, session, r_client)
            if al:
                logger.debug(
                    f"Batch {batch_num}: Received {len(al)} alerts from MBTA API"
                )
                for a in al:
                    try:
                        # Validate alert structure before adding
                        if not hasattr(a, "id") or not a.id:
                            logger.warning(
                                f"Alert missing ID in batch {batch_num}: {a}"
                            )
                            continue

                        if not hasattr(a, "attributes") or not a.attributes:
                            logger.warning(
                                f"Alert {a.id} missing attributes in batch {batch_num}: {a}"
                            )
                            continue

                        if not hasattr(a.attributes, "header"):
                            logger.warning(
                                f"Alert {a.id} missing header in batch {batch_num}: {a}"
                            )
                            continue

                        alerts[a.id] = a
                        logger.debug(
                            f"Alert: {a.attributes.header} (ID: {a.id}, Severity: {a.attributes.severity})"
                        )

                        # Log the full alert structure for debugging
                        logger.debug(f"Alert {a.id} full structure: {a}")
                        logger.debug(f"Alert {a.id} attributes: {a.attributes}")
                        logger.debug(
                            f"Alert {a.id} attributes dir: {dir(a.attributes)}"
                        )

                    except Exception as e:
                        logger.error(
                            f"Error processing alert in batch {batch_num}",
                            exc_info=e,
                        )
                        logger.debug("Problematic alert data", extra={"alert": a})
                        continue
            else:
                logger.debug(f"Batch {batch_num}: No alerts received from MBTA API")

            # Small delay between batches to avoid rate limiting
            if batch_num < len(route_batches):
                await sleep(0.1)
    else:
        logger.warning("No vehicles_by_route configured, cannot collect alerts")

    collected_alerts = list(alerts.values())
    collected_alerts.sort(
        key=lambda alert: alert.attributes.updated_at or alert.attributes.created_at,
        reverse=True,
    )

    logger.info(
        f"Total alerts collected: {len(collected_alerts)} from all route batches"
    )
    return collected_alerts


async def get_vehicle_features(
    r_client: Redis, tg: Optional[TaskGroup] = None
) -> list[Feature]:
    """Extract vehicle features from Redis data"""
    features = dict[str, Feature]()

    async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
        # periodically clean up the set during overnight hours to avoid unnecessary redis calls
        delete_all_pos_data = False
        now = datetime.now().astimezone(ZoneInfo("US/Eastern"))
        pos_data_count = await r_client.scard("pos-data")  # type: ignore[misc]
        if pos_data_count > 500 and 4 > now.hour > 2:
            redis_commands.labels("scard").inc()
            delete_all_pos_data = True

        vehicles = await r_client.smembers("pos-data")  # type: ignore[misc]
        redis_commands.labels("smembers").inc()
        pl = r_client.pipeline()

        for vehicle in vehicles:
            if delete_all_pos_data:
                await r_client.srem("pos-data", vehicle)  # type: ignore[misc]
                redis_commands.labels("srem").inc()
            dec_v = vehicle.decode("utf-8")
            if dec_v:
                await pl.get(vehicle)
                redis_commands.labels("get").inc()

        results = await pl.execute()
        redis_commands.labels("execute").inc()
        for result in results:
            if result:
                vehicle_bearing = None
                try:
                    vehicle_info: VehicleRedisSchema = (
                        VehicleRedisSchema.model_validate_json(
                            strict=False, json_data=result
                        )
                    )
                except ValidationError:
                    continue

                if vehicle_info.route and vehicle_info.stop:
                    point = Point((vehicle_info.longitude, vehicle_info.latitude))
                    stop = None
                    stop_id = None
                    long = None
                    lat = None
                    route_icon = "rail"
                    stop_eta = None
                    if not vehicle_info.route.startswith("Amtrak"):
                        stop = await light_get_stop(
                            r_client, vehicle_info.stop, session
                        )
                        platform_prediction = None
                        if stop:
                            stop_id = stop.stop_id
                            if stop.long and stop.lat:
                                stop_point = Point((stop.long, stop.lat))
                                vehicle_bearing = calculate_bearing(point, stop_point)
                                long = stop.long
                                lat = stop.lat
                                if vehicle_info.speed and vehicle_info.speed >= 10:
                                    stop_eta = calculate_stop_eta(
                                        Feature(geometry=stop_point),
                                        Feature(geometry=point),
                                        vehicle_info.speed,
                                    )
                            # station_id, has_track_predictions = determine_station_id(
                            #     stop_id
                            # )
                            # if (
                            #     vehicle_info.route.startswith("CR")
                            #     and has_track_predictions
                            # ):
                            #     track_predictor = TrackPredictor(r_client=r_client)
                            #     prediction = await track_predictor.predict_track(
                            #         station_id=station_id,
                            #         route_id=vehicle_info.route,
                            #         trip_id=f"{vehicle_info.route}:{vehicle_info.id}",
                            #         headsign=vehicle_info.headsign or "",
                            #         direction_id=vehicle_info.direction_id,
                            #         scheduled_time=vehicle_info.update_time,
                            #         tg=tg,
                            #     )
                            #     if prediction:
                            #         platform_prediction = f"{prediction.track_number} ({round(prediction.confidence_score * 100)}% confidence)"
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
                        vehicle_info.route = silver_line_lookup(vehicle_info.route)
                    feature = Feature(
                        geometry=point,
                        id=vehicle_info.id,
                        properties={
                            "route": vehicle_info.route,
                            "status": vehicle_info.current_status,
                            "marker-size": "medium",
                            "marker-symbol": route_icon,
                            "marker-color": lookup_vehicle_color(vehicle_info),
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
                            "approximate_speed": vehicle_info.approximate_speed,
                            "update_time": vehicle_info.update_time.strftime(
                                "%Y-%m-%dT%H:%M:%S.000Z"
                            ),
                            "platform_prediction": platform_prediction,
                            "headsign": vehicle_info.headsign,
                        },
                    )
                    features[f"v-{vehicle_info.id}"] = feature

    return list(features.values())


async def get_shapes_features(
    config: Config, redis_client: Redis, tg: Optional[TaskGroup], session: ClientSession
) -> list[Feature]:
    """Get route shapes as GeoJSON features"""
    lines = list()
    if config.vehicles_by_route:
        shapes = await get_shapes(redis_client, config.vehicles_by_route, session, None)
        if shapes:
            # Iterate over the mapping of route -> list of line coordinate sequences
            for k, v in shapes.lines.items():
                for line in v:
                    if k.startswith("74") or k.startswith("75"):
                        k = silver_line_lookup(k)
                    lines.append(
                        Feature(
                            geometry=LineString(coordinates=line),
                            properties={"route": k},
                        )
                    )
    return lines


async def background_refresh(r_client: Redis, tg: TaskGroup):
    while True:
        await get_vehicle_features(r_client, tg)
        await sleep(2)
