# client that keeps track of events on the stops specified
import json
import logging
import os
from datetime import datetime
from queue import Queue

import httpx
import sseclient
from expiring_dict import ExpiringDict
from mbta_responses import (
    Route,
    RouteResource,
    ScheduleAttributes,
    ScheduleResource,
    Schedules,
    TripResource,
    Trips,
)
from pydantic import ValidationError
from schedule_tracker import ScheduleEvent

auth_token = os.environ.get("AUTH_TOKEN")
mbta_v3 = "https://api-v3.mbta.com"
thirty_hours = 108000


def with_httpx(url, headers):
    """Get a streaming response for the given event feed using httpx."""
    import httpx

    with httpx.stream("GET", url, headers=headers) as s:
        # Note: 'yield from' is Python >= 3.3. Use for/yield instead if you
        # are using an earlier version.
        yield from s.iter_bytes()


class Watcher:
    stop_id: str
    route: str | None
    direction: int | None
    trips: ExpiringDict[str, TripResource]
    predictions: ExpiringDict[str, ScheduleResource]
    routes: dict[str, RouteResource]

    def __init__(self, stop_id: str, route: str | None, direction: int | None):
        self.stop_id = stop_id
        self.route = route
        self.direction = direction
        self.trips = ExpiringDict[str, TripResource](thirty_hours)
        self.predictions = ExpiringDict[str, ScheduleResource](thirty_hours)
        self.routes = dict[str, RouteResource]()

    @staticmethod
    def determine_time(attributes: ScheduleAttributes) -> datetime:
        if attributes.arrival_time:
            return datetime.fromisoformat(attributes.arrival_time)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time)

    def get_headsign(self, trip_id: str):
        if trip_id in self.trips:
            return self.trips[trip_id].attributes.headsign
        return ""

    def parse_station_response(
        self, data, event_type: str, queue: Queue[ScheduleEvent]
    ):
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            schedule = Schedules.model_validate_json(json.loads(data))
            schedule_entries = list[ScheduleResource]

            # capture all predictions first and save all the trip info
            for item in schedule.data:
                trip_id = item.relationships.trip.data.id
                if trip_id not in self.trips:
                    self.save_trip(trip_id)

                route_id = item.relationships.route.data.id
                if route_id not in self.routes:
                    self.save_route(route_id)

                if item.type == "prediction":
                    self.predictions[item.id] = item
                else:
                    schedule_entries += item

            # and then iterate through station data to send to the queue
            for item in schedule_entries:
                schedule_time = self.determine_time(item.attributes)
                route_id = item.relationships.route.data.id
                headsign = self.get_headsign(item.relationships.trip.data.id)

                if item.relationships.prediction.data is not None:
                    prediction = self.predictions[item.relationships.prediction.data.id]
                    schedule_time = self.determine_time(prediction.attributes)
                queue.put(
                    ScheduleEvent(
                        action=event_type,
                        time=schedule_time,
                        route_id=route_id,
                        headsign=headsign,
                    )
                )

        except ValidationError as err:
            logging.error(f"Unable to parse schedule, {err}")
        except KeyError as err:
            logging.error(f"Could not find prediction, {err}")

    def save_trip(self, trip_id: str):
        try:
            response = httpx.get(
                f"{mbta_v3}/trips&filter[id]={trip_id}&api_key={auth_token}"
            )

            trip = Trips.model_validate_json(response.json())
            for tr in trip.data:
                self.trips[trip_id] = tr

        except httpx.RequestError as exc:
            logging.error(f"Unable to GET MBTA API, {exc}")
        except ValidationError as err:
            logging.error(f"Unable to parse trip, {err}")

    def save_route(self, route_id: str):
        try:
            response = httpx.get(
                f"{mbta_v3}/routes&filter[id]={route_id}&api_key={auth_token}"
            )

            route = Route.model_validate_json(response.json())
            self.routes[route_id] = route.data

        except httpx.RequestError as exc:
            logging.error(f"Unable to GET MBTA API, {exc}")
        except ValidationError as err:
            logging.error(f"Unable to parse route, {err}")


def watch_station(
    stop_id: str, route: str | None, direction: int | None, queue: Queue[ScheduleEvent]
):
    now = datetime.now()
    endpoint = f"{mbta_v3}/schedules&include=prediction&filter[min_time]={now.hour}:{now.minute}&filter[stop]={stop_id}&sort=time&api_key={auth_token}"
    if route:
        endpoint += f"&filter[route]={route}"
    if direction:
        endpoint += f"&filter[direction_id]={direction}"
    headers = {"Accept": "text/event-stream"}
    watcher = Watcher(stop_id, route, direction)

    response = with_httpx(endpoint, headers)
    client = sseclient.SSEClient(response)
    for event in client.events():
        watcher.parse_station_response(event.data, event.event, queue)
