import os

from aiohttp import ClientSession
from api.models import RouteStopAttributes, RouteStopsResponse
from mbta_rate_limiter import rate_limited_get
from prometheus import mbta_api_requests
from redis.asyncio import Redis

MBTA_AUTH = os.environ.get("AUTH_TOKEN")


class StopsUpstreamError(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"MBTA API returned HTTP {status_code}")


async def fetch_route_stops(
    session: ClientSession, r_client: Redis, route_id: str
) -> RouteStopsResponse:
    params = {
        "filter[route]": route_id,
        "fields[stop]": ",".join(RouteStopAttributes.model_fields),
    }
    if MBTA_AUTH:
        params["api_key"] = MBTA_AUTH
    async with rate_limited_get(session, r_client, "/stops", params=params) as response:
        if response.status != 200:
            raise StopsUpstreamError(response.status)
        mbta_api_requests.labels("stops").inc()
        return RouteStopsResponse.model_validate_json(await response.text())
