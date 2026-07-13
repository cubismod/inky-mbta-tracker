import logging
import os
from dataclasses import dataclass
from datetime import datetime

import aiohttp
import anyio
from consts import MBTA_V3_ENDPOINT
from mbta_rate_limiter import rate_limited_get
from mbta_responses import Predictions
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from prometheus import mbta_api_requests
from pydantic import ValidationError
from redis.asyncio import Redis
from redis_cache import get_cache, write_cache

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
MBTA_AUTH = os.environ.get("AUTH_TOKEN")


@dataclass(frozen=True)
class PredictionsResult:
    body: str
    count: int


class MBTAUpstreamError(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"MBTA API returned HTTP {status_code}")


async def fetch_predictions(
    session: aiohttp.ClientSession,
    r_client: Redis,
    trip_id: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    radius: float | None = None,
) -> PredictionsResult:
    with tracer.start_as_current_span("api.services.fetch_predictions") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "trip.id": trip_id,
                "filter.latitude": latitude is not None,
                "filter.longitude": longitude is not None,
                "filter.radius": radius is not None,
                "mbta.endpoint": "predictions",
                "session.closed": session.closed,
            },
        )

        if session.closed:
            raise ConnectionError("MBTA API session is closed")

        params: dict[str, str] = {}
        if trip_id is not None:
            params["filter[trip]"] = trip_id
        if latitude is not None:
            params["filter[latitude]"] = str(latitude)
        if longitude is not None:
            params["filter[longitude]"] = str(longitude)
        if radius is not None:
            params["filter[radius]"] = str(radius)
        if MBTA_AUTH:
            params["api_key"] = MBTA_AUTH
        endpoint = "/predictions"
        try:
            async with rate_limited_get(
                session, r_client, endpoint, params=params
            ) as response:
                add_span_attributes(span, {"http.status_code": response.status})
                if response.status != 200:
                    raise MBTAUpstreamError(response.status)

                body = await response.text()
                mbta_api_requests.labels("predictions").inc()
                predictions = Predictions.model_validate_json(body, strict=False)
                add_span_attributes(
                    span,
                    {
                        "predictions.count": len(predictions.data),
                        "predictions.fetch.status": "success",
                    },
                )
                return PredictionsResult(body=body, count=len(predictions.data))
        except (
            ValidationError,
            MBTAUpstreamError,
            ConnectionError,
            TimeoutError,
        ) as exc:
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": type(exc).__name__})
            raise


async def batch_fetch_trip_predictions(
    session: aiohttp.ClientSession | None,
    r_client: Redis,
    stop_ids: list[str],
) -> dict[tuple[str, str], datetime]:
    span = trace.get_current_span()
    unique_stops = list(set(stop_ids))
    add_span_attributes(
        span,
        {
            "stop_ids.count": len(unique_stops),
        },
    )

    if not unique_stops:
        return {}

    results: dict[tuple[str, str], datetime] = {}
    limiter = anyio.CapacityLimiter(5)

    async def fetch_single(client: aiohttp.ClientSession, stop_id: str) -> None:
        cache_key = f"prediction:stop:{stop_id}"
        cached = await get_cache(r_client, cache_key)
        if cached is not None:
            try:
                predictions = Predictions.model_validate_json(cached, strict=False)
            except ValidationError:
                logger.warning(
                    "Failed to parse cached predictions for stop %s",
                    stop_id,
                    exc_info=True,
                )
            else:
                for pred in predictions.data:
                    trip_data = pred.relationships.trip.data
                    stop_data = pred.relationships.stop.data
                    if trip_data is None or stop_data is None:
                        continue
                    arrival_time = pred.attributes.arrival_time
                    if arrival_time:
                        results[(trip_data.id, stop_data.id)] = datetime.fromisoformat(
                            arrival_time
                        )
                return

        params = [("api_key", MBTA_AUTH), ("filter[stop]", stop_id)]
        async with limiter:
            try:
                async with rate_limited_get(
                    client, r_client, "/predictions", params=params
                ) as response:
                    if response.status != 200:
                        logger.warning(
                            "MBTA API returned HTTP %d for stop %s",
                            response.status,
                            stop_id,
                        )
                        return
                    body = await response.text()
                    await write_cache(r_client, cache_key, body, 30)
                    predictions = Predictions.model_validate_json(body, strict=False)
            except ValidationError:
                logger.warning(
                    "Failed to parse predictions for stop %s",
                    stop_id,
                    exc_info=True,
                )
                return
            except Exception:
                logger.warning(
                    "Failed to fetch predictions for stop %s",
                    stop_id,
                    exc_info=True,
                )
                return

            for pred in predictions.data:
                trip_data = pred.relationships.trip.data
                stop_data = pred.relationships.stop.data
                if trip_data is None or stop_data is None:
                    continue
                arrival_time = pred.attributes.arrival_time
                if arrival_time:
                    results[(trip_data.id, stop_data.id)] = datetime.fromisoformat(
                        arrival_time
                    )

    async def run_with_session(client: aiohttp.ClientSession) -> None:
        async with anyio.create_task_group() as tg:
            for stop_id in unique_stops:
                tg.start_soon(fetch_single, client, stop_id)

    if session is not None:
        await run_with_session(session)
    else:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as local_session:
            await run_with_session(local_session)

    add_span_attributes(
        span,
        {
            "predictions.fetched": len(results),
            "predictions.fetch.status": "success" if results else "empty",
        },
    )

    return results
