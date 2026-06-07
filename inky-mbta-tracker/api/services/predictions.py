import logging
import os
from dataclasses import dataclass
from urllib.parse import urlencode

import aiohttp
from mbta_rate_limiter import rate_limited_get
from mbta_responses import Predictions
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from prometheus import mbta_api_requests
from pydantic import ValidationError
from redis.asyncio import Redis

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
        endpoint = f"/predictions?{urlencode(params)}"

        try:
            async with rate_limited_get(session, r_client, endpoint) as response:
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
