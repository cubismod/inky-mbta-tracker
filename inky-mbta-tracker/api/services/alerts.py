import logging
from asyncio import CancelledError
from dataclasses import dataclass

import aiohttp
import orjson
from config import Config
from geojson_utils import collect_alerts
from mbta_responses import AlertResource
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from pydantic import ValidationError
from redis.asyncio import Redis
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass(frozen=True)
class AlertsResult:
    body: str
    count: int


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    stop=stop_after_attempt(3),
    retry=retry_if_not_exception_type((ValueError, CancelledError)),
    before_sleep=before_sleep_log(logger, logging.DEBUG),
    before=before_log(logger, logging.DEBUG),
)
async def fetch_alerts_with_retry(
    config: Config, session: aiohttp.ClientSession, r_client: Redis
) -> AlertsResult:
    """Fetch alerts with retry logic for rate limiting."""
    with tracer.start_as_current_span("api.services.fetch_alerts_with_retry") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            count, body = await collect_alerts(config, session, r_client)
            add_span_attributes(
                span,
                {
                    "alerts.fetched": count,
                    "alerts.fetch.status": "success",
                },
            )
            return AlertsResult(body=body, count=count)
        except Exception as e:
            set_span_error(span, e)
            add_span_attributes(span, {"error.type": type(e).__name__})
            raise


async def fetch_bus_alerts(r_client: Redis, route_id: str) -> AlertsResult:
    """Fetch alerts for a single bus route from Redis sets populated by SSE watchers."""
    with tracer.start_as_current_span("api.services.fetch_bus_alerts") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "route.id": route_id,
                "route.type": "bus",
            },
        )
        ids = await r_client.smembers(f"alerts:route:{route_id}")  # type: ignore[misc]
        add_span_attributes(span, {"alerts.ids.count": len(ids)})
        if not ids:
            return AlertsResult(body=orjson.dumps({"data": []}).decode(), count=0)

        pl = r_client.pipeline()
        for raw in ids:
            alert_id = (
                raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
            )
            pl.get(f"alert:{alert_id}")
        results = await pl.execute()

        alerts: list[dict] = []
        for raw in results:
            if not raw:
                continue
            try:
                AlertResource.model_validate_json(raw, strict=False)
            except ValidationError as err:
                logger.error("Unable to validate bus alert", exc_info=err)
                continue
            alerts.append(orjson.loads(raw))

        add_span_attributes(span, {"alerts.count": len(alerts)})
        body = orjson.dumps({"data": alerts}).decode("utf-8")
        return AlertsResult(body=body, count=len(alerts))
