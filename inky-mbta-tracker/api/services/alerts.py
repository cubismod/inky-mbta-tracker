import logging
from asyncio import CancelledError
from typing import List

import aiohttp
from config import Config
from geojson_utils import collect_alerts
from mbta_responses import AlertResource
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
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


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    stop=stop_after_attempt(3),
    retry=retry_if_not_exception_type((ValueError, CancelledError)),
    before_sleep=before_sleep_log(logger, logging.DEBUG),
    before=before_log(logger, logging.DEBUG),
)
async def fetch_alerts_with_retry(
    config: Config, session: aiohttp.ClientSession, r_client: Redis
) -> List[AlertResource]:
    """Fetch alerts with retry logic for rate limiting."""
    with tracer.start_as_current_span("api.services.fetch_alerts_with_retry") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            result = await collect_alerts(config, session, r_client)
            add_span_attributes(
                span,
                {
                    "alerts.fetched": len(result),
                    "alerts.fetch.status": "success",
                },
            )
            return result
        except Exception as e:
            set_span_error(span, e)
            add_span_attributes(span, {"error.type": type(e).__name__})
            raise
