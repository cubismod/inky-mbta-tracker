import logging
from asyncio import CancelledError
from typing import List

import aiohttp
from config import Config
from geojson_utils import collect_alerts
from mbta_responses import AlertResource
from opentelemetry import trace
from otel_utils import add_transaction_ids_to_span
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
            span.set_attribute("alerts.fetched", len(result))
            return result
        except Exception as e:
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            raise
