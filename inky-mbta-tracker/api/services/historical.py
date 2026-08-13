import logging
from datetime import UTC, datetime

import orjson
from api.models import HistoricalSnapshot, HistoricalVehiclesResponse
from consts import HISTORICAL_V_DATA
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span
from redis.asyncio import Redis

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def _format_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


async def fetch_historical_snapshots(r_client: Redis) -> HistoricalVehiclesResponse:
    with tracer.start_as_current_span(
        "api.services.fetch_historical_snapshots"
    ) as span:
        add_transaction_ids_to_span(span)
        raw = await r_client.hgetall(HISTORICAL_V_DATA)  # type: ignore[misc]
        snapshots: list[HistoricalSnapshot] = []
        for ts_bytes, body_bytes in raw.items():
            snapshots.append(
                HistoricalSnapshot(
                    timestamp=_format_timestamp(float(ts_bytes)),
                    vehicles=orjson.loads(body_bytes),
                )
            )
        snapshots.sort(key=lambda snapshot: snapshot.timestamp)
        add_span_attributes(
            span,
            {
                "historical.snapshots.count": len(snapshots),
                "historical.fetch.status": "success",
            },
        )
        return HistoricalVehiclesResponse(snapshots=snapshots)
