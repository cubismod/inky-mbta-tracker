import logging
import os
from typing import Optional

import aiohttp
from consts import AIOHTTP_TIMEOUT
from prometheus_client import Counter, Gauge
from pydantic import ValidationError
from shared_types.shared_types import PrometheusAPIResponse

logger = logging.getLogger(__name__)

schedule_events = Counter(
    "imt_schedule_events", "Processed Schedule Events", ["action", "route_id", "stop"]
)

vehicle_events = Counter(
    "imt_vehicle_events", "Any updates to vehicles", ["action", "route_id"]
)

vehicle_speeds = Gauge(
    "imt_vehicle_speeds", "Vehicle speeds", ["route_id", "vehicle_id"]
)

tracker_executions = Counter("imt_tracker_executions", "Tracker Executions", ["stop"])

mbta_api_requests = Gauge(
    "mbta_api_requests", "Requests we are making to the MBTA API", ["endpoint"]
)

running_threads = Gauge("imt_active_threads", "Active Threads")

redis_commands = Gauge("imt_redis_cmds", "Redis commands made", ["name"])

schema_key_counts = Gauge(
    "imt_schema_key_counts",
    "Number of keys in each Redis schema namespace",
    ["schema_id"],
)

current_buffer_used = Gauge(
    "imt_current_buffer_used", "number of items stored in the buffer", ["name"]
)
max_buffer_size = Gauge(
    "imt_max_buffer_size",
    "maximum number of items that can be stored on this stream (or math.inf)",
    ["name"],
)
open_receive_streams = Gauge(
    "imt_open_receive_streams",
    "number of unclosed clones of the receive stream",
    ["name"],
)
open_send_streams = Gauge(
    "imt_open_send_streams", "number of unclosed clones of the send stream", ["name"]
)
tasks_waiting_receive = Gauge(
    "imt_tasks_waiting_receive",
    "number of tasks blocked on MemoryObjectReceiveStream.receive()",
    ["name"],
)
tasks_waiting_send = Gauge(
    "imt_tasks_waiting_send",
    "number of tasks blocked on MemoryObjectSendStream.send()",
    ["name"],
)

server_side_events = Counter(
    "imt_server_side_events", "Number of server-side events", ["id"]
)

vehicle_batch_items = Gauge(
    "imt_vehicle_batch_items", "Vehicles processed in last batch", ["name"]
)

schedule_batch_items = Gauge(
    "imt_schedule_batch_items", "Schedule events processed in last batch", ["name"]
)

batch_flushes = Counter(
    "imt_batch_flushes", "Batch flushes by outcome", ["name", "outcome"]
)

last_vehicle_write_ts = Gauge(
    "imt_last_vehicle_write_ts_seconds",
    "Epoch seconds of last vehicle write",
    ["name"],
)

last_batch_flush_ts = Gauge(
    "imt_last_batch_flush_ts_seconds",
    "Epoch seconds of last batch flush",
    ["name"],
)

pos_data_count = Gauge("imt_pos_data_count", "Count of members in pos-data", ["name"])


async def query_server_side_events(
    session: aiohttp.ClientSession, job: str, id: str
) -> Optional[PrometheusAPIResponse]:
    metric = os.getenv("IMT_METRIC_NAME", "mbta_server_side_events:rate30m")
    prom_query = f'{metric}{{job="{job}",id="{id}"}}'
    try:
        resp = await session.get(
            f"{os.getenv('IMT_PROMETHEUS_ENDPOINT')}/api/v1/query",
            params={"query": prom_query},
            timeout=AIOHTTP_TIMEOUT,
        )
        resp_json = await resp.json()
        logger.debug(resp_json)
        prom_resp = PrometheusAPIResponse.model_validate(resp_json)
        logger.debug(f"Prometheus response: {prom_resp.model_dump_json()}")
        return prom_resp

    except ValidationError as err:
        logger.error("Unable to parse response", exc_info=err)
    except (
        aiohttp.ClientError,
        TimeoutError,
        ValueError,
        OSError,
        RuntimeError,
    ) as err:
        logger.error("Failed to query Prometheus", exc_info=err)
