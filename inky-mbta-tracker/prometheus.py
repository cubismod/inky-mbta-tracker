import logging
from urllib.parse import urlsplit

from anyio import get_running_tasks, sleep
from prometheus_client import Counter, Gauge

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

mbta_api_rate_limit_hits = Counter(
    "mbta_api_rate_limit_hits",
    "MBTA API responses that returned HTTP 429 rate limits",
    ["endpoint"],
)


def mbta_api_endpoint_label(url: str) -> str:
    path = urlsplit(url).path.strip("/")
    if not path:
        return "unknown"
    return path.split("/", maxsplit=1)[0]


def record_mbta_api_rate_limit_hit(url: str) -> None:
    mbta_api_rate_limit_hits.labels(mbta_api_endpoint_label(url)).inc()


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

alerts_counter = Counter(
    "imt_alerts", "Counts of Alerts", ["route", "severity", "effect"]
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

running_tasks = Gauge("imt_tasks", "Number of anyio tasks")


async def watch_running_tasks():
    running_tasks.set(len(get_running_tasks()))
    await sleep(15)
