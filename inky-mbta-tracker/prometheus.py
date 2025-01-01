from prometheus_client import Counter, Gauge

schedule_events = Counter(
    name="imt_schedule_events",
    documentation="Total MBTA Schedule Events that have been processed",
    labelnames=["action", "route_id", "stop"],
)

tracker_executions = Counter(
    name="imt_tracker_executions",
    documentation="Total executions of each MBTA tracker task",
    labelnames=["stop"],
)

mbta_api_requests = Gauge(
    name="mbta_api_requests",
    documentation="Total requests made to the MBTA API",
    labelnames=["endpoint"],
)
