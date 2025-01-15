from prometheus_client import Counter, Gauge

schedule_events = Counter(
    "imt_schedule_events", "Processed Schedule Events", ["action", "route_id", "stop"]
)

vehicle_events = Counter(
    "imt_vehicle_events", "Any updates to vehicles", ["action", "route_id", "id"]
)

tracker_executions = Counter("imt_tracker_executions", "Tracker Executions", ["stop"])

mbta_api_requests = Gauge(
    "mbta_api_requests", "Requests we are making to the MBTA API", ["endpoint"]
)

running_threads = Gauge("imt_active_threads", "Active Threads")
