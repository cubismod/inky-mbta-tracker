from prometheus_client import Counter, Gauge

schedule_events = Counter(
    "imt_schedule_events", "Processed Schedule Events", ["action", "route_id", "stop"]
)

mbta_api_requests = Gauge(
    "mbta_api_requests", "Requests we are making to the MBTA API", ["endpoint"]
)
