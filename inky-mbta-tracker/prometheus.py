from prometheus_client import Counter, Gauge

schedule_events = Counter(
    "imt_schedule_events", "Processed Schedule Events", ["action", "route_id", "stop"]
)

tracked_events = Gauge("imt_tracked_events", "Cached schedule events")

mbta_api_requests = Gauge(
    "mbta_api_requests", "Requests we are making to the MBTA API", ["endpoint"]
)
