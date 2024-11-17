from prometheus_client import Counter

api_events = Counter(
    "imt_api_events", "Processed API Events", ["action", "route_id", "stop"]
)
