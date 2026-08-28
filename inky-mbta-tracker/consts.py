MBTA_V3_ENDPOINT = "https://api-v3.mbta.com"
TWO_MONTHS = 5256000
DAY = 86400
HOUR = 3600
YEAR = 31536000
WEEK = 604800
MINUTE = 60
# SSE connections that deliver no events for this long are presumed half-open and
# reconnected; MBTA streams emit events/keepalives far more often than this
SSE_INACTIVITY_TIMEOUT = 30 * MINUTE
ALERTS_SET_KEY = "alerts:stats"
LIVE_NEGATIVE_CACHE_KEY = "live_negative_cache"
VEHICLE_STREAM_KEY = "vehicle_stream_diff"
