import uuid

import aiohttp

MBTA_V3_ENDPOINT = "https://api-v3.mbta.com"
FOUR_WEEKS = 2419200
TWO_MONTHS = 5256000
DAY = 86400
HOUR = 3600
YEAR = 31536000
WEEK = 604800
MONTH = 2628000
TEN_MIN = 600
THIRTY_MIN = 1800
FORTY_FIVE_MIN = 2700
MINUTE = 60
INSTANCE_ID = uuid.uuid4()

# API Cache TTL configurations
VEHICLES_CACHE_TTL = 3  # seconds
ALERTS_CACHE_TTL = 10 * MINUTE  # 10 minutes
SHAPES_CACHE_TTL = 2 * WEEK  # 2 weeks

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=10)
