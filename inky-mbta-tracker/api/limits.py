from slowapi import Limiter
from utils import create_redis_url

from .core import RATE_LIMITING_ENABLED
from .middleware.header_middleware import NoOpLimiter, get_client_ip

# Global limiter used by route decorators
if RATE_LIMITING_ENABLED:
    limiter = Limiter(
        key_func=get_client_ip,
        default_limits=["300/min"],
        storage_uri=create_redis_url(),
    )
else:
    limiter = NoOpLimiter()  # type: ignore
