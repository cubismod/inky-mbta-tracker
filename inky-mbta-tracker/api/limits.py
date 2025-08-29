from slowapi import Limiter

from .core import RATE_LIMITING_ENABLED
from .middleware import NoOpLimiter, get_client_ip

# Global limiter used by route decorators
if RATE_LIMITING_ENABLED:
    limiter = Limiter(key_func=get_client_ip)
else:
    limiter = NoOpLimiter()  # type: ignore
