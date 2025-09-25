from functools import wraps
from inspect import iscoroutinefunction
from typing import Awaitable, Callable, ParamSpec, TypeVar

from fastapi import Request, Response
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware

from api.core import logger


class HeaderLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log request headers for debugging and monitoring"""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        headers_to_log = {
            "user-agent": request.headers.get("user-agent"),
            "x-forwarded-for": request.headers.get("x-forwarded-for"),
            "x-real-ip": request.headers.get("x-real-ip"),
            "origin": request.headers.get("origin"),
            "authorization": "***"
            if request.headers.get("authorization")
            else None,  # Mask sensitive data
            "content-type": request.headers.get("content-type"),
            "accept": request.headers.get("accept"),
            "cf-connecting-ip": request.headers.get("cf-connecting-ip"),
            "cf-ipcountry": request.headers.get("cf-ipcountry"),
            "cf-ray": request.headers.get("cf-ray"),
            "cf-request-id": request.headers.get("cf-request-id"),
            "cf-request-priority": request.headers.get("cf-request-priority"),
        }

        # Filter out None values
        headers_to_log = {k: v for k, v in headers_to_log.items() if v is not None}

        logger.debug(
            f"{request.method} {request.url.path} from {request.client.host if request.client else 'unknown'} "
            f"- Headers: {headers_to_log}"
        )

        response = await call_next(request)
        logger.debug(f"Response status: {response.status_code}")
        return response


def get_client_ip(request: Request) -> str:
    """Get client IP, handling Cloudflare proxy headers"""
    # Check for Cloudflare's real IP header first
    if "CF-Connecting-IP" in request.headers:
        return request.headers["CF-Connecting-IP"]
    # Fallback to standard proxy headers
    if "X-Forwarded-For" in request.headers:
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    if "X-Real-IP" in request.headers:
        return request.headers["X-Real-IP"]
    # Default to remote address
    remote = get_remote_address(request)
    return remote or "unknown"


P = ParamSpec("P")
R = TypeVar("R")


class NoOpLimiter:
    """No-op limiter for when rate limiting is disabled"""

    def limit(self, rate: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            # Preserve sync/async behavior of the wrapped function
            if iscoroutinefunction(func):

                @wraps(func)
                async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[func-returns-value]
                    return await func(*args, **kwargs)  # type: ignore[misc]

                return async_wrapper  # type: ignore[return-value]
            else:

                @wraps(func)
                def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                    return func(*args, **kwargs)

                return wrapper

        return decorator
