from functools import wraps
from typing import Any, Awaitable, Callable

from fastapi import Request, Response
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from vehicles_background_worker import State

from .core import VEHICLES_QUEUE, logger


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
    return get_remote_address(request)


class NoOpLimiter:
    """No-op limiter for when rate limiting is disabled"""

    def limit(self, rate: str) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            return wrapper

        return decorator


class TrafficMonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to queue TRAFFIC state messages for background vehicle data processing"""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # Skip queuing for health/metrics endpoints
        if request.url.path not in ("/health", "/metrics"):
            try:
                VEHICLES_QUEUE.put_nowait(State.TRAFFIC)
                logger.debug("Queued TRAFFIC state for background worker")
            except Exception:
                logger.debug("Vehicles queue full; skipping TRAFFIC enqueue")

        response = await call_next(request)
        return response
