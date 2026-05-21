"""
Simple function-style cache middleware factory for FastAPI/Starlette.

Usage:
    from api.middleware.cache_middleware import create_cache_middleware
    app.middleware("http")(create_cache_middleware(cache_methods={"GET"}, default_ttl=5))

This middleware is typically added to the FastAPI app with the function-style
middleware API (see FastAPI docs). It will check for a Redis client attached to
`app.state.r_client` and is a no-op if that client is not present.

Behavior:
- Only uses a Redis client exposed on `app.state.r_client`. If that attribute is missing,
  the middleware becomes a no-op and passes requests through.
- By default only caches GET responses.
- Only caches successful (HTTP 200) responses.
- Uses the repository's existing cache helpers `get_cache` and `write_cache`.
- Cache payloads are stored as UTF-8 strings (most API responses are JSON).

Per-endpoint usage examples
---------------------------
You can restrict caching to specific endpoints (or exclude specific endpoints)
by using the `include_paths` and `exclude_paths` parameters when creating the
middleware. Paths are matched exactly against `request.url.path`.

Example 1 — cache only specific endpoints:
    from fastapi import FastAPI
    from api.middleware.cache_middleware import create_cache_middleware

    app = FastAPI()
    # Only cache GET requests to /api/predictions and /api/stats
    app.middleware("http")(
        create_cache_middleware(
            cache_methods={"GET"},
            default_ttl=5,
            include_paths={"/api/predictions", "/api/stats"},
        )
    )

Example 2 — exclude a specific endpoint from global caching:
    from fastapi import FastAPI
    from api.middleware.cache_middleware import create_cache_middleware

    app = FastAPI()
    # Cache all GET requests except /api/health
    app.middleware("http")(
        create_cache_middleware(
            cache_methods={"GET"},
            default_ttl=5,
            exclude_paths={"/api/health"},
        )
    )

Notes:
- include_paths and exclude_paths apply to exact path matches (no wildcarding).
  If you need prefix matching you can check `request.url.path.startswith(...)`
  in a custom wrapper around this middleware factory.
- The middleware must be added at app startup (e.g., in your app factory).
"""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
from typing import Awaitable, Callable, Optional, ParamSpec, Set, TypeVar

from fastapi import Request, Response
from opentelemetry import trace
from otel_utils import add_cache_key_attributes, add_span_attributes
from redis.asyncio import Redis

# Use the project's cache helpers (they operate on redis.asyncio.Redis)
from redis_cache import get_cache, write_cache

logger = logging.getLogger("api.cache_middleware")


def _make_key(method: str, path: str, query: str, body: Optional[bytes]) -> str:
    """Create a compact deterministic cache key for a request."""
    base = f"api_cache:{method}:{path}"
    if query:
        base = f"{base}?{query}"
    if body:
        h = hashlib.sha256(body).hexdigest()
        base = f"{base}:body:{h}"
    return base


P = ParamSpec("P")
R = TypeVar("R")


def cache_ttl(seconds: int) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to attach a per-endpoint cache TTL.

    Usage:
        @router.get("/foo")
        @cache_ttl(30)
        async def endpoint(...):
            ...
    The middleware will inspect `request.scope["endpoint"]` for `_cache_ttl`.
    """

    def _decorator(fn: Callable[P, R]) -> Callable[P, R]:
        setattr(fn, "_cache_ttl", int(seconds))
        return fn

    return _decorator


def _ttl_for_request(request: Request, response: Response, default_ttl: int) -> int:
    """Determine TTL precedence: response header > endpoint decorator > default."""
    # 1) Response header override
    hdr = response.headers.get("X-Cache-TTL", None)
    if hdr:
        try:
            val = int(hdr)
            if val >= 0:
                return val
        except Exception:
            pass

    # 2) Endpoint decorator attribute
    endpoint = request.scope.get("endpoint")
    if endpoint is not None:
        ttl_attr = getattr(endpoint, "_cache_ttl", None)
        if ttl_attr is not None:
            try:
                val = int(ttl_attr)
                if val >= 0:
                    return val
            except Exception:
                pass

    # Fallback
    return int(default_ttl)


def _ttl_with_source(
    request: Request, response: Response, default_ttl: int
) -> tuple[int, str]:
    """Determine TTL and identify which policy supplied it."""
    hdr = response.headers.get("X-Cache-TTL", None)
    if hdr:
        try:
            val = int(hdr)
            if val >= 0:
                return val, "response_header"
        except Exception:
            pass

    endpoint = request.scope.get("endpoint")
    if endpoint is not None:
        ttl_attr = getattr(endpoint, "_cache_ttl", None)
        if ttl_attr is not None:
            try:
                val = int(ttl_attr)
                if val >= 0:
                    return val, "endpoint_decorator"
            except Exception:
                pass

    return int(default_ttl), "default"


def create_cache_middleware(
    cache_methods: Optional[Set[str]] = None,
    default_ttl: int = 5,
    include_paths: Optional[Set[str]] = None,
    exclude_paths: Optional[Set[str]] = None,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    """
    Return a function-style middleware callable for FastAPI.

    Parameters
    - cache_methods: set of HTTP methods to cache (defaults to {"GET"})
    - default_ttl: seconds to cache successful responses (defaults to 5)
    - include_paths: optional set of exact paths to include (if provided only these paths are cached)
    - exclude_paths: optional set of exact paths to exclude from caching

    Notes:
    - The middleware looks for a redis client at `request.app.state.r_client`.
      If it's not present, the middleware simply forwards the request without caching.
    """

    # Normalize inputs
    cache_methods = {m.upper() for m in (cache_methods or {"GET"})}
    # Accept glob-style patterns for include/exclude and keep them as ordered lists
    include_patterns = list(include_paths) if include_paths else None
    exclude_patterns = list(exclude_paths) if exclude_paths else None

    r_client = Redis().from_url(
        f"redis://:{os.environ.get('IMT_REDIS_PASSWORD', '')}@{os.environ.get('IMT_REDIS_ENDPOINT', '')}:{int(os.environ.get('IMT_REDIS_PORT', '6379'))}"
    )

    async def middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        method = request.method.upper()
        span = trace.get_current_span()

        # Path-based include/exclude handling for per-endpoint caching with glob support
        path = request.url.path
        if include_patterns is not None:
            # include_patterns are glob patterns; only cache if any pattern matches
            matched = any(fnmatch.fnmatch(path, pat) for pat in include_patterns)
            if not matched:
                add_span_attributes(
                    span,
                    {
                        "api.cache.eligible": False,
                        "api.cache.skip_reason": "include_path_miss",
                    },
                )
                # No include pattern matched; pass through
                return await call_next(request)
        if exclude_patterns is not None:
            # If any exclude pattern matches, skip caching
            if any(fnmatch.fnmatch(path, pat) for pat in exclude_patterns):
                add_span_attributes(
                    span,
                    {
                        "api.cache.eligible": False,
                        "api.cache.skip_reason": "exclude_path_match",
                    },
                )
                return await call_next(request)

        if method not in cache_methods:
            add_span_attributes(
                span,
                {
                    "api.cache.eligible": False,
                    "api.cache.skip_reason": "method_not_cacheable",
                },
            )
            return await call_next(request)

        # Read request body to include in key when present. For GET this is cheap/empty.
        try:
            body = await request.body()
        except Exception:
            body = b""

        key = _make_key(method, path, request.url.query, body if body else None)
        add_span_attributes(
            span,
            {
                "api.cache.eligible": True,
                "api.cache.method": method,
                "api.cache.query.present": bool(request.url.query),
                "api.cache.request_body.bytes": len(body),
            },
        )
        add_cache_key_attributes(span, key, attr_prefix="api.cache.key")

        # Try cache read
        try:
            cached = await get_cache(r_client, key)
        except Exception:
            logger.debug("Cache middleware: error reading cache", exc_info=True)
            add_span_attributes(
                span,
                {
                    "api.cache.read_error": True,
                    "api.cache.hit": False,
                },
            )
            cached = None

        if cached is not None:
            try:
                cached_bytes = cached.encode("utf-8")
            except Exception:
                cached_bytes = str(cached).encode("utf-8")
            add_span_attributes(
                span,
                {
                    "api.cache.hit": True,
                    "api.cache.response_body.bytes": len(cached_bytes),
                },
            )
            headers = {"content-length": str(len(cached_bytes))}
            headers.pop("transfer-encoding", None)
            headers.pop("Transfer-Encoding", None)
            return Response(
                content=cached_bytes, media_type="application/json", headers=headers
            )

        # No cached response; call downstream and capture response body.
        add_span_attributes(span, {"api.cache.hit": False})
        response = await call_next(request)

        chunks = []
        async for chunk in response.body_iterator:  # type: ignore
            chunks.append(chunk)
        response_body = b"".join(chunks)
        add_span_attributes(
            span,
            {
                "api.cache.downstream_status_code": response.status_code,
                "api.cache.response_body.bytes": len(response_body),
            },
        )
        # Cache only successful responses with non-empty body
        if response.status_code == 200 and response_body:
            try:
                # TTL precedence: response header > endpoint decorator > default_ttl
                ttl, ttl_source = _ttl_with_source(request, response, default_ttl)
                add_span_attributes(
                    span,
                    {
                        "api.cache.write_attempted": True,
                        "api.cache.ttl_seconds": ttl,
                        "api.cache.ttl_source": ttl_source,
                    },
                )
                await write_cache(r_client, key, response_body.decode("utf-8"), ttl)
            except Exception:
                logger.debug("Cache middleware: failed to write cache", exc_info=True)
                add_span_attributes(span, {"api.cache.write_error": True})
        else:
            add_span_attributes(
                span,
                {
                    "api.cache.write_attempted": False,
                    "api.cache.skip_reason": "non_200_or_empty_body",
                },
            )

        # Recreate and return a Response since .body_iterator may have been consumed
        headers = dict(response.headers)
        # Remove any existing Content-Length/Transfer-Encoding to avoid mismatches after modifying body
        headers.pop("content-length", None)
        headers.pop("Content-Length", None)
        headers.pop("transfer-encoding", None)
        headers.pop("Transfer-Encoding", None)
        # Set correct Content-Length for the new body
        try:
            content_length = str(len(response_body))
        except Exception:
            content_length = "0"
        headers["content-length"] = content_length
        media_type = response.media_type or headers.get(
            "content-type", "application/json"
        )
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=headers,
            media_type=media_type,
        )

    return middleware
