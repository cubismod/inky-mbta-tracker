"""
Transaction ID middleware for FastAPI.

Automatically generates user query transaction IDs for all incoming requests
and ensures they propagate through the request lifecycle.
"""

import logging
import time
from typing import Awaitable, Callable

from fastapi import Request, Response
from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class TransactionIDMiddleware(BaseHTTPMiddleware):
    """Middleware to automatically generate and manage transaction IDs for user queries."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """
        Generate a user query transaction ID for each request and ensure it propagates.

        Args:
            request: FastAPI request object
            call_next: Next middleware/handler in chain

        Returns:
            Response from the next handler with transaction context
        """
        # Import here to avoid circular imports during startup
        from otel_utils import (
            add_span_attributes,
            add_transaction_ids_to_span,
            set_span_error,
            set_user_query_transaction_id,
        )

        # Generate transaction ID based on the request path
        endpoint = f"{request.method}_{request.url.path}".replace("/", "_")
        start_time = time.perf_counter()
        current_span = trace.get_current_span()

        try:
            # Set the user query transaction ID for this request
            txn_id = set_user_query_transaction_id(endpoint)
            route = request.scope.get("route")
            route_path = getattr(route, "path", request.url.path)
            add_transaction_ids_to_span(current_span)
            add_span_attributes(
                current_span,
                {
                    "http.request.method": request.method,
                    "http.route": route_path,
                    "url.path": request.url.path,
                    "url.query.present": bool(request.url.query),
                    "client.address": request.client.host
                    if request.client
                    else "unknown",
                    "http.request.header.user_agent.present": bool(
                        request.headers.get("user-agent")
                    ),
                    "http.request.header.cf_ray.present": bool(
                        request.headers.get("cf-ray")
                    ),
                    "transaction.user_query.id": txn_id,
                },
            )

            logger.debug(
                f"Set user query transaction ID: {txn_id} for {request.method} {request.url.path}"
            )

            # Add transaction ID to request state for potential use by endpoints
            request.state.user_query_txn_id = txn_id

            # Process the request with transaction context
            response = await call_next(request)
            add_span_attributes(
                current_span,
                {
                    "http.response.status_code": response.status_code,
                    "http.request.duration_ms": round(
                        (time.perf_counter() - start_time) * 1000, 2
                    ),
                },
            )

            # Optionally add transaction ID to response headers for client correlation
            response.headers["X-User-Query-Transaction-ID"] = txn_id

            return response

        except Exception as e:
            logger.error(f"Error in TransactionIDMiddleware: {e}")
            set_span_error(current_span, e)
            add_span_attributes(
                current_span,
                {
                    "http.request.duration_ms": round(
                        (time.perf_counter() - start_time) * 1000, 2
                    ),
                },
            )
            # On error, still process the request but without transaction context
            return await call_next(request)
        finally:
            # Clean up transaction context after request is complete
            # Note: Only clear user query context, preserve route monitor and vehicle track contexts
            # as they may span multiple requests
            try:
                from otel_utils import user_query_txn_context

                user_query_txn_context.set(None)
            except ImportError:
                pass
