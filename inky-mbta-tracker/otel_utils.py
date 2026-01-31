"""
OpenTelemetry utilities for trace context propagation and span management.

This module provides helpers for working with OTEL in anyio concurrent contexts,
including context serialization for queue-based trace propagation, decorators
for common tracing patterns, and business transaction ID management.
"""

import functools
import json
import logging
import time
import uuid
from contextvars import ContextVar
from typing import Any, Callable, Optional, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span, SpanKind, Status, StatusCode

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# Context variables for business transaction IDs
route_monitor_txn_context: ContextVar[Optional[str]] = ContextVar(
    "route_monitor_txn_id", default=None
)
vehicle_track_txn_context: ContextVar[Optional[str]] = ContextVar(
    "vehicle_track_txn_id", default=None
)
user_query_txn_context: ContextVar[Optional[str]] = ContextVar(
    "user_query_txn_id", default=None
)

# Transaction ID attribute keys (Sentry-compatible naming)
ROUTE_MONITOR_TXN_ATTR = "transaction.business.route_monitor"
VEHICLE_TRACK_TXN_ATTR = "transaction.business.vehicle_track"
USER_QUERY_TXN_ATTR = "transaction.business.user_query"


def serialize_trace_context() -> Optional[str]:
    """
    Serialize the current trace context to a JSON string.

    This allows trace context to be stored in Redis or passed through
    memory object streams to maintain trace continuity across anyio tasks.

    Returns:
        JSON string containing trace context, or None if no active span
    """
    try:
        span = trace.get_current_span()
        if not span or not span.is_recording():
            return None

        span_context = span.get_span_context()
        if not span_context.is_valid:
            return None

        context_dict = {
            "trace_id": format(span_context.trace_id, "032x"),
            "span_id": format(span_context.span_id, "016x"),
            "trace_flags": span_context.trace_flags,
            "trace_state": (
                str(span_context.trace_state) if span_context.trace_state else None
            ),
        }

        # Include current transaction IDs in serialized context
        txn_ids = get_current_transaction_ids()
        if any(txn_ids.values()):
            context_dict["transaction_ids"] = {
                k: v for k, v in txn_ids.items() if v is not None
            }

        return json.dumps(context_dict)
    except Exception as e:
        logger.debug(f"Failed to serialize trace context: {e}")
        return None


def deserialize_trace_context(context_json: Optional[str]) -> Optional[Context]:
    """
    Deserialize trace context from JSON string.

    Args:
        context_json: JSON string containing trace context

    Returns:
        OpenTelemetry Context object, or None if deserialization fails
    """
    if not context_json:
        return None

    try:
        context_dict = json.loads(context_json)
        trace_id = int(context_dict["trace_id"], 16)
        span_id = int(context_dict["span_id"], 16)
        trace_flags = context_dict["trace_flags"]

        span_context = trace.SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=True,
            trace_flags=trace_flags,
        )

        # Restore transaction IDs if present
        if "transaction_ids" in context_dict:
            restore_transaction_context(context_dict["transaction_ids"])

        # Create a context with this span context
        ctx = trace.set_span_in_context(trace.NonRecordingSpan(span_context))
        return ctx
    except Exception as e:
        logger.debug(f"Failed to deserialize trace context: {e}")
        return None


def create_span_link_from_context(context_json: Optional[str]) -> Optional[Link]:
    """
    Create a span link from serialized trace context.

    Links are useful for connecting spans that don't have a strict parent-child
    relationship, which is common in anyio concurrent task patterns.

    Args:
        context_json: JSON string containing trace context

    Returns:
        Span Link object, or None if creation fails
    """
    if not context_json:
        return None

    try:
        context_dict = json.loads(context_json)
        trace_id = int(context_dict["trace_id"], 16)
        span_id = int(context_dict["span_id"], 16)
        trace_flags = context_dict["trace_flags"]

        span_context = trace.SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=True,
            trace_flags=trace_flags,
        )
        return Link(span_context)
    except Exception as e:
        logger.debug(f"Failed to create span link: {e}")
        return None


# Transaction ID Management Functions


def generate_transaction_id(
    txn_type: str, context_info: str = "", include_timestamp: bool = True
) -> str:
    """
    Generate a structured transaction ID for business operations.

    Args:
        txn_type: Transaction type prefix (e.g., 'route_monitor', 'vehicle_track', 'user_query')
        context_info: Context-specific information (e.g., route_id, vehicle_id, endpoint)
        include_timestamp: Whether to include timestamp in the ID

    Returns:
        Formatted transaction ID string
    """
    uuid_short = str(uuid.uuid4())[:8]
    parts = [txn_type]

    if context_info:
        # Sanitize context info to avoid high cardinality issues
        sanitized_context = context_info.replace("/", "_").replace(":", "_")[:50]
        parts.append(sanitized_context)

    if include_timestamp:
        timestamp = int(time.time())
        parts.append(str(timestamp))

    parts.append(uuid_short)

    return "_".join(parts)


def set_route_monitor_transaction_id(route_id: str = "") -> str:
    """
    Set the route monitoring transaction ID in context.

    Args:
        route_id: Route ID for context

    Returns:
        Generated transaction ID
    """
    txn_id = generate_transaction_id("route_monitor", route_id)
    route_monitor_txn_context.set(txn_id)

    # Set Sentry transaction context if we have a current span
    current_span = trace.get_current_span()
    if current_span and current_span.is_recording():
        context_name = (
            f"route_monitor.{route_id}" if route_id else "route_monitor.batch"
        )
        set_sentry_transaction_context(current_span, context_name, "task")
        # Also add the transaction ID immediately
        add_transaction_ids_to_span(current_span)

    logger.debug(f"Set route monitor transaction ID: {txn_id}")
    return txn_id


def set_vehicle_track_transaction_id(vehicle_id: str = "") -> str:
    """
    Set the vehicle tracking transaction ID in context.

    Args:
        vehicle_id: Vehicle ID for context

    Returns:
        Generated transaction ID
    """
    txn_id = generate_transaction_id("vehicle_track", vehicle_id)
    vehicle_track_txn_context.set(txn_id)

    # Set Sentry transaction context if we have a current span
    current_span = trace.get_current_span()
    if current_span and current_span.is_recording():
        context_name = (
            f"vehicle_track.{vehicle_id}" if vehicle_id else "vehicle_track.event"
        )
        set_sentry_transaction_context(current_span, context_name, "task")
        # Also add the transaction ID immediately
        add_transaction_ids_to_span(current_span)

    logger.debug(f"Set vehicle track transaction ID: {txn_id}")
    return txn_id


def set_user_query_transaction_id(endpoint: str = "") -> str:
    """
    Set the user query transaction ID in context.

    Args:
        endpoint: API endpoint for context

    Returns:
        Generated transaction ID
    """
    txn_id = generate_transaction_id("user_query", endpoint)
    user_query_txn_context.set(txn_id)

    # Set Sentry transaction context if we have a current span
    current_span = trace.get_current_span()
    if current_span and current_span.is_recording():
        # Clean endpoint for transaction naming
        clean_endpoint = endpoint.replace("/", "_").replace("-", "_").strip("_")
        context_name = f"api.{clean_endpoint}" if clean_endpoint else "api.request"
        set_sentry_transaction_context(current_span, context_name, "request")
        # Also add the transaction ID immediately
        add_transaction_ids_to_span(current_span)

    logger.debug(f"Set user query transaction ID: {txn_id}")
    return txn_id


def get_current_transaction_ids() -> dict[str, Optional[str]]:
    """
    Get all current transaction IDs from context.

    Returns:
        Dictionary with transaction type keys and ID values
    """
    return {
        "route_monitor": route_monitor_txn_context.get(),
        "vehicle_track": vehicle_track_txn_context.get(),
        "user_query": user_query_txn_context.get(),
    }


def restore_transaction_context(txn_ids: dict[str, str]) -> None:
    """
    Restore transaction IDs into context variables.

    Args:
        txn_ids: Dictionary of transaction type to ID mappings
    """
    if "route_monitor" in txn_ids:
        route_monitor_txn_context.set(txn_ids["route_monitor"])
    if "vehicle_track" in txn_ids:
        vehicle_track_txn_context.set(txn_ids["vehicle_track"])
    if "user_query" in txn_ids:
        user_query_txn_context.set(txn_ids["user_query"])


def clear_transaction_context() -> None:
    """Clear all transaction IDs from context."""
    route_monitor_txn_context.set(None)
    vehicle_track_txn_context.set(None)
    user_query_txn_context.set(None)


def add_span_attributes(span: Optional[Span], attributes: dict[str, Any]) -> None:
    """
    Safely add multiple attributes to a span.

    Only adds attributes if span is recording. Handles type conversion
    for common attribute types.

    Args:
        span: The span to add attributes to (can be None)
        attributes: Dictionary of attribute key-value pairs
    """
    if span is None or not span.is_recording():
        return

    for key, value in attributes.items():
        if value is None:
            continue

        try:
            # OTEL attributes must be str, bool, int, float, or sequences thereof
            if isinstance(value, (str, bool, int, float)):
                span.set_attribute(key, value)
            elif isinstance(value, (list, tuple)):
                # Convert to list of strings
                span.set_attribute(key, [str(v) for v in value])
            else:
                span.set_attribute(key, str(value))
        except Exception as e:
            logger.debug(f"Failed to set attribute {key}={value}: {e}")


def set_sentry_transaction_context(
    span: Optional[Span], transaction_name: str, transaction_type: str = "task"
) -> None:
    """
    Set Sentry-specific transaction context on a span for better transaction grouping.

    This helps Sentry group related operations and provides better performance insights.

    Args:
        span: The span to add Sentry context to
        transaction_name: Human-readable transaction name (e.g., "route_monitor_Red", "user_query_vehicles")
        transaction_type: Transaction type for Sentry grouping (e.g., "task", "request", "cron")
    """
    if span is None or not span.is_recording():
        return

    # Set Sentry transaction name - this is key for transaction grouping
    span.set_attribute("sentry.transaction", transaction_name)
    span.set_attribute("sentry.transaction_info.source", "custom")
    span.set_attribute("sentry.transaction_info.transaction_type", transaction_type)

    # Add transaction source for debugging
    span.set_attribute("sentry.tags.transaction_source", "business_logic")


def add_transaction_ids_to_span(span: Optional[Span]) -> None:
    """
    Add current transaction IDs as span attributes with Sentry-compatible naming.

    Args:
        span: The span to add transaction IDs to (can be None)
    """
    if span is None or not span.is_recording():
        return

    txn_ids = get_current_transaction_ids()

    # Add transaction IDs as span attributes (primary attributes for OTEL)
    if txn_ids["route_monitor"]:
        span.set_attribute(ROUTE_MONITOR_TXN_ATTR, txn_ids["route_monitor"])
        # Also add as Sentry custom tag for better visibility
        span.set_attribute("sentry.tags.route_monitor_txn", txn_ids["route_monitor"])

    if txn_ids["vehicle_track"]:
        span.set_attribute(VEHICLE_TRACK_TXN_ATTR, txn_ids["vehicle_track"])
        # Also add as Sentry custom tag for better visibility
        span.set_attribute("sentry.tags.vehicle_track_txn", txn_ids["vehicle_track"])

    if txn_ids["user_query"]:
        span.set_attribute(USER_QUERY_TXN_ATTR, txn_ids["user_query"])
        # Also add as Sentry custom tag for better visibility
        span.set_attribute("sentry.tags.user_query_txn", txn_ids["user_query"])

    # Add a general business transaction indicator
    active_txns = [k for k, v in txn_ids.items() if v is not None]
    if active_txns:
        span.set_attribute("sentry.tags.business_context", ",".join(active_txns))


def add_event_to_span(
    span: Span, name: str, attributes: Optional[dict[str, Any]] = None
) -> None:
    """
    Add an event to a span with optional attributes.

    Events are useful for marking significant moments within a span's lifetime,
    such as receiving a vehicle update or processing a prediction.

    Args:
        span: The span to add event to
        name: Event name
        attributes: Optional event attributes
    """
    if not span.is_recording():
        return

    try:
        span.add_event(name, attributes=attributes or {})
    except Exception as e:
        logger.debug(f"Failed to add event {name}: {e}")


def set_span_error(span: Optional[Span], exception: Exception) -> None:
    """
    Mark a span as errored and record exception details.

    Args:
        span: The span to mark as errored (can be None)
        exception: The exception that occurred
    """
    if span is None or not span.is_recording():
        return

    try:
        span.set_status(Status(StatusCode.ERROR, str(exception)))
        span.record_exception(exception)
    except Exception as e:
        logger.debug(f"Failed to record exception: {e}")


def traced_anyio_task(
    operation_name: Optional[str] = None,
    span_kind: SpanKind = SpanKind.INTERNAL,
    attributes: Optional[dict[str, Any]] = None,
    include_transaction_ids: bool = True,
):
    """
    Decorator to create a traced anyio task.

    This decorator creates a new root span for the task, suitable for
    long-running concurrent tasks started via TaskGroup.start_soon().

    Args:
        operation_name: Span name (defaults to function name)
        span_kind: Span kind (default: INTERNAL)
        attributes: Additional span attributes
        include_transaction_ids: Whether to include current transaction IDs in span

    Example:
        @traced_anyio_task("watch_vehicles", attributes={"route.id": "Red"})
        async def watch_vehicles(route_id: str):
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            tracer = trace.get_tracer(__name__)
            span_name = operation_name or func.__name__

            with tracer.start_as_current_span(span_name, kind=span_kind) as span:
                if attributes:
                    add_span_attributes(span, attributes)

                if include_transaction_ids:
                    add_transaction_ids_to_span(span)

                try:
                    return await func(*args, **kwargs)  # type: ignore
                except Exception as e:
                    set_span_error(span, e)
                    raise

        return wrapper  # type: ignore

    return decorator


def traced_function(
    operation_name: Optional[str] = None,
    span_kind: SpanKind = SpanKind.INTERNAL,
    capture_args: bool = False,
    include_transaction_ids: bool = True,
):
    """
    Decorator to create a traced async function.

    Creates a child span of the current context. Suitable for functions
    called within an existing trace context.

    Args:
        operation_name: Span name (defaults to function name)
        span_kind: Span kind (default: INTERNAL)
        capture_args: Whether to capture function arguments as span attributes
        include_transaction_ids: Whether to include current transaction IDs in span

    Example:
        @traced_function("compute_pattern_scores")
        async def compute_scores(route_id: str, station_id: str):
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            tracer = trace.get_tracer(func.__module__)
            span_name = operation_name or func.__name__

            with tracer.start_as_current_span(span_name, kind=span_kind) as span:
                if capture_args and kwargs:
                    # Only capture string/numeric kwargs to avoid large payloads
                    safe_attrs = {
                        f"arg.{k}": v
                        for k, v in kwargs.items()
                        if isinstance(v, (str, int, float, bool))
                    }
                    add_span_attributes(span, safe_attrs)

                if include_transaction_ids:
                    add_transaction_ids_to_span(span)

                try:
                    result = func(*args, **kwargs)
                    # Handle both sync and async functions
                    import inspect

                    if inspect.iscoroutine(result):
                        return await result  # type: ignore
                    return result  # type: ignore
                except Exception as e:
                    set_span_error(span, e)
                    raise

        return async_wrapper  # type: ignore

    return decorator


def start_linked_span(
    tracer: trace.Tracer,
    span_name: str,
    context_json: Optional[str] = None,
    attributes: Optional[dict[str, Any]] = None,
    span_kind: SpanKind = SpanKind.INTERNAL,
    include_transaction_ids: bool = True,
) -> trace.Span:
    """
    Start a new span with a link to a previous span context.

    Useful for connecting related operations across anyio task boundaries
    without creating strict parent-child relationships.

    Args:
        tracer: Tracer instance
        span_name: Name for the new span
        context_json: Serialized context to link to
        attributes: Span attributes
        span_kind: Span kind
        include_transaction_ids: Whether to include current transaction IDs

    Returns:
        New span with link to previous context
    """
    links = []
    if context_json:
        link = create_span_link_from_context(context_json)
        if link:
            links.append(link)

    span = tracer.start_span(
        span_name,
        kind=span_kind,
        links=links,
    )

    if attributes:
        add_span_attributes(span, attributes)

    if include_transaction_ids:
        add_transaction_ids_to_span(span)

    return span


def get_high_cardinality_attributes(
    include_vehicle_ids: bool = False,
    include_trip_ids: bool = True,
) -> dict[str, bool]:
    """
    Get configuration for high-cardinality attribute inclusion.

    Args:
        include_vehicle_ids: Whether to include vehicle IDs (very high cardinality)
        include_trip_ids: Whether to include trip IDs (high cardinality)

    Returns:
        Dictionary of attribute inclusion flags
    """
    from otel_config import get_otel_config

    config = get_otel_config()

    return {
        "vehicle_ids": config["include_vehicle_ids"].lower() in {"true", "1", "yes"},
        "trip_ids": config["include_trip_ids"].lower() in {"true", "1", "yes"},
    }


def should_trace_operation(operation_type: str = "default") -> bool:
    """
    Determine if an operation should be traced based on sampling configuration.

    Args:
        operation_type: Type of operation ('default', 'high_volume', 'background')

    Returns:
        True if operation should be traced
    """
    from otel_config import (
        is_otel_enabled,
        should_sample_background,
        should_sample_high_volume,
    )

    if not is_otel_enabled():
        return False

    if operation_type == "high_volume":
        return should_sample_high_volume()
    elif operation_type == "background":
        return should_sample_background()
    else:
        # Default sampling is handled by tracer provider
        return True
