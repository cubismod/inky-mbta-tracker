"""
OpenTelemetry utilities for trace context propagation and span management.

This module provides helpers for working with OTEL in anyio concurrent contexts,
including context serialization for queue-based trace propagation and decorators
for common tracing patterns.
"""

import functools
import json
import logging
from typing import Any, Callable, Optional, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span, SpanKind, Status, StatusCode

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


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
):
    """
    Decorator to create a traced anyio task.

    This decorator creates a new root span for the task, suitable for
    long-running concurrent tasks started via TaskGroup.start_soon().

    Args:
        operation_name: Span name (defaults to function name)
        span_kind: Span kind (default: INTERNAL)
        attributes: Additional span attributes

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
):
    """
    Decorator to create a traced async function.

    Creates a child span of the current context. Suitable for functions
    called within an existing trace context.

    Args:
        operation_name: Span name (defaults to function name)
        span_kind: Span kind (default: INTERNAL)
        capture_args: Whether to capture function arguments as span attributes

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
