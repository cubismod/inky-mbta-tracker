"""
OpenTelemetry configuration and initialization.

This module provides centralized OTEL setup optimized for Grafana Tempo with
smart sampling strategies for high-throughput MBTA tracking operations.
"""

import logging
import os
from typing import Any, Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ParentBasedTraceIdRatio,
    TraceIdRatioBased,
)
from opentelemetry.trace import Span

logger = logging.getLogger(__name__)

_tracer_provider: Optional[TracerProvider] = None
_is_initialized = False


def is_otel_enabled() -> bool:
    """Check if OpenTelemetry is enabled via environment variable."""
    return os.getenv("IMT_OTEL_ENABLED", "false").lower() in {"true", "1", "yes", "on"}


def get_otel_config() -> dict[str, str]:
    """
    Get OpenTelemetry configuration from environment variables.

    Returns:
        Dictionary of OTEL configuration parameters with sensible defaults.
    """
    return {
        "service_name": os.getenv("IMT_OTEL_SERVICE_NAME", "inky-mbta-tracker"),
        "service_version": os.getenv("IMT_OTEL_SERVICE_VERSION", "1.2.0"),
        "deployment_environment": os.getenv(
            "IMT_OTEL_DEPLOYMENT_ENVIRONMENT", "development"
        ),
        "instance_id": os.getenv(
            "IMT_OTEL_SERVICE_INSTANCE_ID", os.getenv("INSTANCE_ID", "unknown")
        ),
        "exporter_endpoint": os.getenv(
            "IMT_OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        ),
        "exporter_protocol": os.getenv("IMT_OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        "exporter_insecure": os.getenv("IMT_OTEL_EXPORTER_OTLP_INSECURE", "true"),
        "traces_sampler": os.getenv(
            "IMT_OTEL_TRACES_SAMPLER", "parentbased_traceidratio"
        ),
        "traces_sampler_arg": os.getenv("IMT_OTEL_TRACES_SAMPLER_ARG", "0.1"),
        "high_volume_sample_rate": os.getenv("IMT_OTEL_HIGH_VOLUME_SAMPLE_RATE", "0.1"),
        "background_sample_rate": os.getenv("IMT_OTEL_BACKGROUND_SAMPLE_RATE", "0.01"),
        "bsp_max_queue_size": os.getenv("IMT_OTEL_BSP_MAX_QUEUE_SIZE", "4096"),
        "bsp_max_export_batch_size": os.getenv(
            "IMT_OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "512"
        ),
        "bsp_schedule_delay": os.getenv("IMT_OTEL_BSP_SCHEDULE_DELAY", "5000"),
        "bsp_export_timeout": os.getenv("IMT_OTEL_BSP_EXPORT_TIMEOUT", "30000"),
        "instrument_redis": os.getenv("IMT_OTEL_INSTRUMENT_REDIS", "true"),
        "instrument_aiohttp": os.getenv("IMT_OTEL_INSTRUMENT_AIOHTTP", "true"),
        "instrument_fastapi": os.getenv("IMT_OTEL_INSTRUMENT_FASTAPI", "true"),
        "include_vehicle_ids": os.getenv("IMT_OTEL_INCLUDE_VEHICLE_IDS", "false"),
        "include_trip_ids": os.getenv("IMT_OTEL_INCLUDE_TRIP_IDS", "true"),
        "redis_sanitize_queries": os.getenv("IMT_OTEL_REDIS_SANITIZE_QUERIES", "false"),
    }


def create_resource(config: dict[str, str]) -> Resource:
    """
    Create OTEL Resource with service attributes.

    Args:
        config: OTEL configuration dictionary

    Returns:
        Resource with service identification attributes
    """
    attributes = {
        "service.name": config["service_name"],
        "service.version": config["service_version"],
        "service.instance.id": config["instance_id"],
        "deployment.environment": config["deployment_environment"],
        "environment": config["deployment_environment"],
    }
    return Resource.create(attributes)


def create_sampler(config: dict[str, str]):
    """
    Create sampler based on configuration.

    Uses parent-based trace ID ratio sampling for intelligent sampling
    of high-volume operations while maintaining trace continuity.

    Args:
        config: OTEL configuration dictionary

    Returns:
        Configured sampler instance
    """
    sample_rate = float(config["traces_sampler_arg"])
    sampler_type = config["traces_sampler"].lower()

    if sampler_type == "parentbased_traceidratio":
        return ParentBasedTraceIdRatio(sample_rate)
    elif sampler_type == "traceidratio":
        return TraceIdRatioBased(sample_rate)
    else:
        logger.warning(
            f"Unknown sampler type '{sampler_type}', defaulting to ParentBasedTraceIdRatio"
        )
        return ParentBasedTraceIdRatio(sample_rate)


def create_exporter(config: dict[str, str]) -> OTLPSpanExporter:
    """
    Create OTLP span exporter configured for Grafana Tempo.

    Args:
        config: OTEL configuration dictionary

    Returns:
        Configured OTLP exporter
    """
    endpoint = config["exporter_endpoint"]
    insecure = config["exporter_insecure"].lower() in {"true", "1", "yes"}

    logger.info(
        f"Creating OTLP exporter with endpoint: {endpoint} (insecure={insecure})"
    )

    return OTLPSpanExporter(
        endpoint=endpoint,
        insecure=insecure,
    )


def initialize_otel(service_name_override: Optional[str] = None) -> bool:
    """
    Initialize OpenTelemetry tracing with Tempo-optimized configuration.

    This function sets up:
    - Tracer provider with service resource attributes
    - Parent-based trace ID ratio sampling (10% default)
    - Batch span processor optimized for high throughput
    - OTLP gRPC exporter for Grafana Tempo

    Args:
        service_name_override: Optional override for service name (useful for api vs worker)

    Returns:
        True if initialization succeeded, False if disabled or already initialized
    """
    global _tracer_provider, _is_initialized

    if _is_initialized:
        logger.debug("OpenTelemetry already initialized, skipping")
        return True

    if not is_otel_enabled():
        logger.info("OpenTelemetry disabled via IMT_OTEL_ENABLED environment variable")
        return False

    try:
        config = get_otel_config()
        if service_name_override:
            config["service_name"] = service_name_override

        logger.info(
            f"Initializing OpenTelemetry for service '{config['service_name']}' "
            f"(instance: {config['instance_id']}, env: {config['deployment_environment']})"
        )

        # Create resource with service attributes
        resource = create_resource(config)

        # Create sampler
        sampler = create_sampler(config)

        # Create tracer provider
        _tracer_provider = TracerProvider(resource=resource, sampler=sampler)

        # Create and configure batch span processor
        exporter = create_exporter(config)
        span_processor = BatchSpanProcessor(
            exporter,
            max_queue_size=int(config["bsp_max_queue_size"]),
            max_export_batch_size=int(config["bsp_max_export_batch_size"]),
            schedule_delay_millis=int(config["bsp_schedule_delay"]),
            export_timeout_millis=int(config["bsp_export_timeout"]),
        )
        _tracer_provider.add_span_processor(span_processor)

        # Set as global tracer provider
        trace.set_tracer_provider(_tracer_provider)

        # Auto-instrument libraries if enabled
        _auto_instrument(config)

        _is_initialized = True
        logger.info(
            f"OpenTelemetry initialized successfully with {config['traces_sampler']} "
            f"sampler (rate: {config['traces_sampler_arg']})"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry: {e}", exc_info=True)
        return False


def _auto_instrument(config: dict[str, str]) -> None:
    """
    Auto-instrument supported libraries based on configuration.

    Args:
        config: OTEL configuration dictionary
    """

    def redis_request_hook(
        span: Span, instance: Any, args: list[Any], kwargs: dict[str, Any]
    ) -> None:
        """
        Custom Redis request hook to control query sanitization.

        By default, OpenTelemetry Redis instrumentation sanitizes queries for security,
        converting commands like 'GET mykey' to 'GET ?'. This hook allows overriding
        that behavior when IMT_OTEL_REDIS_SANITIZE_QUERIES is set to 'false'.
        """
        if not span or not span.is_recording():
            return

        sanitize = config.get("redis_sanitize_queries", "false").lower() in {
            "true",
            "1",
            "yes",
        }

        if not sanitize and args:
            # Override the sanitized db.statement with the full command
            try:
                full_command = " ".join(str(arg) for arg in args)
                # Limit command length to prevent excessively large spans
                max_length = 1000
                if len(full_command) > max_length:
                    full_command = full_command[:max_length] + "..."
                span.set_attribute("db.statement", full_command)
            except Exception as e:
                logger.debug(f"Failed to set full Redis command in span: {e}")

    # Redis instrumentation
    if config["instrument_redis"].lower() in {"true", "1", "yes"}:
        try:
            from opentelemetry.instrumentation.redis import RedisInstrumentor

            RedisInstrumentor().instrument(request_hook=redis_request_hook)

            sanitize_enabled = config.get(
                "redis_sanitize_queries", "false"
            ).lower() in {"true", "1", "yes"}
            logger.info(
                f"Redis auto-instrumentation enabled (sanitize_queries={sanitize_enabled})"
            )
        except ImportError:
            logger.warning(
                "Redis instrumentation not available (package not installed)"
            )
        except Exception as e:
            logger.error(f"Failed to instrument Redis: {e}")

    # aiohttp client instrumentation
    if config["instrument_aiohttp"].lower() in {"true", "1", "yes"}:
        try:
            from opentelemetry.instrumentation.aiohttp_client import (
                AioHttpClientInstrumentor,
            )

            AioHttpClientInstrumentor().instrument()
            logger.info("aiohttp client auto-instrumentation enabled")
        except ImportError:
            logger.warning(
                "aiohttp instrumentation not available (package not installed)"
            )
        except Exception as e:
            logger.error(f"Failed to instrument aiohttp: {e}")

    # FastAPI instrumentation (done separately in api_server.py due to app lifecycle)
    if config["instrument_fastapi"].lower() in {"true", "1", "yes"}:
        logger.debug("FastAPI instrumentation will be applied in api_server.py")


def get_tracer(name: str) -> trace.Tracer:
    """
    Get a tracer instance for the given name.

    Args:
        name: Tracer name (typically __name__ of calling module)

    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)


def shutdown_otel() -> None:
    """
    Shutdown OpenTelemetry and flush any pending spans.

    Should be called on application shutdown to ensure all spans are exported.
    """
    global _tracer_provider, _is_initialized

    if _tracer_provider and _is_initialized:
        logger.info("Shutting down OpenTelemetry tracer provider")
        try:
            _tracer_provider.shutdown()
            _is_initialized = False
            logger.info("OpenTelemetry shutdown complete")
        except Exception as e:
            logger.error(f"Error during OpenTelemetry shutdown: {e}", exc_info=True)


def get_sampling_config() -> dict[str, float]:
    """
    Get sampling rates for different operation types.

    Returns:
        Dictionary mapping operation types to sample rates
    """
    config = get_otel_config()
    return {
        "default": float(config["traces_sampler_arg"]),
        "high_volume": float(config["high_volume_sample_rate"]),
        "background": float(config["background_sample_rate"]),
    }


def should_sample_high_volume() -> bool:
    """
    Determine if a high-volume operation should be sampled.

    Uses the configured high_volume_sample_rate for probabilistic sampling.

    Returns:
        True if the operation should be traced
    """
    import random

    sample_rate = float(get_otel_config()["high_volume_sample_rate"])
    return random.random() < sample_rate


def should_sample_background() -> bool:
    """
    Determine if a background operation should be sampled.

    Uses the configured background_sample_rate for probabilistic sampling.

    Returns:
        True if the operation should be traced
    """
    import random

    sample_rate = float(get_otel_config()["background_sample_rate"])
    return random.random() < sample_rate
