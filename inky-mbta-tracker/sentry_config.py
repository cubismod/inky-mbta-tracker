"""Sentry SDK initialization and configuration."""

import logging
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration

logger = logging.getLogger(__name__)


def is_sentry_enabled() -> bool:
    """Check if Sentry is enabled via environment variable."""
    return os.getenv("IMT_ENABLE_SENTRY", "false").lower() in {"true", "1", "yes", "on"}


def initialize_sentry(
    service_name_override: Optional[str] = None, include_fastapi: bool = False
) -> None:
    """
    Initialize Sentry SDK with appropriate integrations.

    This should be called early in the application lifecycle, typically right after
    logging setup and before any other initialization.

    Args:
        service_name_override: Optional service name to use instead of env var
        include_fastapi: Whether to include FastAPI/Starlette integrations
    """
    if not is_sentry_enabled():
        logger.debug("Sentry is disabled")
        return

    dsn = os.getenv("IMT_SENTRY_DSN")
    if not dsn:
        logger.warning(
            "Sentry is enabled but IMT_SENTRY_DSN is not set. Skipping Sentry initialization."
        )
        return

    service_name = service_name_override or os.getenv(
        "IMT_OTEL_SERVICE_NAME", "inky-mbta-tracker"
    )
    environment = os.getenv("IMT_OTEL_DEPLOYMENT_ENVIRONMENT", "development")
    release = os.getenv("IMT_OTEL_SERVICE_VERSION", "1.2.0")

    # Get sampling rates from environment
    traces_sample_rate = float(os.getenv("IMT_SENTRY_TRACE_SAMPLE", "0.05"))
    profiles_sample_rate = float(os.getenv("IMT_SENTRY_PROFILE_TRACE_SAMPLE", "0.05"))

    # Configure logging integration
    # This will automatically capture ERROR and above logs as Sentry events
    logging_integration = LoggingIntegration(
        level=logging.INFO,  # Capture INFO and above as breadcrumbs
        event_level=logging.ERROR,  # Send ERROR and above as events
    )

    # Build integrations list
    integrations = [
        logging_integration,
        RedisIntegration(),
        AioHttpIntegration(),
    ]

    # Add FastAPI integrations if requested
    if include_fastapi:
        try:
            from sentry_sdk.integrations.fastapi import FastApiIntegration
            from sentry_sdk.integrations.starlette import StarletteIntegration

            integrations.extend([FastApiIntegration(), StarletteIntegration()])
        except ImportError:
            logger.warning(
                "FastAPI integration requested but not available. Install sentry-sdk[fastapi]."
            )

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            release=release,
            traces_sample_rate=traces_sample_rate,
            profiles_sample_rate=profiles_sample_rate,
            integrations=integrations,
            # Set additional context
            send_default_pii=False,  # Don't send PII by default
            attach_stacktrace=True,  # Include stack traces for all messages
            # Set custom tags
            _experiments={
                "continuous_profiling_auto_start": True,
            },
        )

        # Set service name as a tag for filtering in Sentry
        sentry_sdk.set_tag("service.name", service_name)

        integration_names = ", ".join([i.__class__.__name__ for i in integrations])
        logger.info(
            f"Sentry initialized for {service_name} in {environment} environment "
            f"(traces: {traces_sample_rate * 100}%, profiles: {profiles_sample_rate * 100}%) "
            f"with integrations: {integration_names}"
        )

    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}", exc_info=True)
