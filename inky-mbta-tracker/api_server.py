import os
from contextlib import asynccontextmanager

import aiohttp
from api.core import RATE_LIMITING_ENABLED
from api.endpoints.alerts import router as alerts_router

# Routers
from api.endpoints.health import router as health_router
from api.endpoints.predictions import router as predictions_router
from api.endpoints.shapes import router as shapes_router
from api.endpoints.vehicles import router as vehicles_router
from api.limits import limiter
from api.middleware.cache_middleware import create_cache_middleware
from api.middleware.header_middleware import HeaderLoggingMiddleware
from consts import MBTA_V3_ENDPOINT
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from logging_setup import setup_logging
from otel_config import initialize_otel, is_otel_enabled, shutdown_otel
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded


def create_app() -> FastAPI:
    load_dotenv()
    setup_logging()

    # Initialize OpenTelemetry for the API server
    initialize_otel(service_name_override="inky-mbta-tracker-api")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.session = aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT)
        try:
            yield
        finally:
            await app.state.session.close()
            # Ensure OTEL spans are flushed before exit
            if is_otel_enabled():
                shutdown_otel()

    app = FastAPI(
        title="MBTA Transit Data API",
        description=(
            "API for MBTA transit data including track predictions, vehicle positions, alerts, and route shapes"
        ),
        version="2.1.0",
        docs_url="/",
        servers=[
            {
                "url": os.getenv(
                    "IMT_FASTAPI_ENDPOINT",
                    f"http://localhost:{os.getenv('IMT_API_PORT', '8080')}",
                ),
                "description": "Production",
            }
        ],
        swagger_ui_parameters={
            "defaultModelsExpandDepth": 1,
            "defaultModelExpandDepth": 1,
            "displayRequestDuration": True,
            "docExpansion": "none",
            "tryItOutEnabled": True,
        },
        lifespan=lifespan,
    )

    if RATE_LIMITING_ENABLED:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore

    app.add_middleware(HeaderLoggingMiddleware)
    app.middleware("http")(
        create_cache_middleware(
            cache_methods={"GET", "POST"},
            default_ttl=5,
            include_paths={
                "/alerts*",
                "/alerts.json",
                "/predictions*",
                "/chained-predictions",
                "/historical*",
                "/shapes*",
                "/vehicles*",
            },
            exclude_paths={"/vehicles/stream"},
        )
    )

    origins = [
        origin.strip()
        for origin in os.environ.get(
            "IMT_API_ORIGIN", "http://localhost:1313,http://localhost:8080"
        ).split(",")
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    Instrumentator().instrument(app).expose(app)

    # Apply FastAPI OTEL instrumentation if enabled
    if is_otel_enabled():
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument_app(app)
        except ImportError:
            pass  # Instrumentation package not available

    # Include routers
    app.include_router(health_router)
    app.include_router(predictions_router)
    app.include_router(vehicles_router)
    app.include_router(alerts_router)
    app.include_router(shapes_router)

    return app


# Export a module-level app for compatibility
app = create_app()
