import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from .core import RATE_LIMITING_ENABLED
from .endpoints.ai import router as ai_router
from .endpoints.alerts import router as alerts_router

# Routers
from .endpoints.health import router as health_router
from .endpoints.predictions import router as predictions_router
from .endpoints.shapes import router as shapes_router
from .endpoints.vehicles import router as vehicles_router
from .lifespan import lifespan
from .limits import limiter
from .middleware import HeaderLoggingMiddleware, TrafficMonitoringMiddleware


def create_app() -> FastAPI:
    app = FastAPI(
        title="MBTA Transit Data API",
        description=(
            "API for MBTA transit data including track predictions, vehicle positions, alerts, and route shapes"
        ),
        version="2.0.0",
        docs_url="/",
        lifespan=lifespan,
        servers=[{"url": "https://imt.ryanwallace.cloud", "description": "Production"}],
        swagger_ui_parameters={
            "defaultModelsExpandDepth": 1,
            "defaultModelExpandDepth": 1,
            "displayRequestDuration": True,
            "docExpansion": "none",
            "tryItOutEnabled": True,
        },
    )

    if RATE_LIMITING_ENABLED:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore

    app.add_middleware(TrafficMonitoringMiddleware)
    app.add_middleware(HeaderLoggingMiddleware)

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

    # Include routers
    app.include_router(health_router)
    app.include_router(predictions_router)
    app.include_router(vehicles_router)
    app.include_router(alerts_router)
    app.include_router(shapes_router)
    app.include_router(ai_router)

    return app


# Export a module-level app for compatibility
app = create_app()
