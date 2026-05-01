from datetime import datetime

from fastapi import APIRouter
from opentelemetry import trace
from otel_utils import add_span_attributes

router = APIRouter()
tracer = trace.get_tracer(__name__)


@router.get("/health")
async def health_check() -> dict[str, str]:
    with tracer.start_as_current_span("api.health.check") as span:
        add_span_attributes(
            span,
            {
                "api.endpoint": "health",
                "health.status": "healthy",
            },
        )
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
