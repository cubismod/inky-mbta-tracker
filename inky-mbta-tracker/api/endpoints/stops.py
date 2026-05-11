import logging

from api.core import GET_DI
from api.limits import limiter
from api.middleware.cache_middleware import cache_ttl
from fastapi import APIRouter, HTTPException, Request, Response
from mbta_client_extended import light_get_stop
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span, set_span_error
from pydantic import ValidationError
from redis import RedisError

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get("/stop", summary="Get limited information about an MBTA stop")
@limiter.limit("10/minute")
@cache_ttl(20 * 60)
async def get_stop(request: Request, commons: GET_DI, id: str) -> Response:
    with tracer.start_as_current_span("api.stops.get_stop") as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span, {"api.endpoint": "stop", "response.format": "json", "stop.id": id}
        )

        try:
            if commons.tg:
                light_stop = await light_get_stop(commons.r_client, id, commons.tg)
                if light_stop:
                    add_span_attributes(span, {"api.response.success": True})
                    return Response(
                        content=light_stop.model_dump_json(),
                        media_type="application/json",
                    )
                else:
                    add_span_attributes(span, {"api.response.success": False})
                    return Response(status_code=404)
            raise HTTPException(status_code=500, detail="Internal server error")
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Error getting stop due to connection issue", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "connection"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except RedisError as exc:
            logger.error("Error getting stop due to Redis error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "redis"})
            raise HTTPException(status_code=500, detail="Internal server error")
        except ValidationError as exc:
            logger.error("Error getting stop due to validation error", exc_info=True)
            set_span_error(span, exc)
            add_span_attributes(span, {"error.type": "validation"})
            raise HTTPException(status_code=500, detail="Internal server error")
