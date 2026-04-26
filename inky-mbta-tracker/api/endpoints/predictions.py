import logging

from api.core import GET_DI
from api.middleware.cache_middleware import cache_ttl
from fastapi import APIRouter, Request, Response
from mbta_client_extended import (
    filter_predictions,
    get_predictions,
    transform_predictions,
)
from opentelemetry import trace
from otel_utils import add_transaction_ids_to_span
from pydantic import ValidationError
from shared_types.shared_types import PredictionResponse, PredictionsRequest

from ..limits import limiter

router = APIRouter()

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.post(
    "/predictions",
    summary="Fetch stop predictions",
    description=("Get a maximum of 10 stop/route filtered predictions"),
)
@cache_ttl(4 * 60)
@limiter.limit("10/minute")
async def predictions_req(
    request: Request, commons: GET_DI, req: PredictionsRequest
) -> Response:
    with tracer.start_as_current_span("api.predictions.get_predictions") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            if commons.tg:
                if len(req.stops) > 10:
                    return Response(
                        content="Too many stops requested. Maximum is 10.",
                        media_type="text/plain",
                        status_code=400,
                    )
                if len(req.stops) == 0:
                    return Response(
                        content="No stops requested. Please provide at least one stop.",
                        media_type="text/plain",
                        status_code=400,
                    )

                stop_ids = [stop.id for stop in req.stops]

                predictions = await get_predictions(
                    commons.session, stop_ids, commons.r_client, commons.tg, span
                )
                if predictions:
                    transformed = await transform_predictions(
                        await filter_predictions(predictions, req, commons.r_client, commons.tg),
                        commons.tg,
                        commons.r_client,
                        commons.session,
                    )
                    return Response(
                        content=PredictionResponse(stops=transformed).json(),
                        media_type="application/json",
                    )
        except ValidationError as e:
            logger.error("Validation error in predictions request", exc_info=e)
            span.set_attribute("error", True)
            span.set_attribute("error.type", "validation")
            return Response(
                content=f"Invalid request: {e}",
                media_type="text/plain",
                status_code=400,
            )

        return Response(status_code=204)
