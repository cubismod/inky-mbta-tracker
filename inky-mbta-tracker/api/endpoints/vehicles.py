import json
import logging
from datetime import datetime
from typing import Any, AsyncGenerator, Optional

from api.middleware.cache_middleware import cache_ttl
from api.services.vehicle_delta import calculate_diff
from fastapi import APIRouter, HTTPException, Request, Response
from geojson import Feature, FeatureCollection, dumps
from geojson_utils import get_vehicle_features
from opentelemetry import trace
from otel_utils import add_transaction_ids_to_span
from starlette.responses import StreamingResponse
from utils import get_vehicles_data

from ..core import GET_DI, SSE_ENABLED, DIParams
from ..limits import limiter
from ..models import VehiclesCountResponse

router = APIRouter()

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@router.get(
    "/vehicles",
    summary="Get Vehicle Positions",
    description=(
        "Get current vehicle positions as GeoJSON FeatureCollection. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
)
@cache_ttl(2)
@limiter.limit("70/minute")
async def get_vehicles(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> Response:
    with tracer.start_as_current_span("api.vehicles.get_vehicles") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            if commons.tg:
                span.set_attribute("cache.hit", False)
                features = await get_vehicle_features(
                    commons.r_client, commons.config, commons.tg, frequent_buses
                )
                span.set_attribute("vehicles.count", len(features))
                result = {"type": "FeatureCollection", "features": features}
                return Response(
                    content=json.dumps(result), media_type="application/json"
                )
            return Response(status_code=500)
        except (ConnectionError, TimeoutError):
            logger.error(
                "Error getting vehicles due to connection issue", exc_info=True
            )
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/vehicles/stream",
    summary="Stream Vehicle Positions",
    description="Server-sent events stream of vehicle positions",
)
@limiter.limit("70/minute")
async def get_vehicles_sse(
    request: Request, delta: bool = False, frequent_buses: bool = False
) -> StreamingResponse:
    if not SSE_ENABLED:
        raise HTTPException(status_code=503, detail="SSE streaming disabled")

    async def event_generator() -> AsyncGenerator[str, None]:
        from anyio import sleep

        # send an initial comment to establish the stream quickly
        yield ": stream-start\n\n"
        last_data: dict[str, Any] = {}

        def _normalize_features(features: dict[str, Any]) -> dict[str, Any]:
            out: dict[str, Any] = {}
            if not isinstance(features, dict):
                return out
            for k, v in features.items():
                try:
                    if isinstance(v, Feature):
                        out[k] = json.loads(dumps(v, sort_keys=True))
                    else:
                        out[k] = v
                except Exception:
                    out[k] = v if isinstance(v, dict) else {"_repr": str(v)}
            return out

        # Keep the dependency context open for the duration of the stream
        async with DIParams(request.app.state.session) as commons:
            while True:
                if await request.is_disconnected():
                    break
                if commons.tg:
                    try:
                        raw_data = await get_vehicles_data(
                            commons.r_client, commons.config, commons.tg, frequent_buses
                        )
                        norm_data = _normalize_features(
                            raw_data if isinstance(raw_data, dict) else {}
                        )
                        if delta:
                            resp = calculate_diff(last_data, raw_data)
                            last_data = raw_data
                        if delta and resp:
                            yield f"data: {resp.model_dump_json()}\n\n"
                        else:
                            try:
                                features_list = [v for v in norm_data.values()]
                                payload = {
                                    "type": "FeatureCollection",
                                    "features": features_list,
                                }
                                yield f"data: {json.dumps(payload)}\n\n"
                            except Exception:
                                yield f"data: {json.dumps(raw_data)}\n\n"
                    except (
                        ConnectionError,
                        TimeoutError,
                        ValueError,
                        RuntimeError,
                        OSError,
                    ) as e:
                        logger.error("Error producing SSE vehicles data", exc_info=e)
                        # comment as heartbeat so client keeps connection
                        yield ": error fetching data\n\n"
                else:
                    # no task group available; emit heartbeat so clients keep connection
                    yield ": tg-unavailable\n\n"

                # throttle update frequency
                await sleep(4)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )


@router.get(
    "/vehicles.json",
    summary="Get Vehicle Positions (JSON File)",
    description=(
        "Get current vehicle positions as GeoJSON file. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
    response_class=Response,
)
@cache_ttl(2)
@limiter.limit("70/minute")
async def get_vehicles_json(
    request: Request, commons: GET_DI, frequent_buses: bool = False
) -> Response:
    with tracer.start_as_current_span("api.vehicles.get_vehicles_json") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            if commons.tg:
                features = await get_vehicle_features(
                    commons.r_client, commons.config, commons.tg, frequent_buses
                )
                span.set_attribute("vehicles.count", len(features))
                feature_collection = FeatureCollection(features)
                geojson_str = dumps(feature_collection, sort_keys=True)

                return Response(
                    content=geojson_str,
                    media_type="application/json",
                    headers={
                        "Content-Disposition": "attachment; filename=vehicles.json"
                    },
                )
            else:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "no_task_group")
                return Response(status_code=500)
        except (ConnectionError, TimeoutError):
            logger.error(
                "Error getting vehicles JSON due to connection issue", exc_info=True
            )
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/vehicles/counts",
    summary="Get counts of vehicles by MBTA line and vehicle type",
    description="Return counts grouped by vehicle type (light rail, heavy rail, regional rail, bus) across main line groups (RL, GL, BL, OL, SL, CR)",
    response_model=Optional[VehiclesCountResponse],
)
@limiter.limit("70/minute")
@cache_ttl(30)
async def get_vehicles_counts(
    request: Request, commons: GET_DI
) -> Optional[VehiclesCountResponse]:
    with tracer.start_as_current_span("api.vehicles.get_vehicles_counts") as span:
        # Add transaction IDs to the span
        add_transaction_ids_to_span(span)

        try:
            if commons.tg:
                data = await get_vehicles_data(
                    commons.r_client, commons.config, commons.tg
                )

                # Initialize counts
                counts = {
                    "light_rail": {
                        "RL": 0,
                        "GL": 0,
                        "BL": 0,
                        "OL": 0,
                        "SL": 0,
                        "CR": 0,
                        "total": 0,
                    },
                    "heavy_rail": {
                        "RL": 0,
                        "GL": 0,
                        "BL": 0,
                        "OL": 0,
                        "SL": 0,
                        "CR": 0,
                        "total": 0,
                    },
                    "regional_rail": {
                        "RL": 0,
                        "GL": 0,
                        "BL": 0,
                        "OL": 0,
                        "SL": 0,
                        "CR": 0,
                        "total": 0,
                    },
                    "bus": {
                        "RL": 0,
                        "GL": 0,
                        "BL": 0,
                        "OL": 0,
                        "SL": 0,
                        "CR": 0,
                        "total": 0,
                    },
                }

                # Helper to increment
                def inc(vtype: str, line: str) -> None:
                    counts[vtype][line] += 1
                    counts[vtype]["total"] += 1

                # Map features to our line buckets and types
                for feat in data.values():
                    try:
                        props = getattr(feat, "properties", None) or (
                            feat.get("properties") if isinstance(feat, dict) else {}
                        )
                    except Exception:
                        props = {}
                    route = (
                        (props.get("route") or "").strip()
                        if isinstance(props, dict)
                        else ""
                    )
                    route_lower = str(route).lower()

                    # determine line group (RL, GL, BL, OL, SL, CR)
                    # Map Mattapan explicitly to the Red Line column (RL). Do not conflate Mattapan with Green Line.
                    if route_lower.startswith("mattapan") or route_lower.startswith(
                        "red"
                    ):
                        line = "RL"
                    elif route_lower.startswith("green"):
                        line = "GL"
                    elif route_lower.startswith("blue"):
                        line = "BL"
                    elif route_lower.startswith("orange"):
                        line = "OL"
                    # silver line may be labeled SL1/SL2/... or 'silver line' or numeric codes - accept 'sl' prefix
                    elif (
                        route_lower.startswith("sl")
                        or "silver" in route_lower
                        or route_lower.startswith("741")
                        or route_lower.startswith("742")
                        or route_lower.startswith("743")
                        or route_lower.startswith("746")
                        or route_lower.startswith("749")
                        or route_lower.startswith("751")
                    ):
                        line = "SL"
                    elif (
                        route_lower.startswith("cr")
                        or route_lower.startswith("commuter")
                        or route_lower == "commuter rail"
                    ):
                        line = "CR"
                    else:
                        # not one of the tracked lines - ignore
                        continue

                    # determine vehicle type independently so each line may contain multiple vehicle types
                    # (e.g., Mattapan PCC cars are light rail but sit in the Red Line column)
                    vtype = None

                    # explicit rules from route string
                    if route_lower.startswith("mattapan"):
                        vtype = "light_rail"
                    elif route_lower.startswith("green"):
                        vtype = "light_rail"
                    elif route_lower.startswith("cr"):
                        vtype = "regional_rail"
                    elif (
                        route_lower.startswith("sl")
                        or "silver" in route_lower
                        or route_lower.startswith("741")
                        or route_lower.startswith("742")
                        or route_lower.startswith("743")
                        or route_lower.startswith("746")
                        or route_lower.startswith("749")
                        or route_lower.startswith("751")
                    ):
                        vtype = "bus"
                    elif route_lower.isdecimal() or route_lower.startswith("7"):
                        # numeric routes are buses
                        vtype = "bus"
                    else:
                        # default heuristics for subway lines: Red/Blue/Orange -> heavy rail
                        if line in ("RL", "BL", "OL"):
                            vtype = "heavy_rail"

                    # fallback to marker-symbol if route-based heuristics are inconclusive
                    if vtype is None:
                        marker = (
                            props.get("marker-symbol")
                            if isinstance(props, dict)
                            else None
                        )
                        if marker == "bus":
                            vtype = "bus"
                        elif marker == "rail_amtrak":
                            vtype = "regional_rail"
                        elif marker == "rail":
                            if route_lower.startswith(
                                "mattapan"
                            ) or route_lower.startswith("green"):
                                vtype = "light_rail"
                            elif route_lower.startswith("cr"):
                                vtype = "regional_rail"
                            else:
                                vtype = "heavy_rail"

                    # increment counts for the determined type and the determined line column
                    if vtype and line:
                        inc(vtype, line)

                # compute totals by line
                totals_by_line = {
                    "RL": 0,
                    "GL": 0,
                    "BL": 0,
                    "OL": 0,
                    "SL": 0,
                    "CR": 0,
                    "total": 0,
                }
                for vtype, row in counts.items():
                    for col in ("RL", "GL", "BL", "OL", "SL", "CR"):
                        totals_by_line[col] += row[col]
                    totals_by_line["total"] += row["total"]

                span.set_attribute("vehicles.total", totals_by_line["total"])

                # Construct response payload (pydantic will validate/serialize)
                response_payload = {
                    "success": True,
                    "counts": {
                        "light_rail": counts["light_rail"],
                        "heavy_rail": counts["heavy_rail"],
                        "regional_rail": counts["regional_rail"],
                        "bus": counts["bus"],
                    },
                    "totals_by_line": totals_by_line,
                    "generated_at": datetime.now(),
                }

                resp_model = VehiclesCountResponse(**response_payload)

                return resp_model
        except (ConnectionError, TimeoutError):
            logger.error(
                "Error getting vehicle counts due to connection issue", exc_info=True
            )
            span.set_attribute("error", True)
            span.set_attribute("error.type", "connection")
            raise HTTPException(status_code=500, detail="Internal server error")
