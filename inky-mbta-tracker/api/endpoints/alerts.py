import logging

import aiohttp
from consts import ALERTS_CACHE_TTL, MBTA_V3_ENDPOINT
from fastapi import APIRouter, HTTPException, Request, Response
from mbta_responses import Alerts
from pydantic import ValidationError
from redis.exceptions import RedisError

from ..core import CONFIG, REDIS_CLIENT
from ..limits import limiter
from ..services.alerts import fetch_alerts_with_retry

router = APIRouter()


@router.get(
    "/alerts",
    summary="Get MBTA Alerts",
    description=(
        "Get current MBTA alerts. ⚠️ WARNING: Do not use 'Try it out' - large response may crash browser!"
    ),
)
@limiter.limit("100/minute")
async def get_alerts(request: Request) -> Response:
    try:
        cache_key = "api:alerts"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if cached_data:
            return Response(content=cached_data, media_type="application/json")

        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)

        alerts_data = Alerts(data=alerts)
        alerts_json = alerts_data.model_dump_json(exclude_unset=True)
        await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)
        return Response(content=alerts_json, media_type="application/json")
    except (ConnectionError, TimeoutError):
        logging.error("Error getting alerts due to connection issue", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except RedisError:
        logging.error("Error getting alerts due to Redis error", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logging.error("Error getting alerts due to validation error", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/alerts.json",
    summary="Get MBTA Alerts (JSON File)",
    description="Get current MBTA alerts as JSON file.",
    response_class=Response,
)
@limiter.limit("100/minute")
async def get_alerts_json(request: Request) -> Response:
    try:
        cache_key = "api:alerts:json"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if cached_data:
            return Response(
                content=cached_data,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=alerts.json"},
            )

        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)
        alerts_data = Alerts(data=alerts)
        alerts_json = alerts_data.model_dump_json(exclude_unset=True)
        await REDIS_CLIENT.setex(cache_key, ALERTS_CACHE_TTL, alerts_json)
        return Response(
            content=alerts_json,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=alerts.json"},
        )
    except (ConnectionError, TimeoutError):
        logging.error(
            "Error getting alerts JSON due to connection issue", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
    except RedisError:
        logging.error("Error getting alerts JSON due to Redis error", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    except ValidationError:
        logging.error(
            "Error getting alerts JSON due to validation error", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")
