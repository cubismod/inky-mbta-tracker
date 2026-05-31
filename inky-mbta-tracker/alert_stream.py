from typing import Optional

from anyio.abc import TaskGroup
from config import Config
from consts import HOUR, WEEK
from mbta_responses import AlertResource, TypeAndID
from pydantic import TypeAdapter, ValidationError
from redis.asyncio.client import Redis
from redis_cache import get_cache, write_cache
from webhook.discord_webhook import delete_webhook, process_alert_event


async def handle_alert_stream_event(
    data: str,
    event_type: str,
    r_client: Redis,
    config: Config,
    tg: TaskGroup,
    route: Optional[str] = None,
) -> None:
    match event_type:
        case "reset":
            alerts = TypeAdapter(list[AlertResource]).validate_json(data, strict=False)
            if route:
                await r_client.delete(f"alerts:route:{route}")
            for alert in alerts:
                await process_alert_event(alert, r_client, config, tg)
                await store_alert(alert, r_client, route)
                await add_alert_memberships(alert, r_client)
        case "add" | "update":
            alert = AlertResource.model_validate_json(data, strict=False)
            await store_alert(alert, r_client, route)
            await process_alert_event(alert, r_client, config, tg)
            await add_alert_memberships(alert, r_client)
        case "remove":
            type_and_id = TypeAndID.model_validate_json(data, strict=False)
            await remove_alert(type_and_id.id, r_client)
        case _:
            return


async def store_alert(
    alert: AlertResource,
    r_client: Redis,
    route: Optional[str] = None,
) -> None:
    await write_cache(r_client, f"alert:{alert.id}", alert.model_dump_json(), 2 * HOUR)
    if route:
        await r_client.sadd(f"alerts:route:{route}", alert.id)  # type: ignore[misc]


async def remove_alert(alert_id: str, r_client: Redis) -> None:
    try:
        cached = await get_cache(r_client, f"alert:{alert_id}")
        if cached:
            try:
                alert = AlertResource.model_validate_json(cached, strict=False)
            except ValidationError:
                return

            await delete_webhook(alert_id, r_client)
            await remove_alert_memberships(alert, r_client)
    finally:
        await r_client.delete(f"alert:{alert_id}")  # type: ignore[misc]


async def add_alert_memberships(alert: AlertResource, r_client: Redis) -> None:
    if not alert.attributes or not alert.attributes.informed_entity:
        return

    for entity in alert.attributes.informed_entity:
        if entity.route:
            await r_client.sadd(f"alerts:route:{entity.route}", alert.id)  # type: ignore[misc]
        if entity.trip:
            await r_client.sadd(f"alerts:trip:{entity.trip}", alert.id)  # type: ignore[misc]
            await r_client.expire(f"alerts:trip:{entity.trip}", WEEK)


async def remove_alert_memberships(alert: AlertResource, r_client: Redis) -> None:
    if not alert.attributes or not alert.attributes.informed_entity:
        return

    for entity in alert.attributes.informed_entity:
        if entity.route:
            await r_client.srem(f"alerts:route:{entity.route}", alert.id)  # type: ignore[misc]
        if entity.trip:
            await r_client.srem(f"alerts:trip:{entity.trip}", alert.id)  # type: ignore[misc]
            await r_client.expire(f"alerts:trip:{entity.trip}", WEEK)
