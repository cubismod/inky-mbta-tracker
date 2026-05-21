
from mbta_responses import AlertResource
from redis.asyncio import Redis
from atproto import AsyncClient


class BlueskyClient:
    """
    A basic client that implements posting of MBTA alerts to bsky.social.

    Utilizes the atproto Python library to post to Bluesky and then store
    references in Redis in order to update alerts in real time.
    """

    r_client: Redis
    at_client: AsyncClient

    def __init__(self, r_client: Redis, at_client: AsyncClient) -> None:
        self.r_client = r_client
        self.at_client = at_client


    async def process_alert_event(
        self,
        alert: AlertResource
    ):
        alert_id = alert.id
        alert_ref =
