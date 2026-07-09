from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mbta_gtfs import _process_gtfs_event
from redis.asyncio.client import Redis as RedisClient


def _outbound_cr_entity() -> dict[str, Any]:
    return {
        "id": "y1817",
        "vehicle": {
            "trip": {
                "trip_id": "CR-Worcester-CR-Weekday-Jul14-25-503",
                "route_id": "CR-Worcester",
                "direction_id": 0,
            },
            "position": {"latitude": 42.35, "longitude": -71.06, "bearing": 180},
            "current_status": "IN_TRANSIT_TO",
            "stop_id": "place-worc",
            "occupancy_status": "FEW_SEATS_AVAILABLE",
        },
    }


@pytest.mark.anyio("asyncio")
async def test_process_gtfs_event_resolves_headsign_for_outbound_direction_zero() -> (
    None
):
    """Regression: direction_id == 0 (outbound for CR) must not skip headsign lookup.

    Previously `if ... and direction_id:` used truthiness, so direction_id=0
    skipped the get_headsign call and left headsign=None. Both the V3 SSE
    watcher and the GTFS loop write to the same vehicle:{id} Redis key, so
    the headsign flipped between a value and None on every poll.
    """
    entity = _outbound_cr_entity()

    send_stream = MagicMock()
    sent: list = []
    send_stream.send = AsyncMock(side_effect=lambda ev: sent.append(ev))

    mock_mbta_client = MagicMock()
    mock_mbta_client.get_headsign = AsyncMock(return_value="Worcester")

    mock_mbta_api_cls = MagicMock()
    mock_mbta_api_cls.return_value.__aenter__ = AsyncMock(return_value=mock_mbta_client)
    mock_mbta_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("mbta_gtfs.MBTAApi", mock_mbta_api_cls),
        patch("mbta_gtfs.get_cache", new=AsyncMock(return_value=None)),
    ):
        await _process_gtfs_event(
            entity,
            cast(RedisClient, AsyncMock()),
            send_stream,
            MagicMock(),
            MagicMock(),
            cast(Any, MagicMock(start_soon=lambda *_a, **_kw: None)),
            "CR-Worcester",
        )

    mock_mbta_client.get_headsign.assert_awaited_once()
    args, _kwargs = mock_mbta_client.get_headsign.call_args
    # positional: session, tg, trip_id, route_id, direction_id
    assert args[2] == "CR-Worcester-CR-Weekday-Jul14-25-503"
    assert args[3] == "CR-Worcester"
    assert args[4] == 0
    assert len(sent) == 1
    assert sent[0].headsign == "Worcester"
    assert sent[0].source == "MBTA Real-Time GTFS"


@pytest.mark.anyio("asyncio")
async def test_process_gtfs_event_skips_headsign_when_trip_id_missing() -> None:
    """Without a trip_id there is nothing to look up; headsign stays None."""
    entity = _outbound_cr_entity()
    entity["vehicle"]["trip"].pop("trip_id")

    send_stream = MagicMock()
    sent: list = []
    send_stream.send = AsyncMock(side_effect=lambda ev: sent.append(ev))

    mock_mbta_client = MagicMock()
    mock_mbta_client.get_headsign = AsyncMock(return_value="should-not-be-called")

    mock_mbta_api_cls = MagicMock()
    mock_mbta_api_cls.return_value.__aenter__ = AsyncMock(return_value=mock_mbta_client)
    mock_mbta_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("mbta_gtfs.MBTAApi", mock_mbta_api_cls),
        patch("mbta_gtfs.get_cache", new=AsyncMock(return_value=None)),
    ):
        await _process_gtfs_event(
            entity,
            cast(RedisClient, AsyncMock()),
            send_stream,
            MagicMock(),
            MagicMock(),
            cast(Any, MagicMock(start_soon=lambda *_a, **_kw: None)),
            "CR-Worcester",
        )

    mock_mbta_client.get_headsign.assert_not_awaited()
    assert len(sent) == 1
    assert sent[0].headsign is None


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__])
