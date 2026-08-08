from typing import cast
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from api.models import TotalsByLine
from config import Config
from redis.asyncio import Redis
from webhook.ntfy import service_start_stop_watcher


class _StopLoop(Exception):
    pass


def _totals(**fields: int) -> TotalsByLine:
    return TotalsByLine(**fields)


def _samples(
    *samples: TotalsByLine,
) -> list[tuple[None, TotalsByLine] | _StopLoop]:
    return [(None, s) for s in samples] + [_StopLoop()]


async def _run_watcher(mock_counts: AsyncMock, mock_send: AsyncMock) -> None:
    with pytest.raises(_StopLoop):
        await service_start_stop_watcher(cast(Redis, MagicMock()), Config())
    assert mock_send is not None


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_single_sample_dip_does_not_flip_service(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=0),
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=1),
    )

    await _run_watcher(mock_counts, mock_send)

    mock_send.assert_not_called()


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_sustained_stop_notifies_once(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=0),
        _totals(RL=0),
        _totals(RL=0),
        _totals(RL=0),
    )

    await _run_watcher(mock_counts, mock_send)

    assert mock_send.call_args_list == [call("Service RL stopped")]


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_sustained_start_notifies_once(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(RL=0),
        _totals(RL=0),
        _totals(RL=0),
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=1),
        _totals(RL=1),
    )

    await _run_watcher(mock_counts, mock_send)

    assert mock_send.call_args_list == [call("Service RL started")]


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_total_field_is_not_treated_as_a_line(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(RL=1, total=6),
        _totals(RL=1, total=6),
        _totals(RL=1, total=6),
        _totals(total=0),
        _totals(total=0),
        _totals(total=0),
    )

    await _run_watcher(mock_counts, mock_send)

    assert mock_send.call_args_list == [call("Service RL stopped")]


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_baseline_is_established_silently_on_first_samples(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(GL=1),
        _totals(GL=1),
        _totals(GL=1),
    )

    await _run_watcher(mock_counts, mock_send)

    mock_send.assert_not_called()


@patch("webhook.ntfy.sleep", new_callable=AsyncMock)
@patch("webhook.ntfy.send_ntfy_message", new_callable=AsyncMock)
@patch("webhook.ntfy.get_vehicle_route_counts")
@pytest.mark.anyio("asyncio")
async def test_isolated_changes_flip_independently(
    mock_counts: AsyncMock, mock_send: AsyncMock, mock_sleep: AsyncMock
) -> None:
    mock_counts.side_effect = _samples(
        _totals(RL=1, GL=1),
        _totals(RL=1, GL=1),
        _totals(RL=1, GL=1),
        _totals(RL=0, GL=1),
        _totals(RL=0, GL=1),
        _totals(RL=0, GL=1),
    )

    await _run_watcher(mock_counts, mock_send)

    assert mock_send.call_args_list == [call("Service RL stopped")]
