#!/usr/bin/env python3
"""Load test the vehicle SSE endpoint with many concurrent clients."""

from dataclasses import dataclass
from urllib.parse import urljoin

import aiohttp
import anyio
import click

DEFAULT_SSE_URL = "http://localhost:8000/vehicles/stream"


@dataclass(frozen=True)
class LoadTestConfig:
    url: str
    clients: int
    duration: float
    ramp_seconds: float
    report_interval: float
    delta: bool
    frequent_buses: bool
    reconnect_delay: float
    request_timeout: float


@dataclass
class LoadStats:
    clients_started: int = 0
    clients_completed: int = 0
    connected: int = 0
    peak_connected: int = 0
    reconnects: int = 0
    successful_connections: int = 0
    data_events: int = 0
    comment_events: int = 0
    bytes_received: int = 0
    errors: int = 0
    first_event_latencies_ms: list[float] | None = None

    def __post_init__(self) -> None:
        self.first_event_latencies_ms = []

    def mark_connected(self) -> None:
        self.connected += 1
        self.peak_connected = max(self.peak_connected, self.connected)

    def mark_disconnected(self) -> None:
        self.connected = max(0, self.connected - 1)

    def mark_first_event(self, latency_ms: float) -> None:
        if self.first_event_latencies_ms is not None:
            self.first_event_latencies_ms.append(latency_ms)


async def sse_client(
    client_id: int,
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    deadline: float,
    reconnect_delay: float,
    stats: LoadStats,
) -> None:
    stats.clients_started += 1
    first_event_seen = False

    while anyio.current_time() < deadline:
        connected_at = anyio.current_time()
        try:
            async with session.get(
                url,
                params=params,
                headers={"Accept": "text/event-stream"},
            ) as response:
                if response.status != 200:
                    stats.errors += 1
                    await response.read()
                    await anyio.sleep(reconnect_delay)
                    continue

                stats.mark_connected()
                stats.successful_connections += 1
                if first_event_seen:
                    stats.reconnects += 1
                buffer = ""
                try:
                    async for chunk in response.content.iter_chunked(4096):
                        if anyio.current_time() >= deadline:
                            break

                        stats.bytes_received += len(chunk)
                        buffer += chunk.decode("utf-8", errors="replace")
                        buffer, first_event_seen = handle_sse_buffer(
                            buffer, connected_at, first_event_seen, stats
                        )
                finally:
                    stats.mark_disconnected()
        except (aiohttp.ClientError, TimeoutError):
            stats.errors += 1

        if anyio.current_time() < deadline:
            sleep_for = min(reconnect_delay, max(0, deadline - anyio.current_time()))
            await anyio.sleep(sleep_for)

    stats.clients_completed += 1
    _ = client_id


def handle_sse_buffer(
    buffer: str,
    connected_at: float,
    first_event_seen: bool,
    stats: LoadStats,
) -> tuple[str, bool]:
    while "\n\n" in buffer:
        frame, buffer = buffer.split("\n\n", 1)
        if not frame:
            continue

        if frame.startswith(":"):
            stats.comment_events += 1
            continue

        if any(line.startswith("data:") for line in frame.splitlines()):
            stats.data_events += 1
            if not first_event_seen:
                latency_ms = (anyio.current_time() - connected_at) * 1000
                stats.mark_first_event(latency_ms)
                first_event_seen = True

    return buffer, first_event_seen


async def reporter(
    stats: LoadStats, started_at: float, deadline: float, interval: float
) -> None:
    while anyio.current_time() < deadline:
        sleep_for = min(interval, max(0, deadline - anyio.current_time()))
        await anyio.sleep(sleep_for)
        if anyio.current_time() >= deadline:
            break
        print_report(stats, started_at, final=False)


def print_report(stats: LoadStats, started_at: float, *, final: bool) -> None:
    elapsed = max(0.001, anyio.current_time() - started_at)
    first_event_p95 = percentile(stats.first_event_latencies_ms or [], 95)
    prefix = "final" if final else "progress"
    print(
        f"{prefix}: elapsed={elapsed:.1f}s "
        f"connected={stats.connected} peak={stats.peak_connected} "
        f"started={stats.clients_started} completed={stats.clients_completed} "
        f"events={stats.data_events} comments={stats.comment_events} "
        f"bytes={stats.bytes_received} "
        f"events/s={stats.data_events / elapsed:.2f} "
        f"MiB/s={stats.bytes_received / elapsed / 1024 / 1024:.2f} "
        f"errors={stats.errors} reconnects={stats.reconnects} "
        f"first_event_p95_ms={first_event_p95:.1f}"
    )


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, round((pct / 100) * (len(sorted_values) - 1)))
    return sorted_values[index]


async def async_main(config: LoadTestConfig) -> None:
    params: dict[str, str] = {}
    if config.delta:
        params["delta"] = "true"
    if config.frequent_buses:
        params["frequent_buses"] = "true"

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=config.request_timeout)
    connector = aiohttp.TCPConnector(limit=0, force_close=False)
    stats = LoadStats()
    started_at = anyio.current_time()
    deadline = started_at + config.duration
    ramp_delay = config.ramp_seconds / config.clients if config.ramp_seconds > 0 else 0

    print(
        "starting: "
        f"url={urljoin(config.url, '')} clients={config.clients} "
        f"duration={config.duration}s ramp={config.ramp_seconds}s "
        f"delta={config.delta} frequent_buses={config.frequent_buses}"
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with anyio.create_task_group() as tg:
            tg.start_soon(reporter, stats, started_at, deadline, config.report_interval)
            for client_id in range(config.clients):
                if anyio.current_time() >= deadline:
                    break
                tg.start_soon(
                    sse_client,
                    client_id,
                    session,
                    config.url,
                    params,
                    deadline,
                    config.reconnect_delay,
                    stats,
                )
                if ramp_delay:
                    await anyio.sleep(ramp_delay)

    print_final_report(config, stats, started_at)


def print_final_report(
    config: LoadTestConfig, stats: LoadStats, started_at: float
) -> None:
    elapsed = max(0.001, anyio.current_time() - started_at)
    first_event_latencies = stats.first_event_latencies_ms or []
    total_events = stats.data_events + stats.comment_events
    success_rate = (
        stats.successful_connections / stats.clients_started * 100
        if stats.clients_started
        else 0
    )

    rows = [
        ("URL", urljoin(config.url, "")),
        ("Mode", "delta" if config.delta else "full"),
        ("Frequent buses", str(config.frequent_buses)),
        ("Requested clients", str(config.clients)),
        ("Started clients", str(stats.clients_started)),
        ("Completed clients", str(stats.clients_completed)),
        ("Successful connections", str(stats.successful_connections)),
        ("Connection success rate", f"{success_rate:.1f}%"),
        ("Reconnects", str(stats.reconnects)),
        ("Errors", str(stats.errors)),
        ("Peak connected", str(stats.peak_connected)),
        ("Elapsed", f"{elapsed:.1f}s"),
        ("Data events", str(stats.data_events)),
        ("Comment events", str(stats.comment_events)),
        ("Total SSE frames", str(total_events)),
        ("Data events/sec", f"{stats.data_events / elapsed:.2f}"),
        ("Frames/sec", f"{total_events / elapsed:.2f}"),
        ("Bytes received", format_bytes(stats.bytes_received)),
        ("Throughput", f"{format_bytes(stats.bytes_received / elapsed)}/s"),
        ("First event p50", format_ms(percentile(first_event_latencies, 50))),
        ("First event p95", format_ms(percentile(first_event_latencies, 95))),
        ("First event p99", format_ms(percentile(first_event_latencies, 99))),
        ("First event max", format_ms(max(first_event_latencies, default=0))),
    ]

    width = max(len(label) for label, _value in rows)
    line = "=" * (width + 35)
    print()
    print(line)
    print("SSE Load Test Results")
    print(line)
    for label, value in rows:
        print(f"{label:<{width}}  {value}")
    print(line)


def format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}" if unit != "B" else f"{size:.0f} {unit}"
        size /= 1024
    return f"{size:.2f} GiB"


def format_ms(value: float) -> str:
    return f"{value:.1f} ms"


@click.command(context_settings={"show_default": True})
@click.option(
    "--url",
    default=DEFAULT_SSE_URL,
    help="SSE endpoint URL.",
)
@click.option(
    "--clients",
    type=click.IntRange(min=1),
    default=100,
    help="Number of concurrent SSE clients.",
)
@click.option(
    "--duration",
    type=click.FloatRange(min=0, min_open=True),
    default=60.0,
    help="Test duration in seconds.",
)
@click.option(
    "--ramp-seconds",
    type=click.FloatRange(min=0),
    default=5.0,
    help="Seconds over which clients are started.",
)
@click.option(
    "--report-interval",
    type=click.FloatRange(min=0, min_open=True),
    default=5.0,
    help="Seconds between progress reports.",
)
@click.option(
    "--delta",
    is_flag=True,
    help="Request delta=true.",
)
@click.option(
    "--frequent-buses",
    is_flag=True,
    help="Request frequent_buses=true.",
)
@click.option(
    "--reconnect-delay",
    type=click.FloatRange(min=0),
    default=1.0,
    help="Delay before reconnecting a dropped client.",
)
@click.option(
    "--request-timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=30.0,
    help="HTTP connect/read timeout in seconds.",
)
def main(
    url: str,
    clients: int,
    duration: float,
    ramp_seconds: float,
    report_interval: float,
    delta: bool,
    frequent_buses: bool,
    reconnect_delay: float,
    request_timeout: float,
) -> None:
    """Open concurrent SSE clients against /vehicles/stream."""
    config = LoadTestConfig(
        url=url,
        clients=clients,
        duration=duration,
        ramp_seconds=ramp_seconds,
        report_interval=report_interval,
        delta=delta,
        frequent_buses=frequent_buses,
        reconnect_delay=reconnect_delay,
        request_timeout=request_timeout,
    )
    anyio.run(async_main, config)


if __name__ == "__main__":
    main()
