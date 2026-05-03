"""Load test the vehicle SSE endpoint with many concurrent clients."""

from dataclasses import dataclass, field
from urllib.parse import urljoin

import aiohttp
import anyio
import click
from anyio import Lock

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
class LoadStatsSnapshot:
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
    errors_by_reason: dict[str, int] = field(default_factory=dict)
    first_event_latencies_ms: list[float] = field(default_factory=list)


@dataclass
class LoadStats:
    lock: Lock = field(default_factory=Lock)
    snapshot_data: LoadStatsSnapshot = field(default_factory=LoadStatsSnapshot)

    async def mark_client_started(self) -> None:
        async with self.lock:
            self.snapshot_data.clients_started += 1

    async def mark_client_completed(self) -> None:
        async with self.lock:
            self.snapshot_data.clients_completed += 1

    async def mark_connected(self) -> None:
        async with self.lock:
            self.snapshot_data.connected += 1
            self.snapshot_data.peak_connected = max(
                self.snapshot_data.peak_connected,
                self.snapshot_data.connected,
            )

    async def mark_disconnected(self) -> None:
        async with self.lock:
            self.snapshot_data.connected = max(0, self.snapshot_data.connected - 1)

    async def mark_successful_connection(self, *, reconnect: bool) -> None:
        async with self.lock:
            self.snapshot_data.successful_connections += 1
            if reconnect:
                self.snapshot_data.reconnects += 1

    async def mark_error(self, reason: str) -> None:
        async with self.lock:
            self.snapshot_data.errors += 1
            self.snapshot_data.errors_by_reason[reason] = (
                self.snapshot_data.errors_by_reason.get(reason, 0) + 1
            )

    async def record_chunk(
        self,
        *,
        bytes_received: int,
        data_events: int,
        comment_events: int,
        first_event_latency_ms: float | None,
    ) -> None:
        async with self.lock:
            self.snapshot_data.bytes_received += bytes_received
            self.snapshot_data.data_events += data_events
            self.snapshot_data.comment_events += comment_events
            if first_event_latency_ms is not None:
                self.snapshot_data.first_event_latencies_ms.append(
                    first_event_latency_ms
                )

    async def snapshot(self) -> LoadStatsSnapshot:
        async with self.lock:
            return LoadStatsSnapshot(
                clients_started=self.snapshot_data.clients_started,
                clients_completed=self.snapshot_data.clients_completed,
                connected=self.snapshot_data.connected,
                peak_connected=self.snapshot_data.peak_connected,
                reconnects=self.snapshot_data.reconnects,
                successful_connections=self.snapshot_data.successful_connections,
                data_events=self.snapshot_data.data_events,
                comment_events=self.snapshot_data.comment_events,
                bytes_received=self.snapshot_data.bytes_received,
                errors=self.snapshot_data.errors,
                errors_by_reason=dict(self.snapshot_data.errors_by_reason),
                first_event_latencies_ms=list(
                    self.snapshot_data.first_event_latencies_ms
                ),
            )


async def sse_client(
    client_id: int,
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    deadline: float,
    reconnect_delay: float,
    stats: LoadStats,
) -> None:
    await stats.mark_client_started()
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
                    await stats.mark_error(f"http_{response.status}")
                    await response.read()
                    await sleep_until_retry_or_deadline(reconnect_delay, deadline)
                    continue

                await stats.mark_connected()
                await stats.mark_successful_connection(reconnect=first_event_seen)
                buffer = ""
                try:
                    async for chunk in response.content.iter_chunked(4096):
                        if anyio.current_time() >= deadline:
                            break

                        buffer += chunk.decode("utf-8", errors="replace")
                        (
                            buffer,
                            first_event_seen,
                            data_events,
                            comment_events,
                            first_event_latency_ms,
                        ) = handle_sse_buffer(
                            buffer,
                            connected_at,
                            first_event_seen,
                        )
                        await stats.record_chunk(
                            bytes_received=len(chunk),
                            data_events=data_events,
                            comment_events=comment_events,
                            first_event_latency_ms=first_event_latency_ms,
                        )
                finally:
                    await stats.mark_disconnected()
        except TimeoutError:
            await stats.mark_error("timeout")
        except aiohttp.ClientConnectorError as exc:
            await stats.mark_error(format_exception_reason(exc.os_error))
        except aiohttp.ClientError as exc:
            await stats.mark_error(format_exception_reason(exc))

        if anyio.current_time() < deadline:
            await sleep_until_retry_or_deadline(reconnect_delay, deadline)

    await stats.mark_client_completed()
    _ = client_id


async def sleep_until_retry_or_deadline(delay: float, deadline: float) -> None:
    sleep_for = min(delay, max(0, deadline - anyio.current_time()))
    await anyio.sleep(sleep_for)


def handle_sse_buffer(
    buffer: str,
    connected_at: float,
    first_event_seen: bool,
) -> tuple[str, bool, int, int, float | None]:
    data_events = 0
    comment_events = 0
    first_event_latency_ms: float | None = None
    while "\n\n" in buffer:
        frame, buffer = buffer.split("\n\n", 1)
        if not frame:
            continue

        if frame.startswith(":"):
            comment_events += 1
            continue

        if any(line.startswith("data:") for line in frame.splitlines()):
            data_events += 1
            if not first_event_seen:
                first_event_latency_ms = (anyio.current_time() - connected_at) * 1000
                first_event_seen = True

    return buffer, first_event_seen, data_events, comment_events, first_event_latency_ms


async def reporter(
    stats: LoadStats, started_at: float, deadline: float, interval: float
) -> None:
    while anyio.current_time() < deadline:
        sleep_for = min(interval, max(0, deadline - anyio.current_time()))
        await anyio.sleep(sleep_for)
        if anyio.current_time() >= deadline:
            break
        await print_report(stats, started_at, final=False)


async def print_report(stats: LoadStats, started_at: float, *, final: bool) -> None:
    snapshot = await stats.snapshot()
    elapsed = max(0.001, anyio.current_time() - started_at)
    first_event_p95 = percentile(snapshot.first_event_latencies_ms, 95)
    prefix = "final" if final else "progress"
    top_errors = format_top_errors(snapshot.errors_by_reason)
    print(
        f"{prefix}: elapsed={elapsed:.1f}s "
        f"connected={snapshot.connected} peak={snapshot.peak_connected} "
        f"started={snapshot.clients_started} completed={snapshot.clients_completed} "
        f"events={snapshot.data_events} comments={snapshot.comment_events} "
        f"bytes={snapshot.bytes_received} "
        f"events/s={snapshot.data_events / elapsed:.2f} "
        f"MiB/s={snapshot.bytes_received / elapsed / 1024 / 1024:.2f} "
        f"errors={snapshot.errors} reconnects={snapshot.reconnects} "
        f"first_event_p95_ms={first_event_p95:.1f} "
        f"top_errors={top_errors}"
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

    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=config.request_timeout,
        sock_read=config.request_timeout,
    )
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

    await print_final_report(config, stats, started_at)


async def print_final_report(
    config: LoadTestConfig, stats: LoadStats, started_at: float
) -> None:
    snapshot = await stats.snapshot()
    elapsed = max(0.001, anyio.current_time() - started_at)
    first_event_latencies = snapshot.first_event_latencies_ms
    total_events = snapshot.data_events + snapshot.comment_events
    success_rate = (
        snapshot.successful_connections / snapshot.clients_started * 100
        if snapshot.clients_started
        else 0
    )

    rows = [
        ("URL", urljoin(config.url, "")),
        ("Mode", "delta" if config.delta else "full"),
        ("Frequent buses", str(config.frequent_buses)),
        ("Requested clients", str(config.clients)),
        ("Started clients", str(snapshot.clients_started)),
        ("Completed clients", str(snapshot.clients_completed)),
        ("Successful connections", str(snapshot.successful_connections)),
        ("Connection success rate", f"{success_rate:.1f}%"),
        ("Reconnects", str(snapshot.reconnects)),
        ("Errors", str(snapshot.errors)),
        ("Top errors", format_top_errors(snapshot.errors_by_reason)),
        ("Peak connected", str(snapshot.peak_connected)),
        ("Elapsed", f"{elapsed:.1f}s"),
        ("Data events", str(snapshot.data_events)),
        ("Comment events", str(snapshot.comment_events)),
        ("Total SSE frames", str(total_events)),
        ("Data events/sec", f"{snapshot.data_events / elapsed:.2f}"),
        ("Frames/sec", f"{total_events / elapsed:.2f}"),
        ("Bytes received", format_bytes(snapshot.bytes_received)),
        ("Throughput", f"{format_bytes(snapshot.bytes_received / elapsed)}/s"),
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


def format_top_errors(errors_by_reason: dict[str, int]) -> str:
    if not errors_by_reason:
        return "none"
    top_errors = sorted(
        errors_by_reason.items(), key=lambda item: item[1], reverse=True
    )[:5]
    return ", ".join(f"{reason}:{count}" for reason, count in top_errors)


def format_exception_reason(exc: BaseException) -> str:
    if isinstance(exc, OSError):
        errno = exc.errno
        strerror = exc.strerror
        if errno is not None and strerror:
            return f"{type(exc).__name__}[{errno}:{strerror}]"
        if errno is not None:
            return f"{type(exc).__name__}[{errno}]"
        if strerror:
            return f"{type(exc).__name__}[{strerror}]"
    return type(exc).__name__


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
    help="HTTP socket connect/read timeout in seconds.",
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
