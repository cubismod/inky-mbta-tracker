import logging
from copy import copy
from typing import Any, cast

from uvicorn.logging import AccessFormatter
from uvicorn_worker import UvicornWorker


class NginxAccessFormatter(AccessFormatter):
    """Format Uvicorn access records in an nginx-style combined log shape."""

    def formatMessage(self, record: logging.LogRecord) -> str:
        recordcopy = copy(record)
        (
            client_addr,
            method,
            full_path,
            http_version,
            status_code,
        ) = cast(tuple[str, str, str, str, int], recordcopy.args)

        remote_addr, remote_port = _split_client_addr(str(client_addr))
        request_line = f"{method} {full_path} HTTP/{http_version}"
        x_forwarded_for = remote_addr if remote_port == "0" else "-"

        recordcopy.__dict__.update(
            {
                "remote_addr": remote_addr,
                "remote_user": "-",
                "request_line": request_line,
                "status_code": str(status_code),
                "body_bytes_sent": "-",
                "http_referer": "-",
                "http_user_agent": "-",
                "http_x_forwarded_for": x_forwarded_for,
            }
        )
        return super(AccessFormatter, self).formatMessage(recordcopy)


class NginxAccessUvicornWorker(UvicornWorker):
    """Gunicorn worker that applies the shared Uvicorn logging config."""

    CONFIG_KWARGS: dict[str, Any] = {
        **UvicornWorker.CONFIG_KWARGS,
        "log_config": "uvicorn_logging_config.json",
        "proxy_headers": True,
    }


def _split_client_addr(client_addr: str) -> tuple[str, str | None]:
    host, separator, port = client_addr.rpartition(":")
    if separator and port.isdigit():
        return host, port
    return client_addr, None
