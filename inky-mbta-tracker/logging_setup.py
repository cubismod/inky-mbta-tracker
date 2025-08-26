import logging
import os
import re


class APIKeyFilter(logging.Filter):
    """Filter to remove API keys from log messages."""

    def __init__(self, name: str = "", mask: str = "[REDACTED]") -> None:
        super().__init__(name)
        self.mask = mask
        self.patterns = [
            re.compile(r"api_key=([^&\s]+)", re.IGNORECASE),
            re.compile(r"Bearer\s+([^\s]+)", re.IGNORECASE),
            re.compile(
                r"AUTH_TOKEN[\'\"]?\s*[:=]\s*[\'\"]?([^\'\"\s&]+)", re.IGNORECASE
            ),
        ]

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, "msg") and record.msg:
            record.msg = self._sanitize_message(str(record.msg))

        if hasattr(record, "args") and record.args:
            sanitized_args: list[object] = []
            for arg in record.args:
                if isinstance(arg, str):
                    sanitized_args.append(self._sanitize_message(arg))
                else:
                    sanitized_args.append(arg)
            record.args = tuple(sanitized_args)

        return True

    def _sanitize_message(self, message: str) -> str:
        for pattern in self.patterns:
            message = pattern.sub(
                lambda m: m.group(0).replace(m.group(1), self.mask), message
            )
        return message


def _ensure_api_filter(logger: logging.Logger) -> None:
    # Add API key filter to the root logger and all handlers if not present
    has_filter = any(isinstance(f, APIKeyFilter) for f in logger.filters)
    if not has_filter:
        logger.addFilter(APIKeyFilter())

    for handler in logger.handlers:
        if not any(isinstance(f, APIKeyFilter) for f in handler.filters):
            handler.addFilter(APIKeyFilter())


def _add_file_handler_if_configured(logger: logging.Logger, level: int) -> None:
    log_file = os.getenv("IMT_LOG_FILE")
    if not log_file:
        return

    # Skip if a FileHandler for the same file already exists
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            try:
                if os.path.abspath(getattr(h, "baseFilename")) == os.path.abspath(
                    log_file
                ):
                    return
            except Exception:
                # If we can't access baseFilename, fall through to add another handler safely
                pass

    try:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
        fh.addFilter(APIKeyFilter())
        logger.addHandler(fh)
    except Exception as err:  # pragma: no cover - defensive only
        logging.getLogger(__name__).warning(
            f"Failed to set up file logging for IMT_LOG_FILE='{log_file}': {err}"
        )


def setup_logging() -> None:
    """Configure root logging once, with optional file logging and filters.

    Idempotent: safe to call multiple times across different entrypoints.
    """
    root = logging.getLogger()

    # Determine level from env
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # If no handlers configured yet, initialize console logging
    if not root.handlers:
        logging.basicConfig(level=level, format="%(levelname)-8s %(message)s")
    else:
        root.setLevel(level)
        # Keep existing handlers; do not re-add stream handler

    # Ensure API key filters are in place
    _ensure_api_filter(root)

    # Optionally add file handler
    _add_file_handler_if_configured(root, level)
