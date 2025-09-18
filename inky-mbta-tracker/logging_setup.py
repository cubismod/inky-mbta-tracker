import json
import logging
import os
import re
from datetime import UTC, datetime


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


class ColorFormatter(logging.Formatter):
    """ANSI color formatter for console logging.

    Applies a color to the level name depending on the record level.
    Only used when IMT_COLOR == 'true'.
    """

    RESET = "\033[0m"

    @staticmethod
    def _rgb(r: int, g: int, b: int) -> str:
        """Return ANSI 24-bit color escape for the given RGB."""
        return f"\033[38;2;{r};{g};{b}m"

    # Catppuccin Mocha palette (https://catppuccin.com/palette)
    # Selected accents per-level
    _GREEN = _rgb.__func__(166, 227, 161)  # Green
    _YELLOW = _rgb.__func__(249, 226, 175)  # Yellow
    _RED = _rgb.__func__(243, 139, 168)  # Red
    _BLUE = _rgb.__func__(137, 180, 250)  # Blue
    _SAPPHIRE = _rgb.__func__(116, 199, 236)  # Sapphire
    _MAROON = _rgb.__func__(235, 160, 172)  # Maroon

    COLORS: dict[int, str] = {
        logging.DEBUG: _SAPPHIRE,  # Debug → Sapphire
        logging.INFO: _GREEN,  # Info → Green
        logging.WARNING: _YELLOW,  # Warning → Yellow
        logging.ERROR: _RED,  # Error → Red
        logging.CRITICAL: _MAROON,  # Critical → Maroon (bold applied below)
    }

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        level_color = self.COLORS.get(record.levelno)
        if level_color:
            original_levelname = record.levelname
            original_filename = getattr(record, "filename", None)
            # Make CRITICAL bold
            if record.levelno == logging.CRITICAL:
                level_color = f"\033[1m{level_color}"
            record.levelname = f"{level_color}{original_levelname}{self.RESET}"
            # Colorize filename to help scan sources quickly
            if original_filename is not None:
                record.filename = f"{self._BLUE}{original_filename}{self.RESET}"
            try:
                formatted = super().format(record)
                # Colorize line number by replacing the "filename:lineno" segment
                if original_filename is not None:
                    colored_filename = f"{self._BLUE}{original_filename}{self.RESET}"
                    colored_lineno = f"{self._SAPPHIRE}{record.lineno}{self.RESET}"
                    needle = f"{colored_filename}:{record.lineno}"
                    if needle in formatted:
                        formatted = formatted.replace(
                            needle, f"{colored_filename}:{colored_lineno}", 1
                        )
                return formatted
            finally:
                # Restore to avoid side effects in other handlers/formatters
                record.levelname = original_levelname
                if original_filename is not None:
                    record.filename = original_filename
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter.

    Produces a single-line JSON object per record with stable keys.
    """

    _EXCLUDE_DEFAULT_KEYS: set[str] = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        # Base fields
        payload: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self._stringify_message(record),
            "file": getattr(record, "filename", None),
            "line": record.lineno,
            "function": getattr(record, "funcName", None),
        }

        # Extra fields (anything custom passed via logger.extra)
        for key, value in record.__dict__.items():
            if key not in self._EXCLUDE_DEFAULT_KEYS and not key.startswith("_"):
                try:
                    json.dumps(value)
                    payload[key] = value
                except (TypeError, ValueError):
                    # json.dumps may raise TypeError/ValueError for non-serializable values;
                    # stringify as a safe fallback rather than catching all exceptions.
                    payload[key] = str(value)

        # Exception information
        if record.exc_info:
            try:
                payload["exc_type"] = (
                    record.exc_info[0].__name__ if record.exc_info[0] else None
                )
                payload["exc_message"] = (
                    str(record.exc_info[1]) if record.exc_info[1] else None
                )
                payload["stack"] = self.formatException(record.exc_info)
            except (AttributeError, IndexError, TypeError):
                # If exc_info is malformed or attributes are missing, avoid raising
                # and provide a sentinel value.
                payload["stack"] = "<unavailable>"

        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _stringify_message(record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()  # respects %-formatting with args
        except (AttributeError, TypeError, ValueError):
            # If message formatting fails due to bad args or types, fall back to raw msg
            msg = str(record.msg)
        return msg


def _add_file_handler_if_configured(
    logger: logging.Logger, level: int, use_json: bool
) -> None:
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
            except (AttributeError, OSError):
                # If we can't access baseFilename due to attribute absence or OS errors,
                # fall through to add another handler safely.
                pass

    try:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        # Formatter depends on JSON mode
        if use_json:
            fh.setFormatter(JSONFormatter())
        else:
            # Include filename and line number for easier debugging
            fh.setFormatter(
                logging.Formatter("%(levelname)-8s %(filename)s:%(lineno)d %(message)s")
            )
        fh.addFilter(APIKeyFilter())
        logger.addHandler(fh)
    except (OSError, PermissionError) as err:  # pragma: no cover - defensive only
        # Only catch expected file/OS related errors when attempting to create
        # directories or open the file. Avoid swallowing unrelated exceptions.
        logging.getLogger(__name__).warning(
            f"Failed to set up file logging for IMT_LOG_FILE='{log_file}': {err}"
        )


def setup_logging() -> None:
    """Configure root logging once, with optional file logging and filters.

    Idempotent: safe to call multiple times across different entrypoints.
    """
    root = logging.getLogger()

    # Determine level from env (prefer IMT_LOG_LEVEL, fallback to LOG_LEVEL)
    level_name = os.getenv("IMT_LOG_LEVEL") or os.getenv("LOG_LEVEL", "INFO")
    level_name = level_name.upper()
    level = getattr(logging, level_name, logging.INFO)

    use_json = os.getenv("IMT_LOG_JSON", "false").lower() == "true"

    # If no handlers configured yet, initialize console logging
    if not root.handlers:
        if use_json:
            logging.basicConfig(level=level)
            # Set JSON formatter on the default stream handler
            for handler in root.handlers:
                if isinstance(handler, logging.StreamHandler):
                    handler.setFormatter(JSONFormatter())
        else:
            # Include filename and line number in console output as well
            logging.basicConfig(
                level=level,
                format="%(levelname)-8s %(filename)s:%(lineno)d %(message)s",
            )
    else:
        root.setLevel(level)
        # Keep existing handlers; do not re-add stream handler

    # Ensure API key filters are in place
    _ensure_api_filter(root)

    # If JSON mode is enabled and handlers pre-exist, ensure they use JSON format
    if use_json:
        for handler in root.handlers:
            handler.setFormatter(JSONFormatter())

    # Optionally colorize console output (disabled if JSON)
    if not use_json and os.getenv("IMT_COLOR", "false").lower() == "true":
        for handler in root.handlers:
            # Only colorize stream (console) handlers
            if isinstance(handler, logging.StreamHandler):
                # Use %(lineno)s so we can inject colorized string safely
                handler.setFormatter(
                    ColorFormatter(
                        "%(levelname)-8s %(filename)s:%(lineno)s %(message)s"
                    )
                )

    # Optionally add file handler
    _add_file_handler_if_configured(root, level, use_json)
