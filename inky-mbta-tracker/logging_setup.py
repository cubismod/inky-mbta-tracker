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
            # Make CRITICAL bold
            if record.levelno == logging.CRITICAL:
                level_color = f"\033[1m{level_color}"
            record.levelname = f"{level_color}{original_levelname}{self.RESET}"
            try:
                return super().format(record)
            finally:
                # Restore to avoid side effects in other handlers/formatters
                record.levelname = original_levelname
        return super().format(record)


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

    # Optionally colorize console output
    if os.getenv("IMT_COLOR", "false").lower() == "true":
        for handler in root.handlers:
            # Only colorize stream (console) handlers
            if isinstance(handler, logging.StreamHandler):
                handler.setFormatter(ColorFormatter("%(levelname)-8s %(message)s"))

    # Optionally add file handler
    _add_file_handler_if_configured(root, level)
