import logging
import os
import pathlib
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class StopSetup(BaseModel):
    stop_id: str
    route_filter: str = Field(default="")
    direction_filter: int = Field(default=-1)
    transit_time_min: int
    schedule_only: bool = Field(default=False)
    show_on_display: bool = Field(default=True)
    route_substring_filter: Optional[str] = None


class OllamaConfig(BaseModel):
    """Configuration for Ollama AI summarizer"""

    enabled: bool = Field(default=False, description="Enable AI summarizer feature")
    base_url: str = Field(
        default="http://localhost:11434", description="Ollama API base URL"
    )
    model: str = Field(
        default="llama3.2:3b", description="Model to use for summarization"
    )
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    temperature: float = Field(
        default=0.1, description="Model temperature for generation"
    )
    cache_ttl: int = Field(
        default=300, description="Cache TTL for summaries in seconds"
    )


class FileOutputConfig(BaseModel):
    """Configuration for local file output of AI summaries"""

    enabled: bool = Field(
        default=False, description="Enable local file output for AI summaries"
    )
    output_directory: str = Field(
        default="./ai_summaries", description="Directory to save markdown files"
    )
    filename: str = Field(
        default="mbta_alerts_summary.md",
        description="Filename for the single updating markdown file",
    )
    include_timestamp: bool = Field(
        default=True, description="Include timestamp in the markdown content"
    )
    include_alert_count: bool = Field(
        default=True, description="Include alert count in the markdown content"
    )
    include_model_info: bool = Field(
        default=True, description="Include model information in the markdown content"
    )


class Config(BaseModel):
    stops: list[StopSetup]
    # fetches real-time vehicle information with the numbers referring to
    # the routes
    vehicles_by_route: Optional[list[str]] = None
    # git repo to store vehicle location information in
    vehicle_git_repo: Optional[str] = None
    vehicle_git_user: Optional[str] = None
    # git auth token to use when cloning
    vehicle_git_token: Optional[str] = None
    vehicle_git_email: Optional[str] = None
    # track prediction precaching settings
    enable_track_predictions: bool = Field(default=False)
    track_prediction_routes: Optional[list[str]] = None
    track_prediction_stations: Optional[list[str]] = None
    track_prediction_interval_hours: int = Field(default=2)
    # AI summarizer settings
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)
    # File output settings for AI summaries
    file_output: FileOutputConfig = Field(default_factory=FileOutputConfig)


def load_config() -> Config:
    conf_location = os.getenv("IMT_CONFIG", "./config.json")
    logger.debug(f"Loading config from: {conf_location}")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)

    logger.debug(
        f"Initial config loaded - file_output.enabled: {config.file_output.enabled}"
    )
    logger.debug(
        f"Initial config loaded - file_output.output_directory: {config.file_output.output_directory}"
    )
    logger.debug(
        f"Initial config loaded - file_output.filename: {config.file_output.filename}"
    )

    # Override with environment variables for Ollama config
    if os.getenv("OLLAMA_BASE_URL"):
        base_url = os.getenv("OLLAMA_BASE_URL")
        if base_url:
            config.ollama.base_url = base_url
            logger.debug(f"Overriding OLLAMA_BASE_URL with: {base_url}")
    if os.getenv("OLLAMA_MODEL"):
        model = os.getenv("OLLAMA_MODEL")
        if model:
            config.ollama.model = model
            logger.debug(f"Overriding OLLAMA_MODEL with: {model}")
    if os.getenv("OLLAMA_TIMEOUT"):
        timeout_str = os.getenv("OLLAMA_TIMEOUT")
        if timeout_str:
            config.ollama.timeout = int(timeout_str)
            logger.debug(f"Overriding OLLAMA_TIMEOUT with: {timeout_str}")
    if os.getenv("OLLAMA_TEMPERATURE"):
        temp_str = os.getenv("OLLAMA_TEMPERATURE")
        if temp_str:
            config.ollama.temperature = float(temp_str)
            logger.debug(f"Overriding OLLAMA_TEMPERATURE with: {temp_str}")
    if os.getenv("OLLAMA_MAX_RETRIES"):
        retries_str = os.getenv("OLLAMA_MAX_RETRIES")
        if retries_str:
            config.ollama.max_retries = int(retries_str)
            logger.debug(f"Overriding OLLAMA_MAX_RETRIES with: {retries_str}")
    if os.getenv("OLLAMA_CACHE_TTL"):
        ttl_str = os.getenv("OLLAMA_CACHE_TTL")
        if ttl_str:
            config.ollama.cache_ttl = int(ttl_str)
            logger.debug(f"Overriding OLLAMA_CACHE_TTL with: {ttl_str}")

    # Override with environment variables for file output config
    if os.getenv("IMT_FILE_OUTPUT_ENABLED"):
        enabled_str = os.getenv("IMT_FILE_OUTPUT_ENABLED")
        if enabled_str:
            config.file_output.enabled = enabled_str.lower() == "true"
            logger.debug(
                f"Overriding IMT_FILE_OUTPUT_ENABLED with: {enabled_str} -> {config.file_output.enabled}"
            )
    if os.getenv("IMT_FILE_OUTPUT_DIR"):
        output_dir = os.getenv("IMT_FILE_OUTPUT_DIR")
        if output_dir:
            config.file_output.output_directory = output_dir
            logger.debug(f"Overriding IMT_FILE_OUTPUT_DIR with: {output_dir}")
    if os.getenv("IMT_FILE_OUTPUT_FILENAME"):
        filename = os.getenv("IMT_FILE_OUTPUT_FILENAME")
        if filename:
            config.file_output.filename = filename
            logger.debug(f"Overriding IMT_FILE_OUTPUT_FILENAME with: {filename}")
    if os.getenv("IMT_FILE_OUTPUT_INCLUDE_TIMESTAMP"):
        timestamp_str = os.getenv("IMT_FILE_OUTPUT_INCLUDE_TIMESTAMP")
        if timestamp_str:
            config.file_output.include_timestamp = timestamp_str.lower() == "true"
            logger.debug(
                f"Overriding IMT_FILE_OUTPUT_INCLUDE_TIMESTAMP with: {timestamp_str} -> {config.file_output.include_timestamp}"
            )
    if os.getenv("IMT_FILE_OUTPUT_INCLUDE_ALERT_COUNT"):
        count_str = os.getenv("IMT_FILE_OUTPUT_INCLUDE_ALERT_COUNT")
        if count_str:
            config.file_output.include_alert_count = count_str.lower() == "true"
            logger.debug(
                f"Overriding IMT_FILE_OUTPUT_INCLUDE_ALERT_COUNT with: {count_str} -> {config.file_output.include_alert_count}"
            )
    if os.getenv("IMT_FILE_OUTPUT_INCLUDE_MODEL_INFO"):
        model_str = os.getenv("IMT_FILE_OUTPUT_INCLUDE_MODEL_INFO")
        if model_str:
            config.file_output.include_model_info = model_str.lower() == "true"
            logger.debug(
                f"Overriding IMT_FILE_OUTPUT_INCLUDE_MODEL_INFO with: {model_str} -> {config.file_output.include_model_info}"
            )

    logger.debug(f"Final config - file_output.enabled: {config.file_output.enabled}")
    logger.debug(
        f"Final config - file_output.output_directory: {config.file_output.output_directory}"
    )
    logger.debug(f"Final config - file_output.filename: {config.file_output.filename}")

    return config
