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
    # Redis queue settings
    enable_queue: bool = Field(
        default=True, description="Enable Redis queue for Ollama commands"
    )
    queue_name: str = Field(default="ollama_commands", description="Redis queue name")
    max_workers: int = Field(default=2, description="Maximum number of queue workers")
    worker_poll_interval: float = Field(
        default=1.0, description="Worker poll interval in seconds"
    )
    max_concurrent_jobs: int = Field(
        default=1, description="Maximum concurrent jobs per worker"
    )
    # Reasoning model configuration
    enable_reasoning: bool = Field(
        default=False,
        description="Enable reasoning mode for models like Qwen that support <think> sections",
    )
    max_conversation_turns: int = Field(
        default=3,
        description="Maximum number of conversation turns for reasoning models",
    )
    enable_streaming: bool = Field(
        default=False, description="Enable streaming responses for real-time output"
    )
    reasoning_prompt_template: str = Field(
        default="<think>Let me analyze this step by step:</think>",
        description="Template for reasoning prompts",
    )
    extract_thinking_only: bool = Field(
        default=False,
        description="Extract only the thinking/reasoning part from responses",
    )
    # Advanced AI configuration
    min_similarity_threshold: float = Field(
        default=0.105,
        description="Minimum similarity threshold for response validation (0.0 to 1.0)",
    )
    validate_only_short_responses: bool = Field(
        default=True,
        description="Only apply similarity validation to responses with 1 sentence or less",
    )
    enable_summary_rewriting: bool = Field(
        default=True,
        description="Enable automatic summary rewriting for clarity and readability",
    )
    rewrite_sentence_tolerance: int = Field(
        default=1,
        description="Maximum allowed sentence count difference between original and rewritten summaries (0 = exact match)",
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
    validate_summaries: bool = Field(
        default=True,
        description="Validate summaries using Levenshtein similarity before saving",
    )
    min_similarity_threshold: float = Field(
        default=0.105,
        description="Minimum similarity threshold for summary validation (0.0 to 1.0)",
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

    # Override with environment variables for reasoning configuration
    if os.getenv("OLLAMA_ENABLE_REASONING"):
        reasoning_str = os.getenv("OLLAMA_ENABLE_REASONING")
        if reasoning_str:
            config.ollama.enable_reasoning = reasoning_str.lower() == "true"
            logger.debug(
                f"Overriding OLLAMA_ENABLE_REASONING with: {reasoning_str} -> {config.ollama.enable_reasoning}"
            )

    if os.getenv("OLLAMA_MAX_CONVERSATION_TURNS"):
        turns_str = os.getenv("OLLAMA_MAX_CONVERSATION_TURNS")
        if turns_str:
            config.ollama.max_conversation_turns = int(turns_str)
            logger.debug(f"Overriding OLLAMA_MAX_CONVERSATION_TURNS with: {turns_str}")

    if os.getenv("OLLAMA_ENABLE_STREAMING"):
        streaming_str = os.getenv("OLLAMA_ENABLE_STREAMING")
        if streaming_str:
            config.ollama.enable_streaming = streaming_str.lower() == "true"
            logger.debug(
                f"Overriding OLLAMA_ENABLE_STREAMING with: {streaming_str} -> {config.ollama.enable_streaming}"
            )

    if os.getenv("OLLAMA_REASONING_PROMPT_TEMPLATE"):
        template = os.getenv("OLLAMA_REASONING_PROMPT_TEMPLATE")
        if template:
            config.ollama.reasoning_prompt_template = template
            logger.debug(
                f"Overriding OLLAMA_REASONING_PROMPT_TEMPLATE with: {template}"
            )

    if os.getenv("OLLAMA_EXTRACT_THINKING_ONLY"):
        thinking_str = os.getenv("OLLAMA_EXTRACT_THINKING_ONLY")
        if thinking_str:
            config.ollama.extract_thinking_only = thinking_str.lower() == "true"
            logger.debug(
                f"Overriding OLLAMA_EXTRACT_THINKING_ONLY with: {thinking_str} -> {config.ollama.extract_thinking_only}"
            )

    # Override with environment variables for advanced AI configuration
    if os.getenv("OLLAMA_MIN_SIMILARITY_THRESHOLD"):
        similarity_str = os.getenv("OLLAMA_MIN_SIMILARITY_THRESHOLD")
        if similarity_str:
            try:
                config.ollama.min_similarity_threshold = float(similarity_str)
                logger.debug(
                    f"Overriding OLLAMA_MIN_SIMILARITY_THRESHOLD with: {similarity_str} -> {config.ollama.min_similarity_threshold}"
                )
            except ValueError:
                logger.warning(
                    f"Invalid similarity threshold value: {similarity_str}, using default"
                )

    if os.getenv("OLLAMA_VALIDATE_ONLY_SHORT_RESPONSES"):
        validate_str = os.getenv("OLLAMA_VALIDATE_ONLY_SHORT_RESPONSES")
        if validate_str:
            config.ollama.validate_only_short_responses = validate_str.lower() == "true"
            logger.debug(
                f"Overriding OLLAMA_VALIDATE_ONLY_SHORT_RESPONSES with: {validate_str} -> {config.ollama.validate_only_short_responses}"
            )

    if os.getenv("OLLAMA_ENABLE_SUMMARY_REWRITING"):
        rewrite_str = os.getenv("OLLAMA_ENABLE_SUMMARY_REWRITING")
        if rewrite_str:
            config.ollama.enable_summary_rewriting = rewrite_str.lower() == "true"
            logger.debug(
                f"Overriding OLLAMA_ENABLE_SUMMARY_REWRITING with: {rewrite_str} -> {config.ollama.enable_summary_rewriting}"
            )

    if os.getenv("OLLAMA_REWRITE_SENTENCE_TOLERANCE"):
        tolerance_str = os.getenv("OLLAMA_REWRITE_SENTENCE_TOLERANCE")
        if tolerance_str:
            config.ollama.rewrite_sentence_tolerance = int(tolerance_str)
            logger.debug(
                f"Overriding OLLAMA_REWRITE_SENTENCE_TOLERANCE with: {tolerance_str}"
            )

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

    # Override with environment variables for similarity validation
    if os.getenv("IMT_FILE_OUTPUT_VALIDATE_SUMMARIES"):
        validate_str = os.getenv("IMT_FILE_OUTPUT_VALIDATE_SUMMARIES")
        if validate_str:
            config.file_output.validate_summaries = validate_str.lower() == "true"
            logger.debug(
                f"Overriding IMT_FILE_OUTPUT_VALIDATE_SUMMARIES with: {validate_str} -> {config.file_output.validate_summaries}"
            )

    if os.getenv("IMT_FILE_OUTPUT_MIN_SIMILARITY"):
        similarity_str = os.getenv("IMT_FILE_OUTPUT_MIN_SIMILARITY")
        if similarity_str:
            try:
                config.file_output.min_similarity_threshold = float(similarity_str)
                logger.debug(
                    f"Overriding IMT_FILE_OUTPUT_MIN_SIMILARITY with: {similarity_str} -> {config.file_output.min_similarity_threshold}"
                )
            except ValueError:
                logger.warning(
                    f"Invalid similarity threshold value: {similarity_str}, using default"
                )

    logger.debug(f"Final config - file_output.enabled: {config.file_output.enabled}")
    logger.debug(
        f"Final config - file_output.output_directory: {config.file_output.output_directory}"
    )
    logger.debug(f"Final config - file_output.filename: {config.file_output.filename}")
    logger.debug(
        f"Final config - file_output.validate_summaries: {config.file_output.validate_summaries}"
    )
    logger.debug(
        f"Final config - file_output.min_similarity_threshold: {config.file_output.min_similarity_threshold}"
    )

    return config
