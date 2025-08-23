import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from mbta_responses import AlertResource
from ollama import AsyncClient
from pydantic import BaseModel, Field
from utils import get_redis

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Status of a summary job"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPriority(Enum):
    """Priority levels for summary jobs"""

    LOW = 1  # Background refresh, non-urgent
    NORMAL = 2  # Regular alert updates
    HIGH = 3  # New critical alerts
    URGENT = 4  # Emergency alerts


@dataclass
class SummaryJob:
    """A job to summarize alerts"""

    id: str
    alerts: List[AlertResource]
    priority: JobPriority
    created_at: datetime
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    backoff_until: Optional[datetime] = None
    config: Dict[str, Any] = field(default_factory=dict)


class SummarizationRequest(BaseModel):
    """Request model for alert summarization"""

    alerts: List[AlertResource]
    include_route_info: bool = Field(
        default=True, description="Include route information in summary"
    )
    include_severity: bool = Field(
        default=True, description="Include severity information in summary"
    )
    format: str = Field(
        default="text",
        description="Output format for the summary (text, markdown, json)",
    )
    style: str = Field(
        default="comprehensive",
        description="Summary style - 'comprehensive' (detailed) or 'concise' (brief, 1-2 sentences per alert)",
    )


class SummarizationResponse(BaseModel):
    """Response model for alert summarization"""

    summary: str
    alert_count: int
    model_used: str
    processing_time_ms: float


class OllamaConfig(BaseModel):
    """Configuration for Ollama AI summarizer"""

    base_url: str = Field(
        default="http://localhost:11434", description="Ollama API base URL"
    )
    model: str = Field(
        default="llama3.2:1b", description="Model to use for summarization"
    )
    timeout: int = Field(default=30, description="Request timeout in seconds")
    temperature: float = Field(
        default=0.1, description="Model temperature for generation"
    )
    max_retries: int = Field(
        default=3, description="Maximum retry attempts for API calls"
    )
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
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


class SummaryJobQueue:
    """
    Job queue system for AI summaries with intelligent backoff.

    This system manages the processing of summary requests to prevent
    overwhelming the CPU and provides better user experience.
    """

    def __init__(
        self,
        max_concurrent_jobs: int = 2,
        max_queue_size: int = 100,
        summarizer: Optional["AISummarizer"] = None,
    ):
        """
        Initialize the job queue.

        Args:
            max_concurrent_jobs: Maximum jobs running simultaneously (CPU-limited)
            max_queue_size: Maximum jobs in queue to prevent memory issues
            summarizer: Reference to the parent AISummarizer for API calls
        """
        self.max_queue_size = 100
        self.max_concurrent_jobs = (
            1  # Disable concurrent processing - queue Ollama requests
        )
        self.summarizer = summarizer  # Reference to parent summarizer
        self.jobs: Dict[str, SummaryJob] = {}
        self.pending_jobs: List[str] = []
        self.running_jobs: set = set()
        self.completed_jobs: Dict[str, SummaryJob] = {}
        self._lock = asyncio.Lock()
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self.redis = get_redis()

        # CPU-aware backoff settings for Intel i5-9400
        self.base_backoff_seconds = 5.0  # Base delay between jobs
        self.max_backoff_seconds = 60.0  # Maximum delay
        self.backoff_multiplier = 1.5  # Exponential backoff multiplier
        self.cpu_threshold = 0.7  # CPU usage threshold to trigger backoff

    async def start(self) -> None:
        """Start the job queue worker."""
        if self._running:
            return

        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("Summary job queue started")

    async def stop(self) -> None:
        """Stop the job queue worker."""
        if not self._running:
            return

        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Summary job queue stopped")

    async def add_job(
        self,
        alerts: List[AlertResource],
        priority: JobPriority = JobPriority.NORMAL,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a summary job to the queue.

        Args:
            alerts: Alerts to summarize
            priority: Job priority level
            config: Additional configuration for summarization

        Returns:
            Job ID

        Raises:
            ValueError: If queue is full
        """
        # First check if we already have a cached summary
        cached_summary = await self._get_summary_from_redis(alerts)
        if cached_summary:
            logger.info("Found cached summary, skipping job creation")
            # Return a special job ID indicating cached result
            return f"cached_{self._create_alerts_hash(alerts)}"

        async with self._lock:
            if len(self.jobs) + len(self.pending_jobs) >= self.max_queue_size:
                raise ValueError("Job queue is full")

            job_id = f"summary_{int(time.time() * 1000)}_{len(self.jobs)}"
            job = SummaryJob(
                id=job_id,
                alerts=alerts,
                priority=priority,
                created_at=datetime.now(),
                config=config or {},
            )

            self.jobs[job_id] = job
            self.pending_jobs.append(job_id)

            # Sort by priority (higher priority first)
            self.pending_jobs.sort(
                key=lambda jid: self.jobs[jid].priority.value, reverse=True
            )

            logger.info(f"Added summary job {job_id} with priority {priority.name}")
            return job_id

    async def get_job_status(self, job_id: str) -> Optional[SummaryJob]:
        """Get the status of a job."""
        async with self._lock:
            if job_id in self.jobs:
                return self.jobs[job_id]
            elif job_id in self.completed_jobs:
                return self.completed_jobs[job_id]
            return None

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        async with self._lock:
            if job_id in self.pending_jobs:
                self.pending_jobs.remove(job_id)
                if job_id in self.jobs:
                    self.jobs[job_id].status = JobStatus.CANCELLED
                    self.completed_jobs[job_id] = self.jobs.pop(job_id)
                logger.info(f"Cancelled summary job {job_id}")
                return True
            return False

    async def _store_summary_in_redis(
        self, alerts: List[AlertResource], summary: str, config: Dict[str, Any]
    ) -> None:
        """Store the generated summary in Redis with appropriate TTL."""
        try:
            # Create a hash of the alerts to use as a cache key
            alerts_hash = self._create_alerts_hash(alerts)
            cache_key = f"ai_summary:{alerts_hash}"

            # Store summary data
            summary_data = {
                "summary": summary,
                "alert_count": len(alerts),
                "alerts_hash": alerts_hash,
                "generated_at": datetime.now().isoformat(),
                "config": config,
                "ttl": 3600,  # 1 hour default TTL
            }

            # Store in Redis with TTL
            await self.redis.setex(
                cache_key,
                3600,  # 1 hour TTL
                json.dumps(summary_data),
            )

            # Also store a mapping from alerts to summary hash for quick lookup
            alerts_key = f"alerts_to_summary:{alerts_hash}"
            await self.redis.setex(alerts_key, 3600, cache_key)

            logger.info(f"Stored summary in Redis with key: {cache_key}")

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to store summary in Redis due to connection issue: {e}"
            )
        except ValueError as e:
            logger.error(f"Failed to store summary in Redis due to invalid data: {e}")
        except Exception as e:
            logger.error(
                f"Failed to store summary in Redis due to unexpected error: {e}"
            )

    async def _get_summary_from_redis(
        self, alerts: List[AlertResource]
    ) -> Optional[str]:
        """Retrieve a cached summary from Redis if available."""
        try:
            alerts_hash = self._create_alerts_hash(alerts)
            cache_key = f"ai_summary:{alerts_hash}"

            cached_data = await self.redis.get(cache_key)
            if cached_data:
                summary_data = json.loads(cached_data)
                logger.info(f"Retrieved cached summary from Redis: {cache_key}")
                return summary_data.get("summary")

            return None

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to retrieve summary from Redis due to connection issue: {e}"
            )
            return None
        except ValueError as e:
            logger.error(
                f"Failed to retrieve summary from Redis due to invalid data: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"Failed to retrieve summary from Redis due to unexpected error: {e}"
            )
            return None

    def _create_alerts_hash(self, alerts: List[AlertResource]) -> str:
        """Create a hash of the alerts for caching purposes."""
        # Create a deterministic string representation of the alerts
        alert_strings = []
        for alert in sorted(alerts, key=lambda a: a.id):
            attrs = alert.attributes
            alert_str = f"{alert.id}:{attrs.header}:{attrs.severity}:{attrs.effect}"
            alert_strings.append(alert_str)

        # Create hash from concatenated strings
        combined = "|".join(alert_strings)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _is_error_message(self, summary: str) -> bool:
        """
        Check if a summary is an error message that shouldn't be cached.

        Args:
            summary: The summary text to check

        Returns:
            True if it's an error message, False if it's a valid summary
        """
        error_indicators = [
            "AI service temporarily unavailable",
            "Please try again later",
            "Connection error",
            "Data error",
            "Runtime error",
            "Unexpected error",
            "Unable to reach AI service",
            "Invalid alert format",
            "AI summarizer not available",
            "No alerts to summarize",
            "Error generating summary",
            "Failed to generate summary",
        ]

        summary_lower = summary.lower()
        return any(indicator.lower() in summary_lower for indicator in error_indicators)

    async def _worker_loop(self) -> None:
        """Main worker loop that processes jobs."""
        while self._running:
            try:
                # Check if we can process more jobs (only one at a time to queue Ollama requests)
                if (
                    len(self.running_jobs) < self.max_concurrent_jobs
                    and self.pending_jobs
                ):
                    job_id = self.pending_jobs.pop(0)
                    asyncio.create_task(self._process_job(job_id))
                    logger.debug(
                        f"Queued job {job_id} for sequential Ollama processing"
                    )

                # Clean up completed jobs (keep last 100)
                if len(self.completed_jobs) > 100:
                    oldest_jobs = sorted(
                        self.completed_jobs.keys(),
                        key=lambda k: self.completed_jobs[k].completed_at
                        or datetime.now(),
                    )
                    for old_job_id in oldest_jobs[:-100]:
                        del self.completed_jobs[old_job_id]

                # Adaptive backoff based on system load
                await self._adaptive_backoff()

            except (ConnectionError, TimeoutError) as e:
                logger.error(f"Connection error in job worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)
            except ValueError as e:
                logger.error(f"Data error in job worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in job worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _adaptive_backoff(self) -> None:
        """Implement adaptive backoff based on system load and job queue status."""
        # Simple backoff: longer delay if many jobs are running
        active_jobs = len(self.running_jobs) + len(self.pending_jobs)

        if active_jobs > self.max_concurrent_jobs * 2:
            # Heavy load - longer delay
            delay = min(self.base_backoff_seconds * 2, self.max_backoff_seconds)
        elif active_jobs > self.max_concurrent_jobs:
            # Moderate load - normal delay
            delay = self.base_backoff_seconds
        else:
            # Light load - minimal delay
            delay = self.base_backoff_seconds * 0.5

        await asyncio.sleep(delay)

    async def _process_job(self, job_id: str) -> None:
        """Process a single summary job."""
        if job_id not in self.jobs:
            return

        job = self.jobs[job_id]
        job.status = JobStatus.PROCESSING
        job.started_at = datetime.now()
        self.running_jobs.add(job_id)

        try:
            # Check if job should be delayed due to backoff
            if job.backoff_until and datetime.now() < job.backoff_until:
                remaining = (job.backoff_until - datetime.now()).total_seconds()
                logger.info(f"Job {job_id} delayed for {remaining:.1f}s due to backoff")
                await asyncio.sleep(remaining)

            # Process the job (sequential - only one Ollama request at a time)
            logger.info(
                f"Processing summary job {job_id} (sequential Ollama processing)"
            )

            # Generate the actual AI summary
            summary = await self._generate_summary(job.alerts, job.config)

            # Only store successful summaries in Redis (not error messages)
            if not self._is_error_message(summary):
                await self._store_summary_in_redis(job.alerts, summary, job.config)
                logger.info(f"Stored successful summary in Redis for job {job_id}")
            else:
                logger.warning(
                    f"Not storing error message in Redis for job {job_id}: {summary}"
                )

            job.result = summary
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()

            logger.info(f"Completed summary job {job_id}")

        except Exception as e:
            job.error = str(e)
            job.status = JobStatus.FAILED

            # Implement retry logic with exponential backoff
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                backoff_delay = min(
                    self.base_backoff_seconds
                    * (self.backoff_multiplier**job.retry_count),
                    self.max_backoff_seconds,
                )
                job.backoff_until = datetime.now() + timedelta(seconds=backoff_delay)
                job.status = JobStatus.PENDING

                # Re-add to pending queue
                async with self._lock:
                    self.pending_jobs.append(job_id)
                    # Re-sort by priority
                    self.pending_jobs.sort(
                        key=lambda jid: self.jobs[jid].priority.value, reverse=True
                    )

                logger.info(
                    f"Retrying job {job_id} in {backoff_delay:.1f}s (attempt {job.retry_count})"
                )
            else:
                logger.error(
                    f"Job {job_id} failed after {job.max_retries} retries: {e}"
                )

        finally:
            self.running_jobs.discard(job_id)

            # Move completed job to completed_jobs
            async with self._lock:
                if job_id in self.jobs:
                    self.completed_jobs[job_id] = self.jobs.pop(job_id)

    async def _generate_summary(
        self, alerts: List[AlertResource], config: Dict[str, Any]
    ) -> str:
        """Generate a summary for the alerts using the AI summarizer."""
        if not alerts:
            return "No alerts to summarize."

        try:
            # Use the parent summarizer to generate the actual summary
            if self.summarizer:
                # Determine if this is an individual alert summary (single alert)
                # Individual summaries should use concise style, group summaries use comprehensive
                style = "concise" if len(alerts) == 1 else "comprehensive"

                # Create a summarization request
                request = SummarizationRequest(
                    alerts=alerts,
                    include_route_info=config.get("include_route_info", True),
                    include_severity=config.get("include_severity", True),
                    style=style,
                )

                # For individual alerts, use sentence_limit if specified in config
                if len(alerts) == 1 and "sentence_limit" in config:
                    # Call the individual alert method with sentence_limit
                    return await self.summarizer.summarize_individual_alert(
                        alerts[0], style=style, sentence_limit=config["sentence_limit"]
                    )

                # Use the parent summarizer's method
                response = await self.summarizer.summarize_alerts(request)
                return response.summary
            else:
                # Fallback if no summarizer reference
                return "AI summarizer not available"

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to generate AI summary due to connection issue: {e}")
            return "Connection error: Unable to reach AI service"
        except ValueError as e:
            logger.error(f"Failed to generate AI summary due to invalid data: {e}")
            return "Data error: Invalid alert format"
        except RuntimeError as e:
            logger.error(f"Failed to generate AI summary due to runtime error: {e}")
            return "Runtime error: AI service temporarily unavailable"
        except Exception as e:
            logger.error(f"Failed to generate AI summary due to unexpected error: {e}")
            return f"Unexpected error: {str(e)}"


class AISummarizer:
    """
    AI-powered summarizer for MBTA alerts using Ollama.

    This class provides functionality to summarize multiple MBTA alerts into
    a concise, human-readable summary using local AI models via Ollama.
    """

    def __init__(self, config: Optional[OllamaConfig] = None):
        """
        Initialize the AI summarizer.

        Args:
            config: Configuration for Ollama endpoint. If None, loads from environment.
        """
        self.config = config or self._load_config_from_env()
        self._client: Optional[AsyncClient] = None
        self.job_queue = SummaryJobQueue(summarizer=self)

    def _load_config_from_env(self) -> OllamaConfig:
        """Load configuration from environment variables first, then fallback to config.json."""

        # Check environment variables first (highest priority)
        env_base_url = os.getenv("OLLAMA_BASE_URL")
        env_model = os.getenv("OLLAMA_MODEL")
        env_timeout = os.getenv("OLLAMA_TIMEOUT")
        env_temperature = os.getenv("OLLAMA_TEMPERATURE")
        env_max_retries = os.getenv("OLLAMA_MAX_RETRIES")
        env_cache_ttl = os.getenv("OLLAMA_CACHE_TTL")
        env_min_similarity = os.getenv("OLLAMA_MIN_SIMILARITY")
        env_validate_only_short = os.getenv("OLLAMA_VALIDATE_ONLY_SHORT_RESPONSES")
        env_enable_rewriting = os.getenv("OLLAMA_ENABLE_SUMMARY_REWRITING")
        env_sentence_tolerance = os.getenv("OLLAMA_REWRITE_SENTENCE_TOLERANCE")

        # If all required environment variables are set, use them
        if env_base_url and env_model:
            logger.info("Using environment variables for Ollama config")
            return OllamaConfig(
                base_url=env_base_url,
                model=env_model,
                timeout=self._safe_int(env_timeout, 30),
                temperature=self._safe_float(env_temperature, 0.1),
                max_retries=self._safe_int(env_max_retries, 3),
                cache_ttl=self._safe_int(env_cache_ttl, 3600),
                min_similarity_threshold=self._safe_float(env_min_similarity, 0.105),
                validate_only_short_responses=self._safe_bool(
                    env_validate_only_short, True
                ),
                enable_summary_rewriting=self._safe_bool(env_enable_rewriting, True),
                rewrite_sentence_tolerance=self._safe_int(env_sentence_tolerance, 1),
            )

        # Fallback to config.json if environment variables are not complete
        try:
            config_path = "config.json"
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    config_data = json.loads(f.read())

                ollama_config = config_data.get("ollama", {})
                if ollama_config:
                    logger.info(
                        f"Loaded Ollama config from {config_path}: {ollama_config}"
                    )
                    return OllamaConfig(
                        base_url=ollama_config.get(
                            "base_url", "http://localhost:11434"
                        ),
                        model=ollama_config.get("model", "llama3.2:1b"),
                        timeout=ollama_config.get("timeout", 30),
                        temperature=ollama_config.get("temperature", 0.1),
                        max_retries=ollama_config.get("max_retries", 3),
                        cache_ttl=ollama_config.get("cache_ttl", 3600),
                        min_similarity_threshold=ollama_config.get(
                            "min_similarity_threshold", 0.105
                        ),
                        validate_only_short_responses=ollama_config.get(
                            "validate_only_short_responses", True
                        ),
                        enable_summary_rewriting=ollama_config.get(
                            "enable_summary_rewriting", True
                        ),
                        rewrite_sentence_tolerance=ollama_config.get(
                            "rewrite_sentence_tolerance", 1
                        ),
                    )
        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}")

        # Final fallback to hardcoded defaults
        logger.info("Using hardcoded defaults for Ollama config")
        return OllamaConfig(
            base_url="http://localhost:11434",
            model="llama3.2:1b",
            timeout=30,
            temperature=0.1,
            max_retries=3,
            cache_ttl=3600,
            min_similarity_threshold=0.105,
            validate_only_short_responses=True,
            enable_summary_rewriting=True,
            rewrite_sentence_tolerance=1,
        )

    def _safe_int(self, value: Optional[str], default: int) -> int:
        """Safely parse integer from environment variable."""
        if not value:
            return default
        try:
            # Remove any comments and whitespace
            clean_value = value.split("#")[0].strip()
            return int(clean_value)
        except (ValueError, AttributeError):
            return default

    def _safe_float(self, value: Optional[str], default: float) -> float:
        """Safely parse float from environment variable."""
        if not value:
            return default
        try:
            # Remove any comments and whitespace
            clean_value = value.split("#")[0].strip()
            return float(clean_value)
        except (ValueError, AttributeError):
            return default

    def _safe_bool(self, value: Optional[str], default: bool) -> bool:
        """Safely parse boolean from environment variable."""
        if not value:
            return default
        try:
            # Remove any comments and whitespace
            clean_value = value.split("#")[0].strip().lower()
            if clean_value in ("true", "1", "yes", "on"):
                return True
            elif clean_value in ("false", "0", "no", "off"):
                return False
            else:
                return default
        except (ValueError, AttributeError):
            return default

    def _get_client(self) -> AsyncClient:
        """Get or create Ollama async client."""
        if self._client is None:
            self._client = AsyncClient(
                host=self.config.base_url, timeout=self.config.timeout
            )
        return self._client

    async def start(self) -> None:
        """Start the AI summarizer and job queue."""
        await self.job_queue.start()

    async def stop(self) -> None:
        """Stop the AI summarizer and job queue."""
        await self.job_queue.stop()

    async def close(self) -> None:
        """Close the Ollama client."""
        if self._client:
            # Ollama client doesn't have aclose method, just set to None
            self._client = None

    def _format_alerts_for_prompt(
        self,
        alerts: List[AlertResource],
        include_route_info: bool = True,
        include_severity: bool = True,
    ) -> str:
        """
        Format alerts into a prompt for the AI model.

        Args:
            alerts: List of alerts to format
            include_route_info: Whether to include route information
            include_severity: Whether to include severity information

        Returns:
            Formatted prompt string
        """
        if not alerts:
            return "No alerts to summarize."

        prompt_parts = [
            "Please provide a concise summary of the following MBTA transit alerts:"
        ]

        for i, alert in enumerate(alerts, 1):
            attrs = alert.attributes
            parts = [f"{i}. {attrs.header}"]

            if attrs.short_header and attrs.short_header != attrs.header:
                parts.append(f"   Brief: {attrs.short_header}")

            if attrs.effect:
                parts.append(f"   Effect: {attrs.effect}")

            if include_severity:
                severity_map = {
                    1: "Minor",
                    2: "Minor",
                    3: "Minor",
                    4: "Moderate",
                    5: "Moderate",
                    6: "Moderate",
                    7: "Major",
                    8: "Major",
                    9: "Major",
                    10: "Severe",
                }
                severity_text = severity_map.get(
                    attrs.severity, f"Level {attrs.severity}"
                )
                parts.append(f"   Severity: {severity_text}")

            if attrs.timeframe:
                parts.append(f"   Timeframe: {attrs.timeframe}")

            if attrs.cause and attrs.cause != "UNKNOWN_CAUSE":
                parts.append(f"   Cause: {attrs.cause}")

            prompt_parts.append("\n".join(parts))

        prompt_parts.append(
            "\nPlease provide a comprehensive, structured summary that covers each alert individually with detailed analysis."
            "\nDo not directly respond to this prompt with 'Okay', 'Got it', 'I understand', or anything else."
            "\nConvert IDs like 'UNKNOWN_CAUSE' to 'Unknown Cause'."
            "\nUse proper grammar and punctuation."
            "\n\nFor each alert, provide a detailed summary including:"
            "\n- What the issue is and its scope"
            "\n- Impact on transit service and commuters"
            "\n- Affected routes, stations, or areas"
            "\n- Severity level and expected duration"
            "\n- Any specific instructions or alternatives for passengers"
            "\n\nAfter covering all individual alerts, provide:"
            "\n- Overall assessment of the transit system status"
            "\n- Common themes or patterns across alerts"
            "\n- Recommendations for commuters"
            "\n- Priority areas requiring attention"
            "\n\nIMPORTANT: Generate a complete, comprehensive summary covering all alerts thoroughly."
            "\nDo not truncate or stop early. Provide detailed analysis for each alert."
            "\n\nExpected format:"
            "\n1. Executive summary of the overall situation"
            "\n2. Detailed individual alert analysis (comprehensive coverage per alert)"
            "\n3. Systemic analysis and patterns"
            "\n4. Commuter impact assessment"
            "\n5. Recommendations and key points for travelers"
            "\n\nMake sure to cover ALL alerts completely with full details. Do not stop until you have provided a comprehensive summary."
        )

        return "\n\n".join(prompt_parts)

    def _format_alerts_for_prompt_adaptive(
        self, alerts: List[AlertResource], style: str = "comprehensive"
    ) -> str:
        """
        Format alerts for AI prompt with adaptive style selection.

        Args:
            alerts: List of alerts to format
            style: Prompt style - "comprehensive" (detailed) or "concise" (brief)

        Returns:
            Formatted prompt text
        """
        if not alerts:
            return "No alerts to summarize."

        # Start with alert data
        prompt_parts = [
            f"Please analyze {len(alerts)} MBTA transit alerts and provide a summary."
        ]

        # Add alert details
        for i, alert in enumerate(alerts, 1):
            if not self._validate_alert_structure(alert):
                continue

            attrs = alert.attributes
            parts = [f"\nAlert {i}:"]

            if attrs.header:
                parts.append(f"   Header: {attrs.header}")

            if hasattr(attrs, "short_header") and attrs.short_header:
                parts.append(f"   Short Header: {attrs.short_header}")

            if hasattr(attrs, "effect") and attrs.effect:
                parts.append(f"   Effect: {attrs.effect}")

            if hasattr(attrs, "severity") and attrs.severity is not None:
                severity_map = {
                    1: "Minor",
                    2: "Minor",
                    3: "Minor",
                    4: "Moderate",
                    5: "Moderate",
                    6: "Moderate",
                    7: "Major",
                    8: "Major",
                    9: "Major",
                    10: "Severe",
                }
                severity_text = severity_map.get(
                    attrs.severity, f"Level {attrs.severity}"
                )
                parts.append(f"   Severity: {severity_text}")

            if attrs.timeframe:
                parts.append(f"   Timeframe: {attrs.timeframe}")

            if attrs.cause and attrs.cause != "UNKNOWN_CAUSE":
                parts.append(f"   Cause: {attrs.cause}")

            prompt_parts.append("\n".join(parts))

        # Add style-specific instructions
        if style == "comprehensive":
            prompt_parts.append(
                "\nPlease provide a comprehensive, structured summary that covers each alert individually with detailed analysis."
                "\nDo not directly respond to this prompt with 'Okay', 'Got it', 'I understand', or anything else."
                "\nConvert IDs like 'UNKNOWN_CAUSE' to 'Unknown Cause'."
                "\nUse proper grammar and punctuation."
                "\n\nFor each alert, provide a detailed summary including:"
                "\n- What the issue is and its scope"
                "\n- Impact on transit service and commuters"
                "\n- Affected routes, stations, or areas"
                "\n- Severity level and expected duration"
                "\n- Any specific instructions or alternatives for passengers"
                "\n\nAfter covering all individual alerts, provide:"
                "\n- Overall assessment of the transit system status"
                "\n- Common themes or patterns across alerts"
                "\n- Recommendations for commuters"
                "\n- Priority areas requiring attention"
                "\n\nIMPORTANT: Generate a complete, comprehensive summary covering all alerts thoroughly."
                "\nDo not truncate or stop early. Provide detailed analysis for each alert."
                "\n\nExpected format:"
                "\n1. Executive summary of the overall situation"
                "\n2. Detailed individual alert analysis (comprehensive coverage per alert)"
                "\n3. Systemic analysis and patterns"
                "\n4. Commuter impact assessment"
                "\n5. Recommendations and key points for travelers"
                "\n\nMake sure to cover ALL alerts completely with full details. Do not stop until you have provided a comprehensive summary."
            )
        else:  # concise style
            prompt_parts.append(
                "\nPlease provide a concise, focused summary that covers each alert in 1-2 sentences maximum."
                "\nDo not directly respond to this prompt with 'Okay', 'Got it', 'I understand', or anything else."
                "\nConvert IDs like 'UNKNOWN_CAUSE' to 'Unknown Cause'."
                "\nUse proper grammar and punctuation."
                "\nDo not include [Train Number] in your response if the information is not available."
                "\n\nFor each alert, provide a brief summary that:"
                "\n- Clearly explains what the issue is"
                "\n- Describes the impact on transit service"
                "\n- Mentions affected routes/areas if relevant"
                "\n- Uses professional, informative language"
                "\n- Avoids conversational phrases or unnecessary details"
                "\n\nIMPORTANT: Keep summaries brief and focused. Each alert should be summarized in 1-2 sentences maximum."
                "\n\nExpected format:"
                "\n1. Brief overview (1 sentence)"
                "\n2. Individual alert summaries (1-2 sentences each)"
                "\n\nKeep it concise and to the point. Be specific about locations, routes, vehicle numbers, and times."
            )

        return "\n\n".join(prompt_parts)

    def _format_summary(self, summary: str, format_type: str) -> str:
        """Format a summary according to the requested format type."""
        if format_type == "text":
            return summary
        elif format_type == "markdown":
            # Convert plain text to markdown format
            lines = summary.split("\n")
            formatted_lines = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Check if line looks like a header (starts with common alert patterns)
                if any(
                    line.lower().startswith(prefix)
                    for prefix in [
                        "alert",
                        "route",
                        "effect",
                        "cause",
                        "severity",
                        "timeframe",
                    ]
                ):
                    formatted_lines.append(f"## {line}")
                elif line.endswith(":") and len(line) < 50:
                    formatted_lines.append(f"**{line}**")
                else:
                    formatted_lines.append(line)

            return "\n\n".join(formatted_lines)
        elif format_type == "json":
            # Convert to structured JSON format
            try:
                # Try to parse as JSON first
                import json

                json.loads(summary)
                return summary  # Already JSON
            except (ValueError, TypeError):
                # Convert plain text to structured JSON
                lines = summary.split("\n")
                structured_data = {
                    "summary": summary,
                    "formatted_summary": {
                        "overview": lines[0] if lines else "",
                        "details": [line.strip() for line in lines[1:] if line.strip()],
                        "alert_count": len(
                            [line for line in lines if "alert" in line.lower()]
                        ),
                        "routes_affected": [
                            line for line in lines if "route" in line.lower()
                        ],
                        "effects": [
                            line
                            for line in lines
                            if "effect" in line.lower()
                            or "delay" in line.lower()
                            or "detour" in line.lower()
                        ],
                    },
                }
                return json.dumps(structured_data, indent=2)
        else:
            return summary

    def _clean_model_response(
        self,
        summary: str,
        original_input: Optional[str] = None,
        min_similarity: Optional[float] = None,
        use_config_threshold: bool = True,
    ) -> str:
        """
        Clean up direct responses from the model and remove conversational phrases using regex.
        Optionally validate similarity to original input.

        Args:
            summary: The AI-generated summary text to clean
            original_input: Original input text for similarity validation (optional)
            min_similarity: Minimum similarity threshold for validation (0.0 to 1.0)

        Returns:
            Cleaned summary text, or error message if similarity validation fails
        """
        if not summary or len(summary.strip()) == 0:
            return summary

        original_summary = summary
        logger.debug(f"Cleaning model response (original length: {len(summary)})")

        # Regex patterns for conversational starters and phrases
        # These patterns are case-insensitive and handle variations
        conversational_patterns = [
            # Remove anything that starts with "Okay" - it's conversational nonsense
            r"^okay[^a-zA-Z]*",
            r"^okay\s+[^a-zA-Z]*",
            r"^here[^a-zA-Z]*",
            r"^here\s+[^a-zA-Z]*",
            # Also catch other common conversational starters
            r"^alright[^a-zA-Z]*",
            r"^well[^a-zA-Z]*",
            r"^so[^a-zA-Z]*",
            r"^here\'?s?\s+(?:a\s+)?(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown|situation|status|update|information|details?)(?:\s+of\s+the\s+(?:provided\s+)?(?:mbta\s+)?(?:transit\s+)?alerts?)?(?:\s*,?\s*structured)?(?:\s*:)?",
            r"^here\s+is\s+(?:a\s+)?(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown|situation|status|update|information|details?)(?:\s+of\s+the\s+(?:provided\s+)?(?:mbta\s+)?(?:transit\s+)?alerts?)?(?:\s*,?\s*structured)?(?:\s*:)?",
            r"^i\'?ll?\s+(?:provide|give\s+you|break\s+(?:this\s+)?down|explain|clarify|summarize)(?:\s+a\s+(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown))?",
            r"^i\s+will\s+(?:provide|give\s+you|break\s+(?:this\s+)?down|explain|clarify|summarize)(?:\s+a\s+(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown))?",
            r"^let\s+me\s+(?:provide|give\s+you|break\s+(?:this\s+)?down|explain|clarify|summarize)(?:\s+a\s+(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown))?",
            r"^i\s+can\s+(?:provide|give\s+you|break\s+(?:this\s+)?down|explain|clarify|summarize)(?:\s+a\s+(?:comprehensive\s+)?(?:detailed\s+)?(?:structured\s+)?(?:clear\s+)?(?:concise\s+)?(?:brief\s+)?(?:summary|overview|breakdown))?",
        ]

        # Apply each regex pattern to remove conversational starters
        cleaned_summary = summary
        for pattern in conversational_patterns:
            # Use re.IGNORECASE for case-insensitive matching
            match = re.search(pattern, cleaned_summary, re.IGNORECASE)
            if match:
                logger.debug(f"Found conversational pattern match: '{match.group(0)}'")
                logger.debug(f"Pattern that matched: {pattern}")
                # Remove the matched pattern and everything up to the first sentence end
                start_pos = match.end()

                # Find the first sentence end (period, colon, newline, or dash)
                end_markers = [".", ":", "\n", " -", " - "]
                end_pos = len(cleaned_summary)

                for marker in end_markers:
                    marker_pos = cleaned_summary.find(marker, start_pos)
                    if marker_pos != -1:
                        end_pos = min(end_pos, marker_pos + 1)

                # Extract the content after the conversational phrase
                cleaned_summary = cleaned_summary[end_pos:].strip()

                # Remove any leading punctuation or whitespace
                cleaned_summary = re.sub(r"^[.,:;\s]+", "", cleaned_summary)

                logger.info(f"Removed conversational starter: '{match.group(0)}'")
                break
        else:
            logger.debug("No conversational pattern matched")
            logger.debug(f"Text to match: '{summary[:100]}...'")
            logger.debug(
                f"Available patterns: {[p[:50] + '...' for p in conversational_patterns]}"
            )

        # Now remove conversational phrases that might appear anywhere in the response
        conversational_phrases_patterns = [
            # "I understand..." patterns
            r"\b(?:i\s+understand(?:\s+that)?|i\s+can\s+see(?:\s+that)?)\b",
            # "As you..." patterns
            r"\b(?:as\s+you\s+(?:can\s+)?see|as\s+(?:you\s+)?requested|as\s+asked)\b",
            # "Per..." patterns
            r"\b(?:per\s+(?:your\s+)?request|per\s+the\s+request)\b",
            # "Based on..." patterns
            r"\b(?:based\s+on\s+(?:the\s+)?(?:information|alerts|what\s+i\s+can\s+see))\b",
            # "Looking at..." patterns
            r"\b(?:looking\s+at\s+(?:the\s+)?(?:alerts|information))\b",
            # "Examining..." patterns
            r"\b(?:examining\s+(?:the\s+)?(?:alerts|information))\b",
            # "Analyzing..." patterns
            r"\b(?:analyzing\s+(?:the\s+)?(?:alerts|information))\b",
            # "Reviewing..." patterns
            r"\b(?:reviewing\s+(?:the\s+)?(?:alerts|information))\b",
            # "Going through..." patterns
            r"\b(?:going\s+through\s+(?:the\s+)?(?:alerts|information))\b",
            # "Let me break..." patterns
            r"\b(?:let\s+me\s+(?:break\s+(?:this\s+)?down|explain|clarify|provide|give\s+you))\b",
            # "I'll..." patterns
            r"\b(?:i\'?ll?\s+(?:break\s+(?:this\s+)?down|explain|clarify|provide|give\s+you))\b",
            # "I will..." patterns
            r"\b(?:i\s+will\s+(?:break\s+(?:this\s+)?down|explain|clarify|provide|give\s+you))\b",
        ]

        # Remove conversational phrases from anywhere in the text
        for pattern in conversational_phrases_patterns:
            # Find all matches and remove them
            matches = list(re.finditer(pattern, cleaned_summary, re.IGNORECASE))
            # Process matches in reverse order to maintain positions
            for match in reversed(matches):
                logger.debug(f"Removing conversational phrase: '{match.group(0)}'")
                # Remove the phrase and clean up spacing
                before = cleaned_summary[: match.start()].strip()
                after = cleaned_summary[match.end() :].strip()

                if before and after:
                    cleaned_summary = before + " " + after
                elif before:
                    cleaned_summary = before
                elif after:
                    cleaned_summary = after
                else:
                    cleaned_summary = ""

        # Final cleanup: remove any remaining artifacts
        # Remove multiple consecutive newlines
        cleaned_summary = re.sub(r"\n\s*\n\s*\n+", "\n\n", cleaned_summary)

        # Remove multiple consecutive spaces
        cleaned_summary = re.sub(r"\s+", " ", cleaned_summary)

        # Remove any leading/trailing whitespace
        cleaned_summary = cleaned_summary.strip()

        # Log what was cleaned up
        if len(cleaned_summary) != len(original_summary):
            removed_chars = len(original_summary) - len(cleaned_summary)
            logger.info(
                f"Cleaned model response: removed {removed_chars} characters of conversational phrases and artifacts"
            )
            logger.debug(f"Original: {original_summary[:100]}...")
            logger.debug(f"Cleaned:  {cleaned_summary[:100]}...")

        logger.debug(
            f"Cleaning model response (cleaned length: {len(cleaned_summary)})"
        )

        # Count sentences to determine if similarity validation should be applied
        sentence_count = self._count_sentences(cleaned_summary)
        logger.debug(f"Response contains {sentence_count} sentence(s)")

        # Apply similarity validation if original input is provided, validation is enabled, and response length criteria are met
        should_validate = (
            original_input
            and cleaned_summary
            and self.config.min_similarity_threshold > 0
            and (not self.config.validate_only_short_responses or sentence_count <= 1)
        )

        if should_validate:
            if sentence_count <= 1:
                logger.debug(
                    "Applying similarity validation for short response (1 sentence)"
                )
            else:
                logger.debug(
                    f"Applying similarity validation for longer response ({sentence_count} sentences) - short response validation disabled"
                )
        elif (
            original_input
            and cleaned_summary
            and self.config.min_similarity_threshold > 0
            and self.config.validate_only_short_responses
            and sentence_count > 1
        ):
            logger.debug(
                f"Skipping similarity validation for longer response ({sentence_count} sentences)"
            )
        elif not original_input:
            logger.debug("Skipping similarity validation - no original input provided")
        elif not self.config.min_similarity_threshold > 0:
            logger.debug("Skipping similarity validation - validation disabled")

        if should_validate:
            try:
                # Import textdistance for similarity calculation
                import textdistance

                if min_similarity is not None:
                    threshold = min_similarity
                elif use_config_threshold:
                    threshold = self.config.min_similarity_threshold
                else:
                    threshold = 0.105

                # Calculate Levenshtein similarity, handling None values safely
                original = original_input.lower() if original_input is not None else ""
                summary = cleaned_summary.lower() if cleaned_summary is not None else ""
                similarity = textdistance.levenshtein.normalized_similarity(
                    original, summary
                )

                logger.debug(
                    f"Similarity validation - score: {similarity:.3f}, threshold: {threshold}"
                )

                if similarity < threshold:
                    logger.warning(
                        f"Cleaned summary failed similarity validation: {similarity:.3f} below threshold {threshold}"
                    )
                    # Return an error message indicating the summary was too different
                    return f"Generated summary failed similarity validation (score: {similarity:.3f}, threshold: {threshold}). The AI response may be off-topic or irrelevant."
                else:
                    logger.debug(
                        f"Cleaned summary passed similarity validation: {similarity:.3f}"
                    )

            except ImportError:
                logger.warning(
                    "textdistance not available, skipping similarity validation"
                )
            except Exception as e:
                logger.error(f"Error during similarity validation: {e}", exc_info=True)
                # Continue with the cleaned summary even if validation fails

        return cleaned_summary

    async def _analyze_and_rewrite_summary(
        self,
        summary: str,
        original_alerts: Optional[List[AlertResource]] = None,
        style: str = "comprehensive",
    ) -> str:
        """
        Analyze the generated summary and rewrite it for clarity and readability.

        Args:
            summary: The initial AI-generated summary to analyze
            original_alerts: Original alerts for context (optional)
            style: Summary style for rewriting guidance

        Returns:
            Rewritten summary with improved clarity and readability
        """
        if not summary or len(summary.strip()) < 20:
            logger.debug("Summary too short for rewriting, returning original")
            return summary

        try:
            # Create a prompt for analyzing and rewriting the summary
            rewrite_prompt = self._create_rewrite_prompt(
                summary, original_alerts, style
            )

            # Call Ollama for the rewrite
            client = self._get_client()
            response = await client.chat(
                model=self.config.model,
                messages=[{"role": "user", "content": rewrite_prompt}],
                options={
                    "temperature": 0.05,  # Lower temperature for more focused rewriting
                    "top_p": 0.9,
                    "top_k": 40,
                    "num_predict": 1500,
                    "stop": ["###", "---"],
                    "repeat_penalty": 1.1,
                    "num_ctx": 4096,
                },
            )

            # Extract the rewritten content
            if hasattr(response, "message") and hasattr(response.message, "content"):
                rewritten_content = response.message.content
            elif hasattr(response, "content"):
                rewritten_content = response.content
            else:
                rewritten_content = str(response)

            if rewritten_content and rewritten_content.strip():
                cleaned_rewrite = self._clean_model_response(
                    rewritten_content.strip(),
                    summary,  # Use original summary as input for validation
                    use_config_threshold=False,  # Skip similarity validation for rewrites
                )

                # Check sentence length similarity and content difference
                original_sentences = self._count_sentences(summary)
                rewritten_sentences = self._count_sentences(cleaned_rewrite)

                # Only use rewrite if sentence count is within tolerance and content is different
                sentence_diff = abs(original_sentences - rewritten_sentences)
                tolerance = self.config.rewrite_sentence_tolerance
                rewritten_lower = cleaned_rewrite.lower()
                if (
                    "revise" in rewritten_lower
                    or "rewrite" in rewritten_lower
                    or "rewritten" in rewritten_lower
                    or "[train number]" in rewritten_lower
                ):
                    logger.warning(
                        "Rewrite rejected for containing illegal phrases, returning original summary"
                    )
                    return summary
                if (
                    sentence_diff <= tolerance  # Allow configurable sentence difference
                    and cleaned_rewrite != summary
                    and len(cleaned_rewrite.strip()) > 10  # Ensure minimum content
                ):
                    logger.info(
                        f"Summary successfully rewritten for clarity (sentences: {original_sentences} → {rewritten_sentences})"
                    )
                    return cleaned_rewrite
                else:
                    if sentence_diff > tolerance:
                        logger.debug(
                            f"Rewrite rejected - sentence count too different ({original_sentences} vs {rewritten_sentences}, tolerance: {tolerance})"
                        )
                    elif cleaned_rewrite == summary:
                        logger.debug("Rewrite rejected - content identical to original")
                    else:
                        logger.debug("Rewrite rejected - insufficient content length")
                    return summary
            else:
                logger.warning("Failed to get rewritten content, keeping original")
                return summary

        except Exception as e:
            logger.error(f"Error during summary rewriting: {e}", exc_info=True)
            # Return original summary if rewriting fails
            return summary

    def _create_rewrite_prompt(
        self,
        summary: str,
        original_alerts: Optional[List[AlertResource]] = None,
        style: str = "comprehensive",
    ) -> str:
        """
        Create a prompt for analyzing and rewriting the summary.

        Args:
            summary: The summary to rewrite
            original_alerts: Original alerts for context
            style: Summary style for guidance

        Returns:
            Prompt for the rewrite process
        """
        prompt_parts = [
            "Please analyze and rewrite the following MBTA transit alert summary to improve clarity, readability, and professional tone.",
            "",
            "ORIGINAL SUMMARY:",
            summary,
            "",
        ]

        if original_alerts:
            prompt_parts.extend(
                [
                    "ORIGINAL ALERT CONTEXT:",
                    self._extract_alert_text_for_validation(original_alerts),
                    "",
                ]
            )

        prompt_parts.extend(
            [
                "REWRITING REQUIREMENTS:",
                "1. Maintain all factual information from the original summary",
                "2. Improve sentence structure and flow",
                "3. Use clear, professional language appropriate for transit communications",
                "4. Ensure logical organization of information",
                "5. Remove any conversational or informal language",
                "6. Make the summary more accessible to commuters",
                "7. Maintain the same level of detail and scope",
                "8. Use consistent terminology throughout",
                "9. VERY IMPORTANT: Do not say that this is a rewrite, a revised summary, or any other similar phrase in your response!!!!",
                "10. Do not include [Train Number] in your response if the information is not available.",
                "",
                "SENTENCE LENGTH REQUIREMENT:",
                f"- Maintain exactly {self._count_sentences(summary)} sentences",
                "- Do not add or remove sentences",
                "- Focus on improving clarity within the same sentence structure",
                "",
                "STYLE GUIDANCE:",
                f"- Target style: {style}",
                "- Focus on readability and professional communication",
                "",
                "Please provide the rewritten summary that addresses these requirements.",
            ]
        )

        return "\n".join(prompt_parts)

    async def _call_ollama(
        self, prompt: str, alerts: Optional[List[AlertResource]] = None
    ) -> str:
        """
        Call the Ollama API to generate a summary using the official client.

        Args:
            prompt: The prompt to send to the model
            alerts: Optional list of alerts for similarity validation

        Returns:
            Generated summary text

        Raises:
            Exception: If the API call fails
        """
        client = self._get_client()

        # Log the prompt and configuration for debugging
        logger.debug(f"Calling Ollama with model: {self.config.model}")
        logger.debug(f"Prompt length: {len(prompt)} characters")
        logger.debug(
            f"Model config: temperature={self.config.temperature}, timeout={self.config.timeout}"
        )
        logger.debug(f"Full prompt:\n{prompt}")

        try:
            response = await client.chat(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                options={
                    "temperature": self.config.temperature,
                    "top_p": 0.9,
                    "top_k": 40,
                    "num_predict": 2000,  # Increased from 500 to allow much longer summaries
                    "stop": [
                        "###",
                        "---",
                    ],  # Removed "\n\n" which was causing early truncation
                    "repeat_penalty": 1.1,  # Prevent repetitive text
                    "num_ctx": 4096,  # Context window size
                },
            )

            # Handle the response based on Ollama client structure
            if hasattr(response, "message") and hasattr(response.message, "content"):
                content = response.message.content
                if content is None:
                    return "No content generated by model"
                cleaned_content = content.strip()
                # Clean up conversational starters and validate similarity
                alert_text = (
                    self._extract_alert_text_for_validation(alerts)
                    if alerts
                    else prompt
                )
                cleaned_content = self._clean_model_response(
                    cleaned_content, alert_text
                )

                # Apply summary rewriting if enabled
                if self.config.enable_summary_rewriting:
                    logger.debug("Applying summary rewriting for clarity")
                    rewritten_content = await self._analyze_and_rewrite_summary(
                        cleaned_content, alerts, "comprehensive"
                    )
                    if rewritten_content != cleaned_content:
                        logger.info("Summary rewritten for improved clarity")
                        cleaned_content = rewritten_content

                # Check for truncation
                # logger.debug(
                #     f"Generated summary content (length: {len(final_content)}):\n{final_content}"
                # )
                # logger.debug(f"Raw Ollama response: {response}")
                return cleaned_content
            elif hasattr(response, "content"):
                content = response.content
                if content is None:
                    return "No content generated by model"
                cleaned_content = content.strip()
                # Clean up conversational starters and validate similarity
                alert_text = (
                    self._extract_alert_text_for_validation(alerts)
                    if alerts
                    else prompt
                )
                cleaned_content = self._clean_model_response(
                    cleaned_content, alert_text
                )

                # Apply summary rewriting if enabled
                if self.config.enable_summary_rewriting:
                    logger.debug("Applying summary rewriting for clarity")
                    rewritten_content = await self._analyze_and_rewrite_summary(
                        cleaned_content, alerts, "comprehensive"
                    )
                    if rewritten_content != cleaned_content:
                        logger.info("Summary rewritten for improved clarity")
                        cleaned_content = rewritten_content

                # Check for truncation

                return cleaned_content
            else:
                # Fallback: try to get content from response directly
                fallback_content = str(response).strip()
                # Clean up conversational starters and validate similarity
                alert_text = (
                    self._extract_alert_text_for_validation(alerts)
                    if alerts
                    else prompt
                )
                fallback_content = self._clean_model_response(
                    fallback_content, alert_text
                )

                # Apply summary rewriting if enabled
                if self.config.enable_summary_rewriting:
                    logger.debug("Applying summary rewriting for clarity")
                    rewritten_content = await self._analyze_and_rewrite_summary(
                        fallback_content, alerts, "comprehensive"
                    )
                    if rewritten_content != fallback_content:
                        logger.info("Summary rewritten for improved clarity")
                        fallback_content = rewritten_content

                # Check for truncation
                # final_content = self._detect_truncation(fallback_content, prompt)
                # logger.debug(
                #     f"Generated summary content (fallback, length: {len(final_content)}):\n{final_content}"
                # )
                # logger.debug(f"Raw Ollama response: {response}")
                return fallback_content

        except (ConnectionError, TimeoutError) as e:
            raise ConnectionError(f"Failed to connect to Ollama API: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid response from Ollama API: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error calling Ollama API: {e}")

    async def queue_summary_job(
        self,
        alerts: List[AlertResource],
        priority: JobPriority = JobPriority.NORMAL,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Queue a summary job for background processing.

        Args:
            alerts: Alerts to summarize
            priority: Job priority level
            config: Additional configuration for summarization

        Returns:
            Job ID for tracking
        """
        return await self.job_queue.add_job(alerts, priority, config)

    async def get_job_status(self, job_id: str) -> Optional[SummaryJob]:
        """Get the status of a queued summary job."""
        return await self.job_queue.get_job_status(job_id)

    async def get_cached_summary(self, alerts: List[AlertResource]) -> Optional[str]:
        """Get a cached summary from Redis if available."""
        return await self.job_queue._get_summary_from_redis(alerts)

    async def force_generate_summary(
        self, alerts: List[AlertResource], config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Force generation of a summary, bypassing cache.
        Useful for testing or when fresh summaries are needed.
        """
        request = SummarizationRequest(
            alerts=alerts,
            include_route_info=config.get("include_route_info", True)
            if config
            else True,
            include_severity=config.get("include_severity", True) if config else True,
        )

        response = await self.summarize_alerts(request)
        return response.summary

    async def summarize_alerts(
        self, request: SummarizationRequest
    ) -> SummarizationResponse:
        """
        Summarize a list of MBTA alerts using AI.

        Args:
            request: Summarization request containing alerts and options

        Returns:
            Summarization response with generated summary
        """
        start_time = asyncio.get_event_loop().time()

        try:
            if not request.alerts:
                return SummarizationResponse(
                    summary="No alerts to summarize.",
                    alert_count=0,
                    model_used=self.config.model,
                    processing_time_ms=0.0,
                )

            # Format alerts for the AI prompt
            if request.style == "concise":
                prompt = self._format_alerts_for_prompt_adaptive(
                    request.alerts, "concise"
                )
            else:
                prompt = self._format_alerts_for_prompt_adaptive(
                    request.alerts, "comprehensive"
                )

            # Generate group summary using Ollama
            summary = await self._call_ollama(prompt, request.alerts)

            # Apply formatting if requested
            if request.format != "text":
                summary = self._format_summary(summary, request.format)

            processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

            return SummarizationResponse(
                summary=summary,
                alert_count=len(request.alerts),
                model_used=self.config.model,
                processing_time_ms=round(processing_time, 2),
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to summarize alerts due to connection issue: {e}")
            fallback_summary = (
                "Unable to connect to AI service. Please try again later. "
            )
            if request.alerts:
                fallback_summary += f"There are {len(request.alerts)} active alerts."
        except ValueError as e:
            logger.error(f"Failed to summarize alerts due to invalid data: {e}")
            fallback_summary = (
                "Unable to process alert data. Please check alert format. "
            )
            if request.alerts:
                fallback_summary += f"There are {len(request.alerts)} active alerts."
        except RuntimeError as e:
            logger.error(f"Failed to summarize alerts due to runtime error: {e}")
            fallback_summary = (
                "AI service temporarily unavailable. Please try again later. "
            )
            if request.alerts:
                fallback_summary += f"There are {len(request.alerts)} active alerts."
        except Exception as e:
            logger.error(f"Failed to summarize alerts due to unexpected error: {e}")
            fallback_summary = "An unexpected error occurred while generating summary. "
            if request.alerts:
                fallback_summary += f"There are {len(request.alerts)} active alerts."

        processing_time = (asyncio.get_event_loop().time() - start_time) * 1000

        return SummarizationResponse(
            summary=fallback_summary,
            alert_count=len(request.alerts),
            model_used=self.config.model,
            processing_time_ms=round(processing_time, 2),
        )

    def _extract_alert_text_for_validation(self, alerts: List[AlertResource]) -> str:
        """
        Extract key text from alerts for similarity validation.

        Args:
            alerts: List of alerts to extract text from

        Returns:
            Combined text string for similarity comparison
        """
        if not alerts:
            return ""

        alert_texts = []
        for alert in alerts:
            if not self._validate_alert_structure(alert):
                continue

            attrs = alert.attributes
            alert_text = f"{attrs.header} "

            if attrs.effect:
                alert_text += f"{attrs.effect} "
            if attrs.cause and attrs.cause != "UNKNOWN_CAUSE":
                alert_text += f"{attrs.cause} "
            if attrs.severity:
                alert_text += f"{attrs.severity} "

            alert_texts.append(alert_text.strip())

        return " ".join(alert_texts)

    def _count_sentences(self, text: str) -> int:
        """
        Count the number of sentences in the given text.

        Args:
            text: The text to analyze

        Returns:
            Number of sentences (minimum 1)
        """
        if not text or not text.strip():
            return 0

        # Split by common sentence terminators
        # This handles periods, exclamation marks, question marks, and colons
        sentence_terminators = r"[.!?:]\s+"
        sentences = re.split(sentence_terminators, text.strip())

        # Filter out empty sentences and count
        sentence_count = len([s for s in sentences if s.strip()])

        # Ensure at least 1 sentence is returned
        return max(1, sentence_count)

    def _validate_alert_structure(self, alert: AlertResource) -> bool:
        """
        Validate that an alert has the required structure for summarization.

        Args:
            alert: The alert to validate

        Returns:
            True if the alert is valid, False otherwise
        """
        try:
            # Check if alert has required attributes
            if not hasattr(alert, "id") or not alert.id:
                return False

            if not hasattr(alert, "attributes") or not alert.attributes:
                return False

            if not hasattr(alert.attributes, "header") or not alert.attributes.header:
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating alert structure: {e}")
            return False

    async def summarize_individual_alert(
        self,
        alert: AlertResource,
        priority: JobPriority = JobPriority.NORMAL,
        style: str = "concise",
        sentence_limit: int = 2,
    ) -> str:
        """
        Generate an AI summary for a single alert.

        Args:
            alert: The alert to summarize
            priority: Job priority for processing
            style: Summary style - 'concise' (default, 1-2 sentences) or 'comprehensive' (detailed)
            sentence_limit: Maximum number of sentences for the summary (1 for ultra-concise, 2+ for standard)

        Returns:
            The generated summary text
        """
        try:
            # Validate the alert structure
            if not self._validate_alert_structure(alert):
                logger.warning(
                    f"Skipping invalid alert {alert.id}: missing required attributes"
                )
                return f"Unable to process alert {alert.id}. Please check alert format."

            # Format the single alert for the prompt
            if style == "concise":
                alert_text = self._format_single_alert_for_prompt(alert, sentence_limit)
            else:
                # Use the adaptive method for comprehensive individual summaries
                alert_text = self._format_alerts_for_prompt_adaptive(
                    [alert], "comprehensive"
                )

            # Generate the summary using Ollama
            summary = await self._call_ollama(alert_text, [alert])

            if not summary:
                return f"Unable to generate summary for alert {alert.id}."

            # Clean the model response and validate similarity
            cleaned_summary = self._clean_model_response(summary, alert_text)

            # Apply summary rewriting if enabled
            if self.config.enable_summary_rewriting:
                logger.debug("Applying summary rewriting for individual alert clarity")
                rewritten_summary = await self._analyze_and_rewrite_summary(
                    cleaned_summary, [alert], style
                )
                if rewritten_summary != cleaned_summary:
                    logger.info(
                        "Individual alert summary rewritten for improved clarity"
                    )
                    cleaned_summary = rewritten_summary

            logger.info(f"Generated {style} individual summary for alert {alert.id}")
            return cleaned_summary

        except Exception as e:
            logger.error(
                f"Error generating individual summary for alert {alert.id}: {e}"
            )
            return f"Error generating summary for alert {alert.id}: {str(e)}"

    def _format_single_alert_for_prompt(
        self, alert: AlertResource, sentence_limit: int = 2
    ) -> str:
        """
        Format a single alert for the AI prompt.

        Args:
            alert: The alert to format
            sentence_limit: Maximum number of sentences for the summary

        Returns:
            Formatted alert text for the prompt
        """
        try:
            # Extract alert information
            alert_id = getattr(alert, "id", "Unknown ID")
            header = getattr(alert.attributes, "header", "No header available")
            short_header = getattr(
                alert.attributes, "short_header", "No short header available"
            )
            effect = getattr(
                alert.attributes, "effect", "No effect information available"
            )
            severity = getattr(alert.attributes, "severity", "Unknown severity")
            cause = getattr(alert.attributes, "cause", None)

            # Get route information if available
            route_info = ""
            if hasattr(alert, "relationships") and alert.relationships:
                routes = alert.relationships.get("route", {}).get("data", [])
                if routes:
                    route_names = [route.get("id", "Unknown route") for route in routes]
                    route_info = f"Affected routes: {', '.join(route_names)}"

                    # Format the prompt for a single alert
            sentence_text = (
                "1 sentence"
                if sentence_limit == 1
                else f"{sentence_limit} sentences maximum"
            )
            prompt = f"""Please provide a concise, focused summary of this MBTA transit alert in {sentence_text}. Include the locations affected, cause of the delay, and vehicle numbers or IDs if available.

Alert ID: {alert_id}
Header: {header}
Short Header: {short_header}
Effect: {effect}
Severity: {severity}"""

            # Only add cause if it's not UNKNOWN_CAUSE
            if cause and cause != "UNKNOWN_CAUSE":
                prompt += f"\nCause: {cause}"

            prompt += f"""
{route_info}

IMPORTANT: Provide a single, concise summary in {sentence_text} that:
1. Clearly explains what the issue is
2. Describes the impact on transit service
3. Mentions affected routes/areas if relevant
4. Uses professional, informative language
5. Avoids conversational phrases or unnecessary details
6. If this is a commuter rail alert, include the specific train number.
7. Don't repeat the same information in multiple locations.
8. For multi-day alerts do not mention individual train numbers or departures.

Keep it brief and focused."""

            return prompt

        except Exception as e:
            logger.error(f"Error formatting single alert for prompt: {e}")
            return f"Error formatting alert {getattr(alert, 'id', 'Unknown')}: {str(e)}"

    def set_similarity_threshold(self, threshold: float) -> None:
        """
        Set the minimum similarity threshold for response validation.

        Args:
            threshold: Minimum similarity threshold (0.0 to 1.0).
                      Set to 0.0 to disable validation entirely.
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Similarity threshold must be between 0.0 and 1.0")

        self.config.min_similarity_threshold = threshold
        logger.info(f"Updated similarity threshold to {threshold}")

    def set_short_response_validation(self, enabled: bool) -> None:
        """
        Enable or disable similarity validation for responses longer than 1 sentence.

        Args:
            enabled: If True, only validate responses with 1 sentence or less.
                    If False, validate all responses regardless of length.
        """
        self.config.validate_only_short_responses = enabled
        status = "enabled" if enabled else "disabled"
        logger.info(f"Short response validation {status}")

    def set_summary_rewriting(self, enabled: bool) -> None:
        """
        Enable or disable automatic summary rewriting for clarity and readability.

        Args:
            enabled: If True, automatically rewrite summaries for improved clarity.
                    If False, skip the rewriting step and return summaries as-is.
        """
        self.config.enable_summary_rewriting = enabled
        status = "enabled" if enabled else "disabled"
        logger.info(f"Summary rewriting {status}")

    def set_sentence_tolerance(self, tolerance: int) -> None:
        """
        Set the maximum allowed sentence count difference for summary rewriting.

        Args:
            tolerance: Maximum allowed sentence count difference (0 = exact match, 1 = ±1 sentence, etc.)
        """
        if tolerance < 0:
            raise ValueError("Sentence tolerance must be non-negative")

        self.config.rewrite_sentence_tolerance = tolerance
        logger.info(f"Updated sentence tolerance to {tolerance}")

    async def health_check(self) -> bool:
        """
        Check if the Ollama endpoint is healthy.

        Returns:
            True if healthy, False otherwise
        """
        try:
            client = self._get_client()
            # Use the list models endpoint to check health
            await client.list()
            return True

        except (ConnectionError, TimeoutError) as e:
            logger.warning(f"Ollama health check failed due to connection issue: {e}")
            return False
        except ValueError as e:
            logger.warning(f"Ollama health check failed due to invalid response: {e}")
            return False
        except Exception as e:
            logger.warning(f"Ollama health check failed due to unexpected error: {e}")
            return False

    def queue_individual_alert_summary_job(
        self,
        alert: AlertResource,
        priority: JobPriority = JobPriority.HIGH,
        sentence_limit: int = 2,
    ) -> str:
        """
        Queue a job to generate an individual alert summary.

        Args:
            alert: The alert to summarize
            priority: Job priority for processing
            sentence_limit: Maximum number of sentences for the summary (1 for ultra-concise, 2+ for standard)

        Returns:
            Job ID for tracking
        """
        try:
            # Use the existing job queue mechanism for individual alerts
            # The job queue will handle creating the SummaryJob internally
            job_id = asyncio.create_task(self.job_queue.add_job([alert], priority))

            logger.info(
                f"Queued individual alert summary job for alert {alert.id}, job_id: {job_id}"
            )
            return f"individual_{alert.id}_{int(time.time())}"

        except Exception as e:
            logger.error(f"Error queuing individual alert summary job: {e}")
            return ""

    def queue_bulk_individual_summaries(
        self,
        alerts: List[AlertResource],
        priority: JobPriority = JobPriority.LOW,
        sentence_limit: int = 2,
    ) -> List[str]:
        """
        Queue multiple individual alert summary jobs.

        Args:
            alerts: List of alerts to summarize individually
            priority: Job priority for processing
            sentence_limit: Maximum number of sentences for each summary (1 for ultra-concise, 2+ for standard)

        Returns:
            List of job IDs that were queued
        """
        job_ids = []

        for alert in alerts:
            if self._validate_alert_structure(alert):
                # Create config with sentence_limit for individual alerts
                config = {"sentence_limit": sentence_limit}
                job_id = asyncio.create_task(
                    self.job_queue.add_job([alert], priority, config)
                )
                if job_id:
                    job_ids.append(f"individual_{alert.id}_{int(time.time())}")
            else:
                logger.warning(
                    f"Skipping invalid alert {alert.id} for individual summary"
                )

        logger.info(f"Queued {len(job_ids)} individual alert summary jobs")
        return job_ids
