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
        self.max_concurrent_jobs = max_concurrent_jobs
        self.max_queue_size = max_queue_size
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

    async def _worker_loop(self) -> None:
        """Main worker loop that processes jobs."""
        while self._running:
            try:
                # Check if we can process more jobs
                if (
                    len(self.running_jobs) < self.max_concurrent_jobs
                    and self.pending_jobs
                ):
                    job_id = self.pending_jobs.pop(0)
                    asyncio.create_task(self._process_job(job_id))

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

            # Process the job
            logger.info(f"Processing summary job {job_id}")

            # Generate the actual AI summary
            summary = await self._generate_summary(job.alerts, job.config)

            # Store the summary in Redis
            await self._store_summary_in_redis(job.alerts, summary, job.config)

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
                # Create a summarization request
                request = SummarizationRequest(
                    alerts=alerts,
                    include_route_info=config.get("include_route_info", True),
                    include_severity=config.get("include_severity", True),
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

            if attrs.cause:
                parts.append(f"   Cause: {attrs.cause}")

            prompt_parts.append("\n".join(parts))

        prompt_parts.append(
            "\nPlease provide a clear, structured summary that covers each alert individually."
            "\nDo not directly respond to this prompt with 'Okay', 'Got it', or 'I understand'. or anything else."
            "\nConvert IDs like 'UNKNOWN_CAUSE' to 'Unknown Cause'."
            "\nUse proper grammar and punctuation."
            "\nFor each alert, provide a brief summary of the key details, then provide an overall assessment."
            "\nFocus on the most important details and provide a complete summary. Do not stop mid-sentence."
            "\n\nIMPORTANT: Generate a complete summary covering all alerts. Do not truncate or stop early."
            "\nInclude specific details for each alert and provide a comprehensive overview."
            "\n\nExpected format:"
            "\n1. Brief overview of the situation"
            "\n2. Individual alert summaries (one paragraph per alert)"
            "\n3. Overall assessment and common themes"
            "\n4. Recommendations or key points for commuters"
            "\n\nMake sure to cover ALL alerts completely. Do not stop until you have provided a full summary."
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

    def _clean_model_response(self, summary: str) -> str:
        """Clean up direct responses from the model and remove conversational phrases using regex."""
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
        return cleaned_summary

    async def _call_ollama(self, prompt: str) -> str:
        """
        Call the Ollama API to generate a summary using the official client.

        Args:
            prompt: The prompt to send to the model

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
                # Clean up conversational starters
                cleaned_content = self._clean_model_response(cleaned_content)
                # Check for truncation
                final_content = self._detect_truncation(cleaned_content, prompt)
                logger.debug(
                    f"Generated summary content (length: {len(final_content)}):\n{final_content}"
                )
                logger.debug(f"Raw Ollama response: {response}")
                return final_content
            elif hasattr(response, "content"):
                content = response.content
                if content is None:
                    return "No content generated by model"
                cleaned_content = content.strip()
                # Clean up conversational starters
                cleaned_content = self._clean_model_response(cleaned_content)
                # Check for truncation
                final_content = self._detect_truncation(cleaned_content, prompt)
                logger.debug(
                    f"Generated summary content (length: {len(final_content)}):\n{final_content}"
                )
                logger.debug(f"Raw Ollama response: {response}")
                return final_content
            else:
                # Fallback: try to get content from response directly
                fallback_content = str(response).strip()
                # Clean up conversational starters
                fallback_content = self._clean_model_response(fallback_content)
                # Check for truncation
                final_content = self._detect_truncation(fallback_content, prompt)
                logger.debug(
                    f"Generated summary content (fallback, length: {len(final_content)}):\n{final_content}"
                )
                logger.debug(f"Raw Ollama response: {response}")
                return final_content

        except (ConnectionError, TimeoutError) as e:
            raise ConnectionError(f"Failed to connect to Ollama API: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid response from Ollama API: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error calling Ollama API: {e}")

    def _detect_truncation(self, summary: str, prompt: str) -> str:
        """Detect if a summary appears to be truncated and add a note."""
        # Check for common truncation indicators
        truncation_indicators = [
            "Do not stop mid-sentence",
            "Do not truncate",
            "complete summary",
            "full summary",
            "all alerts completely",
        ]

        # If the summary ends with any of these phrases, it's likely truncated
        summary_lower = summary.lower()
        for indicator in truncation_indicators:
            if summary_lower.endswith(indicator.lower()):
                logger.warning(
                    f"Summary appears to be truncated, ending with: {indicator}"
                )
                return (
                    summary
                    + "\n\n[Note: This summary appears to have been truncated. Please regenerate for complete coverage.]"
                )

        # Check if summary is suspiciously short for the number of alerts
        if len(summary) < 200:  # Very short summary
            logger.warning(
                f"Summary is very short ({len(summary)} chars), may be truncated"
            )
            return (
                summary
                + "\n\n[Note: This summary appears to be incomplete. Please regenerate for complete coverage.]"
            )

        return summary

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

            # Format alerts into a prompt for group summary
            prompt = self._format_alerts_for_prompt(
                request.alerts, request.include_route_info, request.include_severity
            )

            # Generate group summary using Ollama
            summary = await self._call_ollama(prompt)

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

    async def summarize_individual_alert(self, alert: AlertResource, priority: JobPriority = JobPriority.NORMAL) -> str:
        """
        Generate an AI summary for a single alert.
        
        Args:
            alert: The alert to summarize
            priority: Job priority for processing
            
        Returns:
            The generated summary text
        """
        try:
            # Validate the alert structure
            if not self._validate_alert_structure(alert):
                logger.warning(f"Skipping invalid alert {alert.id}: missing required attributes")
                return f"Unable to process alert {alert.id}. Please check alert format."
            
            # Format the single alert for the prompt
            alert_text = self._format_single_alert_for_prompt(alert)
            
            # Generate the summary using Ollama
            summary = await self._call_ollama(alert_text)
            
            if not summary:
                return f"Unable to generate summary for alert {alert.id}."
            
            # Clean the model response
            cleaned_summary = self._clean_model_response(summary)
            
            logger.info(f"Generated individual summary for alert {alert.id}")
            return cleaned_summary
            
        except Exception as e:
            logger.error(f"Error generating individual summary for alert {alert.id}: {e}")
            return f"Error generating summary for alert {alert.id}: {str(e)}"
    
    def _format_single_alert_for_prompt(self, alert: AlertResource) -> str:
        """
        Format a single alert for the AI prompt.
        
        Args:
            alert: The alert to format
            
        Returns:
            Formatted alert text for the prompt
        """
        try:
            # Extract alert information
            alert_id = getattr(alert, 'id', 'Unknown ID')
            header = getattr(alert.attributes, 'header', 'No header available')
            description = getattr(alert.attributes, 'description', 'No description available')
            severity = getattr(alert.attributes, 'severity', 'Unknown severity')
            
            # Get route information if available
            route_info = ""
            if hasattr(alert, 'relationships') and alert.relationships:
                routes = alert.relationships.get('route', {}).get('data', [])
                if routes:
                    route_names = [route.get('id', 'Unknown route') for route in routes]
                    route_info = f"Affected routes: {', '.join(route_names)}"
            
            # Format the prompt for a single alert
            prompt = f"""Please provide a clear, concise summary of this MBTA transit alert:

Alert ID: {alert_id}
Header: {header}
Description: {description}
Severity: {severity}
{route_info}

Please provide a summary that:
1. Explains what the issue is in simple terms
2. Describes the impact on transit service
3. Mentions any affected routes or areas
4. Is written in a professional, informative tone
5. Avoids conversational language or unnecessary phrases

Summary:"""

            return prompt
            
        except Exception as e:
            logger.error(f"Error formatting single alert for prompt: {e}")
            return f"Error formatting alert {getattr(alert, 'id', 'Unknown')}: {str(e)}"

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

    def queue_individual_alert_summary_job(self, alert: AlertResource, priority: JobPriority = JobPriority.NORMAL) -> str:
        """
        Queue a job to generate an individual alert summary.
        
        Args:
            alert: The alert to summarize
            priority: Job priority for processing
            
        Returns:
            Job ID for tracking
        """
        try:
            # Create a unique job ID for this individual alert
            job_id = f"individual_{alert.id}_{int(time.time())}"
            
            # Create the job
            job = SummaryJob(
                id=job_id,
                alerts=[alert], # Changed to a single alert
                priority=priority,
                created_at=datetime.utcnow(),
                status=JobStatus.PENDING,
                job_type="individual_alert"
            )
            
            # Add to the queue
            self.job_queue._add_job_to_queue(job) # Assuming _add_job_to_queue is a method of SummaryJobQueue
            logger.info(f"Queued individual alert summary job {job_id} for alert {alert.id}")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Error queuing individual alert summary job: {e}")
            return ""
    
    def queue_bulk_individual_summaries(self, alerts: List[AlertResource], priority: JobPriority = JobPriority.NORMAL) -> List[str]:
        """
        Queue jobs to generate individual summaries for multiple alerts.
        
        Args:
            alerts: List of alerts to summarize
            priority: Job priority for processing
            
        Returns:
            List of job IDs for tracking
        """
        job_ids = []
        
        for alert in alerts:
            if self._validate_alert_structure(alert):
                job_id = self.queue_individual_alert_summary_job(alert, priority)
                if job_id:
                    job_ids.append(job_id)
            else:
                logger.warning(f"Skipping invalid alert {alert.id} for individual summary")
        
        logger.info(f"Queued {len(job_ids)} individual alert summary jobs")
        return job_ids
