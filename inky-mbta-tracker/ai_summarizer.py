import asyncio
import hashlib
import json
import logging
import os
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
    max_length: int = Field(default=200, description="Maximum length of summary")
    include_route_info: bool = Field(
        default=True, description="Include route information in summary"
    )
    include_severity: bool = Field(
        default=True, description="Include severity information in summary"
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
                    max_length=config.get("max_length", 300),
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
        """Load configuration from environment variables."""
        return OllamaConfig(
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            model=os.getenv("OLLAMA_MODEL", "llama3.2:1b"),
            timeout=int(os.getenv("OLLAMA_TIMEOUT", "30")),
            temperature=float(os.getenv("OLLAMA_TEMPERATURE", "0.1")),
        )

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
            "\nFor each alert, provide a brief summary of the key details, then provide an overall assessment."
            "\nFocus on the most important details and provide a complete summary. Do not stop mid-sentence."
        )

        return "\n\n".join(prompt_parts)

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

        try:
            response = await client.chat(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                options={
                    "temperature": self.config.temperature,
                    "top_p": 0.9,
                    "top_k": 40,
                    "num_predict": 500,
                    "stop": ["\n\n", "###", "---"],
                },
            )

            # Handle the response based on Ollama client structure
            if hasattr(response, "message") and hasattr(response.message, "content"):
                content = response.message.content
                if content is None:
                    return "No content generated by model"
                return content.strip()
            elif hasattr(response, "content"):
                content = response.content
                if content is None:
                    return "No content generated by model"
                return content.strip()
            else:
                # Fallback: try to get content from response directly
                return str(response).strip()

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
            max_length=config.get("max_length", 300) if config else 300,
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

            # Truncate if necessary
            if len(summary) > request.max_length:
                summary = summary[: request.max_length].rsplit(" ", 1)[0] + "..."

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
