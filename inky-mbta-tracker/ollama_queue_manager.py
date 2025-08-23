import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Awaitable, Dict, List, Optional, TypeVar, Union, cast, overload

from redis.asyncio import Redis

T = TypeVar("T")


logger = logging.getLogger(__name__)


class OllamaCommandType(Enum):
    """Types of Ollama commands that can be queued"""

    SUMMARIZE_ALERTS = "summarize_alerts"
    SUMMARIZE_ALERT_GROUP = "summarize_alert_group"
    REWRITE_SUMMARY = "rewrite_summary"
    GENERATE_TEXT = "generate_text"
    EMBED_TEXT = "embed_text"
    CHAT_COMPLETION = "chat_completion"
    CUSTOM_PROMPT = "custom_prompt"


class OllamaJobStatus(Enum):
    """Status of an Ollama job in the queue"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class OllamaJobPriority(Enum):
    """Priority levels for Ollama jobs"""

    LOW = 1  # Background tasks, non-urgent
    NORMAL = 2  # Regular requests
    HIGH = 3  # Important requests
    URGENT = 4  # Critical requests
    EMERGENCY = 5  # Emergency requests


@dataclass
class OllamaJob:
    """Represents a job in the Ollama command queue"""

    id: str
    command_type: OllamaCommandType
    priority: OllamaJobPriority
    payload: Dict[str, Any]
    created_at: datetime
    status: OllamaJobStatus = OllamaJobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    backoff_until: Optional[datetime] = None
    worker_id: Optional[str] = None
    timeout_seconds: int = 300
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for Redis storage"""
        return {
            "id": self.id,
            "command_type": self.command_type.value,
            "priority": self.priority.value,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "result": self.result,
            "error": self.error,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "backoff_until": self.backoff_until.isoformat()
            if self.backoff_until
            else None,
            "worker_id": self.worker_id,
            "timeout_seconds": self.timeout_seconds,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OllamaJob":
        """Create job from dictionary from Redis storage"""
        return cls(
            id=data["id"],
            command_type=OllamaCommandType(data["command_type"]),
            priority=OllamaJobPriority(data["priority"]),
            payload=data["payload"],
            created_at=datetime.fromisoformat(data["created_at"]),
            status=OllamaJobStatus(data["status"]),
            started_at=datetime.fromisoformat(data["started_at"])
            if data.get("started_at")
            else None,
            completed_at=datetime.fromisoformat(data["completed_at"])
            if data.get("completed_at")
            else None,
            result=data.get("result"),
            error=data.get("error"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            backoff_until=datetime.fromisoformat(data["backoff_until"])
            if data.get("backoff_until")
            else None,
            worker_id=data.get("worker_id"),
            timeout_seconds=data.get("timeout_seconds", 300),
            metadata=data.get("metadata", {}),
        )


class OllamaQueueManager:
    """Redis-based queue manager for Ollama commands"""

    def __init__(self, redis_client: Redis, queue_name: str = "ollama_commands"):
        self.redis = redis_client
        self.queue_name = queue_name
        self.processing_set = f"{queue_name}:processing"
        self.completed_set = f"{queue_name}:completed"
        self.failed_set = f"{queue_name}:failed"
        self.stats_key = f"{queue_name}:stats"
        self.worker_prefix = f"{queue_name}:worker"

    @overload
    async def _await_maybe(self, value: Awaitable[T]) -> T: ...

    @overload
    async def _await_maybe(self, value: T) -> T: ...

    async def _await_maybe(self, value: Union[Awaitable[T], T]) -> T:
        """Await a value if it's awaitable; otherwise return it directly.
        Some redis.asyncio APIs are typed to possibly return either an
        awaitable or a concrete value depending on configuration.
        """
        if asyncio.iscoroutine(value):
            return await value
        return cast(T, value)

    async def _redis_srem(self, key: str, member: str) -> int:
        """Helper to handle Redis SREM operation with proper typing"""
        result = await self._await_maybe(self.redis.srem(key, member))
        if isinstance(result, int):
            return result
        return 0

    async def _redis_sadd(self, key: str, member: str) -> int:
        """Helper to handle Redis SADD operation with proper typing"""
        result = await self._await_maybe(self.redis.sadd(key, member))
        if isinstance(result, int):
            return result
        return 0

    async def _redis_zrem(self, key: str, member: str) -> int:
        """Helper to handle Redis ZREM operation with proper typing"""
        result = await self._await_maybe(self.redis.zrem(key, member))
        if isinstance(result, int):
            return result
        return 0

    async def _redis_zcard(self, key: str) -> int:
        """Helper to handle Redis ZCARD operation with proper typing"""
        result = await self._await_maybe(self.redis.zcard(key))
        if isinstance(result, int):
            return result
        return 0

    async def _redis_scard(self, key: str) -> int:
        """Helper to handle Redis SCARD operation with proper typing"""
        result = await self._await_maybe(self.redis.scard(key))
        if isinstance(result, int):
            return result
        return 0

    async def _redis_hgetall(self, key: str) -> Dict[str, str]:
        """Helper to handle Redis HGETALL operation with proper typing"""
        result = await self._await_maybe(self.redis.hgetall(key))
        if isinstance(result, dict):
            return result
        elif isinstance(result, list):
            # Handle case where Redis returns list of key-value pairs
            return dict(zip(result[::2], result[1::2]))
        else:
            return {}

    async def enqueue_job(
        self,
        command_type: OllamaCommandType,
        payload: Dict[str, Any],
        priority: OllamaJobPriority = OllamaJobPriority.NORMAL,
        timeout_seconds: int = 300,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Enqueue a new Ollama job"""
        job_id = str(uuid.uuid4())
        job = OllamaJob(
            id=job_id,
            command_type=command_type,
            priority=priority,
            payload=payload,
            created_at=datetime.utcnow(),
            timeout_seconds=timeout_seconds,
            metadata=metadata or {},
        )

        # Store job data
        job_key = f"{self.queue_name}:job:{job_id}"
        await self.redis.setex(
            job_key,
            timeout_seconds + 3600,  # Keep job data for 1 hour after timeout
            json.dumps(job.to_dict()),
        )

        # Add to priority queue (using Redis sorted set with timestamp + priority)
        score = self._calculate_priority_score(job.priority, job.created_at)
        await self.redis.zadd(self.queue_name, {job_id: score})

        # Update stats
        await self._increment_stat("enqueued")

        logger.info(
            f"Enqueued Ollama job {job_id} ({command_type.value}) with priority {priority.value}"
        )
        return job_id

    async def dequeue_job(
        self, worker_id: str, timeout: int = 5
    ) -> Optional[OllamaJob]:
        """Dequeue the next available job for processing"""
        # Get jobs that are not in processing state and not in backoff
        now = datetime.utcnow()

        # Get all pending jobs
        pending_jobs = await self.redis.zrange(self.queue_name, 0, -1, withscores=True)

        for job_id, score in pending_jobs:
            job_id = job_id.decode() if isinstance(job_id, bytes) else job_id

            # Check if job is available for processing
            if await self._is_job_available(job_id, now):
                # Try to claim the job
                if await self._claim_job(job_id, worker_id):
                    job = await self._get_job(job_id)
                    if job:
                        await self._update_job_status(
                            job_id, OllamaJobStatus.PROCESSING, worker_id
                        )
                        await self._increment_stat("dequeued")
                        logger.debug(f"Worker {worker_id} claimed job {job_id}")
                        return job

        return None

    async def complete_job(self, job_id: str, result: Any, worker_id: str) -> bool:
        """Mark a job as completed with results"""
        job = await self._get_job(job_id)
        if not job or job.worker_id != worker_id:
            return False

        # Update job status
        job.status = OllamaJobStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        job.result = result

        # Store completed job
        await self._store_job(job)

        # Remove from processing and add to completed
        await self._redis_srem(self.processing_set, job_id)
        await self._redis_sadd(self.completed_set, job_id)

        # Remove from main queue
        await self._redis_zrem(self.queue_name, job_id)

        # Update stats
        await self._increment_stat("completed")

        logger.info(f"Job {job_id} completed by worker {worker_id}")
        return True

    async def fail_job(self, job_id: str, error: str, worker_id: str) -> bool:
        """Mark a job as failed with error details"""
        job = await self._get_job(job_id)
        if not job or job.worker_id != worker_id:
            return False

        # Check if we should retry
        if job.retry_count < job.max_retries:
            # Calculate backoff delay
            backoff_delay = min(
                60 * (2**job.retry_count), 3600
            )  # Exponential backoff, max 1 hour
            backoff_until = datetime.utcnow() + timedelta(seconds=backoff_delay)

            job.status = OllamaJobStatus.RETRYING
            job.retry_count += 1
            job.backoff_until = backoff_until
            job.error = error

            await self._store_job(job)
            await self._redis_srem(self.processing_set, job_id)

            # Update stats
            await self._increment_stat("retrying")

            logger.warning(
                f"Job {job_id} failed, will retry {job.retry_count}/{job.max_retries} at {backoff_until}"
            )
        else:
            # Max retries exceeded, mark as failed
            job.status = OllamaJobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error = error

            await self._store_job(job)
            await self._redis_srem(self.processing_set, job_id)
            await self._redis_sadd(self.failed_set, job_id)
            await self._redis_zrem(self.queue_name, job_id)

            # Update stats
            await self._increment_stat("failed")

            logger.error(
                f"Job {job_id} failed permanently after {job.max_retries} retries: {error}"
            )

        return True

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job"""
        job = await self._get_job(job_id)
        if not job or job.status != OllamaJobStatus.PENDING:
            return False

        # Remove from queue
        await self._redis_zrem(self.queue_name, job_id)

        # Update job status
        job.status = OllamaJobStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        await self._store_job(job)

        # Update stats
        await self._increment_stat("cancelled")

        logger.info(f"Job {job_id} cancelled")
        return True

    async def get_job_status(self, job_id: str) -> Optional[OllamaJobStatus]:
        """Get the current status of a job"""
        job = await self._get_job(job_id)
        return job.status if job else None

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""
        stats: Dict[str, Any] = {}

        # Get counts from Redis
        stats["pending"] = await self._redis_zcard(self.queue_name)
        stats["processing"] = await self._redis_scard(self.processing_set)
        stats["completed"] = await self._redis_scard(self.completed_set)
        stats["failed"] = await self._redis_scard(self.failed_set)

        # Get detailed stats
        raw_stats = await self._redis_hgetall(self.stats_key)
        for key, value in raw_stats.items():
            if isinstance(key, bytes):
                key = key.decode()
            if isinstance(value, bytes):
                value = value.decode()
            try:
                stats[key] = int(value)
            except ValueError:
                stats[key] = value

        # Calculate additional metrics
        total_processed = stats.get("completed", 0) + stats.get("failed", 0)
        if total_processed > 0:
            stats["success_rate"] = round(
                stats.get("completed", 0) / total_processed * 100, 2
            )
        else:
            stats["success_rate"] = 0.0

        return stats

    async def get_job_details(self, job_id: str) -> Optional[OllamaJob]:
        """Get detailed information about a specific job"""
        return await self._get_job(job_id)

    async def list_jobs(
        self,
        status: Optional[OllamaJobStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[OllamaJob]:
        """List jobs with optional filtering"""
        jobs = []

        if status == OllamaJobStatus.PENDING:
            # Get from priority queue
            job_ids = await self.redis.zrange(
                self.queue_name, offset, offset + limit - 1
            )
        elif status == OllamaJobStatus.PROCESSING:
            # Get from processing set
            job_ids = await self._await_maybe(self.redis.smembers(self.processing_set))
            job_ids = list(job_ids)[offset : offset + limit]
        elif status == OllamaJobStatus.COMPLETED:
            # Get from completed set
            job_ids = await self._await_maybe(self.redis.smembers(self.completed_set))
            job_ids = list(job_ids)[offset : offset + limit]
        elif status == OllamaJobStatus.FAILED:
            # Get from failed set
            job_ids = await self._await_maybe(self.redis.smembers(self.failed_set))
            job_ids = list(job_ids)[offset : offset + limit]
        else:
            # Get all jobs
            all_jobs = []
            all_jobs.extend(await self.redis.zrange(self.queue_name, 0, -1))
            all_jobs.extend(
                await self._await_maybe(self.redis.smembers(self.processing_set))
            )
            all_jobs.extend(
                await self._await_maybe(self.redis.smembers(self.completed_set))
            )
            all_jobs.extend(
                await self._await_maybe(self.redis.smembers(self.failed_set))
            )
            job_ids = all_jobs[offset : offset + limit]

        # Decode job IDs and fetch job details
        for job_id in job_ids:
            if isinstance(job_id, bytes):
                job_id = job_id.decode()
            job = await self._get_job(job_id)
            if job:
                jobs.append(job)

        return jobs

    async def clear_completed_jobs(self, older_than_hours: int = 24) -> int:
        """Clear completed jobs older than specified hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=older_than_hours)
        cleared_count = 0

        # Get completed jobs
        completed_jobs: set[Any] = await self._await_maybe(
            self.redis.smembers(self.completed_set)
        )

        for job_id in completed_jobs:
            if isinstance(job_id, bytes):
                job_id = job_id.decode()

            job = await self._get_job(job_id)
            if job and job.completed_at and job.completed_at < cutoff_time:
                # Remove job data and from completed set
                await self.redis.delete(f"{self.queue_name}:job:{job_id}")
                await self._redis_srem(self.completed_set, job_id)
                cleared_count += 1

        # Update stats
        if cleared_count > 0:
            await self._increment_stat("cleared", cleared_count)

        logger.info(
            f"Cleared {cleared_count} completed jobs older than {older_than_hours} hours"
        )
        return cleared_count

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the queue system"""
        try:
            # Test basic Redis operations
            await self.redis.ping()

            # Test queue operations
            test_job_id = await self.enqueue_job(
                OllamaCommandType.CUSTOM_PROMPT,
                {"test": True},
                OllamaJobPriority.LOW,
                timeout_seconds=60,
            )

            # Clean up test job
            await self.cancel_job(test_job_id)

            return {
                "status": "healthy",
                "redis_connection": "ok",
                "queue_operations": "ok",
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    # Private helper methods

    def _calculate_priority_score(
        self, priority: OllamaJobPriority, created_at: datetime
    ) -> float:
        """Calculate Redis sorted set score based on priority and creation time"""
        # Higher priority = lower score (Redis sorts ascending)
        # Within same priority, older jobs get lower scores
        base_score = 1000000 - priority.value * 100000
        time_score = created_at.timestamp()
        return base_score + time_score

    async def _is_job_available(self, job_id: str, now: datetime) -> bool:
        """Check if a job is available for processing"""
        job = await self._get_job(job_id)
        if not job:
            return False

        # Check if job is in backoff
        if job.backoff_until and job.backoff_until > now:
            return False

        # Check if job is already being processed
        is_processing = await self._await_maybe(
            self.redis.sismember(self.processing_set, job_id)
        )
        if bool(is_processing):
            return False

        return True

    async def _claim_job(self, job_id: str, worker_id: str) -> bool:
        """Try to claim a job for processing"""
        # Use Redis SETNX to atomically claim the job
        claimed = await self._redis_sadd(self.processing_set, job_id)
        if claimed:
            # Update job with worker ID
            job = await self._get_job(job_id)
            if job:
                job.worker_id = worker_id
                job.started_at = datetime.utcnow()
                await self._store_job(job)
            return True
        return False

    async def _get_job(self, job_id: str) -> Optional[OllamaJob]:
        """Retrieve job data from Redis"""
        try:
            job_data = await self.redis.get(f"{self.queue_name}:job:{job_id}")
            if job_data:
                return OllamaJob.from_dict(json.loads(job_data))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error parsing job {job_id}: {e}")
        return None

    async def _store_job(self, job: OllamaJob) -> None:
        """Store job data in Redis"""
        job_key = f"{self.queue_name}:job:{job.id}"
        await self.redis.setex(
            job_key, job.timeout_seconds + 3600, json.dumps(job.to_dict())
        )

    async def _update_job_status(
        self, job_id: str, status: OllamaJobStatus, worker_id: Optional[str] = None
    ) -> None:
        """Update job status in Redis"""
        job = await self._get_job(job_id)
        if job:
            job.status = status
            if worker_id:
                job.worker_id = worker_id
            if status == OllamaJobStatus.PROCESSING:
                job.started_at = datetime.utcnow()
            await self._store_job(job)

    async def _increment_stat(self, stat_name: str, increment: int = 1) -> None:
        """Increment a statistic counter"""
        try:
            await self._await_maybe(
                self.redis.hincrby(self.stats_key, stat_name, increment)
            )
            # Set expiry on stats (1 day)
            await self._await_maybe(self.redis.expire(self.stats_key, 86400))
        except Exception as e:
            logger.warning(f"Failed to increment stat {stat_name}: {e}")


class OllamaQueueWorker:
    """Worker for processing Ollama jobs from the queue"""

    def __init__(
        self,
        queue_manager: OllamaQueueManager,
        worker_id: str,
        poll_interval: float = 1.0,
        max_concurrent_jobs: int = 1,
        ai_summarizer: Optional[
            Any
        ] = None,  # Reference to AISummarizer for processing jobs
    ):
        self.queue_manager = queue_manager
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        self.max_concurrent_jobs = max_concurrent_jobs
        self.ai_summarizer = ai_summarizer
        self.running = False
        self.active_jobs: Dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        """Start the worker"""
        self.running = True
        logger.info(f"Starting Ollama queue worker {self.worker_id}")

        while self.running:
            try:
                # Check if we can process more jobs
                if len(self.active_jobs) < self.max_concurrent_jobs:
                    job = await self.queue_manager.dequeue_job(
                        self.worker_id, timeout=1
                    )
                    if job:
                        # Start processing the job
                        task = asyncio.create_task(self._process_job(job))
                        self.active_jobs[job.id] = task

                        # Clean up completed tasks
                        self.active_jobs = {
                            job_id: task
                            for job_id, task in self.active_jobs.items()
                            if not task.done()
                        }

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        """Stop the worker"""
        self.running = False
        logger.info(f"Stopping Ollama queue worker {self.worker_id}")

        # Wait for active jobs to complete
        if self.active_jobs:
            logger.info(
                f"Waiting for {len(self.active_jobs)} active jobs to complete..."
            )
            await asyncio.gather(*self.active_jobs.values(), return_exceptions=True)

    async def _process_job(self, job: OllamaJob) -> None:
        """Process a single job"""
        try:
            logger.info(
                f"Worker {self.worker_id} processing job {job.id} ({job.command_type.value})"
            )

            # Process the job based on command type
            if job.command_type == OllamaCommandType.SUMMARIZE_ALERTS:
                result = await self._process_summarize_alerts_job(job)
            elif job.command_type == OllamaCommandType.SUMMARIZE_ALERT_GROUP:
                result = await self._process_summarize_alert_group_job(job)
            elif job.command_type == OllamaCommandType.REWRITE_SUMMARY:
                result = await self._process_rewrite_summary_job(job)
            elif job.command_type == OllamaCommandType.GENERATE_TEXT:
                result = await self._process_generate_text_job(job)
            elif job.command_type == OllamaCommandType.EMBED_TEXT:
                result = await self._process_embed_text_job(job)
            elif job.command_type == OllamaCommandType.CHAT_COMPLETION:
                result = await self._process_chat_completion_job(job)
            elif job.command_type == OllamaCommandType.CUSTOM_PROMPT:
                result = await self._process_custom_prompt_job(job)
            else:
                raise ValueError(f"Unknown command type: {job.command_type}")

            # Mark job as completed
            await self.queue_manager.complete_job(job.id, result, self.worker_id)

        except Exception as e:
            logger.error(f"Worker {self.worker_id} failed to process job {job.id}: {e}")
            await self.queue_manager.fail_job(job.id, str(e), self.worker_id)
        finally:
            # Remove from active jobs
            if job.id in self.active_jobs:
                del self.active_jobs[job.id]

    async def _process_summarize_alerts_job(self, job: OllamaJob) -> str:
        """Process a summarize alerts job."""
        if not self.ai_summarizer:
            raise RuntimeError("AI summarizer not available")

        # Extract alerts from payload
        alerts_data = job.payload.get("alerts", [])
        if not alerts_data:
            raise ValueError("No alerts in job payload")

        # Convert back to AlertResource objects (simplified - you may need to enhance this)
        from mbta_responses import AlertResource

        alerts = [AlertResource(**alert) for alert in alerts_data]

        # Create summarization request
        from ai_summarizer import SummarizationRequest

        request = SummarizationRequest(
            alerts=alerts,
            include_route_info=job.payload.get("include_route_info", True),
            include_severity=job.payload.get("include_severity", True),
            format=job.payload.get("format", "text"),
            style=job.payload.get("style", "comprehensive"),
        )

        # Call the AI summarizer
        response = await self.ai_summarizer.summarize_alerts(request)
        return response.summary

    async def _process_summarize_alert_group_job(self, job: OllamaJob) -> str:
        """Process a summarize alert group job."""
        # Similar to summarize alerts but for grouped alerts
        return await self._process_summarize_alerts_job(job)

    async def _process_rewrite_summary_job(self, job: OllamaJob) -> str:
        """Process a rewrite summary job."""
        if not self.ai_summarizer:
            raise RuntimeError("AI summarizer not available")

        # Extract summary and alerts from payload
        summary = job.payload.get("summary", "")
        alerts_data = job.payload.get("alerts", [])

        if not summary or not alerts_data:
            raise ValueError("Missing summary or alerts in job payload")

        # Convert alerts back to AlertResource objects
        from mbta_responses import AlertResource

        alerts = [AlertResource(**alert) for alert in alerts_data]

        # Call the AI summarizer's rewrite method if available
        # This would need to be implemented in the AI summarizer
        return f"Rewritten summary for {len(alerts)} alerts: {summary[:100]}..."

    async def _process_generate_text_job(self, job: OllamaJob) -> str:
        """Process a generate text job."""
        prompt = job.payload.get("prompt", "")
        if not prompt:
            raise ValueError("No prompt in job payload")

        # For now, return a simple response
        # In a real implementation, you'd call Ollama directly
        return f"Generated text for prompt: {prompt[:100]}..."

    async def _process_embed_text_job(self, job: OllamaJob) -> str:
        """Process an embed text job."""
        text = job.payload.get("text", "")
        if not text:
            raise ValueError("No text in job payload")

        # For now, return a simple response
        # In a real implementation, you'd call Ollama's embedding API
        return f"Generated embedding for text: {text[:100]}..."

    async def _process_chat_completion_job(self, job: OllamaJob) -> str:
        """Process a chat completion job."""
        messages = job.payload.get("messages", [])
        if not messages:
            raise ValueError("No messages in job payload")

        # For now, return a simple response
        # In a real implementation, you'd call Ollama's chat API
        return f"Chat completion for {len(messages)} messages"

    async def _process_custom_prompt_job(self, job: OllamaJob) -> str:
        """Process a custom prompt job."""
        prompt = job.payload.get("prompt", "")
        if not prompt:
            raise ValueError("No prompt in job payload")

        # For now, return a simple response
        # In a real implementation, you'd call Ollama with the custom prompt
        return f"Custom prompt response: {prompt[:100]}..."
