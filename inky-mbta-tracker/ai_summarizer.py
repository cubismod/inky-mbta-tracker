import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from anyio import CapacityLimiter, create_task_group
from anyio.abc import TaskGroup
from config import OllamaConfig
from mbta_responses import AlertResource
from ollama import AsyncClient
from pydantic import BaseModel, Field
from shared_types.shared_types import (
    IndividualSummaryCacheEntry,
    SummarizationResponse,
    SummaryCacheEntry,
)
from utils import get_redis
import culsans

logger = logging.getLogger(__name__)


class SummarizationRequest(BaseModel):
    """Request model retained for API compatibility."""

    alerts: List[AlertResource]
    include_route_info: bool = Field(default=True)
    include_severity: bool = Field(default=True)
    format: str = Field(default="text")
    style: str = Field(default="comprehensive")


# Priority queue wrapper — uses culsans if available, falls back to a simple heap


@dataclass
class _Job:
    id: str
    priority: int  # lower number = higher priority
    kind: str  # "group" | "individual"
    alerts: List[AlertResource]
    config: Dict[str, Any] = field(default_factory=dict)
    sentence_limit: int = 2
    user_id: str = "anon"


class AISummarizer():
    """Ollama summarizer with per-user concurrency and a priority queue."""

    def __init__(self, config: OllamaConfig, per_user_limit: int = 1) -> None:
        self.config = config
        self._client: Optional[AsyncClient] = None
        self._per_user_limit = max(1, per_user_limit)
        self._user_limiters: dict[str, CapacityLimiter] = {}
        self._queue: culsans.PriorityQueue = culsans.PriorityQueue()
        self._tg: Optional[TaskGroup] = None
        self._running = False
        self.redis = get_redis()

    # Lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Manually enter a TaskGroup to keep it alive across lifespan
        self._tg = await create_task_group().__aenter__()
        self._tg.start_soon(self._worker_loop)
        logger.info("AI summarizer worker started")

    async def stop(self) -> None:
        self._running = False
        if self._tg is not None:
            tg, self._tg = self._tg, None
            await tg.__aexit__(None, None, None)

    # Public API ------------------------------------------------------------
    async def summarize_alerts(
        self, request: SummarizationRequest, user_id: Optional[str] = None, priority: int = 2
    ) -> SummarizationResponse:
        started = time.perf_counter()
        text = await self._generate_with_limits(
            request.alerts, request.style, user_id or "anon", request
        )
        took_ms = (time.perf_counter() - started) * 1000.0
        return SummarizationResponse(
            summary=text,
            alert_count=len(request.alerts),
            model_used=self.config.model,
            processing_time_ms=round(took_ms, 2),
        )

    async def summarize_individual_alert(
        self,
        alert: AlertResource,
        style: str = "concise",
        sentence_limit: int = 2,
        user_id: Optional[str] = None,
        priority: int = 2,
    ) -> str:
        return await self._generate_with_limits(
            [alert], style, user_id or "anon", None, sentence_limit
        )

    async def force_generate_summary(
        self, alerts: List[AlertResource], config: Optional[Dict[str, Any]] = None
    ) -> str:
        style = "concise" if len(alerts) == 1 else "comprehensive"
        return await self._generate_with_limits(
            alerts,
            style,
            "anon",
            None,
            config.get("sentence_limit", 2) if config else 2,
        )

    async def health_check(self) -> bool:
        try:
            client = self._get_client()
            await client.list()
            return True
        except Exception as e:
            logger.warning(f"Ollama health check failed: {e}")
            return False

    async def queue_summary_job(
        self,
        alerts: List[AlertResource],
        priority: int = 2,
        config: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> str:
        job_id = f"sum:{int(time.time()*1000)}"
        job = _Job(
            id=job_id,
            priority=priority,
            kind="group",
            alerts=alerts,
            config=config or {},
            user_id=user_id or "anon",
        )
        await self._queue.put((priority, job))
        return job_id

    def queue_bulk_individual_summaries(
        self, alerts: List[AlertResource], sentence_limit: int = 2, priority: int = 3, user_id: Optional[str] = None
    ) -> List[str]:
        job_ids: List[str] = []
        uid = user_id or "anon"

        # Generate IDs synchronously, enqueue with TaskGroup if available
        jobs: List[Tuple[str, _Job]] = []
        for alert in alerts:
            job_id = f"ind:{alert.id}:{int(time.time()*1000)}"
            job = _Job(
                id=job_id,
                priority=priority,
                kind="individual",
                alerts=[alert],
                sentence_limit=sentence_limit,
                user_id=uid,
            )
            jobs.append((job_id, job))

        if self._tg is not None:
            for jid, job in jobs:
                self._tg.start_soon(self._queue.put, (priority, job))
                job_ids.append(jid)
        else:
            # If not started yet, this becomes a no-op; return ids for tracking
            for jid, job in jobs:
                job_ids.append(jid)
        return job_ids

    # Internals -------------------------------------------------------------
    def _get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = AsyncClient(
                host=self.config.base_url, timeout=self.config.timeout
            )
        return self._client

    def _get_user_limiter(self, user_id: str) -> CapacityLimiter:
        lim = self._user_limiters.get(user_id)
        if lim is None:
            lim = CapacityLimiter(self._per_user_limit)
            self._user_limiters[user_id] = lim
        return lim

    async def _worker_loop(self) -> None:
        try:
            while self._running:
                priority, job = await self._queue.get()
                try:
                    if job.kind == "group":
                        style = "concise" if len(job.alerts) == 1 else "comprehensive"
                        text = await self._generate_with_limits(
                            job.alerts, style, job.user_id, None, job.sentence_limit
                        )
                        await self._store_group_summary(job.alerts, text, job.config)
                    else:
                        text = await self._generate_with_limits(
                            job.alerts, "concise", job.user_id, None, job.sentence_limit
                        )
                        await self._store_individual_summary(
                            job.alerts[0], text, job.sentence_limit
                        )
                except Exception as e:
                    logger.error(f"AI job {job.id} failed: {e}")
        except Exception:
            # Let TaskGroup cancellation propagate cleanly
            pass

    async def _generate_with_limits(
        self,
        alerts: List[AlertResource],
        style: str,
        user_id: str,
        request: Optional[SummarizationRequest] = None,
        sentence_limit: int = 2,
    ) -> str:
        if not alerts:
            return "No alerts to summarize."
        prompt = self._build_prompt(
            alerts,
            style=style,
            sentence_limit=sentence_limit,
            include_route_info=(request.include_route_info if request else True),
            include_severity=(request.include_severity if request else True),
        )

        limiter = self._get_user_limiter(user_id)
        async with limiter:
            client = self._get_client()
            resp = await client.chat(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": self.config.temperature, "num_ctx": 4096},
            )
            content = getattr(getattr(resp, "message", None), "content", None)
            if not content:
                content = getattr(resp, "content", None) or str(resp)
            return content.strip()

    def _build_prompt(
        self,
        alerts: List[AlertResource],
        *,
        style: str = "comprehensive",
        sentence_limit: int = 2,
        include_route_info: bool = True,
        include_severity: bool = True,
    ) -> str:
        parts: List[str] = []
        parts.append("You summarize MBTA transit alerts clearly and concisely.")
        parts.append("Use professional tone. No chit‑chat. No prefaces.")

        if style == "concise":
            parts.append(
                f"Summarize each alert in at most {max(1, sentence_limit)} sentence(s)."
            )
        else:
            parts.append("Provide a brief overview then bullet summaries for each alert.")

        parts.append("")
        parts.append("ALERTS:")
        for a in alerts:
            attrs = a.attributes
            seg = [f"- {attrs.header}"]
            if include_route_info and getattr(attrs, "informed_entity", None):
                seg.append("  routes: present")
            if getattr(attrs, "effect", None):
                seg.append(f"  effect: {self._humanize(attrs.effect)}")
            if include_severity and getattr(attrs, "severity", None) is not None:
                seg.append(f"  severity: {self._severity_to_text(int(attrs.severity))}")
            if getattr(attrs, "timeframe", None):
                seg.append(f"  timeframe: {attrs.timeframe}")
            if getattr(attrs, "cause", None):
                seg.append(f"  cause: {self._humanize(attrs.cause)}")
            parts.append("\n".join(seg))

        parts.append("")
        parts.append("Output plain text only.")
        return "\n".join(parts)

    def _humanize(self, text: str) -> str:
        return text.replace("_", " ").title() if isinstance(text, str) else str(text)

    def _severity_to_text(self, severity: int) -> str:
        mapping = {
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
        return mapping.get(severity, f"Level {severity}")

    def _alerts_hash(self, alerts: List[AlertResource]) -> str:
        base = "|".join(
            f"{a.id}:{getattr(a.attributes,'header','')}:{getattr(a.attributes,'severity','')}:{getattr(a.attributes,'effect','')}"
            for a in sorted(alerts, key=lambda x: x.id)
        )
        return hashlib.sha256(base.encode()).hexdigest()[:16]

    async def _store_group_summary(
        self, alerts: List[AlertResource], summary: str, config: Dict[str, Any]
    ) -> None:
        try:
            alerts_hash = self._alerts_hash(alerts)
            key = f"ai_summary:{alerts_hash}"
            ttl = max(60, int(self.config.cache_ttl))
            payload = SummaryCacheEntry(
                summary=summary,
                alert_count=len(alerts),
                alerts_hash=alerts_hash,
                generated_at=datetime.now(),
                config=config,
                ttl=ttl,
            ).model_dump_json()
            await self.redis.setex(key, ttl, payload)
            await self.redis.setex(f"alerts_to_summary:{alerts_hash}", ttl, key)
        except Exception as e:
            logger.warning(f"Failed to store group summary: {e}")

    async def _store_individual_summary(
        self, alert: AlertResource, summary: str, sentence_limit: int
    ) -> None:
        try:
            key = f"ai_summary_individual:{alert.id}"
            ttl = max(60, int(self.config.cache_ttl))
            payload = IndividualSummaryCacheEntry(
                alert_id=alert.id,
                summary=summary,
                style="concise",
                sentence_limit=sentence_limit,
                generated_at=datetime.now(),
                format="text",
                ttl=ttl,
            ).model_dump_json()
            await self.redis.setex(key, ttl, payload)
        except Exception as e:
            logger.warning(f"Failed to store individual summary: {e}")
