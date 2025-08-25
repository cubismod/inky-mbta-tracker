import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Self

import llm
from anyio import (
    AsyncContextManagerMixin,
    Lock,
    create_memory_object_stream,
    create_task_group,
    open_file,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from llm.models import AsyncModel
from mbta_responses import AlertResource
from redis.asyncio import Redis

from .config import OllamaConfig


class Ollama(AsyncContextManagerMixin):
    def __init__(self, config: OllamaConfig, r_client: Redis):
        self.config = config
        self.model: Optional[AsyncModel] = None
        self._tg: Optional[TaskGroup] = None
        self._receive_stream: Optional[MemoryObjectReceiveStream[str]] = None
        self._send_stream: Optional[MemoryObjectSendStream[str]] = None
        self.r_client = r_client

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:
        self.model = llm.get_async_model(os.getenv("OLLAMA_MODEL", "llama2"))
        async with create_task_group() as tg:
            self._tg = tg
            self._send_stream, self._receive_stream = create_memory_object_stream[str]()
            try:
                yield self
            finally:
                self._tg = None

    async def queue_summary(
        self,
        alert: AlertResource,
        lock: Lock,
        send_stream: MemoryObjectSendStream[str],
        sentence_limit: int = 2,
    ):
        if self._tg:
            self._tg.start_soon(
                self.summarize_alert, alert, lock, send_stream, sentence_limit
            )

    def _format_alert(self, alert: AlertResource) -> str:
        return f"Alert ID: {alert.id}\nDescription: {alert.attributes.header}\nSeverity: {self._severity_to_text(alert.attributes.severity)}\nTimeframe: {alert.attributes.timeframe}\nCreated At: {alert.attributes.created_at}\nUpdated At: {alert.attributes.updated_at}\nShort Description: {alert.attributes.short_header}"

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

    async def summarize_alert(
        self,
        alert: AlertResource,
        lock: Lock,
        send_stream: MemoryObjectSendStream[str],
        sentence_limit: int = 2,
    ):
        if self.model:
            async with lock:
                async with await open_file(
                    "../fragments/agent-prompt.txt"
                ) as prompt_file:
                    agent_prompt = await prompt_file.read()
                response = await self.model.prompt(
                    fragments=[agent_prompt],
                    prompt=f"Summarize this alert in {sentence_limit} sentences.\n{self._format_alert(alert)}",
                )
