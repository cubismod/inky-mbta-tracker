import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Self, List

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
from redis_cache import check_cache, write_cache
from .config import OllamaConfig
import hashlib
from .consts import HOUR
import logging
import re

logger = logging.getLogger(__name__)


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

    def _alert_hash(self, alert: AlertResource) -> str:
        base = f"{alert.id}:{getattr(alert.attributes, 'header', '')}:{getattr(alert.attributes, 'severity', '')}:{getattr(alert.attributes, 'effect', '')}"

        return hashlib.sha256(base.encode()).hexdigest()[:16]

    def _clean_model_response(
        self,
        summary: str,
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
                        logger.debug(
                            f"Removing conversational phrase: '{match.group(0)}'"
                        )
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
                            cleaned_summary = re.sub(
                                r"\n\s*\n\s*\n+", "\n\n", cleaned_summary
                            )

                            # Remove multiple consecutive spaces
                            cleaned_summary = re.sub(r"\s+", " ", cleaned_summary)

                            # Remove any leading/trailing whitespace
                            cleaned_summary = cleaned_summary.strip()

                            # Log what was cleaned up
                            if len(cleaned_summary) != len(original_summary):
                                removed_chars = len(original_summary) - len(
                                    cleaned_summary
                                )
                                logger.info(
                                    f"Cleaned model response: removed {removed_chars} characters of conversational phrases and artifacts"
                                )
                                logger.debug(f"Original: {original_summary[:100]}...")
                                logger.debug(f"Cleaned:  {cleaned_summary[:100]}...")

                            logger.debug(
                                f"Cleaning model response (cleaned length: {len(cleaned_summary)})"
                            )

                            return cleaned_summary
        return summary

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

    async def summarize_alert(
        self,
        alert: AlertResource,
        lock: Lock,
        send_stream: MemoryObjectSendStream[str],
        sentence_limit: int = 2,
    ) -> None:
        key = self._alert_hash(alert)
        cached = await check_cache(self.r_client, key)
        if cached:
            await send_stream.send(cached)
            return
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
                resp_text = await response.text()
                cleaned_text = self._clean_model_response(resp_text)
                await write_cache(self.r_client, key, cleaned_text, 1 * HOUR)
