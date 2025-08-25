import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from anyio import create_task_group, AsyncContextManagerMixin, Lock
from anyio.streams.memory import MemoryObjectSendStream
from anyio.abc import TaskGroup
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self
from config import OllamaConfig
from mbta_responses import AlertResource
from ollama import AsyncClient
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from shared_types.shared_types import (
    IndividualSummaryCacheEntry,
    SummaryCacheEntry,
)
from utils import get_redis

logger = logging.getLogger(__name__)


class SummarizationRequest(BaseModel):
    """Request model retained for API compatibility."""

    alerts: List[AlertResource]
    include_route_info: bool = Field(default=True)
    include_severity: bool = Field(default=True)
    format: str = Field(default="text")
    style: str = Field(default="comprehensive")


@dataclass
class _Job:
    id: str
    priority: int  # lower number = higher priority
    kind: str  # "group" | "individual"
    alerts: List[AlertResource]
    config: Dict[str, Any] = field(default_factory=dict)
    sentence_limit: int = 2
    user_id: str = "anon"


class AISummarizer(AsyncContextManagerMixin):
    """Ollama summarizer"""

    def __init__(self, config: OllamaConfig, redis: Redis) -> None:
        self.config = config
        self._client: Optional[AsyncClient] = None
        self._tg: Optional[TaskGroup] = None
        self.redis = redis

    # Lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        # Use async context manager entry for unified lifecycle
        await self.__aenter__()
        logger.info("AI summarizer worker started")

    async def stop(self) -> None:
        await self.__aexit__(None, None, None)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:
        async with create_task_group() as tg:
            self._running = True
            self._tg = tg
            try:
                yield self
            finally:
                self._tg = None

    # Public API ------------------------------------------------------------
    async def summarize_alerts(
        self, tg: TaskGroup, request: SummarizationRequest, lock: Lock, send_stream: MemoryObjectSendStream[str]
    ) -> None:
        """Summarizes a set of alerts into a single response asynchronously, returning the result via the memory object stream.
           Closes send_stream on completion or error"""
        tg.start_soon(self._generate_response,request.alerts, request.style, lock, send_stream)

    async def summarize_individual_alert(
        self,
        tg: TaskGroup,
        alert: AlertResource,
        lock: Lock,
        send_stream: MemoryObjectSendStream[str],
        style: str = "concise",
        sentence_limit: int = 2,
    ) -> None:
        """Summarizes an individual alert into a single response asynchronously, returning the result via the memory object stream.
           Closes send_stream on completion or error"""
        tg.start_soon(self._generate_response,[alert], style, lock, send_stream, None, sentence_limit)


    async def health_check(self) -> bool:
        try:
            client = self._get_client()
            await client.list()
            return True
        except Exception as e:
            logger.warning(f"Ollama health check failed: {e}")
            return False


    # Internals -------------------------------------------------------------
    def _get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = AsyncClient(
                host=self.config.base_url, timeout=self.config.timeout
            )
        return self._client

    async def _generate_response(
        self,
        alerts: List[AlertResource],
        style: str,
        lock: Lock,
        send_stream: MemoryObjectSendStream[str],
        request: Optional[SummarizationRequest] = None,
        sentence_limit: int = 2,
    ) -> None:
        """Generates a summary of the given alerts. Returns the summary asynchronously with a memory object stream."""
        if not alerts:
            send_stream.close()
        prompt = self._build_prompt(
            alerts,
            style=style,
            sentence_limit=sentence_limit,
            include_route_info=(request.include_route_info if request else True),
            include_severity=(request.include_severity if request else True),
        )
        async with lock:
            client = self._get_client()
            resp = await client.chat(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": self.config.temperature, "num_ctx": 4096},
            )
        content = getattr(getattr(resp, "message", None), "content", None)
        if not content:
            content = getattr(resp, "content", None) or str(resp)
        await send_stream.send(content)
        send_stream.close()

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
            if include_route_info and attrs.informed_entity:
                seg.append("  routes: present")
            if attrs.effect:
                seg.append(f"  effect: {self._humanize(attrs.effect)}")
            if include_severity and attrs.severity:
                seg.append(f"  severity: {self._severity_to_text(int(attrs.severity))}")
            if attrs.timeframe:
                seg.append(f"  timeframe: {attrs.timeframe}")
            if attrs.cause:
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

    def _format_alerts_for_prompt() -> str:
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
                parts.append(
                    f"   Effect: {self._human_readable_effect_or_cause(attrs.effect)}"
                )

            if include_severity:
                severity_text = self._severity_to_text(attrs.severity)
                parts.append(f"   Severity: {severity_text}")

            if attrs.timeframe:
                parts.append(f"   Timeframe: {attrs.timeframe}")

            if attrs.cause and attrs.cause != "UNKNOWN_CAUSE":
                parts.append(
                    f"   Cause: {self._human_readable_effect_or_cause(attrs.cause)}"
                )

            prompt_parts.append("\n".join(parts))

        prompt_parts.append(
            "\nPlease provide a comprehensive, structured summary that covers each alert individually with detailed analysis."
            "\nDo not directly respond to this prompt with 'Okay', 'Got it', 'I understand', or anything else."
            "\nConvert IDs like 'UNKNOWN_CAUSE' to 'Unknown Cause'."
            "\nUse proper grammar and punctuation."
            "\n" + self._time_prompt() + "\n"
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
            "\n\nMake sure to cover ALL alerts completely with full details. Do not stop until you have provided a comprehensive summary.")
        return "\n\n".join(prompt_parts)

    def _human_readable_effect_or_cause(self, cause: str) -> str:
        """
        Convert a cause to a human readable string.
        """
        return cause.lower().replace("_", " ").capitalize()

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

    async def _ollama_pull_model(self, model: str) -> None:
        """
        Pull a model from Ollama.
        """
        client = self._get_client()
        async for response in await client.pull(model, stream=True):
            logger.info(f"Pulling model {model}{response.digest}: {response.status} {response.total}")

    def _create_questioning_prompt(
        self, rewritten_content: str, summary: str, api_summary: str
    ) -> list[str]:
        """
        Create a prompt for questioning the rewritten content.
        """
        return [
            "Review the following rewritten summary compared to the headers returned by the api and provide a single answer for which summary to return: rewritten or api.",
            "Focus on picking the most accurate answer. Hallucinations are not allowed.",
            "The original API headers should be considered the source of truth. If information is missing from the summarized versions then the original API headers should be returned.",
            f"Rewritten Summary:\n{rewritten_content}\n\n",
            f"API Headers:\n{api_summary}\n\n",
        ]

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

            # Call Ollama for the rewrite (with reasoning support if enabled)
            if self.config.enable_reasoning:
                rewritten_content = await self._chat_with_reasoning(
                    rewrite_prompt, original_alerts
                )
            else:
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
                        "think": False,
                    },
                )

                # Extract the rewritten content
                if hasattr(response, "message") and hasattr(
                    response.message, "content"
                ):
                    rewritten_content = response.message.content or ""
                elif hasattr(response, "content"):
                    rewritten_content = response.content or ""
                else:
                    rewritten_content = str(response)

            # another step selects whether the rewritten summary is better than the original
            if original_alerts:
                try:
                    original_alerts_headers = str.join(
                        ",", [f"{alert.attributes.header}" for alert in original_alerts]
                    )

                    questioning_prompt = str.join(
                        "\n",
                        self._create_questioning_prompt(
                            rewritten_content,
                            summary,
                            original_alerts_headers,
                        ),
                    )
                    logger.debug(f"Calling Ollama with model: {self.config.model}")
                    logger.debug(f"Prompt length: {len(questioning_prompt)} characters")
                    logger.debug(
                        f"Model config: temperature={self.config.temperature}, timeout={self.config.timeout}"
                    )
                    logger.debug(f"Full prompt:\n{questioning_prompt}")
                    client = self._get_client()
                    response = await client.chat(
                        model=self.config.model,
                        messages=[{"role": "user", "content": questioning_prompt}],
                        options={
                            "temperature": 0.2,
                            "top_p": 0.9,
                            "top_k": 40,
                            "num_predict": 1500,
                            "stop": ["###", "---"],
                            "repeat_penalty": 1.1,
                            "num_ctx": 4096,
                            "think": False,
                        },
                    )
                    logger.debug(f"Response: {response.message.content}")
                    if (
                        "api" in response.message.content.lower()
                        if response.message.content
                        else ""
                    ):
                        return original_alerts_headers
                    if (
                        "rewritten" in response.message.content.lower()
                        if response.message.content
                        else ""
                    ):
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
                            sentence_diff
                            <= tolerance  # Allow configurable sentence difference
                            and cleaned_rewrite != summary
                            and len(cleaned_rewrite.strip())
                            > 10  # Ensure minimum content
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
                                logger.debug(
                                    "Rewrite rejected - content identical to original"
                                )
                            else:
                                logger.debug(
                                    "Rewrite rejected - insufficient content length"
                                )
                            return summary
                except Exception as e:
                    logger.error(
                        f"Error creating questioning prompt: {e}", exc_info=True
                    )
            else:
                logger.warning("Failed to get rewritten content, keeping original")
                return summary

        except Exception as e:
            logger.error(f"Error during summary rewriting: {e}", exc_info=True)

        return summary

    def _create_default_prompt(self) -> str:
        return str.join(
            "\n",
            [
                "You are a helpful assistant who is in charge of summarizing MBTA transit alerts and managing rewrites.",
                "You strive for accuracy, clarity, and professionalism in your summaries.",
                "Your responses do not include thinking or reasoning processes and are concise.",
            ]
        )

    def _create_rewrite_prompt(self,
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
            self._create_default_prompt(),
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
    )
