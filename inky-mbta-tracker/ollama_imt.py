import hashlib
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional, Self
from zoneinfo import ZoneInfo

import llm
from anyio import (
    AsyncContextManagerMixin,
    Path,
    create_memory_object_stream,
    create_task_group,
    open_file,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from consts import HOUR, MINUTE
from llm.models import AsyncModel
from mbta_responses import AlertResource
from redis.asyncio import Redis
from redis_cache import check_cache, write_cache
from redis_lock.asyncio.async_lock import RedisLock

logger = logging.getLogger(__name__)


def strip_think_sections(text: str) -> str:
    """Remove any `<think>...</think>` sections from model output.

    Handles multiple occurrences and preserves surrounding text. Case-insensitive
    and dotall to match across newlines.

    Args:
        text: The original model output.

    Returns:
        The text with all think sections removed.
    """
    if not text:
        return text

    # Remove all <think>...</think> blocks (non-greedy, across newlines)
    cleaned = re.sub(
        r"<\s*think\s*>.*?<\s*/\s*think\s*>", "", text, flags=re.IGNORECASE | re.DOTALL
    )

    # Also handle stray opening or closing tags without pairs
    cleaned = re.sub(r"<\s*/?\s*think\s*>", "", cleaned, flags=re.IGNORECASE)

    # Normalize whitespace left behind
    return re.sub(r"\n\s*\n\s*\n+", "\n\n", cleaned).strip()


def _normalize_us_times(text: str) -> str:
    """Normalize time-like strings to US 12-hour format such as "9:46 AM".

    - Converts 24-hour times like "21:05" to "9:05 PM".
    - Ensures a space before AM/PM and uppercases it (e.g., "9:05pm" -> "9:05 PM").
    - Removes any leading zero from the hour (e.g., "09:05 AM" -> "9:05 AM").

    Only affects tokens matching HH:MM with HH in 0–23 and MM in 00–59,
    optionally followed by AM/PM (any case, with or without space).
    """
    if not text:
        return text

    time_re = re.compile(r"\b(\d{1,2}):([0-5]\d)\s*([AaPp][Mm])?\b")

    def repl(match: re.Match[str]) -> str:
        hour_str, minute, ampm = match.group(1), match.group(2), match.group(3)
        try:
            hour = int(hour_str)
        except ValueError:
            return match.group(0)

        if hour < 0 or hour > 23:
            return match.group(0)

        if ampm:
            ampm = ampm.upper()
            if hour == 0:
                hour12 = 12
                ampm = "AM"
            elif 1 <= hour <= 11:
                hour12 = hour
            elif hour == 12:
                hour12 = 12
                ampm = "PM"
            else:
                hour12 = hour - 12
                ampm = "PM"
        else:
            if hour == 0:
                hour12 = 12
                ampm = "AM"
            elif 1 <= hour <= 11:
                hour12 = hour
                ampm = "AM"
            elif hour == 12:
                hour12 = 12
                ampm = "PM"
            else:
                hour12 = hour - 12
                ampm = "PM"

        return f"{hour12}:{minute} {ampm}"

    return time_re.sub(repl, text)


def strip_english_reasoning_sections(text: str) -> str:
    """Remove common English-only "reasoning" patterns from model output.

    This targets XML-like tags, fenced code blocks, and heading-style markers
    that models sometimes use to wrap chain-of-thought. It intentionally avoids
    multilingual terms to reduce false positives and keeps only content after a
    clear final-answer marker when present.

    Args:
        text: The original model output.

    Returns:
        Text with English reasoning sections removed.
    """
    if not text:
        return text

    cleaned = text

    # 1) XML-ish tags frequently used for reasoning (English only)
    reasoning_tags = {
        "analysis",
        "reasoning",
        "reflection",
        "thoughts",
        "cot",
        "scratchpad",
        "notes",
        "assistant_think",
    }
    tag_re = "|".join(map(re.escape, reasoning_tags))

    # Remove closed sections like <analysis> ... </analysis>
    cleaned = re.sub(
        rf"<\s*(?:{tag_re})\s*>.*?<\s*/\s*(?:{tag_re})\s*>",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Remove unmatched opening tag to end of text
    cleaned = re.sub(
        rf"<\s*(?:{tag_re})\s*>.*\Z",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # 2) Fenced code blocks that advertise reasoning content
    fence_labels = r"(?:thinking|thoughts|analysis|reasoning|cot|scratchpad)"
    cleaned = re.sub(
        rf"```{fence_labels}\s*.*?```",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Generic fenced blocks that contain a reasoning tag name
    cleaned = re.sub(
        rf"```.*?(?:{tag_re}).*?```",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # 3) Heading/marker styles like "Reasoning:", "Thoughts:", "Analysis:"
    cleaned = re.sub(
        r"^\s*(?:\[|\(|\{)?\s*(?:thoughts?|reasoning|analysis|cot|scratchpad)\s*(?:\]|\)|\})?\s*[:：-]\s*.*?(?=\n\S|\Z)",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )

    # 4) Common prefatory phrases in English
    cleaned = re.sub(
        r"^\s*(?:let'?s\s+think(?:\s+step\s+by\s+step)?|here'?s\s+my\s+reasoning)\b.*?(?=\n|\Z)",
        "",
        cleaned,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # 5) BEGIN/END marker blocks
    cleaned = re.sub(
        r"^\s*BEGIN\s+(?:THINKING|THOUGHTS|ANALYSIS|REASONING|COT)\s*.*?^\s*END\s+(?:THINKING|THOUGHTS|ANALYSIS|REASONING|COT)\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )

    # 6) Keep only content after explicit final markers when present
    final_markers = [
        r"\bfinal\s+answer\b",
        r"\bfinal\s*:",
        r"\bsummary\s*:",
        r"\banswer\s*:",
        r"\boutput\s*:",
    ]
    m = re.search("|".join(final_markers), cleaned, flags=re.IGNORECASE)
    if m:
        cleaned = cleaned[m.end() :].strip()

    # 7) Normalize whitespace and remove any stray closing tags left
    cleaned = re.sub(r"\n\s*\n\s*\n+", "\n\n", cleaned)
    cleaned = re.sub(rf"<\s*/?\s*(?:{tag_re})\s*>", "", cleaned, flags=re.IGNORECASE)

    return cleaned.strip()


def _humanize_effect_codes(text: str) -> str:
    """Convert enum-like codes (e.g., SERVICE_CHANGE) to friendlier text.

    Rewrites any ALL_CAPS_WITH_UNDERSCORES token to lower case with spaces,
    such as "SERVICE_CHANGE" -> "service change". This targets model
    responses that echo enum values from the prompt/schema.
    """
    if not text:
        return text

    pattern = re.compile(r"\b([A-Z]+(?:_[A-Z]+)+)\b")
    return pattern.sub(lambda m: m.group(1).replace("_", " ").lower(), text)


def _remove_cr_prefixes(text: str) -> str:
    """Remove "CR-" prefixes from line names for cleaner display.

    Example: "CR-Franklin/Foxboro" -> "Franklin/Foxboro".
    Only targets the literal "CR-" sequence at word boundaries.
    """
    if not text:
        return text
    return re.sub(r"\bCR-", "", text)


def _remove_duplicate_words(text: str) -> str:
    """Collapse consecutive duplicate words while preserving case and spacing.

    Example: "the the Green Line delay" -> "the Green Line delay".
    Matching is case-insensitive and considers word boundaries; only
    consecutive duplicates are removed to avoid changing meaning.
    """
    if not text:
        return text

    # Replace runs of the same word with a single instance (case-insensitive)
    pattern = re.compile(r"(\b[\w]+\b)(?:\s+\1\b)+", flags=re.IGNORECASE)

    prev = None
    cur = text
    # Iterate until no further changes to handle triples like "the the the"
    while prev != cur:
        prev = cur
        cur = pattern.sub(r"\1", cur)
    return cur


class OllamaClientIMT(AsyncContextManagerMixin):
    """Lightweight async helper for summarizing MBTA alerts.

    Wraps an async `llm` model, provides a small background task mechanism,
    and caches per-alert summaries in Redis.
    """

    def __init__(self, r_client: Redis):
        """Initialize the helper.

        Args:
            r_client: Redis client used for read/write caching of summaries.
        """
        self.model: Optional[AsyncModel] = None
        self._tg: Optional[TaskGroup] = None
        self._receive_stream: Optional[MemoryObjectReceiveStream[str]] = None
        self._send_stream: Optional[MemoryObjectSendStream[str]] = None
        self.r_client = r_client

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:
        """Set up the async model and a task group for background work.

        Yields:
            Self: Instance with an initialized model and task group.
        """
        self.model = llm.get_async_model(os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b"))
        async with create_task_group() as tg:
            self._tg = tg
            self._send_stream, self._receive_stream = create_memory_object_stream[str]()
            try:
                yield self
            finally:
                self._tg = None

    async def fetch_cached_summary(
        self,
        alert: AlertResource,
        send_stream: Optional[MemoryObjectSendStream[str]] = None,
        sentence_limit: int = 1,
    ) -> Optional[str]:
        key = self._alert_hash(alert)
        cached = await check_cache(self.r_client, key)
        if cached:
            return cached
        else:
            await self.queue_summary(alert, send_stream, sentence_limit)

    async def queue_summary(
        self,
        alert: AlertResource,
        send_stream: Optional[MemoryObjectSendStream[str]],
        sentence_limit: int = 2,
    ):
        """Queue an alert summarization task if a TaskGroup is active.

        Args:
            alert: The MBTA alert to summarize.
            lock: Shared lock to serialize model requests.
            send_stream: Stream to send the final summary text to.
            sentence_limit: Maximum sentences to request from the model.

        Returns:
            None
        """
        key = self._alert_hash(alert)
        cached = await check_cache(self.r_client, key)
        if cached and send_stream:
            await send_stream.send(cached)
        if self._tg:
            if send_stream:
                # send a message to indicate the ID is queued to get a summary
                await send_stream.send(f"{alert.id}")
            self._tg.start_soon(
                self.summarize_alert, alert, send_stream, sentence_limit
            )

    def _format_alert(self, alert: AlertResource) -> str:
        """Format an alert into plain text for prompting the model.

        Args:
            alert: The alert to format.

        Returns:
            Plain text block with the alert's key fields.
        """
        return alert.model_dump_json()

    def _severity_to_text(self, severity: int) -> str:
        """Convert a numeric severity into a human-readable label.

        Args:
            severity: Integer severity level from the alert.

        Returns:
            Human-friendly severity label.
        """
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
        """Build a short, stable cache key for an alert.

        Args:
            alert: The alert to derive the cache key from.

        Returns:
            Hex digest prefix that changes when key fields change.
        """
        base = f"{alert.id}:{getattr(alert.attributes, 'header', '')}:{getattr(alert.attributes, 'severity', '')}:{getattr(alert.attributes, 'effect', '')}"

        return f"ai_summary:{hashlib.sha256(base.encode()).hexdigest()[:16]}"

    def _clean_model_response(
        self,
        summary: str,
    ) -> str:
        """Remove conversational prefaces and artifacts from the model output.

        Args:
            summary: The AI-generated text to clean.

        Returns:
            Cleaned summary text, or the original text if unchanged.
        """
        if not summary or len(summary.strip()) == 0:
            return summary

        # First, strip any <think>...</think> sections that some models include
        summary = strip_think_sections(summary)

        # Then, strip broader English-only reasoning wrappers/markers
        summary = strip_english_reasoning_sections(summary)

        # Normalize time strings and humanize enum-like codes
        summary = _normalize_us_times(summary)
        summary = _humanize_effect_codes(summary)
        summary = _remove_cr_prefixes(summary)
        summary = _remove_duplicate_words(summary)

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

                            return _remove_duplicate_words(
                                _remove_cr_prefixes(
                                    _humanize_effect_codes(
                                        _normalize_us_times(cleaned_summary)
                                    )
                                )
                            )
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
        send_stream: Optional[MemoryObjectSendStream[str]] = None,
        sentence_limit: int = 2,
    ) -> None:
        """Summarize one alert, using Redis for caching.

        Args:
            alert: The MBTA alert to summarize.
            send_stream: Stream to send the result to.
            sentence_limit: Maximum number of sentences to request.

        Returns:
            None
        """
        if self.model:
            key = self._alert_hash(alert)
            async with RedisLock(
                self.r_client,
                "ollama_lock",
                blocking_timeout=10 * MINUTE,
                expire_timeout=8 * MINUTE,
            ):
                # this call can wait around for a while and the event may have already been cached in the waiting time
                # so we try to return a cached value if available once we have the lock
                cached = await check_cache(self.r_client, key)
                if cached and send_stream:
                    await send_stream.send(cached)
                async with await open_file(
                    Path("./fragments/agent-prompt.txt")
                ) as prompt_file:
                    start = datetime.now()
                    agent_prompt = await prompt_file.read()
                    prompt_str = f"Summarize this alert in {sentence_limit} sentences.\nThe current time is {datetime.now().astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}\n{self._format_alert(alert)}"
                    response = await self.model.prompt(
                        fragments=[agent_prompt], prompt=prompt_str
                    )
                    resp_text = await response.text()
                    cleaned_text = self._clean_model_response(resp_text)
                    await write_cache(self.r_client, key, cleaned_text, 1 * HOUR)
                    if send_stream:
                        await send_stream.send(cleaned_text)
                    end = datetime.now()

                    logger.info(
                        f"Ollama generated a response in {(end - start).seconds}s"
                    )
                    logger.info(f"Response:\n{cleaned_text}")
