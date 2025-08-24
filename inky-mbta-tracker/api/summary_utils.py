import json
from datetime import datetime
from typing import Any, Dict, List, Tuple

import textdistance
from config import Config
from mbta_responses import AlertResource

from .core import logger
from .models import SummaryFormat


def format_summary(summary: str, format_type: SummaryFormat) -> str:
    """Format a summary according to the requested format type."""
    if format_type == SummaryFormat.TEXT:
        return summary
    elif format_type == SummaryFormat.MARKDOWN:
        lines = summary.split("\n")
        formatted_lines: List[str] = []

        for line in lines:
            line = line.strip()
            if not line:
                continue
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
    elif format_type == SummaryFormat.JSON:
        try:
            json.loads(summary)
            return summary
        except (ValueError, TypeError):
            lines = summary.split("\n")
            structured_data: Dict[str, Any] = {
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


def validate_ai_summary(
    summary: str, alerts: List[AlertResource], min_similarity: float = 0.105
) -> Tuple[bool, float, str]:
    try:
        if not alerts:
            return False, 0.0, "No alerts provided for validation"
        if not summary or len(summary.strip()) < 10:
            return False, 0.0, "Summary too short or empty"

        alert_texts: List[str] = []
        for alert in alerts:
            alert_text = f"{alert.attributes.header} "
            if alert.attributes.effect:
                alert_text += f"{alert.attributes.effect} "
            if alert.attributes.cause:
                alert_text += f"{alert.attributes.cause} "
            if alert.attributes.severity:
                alert_text += f"{alert.attributes.severity} "
            alert_texts.append(alert_text.strip())

        input_text = " ".join(alert_texts)
        similarity = textdistance.levenshtein.normalized_similarity(
            input_text.lower(), summary.lower()
        )

        is_valid = similarity >= min_similarity
        reason = (
            f"Similarity {similarity:.3f} above threshold {min_similarity}"
            if is_valid
            else f"Similarity {similarity:.3f} below threshold {min_similarity}"
        )

        logger.debug(
            f"Summary validation - similarity: {similarity:.3f}, valid: {is_valid}, reason: {reason}"
        )

        return is_valid, similarity, reason
    except Exception as e:
        logger.error(f"Error validating AI summary: {e}", exc_info=True)
        return False, 0.0, f"Validation error: {str(e)}"


async def save_summary_to_file(
    summary: str, alerts: List[AlertResource], model_info: str, config: Config
) -> None:
    logger.debug(
        f"save_summary_to_file called - enabled: {config.file_output.enabled}, alerts: {len(alerts) if alerts else 0}"
    )
    if not config.file_output.enabled:
        logger.debug("File output is disabled, skipping file save")
        return

    if config.file_output.validate_summaries:
        is_valid, similarity, reason = validate_ai_summary(
            summary, alerts, config.file_output.min_similarity_threshold
        )
        if not is_valid:
            logger.warning(f"Summary validation failed: {reason}")
            logger.warning(
                f"Summary rejected - similarity {similarity:.3f} below threshold {config.file_output.min_similarity_threshold}"
            )
            return
        else:
            logger.info(f"Summary validation passed - similarity {similarity:.3f}")
    else:
        logger.debug("Summary validation disabled, proceeding with file save")

    try:
        import pathlib

        output_dir = pathlib.Path(config.file_output.output_directory)
        logger.debug(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / config.file_output.filename
        logger.debug(f"Will save to file: {file_path}")

        if not output_dir.exists() or not output_dir.is_dir():
            logger.error(f"Invalid output directory: {output_dir}")
            return

        try:
            test_file = output_dir / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
            logger.debug("Write permissions verified for output directory")
        except Exception as e:
            logger.error(f"Write permission test failed: {e}")
            return

        markdown_content: List[str] = []
        markdown_content.append("# MBTA Alerts Summary")
        markdown_content.append("")

        if config.file_output.include_timestamp:
            markdown_content.append(
                f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            markdown_content.append("")

        if config.file_output.include_alert_count:
            markdown_content.append(f"**Active Alerts:** {len(alerts)}")
            markdown_content.append("")

        if config.file_output.include_model_info:
            markdown_content.append(f"**AI Model:** {model_info}")
            markdown_content.append("")

        if config.file_output.validate_summaries:
            is_valid, similarity, _ = validate_ai_summary(
                summary, alerts, config.file_output.min_similarity_threshold
            )
            markdown_content.append(
                f"**Validation:** PASSED (similarity: {similarity:.3f})"
            )
            markdown_content.append("")

        markdown_content.append("## Summary")
        markdown_content.append("")
        summary_lines = summary.split("\n")
        for line in summary_lines:
            line = line.strip()
            if not line:
                continue
            if any(
                line.lower().startswith(prefix)
                for prefix in [
                    "alert",
                    "route",
                    "effect",
                    "cause",
                    "severity",
                    "timeframe",
                    "overview",
                    "summary",
                ]
            ):
                markdown_content.append(f"### {line}")
            elif line.endswith(":") and len(line) < 50:
                markdown_content.append(f"**{line}**")
            else:
                markdown_content.append(line)

        markdown_content.append("")

        if alerts:
            markdown_content.append("## Alert Details")
            markdown_content.append("")
            for i, alert in enumerate(alerts, 1):
                markdown_content.append(f"### Alert {i}: {alert.attributes.header}")
                markdown_content.append("")
                if (
                    alert.attributes.short_header
                    and alert.attributes.short_header != alert.attributes.header
                ):
                    markdown_content.append(
                        f"**Short Header:** {alert.attributes.short_header}"
                    )
                    markdown_content.append("")
                if alert.attributes.effect:
                    markdown_content.append(f"**Effect:** {alert.attributes.effect}")
                if alert.attributes.severity:
                    markdown_content.append(
                        f"**Severity:** {alert.attributes.severity}"
                    )
                if alert.attributes.cause:
                    markdown_content.append(f"**Cause:** {alert.attributes.cause}")
                if alert.attributes.timeframe:
                    markdown_content.append(
                        f"**Timeframe:** {alert.attributes.timeframe}"
                    )
                markdown_content.append("")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(markdown_content))

        logger.info(f"AI summary successfully saved to {file_path}")

    except Exception as e:
        logger.error(f"Failed to save summary to file: {e}", exc_info=True)
