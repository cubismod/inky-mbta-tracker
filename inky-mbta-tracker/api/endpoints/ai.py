import json
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
from consts import MBTA_V3_ENDPOINT
from fastapi import APIRouter, HTTPException, Query, Request, Response
from mbta_responses import AlertResource, Alerts

from ..core import AI_SUMMARIZER, CONFIG, REDIS_CLIENT
from ..limits import limiter
from ..models import SummaryFormat
from ..services.alerts import fetch_alerts_with_retry
from ..summary_utils import format_summary, save_summary_to_file, validate_ai_summary

router = APIRouter()


@router.get(
    "/ai/test",
    summary="Test AI Summarization",
    description="Test endpoint to manually trigger AI summarization for debugging",
)
@limiter.limit("10/minute")
async def test_ai_summarization(
    request: Request, format: SummaryFormat = Query(SummaryFormat.TEXT)
) -> dict:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    try:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)
            if not alerts:
                return {
                    "status": "no_alerts",
                    "message": "No active alerts to summarize",
                    "alert_count": 0,
                }

            summary = await AI_SUMMARIZER.force_generate_summary(
                alerts[:3],
                config={"include_route_info": True, "include_severity": True},
            )
            if format != SummaryFormat.TEXT:
                summary = format_summary(summary, format)
            return {
                "status": "success",
                "message": "AI summary generated successfully",
                "alert_count": len(alerts[:3]),
                "summary": summary,
                "format": format,
                "model": AI_SUMMARIZER.config.model,
            }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"AI summarization failed: {str(e)}"
        )


@router.get(
    "/ai/status",
    summary="AI Summarizer Status",
    description="Get the current status of the AI summarizer",
)
@limiter.limit("30/minute")
async def get_ai_status(request: Request) -> dict:
    if not AI_SUMMARIZER:
        return {"enabled": False, "message": "AI summarizer not enabled"}
    try:
        status: Dict[str, Any] = {
            "enabled": True,
            "model": AI_SUMMARIZER.config.model,
            "base_url": AI_SUMMARIZER.config.base_url,
            "timeout": AI_SUMMARIZER.config.timeout,
            "temperature": AI_SUMMARIZER.config.temperature,
        }
        if hasattr(AI_SUMMARIZER, "job_queue"):
            queue = AI_SUMMARIZER.job_queue
            status.update(
                {
                    "queue_status": {
                        "pending_jobs": len(queue.pending_jobs),
                        "running_jobs": len(queue.running_jobs),
                        "completed_jobs": len(queue.completed_jobs),
                        "max_queue_size": queue.max_queue_size,
                        "max_concurrent_jobs": queue.max_concurrent_jobs,
                    }
                }
            )
        return status
    except Exception as e:
        return {"enabled": True, "error": str(e)}


@router.get(
    "/alerts/summarize/health",
    summary="AI Summarizer Health Check",
    description="Check if the Ollama AI summarizer is healthy and available",
)
async def ai_summarizer_health() -> dict:
    if not AI_SUMMARIZER:
        return {
            "status": "disabled",
            "message": "AI summarizer is not enabled in configuration",
        }
    try:
        is_healthy = await AI_SUMMARIZER.health_check()
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "model": AI_SUMMARIZER.config.model,
            "endpoint": AI_SUMMARIZER.config.base_url,
            "enabled": True,
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "enabled": True}


@router.get(
    "/ai/summaries",
    summary="Get AI Summarized Alerts",
    description="Get current alerts with AI-generated summaries",
)
@limiter.limit("30/minute")
async def get_ai_summaries(request: Request) -> Response:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    try:
        cache_key = "api:alerts"
        cached_data = await REDIS_CLIENT.get(cache_key)
        if not cached_data:
            raise HTTPException(status_code=504, detail="Alerts cache not warmed up")

        alerts_data = Alerts.model_validate_json(cached_data)
        alerts = alerts_data.data

        summaries_cache_key = "api:alerts:summaries"
        cached_summaries = await REDIS_CLIENT.get(summaries_cache_key)
        if not cached_summaries:
            raise HTTPException(status_code=504, detail="Summaries cache not warmed up")

        summaries_data = json.loads(cached_summaries)
        summaries = summaries_data.get("data", [])

        combined_data: List[Dict[str, Any]] = []
        for alert in alerts:
            alert_data: Dict[str, Any] = {
                "id": alert.id,
                "type": alert.type,
                "attributes": {
                    "header": alert.attributes.header,
                    "short_header": alert.attributes.short_header,
                    "effect": alert.attributes.effect,
                    "severity": alert.attributes.severity,
                    "cause": alert.attributes.cause,
                    "timeframe": alert.attributes.timeframe,
                    "created_at": alert.attributes.created_at,
                    "updated_at": alert.attributes.updated_at,
                },
            }
            for summary in summaries:
                if summary.get("id") == alert.id:
                    attributes = dict(alert_data["attributes"])  # copy
                    attributes["summary"] = summary.get("summary", "")
                    alert_data["attributes"] = attributes
                    break
            combined_data.append(alert_data)

        return Response(
            content=json.dumps({"data": combined_data}), media_type="application/json"
        )
    except HTTPException:
        raise
    except (ConnectionError, TimeoutError):
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summaries: {str(e)}"
        )


@router.get(
    "/ai/summaries/cached",
    summary="Get Cached AI Summaries",
    description="Get the most recently cached AI summaries for alerts",
)
@limiter.limit("30/minute")
async def get_cached_alert_summaries(request: Request) -> dict:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    try:
        cache_pattern = "ai:summaries:active:*"
        cache_keys = await REDIS_CLIENT.keys(cache_pattern)
        if not cache_keys:
            return {
                "status": "no_cache",
                "message": "No cached summaries found",
                "cached_summaries": None,
            }
        cache_keys.sort(reverse=True)
        latest_key = cache_keys[0]
        cached_data = await REDIS_CLIENT.get(latest_key)
        if not cached_data:
            return {
                "status": "cache_expired",
                "message": "Cached summaries have expired",
                "cached_summaries": None,
            }
        summaries_data = json.loads(cached_data)
        return {
            "status": "success",
            "message": "Retrieved cached summaries",
            "cache_key": latest_key,
            "cached_summaries": summaries_data,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve cached summaries: {str(e)}"
        )


@router.get(
    "/ai/summaries/alert/{alert_id}",
    summary="Get AI Summary for Specific Alert",
)
@limiter.limit("30/minute")
async def get_alert_summary_by_id(
    request: Request, alert_id: str, format: SummaryFormat = Query(SummaryFormat.TEXT)
) -> dict:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    try:
        cache_pattern = "ai:summaries:active:*"
        cache_keys = await REDIS_CLIENT.keys(cache_pattern)
        for cache_key in sorted(cache_keys, reverse=True):
            cached_data = await REDIS_CLIENT.get(cache_key)
            if cached_data:
                summaries_data = json.loads(cached_data)
                for summary in summaries_data.get("summaries", []):
                    if summary.get("alert_id") == alert_id:
                        if format != SummaryFormat.TEXT:
                            summary["ai_summary"] = format_summary(
                                summary["ai_summary"], format
                            )
                            summary["format"] = format
                        return {
                            "status": "success",
                            "message": "Retrieved cached summary",
                            "cache_source": cache_key,
                            "summary": summary,
                        }

        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)
            target_alert = None
            for alert in alerts:
                if alert.id == alert_id:
                    target_alert = alert
                    break
            if not target_alert:
                raise HTTPException(
                    status_code=404, detail=f"Alert {alert_id} not found"
                )
            summary = await AI_SUMMARIZER.force_generate_summary(
                [target_alert],
                config={"include_route_info": True, "include_severity": True},
            )
            if format != SummaryFormat.TEXT:
                summary = format_summary(summary, format)
            summary_data = {
                "alert_id": target_alert.id,
                "alert_header": target_alert.attributes.header,
                "alert_short_header": target_alert.attributes.short_header,
                "alert_effect": target_alert.attributes.effect,
                "alert_severity": target_alert.attributes.severity,
                "alert_cause": target_alert.attributes.cause,
                "alert_timeframe": target_alert.attributes.timeframe,
                "ai_summary": summary,
                "format": format,
                "model_used": AI_SUMMARIZER.config.model,
                "generated_at": datetime.now().isoformat(),
                "cache_source": "generated_on_demand",
            }
            return {
                "status": "success",
                "message": "Generated summary on-demand",
                "summary": summary_data,
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summary: {str(e)}"
        )


@router.post("/ai/summaries/bulk")
@limiter.limit("10/minute")
async def generate_bulk_alert_summaries(
    request: Request,
    alert_ids: List[str],
    style: str = Query(default="comprehensive"),
    format: SummaryFormat = Query(default=SummaryFormat.TEXT),
) -> dict:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    if not alert_ids:
        raise HTTPException(status_code=400, detail="No alert IDs provided")
    if len(alert_ids) > 20:
        raise HTTPException(
            status_code=400, detail="Maximum 20 alerts allowed per request"
        )
    try:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            all_alerts = await fetch_alerts_with_retry(CONFIG, session)
        target_alerts: List[AlertResource] = []
        for alert_id in alert_ids:
            for alert in all_alerts:
                if alert.id == alert_id:
                    target_alerts.append(alert)
                    break
        if not target_alerts:
            return {
                "status": "no_alerts",
                "message": "None of the requested alerts were found",
                "requested_count": len(alert_ids),
                "found_count": 0,
                "summaries": [],
            }

        summaries: List[Dict[str, Any]] = []
        for alert in target_alerts:
            try:
                summary = await AI_SUMMARIZER.force_generate_summary(
                    [alert],
                    config={
                        "include_route_info": True,
                        "include_severity": True,
                        "style": style,
                    },
                )
                if CONFIG.file_output.validate_summaries:
                    is_valid, similarity, reason = validate_ai_summary(
                        summary, [alert], CONFIG.file_output.min_similarity_threshold
                    )
                    if not is_valid:
                        summaries.append(
                            {
                                "alert_id": alert.id,
                                "alert_header": alert.attributes.header,
                                "alert_short_header": alert.attributes.short_header,
                                "alert_effect": alert.attributes.effect,
                                "alert_severity": alert.attributes.severity,
                                "alert_cause": alert.attributes.cause,
                                "alert_timeframe": alert.attributes.timeframe,
                                "ai_summary": f"Summary validation failed: {reason}",
                                "format": format,
                                "model_used": AI_SUMMARIZER.config.model,
                                "generated_at": datetime.now().isoformat(),
                                "error": True,
                                "validation_failed": True,
                                "similarity_score": similarity,
                            }
                        )
                        continue
                if format != SummaryFormat.TEXT:
                    summary = format_summary(summary, format)
                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": summary,
                        "format": format,
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                    }
                )
            except Exception as e:
                summaries.append(
                    {
                        "alert_id": alert.id,
                        "alert_header": alert.attributes.header,
                        "alert_short_header": alert.attributes.short_header,
                        "alert_effect": alert.attributes.effect,
                        "alert_severity": alert.attributes.severity,
                        "alert_cause": alert.attributes.cause,
                        "alert_timeframe": alert.attributes.timeframe,
                        "ai_summary": f"Error generating summary: {str(e)}",
                        "model_used": AI_SUMMARIZER.config.model,
                        "generated_at": datetime.now().isoformat(),
                        "error": True,
                    }
                )

        if CONFIG.file_output.enabled and target_alerts:
            combined_summary = f"Bulk Alerts Summary ({style} style)\n\n"
            for summary_data in summaries:
                if not summary_data.get("error"):
                    combined_summary += f"## {summary_data['alert_header']}\n\n"
                    combined_summary += f"{summary_data['ai_summary']}\n\n"
                    if summary_data.get("alert_effect"):
                        combined_summary += (
                            f"**Effect:** {summary_data['alert_effect']}\n"
                        )
                    if summary_data.get("alert_severity"):
                        combined_summary += (
                            f"**Severity:** {summary_data['alert_severity']}\n"
                        )
                    if summary_data.get("alert_cause"):
                        combined_summary += (
                            f"**Cause:** {summary_data['alert_cause']}\n"
                        )
                    combined_summary += "\n"
            await save_summary_to_file(
                combined_summary, target_alerts, AI_SUMMARIZER.config.model, CONFIG
            )

        return {
            "status": "success",
            "message": f"Generated summaries for {len(target_alerts)} alerts",
            "requested_count": len(alert_ids),
            "found_count": len(target_alerts),
            "model_used": AI_SUMMARIZER.config.model,
            "generated_at": datetime.now().isoformat(),
            "summaries": summaries,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate bulk summaries: {str(e)}"
        )


@router.get("/ai/summaries/individual/{alert_id}")
async def get_individual_alert_summary(
    alert_id: str,
    sentence_limit: int = Query(default=2, ge=1, le=5),
    format: SummaryFormat = Query(default=SummaryFormat.TEXT),
) -> Dict[str, Any]:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not available")
    try:
        cache_key = f"ai_summary_individual:{alert_id}"
        cached_summary = await REDIS_CLIENT.get(cache_key)
        if cached_summary:
            summary_data = json.loads(cached_summary)
            if format != SummaryFormat.TEXT:
                summary_data["summary"] = format_summary(
                    summary_data["summary"], format
                )
            return summary_data
        return {
            "alert_id": alert_id,
            "summary": f"Individual summary not yet generated. Use the generate endpoint to create a {sentence_limit}-sentence summary.",
            "status": "pending",
            "message": f"Individual summaries are generated periodically. Check back later or use the generate endpoint for a {sentence_limit}-sentence summary.",
            "sentence_limit": sentence_limit,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching individual summary: {str(e)}"
        )


@router.post("/ai/summaries/individual/generate")
async def generate_individual_alert_summary(
    alert_id: str,
    style: str = Query(default="concise"),
    sentence_limit: int = Query(default=2, ge=1, le=5),
    format: SummaryFormat = Query(default=SummaryFormat.TEXT),
) -> Dict[str, Any]:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not available")
    try:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            alerts = await fetch_alerts_with_retry(CONFIG, session)
        target_alert = None
        for alert in alerts:
            if alert.id == alert_id:
                target_alert = alert
                break
        if not target_alert:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

        summary = await AI_SUMMARIZER.summarize_individual_alert(
            target_alert, style=style, sentence_limit=sentence_limit
        )
        if CONFIG.file_output.validate_summaries:
            is_valid, similarity, reason = validate_ai_summary(
                summary, [target_alert], CONFIG.file_output.min_similarity_threshold
            )
            if not is_valid:
                raise HTTPException(
                    status_code=422,
                    detail=f"Generated summary failed validation: {reason}. Similarity: {similarity:.3f}",
                )
        if format != SummaryFormat.TEXT:
            summary = format_summary(summary, format)

        cache_key = f"ai_summary_individual:{alert_id}"
        summary_data = {
            "alert_id": alert_id,
            "summary": summary,
            "style": style,
            "sentence_limit": sentence_limit,
            "generated_at": datetime.now().isoformat(),
            "format": format.value,
            "ttl": 3600,
        }
        await REDIS_CLIENT.setex(cache_key, 3600, json.dumps(summary_data))

        if CONFIG.file_output.enabled and target_alert:
            file_summary = "Individual Alert Summary\n\n"
            file_summary += f"## {target_alert.attributes.header}\n\n"
            file_summary += f"{summary}\n\n"
            if target_alert.attributes.effect:
                file_summary += f"**Effect:** {target_alert.attributes.effect}\n"
            if target_alert.attributes.severity:
                file_summary += f"**Severity:** {target_alert.attributes.severity}\n"
            if target_alert.attributes.cause:
                file_summary += f"**Cause:** {target_alert.attributes.cause}\n"
            if target_alert.attributes.timeframe:
                file_summary += f"**Timeframe:** {target_alert.attributes.timeframe}\n"
            file_summary += (
                f"\n**Style:** {style}\n**Sentence Limit:** {sentence_limit}\n"
            )
            await save_summary_to_file(
                file_summary, [target_alert], AI_SUMMARIZER.config.model, CONFIG
            )

        return summary_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating individual summary: {str(e)}"
        )


# Compatibility endpoint: summarize alerts with request body
from ai_summarizer import SummarizationRequest, SummarizationResponse


@router.post("/alerts/summarize", response_model=SummarizationResponse)
async def summarize_alerts(request: SummarizationRequest) -> SummarizationResponse:
    if not AI_SUMMARIZER:
        raise HTTPException(status_code=503, detail="AI summarizer not enabled")
    try:
        # If request contains no alerts, try loading from cache
        if not request.alerts:
            cached = await REDIS_CLIENT.get("api:alerts")
            if cached:
                alerts_data = Alerts.model_validate_json(cached)
                request.alerts = alerts_data.data

        response = await AI_SUMMARIZER.summarize_alerts(request)

        if request.alerts:
            cache_key = f"api:alerts:summary:{hash(str(request.alerts))}"
            cache_data = response.model_dump_json()
            await REDIS_CLIENT.setex(cache_key, CONFIG.ollama.cache_ttl, cache_data)

        if CONFIG.file_output.enabled and request.alerts:
            await save_summary_to_file(
                response.summary, request.alerts, AI_SUMMARIZER.config.model, CONFIG
            )
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate summary: {str(e)}"
        )
