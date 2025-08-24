import os
from contextlib import asynccontextmanager

import aiohttp
from anyio import create_task_group, sleep
from consts import HOUR, MBTA_V3_ENDPOINT, MINUTE
from fastapi import FastAPI

from .core import AI_SUMMARIZER, CONFIG, logger
from .services.alerts import fetch_alerts_with_retry


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown"""
    logger.info("Starting MBTA Transit Data API...")

    logger.debug(f"File output configuration - enabled: {CONFIG.file_output.enabled}")
    if CONFIG.file_output.enabled:
        logger.debug(f"File output directory: {CONFIG.file_output.output_directory}")
        logger.debug(f"File output filename: {CONFIG.file_output.filename}")
        logger.debug(
            f"File output include_timestamp: {CONFIG.file_output.include_timestamp}"
        )
        logger.debug(
            f"File output include_alert_count: {CONFIG.file_output.include_alert_count}"
        )
        logger.debug(
            f"File output include_model_info: {CONFIG.file_output.include_model_info}"
        )
    else:
        logger.debug("File output is disabled")

    async with create_task_group() as tg:
        if AI_SUMMARIZER:
            try:
                await AI_SUMMARIZER.start()
                logger.info("AI summarizer started successfully")
                sleep_max = os.getenv("IMT_SUMMARY_SLEEP_MAX", "30")
                try:
                    sleep_time = int(sleep_max)
                except Exception:
                    sleep_time = 30

                try:
                    await sleep(sleep_time)
                    logger.info(
                        f"Sleeping for {sleep_time} seconds before creating initial AI summarization job"
                    )
                    logger.info(
                        "Creating initial AI summarization job for existing alerts..."
                    )

                    async with aiohttp.ClientSession(
                        base_url=MBTA_V3_ENDPOINT
                    ) as session:
                        alerts = await fetch_alerts_with_retry(CONFIG, session)

                    if alerts:
                        job_id = await AI_SUMMARIZER.queue_summary_job(
                            alerts,
                            priority=1,  # High priority for startup
                            config={
                                "include_route_info": True,
                                "include_severity": True,
                            },
                        )
                        logger.info(
                            f"Initial startup: queued AI summary job {job_id} for {len(alerts)} alerts"
                        )
                    else:
                        logger.info("No existing alerts to summarize on startup")
                except Exception as e:
                    logger.warning(
                        f"Failed to create initial AI summarization job: {e}"
                    )

                logger.info("Creating periodic AI summary refresh task...")
                tg.start_soon(periodic_ai_summary_refresh)

                logger.info("Creating periodic individual alert summary task...")
                tg.start_soon(_periodic_individual_alert_summaries)

            except Exception as e:
                logger.error(f"Failed to start AI summarizer: {e}", exc_info=True)
        else:
            logger.info("AI summarizer not enabled")

        yield


async def periodic_ai_summary_refresh() -> None:
    """Periodically refresh AI summaries for current alerts"""
    logger.info("Starting periodic AI summary refresh task")
    while True:
        try:
            sleep_time = int(os.getenv("IMT_SUMMARY_REFRESH_MIN", str(4 * MINUTE)))
            sleep_time_max = int(os.getenv("IMT_SUMMARY_REFRESH_MAX", str(2 * HOUR)))
            await sleep(min(sleep_time_max, max(MINUTE, sleep_time)))

            if AI_SUMMARIZER:
                try:
                    logger.info("Starting AI summary refresh cycle")
                    async with aiohttp.ClientSession(
                        base_url=MBTA_V3_ENDPOINT
                    ) as session:
                        alerts = await fetch_alerts_with_retry(CONFIG, session)

                    if alerts:
                        job_id = await AI_SUMMARIZER.queue_summary_job(
                            alerts,
                            priority=3,
                            config={
                                "include_route_info": True,
                                "include_severity": True,
                            },
                        )
                        logger.info(
                            f"Periodic refresh: queued AI summary job {job_id} for {len(alerts)} alerts"
                        )
                    else:
                        logger.info("Periodic refresh: no alerts to summarize")

                except Exception as e:
                    logger.error(
                        f"Periodic AI summary refresh failed: {e}", exc_info=True
                    )
        except Exception as e:
            logger.error(
                f"Unexpected error in periodic AI summary refresh: {e}", exc_info=True
            )
            await sleep(60)


async def _periodic_individual_alert_summaries() -> None:
    """Periodically generate individual alert summaries."""
    while True:
        try:
            logger.debug("Starting periodic individual alert summaries")
            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                alerts = await fetch_alerts_with_retry(CONFIG, session)

            if not alerts:
                logger.warning("No alerts found for individual summaries")
                await sleep(5 * MINUTE)
                continue

            if AI_SUMMARIZER:
                job_ids = AI_SUMMARIZER.queue_bulk_individual_summaries(
                    alerts, sentence_limit=1
                )
                logger.info(
                    f"Queued {len(job_ids)} individual alert summary jobs (1-sentence limit)"
                )

            await sleep(5 * MINUTE)

        except Exception as e:
            logger.error(f"Error in periodic individual alert summaries: {e}")
            await sleep(5 * MINUTE)
