import logging
import os
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from anyio import sleep
from anyio.abc import TaskGroup
from consts import HOUR
from redis.asyncio import Redis
from redis_backup import RedisBackup

logger = logging.getLogger(__name__)


class BackupScheduler:
    """Schedules Redis backups to run at 3:00am daily"""

    def __init__(
        self,
        tg: TaskGroup,
        r_client: Redis,
        backup_time: time = time(3, 0),
        backup_dir: str = "./backups",
    ):
        self.backup_time = backup_time
        self.backup = RedisBackup(r_client, backup_dir)
        self.running = False
        self.tg = tg

    def _get_next_backup_time(self) -> datetime:
        """Calculate the next backup time in UTC"""
        east_coast_tz = ZoneInfo("America/New_York")
        now_eastern = datetime.now(east_coast_tz)

        next_backup_eastern = datetime.combine(
            now_eastern.date(), self.backup_time, tzinfo=east_coast_tz
        )

        # If it's already past the backup time today, schedule for tomorrow
        if now_eastern >= next_backup_eastern:
            next_backup_eastern += timedelta(days=1)
            next_backup_eastern = next_backup_eastern.replace(
                hour=self.backup_time.hour,
                minute=self.backup_time.minute,
                second=0,
                microsecond=0,
            )

        # Convert to UTC for consistent internal handling
        return next_backup_eastern.astimezone(UTC)

    async def run_scheduler(self, tg: TaskGroup) -> None:
        """Main scheduler loop that runs the backup task at 3:00am daily"""
        self.running = True
        logger.info("Redis backup scheduler started")

        while self.running:
            try:
                next_backup_time = self._get_next_backup_time()
                now = datetime.now(UTC)
                sleep_seconds = (next_backup_time - now).total_seconds()

                # Convert backup time back to Eastern for logging
                east_coast_tz = ZoneInfo("America/New_York")
                next_backup_eastern = next_backup_time.astimezone(east_coast_tz)

                logger.info(
                    f"Next backup scheduled for: {next_backup_eastern.strftime('%Y-%m-%d %I:%M %p %Z')} (in {sleep_seconds / 3600:.1f} hours)"
                )

                await sleep(sleep_seconds)

                if not self.running:
                    break

                # Perform backup
                logger.info("Starting scheduled Redis backup")
                try:
                    backup_filename = await self.backup.create_backup()
                    logger.info(
                        f"Scheduled backup completed successfully: {backup_filename}"
                    )

                    # Cleanup old backups (keep 7 days by default)
                    tg.start_soon(self.backup.cleanup_old_backups)

                except Exception as e:
                    logger.error("Scheduled backup failed", exc_info=e)

            except Exception as e:
                logger.error("Backup scheduler error", exc_info=e)
                # Wait 1 hour before retrying on error
                await sleep(HOUR)

        logger.info("Redis backup scheduler stopped")

    def stop(self) -> None:
        """Stop the backup scheduler"""
        self.running = False


async def run_backup_scheduler(tg: TaskGroup) -> None:
    backup_time_str = os.getenv("IMT_REDIS_BACKUP_TIME", "03:00")
    if isinstance(backup_time_str, str):
        try:
            hour, minute = map(int, backup_time_str.split(":"))
            # Parse time as East Coast time
            backup_time = time(hour=hour, minute=minute)
        except Exception:
            logger.error(
                f"Invalid IMT_REDIS_BACKUP_TIME format: {backup_time_str}, using default 03:00 EST"
            )
            backup_time = time(hour=3, minute=0)
    else:
        backup_time = time(hour=3, minute=0)

    scheduler = BackupScheduler(tg=tg, backup_time=backup_time)
    await scheduler.run_scheduler(tg)
