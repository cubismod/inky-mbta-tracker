import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

from redis.asyncio.client import Redis

logger = logging.getLogger(__name__)


class RedisBackup:
    """Handles Redis backups in RESP format"""

    def __init__(self, redis_url: Optional[str] = None, backup_dir: str = "./backups"):
        r = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT") or "",
            port=int(os.environ.get("IMT_REDIS_PORT", "6379") or ""),
            password=os.environ.get("IMT_REDIS_PASSWORD") or "",
        )
        self.redis = r
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)

    async def create_backup(self) -> str:
        """Create a Redis backup in RESP format and return the backup filename"""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_filename = f"redis_backup_{timestamp}.resp"
        backup_path = self.backup_dir / backup_filename

        try:
            # Get all keys
            keys = await self.redis.keys("*")
            logger.info(f"Found {len(keys)} keys to backup")

            with open(backup_path, "wb") as backup_file:
                backup_file.write(b"*" + str(len(keys) * 2).encode() + b"\r\n")

                for key in keys:
                    key_type = await self.redis.type(key)

                    backup_file.write(b"$" + str(len(key)).encode() + b"\r\n")
                    backup_file.write(key + b"\r\n")

                    if key_type == b"string":
                        value = await self.redis.get(key)
                        if value:
                            backup_file.write(b"$" + str(len(value)).encode() + b"\r\n")
                            backup_file.write(value + b"\r\n")
                        else:
                            backup_file.write(b"$-1\r\n")

                    elif key_type == b"hash":
                        hash_data = await self.redis.hgetall(key)
                        # Write hash as array of field-value pairs
                        backup_file.write(
                            b"*" + str(len(hash_data) * 2).encode() + b"\r\n"
                        )
                        for field, value in hash_data.items():
                            backup_file.write(b"$" + str(len(field)).encode() + b"\r\n")
                            backup_file.write(field + b"\r\n")
                            backup_file.write(b"$" + str(len(value)).encode() + b"\r\n")
                            backup_file.write(value + b"\r\n")

                    elif key_type == b"list":
                        list_data = await self.redis.lrange(key, 0, -1)
                        backup_file.write(b"*" + str(len(list_data)).encode() + b"\r\n")
                        for item in list_data:
                            backup_file.write(b"$" + str(len(item)).encode() + b"\r\n")
                            backup_file.write(item + b"\r\n")

                    elif key_type == b"set":
                        set_data = await self.redis.smembers(key)
                        backup_file.write(b"*" + str(len(set_data)).encode() + b"\r\n")
                        for member in set_data:
                            backup_file.write(
                                b"$" + str(len(member)).encode() + b"\r\n"
                            )
                            backup_file.write(member + b"\r\n")

                    elif key_type == b"zset":
                        zset_data = await self.redis.zrange(key, 0, -1, withscores=True)
                        backup_file.write(b"*" + str(len(zset_data)).encode() + b"\r\n")
                        for member, score in zset_data:
                            backup_file.write(
                                b"$" + str(len(member)).encode() + b"\r\n"
                            )
                            backup_file.write(member + b"\r\n")
                            score_str = str(score).encode()
                            backup_file.write(
                                b"$" + str(len(score_str)).encode() + b"\r\n"
                            )
                            backup_file.write(score_str + b"\r\n")

                    else:
                        # Unknown type, write as null
                        backup_file.write(b"$-1\r\n")

            await self.redis.aclose()

            backup_size = backup_path.stat().st_size
            logger.info(f"Backup created: {backup_filename} ({backup_size} bytes)")

            return backup_filename

        except Exception as e:
            logger.error("Failed to create Redis backup", exc_info=e)
            if backup_path.exists():
                backup_path.unlink()
            raise

    async def cleanup_old_backups(self, keep_days: int = 7) -> None:
        """Remove backup files older than specified days"""
        try:
            cutoff_time = datetime.now(UTC).timestamp() - (keep_days * 24 * 3600)

            removed_count = 0
            for backup_file in self.backup_dir.glob("redis_backup_*.resp"):
                if backup_file.stat().st_mtime < cutoff_time:
                    backup_file.unlink()
                    removed_count += 1
                    logger.info(f"Removed old backup: {backup_file.name}")

            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old backup files")

        except Exception as e:
            logger.error("Failed to cleanup old backups", exc_info=e)


async def run_backup() -> None:
    """Standalone function to run a backup"""
    backup = RedisBackup()
    await backup.create_backup()
    await backup.cleanup_old_backups()
