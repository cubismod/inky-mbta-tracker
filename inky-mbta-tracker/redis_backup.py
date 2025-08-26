import logging
from datetime import UTC, datetime
from typing import Any

from anyio import Path, open_file
from prometheus import redis_commands
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisBackup:
    """Handles Redis backups in RESP format"""

    def __init__(self, r_client: Redis, backup_dir: str = "./backups"):
        self.redis = r_client
        self.backup_dir = Path(backup_dir)

    async def create_backup(self) -> str:
        """Create a Redis backup in RESP format and return the backup filename"""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_filename = f"redis_backup_{timestamp}.resp"
        backup_path = self.backup_dir / backup_filename

        await self.backup_dir.mkdir(exist_ok=True)

        try:
            # Get all keys
            keys = await self.redis.keys("*")
            redis_commands.labels("keys").inc()
            logger.info(f"Found {len(keys)} keys to backup")

            async with await open_file(backup_path, "wb") as backup_file:
                await backup_file.write(b"*" + str(len(keys) * 2).encode() + b"\r\n")

                for key in keys:
                    key_type = await self.redis.type(key)
                    redis_commands.labels("type").inc()

                    await backup_file.write(b"$" + str(len(key)).encode() + b"\r\n")
                    await backup_file.write(key + b"\r\n")

                    if key_type == b"string":
                        value = await self.redis.get(key)
                        redis_commands.labels("get").inc()
                        if value:
                            await backup_file.write(
                                b"$" + str(len(value)).encode() + b"\r\n"
                            )
                            await backup_file.write(value + b"\r\n")
                        else:
                            await backup_file.write(b"$-1\r\n")

                    elif key_type == b"hash":
                        hash_data: dict[Any, Any] = await self.redis.hgetall(key)  # type: ignore[misc]
                        redis_commands.labels("hgetall").inc()
                        # Write hash as array of field-value pairs
                        await backup_file.write(
                            b"*" + str(len(hash_data) * 2).encode() + b"\r\n"
                        )
                        for field, value in hash_data.items():
                            await backup_file.write(
                                b"$" + str(len(field)).encode() + b"\r\n"
                            )
                            await backup_file.write(field + b"\r\n")
                            await backup_file.write(
                                b"$" + str(len(value)).encode() + b"\r\n"
                            )
                            await backup_file.write(value + b"\r\n")

                    elif key_type == b"list":
                        list_data: list[Any] = await self.redis.lrange(key, 0, -1)  # type: ignore[misc]
                        redis_commands.labels("lrange").inc()
                        await backup_file.write(
                            b"*" + str(len(list_data)).encode() + b"\r\n"
                        )
                        for item in list_data:
                            await backup_file.write(
                                b"$" + str(len(item)).encode() + b"\r\n"
                            )
                            await backup_file.write(item + b"\r\n")

                    elif key_type == b"set":
                        set_data: set[Any] = await self.redis.smembers(key)  # type: ignore[misc]
                        redis_commands.labels("smembers").inc()
                        await backup_file.write(
                            b"*" + str(len(set_data)).encode() + b"\r\n"
                        )
                        for member in set_data:
                            await backup_file.write(
                                b"$" + str(len(member)).encode() + b"\r\n"
                            )
                            await backup_file.write(member + b"\r\n")

                    elif key_type == b"zset":
                        zset_data = await self.redis.zrange(key, 0, -1, withscores=True)
                        redis_commands.labels("zrange").inc()
                        await backup_file.write(
                            b"*" + str(len(zset_data)).encode() + b"\r\n"
                        )
                        for member, score in zset_data:
                            await backup_file.write(
                                b"$" + str(len(member)).encode() + b"\r\n"
                            )
                            await backup_file.write(member + b"\r\n")
                            score_str = str(score).encode()
                            await backup_file.write(
                                b"$" + str(len(score_str)).encode() + b"\r\n"
                            )
                            await backup_file.write(score_str + b"\r\n")

                    else:
                        # Unknown type, write as null
                        await backup_file.write(b"$-1\r\n")

            await self.redis.aclose()
            redis_commands.labels("aclose").inc()

            backup_size = (await backup_path.stat()).st_size
            logger.info(f"Backup created: {backup_filename} ({backup_size} bytes)")

            return backup_filename

        except Exception as e:
            logger.error("Failed to create Redis backup", exc_info=e)
            if await backup_path.exists():
                await backup_path.unlink()
            raise

    async def cleanup_old_backups(self, keep_days: int = 7) -> None:
        """Remove backup files older than specified days"""
        try:
            cutoff_time = datetime.now(UTC).timestamp() - (keep_days * 24 * 3600)

            removed_count = 0
            async for backup_file in self.backup_dir.glob("redis_backup_*.resp"):
                if (await backup_file.stat()).st_mtime < cutoff_time:
                    await backup_file.unlink()
                    removed_count += 1
                    logger.info(f"Removed old backup: {backup_file.name}")

            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old backup files")

        except Exception as e:
            logger.error("Failed to cleanup old backups", exc_info=e)
