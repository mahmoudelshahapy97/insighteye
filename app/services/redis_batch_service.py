# app/services/redis_batch_service.py
"""
Redis Batch Detection Service

Buffers detection metadata in Redis lists (per stream/workspace) and
flushes them to PostgreSQL in bulk every DETECTION_BATCH_FLUSH_INTERVAL seconds.

Flow:
  detection → push_detection() → Redis list
  background loop (every 5 min) → flush_all_to_postgres() → batch_insert_detection_data()

Writes are buffered here.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

import redis.asyncio as aioredis

from app.config.settings import config

logger = logging.getLogger(__name__)

# Redis key prefix for detection batch lists
_KEY_PREFIX = "detection_batch"


def _make_key(workspace_id: str, stream_id: str) -> str:
    return f"{_KEY_PREFIX}:{workspace_id}:{stream_id}"


class RedisBatchService:
    """
    Async Redis buffer for detection data.

    Key design decisions
    --------------------
    * Frames (numpy arrays / base64 strings) are NOT stored in Redis —
      they're too large. Only the scalar metadata that goes into stream_results is stored.
    * If Redis is unavailable, we fall back to a direct PostgreSQL insert so
      no detection is ever silently dropped.
    * A single background asyncio.Task runs the 5-minute flush loop; it is
      started on app startup and cancelled (after a final flush) on shutdown.
    """

    def __init__(self) -> None:
        self._client: Optional[aioredis.Redis] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._connected: bool = False

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create / verify the Redis connection."""
        try:
            auth_kwargs: Dict[str, Any] = {}
            if config.redis_password:
                auth_kwargs["password"] = config.redis_password

            self._client = aioredis.Redis(
                host=config.redis_host,
                port=config.redis_port,
                db=config.redis_db,
                socket_timeout=config.redis_socket_timeout,
                socket_connect_timeout=config.redis_socket_connect_timeout,
                max_connections=config.redis_max_connections,
                decode_responses=True,
                **auth_kwargs,
            )
            await self._client.ping()
            self._connected = True
            logger.info(
                f"✅ Redis connected at {config.redis_host}:{config.redis_port}"
            )
        except Exception as exc:
            self._connected = False
            logger.error(
                f"❌ Redis connection failed: {exc}. Batch detection will fall "
                "back to direct PostgreSQL inserts.",
                exc_info=True,
            )

    async def disconnect(self) -> None:
        """Cleanly close the Redis connection."""
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None
            self._connected = False

    # ------------------------------------------------------------------
    # Push a single detection into Redis
    # ------------------------------------------------------------------

    async def push_detection(
        self,
        stream_id: str,
        workspace_id: str,
        user_id: str,
        camera_name: str,
        username: str,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        location_info: Optional[Dict[str, Any]] = None,
        result_id: Optional[str] = None,
    ) -> bool:
        """
        Serialise detection metadata and push it to a Redis list.

        Returns True on success, False if Redis is unavailable (caller should
        then fall back to a direct PG insert).
        """
        if not self._connected or self._client is None:
            logger.debug("Redis not connected — skipping push.")
            return False

        now = datetime.now(ZoneInfo("Africa/Cairo"))
        record = {
            "result_id": result_id,
            "camera_id": str(stream_id),
            "camera_name": camera_name,
            "user_id": str(user_id),
            "username": username,
            "workspace_id": str(workspace_id),
            "person_count": person_count,
            "male_count": male_count,
            "female_count": female_count,
            "fire_status": fire_status,
            "timestamp": now.timestamp(),
            "date": now.date().isoformat(),
            "time": now.time().isoformat(),
            "location_info": location_info or {},
        }

        key = _make_key(workspace_id, stream_id)

        try:
            await self._client.rpush(key, json.dumps(record))
            logger.debug(f"📥 Pushed detection to Redis key={key}")
            return True
        except Exception as exc:
            logger.warning(f"Failed to push detection to Redis ({key}): {exc}")
            self._connected = False  # Mark as disconnected; reconnect next cycle
            return False

    # ------------------------------------------------------------------
    # Flush all queues to PostgreSQL
    # ------------------------------------------------------------------

    async def flush_all_to_postgres(self) -> int:
        """
        Read every detection_batch:* key from Redis, delete it atomically,
        then send the records to PostgreSQL via batch_insert_detection_data().

        Returns the total number of records flushed.
        """
        if not self._connected or self._client is None:
            logger.debug("Redis not connected — skipping flush.")
            return 0

        # Import here to avoid circular imports at module load time
        from app.services.postgres_service import postgres_service

        total_flushed = 0

        try:
            # Discover all live batch keys
            keys: List[str] = await self._client.keys(f"{_KEY_PREFIX}:*")
        except Exception as exc:
            logger.error(f"Redis KEYS scan failed: {exc}")
            return 0

        if not keys:
            logger.debug("No detection_batch keys found in Redis — nothing to flush.")
            return 0

        for key in keys:
            try:
                # --- Atomic read-then-delete using a pipeline ---
                # LRANGE reads all items; DEL removes the key.
                # If the app crashes between LRANGE and DEL the items stay and
                # will be flushed on the next cycle (at-least-once semantics).
                async with self._client.pipeline(transaction=True) as pipe:
                    pipe.lrange(key, 0, -1)
                    pipe.delete(key)
                    results = await pipe.execute()

                raw_items: List[str] = results[0]
                if not raw_items:
                    continue

                # Deserialise
                records = []
                for raw in raw_items:
                    try:
                        records.append(json.loads(raw))
                    except json.JSONDecodeError as jex:
                        logger.warning(f"Bad JSON in Redis key {key}: {jex}")

                if not records:
                    continue

                # Group by workspace_id (each key encodes it, but be explicit)
                ws_id_str = records[0]["workspace_id"]
                try:
                    ws_uuid = UUID(ws_id_str)
                except ValueError:
                    logger.error(f"Invalid workspace_id in Redis key {key}: {ws_id_str}")
                    continue

                result = await postgres_service.batch_insert_detection_data(
                    detection_batch=records,
                    workspace_id=ws_uuid,
                )

                if result.get("success"):
                    n = result.get("inserted_count", len(records))
                    total_flushed += n
                    logger.info(
                        f"✅ Flushed {n} detections to PostgreSQL "
                        f"(key={key}, workspace={ws_id_str})"
                    )
                else:
                    logger.error(
                        f"❌ batch_insert_detection_data failed for key {key}: "
                        f"{result.get('error')}"
                    )
                    # Re-push items back so they are not lost
                    for raw in raw_items:
                        await self._client.rpush(key, raw)

            except Exception as exc:
                logger.error(
                    f"Error flushing Redis key {key}: {exc}", exc_info=True
                )

        if total_flushed:
            logger.info(f"🎯 Total detections flushed to PostgreSQL: {total_flushed}")
        return total_flushed

    # ------------------------------------------------------------------
    # Background flush loop
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        """
        Runs forever, flushing Redis → PostgreSQL every
        config.detection_batch_flush_interval seconds.
        """
        interval = config.detection_batch_flush_interval
        logger.info(
            f"🔄 Detection batch flush loop started "
            f"(interval={interval}s / {interval // 60} min)"
        )

        while True:
            try:
                await asyncio.sleep(interval)
                logger.info("⏰ Running scheduled detection batch flush...")
                # Re-attempt connection if previously disconnected
                if not self._connected:
                    await self.connect()
                await self.flush_all_to_postgres()
            except asyncio.CancelledError:
                logger.info("Detection batch flush loop cancelled.")
                raise
            except Exception as exc:
                logger.error(
                    f"Unexpected error in flush loop: {exc}", exc_info=True
                )

    async def start_flush_loop(self) -> None:
        """Connect to Redis and start the background flush task."""
        await self.connect()
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(
                self._flush_loop(), name="detection_batch_flush"
            )
            logger.info("✅ Detection batch flush task started.")

    async def stop_flush_loop(self) -> None:
        """
        Cancel the background task and perform a final flush before shutdown.
        """
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Performing final Redis → PostgreSQL flush before shutdown...")
        try:
            flushed = await self.flush_all_to_postgres()
            logger.info(f"🛑 Final flush complete: {flushed} records written.")
        except Exception as exc:
            logger.error(f"Error during final flush: {exc}", exc_info=True)
        await self.disconnect()

    # ------------------------------------------------------------------
    # Health / introspection helpers
    # ------------------------------------------------------------------

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Return current queue lengths per stream for monitoring."""
        if not self._connected or self._client is None:
            return {"connected": False, "queues": {}}

        try:
            keys = await self._client.keys(f"{_KEY_PREFIX}:*")
            queues = {}
            for key in keys:
                length = await self._client.llen(key)
                queues[key] = length
            return {"connected": True, "queues": queues, "total_keys": len(keys)}
        except Exception as exc:
            return {"connected": False, "error": str(exc), "queues": {}}


# Global singleton
redis_batch_service = RedisBatchService()
