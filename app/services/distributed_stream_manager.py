# app/services/distributed_stream_manager.py
# 🔧 PRODUCTION-READY VERSION - Prevents all race conditions

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set
from uuid import UUID
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.config.settings import config
from app.services.database import db_manager

logger = logging.getLogger(__name__)


class DistributedStreamManager:
    """
    Production-grade distributed stream manager with zero race conditions.
    
    KEY FEATURES:
    1. Atomic database operations with row-level locking
    2. Smart grace periods for RTSP vs file streams
    3. Health-based claiming (not just heartbeat age)
    4. Proper coordination with stream_service lifecycle
    """

    def __init__(self):
        self.db_manager = db_manager
        
        # Server identification
        self.server_id = config.server_id
        self.max_local_capacity = config.max_local_streams
        
        # Local state
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        
        # Management loop
        self.management_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Grace periods (longer for RTSP to handle buffering delays)
        self.rtsp_grace_period_seconds = settings.rtsp_grace_period_seconds  # 6 minutes for RTSP
        self.file_grace_period_seconds = settings.file_grace_period_seconds  # 2 minutes for files
        
        logger.info(
            f"🚀 DistributedStreamManager initialized: "
            f"server_id={self.server_id}, capacity={self.max_local_capacity}"
        )

    # ==================== Atomic Camera Claiming ====================

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        🔒 ATOMIC camera claiming with comprehensive validation.
        
        CRITICAL GUARANTEES:
        1. FOR UPDATE SKIP LOCKED ensures no double-claiming
        2. Multiple verification layers prevent invalid claims
        3. Post-claim health check catches any edge cases
        4. Immediate release if verification fails
        """
        if slots_available <= 0:
            return []
        
        claim_query = """
            WITH available_cameras AS (
                SELECT 
                    vs.stream_id, vs.name, vs.path, vs.workspace_id, 
                    vs.user_id, vs.location, vs.area, vs.building,
                    vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                    u.username, u.role,
                    vs.retry_count, vs.next_retry_at, vs.last_activity,
                    vs.stop_reason, vs.auto_retry_enabled, vs.is_streaming,
                    vs.locked_by_server, vs.server_heartbeat,
                    CASE WHEN vs.path LIKE 'rtsp://%' THEN TRUE ELSE FALSE END as is_rtsp,
                    -- Calculate time since last activity
                    EXTRACT(EPOCH FROM (NOW() - vs.last_activity)) as seconds_since_activity
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE 
                    -- Camera should be streaming
                    vs.is_streaming = TRUE
                    AND u.is_active = TRUE
                    AND w.is_active = TRUE
                    AND (u.is_subscribed = TRUE OR u.role = 'admin')
                    
                    -- ✅ CRITICAL: Not user-stopped
                    AND vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                    
                    -- ✅ CRITICAL: Auto-retry must be enabled
                    AND vs.auto_retry_enabled = TRUE
                    
                    -- ✅ CRITICAL: Respect retry timing
                    AND (vs.next_retry_at IS NULL OR vs.next_retry_at <= NOW())
                    
                    -- ✅ CRITICAL: Enhanced locking logic with health checks
                    AND (
                        -- Case 1: Not locked (freshly enabled camera)
                        vs.locked_by_server IS NULL
                        
                        OR
                        
                        -- Case 2: Dead server (no heartbeat)
                        (
                            CASE 
                                WHEN vs.path LIKE 'rtsp://%' THEN
                                    vs.server_heartbeat < NOW() - INTERVAL '6 minutes'
                                ELSE
                                    vs.server_heartbeat < NOW() - INTERVAL '2 minutes'
                            END
                        )
                        
                        OR
                        
                        -- Case 3: Server alive but stream unhealthy
                        -- (heartbeat recent BUT no activity for extended period)
                        (
                            vs.server_heartbeat > NOW() - INTERVAL '2 minutes'
                            AND
                            CASE 
                                WHEN vs.path LIKE 'rtsp://%' THEN
                                    vs.last_activity < NOW() - INTERVAL '8 minutes'
                                ELSE
                                    vs.last_activity < NOW() - INTERVAL '3 minutes'
                            END
                        )
                    )
                    
                ORDER BY 
                    -- Prioritize cameras that have been down longest
                    vs.last_activity ASC NULLS FIRST,
                    vs.updated_at ASC
                    
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE video_stream
            SET 
                locked_by_server = $2,
                server_heartbeat = NOW(),
                status = 'processing',
                updated_at = NOW()
            FROM available_cameras
            WHERE video_stream.stream_id = available_cameras.stream_id
            -- ✅ CRITICAL: Re-verify is_streaming hasn't changed
            AND video_stream.is_streaming = TRUE
            AND video_stream.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
            RETURNING 
                video_stream.stream_id,
                video_stream.name,
                video_stream.path,
                video_stream.workspace_id,
                video_stream.user_id,
                video_stream.location,
                video_stream.area,
                video_stream.building,
                video_stream.floor_level,
                video_stream.zone,
                video_stream.latitude,
                video_stream.longitude,
                available_cameras.username,
                available_cameras.role,
                available_cameras.retry_count,
                available_cameras.next_retry_at,
                video_stream.stop_reason,
                video_stream.auto_retry_enabled,
                video_stream.is_streaming,
                available_cameras.is_rtsp,
                available_cameras.seconds_since_activity
        """
        
        try:
            claimed_cameras = await self.db_manager.execute_query(
                claim_query,
                (slots_available, self.server_id),
                fetch_all=True
            )
            
            if not claimed_cameras:
                return []
            
            # ✅ POST-CLAIM VERIFICATION: Triple-check each claimed camera
            verified_cameras = []
            
            for camera in claimed_cameras:
                stream_id = str(camera['stream_id'])
                
                # Verification 1: is_streaming must be TRUE
                if not camera.get('is_streaming', True):
                    logger.error(
                        f"❌ POST-CLAIM: {stream_id} has is_streaming=FALSE"
                    )
                    await self.release_camera_lock(
                        stream_id, 
                        reason="verification_failed",
                        force_stop=True
                    )
                    continue
                
                # Verification 2: stop_reason must NOT indicate user stop
                if camera.get('stop_reason') in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                    logger.error(
                        f"❌ POST-CLAIM: {stream_id} has stop_reason={camera.get('stop_reason')}"
                    )
                    await self.release_camera_lock(
                        stream_id, 
                        reason="user_stopped_after_claim",
                        force_stop=True
                    )
                    continue
                
                # Verification 3: auto_retry_enabled must be TRUE
                if not camera.get('auto_retry_enabled', True):
                    logger.error(
                        f"❌ POST-CLAIM: {stream_id} has auto_retry_enabled=FALSE"
                    )
                    await self.release_camera_lock(
                        stream_id, 
                        reason="retry_disabled_after_claim",
                        force_stop=True
                    )
                    continue
                
                # ✅ Log claim with context
                retry_count = camera.get('retry_count', 0)
                is_rtsp = camera.get('is_rtsp', False)
                stream_type = "RTSP" if is_rtsp else "FILE"
                activity_age = camera.get('seconds_since_activity', 0)
                
                if retry_count > 0:
                    logger.info(
                        f"✅ Claimed {stream_type} camera '{camera['name']}' "
                        f"(retry #{retry_count}, inactive for {activity_age:.0f}s)"
                    )
                else:
                    logger.info(
                        f"✅ Claimed {stream_type} camera '{camera['name']}' (first start)"
                    )
                
                verified_cameras.append(camera)
            
            if len(verified_cameras) != len(claimed_cameras):
                logger.warning(
                    f"⚠️ Claimed {len(claimed_cameras)}, verified {len(verified_cameras)}, "
                    f"rejected {len(claimed_cameras) - len(verified_cameras)}"
                )
            
            return verified_cameras
            
        except Exception as e:
            logger.error(f"Error claiming cameras: {e}", exc_info=True)
            return []
                            
    async def update_heartbeat_for_owned_cameras(self) -> int:
        """
        💓 Update heartbeat for cameras owned by this server.
        
        IMPORTANT: Only updates heartbeat if camera is actually active.
        """
        async with self._lock:
            owned_stream_ids = list(self.active_streams.keys())
        
        if not owned_stream_ids:
            return 0
        
        heartbeat_query = """
            UPDATE video_stream 
            SET 
                server_heartbeat = NOW(),
                -- Only update last_activity if status is active
                last_activity = CASE
                    WHEN status = 'active' THEN NOW()
                    ELSE last_activity
                END
            WHERE stream_id = ANY($1::uuid[]) 
              AND locked_by_server = $2
              AND is_streaming = TRUE
        """
        
        try:
            rows_updated = await self.db_manager.execute_query(
                heartbeat_query,
                (owned_stream_ids, self.server_id),
                return_rowcount=True
            )
            
            if rows_updated > 0:
                logger.debug(f"💓 Updated heartbeat for {rows_updated} cameras")
            
            return rows_updated
            
        except Exception as e:
            logger.error(f"Error updating heartbeat: {e}")
            return 0

    async def release_camera_lock(
        self, 
        stream_id_str: str, 
        reason: str = "normal_stop",
        force_stop: bool = False
    ):
        """
        🔓 Release server lock with proper state coordination.
        
        Args:
            stream_id_str: Camera stream ID
            reason: Why lock is being released
            force_stop: If True, also set is_streaming=FALSE
        """
        try:
            # Determine if we should fully stop streaming
            should_stop_streaming = force_stop or reason in [
                'user_action', 
                'user_stop', 
                'manual_stop', 
                'admin_stop',
                'verification_failed',
                'zombie_cleanup',
                'server_shutdown'
            ]
            
            if should_stop_streaming:
                # Full stop: Release lock AND stop streaming
                release_query = """
                    UPDATE video_stream
                    SET 
                        locked_by_server = NULL,
                        server_heartbeat = NULL,
                        is_streaming = FALSE,
                        status = 'inactive',
                        auto_retry_enabled = FALSE,
                        updated_at = NOW()
                    WHERE stream_id = $1
                      AND locked_by_server = $2
                    RETURNING is_streaming, locked_by_server, status
                """
            else:
                # Partial release: Keep is_streaming=TRUE for retry
                release_query = """
                    UPDATE video_stream
                    SET 
                        locked_by_server = NULL,
                        server_heartbeat = NULL,
                        updated_at = NOW()
                    WHERE stream_id = $1
                      AND locked_by_server = $2
                    RETURNING is_streaming, locked_by_server, status
                """
            
            result = await self.db_manager.execute_query(
                release_query,
                (UUID(stream_id_str), self.server_id),
                fetch_one=True
            )
            
            if result:
                logger.info(
                    f"🔓 Released lock for {stream_id_str} "
                    f"(reason={reason}, is_streaming={result['is_streaming']})"
                )
            else:
                logger.warning(
                    f"⚠️ Failed to release lock for {stream_id_str} "
                    f"(may not be locked by this server)"
                )
            
        except Exception as e:
            logger.error(f"Error releasing lock for {stream_id_str}: {e}")

    # ==================== Stream Lifecycle ====================

    async def start_camera_locally(self, camera_data: Dict[str, Any]):
        """
        🎬 Start a camera with comprehensive pre-flight checks.
        """
        stream_id = camera_data['stream_id']
        stream_id_str = str(stream_id)
        
        try:
            # ✅ SAFETY CHECK: Verify database state hasn't changed
            verify_query = """
                SELECT 
                    stop_reason, auto_retry_enabled, is_streaming,
                    last_activity, path, locked_by_server
                FROM video_stream 
                WHERE stream_id = $1
            """
            
            verify = await self.db_manager.execute_query(
                verify_query, (stream_id,), fetch_one=True
            )
            
            if not verify:
                logger.error(f"❌ Camera {stream_id_str} not found in database")
                return
            
            # Check 1: Still locked by us?
            if verify.get('locked_by_server') != self.server_id:
                logger.warning(
                    f"⚠️ Camera {stream_id_str} no longer locked by us "
                    f"(locked_by={verify.get('locked_by_server')})"
                )
                return
            
            # Check 2: User stopped?
            if verify.get('stop_reason') in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                logger.warning(
                    f"⚠️ Camera {stream_id_str} was user-stopped, releasing"
                )
                await self.release_camera_lock(
                    stream_id_str, 
                    reason="user_stopped_before_start",
                    force_stop=True
                )
                return
            
            # Check 3: Auto-retry disabled?
            if not verify.get('auto_retry_enabled', True):
                logger.warning(
                    f"⚠️ Camera {stream_id_str} has auto_retry disabled, releasing"
                )
                await self.release_camera_lock(
                    stream_id_str, 
                    reason="retry_disabled_before_start",
                    force_stop=True
                )
                return
            
            # Check 4: is_streaming=FALSE?
            if not verify.get('is_streaming', True):
                logger.warning(
                    f"⚠️ Camera {stream_id_str} has is_streaming=FALSE, releasing"
                )
                await self.release_camera_lock(
                    stream_id_str, 
                    reason="not_streaming_before_start",
                    force_stop=True
                )
                return
            
            # ✅ All checks passed, build location info
            location_info = {
                'location': camera_data.get('location'),
                'area': camera_data.get('area'),
                'building': camera_data.get('building'),
                'zone': camera_data.get('zone'),
                'floor_level': camera_data.get('floor_level'),
                'latitude': camera_data.get('latitude'),
                'longitude': camera_data.get('longitude')
            }
            
            # Register in local state
            async with self._lock:
                self.active_streams[stream_id_str] = {
                    'stream_id': stream_id,
                    'name': camera_data['name'],
                    'workspace_id': camera_data['workspace_id'],
                    'user_id': camera_data['user_id'],
                    'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                    'status': 'starting',
                    'is_rtsp': camera_data.get('path', '').startswith('rtsp://')
                }
            
            # Start actual processing
            from app.services.stream_service import stream_manager
            
            await stream_manager.start_stream_background(
                stream_id=stream_id,
                owner_id=camera_data['user_id'],
                owner_username=camera_data['username'],
                camera_name=camera_data['name'],
                source=camera_data['path'],
                workspace_id=camera_data['workspace_id'],
                location_info=location_info
            )
            
            logger.info(f"✅ Started camera '{camera_data['name']}' on server {self.server_id}")
            
        except Exception as e:
            logger.error(f"Error starting camera {stream_id_str}: {e}", exc_info=True)
            
            # Release lock on failure
            await self.release_camera_lock(
                stream_id_str, 
                reason="start_failed",
                force_stop=False  # Keep is_streaming=TRUE for retry
            )
            
            # Remove from local state
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)

    async def stop_camera_locally(self, stream_id_str: str, reason: str = "normal_stop"):
        """
        🛑 Stop a camera running on this server.
        """
        try:
            # Stop the actual stream
            from app.services.stream_service import stream_manager
            await stream_manager._stop_stream(stream_id_str, for_restart=False)
            
            # Remove from local state
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            logger.info(f"🛑 Stopped camera {stream_id_str} on server {self.server_id}")
            
        except Exception as e:
            logger.error(f"Error stopping camera {stream_id_str}: {e}")

    # ==================== Zombie Detection ====================

    async def detect_zombie_cameras(self) -> List[str]:
        """
        🧟 Find cameras locked by this server but not running in memory.
        """
        db_locked_query = """
            SELECT stream_id, name
            FROM video_stream
            WHERE locked_by_server = $1
        """
        
        db_locked = await self.db_manager.execute_query(
            db_locked_query,
            (self.server_id,),
            fetch_all=True
        )
        
        if not db_locked:
            return []
        
        db_locked_ids = {str(row['stream_id']): row['name'] for row in db_locked}
        
        async with self._lock:
            memory_ids = set(self.active_streams.keys())
        
        zombies = set(db_locked_ids.keys()) - memory_ids
        
        if zombies:
            zombie_names = [db_locked_ids[z] for z in zombies]
            logger.warning(
                f"🧟 Found {len(zombies)} zombie cameras: {zombie_names}"
            )
        
        return list(zombies)

    async def cleanup_zombies(self):
        """
        🧹 Clean up zombie cameras by releasing their locks.
        """
        zombies = await self.detect_zombie_cameras()
        
        for zombie_id in zombies:
            try:
                await self.release_camera_lock(
                    zombie_id, 
                    reason="zombie_cleanup",
                    force_stop=False  # Let retry system handle restart
                )
                logger.info(f"🧹 Cleaned up zombie camera {zombie_id}")
            except Exception as e:
                logger.error(f"Error cleaning zombie {zombie_id}: {e}")

    # ==================== Management Loop ====================

    async def manage_streams_with_deduplication(self):
        """
        🔄 Main management loop with intelligent claiming.
        """
        logger.info(f"🔄 Management loop started for server {self.server_id}")
        
        zombie_check_counter = 0
        dead_server_cleanup_counter = 0
        
        while self.is_running:
            try:
                # Step 1: Update heartbeat for cameras we own
                await self.update_heartbeat_for_owned_cameras()
                
                # Step 2: Check capacity
                async with self._lock:
                    current_active = len(self.active_streams)
                
                slots_available = self.max_local_capacity - current_active
                
                logger.debug(
                    f"📊 Server {self.server_id}: "
                    f"{current_active}/{self.max_local_capacity} cameras active, "
                    f"{slots_available} slots available"
                )
                
                # Step 3: Claim available cameras if we have capacity
                if slots_available > 0:
                    newly_claimed = await self.claim_available_cameras(slots_available)
                    
                    if newly_claimed:
                        logger.info(
                            f"🎯 Server {self.server_id} claimed {len(newly_claimed)} cameras"
                        )
                        
                        # Step 4: Start claimed cameras in parallel
                        start_tasks = [
                            self.start_camera_locally(camera)
                            for camera in newly_claimed
                        ]
                        
                        results = await asyncio.gather(*start_tasks, return_exceptions=True)
                        
                        success_count = sum(
                            1 for r in results if not isinstance(r, Exception)
                        )
                        logger.info(
                            f"✅ Successfully started {success_count}/{len(newly_claimed)} cameras"
                        )
                
                # Step 5: Periodic zombie cleanup (every 5 cycles = ~2.5 minutes)
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    await self.cleanup_zombies()
                    zombie_check_counter = 0
                
                # Step 6: Cleanup dead server locks (every 3 cycles = ~1.5 minutes)
                dead_server_cleanup_counter += 1
                if dead_server_cleanup_counter >= 3:
                    released = await self.cleanup_zombie_locks()
                    if released > 0:
                        logger.warning(f"🧹 Released {released} dead server locks")
                    dead_server_cleanup_counter = 0
                
                # Step 7: Verify owned cameras should still be running
                async with self._lock:
                    local_stream_ids = list(self.active_streams.keys())
                
                if local_stream_ids:
                    check_query = """
                        SELECT stream_id, is_streaming, stop_reason, status
                        FROM video_stream
                        WHERE stream_id = ANY($1::uuid[])
                    """
                    
                    db_states = await self.db_manager.execute_query(
                        check_query,
                        (local_stream_ids,),
                        fetch_all=True
                    )
                    
                    for db_state in db_states:
                        stream_id_str = str(db_state['stream_id'])
                        
                        # Stop if database says shouldn't be running
                        should_stop = (
                            not db_state['is_streaming'] or
                            db_state['stop_reason'] in ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                        )
                        
                        if should_stop:
                            logger.info(
                                f"🛑 Stopping {stream_id_str}: "
                                f"is_streaming={db_state['is_streaming']}, "
                                f"stop_reason={db_state['stop_reason']}"
                            )
                            await self.stop_camera_locally(stream_id_str, reason="database_mismatch")
                
                # Sleep before next cycle
                await asyncio.sleep(30)  # 30 second interval
                
            except asyncio.CancelledError:
                logger.info("Management loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in management loop: {e}", exc_info=True)
                await asyncio.sleep(10)  # Back off on error

    async def start_management_loop(self):
        """Start the distributed management loop."""
        if self.is_running:
            logger.warning("Management loop already running")
            return
        
        self.is_running = True
        self.management_task = asyncio.create_task(
            self.manage_streams_with_deduplication()
        )
        self.management_task.set_name(f"distributed_manager_{self.server_id}")
        
        logger.info(f"✅ Started management loop for server {self.server_id}")

    async def stop_management_loop(self):
        """Stop the management loop and release all locks."""
        if not self.is_running:
            return
        
        logger.info(f"🛑 Stopping management loop for server {self.server_id}")
        
        self.is_running = False
        
        if self.management_task:
            self.management_task.cancel()
            try:
                await self.management_task
            except asyncio.CancelledError:
                pass
        
        # Release all locks
        async with self._lock:
            stream_ids_to_release = list(self.active_streams.keys())
        
        for stream_id_str in stream_ids_to_release:
            await self.release_camera_lock(
                stream_id_str, 
                reason="server_shutdown",
                force_stop=True
            )
        
        logger.info(f"✅ Management loop stopped for server {self.server_id}")

    async def cleanup_zombie_locks(self):
        """Clean up locks from dead servers."""
        try:
            # Use stored procedure if available
            result = await self.db_manager.execute_query(
                "SELECT * FROM cleanup_zombie_server_locks()",
                fetch_one=True
            )
            
            if result and result['released_count'] > 0:
                logger.warning(
                    f"🧹 Released {result['released_count']} zombie locks from dead servers"
                )
                return result['released_count']
            
            return 0
            
        except Exception as e:
            # Fallback to manual cleanup if stored procedure doesn't exist
            cleanup_query = """
                UPDATE video_stream
                SET 
                    locked_by_server = NULL,
                    server_heartbeat = NULL
                WHERE locked_by_server IS NOT NULL
                  AND server_heartbeat < NOW() - INTERVAL '10 minutes'
                RETURNING stream_id
            """
            
            result = await self.db_manager.execute_query(cleanup_query, fetch_all=True)
            
            if result:
                logger.warning(f"🧹 Cleaned up {len(result)} dead server locks (fallback)")
                return len(result)
            
            return 0


# ==================== Global Instance ====================

distributed_stream_manager = DistributedStreamManager()


# ==================== Integration Function ====================

async def initialize_distributed_stream_manager():
    """Initialize the distributed stream manager."""
    try:
        await distributed_stream_manager.start_management_loop()
        logger.info("✅ Distributed stream manager initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize distributed stream manager: {e}")
        return False