# app/services/distributed_stream_manager.py
# 🔧 FIXED VERSION - Prevents interference with RTSP streams

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
    FIXED: Distributed stream manager that doesn't interfere with recovering streams.
    
    KEY FIXES:
    1. Longer grace periods for RTSP streams (5 minutes instead of 2)
    2. Distinction between "temporary failure" and "permanent failure"
    3. No lock release during recovery period
    4. Smarter health checks that account for RTSP buffering
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
        
        # ✅ FIX #1: Longer grace periods for RTSP
        self.rtsp_grace_period_seconds = 300  # 5 minutes for RTSP
        self.file_grace_period_seconds = 120  # 2 minutes for files
        
        logger.info(
            f"🚀 DistributedStreamManager initialized: "
            f"server_id={self.server_id}, capacity={self.max_local_capacity}, "
            f"rtsp_grace={self.rtsp_grace_period_seconds}s"
        )

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        ✅ FIX #2: Enhanced camera claiming with better filtering.
        
        CRITICAL CHANGES:
        1. Check if camera is in "recovery mode" (recent failure but not giving up)
        2. Don't claim cameras that another server is actively recovering
        3. Verify last_activity timestamp to avoid claiming recently active cameras
        """
        if slots_available <= 0:
            return []
        
        claim_query = """
            WITH available_cameras AS (
                SELECT vs.stream_id, vs.name, vs.path, vs.workspace_id, 
                    vs.user_id, vs.location, vs.area, vs.building,
                    vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                    u.username, u.role,
                    vs.retry_count, vs.next_retry_at,
                    vs.stop_reason, vs.auto_retry_enabled,
                    vs.last_activity, vs.updated_at,
                    vs.locked_by_server, vs.server_heartbeat,
                    -- ✅ NEW: Detect if stream is "recovering"
                    CASE 
                        WHEN vs.last_activity > NOW() - INTERVAL '5 minutes' THEN true
                        ELSE false
                    END as recently_active,
                    CASE
                        WHEN vs.path LIKE 'rtsp://%' THEN true
                        ELSE false
                    END as is_rtsp
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE vs.is_streaming = TRUE
                AND u.is_active = TRUE
                AND w.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
                -- ✅ CRITICAL: Strict user stop checks
                AND vs.stop_reason IS DISTINCT FROM 'user_action'
                AND vs.stop_reason IS DISTINCT FROM 'user_stop'
                AND vs.stop_reason IS DISTINCT FROM 'manual_stop'
                AND vs.stop_reason IS DISTINCT FROM 'admin_stop'
                AND vs.auto_retry_enabled = TRUE
                -- ✅ CRITICAL: Respect retry timing
                AND (
                    vs.next_retry_at IS NULL
                    OR vs.next_retry_at <= NOW()
                )
                -- ✅ FIX #3: Enhanced locking logic
                AND (
                    vs.locked_by_server IS NULL
                    OR (
                        -- Server is dead (no heartbeat for 5 minutes for RTSP, 2 for files)
                        vs.server_heartbeat < NOW() - INTERVAL '5 minutes'
                        AND vs.path LIKE 'rtsp://%'
                    )
                    OR (
                        vs.server_heartbeat < NOW() - INTERVAL '2 minutes'
                        AND vs.path NOT LIKE 'rtsp://%'
                    )
                )
                -- ✅ FIX #4: Don't claim recently active cameras
                AND NOT (
                    vs.last_activity > NOW() - INTERVAL '2 minutes'
                    AND vs.locked_by_server IS NOT NULL
                )
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE video_stream
            SET locked_by_server = $2,
                server_heartbeat = NOW(),
                status = 'processing',
                updated_at = NOW()
            FROM available_cameras
            WHERE video_stream.stream_id = available_cameras.stream_id
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
                available_cameras.recently_active,
                available_cameras.is_rtsp
        """
        
        try:
            claimed_cameras = await self.db_manager.execute_query(
                claim_query,
                (slots_available, self.server_id),
                fetch_all=True
            )
            
            if not claimed_cameras:
                return []
            
            # Post-claim verification
            verified_cameras = []
            
            for camera in claimed_cameras:
                stream_id = str(camera['stream_id'])
                
                # Check 1: is_streaming must be TRUE
                if not camera.get('is_streaming', True):
                    logger.error(
                        f"❌ POST-CLAIM CHECK FAILED: {stream_id} ({camera['name']}) "
                        f"has is_streaming=FALSE! Releasing immediately."
                    )
                    await self.release_camera_lock(stream_id, reason="claimed_but_not_streaming")
                    continue
                
                # Check 2: stop_reason must NOT be user_action
                if camera.get('stop_reason') in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                    logger.error(
                        f"❌ POST-CLAIM CHECK FAILED: {stream_id} ({camera['name']}) "
                        f"has stop_reason={camera.get('stop_reason')}! Releasing immediately."
                    )
                    await self.release_camera_lock(stream_id, reason="claimed_but_user_stopped")
                    continue
                
                # Check 3: auto_retry_enabled must be TRUE
                if not camera.get('auto_retry_enabled', True):
                    logger.error(
                        f"❌ POST-CLAIM CHECK FAILED: {stream_id} ({camera['name']}) "
                        f"has auto_retry_enabled=FALSE! Releasing immediately."
                    )
                    await self.release_camera_lock(stream_id, reason="claimed_but_retry_disabled")
                    continue
                
                # ✅ FIX #5: Warn if claiming recently active camera
                if camera.get('recently_active'):
                    logger.warning(
                        f"⚠️ Claimed recently active camera {stream_id} ({camera['name']}) "
                        f"- may be recovering from temporary failure"
                    )
                
                # Log claim with context
                retry_count = camera.get('retry_count', 0)
                is_rtsp = camera.get('is_rtsp', False)
                stream_type = "RTSP" if is_rtsp else "FILE"
                
                if retry_count > 0:
                    logger.info(
                        f"✅ Claimed {stream_type} camera {camera['name']} for retry attempt #{retry_count} "
                        f"(verified: is_streaming=TRUE, stop_reason={camera.get('stop_reason')}, "
                        f"auto_retry=TRUE)"
                    )
                else:
                    logger.info(
                        f"✅ Claimed {stream_type} camera {camera['name']} for first start "
                        f"(verified: is_streaming=TRUE, auto_retry=TRUE)"
                    )
                
                verified_cameras.append(camera)
            
            if len(verified_cameras) != len(claimed_cameras):
                logger.warning(
                    f"⚠️ Post-claim verification: {len(claimed_cameras)} claimed, "
                    f"{len(verified_cameras)} verified, "
                    f"{len(claimed_cameras) - len(verified_cameras)} rejected"
                )
            
            return verified_cameras
            
        except Exception as e:
            logger.error(f"Error claiming cameras: {e}", exc_info=True)
            return []
                            
    async def update_heartbeat_for_owned_cameras(self) -> int:
        """
        ✅ FIX #6: Smarter heartbeat with status preservation.
        
        CRITICAL: Don't overwrite 'error' status during recovery.
        """
        async with self._lock:
            owned_stream_ids = list(self.active_streams.keys())
        
        if not owned_stream_ids:
            return 0
        
        # ✅ Changed: Only update heartbeat, preserve status
        heartbeat_query = """
            UPDATE video_stream 
            SET server_heartbeat = NOW(),
                last_activity = CASE
                    WHEN status = 'active' THEN NOW()
                    ELSE last_activity
                END
            WHERE stream_id = ANY($1::uuid[]) 
              AND locked_by_server = $2
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

    async def release_camera_lock(self, stream_id_str: str, reason: str = "normal_stop"):
        """Release server lock on a camera when it stops."""
        release_query = """
            UPDATE video_stream
            SET locked_by_server = NULL,
                server_heartbeat = NULL,
                updated_at = NOW()
            WHERE stream_id = $1
              AND locked_by_server = $2
        """
        
        try:
            await self.db_manager.execute_query(
                release_query,
                (UUID(stream_id_str), self.server_id)
            )
            
            logger.info(
                f"🔓 Released lock for camera {stream_id_str} (reason: {reason})"
            )
            
        except Exception as e:
            logger.error(f"Error releasing camera lock: {e}")

    async def start_camera_locally(self, camera_data: Dict[str, Any]):
        """
        ✅ FIX #7: Enhanced pre-start validation.
        """
        stream_id = camera_data['stream_id']
        stream_id_str = str(stream_id)
        
        try:
            # Safety check
            verify_query = """
                SELECT stop_reason, auto_retry_enabled, last_activity, path
                FROM video_stream 
                WHERE stream_id = $1
            """
            
            verify = await self.db_manager.execute_query(
                verify_query, (stream_id,), fetch_one=True
            )
            
            if verify:
                if verify['stop_reason'] == 'user_action':
                    logger.warning(
                        f"⚠️ Camera {stream_id_str} was user-stopped after claim, "
                        f"releasing lock and skipping start"
                    )
                    await self.release_camera_lock(stream_id_str, reason="user_stopped_after_claim")
                    return
                
                if not verify.get('auto_retry_enabled', True):
                    logger.warning(
                        f"⚠️ Camera {stream_id_str} has auto_retry_enabled=FALSE, "
                        f"releasing lock and skipping start"
                    )
                    await self.release_camera_lock(stream_id_str, reason="auto_retry_disabled")
                    return
                
                # ✅ FIX #8: Check if recently active (might be recovering)
                last_activity = verify.get('last_activity')
                if last_activity:
                    age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_activity).total_seconds()
                    is_rtsp = verify.get('path', '').startswith('rtsp://')
                    grace_period = self.rtsp_grace_period_seconds if is_rtsp else self.file_grace_period_seconds
                    
                    if age < grace_period:
                        logger.warning(
                            f"⚠️ Camera {stream_id_str} was active {age:.0f}s ago "
                            f"(grace period: {grace_period}s). May be recovering. "
                            f"Starting anyway but monitoring closely."
                        )
                    
            # Build location info
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
            
            logger.info(f"✅ Started camera {camera_data['name']} on server {self.server_id}")
            
        except Exception as e:
            logger.error(f"Error starting camera {stream_id_str}: {e}", exc_info=True)
            
            # Release lock on failure
            await self.release_camera_lock(stream_id_str, reason="start_failed")
            
            # Remove from local state
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)

    async def stop_camera_locally(self, stream_id_str: str, reason: str = "normal_stop"):
        """Stop a camera running on this server and release its lock."""
        try:
            # Stop the actual stream
            from app.services.stream_service import stream_manager
            await stream_manager._stop_stream(stream_id_str, for_restart=False)
            
            # Release server lock
            await self.release_camera_lock(stream_id_str, reason=reason)
            
            # Remove from local state
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            logger.info(f"🛑 Stopped camera {stream_id_str} on server {self.server_id}")
            
        except Exception as e:
            logger.error(f"Error stopping camera {stream_id_str}: {e}")

    async def detect_zombie_cameras(self) -> List[str]:
        """Find cameras locked by this server but not running in memory."""
        db_locked_query = """
            SELECT stream_id 
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
        
        db_locked_ids = {str(row['stream_id']) for row in db_locked}
        
        async with self._lock:
            memory_ids = set(self.active_streams.keys())
        
        zombies = db_locked_ids - memory_ids
        
        if zombies:
            logger.warning(
                f"🧟 Found {len(zombies)} zombie cameras on server {self.server_id}"
            )
        
        return list(zombies)

    async def cleanup_zombies(self):
        """Clean up zombie cameras by releasing their locks."""
        zombies = await self.detect_zombie_cameras()
        
        for zombie_id in zombies:
            try:
                await self.release_camera_lock(zombie_id, reason="zombie_cleanup")
                logger.warning(f"🧹 Cleaned up zombie camera {zombie_id}")
            except Exception as e:
                logger.error(f"Error cleaning zombie {zombie_id}: {e}")

    async def manage_streams_with_deduplication(self):
        """
        ✅ FIX #9: Less aggressive management loop.
        
        CRITICAL CHANGES:
        1. Longer polling interval (30s instead of 20s)
        2. More lenient zombie detection
        3. Better handling of recovering cameras
        """
        logger.info(f"🔄 Management loop started for server {self.server_id}")
        
        zombie_check_counter = 0
        zombie_lock_counter = 0
        stuck_cleanup_counter = 0
        
        while self.is_running:
            try:
                # Cleanup stuck cameras every 10 cycles
                stuck_cleanup_counter += 1
                if stuck_cleanup_counter >= 10:
                    cleared = await self.clear_stuck_user_stopped_cameras()
                    if cleared > 0:
                        logger.warning(f"🧹 Auto-cleaned {cleared} stuck user-stopped cameras")
                    stuck_cleanup_counter = 0

                # Cleanup zombie locks every 3 cycles
                zombie_lock_counter += 1
                if zombie_lock_counter >= 3:
                    released = await self.cleanup_zombie_locks()
                    if released > 0:
                        logger.warning(f"🧹 Cleaned up {released} zombie server locks")
                    zombie_lock_counter = 0
                
                # Update heartbeat
                await self.update_heartbeat_for_owned_cameras()
                
                # Check capacity
                async with self._lock:
                    current_active = len(self.active_streams)
                
                slots_available = self.max_local_capacity - current_active
                
                logger.debug(
                    f"📊 Server {self.server_id}: "
                    f"{current_active}/{self.max_local_capacity} cameras active, "
                    f"{slots_available} slots available"
                )
                
                # Claim available cameras
                if slots_available > 0:
                    newly_claimed = await self.claim_available_cameras(slots_available)
                    
                    if newly_claimed:
                        logger.info(
                            f"🎯 Server {self.server_id} claimed {len(newly_claimed)} cameras"
                        )
                        
                        # Start claimed cameras
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
                
                # Zombie cleanup (every 5 cycles)
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    await self.cleanup_zombies()
                    zombie_check_counter = 0
                
                # ✅ FIX #10: More lenient "should stop" check
                async with self._lock:
                    local_stream_ids = list(self.active_streams.keys())
                
                if local_stream_ids:
                    check_query = """
                        SELECT stream_id, is_streaming, stop_reason, status, last_activity, path
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
                        
                        # Only stop if DEFINITELY should not run
                        is_rtsp = db_state.get('path', '').startswith('rtsp://')
                        
                        # Definite stop conditions
                        should_stop = (
                            not db_state['is_streaming'] or
                            db_state['stop_reason'] in ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                        )
                        
                        # ✅ FIX #11: Don't stop cameras in error status if they're still streaming
                        if db_state['status'] == 'error' and db_state['is_streaming']:
                            # Check if within grace period
                            last_activity = db_state.get('last_activity')
                            if last_activity:
                                age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_activity).total_seconds()
                                grace_period = self.rtsp_grace_period_seconds if is_rtsp else self.file_grace_period_seconds
                                
                                if age < grace_period:
                                    logger.debug(
                                        f"⏳ Camera {stream_id_str} in error but within grace period "
                                        f"({age:.0f}s / {grace_period}s). Allowing recovery."
                                    )
                                    should_stop = False
                        
                        if should_stop:
                            logger.info(
                                f"🛑 Stopping camera {stream_id_str}: "
                                f"is_streaming={db_state['is_streaming']}, "
                                f"stop_reason={db_state['stop_reason']}, "
                                f"status={db_state['status']}"
                            )
                            await self.stop_camera_locally(stream_id_str)
                
                # ✅ FIX #12: Longer sleep interval
                await asyncio.sleep(30)  # Increased from 20 to 30 seconds
                
            except asyncio.CancelledError:
                logger.info("Management loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in management loop: {e}", exc_info=True)
                await asyncio.sleep(10)

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
            await self.release_camera_lock(stream_id_str, reason="server_shutdown")
        
        logger.info(f"✅ Management loop stopped for server {self.server_id}")

    async def cleanup_zombie_locks(self):
        """Clean up locks from dead servers."""
        try:
            result = await self.db_manager.execute_query(
                "SELECT * FROM cleanup_zombie_server_locks()",
                fetch_one=True
            )
            
            if result and result['released_count'] > 0:
                logger.warning(
                    f"🧹 Released {result['released_count']} zombie locks: "
                    f"{result['affected_cameras']}"
                )
                return result['released_count']
            
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning zombie locks: {e}", exc_info=True)
            return 0

    async def clear_stuck_user_stopped_cameras(self):
        """Clean up cameras stuck with user_action but is_streaming=TRUE."""
        query = """
            WITH stuck_cameras AS (
                SELECT stream_id, name, workspace_id
                FROM video_stream
                WHERE is_streaming = TRUE
                AND stop_reason IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                AND auto_retry_enabled = FALSE
            )
            UPDATE video_stream
            SET is_streaming = FALSE,
                status = 'inactive',
                locked_by_server = NULL,
                server_heartbeat = NULL,
                next_retry_at = NULL,
                retry_count = 0,
                updated_at = NOW()
            FROM stuck_cameras
            WHERE video_stream.stream_id = stuck_cameras.stream_id
            RETURNING video_stream.stream_id, video_stream.name
        """
        
        try:
            result = await self.db_manager.execute_query(query, fetch_all=True)
            
            if result:
                camera_names = [r['name'] for r in result]
                logger.info(
                    f"✅ Cleared {len(result)} stuck user-stopped cameras: {camera_names}"
                )
                return len(result)
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Error clearing stuck cameras: {e}", exc_info=True)
            return 0


# Global instance
distributed_stream_manager = DistributedStreamManager()


# Integration function
async def initialize_distributed_stream_manager():
    """Initialize the distributed stream manager."""
    try:
        await distributed_stream_manager.start_management_loop()
        logger.info("✅ Distributed stream manager initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize distributed stream manager: {e}")
        return False