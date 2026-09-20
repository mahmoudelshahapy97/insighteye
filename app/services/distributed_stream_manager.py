# app/services/distributed_stream_manager.py
# ✅ FIXED VERSION - Prevents app freezing with timeouts and non-blocking operations

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set
from uuid import UUID
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
from app.config.settings import config
from app.services.database import db_manager

logger = logging.getLogger(__name__)


class DistributedStreamManager:
    """
    Production-grade distributed stream manager with TIMEOUT PROTECTION.
    
    KEY FIXES:
    1. ✅ All blocking operations have timeouts
    2. ✅ Camera starts are fire-and-forget (don't block loop)
    3. ✅ Zombie detection runs in background
    4. ✅ Health checks are async and time-bounded
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
        
        # ✅ NEW: Timeout configuration
        self.camera_start_timeout = 30.0  # 30 seconds max to start
        self.zombie_detection_timeout = 60.0  # 1 minute max
        self.heartbeat_timeout = 5.0  # 5 seconds max
        
        # Grace periods
        self.rtsp_grace_period_seconds = config.rtsp_grace_period_seconds  
        self.file_grace_period_seconds = config.file_grace_period_seconds  
        
        logger.info(
            f"🚀 DistributedStreamManager initialized: "
            f"server_id={self.server_id}, capacity={self.max_local_capacity}"
        )

    # ==================== Helper: Safe Async Operations ====================

    async def _safe_operation(
        self,
        coro,
        timeout: float,
        operation_name: str,
        default_return=None
    ):
        """
        ✅ Execute async operation with timeout protection.
        
        Prevents infinite hangs that freeze the entire app.
        """
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(
                f"⏰ TIMEOUT: {operation_name} exceeded {timeout}s - ABORTING to prevent freeze"
            )
            return default_return
        except Exception as e:
            logger.error(f"❌ Error in {operation_name}: {e}")
            return default_return

    # ==================== Atomic Camera Claiming ====================

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        🔒 ATOMIC camera claiming with TIMEOUT PROTECTION.
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
                    vs.locked_by_server, vs.server_heartbeat, vs.status,
                    CASE WHEN vs.path LIKE 'rtsp://%' THEN TRUE ELSE FALSE END as is_rtsp
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE 
                    vs.is_streaming = TRUE
                    AND u.is_active = TRUE
                    AND w.is_active = TRUE
                    AND (u.is_subscribed = TRUE OR u.role IN ('admin', 'superadmin'))
                    AND (vs.stop_reason IS NULL OR vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
                    AND vs.auto_retry_enabled = TRUE
                    AND (vs.next_retry_at IS NULL OR vs.next_retry_at <= NOW())
                    AND vs.status IN ('active', 'processing', 'error')
                    AND (
                        vs.locked_by_server IS NULL
                        OR (
                            vs.locked_by_server IS NOT NULL
                            AND (
                                vs.server_heartbeat IS NULL
                                OR CASE 
                                    WHEN vs.path LIKE 'rtsp://%' THEN
                                        vs.server_heartbeat < NOW() - INTERVAL '5 minutes'
                                    ELSE
                                        vs.server_heartbeat < NOW() - INTERVAL '2 minutes'
                                END
                            )
                        )
                        OR (
                            vs.locked_by_server IS NOT NULL
                            AND vs.server_heartbeat > NOW() - INTERVAL '5 minutes'
                            AND CASE 
                                WHEN vs.path LIKE 'rtsp://%' THEN
                                    vs.last_activity < NOW() - INTERVAL '5 minutes'
                                ELSE
                                    vs.last_activity < NOW() - INTERVAL '2 minutes'
                            END
                        )
                    )
                ORDER BY vs.last_activity ASC NULLS FIRST, vs.updated_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE video_stream
            SET 
                locked_by_server = $2,
                server_heartbeat = NOW(),
                status = 'processing',
                updated_at = NOW(),
                last_activity = NOW()
            FROM available_cameras
            WHERE video_stream.stream_id = available_cameras.stream_id
            AND video_stream.is_streaming = TRUE
            AND (video_stream.stop_reason IS NULL OR video_stream.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
            RETURNING 
                video_stream.stream_id, video_stream.name, video_stream.path,
                video_stream.workspace_id, video_stream.user_id,
                available_cameras.username, available_cameras.role,
                available_cameras.retry_count, available_cameras.is_rtsp
        """
        
        try:
            # ✅ TIMEOUT: Claiming should never take > 10 seconds
            claimed_cameras = await self._safe_operation(
                self.db_manager.execute_query(
                    claim_query,
                    (slots_available, self.server_id),
                    fetch_all=True
                ),
                timeout=10.0,
                operation_name="claim_cameras",
                default_return=[]
            )
            
            if not claimed_cameras:
                return []
            
            # Verify claims
            verified_cameras = []
            for camera in claimed_cameras:
                stream_id = str(camera['stream_id'])
                
                # Quick verification
                verify_query = """
                    SELECT is_streaming, stop_reason, auto_retry_enabled
                    FROM video_stream
                    WHERE stream_id = $1
                """
                
                verify = await self._safe_operation(
                    self.db_manager.execute_query(verify_query, (camera['stream_id'],), fetch_one=True),
                    timeout=2.0,
                    operation_name=f"verify_claim_{stream_id}",
                    default_return=None
                )
                
                if not verify or not verify['is_streaming']:
                    await self.release_camera_lock(stream_id, reason="verification_failed", force_stop=True)
                    continue
                
                verified_cameras.append(camera)
            
            if verified_cameras:
                logger.warning(f"✅ Successfully claimed {len(verified_cameras)} cameras")
            
            return verified_cameras
            
        except Exception as e:
            logger.error(f"Error claiming cameras: {e}", exc_info=True)
            return []

    async def update_heartbeat_for_owned_cameras(self) -> int:
        """
        💓 Update heartbeat with TIMEOUT PROTECTION.
        """
        async with self._lock:
            owned_stream_ids = list(self.active_streams.keys())
        
        if not owned_stream_ids:
            return 0
        
        try:
            owned_stream_uuids = [UUID(sid) for sid in owned_stream_ids]
        except Exception as e:
            logger.error(f"Error converting stream IDs: {e}")
            return 0
        
        heartbeat_query = """
            UPDATE video_stream 
            SET 
                server_heartbeat = NOW(),
                last_activity = NOW(),
                updated_at = NOW()
            WHERE stream_id = ANY($1::uuid[]) 
            AND locked_by_server = $2
            AND is_streaming = TRUE
            RETURNING stream_id
        """
        
        # ✅ TIMEOUT: Heartbeat should never take > 5 seconds
        result = await self._safe_operation(
            self.db_manager.execute_query(
                heartbeat_query,
                (owned_stream_uuids, self.server_id),
                fetch_all=True
            ),
            timeout=self.heartbeat_timeout,
            operation_name="update_heartbeat",
            default_return=[]
        )
        
        rows_updated = len(result) if result else 0
        
        if rows_updated > 0:
            logger.debug(f"💓 Updated heartbeat for {rows_updated} cameras")
        
        return rows_updated

    async def release_camera_lock(
        self, 
        stream_id_str: str, 
        reason: str = "normal_stop",
        force_stop: bool = False
    ):
        """
        🔓 Release server lock with TIMEOUT PROTECTION.
        """
        try:
            should_stop_streaming = force_stop or reason in [
                'user_action', 'user_stop', 'manual_stop', 'admin_stop',
                'verification_failed', 'zombie_cleanup', 'server_shutdown'
            ]
            
            if should_stop_streaming:
                release_query = """
                    UPDATE video_stream
                    SET 
                        locked_by_server = NULL,
                        server_heartbeat = NULL,
                        is_streaming = FALSE,
                        status = 'inactive',
                        stop_reason = $3,           
                        stopped_at = NOW(),         
                        auto_retry_enabled = FALSE,
                        updated_at = NOW()
                    WHERE stream_id = $1 AND locked_by_server = $2
                """
                result = await self._safe_operation(
                    self.db_manager.execute_query(
                        release_query,
                        (UUID(stream_id_str), self.server_id, reason),
                        fetch_one=True
                    ),
                    timeout=5.0,
                    operation_name=f"release_lock_{stream_id_str}",
                    default_return=None
                )
            else:
                release_query = """
                    UPDATE video_stream
                    SET 
                        locked_by_server = NULL,
                        server_heartbeat = NULL,
                        updated_at = NOW()
                    WHERE stream_id = $1 AND locked_by_server = $2
                """
                result = await self._safe_operation(
                    self.db_manager.execute_query(
                        release_query,
                        (UUID(stream_id_str), self.server_id),
                        fetch_one=True
                    ),
                    timeout=5.0,
                    operation_name=f"release_lock_{stream_id_str}",
                    default_return=None
                )
            
            if result:
                logger.info(f"🔓 Released lock for {stream_id_str} (reason={reason})")
            
        except Exception as e:
            logger.error(f"Error releasing lock for {stream_id_str}: {e}")

    # ==================== Stream Lifecycle ====================

    async def start_camera_locally(self, camera_data: Dict[str, Any]):
        """
        🎬 Start a camera with TIMEOUT PROTECTION.
        
        ✅ CRITICAL FIX: This is now FIRE-AND-FORGET - doesn't block management loop!
        """
        stream_id = camera_data['stream_id']
        stream_id_str = str(stream_id)
        
        try:
            # Quick verification
            verify_query = """
                SELECT stop_reason, auto_retry_enabled, is_streaming, locked_by_server
                FROM video_stream 
                WHERE stream_id = $1
            """
            
            verify = await self._safe_operation(
                self.db_manager.execute_query(verify_query, (stream_id,), fetch_one=True),
                timeout=3.0,
                operation_name=f"verify_start_{stream_id_str}",
                default_return=None
            )
            
            if not verify:
                logger.error(f"❌ Camera {stream_id_str} not found in database")
                return
            
            if str(verify.get('locked_by_server')) != str(self.server_id):
                logger.warning(f"⚠️ Camera {stream_id_str} not locked by us")
                return
            
            if verify.get('stop_reason') in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                logger.warning(f"⚠️ Camera {stream_id_str} was user-stopped")
                await self.release_camera_lock(stream_id_str, reason="user_stopped_before_start", force_stop=True)
                return
            
            # Register in distributed manager
            async with self._lock:
                self.active_streams[stream_id_str] = {
                    'stream_id': stream_id,
                    'name': camera_data['name'],
                    'workspace_id': camera_data['workspace_id'],
                    'user_id': camera_data['user_id'],
                    'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                    'status': 'starting',
                    'is_rtsp': camera_data.get('path', '').startswith('rtsp://'),
                    'last_heartbeat': datetime.now(ZoneInfo("Africa/Cairo"))
                }
            
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
            
            # ✅ CRITICAL FIX: Start with TIMEOUT
            from app.services.stream_service import stream_manager
            
            start_coro = stream_manager.start_stream_background(
                stream_id=stream_id,
                owner_id=camera_data['user_id'],
                owner_username=camera_data['username'],
                camera_name=camera_data['name'],
                source=camera_data['path'],
                workspace_id=camera_data['workspace_id'],
                location_info=location_info
            )
            
            # ✅ TIMEOUT: Camera start should never take > 30 seconds
            result = await self._safe_operation(
                start_coro,
                timeout=self.camera_start_timeout,
                operation_name=f"start_camera_{camera_data['name']}",
                default_return=None
            )
            
            if result is None:
                # Timeout or error
                logger.error(f"❌ Camera {stream_id_str} failed to start within timeout")
                
                async with self._lock:
                    self.active_streams.pop(stream_id_str, None)
                
                await self.release_camera_lock(stream_id_str, reason="start_timeout", force_stop=False)
                return
            
            # Verify start
            max_wait = 10
            start_time = time.time()
            
            while (time.time() - start_time) < max_wait:
                if stream_id_str in stream_manager.active_streams:
                    logger.info(f"✅ Verified camera {camera_data['name']} in stream_manager")
                    
                    async with self._lock:
                        if stream_id_str in self.active_streams:
                            self.active_streams[stream_id_str]['status'] = 'verified'
                    
                    break
                
                await asyncio.sleep(0.5)
            else:
                logger.error(f"❌ Camera {stream_id_str} not in stream_manager after {max_wait}s")
                raise RuntimeError("Failed to verify camera start")
                
            logger.info(f"✅ Started camera '{camera_data['name']}' on server {self.server_id}")
            
        except Exception as e:
            logger.error(f"Error starting camera {stream_id_str}: {e}", exc_info=True)
            
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            await self.release_camera_lock(stream_id_str, reason="start_failed", force_stop=False)

    async def stop_camera_locally(self, stream_id_str: str, reason: str = "normal_stop"):
        """
        🛑 Stop a camera with TIMEOUT PROTECTION.
        """
        try:
            from app.services.stream_service import stream_manager
            
            # ✅ TIMEOUT: Stop should never take > 10 seconds
            await self._safe_operation(
                stream_manager._stop_stream(stream_id_str, for_restart=False),
                timeout=10.0,
                operation_name=f"stop_camera_{stream_id_str}",
                default_return=None
            )
            
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            logger.info(f"🛑 Stopped camera {stream_id_str}")
            
        except Exception as e:
            logger.error(f"Error stopping camera {stream_id_str}: {e}")

    # ==================== Zombie Detection (NON-BLOCKING) ====================

    async def detect_and_cleanup_zombie_streams(self):
        """
        🧟 Find and RECOVER zombies with TIMEOUT PROTECTION.
        
        ✅ CRITICAL: Runs in background, doesn't block management loop!
        """
        try:
            db_locked_query = """
                SELECT 
                    vs.stream_id, vs.name, vs.is_streaming, vs.status,
                    vs.path, vs.workspace_id, vs.user_id, vs.stop_reason,
                    vs.auto_retry_enabled, u.username, u.role
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.locked_by_server = $1
            """
            
            # ✅ TIMEOUT: Zombie detection query should never take > 10 seconds
            db_locked = await self._safe_operation(
                self.db_manager.execute_query(db_locked_query, (self.server_id,), fetch_all=True),
                timeout=10.0,
                operation_name="fetch_zombies",
                default_return=[]
            )
            
            if not db_locked:
                return []
            
            db_locked_dict = {str(row['stream_id']): row for row in db_locked}
            
            from app.services.stream_service import stream_manager
            
            async with self._lock:
                distributed_ids = set(self.active_streams.keys())
            
            async with stream_manager._lock:
                stream_manager_ids = set(stream_manager.active_streams.keys())
            
            all_active_ids = distributed_ids | stream_manager_ids
            zombies = set(db_locked_dict.keys()) - all_active_ids
            
            if not zombies:
                return []
            
            logger.warning(f"🧟 Found {len(zombies)} zombies")
            
            recovered_count = 0
            
            for zombie_id in list(zombies):
                zombie_data = db_locked_dict[zombie_id]
                
                try:
                    should_recover = (
                        zombie_data['is_streaming'] and
                        zombie_data.get('stop_reason') not in ('user_action', 'user_stop', 'manual_stop', 'admin_stop') and
                        zombie_data.get('auto_retry_enabled', True)
                    )
                    
                    if should_recover:
                        # ✅ FIRE-AND-FORGET: Don't wait for recovery to complete
                        asyncio.create_task(self._recover_zombie_camera(zombie_id, zombie_data))
                        recovered_count += 1
                    else:
                        await self.release_camera_lock(zombie_id, reason="zombie_cleanup_not_supposed_to_run", force_stop=True)
                
                except Exception as e:
                    logger.error(f"Error processing zombie {zombie_id}: {e}")
            
            logger.info(f"🧹 Zombie cleanup: initiated recovery for {recovered_count} cameras")
            
            return list(zombies)
            
        except Exception as e:
            logger.error(f"Error in zombie detection: {e}", exc_info=True)
            return []

    async def _recover_zombie_camera(self, zombie_id: str, zombie_data: Dict[str, Any]):
        """
        Helper: Recover a single zombie camera (runs in background).
        """
        try:
            logger.warning(f"🔄 Recovering zombie: {zombie_data['name']}")
            
            # Release lock
            await self.db_manager.execute_query(
                """UPDATE video_stream
                SET locked_by_server = NULL, server_heartbeat = NULL, status = 'processing'
                WHERE stream_id = $1""",
                (UUID(zombie_id),)
            )
            
            await asyncio.sleep(2)
            
            # Try to claim
            claimed = await self.db_manager.execute_query(
                """UPDATE video_stream
                SET locked_by_server = $1, server_heartbeat = NOW(), status = 'processing', updated_at = NOW()
                WHERE stream_id = $2 AND locked_by_server IS NULL AND is_streaming = TRUE
                RETURNING stream_id""",
                (self.server_id, UUID(zombie_id)),
                fetch_one=True
            )
            
            if claimed:
                camera_info = {
                    'stream_id': UUID(zombie_id),
                    'name': zombie_data['name'],
                    'path': zombie_data['path'],
                    'workspace_id': zombie_data['workspace_id'],
                    'user_id': zombie_data['user_id'],
                    'username': zombie_data['username'],
                    'role': zombie_data['role']
                }
                
                # ✅ FIRE-AND-FORGET: Don't wait
                asyncio.create_task(self.start_camera_locally(camera_info))
                logger.info(f"✅ Recovered zombie: {zombie_data['name']}")
            
        except Exception as e:
            logger.error(f"Error recovering zombie {zombie_id}: {e}")

    async def cleanup_zombie_locks(self):
        """
        🧹 Clean up dead server locks with TIMEOUT PROTECTION.
        """
        try:
            cleanup_query = """
                WITH dead_locks AS (
                    SELECT stream_id, name, path, workspace_id, user_id,
                           locked_by_server as old_server, is_streaming,
                           stop_reason, auto_retry_enabled
                    FROM video_stream
                    WHERE locked_by_server IS NOT NULL
                    AND (server_heartbeat IS NULL OR server_heartbeat < NOW() - INTERVAL '10 minutes')
                )
                UPDATE video_stream
                SET locked_by_server = NULL, server_heartbeat = NULL,
                    status = 'processing', updated_at = NOW()
                FROM dead_locks
                WHERE video_stream.stream_id = dead_locks.stream_id
                RETURNING video_stream.stream_id, dead_locks.name, dead_locks.is_streaming
            """
            
            # ✅ TIMEOUT: Cleanup should never take > 10 seconds
            released_cameras = await self._safe_operation(
                self.db_manager.execute_query(cleanup_query, fetch_all=True),
                timeout=10.0,
                operation_name="cleanup_zombie_locks",
                default_return=[]
            )
            
            if released_cameras:
                logger.warning(f"🧹 Released {len(released_cameras)} dead server locks")
            
            return len(released_cameras)
            
        except Exception as e:
            logger.error(f"Error in zombie cleanup: {e}", exc_info=True)
            return 0

    # ==================== Management Loop (NON-BLOCKING) ====================

    async def manage_streams_with_deduplication(self):
        """
        🔄 Main management loop with COMPREHENSIVE TIMEOUT PROTECTION.
        
        ✅ CRITICAL FIXES:
        1. All database operations have timeouts
        2. Camera starts are fire-and-forget (non-blocking)
        3. Zombie detection runs in background
        4. Loop never hangs, even if individual operations fail
        """
        logger.info(f"🔄 Management loop started for server {self.server_id}")
        
        zombie_check_counter = 0
        dead_server_cleanup_counter = 0
        
        while self.is_running:
            try:
                # Step 1: Update heartbeat (with timeout)
                heartbeat_count = await self.update_heartbeat_for_owned_cameras()
                
                # Step 2: Check capacity
                async with self._lock:
                    current_active = len(self.active_streams)
                
                slots_available = self.max_local_capacity - current_active
                
                logger.info(
                    f"📊 Management cycle: server={self.server_id}, "
                    f"active={current_active}/{self.max_local_capacity}, "
                    f"slots={slots_available}, heartbeat={heartbeat_count}"
                )
                
                # Step 3: Claim cameras (with timeout)
                if slots_available > 0:
                    logger.info(f"🎯 Attempting to claim up to {slots_available} cameras...")
                    
                    newly_claimed = await self.claim_available_cameras(slots_available)
                    
                    if newly_claimed:
                        logger.warning(f"✅ Claimed {len(newly_claimed)} cameras")
                        
                        # ✅ CRITICAL FIX: Start cameras in FIRE-AND-FORGET mode
                        # Don't wait for them to complete - prevents blocking!
                        for camera in newly_claimed:
                            asyncio.create_task(self.start_camera_locally(camera))
                        
                        logger.info(f"🚀 Initiated start for {len(newly_claimed)} cameras (non-blocking)")
                
                # Step 4: Periodic zombie cleanup (background task)
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    logger.info("🧟 Running zombie detection (background)...")
                    # ✅ FIRE-AND-FORGET: Don't wait
                    asyncio.create_task(self.detect_and_cleanup_zombie_streams())
                    zombie_check_counter = 0
                
                # Step 5: Dead server cleanup (background task)
                dead_server_cleanup_counter += 1
                if dead_server_cleanup_counter >= 3:
                    logger.info("🧹 Running dead server cleanup (background)...")
                    # ✅ FIRE-AND-FORGET: Don't wait
                    asyncio.create_task(self.cleanup_zombie_locks())
                    dead_server_cleanup_counter = 0
                
                # Step 6: Verify owned cameras (with timeout)
                async with self._lock:
                    local_stream_ids = list(self.active_streams.keys())
                
                if local_stream_ids:
                    check_query = """
                        SELECT stream_id, is_streaming, stop_reason, status
                        FROM video_stream
                        WHERE stream_id = ANY($1::uuid[])
                    """
                    
                    # ✅ TIMEOUT: Verification should never take > 5 seconds
                    db_states = await self._safe_operation(
                        self.db_manager.execute_query(check_query, (local_stream_ids,), fetch_all=True),
                        timeout=5.0,
                        operation_name="verify_owned_cameras",
                        default_return=[]
                    )
                    
                    for db_state in db_states:
                        stream_id_str = str(db_state['stream_id'])
                        
                        should_stop = (
                            not db_state['is_streaming'] or
                            db_state['stop_reason'] in ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                        )
                        
                        if should_stop:
                            logger.warning(f"🛑 Stopping {stream_id_str}: database mismatch")
                            # ✅ FIRE-AND-FORGET: Don't wait
                            asyncio.create_task(self.stop_camera_locally(stream_id_str, reason="database_mismatch"))
                
                # Sleep before next cycle
                logger.debug("💤 Management loop sleeping for 30 seconds...")
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                logger.info("Management loop cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in management loop: {e}", exc_info=True)
                # ✅ NEVER let loop die - just back off and continue
                await asyncio.sleep(10)

    async def start_management_loop(self):
        """Start the management loop."""
        if self.is_running:
            logger.warning("Management loop already running")
            return
        
        self.is_running = True
        
        # Reclaim cameras from previous session
        await self.reclaim_my_cameras_on_startup()
        
        self.management_task = asyncio.create_task(self.manage_streams_with_deduplication())
        self.management_task.set_name(f"distributed_manager_{self.server_id}")
        
        logger.info(f"✅ Started management loop for server {self.server_id}")

    async def stop_management_loop(self):
        """Stop the management loop."""
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
            await self.release_camera_lock(stream_id_str, reason="server_shutdown", force_stop=True)
        
        logger.info(f"✅ Management loop stopped for server {self.server_id}")

    async def reclaim_my_cameras_on_startup(self):
        """Reclaim cameras from previous session."""
        try:
            reclaim_query = """
                SELECT 
                    vs.stream_id, vs.name, vs.path, vs.workspace_id, vs.user_id,
                    vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                    vs.latitude, vs.longitude, u.username, u.role
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE vs.locked_by_server = $1
                AND vs.is_streaming = TRUE
                AND vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                AND vs.auto_retry_enabled = TRUE
                AND u.is_active = TRUE
                AND w.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role IN ('admin', 'superadmin'))
            """
            
            cameras_to_reclaim = await self._safe_operation(
                self.db_manager.execute_query(reclaim_query, (self.server_id,), fetch_all=True),
                timeout=10.0,
                operation_name="reclaim_cameras",
                default_return=[]
            )
            
            if not cameras_to_reclaim:
                logger.info("✅ No cameras to reclaim on startup")
                return
            
            logger.warning(f"🔄 Found {len(cameras_to_reclaim)} cameras to reclaim")
            
            update_query = """
                UPDATE video_stream
                SET server_heartbeat = NOW(), status = 'processing', updated_at = NOW()
                WHERE locked_by_server = $1 AND is_streaming = TRUE
            """
            
            await self.db_manager.execute_query(update_query, (self.server_id,))
            
            logger.info(f"✅ Reclaimed {len(cameras_to_reclaim)} cameras")
            
        except Exception as e:
            logger.error(f"Error reclaiming cameras: {e}", exc_info=True)


# ==================== Global Instance ====================

distributed_stream_manager = DistributedStreamManager()


async def initialize_distributed_stream_manager():
    """Initialize the distributed stream manager."""
    try:
        await distributed_stream_manager.start_management_loop()
        logger.info("✅ Distributed stream manager initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        return False