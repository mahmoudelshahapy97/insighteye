# app/services/distributed_stream_manager.py

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
        self.rtsp_grace_period_seconds = config.rtsp_grace_period_seconds  
        self.file_grace_period_seconds = config.file_grace_period_seconds  
        
        logger.info(
            f"🚀 DistributedStreamManager initialized: "
            f"server_id={self.server_id}, capacity={self.max_local_capacity}"
        )

    # ==================== Atomic Camera Claiming ====================

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        🔒 ATOMIC camera claiming with comprehensive validation.
        
        FIXED: Now claims cameras with status='active' OR 'processing'
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
                    CASE WHEN vs.path LIKE 'rtsp://%' THEN TRUE ELSE FALSE END as is_rtsp,
                    EXTRACT(EPOCH FROM (NOW() - vs.last_activity)) as seconds_since_activity
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE 
                    -- ✅ Camera should be streaming
                    vs.is_streaming = TRUE
                    AND u.is_active = TRUE
                    AND w.is_active = TRUE
                    AND (u.is_subscribed = TRUE OR u.role = 'admin')
                    
                    -- ✅ CRITICAL: Not user-stopped
                    AND (vs.stop_reason IS NULL OR vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
                    
                    -- ✅ CRITICAL: Auto-retry must be enabled
                    AND vs.auto_retry_enabled = TRUE
                    
                    -- ✅ CRITICAL: Respect retry timing
                    AND (vs.next_retry_at IS NULL OR vs.next_retry_at <= NOW())
                    
                    -- ✅ FIX: Accept both 'active' and 'processing' status
                    -- (status='active' means camera WAS running before restart)
                    AND vs.status IN ('active', 'processing', 'error')
                    
                    -- ✅ CRITICAL: Enhanced locking logic with health checks
                    AND (
                        -- Case 1: Not locked (freshly enabled or after zombie cleanup)
                        vs.locked_by_server IS NULL
                        
                        OR
                        
                        -- Case 2: Dead server (no heartbeat)
                        (
                            vs.locked_by_server IS NOT NULL
                            AND (
                                vs.server_heartbeat IS NULL
                                OR
                                CASE 
                                    WHEN vs.path LIKE 'rtsp://%' THEN
                                        vs.server_heartbeat < NOW() - INTERVAL '5 minutes'
                                    ELSE
                                        vs.server_heartbeat < NOW() - INTERVAL '2 minutes'
                                END
                            )
                        )
                        
                        OR
                        
                        -- Case 3: Server alive but stream unhealthy
                        (
                            vs.locked_by_server IS NOT NULL
                            AND vs.server_heartbeat > NOW() - INTERVAL '5 minutes'
                            AND
                            CASE 
                                WHEN vs.path LIKE 'rtsp://%' THEN
                                    vs.last_activity < NOW() - INTERVAL '5 minutes'
                                ELSE
                                    vs.last_activity < NOW() - INTERVAL '2 minutes'
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
                updated_at = NOW(),
                last_activity = NOW()
            FROM available_cameras
            WHERE video_stream.stream_id = available_cameras.stream_id
            -- ✅ CRITICAL: Re-verify is_streaming hasn't changed
            AND video_stream.is_streaming = TRUE
            AND (video_stream.stop_reason IS NULL OR video_stream.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
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
                available_cameras.seconds_since_activity,
                video_stream.status as new_status
        """
        
        try:
            claimed_cameras = await self.db_manager.execute_query(
                claim_query,
                (slots_available, self.server_id),
                fetch_all=True
            )
            
            if not claimed_cameras:
                # ✅ DEBUG: Log why no cameras were claimed
                debug_query = """
                    SELECT 
                        stream_id,
                        name,
                        path,
                        is_streaming,
                        status,
                        locked_by_server,
                        stop_reason,
                        auto_retry_enabled,
                        next_retry_at,
                        AGE(NOW(), server_heartbeat) as heartbeat_age
                    FROM video_stream
                    WHERE is_streaming = TRUE
                    LIMIT 10
                """
                
                debug_result = await self.db_manager.execute_query(
                    debug_query,
                    fetch_all=True
                )
                
                if debug_result:
                    # Filter out healthy local streams
                    problematic_streams = []
                    healthy_local_streams = 0
                    
                    for cam in debug_result:
                        is_local = cam['locked_by_server'] == self.server_id
                        
                        # Calculate heartbeat age
                        heartbeat_age = timedelta(seconds=0)
                        if cam.get('heartbeat_age'):
                            try:
                                # Convert interval to timedelta if needed
                                heartbeat_age = cam['heartbeat_age']
                            except:
                                pass
                                
                        # Check if healthy: local + fresh heartbeat
                        is_rtsp = cam.get('path', '').startswith('rtsp://')
                        max_age = timedelta(minutes=5) if is_rtsp else timedelta(minutes=2)
                        
                        is_healthy = is_local and heartbeat_age < max_age
                        
                        if is_healthy:
                            healthy_local_streams += 1
                        else:
                            problematic_streams.append(cam)
                    
                    if healthy_local_streams > 0:
                        logger.info(
                            f"ℹ️ Skipping claim for {healthy_local_streams} healthy local streams "
                            f"(already locked by this server)"
                        )
                        
                    if problematic_streams:
                        logger.warning(
                            f"⚠️ No cameras claimed but {len(problematic_streams)} cameras are is_streaming=TRUE:"
                        )
                        for cam in problematic_streams:
                            logger.warning(
                                f"  - {cam['name']}: "
                                f"locked_by={cam['locked_by_server']}, "
                                f"status={cam['status']}, "
                                f"stop_reason={cam['stop_reason']}, "
                                f"auto_retry={cam['auto_retry_enabled']}, "
                                f"heartbeat_age={cam.get('heartbeat_age')}"
                            )
                
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
                old_status = camera.get('new_status', 'unknown')
                
                if retry_count > 0:
                    logger.info(
                        f"✅ Claimed {stream_type} camera '{camera['name']}' "
                        f"(retry #{retry_count}, was status='{old_status}', "
                        f"inactive for {activity_age:.0f}s)"
                    )
                else:
                    logger.info(
                        f"✅ Claimed {stream_type} camera '{camera['name']}' "
                        f"(first start or after restart, was status='{old_status}')"
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
        
        CRITICAL FIX: Now actually updates heartbeats!
        """
        async with self._lock:
            owned_stream_ids = list(self.active_streams.keys())
        
        if not owned_stream_ids:
            return 0
        
        # ✅ CRITICAL: Convert to UUIDs for database query
        try:
            owned_stream_uuids = [UUID(sid) for sid in owned_stream_ids]
        except Exception as e:
            logger.error(f"Error converting stream IDs to UUIDs: {e}")
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
        
        try:
            result = await self.db_manager.execute_query(
                heartbeat_query,
                (owned_stream_uuids, self.server_id),
                fetch_all=True
            )
            
            rows_updated = len(result) if result else 0
            
            if rows_updated > 0:
                logger.debug(f"💓 Updated heartbeat for {rows_updated} cameras")
            elif len(owned_stream_ids) > 0:
                # ⚠️ WARNING: We have cameras in memory but couldn't update heartbeat
                logger.error(
                    f"❌ CRITICAL: Expected to update {len(owned_stream_ids)} cameras, "
                    f"but only updated {rows_updated}. Investigating..."
                )
                
                # ✅ DEBUG: Check which cameras failed
                for stream_id_str in owned_stream_ids:
                    check_query = """
                        SELECT locked_by_server, is_streaming, status
                        FROM video_stream
                        WHERE stream_id = $1
                    """
                    check = await self.db_manager.execute_query(
                        check_query, (UUID(stream_id_str),), fetch_one=True
                    )
                    
                    if check:
                        logger.error(
                            f"  - {stream_id_str}: locked_by={check['locked_by_server']}, "
                            f"is_streaming={check['is_streaming']}, status={check['status']}"
                        )
                    else:
                        logger.error(f"  - {stream_id_str}: NOT FOUND IN DATABASE!")
            
            return rows_updated
            
        except Exception as e:
            logger.error(f"Error updating heartbeat: {e}", exc_info=True)
            return 0

    async def detect_and_cleanup_zombie_streams(self):
        """
        🧟 Find and RECOVER cameras locked by this server but not running.
        
        ✅ ENHANCED: Automatic recovery instead of just cleanup
        """
        db_locked_query = """
            SELECT 
                vs.stream_id, 
                vs.name, 
                vs.is_streaming, 
                vs.status, 
                vs.last_activity,
                vs.path,
                vs.workspace_id,
                vs.user_id,
                vs.stop_reason,
                vs.auto_retry_enabled,
                u.username,
                u.role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.locked_by_server = $1
        """
        
        db_locked = await self.db_manager.execute_query(
            db_locked_query,
            (self.server_id,),
            fetch_all=True
        )
        
        if not db_locked:
            return []
        
        db_locked_dict = {str(row['stream_id']): row for row in db_locked}
        
        # ✅ Check BOTH managers
        from app.services.stream_service import stream_manager
        
        async with self._lock:
            distributed_ids = set(self.active_streams.keys())
        
        async with stream_manager._lock:
            stream_manager_ids = set(stream_manager.active_streams.keys())
        
        # A camera is zombie ONLY if missing from BOTH
        all_active_ids = distributed_ids | stream_manager_ids
        zombies = set(db_locked_dict.keys()) - all_active_ids
        
        if not zombies:
            return []
        
        zombie_info = [
            f"{db_locked_dict[z]['name']} (status={db_locked_dict[z]['status']})"
            for z in zombies
        ]
        logger.warning(
            f"🧟 Found {len(zombies)} TRUE zombies (missing from BOTH managers): "
            f"{zombie_info}"
        )
        
        # ✅ CRITICAL: RECOVER zombies instead of just cleaning
        recovered_count = 0
        released_count = 0
        
        for zombie_id in list(zombies):
            zombie_data = db_locked_dict[zombie_id]
            
            try:
                # Check if this camera SHOULD be running
                should_recover = (
                    zombie_data['is_streaming'] and
                    zombie_data.get('stop_reason') not in ('user_action', 'user_stop', 'manual_stop', 'admin_stop') and
                    zombie_data.get('auto_retry_enabled', True)
                )
                
                if should_recover:
                    logger.warning(
                        f"🔄 Zombie {zombie_data['name']} should be running, "
                        f"attempting automatic recovery..."
                    )
                    
                    # Release lock so we can reclaim it properly
                    await self.db_manager.execute_query(
                        """UPDATE video_stream
                        SET locked_by_server = NULL,
                            server_heartbeat = NULL,
                            status = 'processing'
                        WHERE stream_id = $1""",
                        (UUID(zombie_id),)
                    )
                    
                    # Wait a moment for lock release to propagate
                    await asyncio.sleep(2)
                    
                    # Try to claim it
                    claim_query = """
                        UPDATE video_stream
                        SET 
                            locked_by_server = $1,
                            server_heartbeat = NOW(),
                            status = 'processing',
                            updated_at = NOW()
                        WHERE stream_id = $2
                        AND locked_by_server IS NULL
                        AND is_streaming = TRUE
                        RETURNING stream_id
                    """
                    
                    claimed = await self.db_manager.execute_query(
                        claim_query,
                        (self.server_id, UUID(zombie_id)),
                        fetch_one=True
                    )
                    
                    if claimed:
                        logger.info(f"✅ Re-claimed zombie camera: {zombie_data['name']}")
                        
                        # Start it
                        try:
                            camera_info = {
                                'stream_id': UUID(zombie_id),
                                'name': zombie_data['name'],
                                'path': zombie_data['path'],
                                'workspace_id': zombie_data['workspace_id'],
                                'user_id': zombie_data['user_id'],
                                'username': zombie_data['username'],
                                'role': zombie_data['role']
                            }
                            
                            # Start in background
                            asyncio.create_task(
                                self.start_camera_locally(camera_info)
                            )
                            
                            recovered_count += 1
                            logger.info(f"✅ Recovered zombie camera: {zombie_data['name']}")
                            
                        except Exception as start_err:
                            logger.error(
                                f"Error recovering zombie {zombie_data['name']}: {start_err}"
                            )
                            # Release lock if start failed
                            await self.release_camera_lock(
                                zombie_id,
                                reason="recovery_failed",
                                force_stop=False
                            )
                    else:
                        logger.warning(
                            f"⚠️ Could not reclaim zombie {zombie_data['name']} "
                            f"(claimed by another server)"
                        )
                else:
                    # Not supposed to be running, just cleanup
                    logger.info(
                        f"🗑️ Zombie {zombie_data['name']} should NOT be running, "
                        f"releasing lock (is_streaming={zombie_data['is_streaming']}, "
                        f"stop_reason={zombie_data.get('stop_reason')})"
                    )
                    
                    await self.release_camera_lock(
                        zombie_id,
                        reason="zombie_cleanup_not_supposed_to_run",
                        force_stop=True
                    )
                    released_count += 1
            
            except Exception as e:
                logger.error(f"Error processing zombie {zombie_id}: {e}")
        
        logger.info(
            f"🧹 Zombie cleanup complete: "
            f"recovered={recovered_count}, released={released_count}"
        )
        
        return list(zombies)

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
                        stop_reason = $3,           
                        stopped_at = NOW(),         
                        auto_retry_enabled = FALSE,
                        updated_at = NOW()
                    WHERE stream_id = $1
                      AND locked_by_server = $2
                    RETURNING is_streaming, locked_by_server, status
                """
                result = await self.db_manager.execute_query(
                    release_query,
                    (UUID(stream_id_str), self.server_id, reason),
                    fetch_one=True
                )
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
        🎬 Start a camera with PROPER registration in BOTH managers.
        
        CRITICAL FIX: Ensures distributed_manager tracks what stream_manager starts
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
            
            # ✅ Build location info
            location_info = {
                'location': camera_data.get('location'),
                'area': camera_data.get('area'),
                'building': camera_data.get('building'),
                'zone': camera_data.get('zone'),
                'floor_level': camera_data.get('floor_level'),
                'latitude': camera_data.get('latitude'),
                'longitude': camera_data.get('longitude')
            }
            
            # ✅ CRITICAL FIX: Register in distributed manager BEFORE starting
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
            
            # Start actual processing (will register in stream_manager)
            from app.services.stream_service import stream_manager
            
            try:
                await stream_manager.start_stream_background(
                    stream_id=stream_id,
                    owner_id=camera_data['user_id'],
                    owner_username=camera_data['username'],
                    camera_name=camera_data['name'],
                    source=camera_data['path'],
                    workspace_id=camera_data['workspace_id'],
                    location_info=location_info
                )
                
                # ✅ CRITICAL: Verify it actually started in stream_manager
                max_wait = 10  # seconds
                start_time = time.time()
                
                while (time.time() - start_time) < max_wait:
                    if stream_id_str in stream_manager.active_streams:
                        logger.info(f"✅ Verified camera {camera_data['name']} in stream_manager")
                        
                        # Update distributed manager status
                        async with self._lock:
                            if stream_id_str in self.active_streams:
                                self.active_streams[stream_id_str]['status'] = 'verified'
                        
                        break
                    
                    await asyncio.sleep(0.5)
                else:
                    # Failed to verify
                    logger.error(f"❌ Camera {stream_id_str} not in stream_manager after {max_wait}s")
                    raise RuntimeError(f"Failed to verify camera start in stream_manager")
                    
                logger.info(f"✅ Started camera '{camera_data['name']}' on server {self.server_id}")
                
            except Exception as start_err:
                logger.error(f"Error starting camera {stream_id_str}: {start_err}", exc_info=True)
                
                # ✅ CRITICAL: Clean up distributed manager on failure
                async with self._lock:
                    self.active_streams.pop(stream_id_str, None)

                # Release lock on failure
                await self.release_camera_lock(
                    stream_id_str, 
                    reason="start_failed",
                    force_stop=False  # Keep is_streaming=TRUE for retry
                )
                raise
                
        except Exception as e:
            logger.error(f"Error in start_camera_locally: {e}", exc_info=True)
            
            # Ensure cleanup
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
        🧟 Find cameras locked by this server but not running in EITHER manager.
        
        ✅ FIXED: Checks BOTH distributed AND stream managers
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
        
        # ✅ FIX: Check BOTH managers
        from app.services.stream_service import stream_manager
        
        async with self._lock:
            distributed_ids = set(self.active_streams.keys())
        
        async with stream_manager._lock:
            stream_manager_ids = set(stream_manager.active_streams.keys())
        
        # A camera is zombie ONLY if missing from BOTH
        all_active_ids = distributed_ids | stream_manager_ids
        zombies = set(db_locked_ids.keys()) - all_active_ids
        
        if zombies:
            zombie_names = [db_locked_ids[z] for z in zombies]
            logger.warning(
                f"🧟 Found {len(zombies)} TRUE zombies (missing from BOTH managers): {zombie_names}"
            )
            
            # Also log any discrepancies for debugging
            only_in_distributed = distributed_ids - stream_manager_ids
            only_in_stream_manager = stream_manager_ids - distributed_ids
            
            if only_in_distributed:
                logger.warning(f"⚠️ Only in distributed: {only_in_distributed}")
            if only_in_stream_manager:
                logger.warning(f"⚠️ Only in stream_manager: {only_in_stream_manager}")
        
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
        
        ENHANCED: Added extensive debug logging
        """
        logger.info(f"🔄 Management loop started for server {self.server_id}")
        
        zombie_check_counter = 0
        dead_server_cleanup_counter = 0
        
        while self.is_running:
            try:
                # Step 1: Update heartbeat for cameras we own
                heartbeat_count = await self.update_heartbeat_for_owned_cameras()
                
                # Step 2: Check capacity
                async with self._lock:
                    current_active = len(self.active_streams)
                
                slots_available = self.max_local_capacity - current_active
                
                logger.info(
                    f"📊 Management cycle: "
                    f"server={self.server_id}, "
                    f"active={current_active}/{self.max_local_capacity}, "
                    f"slots_available={slots_available}, "
                    f"heartbeat_updated={heartbeat_count}"
                )
                
                # Step 3: Claim available cameras if we have capacity
                if slots_available > 0:
                    logger.info(
                        f"🎯 Attempting to claim up to {slots_available} cameras..."
                    )
                    
                    newly_claimed = await self.claim_available_cameras(slots_available)
                    
                    if newly_claimed:
                        logger.warning(
                            f"✅ Successfully claimed {len(newly_claimed)} cameras: "
                            f"{[c['name'] for c in newly_claimed]}"
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
                        
                        if success_count != len(newly_claimed):
                            logger.error(
                                f"⚠️ Started {success_count}/{len(newly_claimed)} cameras "
                                f"({len(newly_claimed) - success_count} failed)"
                            )
                            
                            # Log failures
                            for idx, result in enumerate(results):
                                if isinstance(result, Exception):
                                    logger.error(
                                        f"  ❌ Failed to start {newly_claimed[idx]['name']}: {result}"
                                    )
                        else:
                            logger.info(
                                f"✅ Successfully started all {success_count} cameras"
                            )
                    else:
                        logger.debug(
                            f"ℹ️ No cameras claimed (slots_available={slots_available})"
                        )
                else:
                    if zombie_check_counter % 12 == 0:  # Log every ~6 minutes
                        logger.warning(
                            f"⚠️ SERVER AT CAPACITY: {current_active}/{self.max_local_capacity} streams active. "
                            f"Cannot claim more cameras. "
                            f"Check MAX_LOCAL_STREAMS in .env or add more server instances."
                        )
                    else:
                        logger.debug(
                            f"ℹ️ At capacity: {current_active}/{self.max_local_capacity} cameras"
                        )
                
                # Step 5: Periodic zombie cleanup (every 5 cycles = ~2.5 minutes)
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    logger.info("🧟 Running zombie detection cycle...")
                    await self.cleanup_zombies()
                    zombie_check_counter = 0
                
                # Step 6: Cleanup dead server locks (every 3 cycles = ~1.5 minutes)
                dead_server_cleanup_counter += 1
                if dead_server_cleanup_counter >= 3:
                    logger.info("🧹 Running dead server lock cleanup...")
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
                            logger.warning(
                                f"🛑 Stopping {stream_id_str}: "
                                f"is_streaming={db_state['is_streaming']}, "
                                f"stop_reason={db_state['stop_reason']}"
                            )
                            await self.stop_camera_locally(stream_id_str, reason="database_mismatch")
                
                # Sleep before next cycle
                logger.debug(f"💤 Management loop sleeping for 30 seconds...")
                await asyncio.sleep(30)  # 30 second interval
                
            except asyncio.CancelledError:
                logger.info("Management loop cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in management loop: {e}", exc_info=True)
                await asyncio.sleep(10)  # Back off on error

    async def start_management_loop(self):
        """Start the distributed management loop."""
        if self.is_running:
            logger.warning("Management loop already running")
            return
        
        self.is_running = True

        # Reclaim cameras from previous session
        await self.reclaim_my_cameras_on_startup()

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
        """
        🧹 Clean up locks from dead servers AND immediately recover them.
        
        ✅ ENHANCED: Automatic recovery after releasing zombie locks
        """
        try:
            # Step 1: Release zombie locks
            cleanup_query = """
                WITH dead_locks AS (
                    SELECT 
                        stream_id,
                        name,
                        path,
                        workspace_id,
                        user_id,
                        locked_by_server as old_server,
                        is_streaming,
                        stop_reason,
                        auto_retry_enabled
                    FROM video_stream
                    WHERE locked_by_server IS NOT NULL
                    AND (
                        server_heartbeat IS NULL 
                        OR server_heartbeat < NOW() - INTERVAL '10 minutes'  -- ✅ Increased from 5 to 10 min
                    )
                )
                UPDATE video_stream
                SET 
                    locked_by_server = NULL,
                    server_heartbeat = NULL,
                    status = 'processing',
                    updated_at = NOW()
                FROM dead_locks
                WHERE video_stream.stream_id = dead_locks.stream_id
                RETURNING 
                    video_stream.stream_id,
                    dead_locks.name,
                    dead_locks.path,
                    dead_locks.workspace_id,
                    dead_locks.user_id,
                    dead_locks.old_server,
                    dead_locks.is_streaming,
                    dead_locks.stop_reason,
                    dead_locks.auto_retry_enabled
            """
            
            released_cameras = await self.db_manager.execute_query(
                cleanup_query,
                fetch_all=True
            )
            
            if not released_cameras:
                return 0
            
            logger.warning(
                f"🧹 Released {len(released_cameras)} zombie locks from dead servers"
            )
            
            # Step 2: Filter cameras that should be recovered
            cameras_to_recover = [
                cam for cam in released_cameras
                if cam['is_streaming'] and
                cam.get('stop_reason') not in ('user_action', 'user_stop', 'manual_stop', 'admin_stop') and
                cam.get('auto_retry_enabled', True)
            ]
            
            if not cameras_to_recover:
                logger.info("ℹ️ No cameras need recovery")
                return len(released_cameras)
            
            logger.info(
                f"🔄 Attempting to recover {len(cameras_to_recover)} cameras "
                f"from dead servers"
            )
            
            # Step 3: Check capacity
            async with self._lock:
                current_capacity = len(self.active_streams)
            
            slots_available = self.max_local_capacity - current_capacity
            
            if slots_available <= 0:
                logger.warning(
                    f"⚠️ No capacity to recover cameras "
                    f"({current_capacity}/{self.max_local_capacity})"
                )
                return len(released_cameras)
            
            # Step 4: Recover cameras
            cameras_to_claim = cameras_to_recover[:slots_available]
            
            claimed_count = 0
            for camera_data in cameras_to_claim:
                try:
                    # Wait briefly to ensure lock release propagated
                    await asyncio.sleep(0.5)
                    
                    # Try to claim this camera
                    claim_query = """
                        UPDATE video_stream
                        SET 
                            locked_by_server = $1,
                            server_heartbeat = NOW(),
                            status = 'processing',
                            updated_at = NOW()
                        WHERE stream_id = $2
                        AND locked_by_server IS NULL
                        AND is_streaming = TRUE
                        RETURNING stream_id, name
                    """
                    
                    result = await self.db_manager.execute_query(
                        claim_query,
                        (self.server_id, camera_data['stream_id']),
                        fetch_one=True
                    )
                    
                    if result:
                        logger.info(
                            f"✅ Recovered and claimed: {camera_data['name']} "
                            f"(was locked by {camera_data['old_server']})"
                        )
                        claimed_count += 1
                        
                        # Start the camera
                        try:
                            camera_info = {
                                'stream_id': camera_data['stream_id'],
                                'name': camera_data['name'],
                                'path': camera_data['path'],
                                'workspace_id': camera_data['workspace_id'],
                                'user_id': camera_data['user_id'],
                                'username': 'system',
                                'role': 'admin'
                            }
                            
                            # Start in background (don't wait)
                            asyncio.create_task(
                                self.start_camera_locally(camera_info)
                            )
                            
                        except Exception as start_err:
                            logger.error(
                                f"Error starting recovered camera {camera_data['name']}: {start_err}"
                            )
                    else:
                        logger.warning(
                            f"⚠️ Could not claim {camera_data['name']} "
                            f"(claimed by another server)"
                        )
                        
                except Exception as claim_err:
                    logger.error(
                        f"Error claiming camera {camera_data.get('name')}: {claim_err}"
                    )
            
            if claimed_count > 0:
                logger.warning(
                    f"✅ Successfully recovered {claimed_count} cameras from dead servers"
                )
            
            return len(released_cameras)
            
        except Exception as e:
            logger.error(f"Error in zombie cleanup: {e}", exc_info=True)
            return 0

    async def cleanup_zombie_locks_on_startup():
        """Clean up any zombie locks from previous crashed instances"""
        try:
            query = """
                UPDATE video_stream
                SET locked_by_server = NULL,
                    server_heartbeat = NULL,
                    status = 'processing'
                WHERE locked_by_server IS NOT NULL
                AND (
                    server_heartbeat IS NULL 
                    OR server_heartbeat < NOW() - INTERVAL '5 minutes'
                )
                AND is_streaming = TRUE
                RETURNING stream_id, name
            """
            
            result = await db_manager.execute_query(query, fetch_all=True)
            
            if result:
                logger.warning(
                    f"🧹 Cleaned up {len(result)} zombie locks on startup: "
                    f"{[r['name'] for r in result]}"
                )
            else:
                logger.info("✅ No zombie locks found on startup")
                
        except Exception as e:
            logger.error(f"Error cleaning zombie locks: {e}")

    async def reclaim_my_cameras_on_startup(self): 
        """ 
        🔄 Reclaim cameras that were locked by this server before restart.
        Called during startup to recover from unexpected shutdowns.
        """
        try:
            # Find cameras that:
            # 1. Were locked by this server_id
            # 2. Have is_streaming = TRUE
            # 3. Are not user-stopped
            
            reclaim_query = """
                SELECT 
                    vs.stream_id, vs.name, vs.path, vs.workspace_id, vs.user_id,
                    vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                    vs.latitude, vs.longitude,
                    u.username, u.role
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE vs.locked_by_server = $1
                AND vs.is_streaming = TRUE
                AND vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                AND vs.auto_retry_enabled = TRUE
                AND u.is_active = TRUE
                AND w.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
            """
            
            cameras_to_reclaim = await self.db_manager.execute_query(
                reclaim_query,
                (self.server_id,),
                fetch_all=True
            )
            
            if not cameras_to_reclaim:
                logger.info(f"✅ No cameras to reclaim on startup")
                return
            
            logger.warning(
                f"🔄 Found {len(cameras_to_reclaim)} cameras to reclaim from previous session"
            )
            
            # Update heartbeat to mark them as active
            update_query = """
                UPDATE video_stream
                SET server_heartbeat = NOW(),
                    status = 'processing',
                    updated_at = NOW()
                WHERE locked_by_server = $1
                AND is_streaming = TRUE
            """
            
            await self.db_manager.execute_query(update_query, (self.server_id,))
            
            # Log reclaimed cameras
            for camera in cameras_to_reclaim:
                logger.info(
                    f"🔄 Will reclaim: {camera['name']} "
                    f"(stream_id: {camera['stream_id']})"
                )
            
            logger.info(
                f"✅ Reclaimed {len(cameras_to_reclaim)} cameras. "
                f"They will be started by the management loop."
            )
            
        except Exception as e:
            logger.error(f"Error reclaiming cameras on startup: {e}", exc_info=True)

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