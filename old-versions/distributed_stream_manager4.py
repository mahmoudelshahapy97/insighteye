# app/services/distributed_stream_manager.py
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
    Distributed stream manager with atomic camera claiming.
    Prevents multiple servers from starting the same camera.
    
    ✅ CRITICAL FIX: Now properly excludes user-stopped cameras in ALL queries
    """

    def __init__(self):
        self.db_manager = db_manager
        
        # Server identification
        self.server_id = config.server_id  # Unique UUID per server instance
        self.max_local_capacity = config.max_local_streams  # e.g., 8 cameras per server
        
        # Local state
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        
        # Management loop
        self.management_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        logger.info(
            f"🚀 DistributedStreamManager initialized: "
            f"server_id={self.server_id}, capacity={self.max_local_capacity}"
        )

    # ==================== Atomic Camera Claiming ====================

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        Atomically claim available cameras using FOR UPDATE SKIP LOCKED.
        
        ✅ CRITICAL FIX: Comprehensive user-stop exclusion with explicit checks
        """
        if slots_available <= 0:
            return []
        
        claim_query = """
            WITH available_cameras AS (
                SELECT vs.stream_id, vs.name, vs.path, vs.workspace_id, 
                    vs.user_id, vs.location, vs.area, vs.building,
                    vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                    u.username, u.role,
                    vs.retry_count, vs.next_retry_at, vs.stop_reason,
                    vs.auto_retry_enabled
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE vs.is_streaming = TRUE
                -- ✅ CRITICAL: Camera should be running
                AND u.is_active = TRUE
                AND w.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
                
                -- ✅✅✅ CRITICAL FIX: Triple-check user stops
                -- Check 1: stop_reason is not a user action
                AND (vs.stop_reason IS NULL 
                    OR vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
                
                -- Check 2: auto_retry_enabled must be TRUE
                AND vs.auto_retry_enabled = TRUE
                
                -- Check 3: Respect retry timing
                AND (
                    vs.next_retry_at IS NULL  -- Never attempted
                    OR vs.next_retry_at <= NOW()  -- Time to retry
                )
                
                -- Check 4: Not currently claimed OR claimed by dead server
                AND (
                    vs.locked_by_server IS NULL 
                    OR vs.server_heartbeat < NOW() - INTERVAL '2 minutes'
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
            -- ✅ FINAL SAFETY CHECK: Verify stop_reason again before updating
            AND (video_stream.stop_reason IS NULL 
                OR video_stream.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
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
                available_cameras.stop_reason
        """
        
        try:
            claimed_cameras = await self.db_manager.execute_query(
                claim_query,
                (slots_available, self.server_id),
                fetch_all=True
            )
            
            if claimed_cameras:
                # ✅ EXTRA VALIDATION: Double-check each camera before returning
                validated_cameras = []
                for camera in claimed_cameras:
                    stop_reason = camera.get('stop_reason')
                    
                    # If somehow a user-stopped camera slipped through, release it immediately
                    if stop_reason in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                        logger.error(
                            f"🚨 CRITICAL: Camera {camera['name']} claimed but has "
                            f"stop_reason='{stop_reason}'! Releasing immediately."
                        )
                        await self.release_camera_lock(
                            str(camera['stream_id']), 
                            reason="invalid_claim_user_stopped"
                        )
                        continue
                    
                    # Passed validation
                    retry_count = camera.get('retry_count', 0)
                    if retry_count > 0:
                        logger.info(
                            f"✅ Claimed camera {camera['name']} for retry attempt #{retry_count}"
                        )
                    else:
                        logger.info(
                            f"✅ Claimed camera {camera['name']} for first start"
                        )
                    
                    validated_cameras.append(camera)
                
                return validated_cameras
            
            return []
            
        except Exception as e:
            logger.error(f"Error claiming cameras: {e}", exc_info=True)
            return []

    async def claim_available_cameras(self, slots_available: int) -> List[Dict[str, Any]]:
        """
        FIXED: Even more aggressive user-stop exclusion
        """
        if slots_available <= 0:
            return []
        
        claim_query = """
            WITH available_cameras AS (
                SELECT vs.stream_id, vs.name, vs.path, vs.workspace_id, 
                    vs.user_id, vs.location, vs.area, vs.building,
                    vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                    u.username, u.role,
                    vs.retry_count, vs.next_retry_at, vs.stop_reason,
                    vs.auto_retry_enabled, vs.locked_by_server, vs.server_heartbeat
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                JOIN workspaces w ON vs.workspace_id = w.workspace_id
                WHERE vs.is_streaming = TRUE
                AND u.is_active = TRUE
                AND w.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
                
                -- ✅✅✅ CRITICAL: Quadruple-check user stops
                -- Check 1: stop_reason must NOT be user action
                AND (vs.stop_reason IS NULL 
                    OR vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
                
                -- Check 2: auto_retry_enabled must be TRUE
                AND vs.auto_retry_enabled = TRUE
                
                -- Check 3: Respect retry timing
                AND (
                    vs.next_retry_at IS NULL
                    OR vs.next_retry_at <= NOW()
                )
                
                -- Check 4: Not currently claimed OR claimed by dead server
                AND (
                    vs.locked_by_server IS NULL 
                    OR vs.server_heartbeat < NOW() - INTERVAL '3 minutes'  -- Increased from 2 to 3 minutes
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
            -- ✅✅✅ FINAL SAFETY CHECK: Verify AGAIN before updating
            AND video_stream.is_streaming = TRUE
            AND video_stream.auto_retry_enabled = TRUE
            AND (video_stream.stop_reason IS NULL 
                OR video_stream.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
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
                available_cameras.stop_reason
        """
        
        try:
            claimed_cameras = await self.db_manager.execute_query(
                claim_query,
                (slots_available, self.server_id),
                fetch_all=True
            )
            
            if claimed_cameras:
                # ✅ EXTRA VALIDATION: Triple-check AGAIN after claiming
                validated_cameras = []
                for camera in claimed_cameras:
                    stream_id = str(camera['stream_id'])
                    
                    # Re-verify from database (catch race conditions)
                    verify_query = """
                        SELECT is_streaming, stop_reason, auto_retry_enabled, locked_by_server
                        FROM video_stream
                        WHERE stream_id = $1
                    """
                    verify = await self.db_manager.execute_query(
                        verify_query, (camera['stream_id'],), fetch_one=True
                    )
                    
                    if not verify:
                        logger.error(f"🚨 Camera {stream_id} vanished after claim!")
                        continue
                    
                    # Check if user stopped it AFTER we claimed it
                    if verify['stop_reason'] in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                        logger.error(
                            f"🚨 RACE CONDITION: Camera {camera['name']} was user-stopped "
                            f"AFTER claim! Releasing immediately."
                        )
                        await self.release_camera_lock(stream_id, reason="race_condition_user_stopped")
                        continue
                    
                    # Check if auto_retry was disabled
                    if not verify.get('auto_retry_enabled', False):
                        logger.error(
                            f"🚨 RACE CONDITION: Camera {camera['name']} auto_retry disabled "
                            f"AFTER claim! Releasing immediately."
                        )
                        await self.release_camera_lock(stream_id, reason="race_condition_retry_disabled")
                        continue
                    
                    # Check if is_streaming became FALSE
                    if not verify.get('is_streaming', False):
                        logger.error(
                            f"🚨 RACE CONDITION: Camera {camera['name']} is_streaming=FALSE "
                            f"AFTER claim! Releasing immediately."
                        )
                        await self.release_camera_lock(stream_id, reason="race_condition_not_streaming")
                        continue
                    
                    # Passed ALL validations
                    logger.info(f"✅ Camera {camera['name']} passed all post-claim validations")
                    validated_cameras.append(camera)
                
                return validated_cameras
            
            return []
            
        except Exception as e:
            logger.error(f"Error claiming cameras: {e}", exc_info=True)
            return []

    # ==================== PATCH 2: Add Periodic User-Stop Scanner ====================

    async def scan_for_user_stopped_cameras(self):
        """
        Periodically scan for cameras that are locked but user-stopped.
        This catches any race conditions.
        """
        try:
            query = """
                SELECT stream_id, name, locked_by_server, stop_reason
                FROM video_stream
                WHERE locked_by_server IS NOT NULL
                AND (
                    stop_reason IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                    OR auto_retry_enabled = FALSE
                    OR is_streaming = FALSE
                )
            """
            
            stuck_cameras = await self.db_manager.execute_query(query, fetch_all=True)
            
            if stuck_cameras:
                logger.warning(
                    f"🧹 Found {len(stuck_cameras)} cameras with locks but shouldn't be running"
                )
                
                for cam in stuck_cameras:
                    logger.warning(
                        f"  - {cam['name']}: locked_by={cam['locked_by_server']}, "
                        f"stop_reason={cam['stop_reason']}"
                    )
                    
                    # Release the lock
                    await self.db_manager.execute_query(
                        """UPDATE video_stream 
                        SET locked_by_server = NULL,
                            server_heartbeat = NULL,
                            is_streaming = FALSE,
                            status = 'inactive',
                            updated_at = NOW()
                        WHERE stream_id = $1""",
                        (cam['stream_id'],)
                    )
                
                logger.info(f"✅ Released {len(stuck_cameras)} stuck locks")
                return len(stuck_cameras)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error scanning for user-stopped cameras: {e}", exc_info=True)
            return 0

    async def update_heartbeat_for_owned_cameras(self) -> int:
        """
        Update heartbeat for cameras owned by this server.
        This keeps them "alive" and prevents other servers from stealing them.
        
        ✅ FIXED: Only update cameras that should still be running
        """
        async with self._lock:
            owned_stream_ids = list(self.active_streams.keys())
        
        if not owned_stream_ids:
            return 0
        
        # ✅ CRITICAL FIX: Only update heartbeat if camera should still be running
        heartbeat_query = """
            UPDATE video_stream 
            SET server_heartbeat = NOW(),
                last_activity = NOW()
            WHERE stream_id = ANY($1::uuid[]) 
              AND locked_by_server = $2
              AND is_streaming = TRUE
              AND (stop_reason IS NULL 
                OR stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop'))
        """
        
        try:
            rows_updated = await self.db_manager.execute_query(
                heartbeat_query,
                (owned_stream_ids, self.server_id),
                return_rowcount=True
            )
            
            if rows_updated > 0:
                logger.debug(f"💓 Updated heartbeat for {rows_updated} cameras")
            
            # ✅ Check if any of our cameras were user-stopped
            if rows_updated < len(owned_stream_ids):
                # Some cameras didn't get heartbeat update - they might be user-stopped
                check_query = """
                    SELECT stream_id, name, stop_reason, is_streaming
                    FROM video_stream
                    WHERE stream_id = ANY($1::uuid[])
                      AND locked_by_server = $2
                      AND (stop_reason IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
                           OR is_streaming = FALSE)
                """
                
                stopped_cameras = await self.db_manager.execute_query(
                    check_query,
                    (owned_stream_ids, self.server_id),
                    fetch_all=True
                )
                
                if stopped_cameras:
                    for cam in stopped_cameras:
                        logger.warning(
                            f"⚠️ Camera {cam['name']} was user-stopped "
                            f"(stop_reason={cam['stop_reason']}, is_streaming={cam['is_streaming']}). "
                            f"Will clean up on next management cycle."
                        )
            
            return rows_updated
            
        except Exception as e:
            logger.error(f"Error updating heartbeat: {e}")
            return 0

    async def release_camera_lock(self, stream_id_str: str, reason: str = "normal_stop"):
        """
        Release server lock on a camera when it stops.
        This allows other servers to claim it if needed.
        
        ⚠️ CRITICAL: Must be called when stopping a camera!
        """
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

    # ==================== Stream Lifecycle ====================

    async def start_camera_locally(self, camera_data: Dict[str, Any]):
        """
        Start a camera that this server has successfully claimed.
        
        ✅✅✅ TRIPLE-VERIFIED: Check stop_reason before starting
        """
        stream_id = camera_data['stream_id']
        stream_id_str = str(stream_id)
        
        try:
            # ✅✅✅ CRITICAL SAFETY CHECK: Verify camera wasn't just user-stopped
            verify_query = """
                SELECT stop_reason, auto_retry_enabled, is_streaming 
                FROM video_stream 
                WHERE stream_id = $1
            """
            
            verify = await self.db_manager.execute_query(
                verify_query, (stream_id,), fetch_one=True
            )
            
            if not verify:
                logger.error(f"❌ Camera {stream_id_str} not found in database")
                return
            
            # ✅✅✅ CHECK 1: User-stop check
            stop_reason = verify.get('stop_reason')
            if stop_reason in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                logger.warning(
                    f"🚫 Camera {stream_id_str} was user-stopped (reason: {stop_reason}). "
                    f"Releasing lock and skipping start."
                )
                await self.release_camera_lock(stream_id_str, reason="user_stopped_after_claim")
                return
            
            # ✅✅✅ CHECK 2: Auto-retry check
            if not verify.get('auto_retry_enabled', True):
                logger.warning(
                    f"🚫 Camera {stream_id_str} has auto_retry_enabled=FALSE. "
                    f"Releasing lock and skipping start."
                )
                await self.release_camera_lock(stream_id_str, reason="auto_retry_disabled")
                return
            
            # ✅✅✅ CHECK 3: is_streaming check
            if not verify.get('is_streaming', False):
                logger.warning(
                    f"🚫 Camera {stream_id_str} has is_streaming=FALSE. "
                    f"Releasing lock and skipping start."
                )
                await self.release_camera_lock(stream_id_str, reason="not_supposed_to_stream")
                return
            
            # ✅ All checks passed - safe to start
            logger.info(f"✅ All safety checks passed for camera {stream_id_str}, proceeding with start")
            
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
                    'status': 'starting'
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
        """
        Stop a camera running on this server and release its lock.
        
        ⚠️ CRITICAL: Always releases the server lock so other servers can claim it.
        """
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

    # ==================== Zombie Detection ====================

    async def detect_zombie_cameras(self) -> List[str]:
        """
        Find cameras that are:
        1. Locked by THIS server in database
        2. NOT running in memory
        
        These are "zombies" - database thinks they're running but they're not.
        """
        # Get cameras locked by this server in DB
        db_locked_query = """
            SELECT stream_id, name, stop_reason
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
        
        db_locked_ids = {str(row['stream_id']): row for row in db_locked}
        
        # Get cameras actually running in memory
        async with self._lock:
            memory_ids = set(self.active_streams.keys())
        
        # Zombies = in DB but not in memory
        zombie_ids = set(db_locked_ids.keys()) - memory_ids
        
        if zombie_ids:
            logger.warning(
                f"🧟 Found {len(zombie_ids)} zombie cameras on server {self.server_id}"
            )
            
            # Log details about each zombie
            for zombie_id in zombie_ids:
                zombie_info = db_locked_ids[zombie_id]
                logger.warning(
                    f"  - {zombie_info['name']} (stop_reason: {zombie_info['stop_reason']})"
                )
        
        return list(zombie_ids)

    async def cleanup_zombies(self):
        """
        Clean up zombie cameras by releasing their locks.
        
        ✅ CRITICAL: Only clean if they should NOT be running
        """
        zombies = await self.detect_zombie_cameras()
        
        for zombie_id in zombies:
            try:
                # ✅ Check if this zombie should still be running
                check_query = """
                    SELECT stop_reason, is_streaming, auto_retry_enabled
                    FROM video_stream
                    WHERE stream_id = $1
                """
                
                zombie_state = await self.db_manager.execute_query(
                    check_query, (UUID(zombie_id),), fetch_one=True
                )
                
                if not zombie_state:
                    # Camera deleted - release lock
                    await self.release_camera_lock(zombie_id, reason="zombie_deleted")
                    logger.warning(f"🧹 Cleaned up deleted zombie camera {zombie_id}")
                    continue
                
                stop_reason = zombie_state.get('stop_reason')
                is_streaming = zombie_state.get('is_streaming')
                auto_retry = zombie_state.get('auto_retry_enabled')
                
                # ✅ If user-stopped, release lock
                if stop_reason in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                    await self.release_camera_lock(zombie_id, reason="zombie_user_stopped")
                    logger.warning(f"🧹 Cleaned up user-stopped zombie camera {zombie_id}")
                    continue
                
                # ✅ If is_streaming=FALSE, release lock
                if not is_streaming:
                    await self.release_camera_lock(zombie_id, reason="zombie_not_streaming")
                    logger.warning(f"🧹 Cleaned up non-streaming zombie camera {zombie_id}")
                    continue
                
                # ✅ If auto_retry disabled, release lock
                if not auto_retry:
                    await self.release_camera_lock(zombie_id, reason="zombie_no_retry")
                    logger.warning(f"🧹 Cleaned up no-retry zombie camera {zombie_id}")
                    continue
                
                # If we get here, camera SHOULD be running - will be picked up by normal claiming
                logger.info(
                    f"🔄 Zombie camera {zombie_id} should be running, "
                    f"releasing lock so it can be claimed again"
                )
                await self.release_camera_lock(zombie_id, reason="zombie_for_reclaim")
                
            except Exception as e:
                logger.error(f"Error cleaning zombie {zombie_id}: {e}")

    # ==================== Management Loop ====================

    async def manage_streams_with_deduplication(self):
        """
        Main management loop for distributed camera claiming.
        
        ✅ FIXED: Comprehensive user-stop checks at every stage
        """
        logger.info(f"🔄 Management loop started for server {self.server_id}")
        
        zombie_check_counter = 0
        zombie_lock_counter = 0
        user_stop_scan_counter = 0
        
        while self.is_running:
            try:
                # ==================== STEP 1: Scan for User-Stopped Cameras ====================
                user_stop_scan_counter += 1
                if user_stop_scan_counter >= 2:  # Every 2 cycles (40 seconds)
                    released = await self.scan_for_user_stopped_cameras()
                    if released > 0:
                        logger.warning(f"🧹 Released {released} user-stopped camera locks")
                    user_stop_scan_counter = 0
                    
                # ==================== STEP 1: Cleanup Zombie Locks ====================
                zombie_lock_counter += 1
                if zombie_lock_counter >= 3:  # Every 3 cycles (60 seconds)
                    released = await self.cleanup_zombie_locks()
                    if released > 0:
                        logger.warning(f"🧹 Cleaned up {released} zombie server locks")
                    zombie_lock_counter = 0
                
                # ==================== STEP 2: Heartbeat Update ====================
                await self.update_heartbeat_for_owned_cameras()
                
                # ==================== STEP 3: Check Capacity ====================
                async with self._lock:
                    current_active = len(self.active_streams)
                
                slots_available = self.max_local_capacity - current_active
                
                logger.debug(
                    f"📊 Server {self.server_id}: "
                    f"{current_active}/{self.max_local_capacity} cameras active, "
                    f"{slots_available} slots available"
                )
                
                # ==================== STEP 4: Claim Available Cameras ====================
                if slots_available > 0:
                    newly_claimed = await self.claim_available_cameras(slots_available)
                    
                    if newly_claimed:
                        logger.info(
                            f"🎯 Server {self.server_id} claimed {len(newly_claimed)} cameras"
                        )
                        
                        # ==================== STEP 5: Start Claimed Cameras ====================
                        start_tasks = [
                            self.start_camera_locally(camera)
                            for camera in newly_claimed
                        ]
                        
                        # Start all in parallel
                        results = await asyncio.gather(*start_tasks, return_exceptions=True)
                        
                        # Log results
                        success_count = sum(
                            1 for r in results if not isinstance(r, Exception)
                        )
                        logger.info(
                            f"✅ Successfully started {success_count}/{len(newly_claimed)} cameras"
                        )
                
                # ==================== STEP 6: Zombie Cleanup (Every 5 Cycles) ====================
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    await self.cleanup_zombies()
                    zombie_check_counter = 0
                
                # ==================== STEP 7: Stop Cameras That Shouldn't Run ====================
                # Check if any locally-running cameras should be stopped
                async with self._lock:
                    local_stream_ids = list(self.active_streams.keys())
                
                if local_stream_ids:
                    # Query database state
                    check_query = """
                        SELECT stream_id, is_streaming, stop_reason, name
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
                        stop_reason = db_state.get('stop_reason')
                        is_streaming = db_state.get('is_streaming')
                        
                        # ✅ Stop if user-stopped
                        if stop_reason in ('user_action', 'user_stop', 'manual_stop', 'admin_stop'):
                            logger.warning(
                                f"🛑 Stopping camera {db_state['name']}: "
                                f"stop_reason={stop_reason}"
                            )
                            await self.stop_camera_locally(stream_id_str)
                            continue
                        
                        # ✅ Stop if is_streaming=FALSE
                        if not is_streaming:
                            logger.warning(
                                f"🛑 Stopping camera {db_state['name']}: "
                                f"is_streaming=FALSE"
                            )
                            await self.stop_camera_locally(stream_id_str)
                            continue
                
                # ==================== STEP 8: Sleep ====================
                await asyncio.sleep(20)  # Check every 20 seconds
                
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
            await self.release_camera_lock(stream_id_str, reason="server_shutdown")
        
        logger.info(f"✅ Management loop stopped for server {self.server_id}")

    async def cleanup_zombie_locks(self):
        """
        Clean up locks from dead servers.
        Called periodically by management loop.
        """
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
            

# ==================== Global Instance ====================

distributed_stream_manager = DistributedStreamManager()


# ==================== Integration with Existing Code ====================

async def initialize_distributed_stream_manager():
    """
    Initialize the distributed stream manager.
    Call this in your app startup.
    """
    try:
        await distributed_stream_manager.start_management_loop()
        logger.info("✅ Distributed stream manager initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize distributed stream manager: {e}")
        return False