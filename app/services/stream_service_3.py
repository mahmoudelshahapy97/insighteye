# app/services/stream_service.py
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import HTTPException, WebSocket
from starlette.websockets import WebSocketState

from app.services.database import db_manager
from app.services.fire_detection_service import fire_detection_service
from app.services.people_count_service import people_count_service
from app.services.notification_service import notification_service
from app.services.video_stream_service import video_stream_service
from app.services.parameter_service import parameter_service
  
from app.services.unified_data_service import unified_data_service as qdrant_service
from app.services.workspace_service import workspace_service
from app.services.shared_stream_service import video_file_manager
from app.services.stream_processing_service import stream_processing_service
from app.services.retry_service import retry_service
from app.config.settings import config
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)


class StreamState(Enum):
    """Stream lifecycle states."""
    STARTING = "starting"
    ACTIVE = "active"
    STOPPING = "stopping"
    ERROR = "error"
    INACTIVE = "inactive"


class WorkspaceQuotaExceeded(Exception):
    """Raised when workspace stream quota is exceeded."""
    pass

class StreamStatusBatcher:
    """Batches stream status updates to reduce database load"""
    
    def __init__(self, db_manager, batch_interval: float = 5.0):
        self.db_manager = db_manager
        self.batch_interval = batch_interval
        self.pending_updates: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the batch worker"""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._batch_worker())
            logger.info("StreamStatusBatcher started")
    
    async def stop(self):
        """Stop the batch worker"""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def queue_update(self, stream_id: UUID, status: str, is_streaming: bool):
        """Queue a status update"""
        async with self._lock:
            self.pending_updates[str(stream_id)] = {
                'status': status,
                'is_streaming': is_streaming,
                'updated_at': datetime.now(ZoneInfo("Africa/Cairo"))
            }
    
    async def _batch_worker(self):
        """Flush updates every N seconds"""
        while True:
            try:
                await asyncio.sleep(self.batch_interval)
                
                async with self._lock:
                    if not self.pending_updates:
                        continue
                    
                    updates = list(self.pending_updates.items())
                    self.pending_updates.clear()
                
                # Execute batch update
                try:
                    await self._execute_batch_update(updates)
                except Exception as e:
                    logger.error(f"Batch update failed: {e}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch worker: {e}")
                await asyncio.sleep(1)
    
    async def _execute_batch_update(self, updates: List[Tuple[str, Dict]]):
        """Single query to update multiple streams"""
        if not updates:
            return
        
        # Build VALUES list
        values_list = []
        for stream_id_str, data in updates:
            values_list.append(
                f"('{stream_id_str}'::uuid, '{data['status']}', {data['is_streaming']}, '{data['updated_at'].isoformat()}'::timestamptz)"
            )
        
        query = f"""
            UPDATE video_stream
            SET 
                status = batch.status,
                is_streaming = batch.is_streaming,
                updated_at = batch.updated_at,
                last_activity = batch.updated_at
            FROM (VALUES {','.join(values_list)}) AS batch(stream_id, status, is_streaming, updated_at)
            WHERE video_stream.stream_id = batch.stream_id
        """
        
        await self.db_manager.execute_query(query)
        logger.debug(f"✅ Batched {len(updates)} status updates")


class StreamManager:
    """
    Centralized stream manager with deep workspace integration.
    with better error handling, state management, and monitoring.
    """
    def __init__(self):
        # Core dependencies
        self.db_manager = db_manager
        self.video_file_manager = video_file_manager
        self.qdrant_service = qdrant_service
        self.retry_service = retry_service
        self.status_batcher = StreamStatusBatcher(self.db_manager, batch_interval=5.0)
        
        # Service dependencies
        self.fire_service = fire_detection_service
        self.people_service = people_count_service
        self.notification_service = notification_service
        self.video_stream_service = video_stream_service
        self.parameter_service = parameter_service
        self.workspace_service = workspace_service
        self.processing_service = stream_processing_service
        
        # locking with timeout support
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

        # Add retry monitoring task
        self.retry_task: Optional[asyncio.Task] = None
        
        # Stream state management with tracking
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_states: Dict[str, StreamState] = {}  # NEW: Explicit state tracking
        self.stream_processing_stats: Dict[str, Dict[str, Any]] = {}
        self.stream_errors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)  # NEW: Error tracking
        
        # Workspace-aware stream registry
        self.workspace_streams: Dict[str, Set[str]] = defaultdict(set)
        self.stream_workspaces: Dict[str, str] = {}
        
        # Notification management
        self.notification_subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        
        # Cooldown tracking with expiration
        self.people_count_notification_cooldowns: Dict[str, float] = {}
        self.people_count_cooldown_duration = config.get("people_count_cooldown_seconds", 300.0)
        
        # Fire detection tracking
        self.fire_detection_states: Dict[str, Dict[str, Any]] = {}
        self.fire_detection_frame_counts: Dict[str, int] = {}
        self.fire_cooldown_duration = config.get("fire_cooldown_seconds", 600.0)
        
        # Health check configuration
        self.last_healthcheck = datetime.now(ZoneInfo("Africa/Cairo"))
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        
        # Background tasks with monitoring
        self.background_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        self.monitor_task: Optional[asyncio.Task] = None  # NEW: Resource monitoring
        
        # Shared stream management (FIXED: proper cleanup)
        self._shared_stream_registry: Dict[str, Set[str]] = defaultdict(set)  # source -> stream_ids
        
        # Performance metrics (NEW)
        self.metrics = {
            'total_streams_started': 0,
            'total_streams_stopped': 0,
            'total_errors': 0,
            'avg_stream_duration': 0.0,
            'last_reset': datetime.now(ZoneInfo("Africa/Cairo"))
        }
        
        # Resource limits (NEW)
        self.max_concurrent_streams = config.get("max_concurrent_streams", 100)
        self.max_streams_per_workspace = config.get("max_streams_per_workspace", 50)
        
        # Initialize processing service
        self.processing_service.initialize(
            stream_manager=self,
            video_file_manager=self.video_file_manager,
            qdrant_service=self.qdrant_service
        )
        
        logging.info("StreamManager initialized with workspace integration")

    # ==================== Context Manager for Safe Operations ====================

    @asynccontextmanager
    async def _safe_stream_operation(self, stream_id_str: str, operation: str):
        """
        Context manager for safe stream operations with proper cleanup.
        NEW: Ensures consistent state even on errors.
        """
        try:
            yield
        except Exception as e:
            logger.error(f"Error during {operation} for stream {stream_id_str}: {e}", exc_info=True)
            await self._record_stream_error(stream_id_str, operation, str(e))
            
            # Update state to error
            async with self._state_lock:
                self.stream_states[stream_id_str] = StreamState.ERROR
            
            raise
        finally:
            # Ensure cleanup happens
            pass

    async def _record_stream_error(
        self, 
        stream_id_str: str, 
        operation: str, 
        error_msg: str,
        max_errors: int = 10
    ):
        """
        Record stream error with automatic pruning.
        NEW: Better error tracking and analysis.
        """
        error_record = {
            'timestamp': datetime.now(ZoneInfo("Africa/Cairo")),
            'operation': operation,
            'error': error_msg
        }
        
        self.stream_errors[stream_id_str].append(error_record)
        
        # Keep only recent errors
        if len(self.stream_errors[stream_id_str]) > max_errors:
            self.stream_errors[stream_id_str] = self.stream_errors[stream_id_str][-max_errors:]
        
        self.metrics['total_errors'] += 1

    # ==================== State Management ====================
    
    async def _transition_stream_state(
        self, 
        stream_id_str: str, 
        new_state: StreamState,
        force: bool = False
    ) -> bool:
        """
        Safely transition stream state with validation.
        NEW: Prevents invalid state transitions.
        """
        async with self._state_lock:
            current_state = self.stream_states.get(stream_id_str)
            
            # Define valid transitions
            valid_transitions = {
                None: {StreamState.STARTING},
                StreamState.STARTING: {StreamState.ACTIVE, StreamState.ERROR, StreamState.STOPPING},
                StreamState.ACTIVE: {StreamState.STOPPING, StreamState.ERROR},
                StreamState.STOPPING: {StreamState.INACTIVE, StreamState.ERROR},
                StreamState.ERROR: {StreamState.STOPPING, StreamState.INACTIVE},
                StreamState.INACTIVE: {StreamState.STARTING}
            }
            
            if not force and current_state not in valid_transitions:
                logger.warning(f"Unknown current state {current_state} for stream {stream_id_str}")
                return False
            
            if not force and new_state not in valid_transitions.get(current_state, set()):
                logger.warning(
                    f"Invalid state transition for stream {stream_id_str}: "
                    f"{current_state} -> {new_state}"
                )
                return False
            
            self.stream_states[stream_id_str] = new_state
            logger.debug(f"Stream {stream_id_str} state: {current_state} -> {new_state}")
            return True

    # ==================== Workspace Integration ====================

    async def validate_workspace_stream_access(
        self,
        user_id: UUID,
        stream_id: UUID,
        required_role: Optional[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Validate user access to a stream through workspace membership.
        Returns: (stream_info, workspace_membership_info)
        """
        # Get stream info
        stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        workspace_id = stream_info['workspace_id']
        
        # Check workspace membership and role
        membership_info = await self.workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=workspace_id,
            required_role=required_role
        )
        
        return stream_info, membership_info

    async def get_workspace_stream_limits(self, workspace_id: UUID) -> Dict[str, int]:
        """
        Get stream limits for a workspace with caching.
        FIXED: Now correctly counts active streams from database instead of memory.
        """
        # Fetch from database
        query = """
            SELECT 
                SUM(u.count_of_camera) as total_camera_limit,
                COUNT(DISTINCT u.user_id) as active_members,
                COUNT(DISTINCT CASE WHEN u.is_subscribed = TRUE OR u.role = 'admin' 
                    THEN u.user_id END) as subscribed_members
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.workspace_id = $1 AND u.is_active = TRUE
        """
        result = await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)
        
        if not result:
            return {
                "total_camera_limit": 0,
                "active_members": 0,
                "subscribed_members": 0,
                "current_active": 0,
                "available_slots": 0
            }
        
        # FIXED: Get current active streams from DATABASE, not memory
        # This ensures accuracy even when streams aren't in memory
        active_count_query = """
            SELECT COUNT(*) as count
            FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE
        """
        active_result = await self.db_manager.execute_query(
            active_count_query, (workspace_id,), fetch_one=True
        )
        active_count = active_result['count'] if active_result else 0
        
        total_limit = result.get('total_camera_limit', 0) or 0
        
        limits_data = {
            "total_camera_limit": total_limit,
            "active_members": result.get('active_members', 0),
            "subscribed_members": result.get('subscribed_members', 0),
            "current_active": active_count,
            "available_slots": max(0, total_limit - active_count)
        }
        
        return limits_data

    async def can_start_stream_in_workspace(
        self,
        workspace_id: UUID,
        user_id: UUID
    ) -> Tuple[bool, str]:
        """
        Check if a stream can be started with validation.
        ENHANCED: Better quota checking and error messages.
        """
        workspace_id_str = str(workspace_id)
        
        # Check global system limit
        total_active = len(self.active_streams)
        if total_active >= self.max_concurrent_streams:
            return False, f"System limit reached ({self.max_concurrent_streams} concurrent streams)"
        
        # Check workspace limit
        workspace_active = len(self.workspace_streams.get(workspace_id_str, set()))
        if workspace_active >= self.max_streams_per_workspace:
            return False, f"Workspace limit reached ({self.max_streams_per_workspace} streams)"
        
        # Get workspace quota limits
        limits = await self.get_workspace_stream_limits(workspace_id)
        
        if limits['available_slots'] <= 0:
            return False, (
                f"Workspace camera quota exhausted "
                f"({limits['current_active']}/{limits['total_camera_limit']}). "
                f"Contact workspace admin to increase quota."
            )
        
        # Check user's subscription
        user_query = """
            SELECT is_active, is_subscribed, role, count_of_camera 
            FROM users WHERE user_id = $1
        """
        user_info = await self.db_manager.execute_query(user_query, (user_id,), fetch_one=True)
        
        if not user_info:
            return False, "User not found"
        
        if not user_info['is_active']:
            return False, "User account is inactive"
        
        if not user_info['is_subscribed'] and user_info['role'] != 'admin':
            return False, "Subscription expired. Please renew to start streams."
        
        # Check user's personal limit within workspace
        user_active_query = """
            SELECT COUNT(*) as count FROM video_stream
            WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE
        """
        user_active = await self.db_manager.execute_query(
            user_active_query, (user_id, workspace_id), fetch_one=True
        )
        
        user_active_count = user_active['count'] if user_active else 0
        user_limit = user_info['count_of_camera']
        
        if user_info['role'] != 'admin' and user_active_count >= user_limit:
            return False, (
                f"Personal camera limit reached ({user_active_count}/{user_limit}). "
                f"Stop other cameras or upgrade subscription."
            )
        
        return True, "OK"
    
    async def get_workspace_active_streams(
        self,
        workspace_id: UUID,
        user_id: Optional[UUID] = None
    ) -> List[Dict[str, Any]]:
        """Get all active streams in a workspace with info."""
        workspace_id_str = str(workspace_id)
        stream_ids = self.workspace_streams.get(workspace_id_str, set())
        
        streams_info = []
        async with self._lock:
            for stream_id_str in stream_ids:
                if stream_id_str in self.active_streams:
                    stream_data = self.active_streams[stream_id_str].copy()
                    
                    # Add processing stats
                    if stream_id_str in self.stream_processing_stats:
                        stream_data['stats'] = self.stream_processing_stats[stream_id_str]
                    
                    # Add state
                    stream_data['state'] = self.stream_states.get(stream_id_str, StreamState.ACTIVE).value
                    
                    # Filter by user if specified
                    if user_id and stream_data.get('user_id') != user_id:
                        continue
                    
                    streams_info.append({
                        'stream_id': stream_id_str,
                        'camera_name': stream_data.get('camera_name'),
                        'status': stream_data.get('status'),
                        'state': stream_data['state'],
                        'owner_username': stream_data.get('username'),
                        'start_time': stream_data.get('start_time'),
                        'last_frame_time': stream_data.get('last_frame_time'),
                        'client_count': len(stream_data.get('clients', set())),
                        'stats': stream_data.get('stats', {})
                    })
        
        return streams_info

    async def get_workspace_stream_analytics(
        self,
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """
        Get comprehensive analytics for workspace streams.
        ENHANCED: Added error rates and performance metrics.
        """
        # Get workspace info
        workspace_info = await self.workspace_service.get_workspace_by_id(workspace_id)
        
        # Get stream limits
        limits = await self.get_workspace_stream_limits(workspace_id)
        
        # Get all streams (active and inactive)
        all_streams = await self.video_stream_service.get_workspace_streams(workspace_id)
        
        # Get active streams from memory
        active_streams = await self.get_workspace_active_streams(workspace_id)
        
        # Calculate statistics
        total_streams = len(all_streams)
        streaming_now = len(active_streams)
        
        # Stream types
        stream_types = defaultdict(int)
        for stream in all_streams:
            stream_types[stream.get('type', 'unknown')] += 1
        
        # Alert status
        alerts_enabled = sum(1 for s in all_streams if s.get('alert_enabled'))
        
        # Location distribution
        locations = defaultdict(int)
        for stream in all_streams:
            location = stream.get('location') or 'Unknown'
            locations[location] += 1
        
        # Processing performance
        total_frames = 0
        total_detections = 0
        total_errors = 0
        
        for stream in active_streams:
            stats = stream.get('stats', {})
            total_frames += stats.get('frames_processed', 0)
            total_detections += stats.get('detection_count', 0)
            total_errors += stats.get('errors', 0)
        
        # Error rates
        workspace_id_str = str(workspace_id)
        workspace_stream_ids = self.workspace_streams.get(workspace_id_str, set())
        error_count = sum(
            len(self.stream_errors.get(sid, []))
            for sid in workspace_stream_ids
        )
        
        return {
            'workspace': {
                'id': str(workspace_id),
                'name': workspace_info['name'],
                'is_active': workspace_info['is_active']
            },
            'limits': limits,
            'streams': {
                'total': total_streams,
                'streaming_now': streaming_now,
                'alerts_enabled': alerts_enabled,
                'types': dict(stream_types),
                'locations': dict(locations)
            },
            'performance': {
                'total_frames_processed': total_frames,
                'total_detections': total_detections,
                'total_errors': total_errors,
                'error_rate': (total_errors / total_frames * 100) if total_frames > 0 else 0,
                'avg_detections_per_stream': total_detections / streaming_now if streaming_now > 0 else 0,
                'recent_errors': error_count
            },
            'timestamp': datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }

    # ==================== Stream Lifecycle ====================

    # async def start_stream_background(
    #     self,
    #     stream_id: UUID,
    #     owner_id: UUID,
    #     owner_username: str,
    #     camera_name: str,
    #     source: str,
    #     workspace_id: UUID,
    #     location_info: Optional[Dict[str, Any]] = None
    # ):
    #     """
    #     Stream start with error handling and state management.
    #     FIXED: Verifies stream is actually processing before considering it "active"
    #     """
    #     stream_id_str = str(stream_id)
    #     workspace_id_str = str(workspace_id)

    #     logger.info(f"🎬 START REQUEST: stream={stream_id_str}, camera={camera_name}, "
    #         f"workspace={workspace_id_str}, source={source}")
        
    #     # Validate workspace access and limits
    #     try:
    #         can_start, reason = await self.can_start_stream_in_workspace(workspace_id, owner_id)
    #         if not can_start:
    #             logger.warning(f"Cannot start stream {stream_id_str}: {reason}")
    #             await self.notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="error",
    #                 message=f"Cannot start camera '{camera_name}': {reason}",
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
    #             raise WorkspaceQuotaExceeded(reason)
            
    #         logger.info(f"✅ QUOTA CHECK PASSED for stream {stream_id_str}")
            
    #     except Exception as e:
    #         logger.error(f"❌ VALIDATION ERROR: {e}", exc_info=True)
    #         raise
        
    #     async with self._safe_stream_operation(stream_id_str, "start"):
    #         async with self._lock:
    #             # ===== CRITICAL FIX: Verify stream health, not just state =====
    #             if stream_id_str in self.active_streams:
    #                 current_state = self.stream_states.get(stream_id_str)
    #                 logger.warning(f"⚠️ Stream {stream_id_str} already registered in state: {current_state}")
                    
    #                 # Check if it's actually healthy
    #                 stream_info = self.active_streams[stream_id_str]
    #                 task = stream_info.get('task')
    #                 latest_frame = stream_info.get('latest_frame')
    #                 last_frame_time = stream_info.get('last_frame_time')
                    
    #                 # Calculate time since last frame
    #                 is_healthy = False
    #                 if last_frame_time:
    #                     age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame_time).total_seconds()
    #                     # Consider healthy if received frame in last 30 seconds
    #                     is_healthy = age < 30 and latest_frame is not None
                    
    #                 # Check if task is still running
    #                 task_alive = task and not task.done()
                    
    #                 if current_state == StreamState.ACTIVE and is_healthy and task_alive:
    #                     logger.info(
    #                         f"✅ Stream {stream_id_str} is truly active and healthy "
    #                         f"(last frame: {age:.1f}s ago)"
    #                     )
    #                     return  # Actually active and processing
    #                 else:
    #                     # Stream exists but is NOT healthy - force restart
    #                     logger.error(
    #                         f"🔧 Stream {stream_id_str} exists but is UNHEALTHY:\n"
    #                         f"  - State: {current_state}\n"
    #                         f"  - Has frames: {latest_frame is not None}\n"
    #                         f"  - Last frame: {age if last_frame_time else 'never':.1f}s ago\n"
    #                         f"  - Task alive: {task_alive}\n"
    #                         f"  - FORCING RESTART"
    #                     )
                        
    #                     # Clean up the broken stream
    #                     await self._stop_stream(stream_id_str, for_restart=True)
                        
    #                     # Small delay before restart
    #                     await asyncio.sleep(1.0)
                
    #             # ===== Continue with normal start process =====
    #             logger.info(f"🔄 Starting fresh stream instance for {stream_id_str}")
                
    #             # Transition to STARTING state
    #             await self._transition_stream_state(stream_id_str, StreamState.STARTING)
                
    #             # Register stream in workspace
    #             self.workspace_streams[workspace_id_str].add(stream_id_str)
    #             self.stream_workspaces[stream_id_str] = workspace_id_str
    #             logger.info(f"✅ Registered stream in workspace registry")
                
    #             # Initialize stream entry
    #             self.active_streams[stream_id_str] = {
    #                 'status': 'starting',
    #                 'task': None,
    #                 'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
    #                 'location_info': location_info or {},
    #                 'workspace_id': workspace_id,
    #                 'source': source,
    #                 'camera_name': camera_name,
    #                 'username': owner_username,
    #                 'user_id': owner_id,
    #                 'clients': set(),
    #                 'latest_frame': None,
    #                 'last_frame_time': None,
    #                 'last_heartbeat': datetime.now(ZoneInfo("Africa/Cairo"))
    #             }
    #             logger.info(f"✅ Initialized stream entry in active_streams")

    #             # Initialize fire detection state
    #             self.fire_detection_states[stream_id_str] = {
    #                 'status': 'no detection',
    #                 'last_detection_time': None,
    #                 'last_notification_time': None
    #             }
    #             self.fire_detection_frame_counts[stream_id_str] = 0
    #             logger.info(f"✅ Initialized fire detection state")

    #         try:
    #             # Clear stop_reason and retry fields
    #             logger.info(f"💾 Updating database: is_streaming=TRUE, clearing stop_reason")
    #             update_query = """
    #                 UPDATE video_stream
    #                 SET is_streaming = TRUE,
    #                     status = 'processing',
    #                     stop_reason = NULL,
    #                     stopped_at = NULL,
    #                     stopped_by = NULL,
    #                     retry_count = 0,
    #                     last_retry_at = NULL,
    #                     next_retry_at = NULL,
    #                     auto_retry_enabled = TRUE,
    #                     updated_at = NOW(),
    #                     last_activity = NOW()
    #                 WHERE stream_id = $1
    #             """
    #             await self.db_manager.execute_query(update_query, (stream_id,))
    #             logger.info(f"✅ Database updated successfully")

    #             # Verify the database update
    #             verify_query = """
    #                 SELECT is_streaming, status, stop_reason, retry_count 
    #                 FROM video_stream 
    #                 WHERE stream_id = $1
    #             """
    #             verify_result = await self.db_manager.execute_query(
    #                 verify_query, (stream_id,), fetch_one=True
    #             )
                            
    #             if not verify_result:
    #                 logger.error(f"❌ Stream {stream_id_str} not found in database!")
    #                 raise RuntimeError("Stream not found")
                
    #             logger.info(
    #                 f"📊 Database verification: is_streaming={verify_result['is_streaming']}, "
    #                 f"status={verify_result['status']}, stop_reason={verify_result['stop_reason']}"
    #             )
                
    #             if verify_result['stop_reason'] is not None:
    #                 logger.error(f"❌ CRITICAL: stop_reason was not cleared! Value: {verify_result['stop_reason']}")
    #                 raise RuntimeError("Failed to clear stop_reason")

    #             # Initialize processing stats
    #             self.stream_processing_stats[stream_id_str] = {
    #                 "frames_processed": 0,
    #                 "detection_count": 0,
    #                 "avg_processing_time": 0.0,
    #                 "last_updated": datetime.now(ZoneInfo("Africa/Cairo")),
    #                 "errors": 0
    #             }

    #             logger.info(f"✅ Initialized processing stats")

    #             # Ensure Qdrant collection
    #             logger.info(f"🗄️ Ensuring Qdrant collection for workspace {workspace_id_str}")
    #             try:
    #                 if self.qdrant_service:
    #                     await self.qdrant_service.ensure_workspace_collection(workspace_id)
    #                     logger.info(f"✅ Qdrant collection verified")
    #                 else:
    #                     logger.warning(f"⚠️ Qdrant service not initialized")
    #             except Exception as e:
    #                 logger.error(f"❌ Failed to ensure Qdrant collection: {e}")

    #             # Create stop event
    #             stop_event = asyncio.Event()
    #             logger.info(f"✅ Created stop event")

    #             # Create processing task
    #             task = asyncio.create_task(
    #                 self.processing_service.process_stream_with_sharing(
    #                     stream_id=stream_id,
    #                     camera_name=camera_name,
    #                     source=source,
    #                     owner_username=owner_username,
    #                     owner_id=owner_id,
    #                     workspace_id=workspace_id,
    #                     stop_event=stop_event,
    #                     location_info=location_info
    #                 )
    #             )
    #             task.set_name(f"process_stream_{stream_id_str}")
    #             task.add_done_callback(
    #                 lambda t: asyncio.create_task(
    #                     self._handle_stream_task_completion(stream_id_str, t)
    #                 )
    #             )
    #             logger.info(f"✅ Processing task created: {task.get_name()}")

    #             # Update stream info
    #             async with self._lock:
    #                 self.active_streams[stream_id_str].update({
    #                     'stop_event': stop_event,
    #                     'task': task,
    #                     'status': 'active_pending'
    #                 })
                
    #             # Register with shared stream manager
    #             self._shared_stream_registry[source].add(stream_id_str)
    #             logger.info(f"✅ Registered with shared stream manager")
                
    #             # Transition to ACTIVE state
    #             await self._transition_stream_state(stream_id_str, StreamState.ACTIVE)
    #             logger.info(f"✅ Transitioned to ACTIVE state")

    #             # Notify workspace members
    #             await self._notify_workspace_stream_started(workspace_id, camera_name, owner_username)
    #             logger.info(f"✅ Workspace members notified")
                
    #             # Update metrics
    #             self.metrics['total_streams_started'] += 1
    #             logger.info(f"✅ ✅ ✅ Stream {stream_id_str} SUCCESSFULLY STARTED")
                
    #             logger.info(f"✅ Stream {stream_id_str} started in workspace {workspace_id_str}")

    #         except Exception as e:
    #             logger.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
                
    #             # Cleanup on failure
    #             async with self._lock:
    #                 self.active_streams.pop(stream_id_str, None)
    #                 self.workspace_streams[workspace_id_str].discard(stream_id_str)
    #                 self.stream_workspaces.pop(stream_id_str, None)
                
    #             self.stream_processing_stats.pop(stream_id_str, None)
    #             self._shared_stream_registry[source].discard(stream_id_str)
                
    #             # Update state
    #             await self._transition_stream_state(stream_id_str, StreamState.ERROR, force=True)
                
    #             # Update database
    #             await self.video_stream_service.update_stream_status(
    #                 stream_id, 'error', is_streaming=True, last_activity=datetime.now(ZoneInfo("Africa/Cairo"))  
    #             )

    #             logger.warning(
    #                 f"⚠️ Stream {stream_id_str} encountered error but is_streaming=True "
    #                 f"to allow automatic restart by management loop"
    #             )
                
    #             raise

    async def start_stream_background(
        self,
        stream_id: UUID,
        owner_id: UUID,
        owner_username: str,
        camera_name: str,
        source: str,
        workspace_id: UUID,
        location_info: Optional[Dict[str, Any]] = None
    ):
        """
        Stream start with error handling and state management.
        FIXED: Proper string formatting for age calculation
        """
        stream_id_str = str(stream_id)
        workspace_id_str = str(workspace_id)

        logger.info(f"🎬 START REQUEST: stream={stream_id_str}, camera={camera_name}, "
            f"workspace={workspace_id_str}, source={source}")
        
        # Validate workspace access and limits
        try:
            can_start, reason = await self.can_start_stream_in_workspace(workspace_id, owner_id)
            if not can_start:
                logger.warning(f"Cannot start stream {stream_id_str}: {reason}")
                await self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=owner_id,
                    status="error",
                    message=f"Cannot start camera '{camera_name}': {reason}",
                    stream_id=stream_id,
                    camera_name=camera_name
                )
                raise WorkspaceQuotaExceeded(reason)
            
            logger.info(f"✅ QUOTA CHECK PASSED for stream {stream_id_str}")
            
        except Exception as e:
            logger.error(f"❌ VALIDATION ERROR: {e}", exc_info=True)
            raise
        
        async with self._safe_stream_operation(stream_id_str, "start"):
            async with self._lock:
                # ===== CRITICAL FIX: Verify stream health, not just state =====
                if stream_id_str in self.active_streams:
                    current_state = self.stream_states.get(stream_id_str)
                    logger.warning(f"⚠️ Stream {stream_id_str} already registered in state: {current_state}")
                    
                    # Check if it's actually healthy
                    stream_info = self.active_streams[stream_id_str]
                    task = stream_info.get('task')
                    latest_frame = stream_info.get('latest_frame')
                    last_frame_time = stream_info.get('last_frame_time')
                    
                    # Calculate time since last frame
                    is_healthy = False
                    age_str = "never"  # Default string
                    
                    if last_frame_time:
                        age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame_time).total_seconds()
                        age_str = f"{age:.1f}s"  # ✅ FIX: Format age separately
                        # Consider healthy if received frame in last 30 seconds
                        is_healthy = age < 30 and latest_frame is not None
                    
                    # Check if task is still running
                    task_alive = task and not task.done()
                    
                    if current_state == StreamState.ACTIVE and is_healthy and task_alive:
                        logger.info(
                            f"✅ Stream {stream_id_str} is truly active and healthy "
                            f"(last frame: {age_str} ago)"  # ✅ Use pre-formatted string
                        )
                        return  # Actually active and processing
                    else:
                        # Stream exists but is NOT healthy - force restart
                        logger.error(
                            f"🔧 Stream {stream_id_str} exists but is UNHEALTHY:\n"
                            f"  - State: {current_state}\n"
                            f"  - Has frames: {latest_frame is not None}\n"
                            f"  - Last frame: {age_str} ago\n"  # ✅ FIX: Use pre-formatted string
                            f"  - Task alive: {task_alive}\n"
                            f"  - FORCING RESTART"
                        )
                        
                        # Clean up the broken stream
                        await self._stop_stream(stream_id_str, for_restart=True)
                        
                        # Small delay before restart
                        await asyncio.sleep(1.0)
                
                # ===== Continue with normal start process =====
                logger.info(f"🔄 Starting fresh stream instance for {stream_id_str}")
                
                # Transition to STARTING state
                await self._transition_stream_state(stream_id_str, StreamState.STARTING)
                
                # Register stream in workspace
                self.workspace_streams[workspace_id_str].add(stream_id_str)
                self.stream_workspaces[stream_id_str] = workspace_id_str
                logger.info(f"✅ Registered stream in workspace registry")
                
                # Initialize stream entry
                self.active_streams[stream_id_str] = {
                    'status': 'starting',
                    'task': None,
                    'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                    'location_info': location_info or {},
                    'workspace_id': workspace_id,
                    'source': source,
                    'camera_name': camera_name,
                    'username': owner_username,
                    'user_id': owner_id,
                    'clients': set(),
                    'latest_frame': None,
                    'last_frame_time': None,
                    'last_heartbeat': datetime.now(ZoneInfo("Africa/Cairo"))
                }
                logger.info(f"✅ Initialized stream entry in active_streams")

                # Initialize fire detection state
                self.fire_detection_states[stream_id_str] = {
                    'status': 'no detection',
                    'last_detection_time': None,
                    'last_notification_time': None
                }
                self.fire_detection_frame_counts[stream_id_str] = 0
                logger.info(f"✅ Initialized fire detection state")

            try:
                # Clear stop_reason and retry fields
                logger.info(f"💾 Updating database: is_streaming=TRUE, clearing stop_reason")
                update_query = """
                    UPDATE video_stream
                    SET is_streaming = TRUE,
                        status = 'processing',
                        stop_reason = NULL,
                        stopped_at = NULL,
                        stopped_by = NULL,
                        retry_count = 0,
                        last_retry_at = NULL,
                        next_retry_at = NULL,
                        auto_retry_enabled = TRUE,
                        updated_at = NOW(),
                        last_activity = NOW()
                    WHERE stream_id = $1
                """
                await self.db_manager.execute_query(update_query, (stream_id,))
                logger.info(f"✅ Database updated successfully")

                # Verify the database update
                verify_query = """
                    SELECT is_streaming, status, stop_reason, retry_count 
                    FROM video_stream 
                    WHERE stream_id = $1
                """
                verify_result = await self.db_manager.execute_query(
                    verify_query, (stream_id,), fetch_one=True
                )
                            
                if not verify_result:
                    logger.error(f"❌ Stream {stream_id_str} not found in database!")
                    raise RuntimeError("Stream not found")
                
                logger.info(
                    f"📊 Database verification: is_streaming={verify_result['is_streaming']}, "
                    f"status={verify_result['status']}, stop_reason={verify_result['stop_reason']}"
                )
                
                if verify_result['stop_reason'] is not None:
                    logger.error(f"❌ CRITICAL: stop_reason was not cleared! Value: {verify_result['stop_reason']}")
                    raise RuntimeError("Failed to clear stop_reason")

                # Initialize processing stats
                self.stream_processing_stats[stream_id_str] = {
                    "frames_processed": 0,
                    "detection_count": 0,
                    "avg_processing_time": 0.0,
                    "last_updated": datetime.now(ZoneInfo("Africa/Cairo")),
                    "errors": 0
                }

                logger.info(f"✅ Initialized processing stats")

                # Ensure Qdrant collection
                logger.info(f"🗄️ Ensuring Qdrant collection for workspace {workspace_id_str}")
                try:
                    if self.qdrant_service:
                        await self.qdrant_service.ensure_workspace_collection(workspace_id)
                        logger.info(f"✅ Qdrant collection verified")
                    else:
                        logger.warning(f"⚠️ Qdrant service not initialized")
                except Exception as e:
                    logger.error(f"❌ Failed to ensure Qdrant collection: {e}")

                # Create stop event
                stop_event = asyncio.Event()
                logger.info(f"✅ Created stop event")

                # Create processing task
                task = asyncio.create_task(
                    self.processing_service.process_stream_with_sharing(
                        stream_id=stream_id,
                        camera_name=camera_name,
                        source=source,
                        owner_username=owner_username,
                        owner_id=owner_id,
                        workspace_id=workspace_id,
                        stop_event=stop_event,
                        location_info=location_info
                    )
                )
                task.set_name(f"process_stream_{stream_id_str}")
                task.add_done_callback(
                    lambda t: asyncio.create_task(
                        self._handle_stream_task_completion(stream_id_str, t)
                    )
                )
                logger.info(f"✅ Processing task created: {task.get_name()}")

                # Update stream info
                async with self._lock:
                    self.active_streams[stream_id_str].update({
                        'stop_event': stop_event,
                        'task': task,
                        'status': 'active_pending'
                    })
                
                # Register with shared stream manager
                self._shared_stream_registry[source].add(stream_id_str)
                logger.info(f"✅ Registered with shared stream manager")
                
                # Transition to ACTIVE state
                await self._transition_stream_state(stream_id_str, StreamState.ACTIVE)
                logger.info(f"✅ Transitioned to ACTIVE state")

                # Notify workspace members
                await self._notify_workspace_stream_started(workspace_id, camera_name, owner_username)
                logger.info(f"✅ Workspace members notified")
                
                # Update metrics
                self.metrics['total_streams_started'] += 1
                logger.info(f"✅ ✅ ✅ Stream {stream_id_str} SUCCESSFULLY STARTED")
                
                logger.info(f"✅ Stream {stream_id_str} started in workspace {workspace_id_str}")

            except Exception as e:
                logger.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
                
                # Cleanup on failure
                async with self._lock:
                    self.active_streams.pop(stream_id_str, None)
                    self.workspace_streams[workspace_id_str].discard(stream_id_str)
                    self.stream_workspaces.pop(stream_id_str, None)
                
                self.stream_processing_stats.pop(stream_id_str, None)
                self._shared_stream_registry[source].discard(stream_id_str)
                
                # Update state
                await self._transition_stream_state(stream_id_str, StreamState.ERROR, force=True)
                
                # Update database
                await self.video_stream_service.update_stream_status(
                    stream_id, 'error', is_streaming=True, last_activity=datetime.now(ZoneInfo("Africa/Cairo"))  
                )

                logger.warning(
                    f"⚠️ Stream {stream_id_str} encountered error but is_streaming=True "
                    f"to allow automatic restart by management loop"
                )
                
                raise

    async def _handle_stream_task_completion(self, stream_id_str: str, task: asyncio.Task):
        """
        Handle stream task completion with proper cleanup.
        NEW: Ensures cleanup even on unexpected termination.
        """
        try:
            if task.cancelled():
                logger.info(f"Stream {stream_id_str} task was cancelled")
            elif task.exception():
                exc = task.exception()
                logger.error(f"Stream {stream_id_str} task failed: {exc}", exc_info=exc)
                await self._record_stream_error(stream_id_str, "task_execution", str(exc))
            else:
                logger.info(f"Stream {stream_id_str} task completed normally")
            
            # Ensure cleanup
            if stream_id_str in self.active_streams:
                await self._stop_stream(stream_id_str, for_restart=False)
                
        except Exception as e:
            logger.error(f"Error handling task completion for {stream_id_str}: {e}", exc_info=True)

    # ==================== Workspace Notifications ====================

    async def _notify_workspace_stream_started(
        self,
        workspace_id: UUID,
        camera_name: str,
        owner_username: str
    ):
        """Notify workspace members when a stream starts."""
        try:
            # Get workspace members
            members = await self.workspace_service.get_workspace_members(
                workspace_id=workspace_id,
                current_user_id=workspace_id,
                is_admin=True
            )
            
            # Create notifications for members
            notification_tasks = [
                self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"📹 Camera '{camera_name}' started by {owner_username}",
                    camera_name=camera_name
                )
                for member in members
            ]
            
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error notifying workspace of stream start: {e}")

    async def _notify_workspace_stream_stopped(
        self,
        workspace_id: UUID,
        camera_name: str,
        owner_username: str
    ):
        """Notify workspace members when a stream stops."""
        try:
            members = await self.workspace_service.get_workspace_members(
                workspace_id=workspace_id,
                current_user_id=workspace_id,
                is_admin=True
            )
            
            notification_tasks = [
                self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"⏹️ Camera '{camera_name}' stopped (owner: {owner_username})",
                    camera_name=camera_name
                )
                for member in members
            ]
            
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error notifying workspace of stream stop: {e}")

    # ==================== API Methods ====================

    async def start_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """Start stream with workspace validation."""
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None
        )
        
        workspace_id = stream_info['workspace_id']
        
        # Check if can start
        can_start, reason = await self.can_start_stream_in_workspace(
            workspace_id, requester_user_id
        )
        
        if not can_start:
            raise HTTPException(status_code=403, detail=reason)

        logger.info(f"✅ User {requester_user_id} requested start for stream {stream_id}")
        
        # Start the stream (which will update both memory AND database atomically)
        location_info = {
            'location': stream_info.get('location'),
            'area': stream_info.get('area'),
            'building': stream_info.get('building'),
            'zone': stream_info.get('zone'),
            'floor_level': stream_info.get('floor_level'),
            'latitude': stream_info.get('latitude'),
            'longitude': stream_info.get('longitude')
        }
        await self.start_stream_background(
            stream_id=stream_id,
            owner_id=stream_info['user_id'],
            owner_username=membership_info['username'],
            camera_name=stream_info['name'],
            source=stream_info['path'],
            workspace_id=workspace_id,
            location_info=location_info
        )

        return {
            "stream_id": str(stream_id),
            "name": stream_info['name'],
            "workspace_id": str(workspace_id),
            "message": "Stream start initiated (management loop will start processing)"
        }

    async def stop_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID,
        stop_reason: str = 'user_action',
        additional_context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Stop stream with proper database updates.
        
        CRITICAL FIX: Update database BEFORE stopping in memory to prevent race condition.
        """
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None
        )
        
        # Check permissions for user-initiated stops
        if stop_reason == 'user_action':
            is_owner = stream_info['user_id'] == requester_user_id
            is_workspace_admin = membership_info['role'] in ['admin', 'owner']
            
            if not is_owner and not is_workspace_admin:
                raise HTTPException(
                    status_code=403,
                    detail="Only stream owner or workspace admin can stop streams"
                )
        
        stream_id_str = str(stream_id)
        workspace_id = stream_info['workspace_id']
        
        logger.warning(
            f"🛑 STOP REQUESTED: stream={stream_id}, reason={stop_reason}, "
            f"requester={requester_user_id}, context={additional_context}"
        )
        
        # ===== CRITICAL FIX: Update DATABASE FIRST =====
        # This prevents management loop from seeing is_streaming=TRUE during the stop process
        
        if stop_reason == 'user_action':
            # User stop: NO retry, is_streaming=FALSE
            update_query = """
                UPDATE video_stream 
                SET is_streaming = FALSE,
                    status = 'inactive',
                    stop_reason = 'user_action',
                    stopped_by = $1,
                    stopped_at = NOW(),
                    retry_count = 0,
                    next_retry_at = NULL,
                    last_retry_at = NULL,
                    auto_retry_enabled = FALSE,
                    updated_at = NOW(),
                    last_activity = NOW()
                WHERE stream_id = $2
            """
            await self.db_manager.execute_query(update_query, (requester_user_id, stream_id))
                    
            # ✅ VERIFY the update worked
            verify = await self.db_manager.execute_query(
                """SELECT is_streaming, stop_reason, status 
                FROM video_stream 
                WHERE stream_id = $1""",
                (stream_id,),
                fetch_one=True
            )
            
            logger.info(
                f"✅ Stop verified: {stream_id_str} -> "
                f"is_streaming={verify['is_streaming']}, "
                f"status={verify['status']}, "
                f"stop_reason={verify['stop_reason']}"
            )
            
            if verify['is_streaming'] != False or verify['stop_reason'] != 'user_action':
                logger.error(
                    f"❌ DATABASE UPDATE FAILED! "
                    f"Expected: is_streaming=FALSE, stop_reason='user_action', "
                    f"Got: is_streaming={verify['is_streaming']}, stop_reason={verify['stop_reason']}"
                )
                raise RuntimeError("Database update verification failed")

            logger.info(f"✅ Database updated FIRST and VERIFIED: {stream_id_str}")

            await asyncio.sleep(0.1)

            # Cancel any pending retries (don't fail if this errors)
            try:
                await self.retry_service.cancel_retry(stream_id, "user_action")
            except Exception as retry_err:
                # Log but don't fail the stop operation
                logger.warning(
                    f"⚠️ Failed to cancel retry (non-critical): {retry_err}"
                )
                # The database update above already set retry_count=0 and next_retry_at=NULL
                # so the retry loop won't pick it up anyway
            
        else:
            # System error: Schedule retry, keep is_streaming=TRUE
            update_query = """
                UPDATE video_stream 
                SET is_streaming = TRUE,
                    status = 'error',
                    stop_reason = $1,
                    stopped_at = NOW(),
                    updated_at = NOW(),
                    last_activity = NOW()
                WHERE stream_id = $2
            """
            await self.db_manager.execute_query(update_query, (stop_reason, stream_id))
            
            logger.warning(
                f"⚠️ Database updated: {stream_id_str} -> "
                f"is_streaming=TRUE (will auto-retry), stop_reason='{stop_reason}'"
            )
        
        await asyncio.sleep(0.1)

        # ===== NOW stop in memory =====
        async with self._lock:
            if stream_id_str in self.active_streams:
                stop_event = self.active_streams[stream_id_str].get('stop_event')
                if stop_event:
                    stop_event.set()
                
                self.active_streams[stream_id_str]['status'] = 'stopping'
                self.active_streams[stream_id_str]['stop_reason'] = stop_reason
        
        # Record stop in history
        await self.video_stream_service.record_camera_stop(
            stream_id=stream_id,
            stop_reason=stop_reason,
            stopped_by=requester_user_id if stop_reason == 'user_action' else None,
            additional_context=additional_context
        )
        
        # Clean up in memory (this won't change database anymore)
        await self._stop_stream(stream_id_str, for_restart=False)
        
        # Determine response based on stop reason
        if stop_reason == 'user_action':
            return {
                "stream_id": str(stream_id),
                "name": stream_info['name'],
                "workspace_id": str(workspace_id),
                "stop_reason": stop_reason,
                "will_auto_restart": False,
                "message": "Camera stopped by user - will NOT auto-restart"
            }
        else:
            # System error: Schedule retry
            stream_info_db = await self.video_stream_service.get_video_stream_by_id(stream_id)
            current_retry_count = stream_info_db.get('retry_count', 0) if stream_info_db else 0
            
            await self.retry_service.schedule_retry(
                stream_id=stream_id,
                stop_reason=stop_reason,
                current_retry_count=current_retry_count,
                error_context=additional_context
            )
            
            return {
                "stream_id": str(stream_id),
                "name": stream_info['name'],
                "workspace_id": str(workspace_id),
                "stop_reason": stop_reason,
                "will_auto_restart": True,
                "retry_strategy": "infinite_with_backoff",
                "message": f"Camera stopped due to {stop_reason} - will keep trying to reconnect"
            }

    async def restart_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """
        Restart stream: Stop cleanly, then start fresh.
        
        CRITICAL: Uses proper sequencing to avoid management loop interference.
        """
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None
        )
        
        workspace_id = stream_info['workspace_id']
        stream_id_str = str(stream_id)
        
        # Check permissions
        is_owner = stream_info['user_id'] == requester_user_id
        is_workspace_admin = membership_info['role'] in ['admin', 'owner']
        
        if not is_owner and not is_workspace_admin:
            raise HTTPException(
                status_code=403,
                detail="Only stream owner or workspace admin can restart streams"
            )
        
        # Check if can start
        can_start, reason = await self.can_start_stream_in_workspace(
            workspace_id, requester_user_id
        )
        
        if not can_start:
            raise HTTPException(status_code=403, detail=reason)
        
        logger.warning(
            f"🔄 RESTART REQUESTED: stream={stream_id}, requester={requester_user_id}"
        )
        
        # ===== Step 1: Stop the stream (database FIRST, then memory) =====
        
        # Update database to 'restarting' state with is_streaming=TRUE
        # This tells management loop "don't touch this, restart in progress"
        await self.db_manager.execute_query(
            """UPDATE video_stream 
            SET status = 'restarting',
                stop_reason = NULL,
                retry_count = 0,
                next_retry_at = NULL,
                auto_retry_enabled = TRUE,
                updated_at = NOW(),
                last_activity = NOW()
            WHERE stream_id = $1""",
            (stream_id,)
        )
        
        logger.info(f"📊 Database: {stream_id_str} -> status='restarting'")
        
        # Signal stop in memory
        async with self._lock:
            if stream_id_str in self.active_streams:
                stop_event = self.active_streams[stream_id_str].get('stop_event')
                if stop_event:
                    stop_event.set()
                    logger.info(f"🛑 Stop event set for restart of {stream_id_str}")
        
        # Wait for graceful shutdown
        await asyncio.sleep(3.0)
        
        # ===== Step 2: Start fresh =====
        
        # Update database to 'processing' so management loop will start it
        await self.db_manager.execute_query(
            """UPDATE video_stream 
            SET is_streaming = TRUE,
                status = 'processing',
                updated_at = NOW(),
                last_activity = NOW()
            WHERE stream_id = $1""",
            (stream_id,)
        )
        
        logger.info(
            f"✅ Stream {stream_id} restart initiated: "
            f"is_streaming=TRUE, status='processing' (management loop will start it)"
        )
        
        return {
            "stream_id": str(stream_id),
            "name": stream_info['name'],
            "workspace_id": str(workspace_id),
            "message": "Stream restart initiated (will start within 30 seconds)"
        }

    async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
        """
        Stop stream with thorough cleanup.
        FIXED: Always removes from active_streams, even if not found initially.
        """
        logger.info(f"🛑 Stopping stream {stream_id_str} (for_restart={for_restart})")
        
        # Transition to STOPPING state
        await self._transition_stream_state(stream_id_str, StreamState.STOPPING, force=True)
        
        # Get stream info before removing
        async with self._lock:
            stream_info = self.active_streams.get(stream_id_str)
        
        # Always try to clean up, even if stream_info is None
        try:
            if stream_info:
                stop_event_obj = stream_info.get('stop_event')
                task_obj = stream_info.get('task')
                workspace_id = stream_info.get('workspace_id')
                source = stream_info.get('source')
                
                # Signal stop
                if stop_event_obj:
                    stop_event_obj.set()
                    logger.debug(f"Set stop event for {stream_id_str}")
                
                # Cancel task
                if task_obj and not task_obj.done():
                    task_obj.cancel()
                    try:
                        await asyncio.wait_for(task_obj, timeout=5.0)
                        logger.debug(f"Task cancelled for {stream_id_str}")
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        logger.warning(f"Task cancellation timeout for {stream_id_str}")
                
                # Clean up workspace registry
                if workspace_id:
                    workspace_id_str = str(workspace_id)
                    if workspace_id_str in self.workspace_streams:
                        self.workspace_streams[workspace_id_str].discard(stream_id_str)
                        if not self.workspace_streams[workspace_id_str]:
                            del self.workspace_streams[workspace_id_str]
                
                # Clean up shared stream registry
                if source:
                    self._shared_stream_registry[source].discard(stream_id_str)
                    if not self._shared_stream_registry[source]:
                        del self._shared_stream_registry[source]
            
            # CRITICAL: Always remove from these dicts, even if stream_info was None
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            self.stream_workspaces.pop(stream_id_str, None)
            self.stream_processing_stats.pop(stream_id_str, None)
            self.fire_detection_states.pop(stream_id_str, None)
            self.fire_detection_frame_counts.pop(stream_id_str, None)
            self.people_count_notification_cooldowns.pop(stream_id_str, None)
            self.stream_errors.pop(stream_id_str, None)
            
            logger.info(f"✅ Cleaned up all references for {stream_id_str}")
            
            # Update metrics
            if not for_restart:
                self.metrics['total_streams_stopped'] += 1
            
            # Transition to final state
            if for_restart:
                await self._transition_stream_state(stream_id_str, StreamState.STARTING, force=True)
            else:
                await self._transition_stream_state(stream_id_str, StreamState.INACTIVE, force=True)
            
            logger.info(f"✅ Stream {stream_id_str} stopped successfully")
        
        except Exception as e:
            logger.error(f"Error in _stop_stream for {stream_id_str}: {e}", exc_info=True)
            
            # Even on error, force cleanup
            try:
                async with self._lock:
                    self.active_streams.pop(stream_id_str, None)
                self.stream_workspaces.pop(stream_id_str, None)
                self.stream_processing_stats.pop(stream_id_str, None)
                logger.warning(f"⚠️ Force-cleaned {stream_id_str} after error")
            except:
                pass

    async def get_workspace_streams_for_user(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Get all streams accessible to a user through their workspaces."""
        if workspace_id:
            await self.workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=workspace_id
            )
            target_workspaces = [workspace_id]
        else:
            user_workspaces = await self.workspace_service.get_user_workspaces(user_id)
            target_workspaces = [UUID(ws['workspace_id']) for ws in user_workspaces]
        
        if not target_workspaces:
            return {"streams": [], "total": 0, "workspaces": []}
        
        all_streams = []
        workspace_summaries = []
        
        for ws_id in target_workspaces:
            ws_info = await self.workspace_service.get_workspace_by_id(ws_id)
            streams = await self.video_stream_service.get_workspace_streams(ws_id)
            active_streams = await self.get_workspace_active_streams(ws_id)
            
            for stream in streams:
                stream_id_str = str(stream['stream_id'])
                is_active = any(s['stream_id'] == stream_id_str for s in active_streams)
                
                all_streams.append({
                    "stream_id": stream_id_str,
                    "name": stream['name'],
                    "type": stream['type'],
                    "status": stream['status'],
                    "is_streaming": stream['is_streaming'],
                    "is_active_in_memory": is_active,
                    "workspace_id": str(ws_id),
                    "workspace_name": ws_info['name'],
                    "owner_id": str(stream['user_id']),
                    "location": stream.get('location'),
                    "created_at": stream['created_at'].isoformat() if stream['created_at'] else None
                })
            
            workspace_summaries.append({
                "workspace_id": str(ws_id),
                "workspace_name": ws_info['name'],
                "total_streams": len(streams),
                "active_streams": len([s for s in streams if s['is_streaming']]),
                "in_memory_streams": len(active_streams)
            })
        
        return {
            "streams": all_streams,
            "total": len(all_streams),
            "workspaces": workspace_summaries
        }

    # ==================== Background Tasks ====================

    async def start_background_tasks(self):
        """Start background tasks including retry loop"""
        try:
            logging.info("Starting StreamManager background tasks...")
            
            # Existing tasks
            await self._start_background_tasks_internal()

            # Start status batcher
            await self.status_batcher.start()
            
            # NEW: Start retry loop
            self.retry_task = asyncio.create_task(self._retry_loop())
            self.retry_task.set_name("camera_retry_loop")
            self.retry_task.add_done_callback(self._handle_task_done)
            
            logging.info("✅ StreamManager background tasks started successfully")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to start StreamManager background tasks: {e}", exc_info=True)
            return False

    async def _retry_loop(self):
        """
        Dedicated loop for retrying failed cameras.
        Runs every 10 seconds to check for cameras ready to retry.
        """
        logger.info("🔄 Camera retry loop started")
        
        while True:
            try:
                # Get cameras ready for retry
                cameras_to_retry = await self.retry_service.get_cameras_ready_for_retry()
                
                if not cameras_to_retry:
                    await asyncio.sleep(10)  # Check every 10 seconds
                    continue
                
                logger.info(
                    f"🔄 Processing {len(cameras_to_retry)} cameras for retry"
                )
                
                # Process each camera
                for camera in cameras_to_retry:
                    try:
                        stream_id = camera['stream_id']
                        stream_id_str = str(stream_id)
                        
                        # Check if already running in memory
                        async with self._lock:
                            if stream_id_str in self.active_streams:
                                stream_info = self.active_streams[stream_id_str]
                                current_state = self.stream_states.get(stream_id_str)
                                if current_state == StreamState.ACTIVE and stream_info.get('latest_frame') is not None:
                                    logger.info(
                                        f"⏭️ Camera {stream_id_str} already active, "
                                        f"marking retry as success"
                                    )
                                    await self.retry_service.mark_retry_success(
                                        stream_id, camera['retry_count']
                                    )
                                    continue
                        
                        logger.info(
                            f"🔄 Retry attempt #{camera['retry_count']} for camera "
                            f"{camera['name']} (reason: {camera['stop_reason']})"
                        )
                        
                        # Prepare location info
                        location_info = {
                            'location': camera.get('location'),
                            'area': camera.get('area'),
                            'building': camera.get('building'),
                            'zone': camera.get('zone'),
                            'floor_level': camera.get('floor_level'),
                            'latitude': camera.get('latitude'),
                            'longitude': camera.get('longitude')
                        }
                        
                        # Attempt to start stream
                        try:
                            await self.start_stream_background(
                                stream_id=stream_id,
                                owner_id=camera['user_id'],
                                owner_username=camera['username'],
                                camera_name=camera['name'],
                                source=camera['path'],
                                workspace_id=camera['workspace_id'],
                                location_info=location_info
                            )
                            
                            # Wait a bit to see if it stabilizes
                            await asyncio.sleep(5)
                            
                            # Check if successfully connected
                            async with self._lock:
                                if stream_id_str in self.active_streams:
                                    stream_info = self.active_streams[stream_id_str]
                                    if stream_info.get('latest_frame') is not None:
                                        # SUCCESS!
                                        await self.retry_service.mark_retry_success(
                                            stream_id, camera['retry_count']
                                        )
                                        logger.info(
                                            f"✅ Camera {camera['name']} reconnected "
                                            f"successfully after {camera['retry_count']} retries!"
                                        )
                                        continue
                            
                            # If we get here, stream started but no frames yet
                            # Let it run and check on next cycle
                            logger.info(
                                f"⏳ Camera {camera['name']} starting, "
                                f"waiting for frames..."
                            )
                            
                        except Exception as start_error:
                            # Start failed, schedule next retry
                            logger.warning(
                                f"❌ Retry #{camera['retry_count']} failed for "
                                f"{camera['name']}: {start_error}"
                            )
                            
                            await self.retry_service.mark_retry_failure(
                                stream_id, str(start_error)
                            )
                    
                    except Exception as e:
                        logger.error(
                            f"Error processing retry for camera {camera.get('name')}: {e}",
                            exc_info=True
                        )
                
                # Brief pause between batch processing
                await asyncio.sleep(2)
                
            except asyncio.CancelledError:
                logger.info("🛑 Camera retry loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in retry loop: {e}", exc_info=True)
                await asyncio.sleep(10)
        
        logger.info("Camera retry loop stopped")
    
    async def _start_background_tasks_internal(self):
        """
        Internal method to start all background tasks.
        ENHANCED: Added monitoring task.
        """
        await self.stop_background_tasks()
        
        # Stream management task
        self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
        self.background_task.set_name("manage_streams_loop")
        self.background_task.add_done_callback(self._handle_task_done)
        
        # Cleanup task
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.cleanup_task.set_name("periodic_cleanup_loop")
        self.cleanup_task.add_done_callback(self._handle_task_done)
        
        # NEW: Resource monitoring task
        self.monitor_task = asyncio.create_task(self._resource_monitor())
        self.monitor_task.set_name("resource_monitor_loop")
        self.monitor_task.add_done_callback(self._handle_task_done)

    async def _resource_monitor(self):
        """
        NEW: Monitor resource usage and performance.
        Helps identify memory leaks and performance issues.
        """
        while True:
            try:
                await asyncio.sleep(config.get("resource_monitor_interval", 300))  # 5 minutes
                
                # Collect metrics
                metrics = {
                    'timestamp': datetime.now(ZoneInfo("Africa/Cairo")).isoformat(),
                    'active_streams': len(self.active_streams),
                    'total_workspaces': len(self.workspace_streams),
                    'shared_sources': len(self._shared_stream_registry),
                    'notification_subscribers': sum(len(ws) for ws in self.notification_subscribers.values()),
                    'error_records': sum(len(errors) for errors in self.stream_errors.values()),
                    'performance': {
                        'total_started': self.metrics['total_streams_started'],
                        'total_stopped': self.metrics['total_streams_stopped'],
                        'total_errors': self.metrics['total_errors'],
                        'avg_duration': self.metrics['avg_stream_duration']
                    }
                }
                
                logger.info(f"📊 Resource Monitor: {metrics}")
                
                # Check for potential issues
                if len(self.active_streams) > self.max_concurrent_streams * 0.9:
                    logger.warning("⚠️ Approaching maximum concurrent streams limit!")
                
                if metrics['error_records'] > 100:
                    logger.warning(f"⚠️ High error count: {metrics['error_records']} records")
                
            except asyncio.CancelledError:
                logging.info("Resource monitor task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in resource monitor: {e}", exc_info=True)

    async def manage_streams_with_deduplication(self):
        """
        Stream management loop with smart restart logic.
        
        Key behaviors:
        1. Starts cameras with is_streaming=TRUE that aren't running in memory
        2. Stops cameras running in memory that shouldn't be (database says is_streaming=FALSE)
        3. NEVER auto-restarts user-stopped cameras (stop_reason='user_action')
        4. Delegates retry logic to retry_service (which runs in separate loop)
        5. Verifies streams are actually processing before considering them "active"
        """
        consecutive_errors = 0
        max_consecutive_errors = 5
        zombie_check_counter = 0
        
        while True:
            try:

                 # Every 5 cycles, run zombie detection
                zombie_check_counter += 1
                if zombie_check_counter >= 5:
                    try:
                        await self.detect_and_cleanup_zombie_streams()
                    except Exception as zombie_err:
                        logger.error(f"Zombie detection error: {zombie_err}")
                    zombie_check_counter = 0
                
                logger.info("🔄 Management loop cycle starting...")
                
                # ==================== STEP 1: Query Database State ====================
                # Get cameras that SHOULD be running (excluding user-stopped ones)
                streams_to_run_query = """
                    SELECT 
                        vs.stream_id, vs.name, vs.path, vs.user_id, vs.workspace_id, 
                        u.username, u.role as user_role, u.is_subscribed, u.is_active,
                        w.is_active as workspace_active, w.name as workspace_name,
                        vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                        vs.latitude, vs.longitude, vs.is_streaming, vs.status,
                        vs.stop_reason, vs.stopped_at, vs.updated_at, vs.last_activity,
                        vs.retry_count, vs.next_retry_at
                    FROM video_stream vs
                    JOIN users u ON vs.user_id = u.user_id
                    JOIN workspaces w ON vs.workspace_id = w.workspace_id
                    WHERE vs.is_streaming = TRUE 
                        AND u.is_active = TRUE
                        AND w.is_active = TRUE
                        AND (u.is_subscribed = TRUE OR u.role = 'admin')
                        AND (vs.stop_reason IS NULL OR vs.stop_reason != 'user_action')
                        AND (
                            vs.next_retry_at IS NULL 
                            OR vs.next_retry_at <= NOW()
                        )
                """

                potential_streams_db = await self.db_manager.execute_query(
                    streams_to_run_query, fetch_all=True
                )
                potential_streams_db = potential_streams_db or []
                
                logger.info(
                    f"📊 Database: {len(potential_streams_db)} cameras should be running"
                )
                
                # Get memory state
                async with self._lock:
                    current_running_ids_mem = set(self.active_streams.keys())
                
                logger.info(f"📊 Memory: {len(current_running_ids_mem)} cameras active")
                
                db_should_run_ids = {str(s['stream_id']) for s in potential_streams_db}
                
                # Stop streams that shouldn't be running
                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                
                if streams_to_stop_ids:
                    logger.warning(f"⚠️ Found {len(streams_to_stop_ids)} streams to stop")
                    
                    for stream_id_str in streams_to_stop_ids:
                        try:
                            # Double-check with database
                            check_query = """
                                SELECT is_streaming, status, stop_reason, name
                                FROM video_stream
                                WHERE stream_id = $1
                            """
                            db_state = await self.db_manager.execute_query(
                                check_query, (UUID(stream_id_str),), fetch_one=True
                            )
                            
                            if not db_state:
                                logger.warning(f"Stream {stream_id_str} not in database, removing from memory")
                                await self._stop_stream(stream_id_str, for_restart=False)
                                continue
                            
                            # If database says stop, honor it
                            if not db_state['is_streaming'] or db_state.get('stop_reason') == 'user_action':
                                logger.warning(
                                    f"🛑 Stopping {stream_id_str} ({db_state['name']}): "
                                    f"database says is_streaming={db_state['is_streaming']}, "
                                    f"stop_reason={db_state.get('stop_reason')}"
                                )
                                await self._stop_stream(stream_id_str, for_restart=False)
                        
                        except Exception as e:
                            logger.error(f"Error stopping stream {stream_id_str}: {e}")
            

                # ==================== STEP 4: Start Cameras That Should Be Running ====================
                # Group streams by workspace and file path for efficient processing
                workspace_streams_map = defaultdict(lambda: defaultdict(list))
                for stream_data in potential_streams_db:
                    ws_id = str(stream_data['workspace_id'])
                    file_path = stream_data['path']
                    workspace_streams_map[ws_id][file_path].append(stream_data)

                # Track start requests for batching
                start_tasks = []
                cameras_to_start_count = 0

                # Process each workspace
                for workspace_id_str, paths_map in workspace_streams_map.items():
                    try:
                        workspace_id = UUID(workspace_id_str)
                        
                        # Check workspace limits
                        limits = await self.get_workspace_stream_limits(workspace_id)
                        
                        if limits['available_slots'] <= 0:
                            logger.warning(
                                f"⚠️ Workspace {workspace_id_str} at capacity "
                                f"({limits['current_active']}/{limits['total_camera_limit']})"
                            )
                            continue
                        
                        # Process each file path in workspace
                        for file_path, streams_for_path in paths_map.items():
                            # Sort by priority (admin first, then by stream_id)
                            streams_for_path.sort(key=lambda x: (
                                x.get('user_role') != 'admin',
                                x['stream_id']
                            ))
                            
                            # Check file-level concurrency limits
                            enable_sharing = config.get("enable_stream_sharing", True)
                            max_streams_per_file = config.get(
                                "max_streams_per_file", 
                                5 if enable_sharing else 1
                            )
                            
                            active_streams_for_path = [
                                s for s in streams_for_path
                                if str(s['stream_id']) in current_running_ids_mem
                            ]
                            
                            can_start_count = max_streams_per_file - len(active_streams_for_path)
                            started_count = 0
                            
                            for stream_data in streams_for_path:
                                stream_id_str = str(stream_data['stream_id'])
                                stream_id = stream_data['stream_id']
                                
                                # Skip if already running
                                if stream_id_str in current_running_ids_mem:
                                    continue
                                
                                # Skip if reached file limit
                                if started_count >= can_start_count:
                                    logger.debug(
                                        f"File {file_path} at concurrent limit "
                                        f"({max_streams_per_file})"
                                    )
                                    break
                                
                                # Skip if reached workspace limit
                                if limits['available_slots'] <= started_count:
                                    logger.debug(
                                        f"Workspace {workspace_id_str} at limit, "
                                        f"deferring camera {stream_id_str}"
                                    )
                                    break
                                
                                # IMPORTANT: Check if this camera is in retry queue
                                # If next_retry_at is in the future, skip it (retry_service will handle it)
                                next_retry = stream_data.get('next_retry_at')
                                if next_retry and next_retry > datetime.now(ZoneInfo("Africa/Cairo")):
                                    logger.debug(
                                        f"⏰ Camera {stream_id_str} scheduled for retry at {next_retry}, "
                                        f"letting retry_service handle it"
                                    )
                                    continue
                                
                                # Double-check user limits
                                owner_id_obj = stream_data['user_id']
                                can_start, reason = await self.can_start_stream_in_workspace(
                                    workspace_id, owner_id_obj
                                )
                                
                                if not can_start:
                                    logger.warning(
                                        f"Cannot start camera {stream_id_str}: {reason}"
                                    )
                                    # Update database to reflect inability to start
                                    await self.video_stream_service.update_stream_status(
                                        stream_id, 'inactive', is_streaming=False
                                    )
                                    continue

                                # Prepare location info
                                location_info = {
                                    'location': stream_data.get('location'),
                                    'area': stream_data.get('area'),
                                    'building': stream_data.get('building'),
                                    'zone': stream_data.get('zone'),
                                    'floor_level': stream_data.get('floor_level'),
                                    'latitude': stream_data.get('latitude'),
                                    'longitude': stream_data.get('longitude')
                                }
                                
                                # Create start task
                                logger.info(
                                    f"🚀 Queuing start for camera {stream_id_str} "
                                    f"({stream_data['name']})"
                                )
                                
                                task = self.start_stream_background(
                                    stream_id,
                                    owner_id_obj,
                                    stream_data['username'],
                                    stream_data['name'],
                                    stream_data['path'],
                                    workspace_id,
                                    location_info
                                )
                                start_tasks.append((stream_id, task))
                                started_count += 1
                                cameras_to_start_count += 1
                    
                    except Exception as e:
                        logger.error(
                            f"Error processing workspace {workspace_id_str}: {e}", 
                            exc_info=True
                        )
                        consecutive_errors += 1

                # ==================== STEP 5: Execute Start Tasks ====================
                if start_tasks:
                    logger.info(f"🚀 Starting {len(start_tasks)} cameras in PARALLEL...")
                    
                    # CRITICAL FIX: Start ALL cameras at once using asyncio.gather
                    batch_coros = [task for _, task in start_tasks]
                    
                    # Start all cameras simultaneously
                    results = await asyncio.gather(
                        *batch_coros, 
                        return_exceptions=True
                    )
                    
                    # Log results
                    for idx, (stream_id, result) in enumerate(zip([sid for sid, _ in start_tasks], results)):
                        if isinstance(result, Exception):
                            logger.error(f"❌ Failed to start {stream_id}: {result}")
                            
                            # Determine error type
                            if isinstance(result, ConnectionError):
                                stop_reason = 'connection_error'
                            elif isinstance(result, TimeoutError):
                                stop_reason = 'timeout'
                            else:
                                stop_reason = 'system_error'
                            
                            # Record failure (retry_service will pick it up)
                            try:
                                await self.video_stream_service.record_camera_stop(
                                    stream_id=stream_id,
                                    stop_reason=stop_reason,
                                    stopped_by=None,
                                    additional_context=str(result)
                                )
                            except Exception as record_err:
                                logger.error(f"Error recording stop for {stream_id}: {record_err}")
                        else:
                            logger.info(f"✅ Successfully started {stream_id}")
                else:
                    logger.info("✅ No cameras need starting")
                    
                # ==================== STEP 6: Cleanup ====================
                # Cleanup empty shared streams
                await self.video_file_manager.cleanup_empty_streams()

                # ==================== STEP 7: Health Check ====================
                current_time = datetime.now(ZoneInfo("Africa/Cairo"))
                if (current_time - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
                
                # Reset error counter on success
                consecutive_errors = 0
                
                logger.info(
                    f"✅ Management loop cycle complete. "
                    f"Active: {len(current_running_ids_mem)}, "
                    f"Should run: {len(db_should_run_ids)}, "
                    f"Started: {cameras_to_start_count}"
                )
                
            except asyncio.CancelledError:
                logger.info("🛑 Management loop cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Management loop error: {e}", exc_info=True)
                consecutive_errors += 1
                
                # Back off on repeated errors
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"⚠️ {consecutive_errors} consecutive errors. Backing off..."
                    )
                    await asyncio.sleep(60)  # Back off for 1 minute
                    consecutive_errors = 0
            
            # ==================== STEP 8: Sleep ====================
            # Longer sleep to prevent interference with active streams
            configured_sleep = config.get("stream_manager_poll_interval_seconds", 30.0)
            sleep_time = max(30.0, configured_sleep)  # Minimum 30 seconds
            
            logger.info(f"💤 Management loop sleeping for {sleep_time}s")
            await asyncio.sleep(sleep_time)

    async def _periodic_cleanup(self):
        """Periodic cleanup with better error handling."""
        while True:
            try:
                # Clean websocket connections
                await self._clean_websocket_connections()

                current_time = time.time()

                # ==========================
                # Cleanup Fire Detection States - FIXED
                # ==========================
                fire_cooldown_expiry = 86400  # 24 hours
                expired_fire_states = []

                for stream_id, fire_data in list(self.fire_detection_states.items()):
                    # Skip if stream is still active
                    if stream_id in self.active_streams:
                        continue
                    
                    # Check if we have a valid timestamp
                    last_time = None
                    if isinstance(fire_data, dict):
                        last_time = fire_data.get('last_detection_time') or fire_data.get('last_notification_time')
                    
                    # If no timestamp or very old, mark for removal
                    if last_time is None:
                        # Keep for a short grace period (5 minutes) for recently stopped streams
                        # Check if there's any recent activity in active_streams history
                        expired_fire_states.append(stream_id)
                    else:
                        try:
                            # Handle both timestamp formats
                            if isinstance(last_time, datetime):
                                last_time_f = last_time.timestamp()
                            else:
                                last_time_f = float(last_time)
                            
                            if (current_time - last_time_f) > fire_cooldown_expiry:
                                expired_fire_states.append(stream_id)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Invalid fire timestamp for {stream_id}, removing: {e}")
                            expired_fire_states.append(stream_id)

                for key in expired_fire_states:
                    self.fire_detection_states.pop(key, None)
                    self.fire_detection_frame_counts.pop(key, None)

                # ==========================
                # Cleanup People Count Cooldowns
                # ==========================
                people_cooldown_expiry = 86400
                expired_people_cooldowns = []

                for stream_id, last_time in list(self.people_count_notification_cooldowns.items()):
                    # Skip active streams
                    if stream_id in self.active_streams:
                        continue
                    
                    try:
                        if isinstance(last_time, datetime):
                            last_time_f = last_time.timestamp()
                        else:
                            last_time_f = float(last_time)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid people timestamp for {stream_id}, removing")
                        expired_people_cooldowns.append(stream_id)
                        continue

                    if (current_time - last_time_f) > people_cooldown_expiry:
                        expired_people_cooldowns.append(stream_id)

                for key in expired_people_cooldowns:
                    self.people_count_notification_cooldowns.pop(key, None)

                # ==========================
                # Database Cleanup
                # ==========================
                await self.fire_service.cleanup_old_fire_states()

                # ==========================
                # Cleanup orphaned workspace registrations
                # ==========================
                orphaned_total = 0

                async with self._lock:
                    for workspace_id_str in list(self.workspace_streams.keys()):
                        active_stream_ids = self.workspace_streams[workspace_id_str]
                        orphaned = [sid for sid in active_stream_ids if sid not in self.active_streams]

                        for sid in orphaned:
                            self.workspace_streams[workspace_id_str].discard(sid)
                            self.stream_workspaces.pop(sid, None)
                            orphaned_total += 1

                        if not self.workspace_streams[workspace_id_str]:
                            del self.workspace_streams[workspace_id_str]
                            logger.debug(f"Removed empty workspace registry: {workspace_id_str}")

                # ==========================
                # Cleanup shared stream registry
                # ==========================
                for source in list(self._shared_stream_registry.keys()):
                    stream_ids = self._shared_stream_registry[source]
                    active_only = [sid for sid in stream_ids if sid in self.active_streams]

                    if not active_only:
                        del self._shared_stream_registry[source]
                        logger.debug(f"Removed orphaned shared stream registry: {source}")
                    else:
                        self._shared_stream_registry[source] = set(active_only)

                # ==========================
                # Cleanup old error records
                # ==========================
                for stream_id in list(self.stream_errors.keys()):
                    if len(self.stream_errors[stream_id]) > 100:
                        self.stream_errors[stream_id] = self.stream_errors[stream_id][-100:]

                    if stream_id not in self.active_streams:
                        last_error = self.stream_errors[stream_id][-1] if self.stream_errors[stream_id] else None
                        if last_error:
                            age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_error['timestamp']).total_seconds()
                            if age > 86400:
                                del self.stream_errors[stream_id]

                # ==========================
                # Logging Summary
                # ==========================
                cleanup_summary = {
                    'fire_states_cleaned': len(expired_fire_states),
                    'people_cooldowns_cleaned': len(expired_people_cooldowns),
                    'orphaned_registrations': orphaned_total,
                }

                if any(cleanup_summary.values()):
                    logger.debug(f"🧹 Cleanup summary: {cleanup_summary}")

            except asyncio.CancelledError:
                logger.info("Periodic cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}", exc_info=True)

            await asyncio.sleep(config.get("stream_cleanup_interval_seconds", 60.0))

    async def _clean_websocket_connections(self):
        """
        Clean up dead WebSocket connections.
        ENHANCED: Better logging and statistics.
        """
        cleaned_notifications = 0
        cleaned_clients = 0
        
        # Clean notification subscribers
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_ws = {
                    ws for ws in websockets 
                    if ws.client_state != WebSocketState.CONNECTED
                }
                if dead_ws:
                    self.notification_subscribers[user_id] -= dead_ws
                    cleaned_notifications += len(dead_ws)
                    
                    if not self.notification_subscribers[user_id]:
                        del self.notification_subscribers[user_id]
        
        # Clean stream clients
        async with self._lock:
            for stream_id, stream_info in list(self.active_streams.items()):
                if 'clients' in stream_info:
                    dead_clients = {
                        ws for ws in stream_info['clients'] 
                        if ws.client_state != WebSocketState.CONNECTED
                    }
                    if dead_clients:
                        stream_info['clients'] -= dead_clients
                        cleaned_clients += len(dead_clients)
        
        if cleaned_notifications or cleaned_clients:
            logger.debug(
                f"🧹 Cleaned {cleaned_notifications} notification WS, "
                f"{cleaned_clients} stream client WS"
            )

    # async def _check_stream_health(self):
    #     """
    #     health check with better diagnostics.
    #     ENHANCED: Better detection and recovery logic.
    #     """
    #     async with self._health_lock:
    #         # Prevent concurrent health checks
    #         if (datetime.now(ZoneInfo("Africa/Cairo")) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2:
    #             return

    #         current_time_utc = datetime.now(ZoneInfo("Africa/Cairo"))
    #         streams_to_restart_ids = []
    #         health_issues = []
            
    #         async with self._lock:
    #             active_stream_ids_copy = list(self.active_streams.keys())

    #         for stream_id_str in active_stream_ids_copy:
    #             try:
    #                 stream_id_uuid = UUID(stream_id_str)
                    
    #                 # Check database state
    #                 db_stream_state = await self.db_manager.execute_query(
    #                     """SELECT vs.is_streaming, vs.status, vs.workspace_id, 
    #                               w.is_active as workspace_active
    #                        FROM video_stream vs
    #                        JOIN workspaces w ON vs.workspace_id = w.workspace_id
    #                        WHERE vs.stream_id = $1""",
    #                     (stream_id_uuid,), 
    #                     fetch_one=True
    #                 )

    #                 if not db_stream_state:
    #                     health_issues.append(f"{stream_id_str}: not found in database")
    #                     await self._stop_stream(stream_id_str, for_restart=False)
    #                     continue
                    
    #                 # Stop if workspace is inactive
    #                 if not db_stream_state.get('workspace_active'):
    #                     health_issues.append(f"{stream_id_str}: workspace inactive")
    #                     await self._stop_stream(stream_id_str, for_restart=False)
    #                     continue
                    
    #                 # Stop if stream should not be running
    #                 if not db_stream_state.get('is_streaming', False):
    #                     health_issues.append(f"{stream_id_str}: externally stopped")
    #                     await self._stop_stream(stream_id_str, for_restart=False)
    #                     continue
                    
    #                 async with self._lock:
    #                     stream_info_mem = self.active_streams.get(stream_id_str)
                    
    #                 if not stream_info_mem:
    #                     continue

    #                 # Check for stale frames
    #                 last_activity_time_mem = (
    #                     stream_info_mem.get('last_frame_time') or 
    #                     stream_info_mem.get('start_time')
    #                 )
    #                 time_since_last_frame = float('inf')
                    
    #                 if last_activity_time_mem:
    #                     if last_activity_time_mem.tzinfo is None:
    #                         last_activity_time_mem = last_activity_time_mem.replace(tzinfo=ZoneInfo("Africa/Cairo"))
    #                     time_since_last_frame = (
    #                         current_time_utc - last_activity_time_mem
    #                     ).total_seconds()

    #                 stale_threshold = config.get("stream_stale_threshold_seconds", 300.0)
    #                 if time_since_last_frame > stale_threshold:
    #                     health_issues.append(
    #                         f"{stream_id_str}: frozen ({time_since_last_frame:.0f}s since last frame)"
    #                     )
    #                     streams_to_restart_ids.append(stream_id_str)
                    
    #                 # Check task health
    #                 task = stream_info_mem.get('task')
    #                 if task and task.done():
    #                     exc = task.exception()
    #                     if exc:
    #                         health_issues.append(f"{stream_id_str}: task failed - {exc}")
    #                         streams_to_restart_ids.append(stream_id_str)
                        
    #             except Exception as e:
    #                 logger.error(
    #                     f"Error during health check for stream {stream_id_str}: {e}", 
    #                     exc_info=True
    #                 )
    #                 health_issues.append(f"{stream_id_str}: health check error - {e}")

    #         # Restart frozen streams
    #         if streams_to_restart_ids:
    #             logger.warning(f"⚠️ Health check found {len(streams_to_restart_ids)} streams to restart")
    #             for stream_id_to_restart_str in streams_to_restart_ids:
    #                 logger.info(f"🔄 Restarting frozen stream: {stream_id_to_restart_str}")
    #                 await self._stop_stream(stream_id_to_restart_str, for_restart=True)
            
    #         # Log health summary
    #         if health_issues:
    #             logger.warning(f"Health check issues:\n" + "\n".join(f"  - {issue}" for issue in health_issues))
    #         else:
    #             logger.debug(f"✅ Health check passed for {len(active_stream_ids_copy)} streams")
            
    #         self.last_healthcheck = datetime.now(ZoneInfo("Africa/Cairo"))

    async def _check_stream_health(self):
        """
        Health check with proper database state verification.
        FIXED: Always checks database state to avoid reporting stopped cameras as active.
        """
        async with self._health_lock:
            # Prevent concurrent health checks
            if (datetime.now(ZoneInfo("Africa/Cairo")) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2:
                return

            current_time_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            streams_to_restart_ids = []
            health_issues = []
            
            async with self._lock:
                active_stream_ids_copy = list(self.active_streams.keys())

            for stream_id_str in active_stream_ids_copy:
                try:
                    stream_id_uuid = UUID(stream_id_str)
                    
                    # ✅ CRITICAL FIX: ALWAYS check database state first
                    db_stream_state = await self.db_manager.execute_query(
                        """SELECT vs.is_streaming, vs.status, vs.workspace_id, vs.stop_reason,
                                w.is_active as workspace_active
                        FROM video_stream vs
                        JOIN workspaces w ON vs.workspace_id = w.workspace_id
                        WHERE vs.stream_id = $1""",
                        (stream_id_uuid,), 
                        fetch_one=True
                    )

                    if not db_stream_state:
                        health_issues.append(f"{stream_id_str}: not found in database")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # ✅ FIX: Stop if database says is_streaming=FALSE (user stopped)
                    if not db_stream_state.get('is_streaming', False):
                        logger.warning(
                            f"🛑 Camera {stream_id_str} in memory but database says is_streaming=FALSE "
                            f"(stop_reason: {db_stream_state.get('stop_reason')}). Cleaning up from memory."
                        )
                        health_issues.append(f"{stream_id_str}: database says not streaming")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Stop if workspace is inactive
                    if not db_stream_state.get('workspace_active'):
                        health_issues.append(f"{stream_id_str}: workspace inactive")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Get memory state
                    async with self._lock:
                        stream_info_mem = self.active_streams.get(stream_id_str)
                    
                    if not stream_info_mem:
                        continue

                    # Check for stale frames
                    last_activity_time_mem = (
                        stream_info_mem.get('last_frame_time') or 
                        stream_info_mem.get('start_time')
                    )
                    time_since_last_frame = float('inf')
                    
                    if last_activity_time_mem:
                        if last_activity_time_mem.tzinfo is None:
                            last_activity_time_mem = last_activity_time_mem.replace(tzinfo=ZoneInfo("Africa/Cairo"))
                        time_since_last_frame = (
                            current_time_utc - last_activity_time_mem
                        ).total_seconds()

                    stale_threshold = config.get("stream_stale_threshold_seconds", 300.0)
                    if time_since_last_frame > stale_threshold:
                        health_issues.append(
                            f"{stream_id_str}: frozen ({time_since_last_frame:.0f}s since last frame)"
                        )
                        streams_to_restart_ids.append(stream_id_str)
                    
                    # Check task health
                    task = stream_info_mem.get('task')
                    if task and task.done():
                        exc = task.exception()
                        if exc:
                            health_issues.append(f"{stream_id_str}: task failed - {exc}")
                            streams_to_restart_ids.append(stream_id_str)
                        
                except Exception as e:
                    logger.error(
                        f"Error during health check for stream {stream_id_str}: {e}", 
                        exc_info=True
                    )
                    health_issues.append(f"{stream_id_str}: health check error - {e}")

            # Restart frozen streams
            if streams_to_restart_ids:
                logger.warning(f"⚠️ Health check found {len(streams_to_restart_ids)} streams to restart")
                for stream_id_to_restart_str in streams_to_restart_ids:
                    logger.info(f"🔄 Restarting frozen stream: {stream_id_to_restart_str}")
                    await self._stop_stream(stream_id_to_restart_str, for_restart=True)
            
            # Log health summary
            if health_issues:
                logger.warning(f"Health check issues:\n" + "\n".join(f"  - {issue}" for issue in health_issues))
            else:
                logger.debug(f"✅ Health check passed for {len(active_stream_ids_copy)} streams")
            
            self.last_healthcheck = datetime.now(ZoneInfo("Africa/Cairo"))
            
    # ==================== Frame Updates ====================

    def update_stream_frame(
        self,
        stream_id_str: str,
        frame: Any,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str
    ):
        """Update stream frame and statistics."""
        if stream_id_str in self.active_streams:
            self.active_streams[stream_id_str]['latest_frame'] = frame
            self.active_streams[stream_id_str]['last_frame_time'] = datetime.now(ZoneInfo("Africa/Cairo"))
            
            # Update fire detection state - FIXED
            if stream_id_str not in self.fire_detection_states:
                self.fire_detection_states[stream_id_str] = {
                    'status': 'no detection',
                    'last_detection_time': None,
                    'last_notification_time': None
                }
            
            # Update fire status
            current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            self.fire_detection_states[stream_id_str]['status'] = fire_status
            
            if fire_status != 'no detection':
                self.fire_detection_states[stream_id_str]['last_detection_time'] = current_time
            
            # Update processing stats
            if stream_id_str in self.stream_processing_stats:
                stats = self.stream_processing_stats[stream_id_str]
                stats['frames_processed'] += 1
                if person_count > 0:
                    stats['detection_count'] += 1
                stats['last_updated'] = current_time

    # ==================== Notification Management ====================

    async def subscribe_to_notifications(
        self, 
        user_id: str, 
        websocket: WebSocket
    ) -> bool:
        """Subscribe a WebSocket to notifications for a user."""
        async with self._notification_lock:
            self.notification_subscribers[user_id].add(websocket)
        
        logging.info(f"📡 WS client subscribed to notifications for user {user_id}")
        
        try:
            await websocket.send_json({
                "type": "subscription_confirmed",
                "for_user_id": user_id,
                "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
            })
            return True
        except Exception as e:
            logger.error(f"Error sending subscription confirmation: {e}")
            await self.unsubscribe_from_notifications(user_id, websocket)
            return False

    async def unsubscribe_from_notifications(
        self, 
        user_id: str, 
        websocket: WebSocket
    ):
        """Unsubscribe a WebSocket from notifications."""
        async with self._notification_lock:
            if user_id in self.notification_subscribers:
                self.notification_subscribers[user_id].discard(websocket)
                if not self.notification_subscribers[user_id]:
                    del self.notification_subscribers[user_id]
        
        logging.info(f"📡 WS client unsubscribed from notifications for user {user_id}")

    async def connect_client_to_stream(
        self, 
        stream_id: str, 
        websocket: WebSocket
    ) -> bool:
        """Connect a client WebSocket to a stream."""
        async with self._lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]['clients'].add(websocket)
                logger.debug(f"Client connected to stream {stream_id}")
                return True
            return False

    async def disconnect_client(
        self, 
        stream_id: str, 
        websocket: WebSocket
    ):
        """Disconnect a client WebSocket from a stream."""
        async with self._lock:
            if stream_id in self.active_streams and 'clients' in self.active_streams[stream_id]:
                self.active_streams[stream_id]['clients'].discard(websocket)
                logger.debug(f"Client disconnected from stream {stream_id}")

    # ==================== Task Management ====================

    async def _restart_background_task_if_needed(self, failed_task_name: Optional[str] = None):
        """Restart failed background tasks."""
        await asyncio.sleep(config.get("stream_manager_restart_delay_seconds", 15.0))
        logging.info(f"🔄 Attempting to restart background task: {failed_task_name or 'Unknown'}")
        
        # Restart management task
        if failed_task_name == "manage_streams_loop" or not self.background_task or self.background_task.done():
            if self.background_task and not self.background_task.done():
                self.background_task.cancel()
                try:
                    await asyncio.wait_for(self.background_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
            self.background_task.set_name("manage_streams_loop")
            self.background_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted manage_streams task")

        # Restart cleanup task
        if failed_task_name == "periodic_cleanup_loop" or not self.cleanup_task or self.cleanup_task.done():
            if self.cleanup_task and not self.cleanup_task.done():
                self.cleanup_task.cancel()
                try:
                    await asyncio.wait_for(self.cleanup_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.set_name("periodic_cleanup_loop")
            self.cleanup_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted periodic_cleanup task")

        # Restart monitor task
        if failed_task_name == "resource_monitor_loop" or not self.monitor_task or self.monitor_task.done():
            if self.monitor_task and not self.monitor_task.done():
                self.monitor_task.cancel()
                try:
                    await asyncio.wait_for(self.monitor_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.monitor_task = asyncio.create_task(self._resource_monitor())
            self.monitor_task.set_name("resource_monitor_loop")
            self.monitor_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted resource_monitor task")

    def _handle_task_done(self, task: asyncio.Task):
        """Handle completed background tasks."""
        try:
            task_name = task.get_name()
            exception = task.exception()
            if exception:
                logging.error(f"❌ Task '{task_name}' failed: {exception}", exc_info=exception)
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task_name))
            elif task.cancelled():
                logging.info(f"⏹️ Task '{task_name}' was cancelled")
            else:
                logging.info(f"✅ Task '{task_name}' completed successfully")
        except Exception as e:
            logging.error(f"Error in _handle_task_done: {e}", exc_info=True)

    async def stop_background_tasks(self):
        """Stop all background tasks."""
        tasks_to_stop = [
            ("background_task", self.background_task),
            ("cleanup_task", self.cleanup_task),
            ("monitor_task", self.monitor_task),
        ]
        
        for name, task_instance in tasks_to_stop:
            if task_instance and not task_instance.done():
                task_name_str = task_instance.get_name() if hasattr(task_instance, 'get_name') else name
                try:
                    task_instance.cancel()
                    await asyncio.wait_for(task_instance, timeout=5.0)
                    logging.info(f"✅ Task {name} ({task_name_str}) cancelled successfully")
                except asyncio.TimeoutError:
                    logging.warning(f"⚠️ Timeout cancelling {name} ({task_name_str})")
                except asyncio.CancelledError:
                    logging.info(f"✅ Task {name} ({task_name_str}) was already cancelled")
                except Exception as e:
                    logging.error(f"❌ Error cancelling {name} ({task_name_str}): {e}", exc_info=True)
        
        self.background_task = None
        self.cleanup_task = None
        self.monitor_task = None

    async def _ensure_collection_for_stream_workspace(self, workspace_id: UUID):
        """Ensure Qdrant collection exists for workspace."""
        try:
            await self.qdrant_service.ensure_workspace_collection(workspace_id)
        except Exception as e:
            logger.error(f"Error ensuring Qdrant collection for workspace {workspace_id}: {e}")

    # ==================== Shutdown ====================

    async def shutdown(self):
        """shutdown with better cleanup."""
        logging.info("🛑 Shutting down StreamManager...")
        
        # Stop background tasks first
        await self.stop_background_tasks()
        
        # Get all active streams
        async with self._lock:
            active_stream_ids = list(self.active_streams.keys())
        
        # Stop all streams with timeout
        if active_stream_ids:
            logging.info(f"Stopping {len(active_stream_ids)} active streams...")
            stop_tasks = [
                self._stop_stream(stream_id, for_restart=False) 
                for stream_id in active_stream_ids
            ]
            
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logging.warning("⚠️ Stream shutdown timed out after 30s")
        
        # Clear all state
        async with self._lock:
            self.active_streams.clear()
            self.workspace_streams.clear()
            self.stream_workspaces.clear()
            self.stream_states.clear()
        
        self.stream_processing_stats.clear()
        self.stream_errors.clear()
        self._shared_stream_registry.clear()
        
        # Stop shared streams
        for source in list(self.video_file_manager.shared_streams.keys()):
            await self.video_file_manager.remove_shared_stream(source)
        
        logging.info("✅ StreamManager shutdown complete")

    async def broadcast_notification(self, user_id_str: str, notification_data: dict):
        """
        Broadcast a notification to a specific user's WebSocket connections.
        
        Args:
            user_id_str: User ID as string
            notification_data: Notification data dictionary
        """
        if user_id_str not in self.notification_subscribers:
            logger.debug(f"No WebSocket subscribers for user {user_id_str}")
            return
        
        disconnected_sockets = []
        
        for websocket in self.notification_subscribers[user_id_str]:
            try:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(notification_data)
                    logger.debug(f"✅ Sent notification to WebSocket for user {user_id_str}")
                else:
                    disconnected_sockets.append(websocket)
            except Exception as e:
                logger.error(f"Error broadcasting notification to user {user_id_str}: {e}")
                disconnected_sockets.append(websocket)
        
        # Clean up disconnected sockets
        for ws in disconnected_sockets:
            self.notification_subscribers[user_id_str].discard(ws)

    # async def broadcast_notification(self, user_id_str: str, notification_data: dict):
    #     """
    #     Broadcast a notification to a specific user's WebSocket connections.
        
    #     Args:
    #         user_id_str: User ID as string
    #         notification_data: Notification data dictionary
    #     """
    #     if user_id_str not in self.notification_subscribers:
    #         logger.debug(f"No WebSocket subscribers for user {user_id_str}")
    #         return
        
    #     disconnected_sockets = []
    #     sent_count = 0
        
    #     async with self._notification_lock:
    #         websockets = list(self.notification_subscribers.get(user_id_str, set()))
        
    #     for websocket in websockets:
    #         try:
    #             if websocket.client_state == WebSocketState.CONNECTED:
    #                 await websocket.send_json(notification_data)
    #                 sent_count += 1
    #                 logger.info(f"✅ Sent notification (type={notification_data.get('type')}) to user {user_id_str}")
    #             else:
    #                 disconnected_sockets.append(websocket)
    #         except Exception as e:
    #             logger.error(f"Error broadcasting notification to user {user_id_str}: {e}")
    #             disconnected_sockets.append(websocket)
        
    #     # Clean up disconnected sockets
    #     if disconnected_sockets:
    #         async with self._notification_lock:
    #             for ws in disconnected_sockets:
    #                 self.notification_subscribers[user_id_str].discard(ws)
        
    #     logger.info(f"📤 Broadcasted to {sent_count} WebSocket(s) for user {user_id_str}")
    #     return sent_count

    async def broadcast_fire_alert_popup(
        self,
        workspace_id: UUID,
        stream_id: UUID,
        camera_name: str,
        fire_status: str,
        location_info: Optional[Dict[str, Any]] = None,
        broadcast_to_all_members: bool = True
    ):
        """
        Broadcast a critical fire alert popup to workspace members.
        
        Args:
            workspace_id: Workspace UUID
            stream_id: Camera stream UUID
            camera_name: Name of the camera
            fire_status: "fire" or "smoke"
            location_info: Location details dictionary
            broadcast_to_all_members: If True, send to all workspace members
        """
        try:
            # Build location text
            location_text = "Unknown Location"
            if location_info:
                location_parts = []
                if location_info.get('building'):
                    location_parts.append(location_info['building'])
                if location_info.get('floor_level'):
                    location_parts.append(f"Floor {location_info['floor_level']}")
                if location_info.get('zone'):
                    location_parts.append(location_info['zone'])
                if location_info.get('area'):
                    location_parts.append(location_info['area'])
                
                location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
            
            alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
            
            # Create popup alert payload
            popup_alert = {
                "type": "fire_alert_popup",
                "alert": {
                    "severity": "critical",
                    "alert_type": alert_type.lower(),
                    "status": fire_status,
                    "camera_name": camera_name,
                    "camera_id": str(stream_id),
                    "location": location_text,
                    "location_details": location_info,
                    "message": f"🔥 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}",
                    "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp(),
                    "workspace_id": str(workspace_id),
                    "requires_acknowledgment": True,
                    "sound_alert": True,
                    "priority": "high",
                    "actions": [
                        {
                            "label": "View Camera",
                            "action": "navigate",
                            "target": f"/dashboard/camera/{str(stream_id)}"
                        },
                        {
                            "label": "Acknowledge",
                            "action": "acknowledge",
                            "target": None
                        }
                    ]
                }
            }
            
            # Get target users
            target_users = []
            
            if broadcast_to_all_members:
                # Get all workspace members
                members = await self.workspace_service.get_workspace_members(
                    workspace_id=workspace_id,
                    current_user_id=workspace_id,  # Using workspace_id for admin check
                    is_admin=True
                )
                target_users = [str(member['user_id']) for member in members]
            else:
                # Get only camera owner
                stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
                if stream_info:
                    target_users = [str(stream_info['user_id'])]
            
            # Broadcast to all targets
            broadcast_count = 0
            for user_id_str in target_users:
                try:
                    await self.broadcast_notification(user_id_str, popup_alert)
                    broadcast_count += 1
                except Exception as e:
                    logger.error(f"Error broadcasting fire alert to user {user_id_str}: {e}")
            
            logger.warning(
                f"🔥 Fire popup alert broadcasted to {broadcast_count} users "
                f"for camera '{camera_name}' ({alert_type})"
            )
            
            return broadcast_count
            
        except Exception as e:
            logger.error(f"Error broadcasting fire alert popup: {e}", exc_info=True)
            return 0

    async def detect_and_cleanup_zombie_streams(self):
        """
        Detect and clean up zombie streams - streams in memory but not actually processing.
        
        A stream is considered a zombie if:
        1. It's in active_streams dict
        2. BUT its task is dead OR no frames in 60+ seconds
        3. OR database says is_streaming=FALSE
        """
        logger.info("🧟 Starting zombie stream detection...")
        
        zombies_found = []
        current_time = datetime.now(ZoneInfo("Africa/Cairo"))
        
        async with self._lock:
            stream_ids_in_memory = list(self.active_streams.keys())
        
        for stream_id_str in stream_ids_in_memory:
            try:
                # Get memory state
                async with self._lock:
                    stream_info = self.active_streams.get(stream_id_str)
                
                if not stream_info:
                    continue
                
                # Check database state
                db_state = await self.db_manager.execute_query(
                    """SELECT is_streaming, status, stop_reason, name 
                    FROM video_stream 
                    WHERE stream_id = $1""",
                    (UUID(stream_id_str),),
                    fetch_one=True
                )
                
                if not db_state:
                    # Stream deleted from database but still in memory
                    logger.warning(f"🧟 ZOMBIE: {stream_id_str} - deleted from database but in memory")
                    zombies_found.append({
                        'stream_id': stream_id_str,
                        'reason': 'deleted_from_database',
                        'name': stream_info.get('camera_name', 'Unknown')
                    })
                    continue
                
                camera_name = db_state['name']
                
                # Check if database says it should NOT be streaming
                if not db_state['is_streaming']:
                    logger.warning(
                        f"🧟 ZOMBIE: {stream_id_str} ({camera_name}) - "
                        f"in memory but database says is_streaming=FALSE"
                    )
                    zombies_found.append({
                        'stream_id': stream_id_str,
                        'reason': 'database_says_not_streaming',
                        'name': camera_name,
                        'db_status': db_state['status']
                    })
                    continue
                
                # Check if task is dead
                task = stream_info.get('task')
                if not task or task.done():
                    task_exception = None
                    if task and task.done():
                        try:
                            task_exception = str(task.exception())
                        except:
                            pass
                    
                    logger.warning(
                        f"🧟 ZOMBIE: {stream_id_str} ({camera_name}) - "
                        f"task is dead (exception: {task_exception})"
                    )
                    zombies_found.append({
                        'stream_id': stream_id_str,
                        'reason': 'dead_task',
                        'name': camera_name,
                        'exception': task_exception
                    })
                    continue
                
                # Check if frames are stale
                last_frame_time = stream_info.get('last_frame_time')
                if last_frame_time:
                    age = (current_time - last_frame_time).total_seconds()
                    if age > 60:  # No frames for 60 seconds
                        logger.warning(
                            f"🧟 ZOMBIE: {stream_id_str} ({camera_name}) - "
                            f"no frames for {age:.1f} seconds"
                        )
                        zombies_found.append({
                            'stream_id': stream_id_str,
                            'reason': 'stale_frames',
                            'name': camera_name,
                            'age_seconds': age
                        })
                        continue
                else:
                    # Stream has been running but never received any frames
                    start_time = stream_info.get('start_time')
                    if start_time:
                        running_time = (current_time - start_time).total_seconds()
                        if running_time > 60:  # Running for 60s but no frames
                            logger.warning(
                                f"🧟 ZOMBIE: {stream_id_str} ({camera_name}) - "
                                f"running {running_time:.1f}s but never received frames"
                            )
                            zombies_found.append({
                                'stream_id': stream_id_str,
                                'reason': 'no_frames_ever',
                                'name': camera_name,
                                'running_seconds': running_time
                            })
                            continue
            
            except Exception as e:
                logger.error(f"Error checking stream {stream_id_str} for zombies: {e}")
        
        # Clean up zombies
        if zombies_found:
            logger.warning(f"🧟 Found {len(zombies_found)} zombie streams, cleaning up...")
            
            for zombie in zombies_found:
                stream_id_str = zombie['stream_id']
                try:
                    logger.warning(
                        f"🗑️ Cleaning zombie: {stream_id_str} ({zombie['name']}) - "
                        f"reason: {zombie['reason']}"
                    )
                    
                    # Force cleanup
                    await self._stop_stream(stream_id_str, for_restart=False)
                    
                    # Update database to reflect reality
                    await self.db_manager.execute_query(
                        """UPDATE video_stream 
                        SET is_streaming = FALSE,
                            status = 'inactive',
                            stop_reason = $1,
                            stopped_at = NOW(),
                            updated_at = NOW()
                        WHERE stream_id = $2""",
                        (f"zombie_cleanup_{zombie['reason']}", UUID(stream_id_str))
                    )
                    
                except Exception as e:
                    logger.error(f"Error cleaning zombie {stream_id_str}: {e}")
            
            logger.info(f"✅ Cleaned up {len(zombies_found)} zombie streams")
        else:
            logger.info("✅ No zombie streams found")
        
        return zombies_found

# ==================== Global Instance ====================

stream_manager = StreamManager()


async def initialize_stream_manager():
    """Initialize the stream manager."""
    try:
        await stream_manager.start_background_tasks()
        logging.info("✅ StreamManager initialized successfully")
    except Exception as e:
        logging.error(f"❌ Failed to initialize StreamManager: {e}", exc_info=True)
        asyncio.create_task(
            stream_manager._restart_background_task_if_needed("initialization_failure")
        )
