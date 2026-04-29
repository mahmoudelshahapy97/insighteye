# app/services/retry_service.py
import asyncio
import logging
from typing import Dict, Any, List, Optional
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.services.database import db_manager
from app.services.video_stream_service import video_stream_service

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategies for different failure types"""
    EXPONENTIAL_BACKOFF = "exponential"  # For connection errors
    FIXED_INTERVAL = "fixed"             # For timeouts
    IMMEDIATE = "immediate"               # For system errors


class CameraRetryService:
    """
    Service for managing camera reconnection attempts.
    
    Key Features:
    - Infinite retries for system/network failures
    - Exponential backoff with jitter
    - Respects user stops (no retry)
    - Tracks retry history
    - Alerts on excessive failures
    """
    
    def __init__(self):
        self.db_manager = db_manager
        self.video_stream_service = video_stream_service
        
        # Retry configuration
        self.base_retry_interval = 30      # 30 seconds
        self.max_retry_interval = 300      # 5 minutes
        self.alert_threshold = 10           # Alert after 10 consecutive failures
        self.cleanup_success_after_days = 7  # Clean successful retry history after 7 days
        
        # Active retry tracking (in-memory for fast access)
        self._active_retries: Dict[str, Dict[str, Any]] = {}
        self._retry_lock = asyncio.Lock()
        
        logger.info("CameraRetryService initialized")
    
    async def schedule_retry(
        self,
        stream_id: UUID,
        stop_reason: str,
        current_retry_count: int = 0,
        error_context: Optional[str] = None
    ) -> bool:
        """
        Schedule a camera for retry.
        
        Args:
            stream_id: Camera stream UUID
            stop_reason: Why it stopped (connection_error, timeout, system_error)
            current_retry_count: Current number of retry attempts
            error_context: Additional error information
            
        Returns:
            bool: True if scheduled, False if should not retry (user_action)
        """
        # CRITICAL: Never retry user-stopped cameras
        if stop_reason == 'user_action':
            logger.info(f"🚫 Camera {stream_id} stopped by user - NO retry scheduled")
            return False
        
        try:
            # Calculate next retry time with exponential backoff
            next_retry_count = current_retry_count + 1
            retry_interval = min(
                self.base_retry_interval * (2 ** current_retry_count),
                self.max_retry_interval
            )
            
            # Add jitter (±20% randomization to prevent thundering herd)
            import random
            jitter = random.uniform(-0.2, 0.2) * retry_interval
            retry_interval_with_jitter = int(retry_interval + jitter)
            
            next_retry_at = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(seconds=retry_interval_with_jitter)
            
            # Update database
            query = """
                UPDATE video_stream
                SET retry_count = $1,
                    last_retry_at = NOW(),
                    next_retry_at = $2,
                    retry_interval_seconds = $3,
                    is_streaming = TRUE,  -- CRITICAL: Keep TRUE for auto-retry
                    status = 'error',
                    updated_at = NOW()
                WHERE stream_id = $4
                  AND stop_reason != 'user_action'  -- Safety check
            """
            
            rows = await self.db_manager.execute_query(
                query,
                (next_retry_count, next_retry_at, retry_interval_with_jitter, stream_id),
                return_rowcount=True
            )
            
            if rows > 0:
                logger.warning(
                    f"🔄 Retry #{next_retry_count} scheduled for camera {stream_id}: "
                    f"reason={stop_reason}, next_attempt={next_retry_at.isoformat()}, "
                    f"interval={retry_interval_with_jitter}s, context={error_context}"
                )
                
                # Track in memory
                async with self._retry_lock:
                    self._active_retries[str(stream_id)] = {
                        'retry_count': next_retry_count,
                        'next_retry_at': next_retry_at,
                        'stop_reason': stop_reason,
                        'scheduled_at': datetime.now(ZoneInfo("Africa/Cairo"))
                    }
                
                # Alert if too many failures
                if next_retry_count >= self.alert_threshold:
                    await self._alert_excessive_failures(stream_id, next_retry_count, stop_reason)
                
                return True
            else:
                logger.error(f"Failed to schedule retry for {stream_id} - camera may not exist or is user-stopped")
                return False
                
        except Exception as e:
            logger.error(f"Error scheduling retry for {stream_id}: {e}", exc_info=True)
            return False
    
    async def get_cameras_ready_for_retry(self) -> List[Dict[str, Any]]:
        """
        Get cameras that are ready for retry attempt.
        
        Returns:
            List of camera records ready for reconnection
        """
        query = """
            SELECT 
                vs.stream_id,
                vs.workspace_id,
                vs.user_id,
                vs.name,
                vs.path,
                vs.type,
                vs.stop_reason,
                vs.retry_count,
                vs.last_retry_at,
                vs.next_retry_at,
                vs.location,
                vs.area,
                vs.building,
                vs.zone,
                vs.floor_level,
                vs.latitude,
                vs.longitude,
                u.username,
                u.is_active as user_active,
                u.is_subscribed,
                w.is_active as workspace_active,
                w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.is_streaming = TRUE
              AND vs.status = 'error'
              AND vs.stop_reason IS NOT NULL
              AND vs.stop_reason != 'user_action'  -- CRITICAL: Exclude user stops
              AND vs.next_retry_at IS NOT NULL
              AND vs.next_retry_at <= NOW()
              AND u.is_active = TRUE
              AND w.is_active = TRUE
              AND (u.is_subscribed = TRUE OR u.role = 'admin')
            ORDER BY vs.next_retry_at ASC, vs.retry_count ASC
        """
        
        try:
            cameras = await self.db_manager.execute_query(query, fetch_all=True)
            
            if cameras:
                logger.info(
                    f"🔄 Found {len(cameras)} cameras ready for retry: "
                    f"{[str(c['stream_id']) for c in cameras]}"
                )
            
            return cameras or []
            
        except Exception as e:
            logger.error(f"Error fetching cameras for retry: {e}", exc_info=True)
            return []
    
    async def mark_retry_success(
        self,
        stream_id: UUID,
        previous_retry_count: int
    ):
        """
        Mark a retry as successful and reset counters.
        
        Args:
            stream_id: Camera stream UUID
            previous_retry_count: Number of retries it took to succeed
        """
        try:
            query = """
                UPDATE video_stream
                SET retry_count = 0,
                    last_retry_at = NULL,
                    next_retry_at = NULL,
                    stop_reason = NULL,
                    stopped_at = NULL,
                    stopped_by = NULL,
                    status = 'active',
                    is_streaming = TRUE,
                    updated_at = NOW()
                WHERE stream_id = $1
            """
            
            await self.db_manager.execute_query(query, (stream_id,))
            
            logger.info(
                f"✅ Camera {stream_id} reconnected successfully after {previous_retry_count} retries"
            )
            
            # Remove from active retry tracking
            async with self._retry_lock:
                self._active_retries.pop(str(stream_id), None)
            
            # Log success for analytics
            await self._log_retry_success(stream_id, previous_retry_count)
            
        except Exception as e:
            logger.error(f"Error marking retry success for {stream_id}: {e}", exc_info=True)
    
    async def mark_retry_failure(
        self,
        stream_id: UUID,
        error_context: str
    ):
        """
        Mark a retry attempt as failed and schedule next retry.
        
        Args:
            stream_id: Camera stream UUID
            error_context: Description of the failure
        """
        try:
            # Get current retry count
            query = "SELECT retry_count, stop_reason FROM video_stream WHERE stream_id = $1"
            result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
            
            if not result:
                logger.error(f"Camera {stream_id} not found for retry failure marking")
                return
            
            current_retry_count = result['retry_count'] or 0
            stop_reason = result['stop_reason']
            
            # Schedule next retry
            await self.schedule_retry(
                stream_id=stream_id,
                stop_reason=stop_reason,
                current_retry_count=current_retry_count,
                error_context=error_context
            )
            
            logger.warning(
                f"❌ Retry #{current_retry_count + 1} failed for camera {stream_id}: {error_context}"
            )
            
        except Exception as e:
            logger.error(f"Error marking retry failure for {stream_id}: {e}", exc_info=True)
        
    async def cancel_retry(
        self,
        stream_id: UUID,
        cancel_reason: str = "user_action"
    ):
        """
        Cancel any pending retry for a camera.
        
        Args:
            stream_id: Camera stream ID
            cancel_reason: Reason for cancellation
        """
        try:
            # CRITICAL FIX: Include stop_reason in the UPDATE
            # This prevents the database trigger from blocking the query
            query = """
                UPDATE video_stream
                SET retry_count = 0,
                    last_retry_at = NULL,
                    next_retry_at = NULL,
                    is_streaming = FALSE,
                    stop_reason = $2,
                    auto_retry_enabled = FALSE,
                    updated_at = NOW()
                WHERE stream_id = $1
            """
            # status = 'inactive'
            
            await self.db_manager.execute_query(query, (stream_id, cancel_reason))

            # Remove from active tracking
            async with self._retry_lock:
                self._active_retries.pop(str(stream_id), None)
            
            logger.info(
                f"🚫 Retry cancelled for camera {stream_id}: {cancel_reason}"
            )
            
        except Exception as e:
            logger.error(
                f"Error cancelling retry for {stream_id}: {e}",
                exc_info=True
            )
            # Don't raise - cancellation failure shouldn't block the stop

    async def _alert_excessive_failures(
        self,
        stream_id: UUID,
        retry_count: int,
        stop_reason: str
    ):
        """Send alert when camera has too many failures"""
        query = """
            INSERT INTO notifications (notification_id, workspace_id, user_id, stream_id, 
                                     camera_name, status, message, is_read)
            SELECT 
                uuid_generate_v4(),
                vs.workspace_id,
                vs.user_id,
                vs.stream_id,
                vs.name,
                'urgent',
                $1,
                FALSE
            FROM video_stream vs
            WHERE vs.stream_id = $2
        """
        
        message = (
            f"⚠️ Camera has failed {retry_count} times ({stop_reason}). "
            f"System will keep retrying, but please check the camera."
        )
        
        try:
            await self.db_manager.execute_query(query, (message, stream_id))
            logger.warning(f"📧 Alert sent for camera {stream_id} after {retry_count} failures")
        except Exception as e:
            logger.error(f"Error sending excessive failure alert: {e}")
    
    async def _log_retry_success(self, stream_id: UUID, retry_count: int):
        """Log successful retry for analytics"""
        query = """
            INSERT INTO logs (log_id, user_id, workspace_id, action_type, status, content)
            SELECT 
                uuid_generate_v4(),
                vs.user_id,
                vs.workspace_id,
                'camera_retry_success',
                'success',
                jsonb_build_object(
                    'stream_id', $1::text,
                    'camera_name', vs.name,
                    'retry_count', $2,
                    'reconnect_duration_seconds', 
                        EXTRACT(EPOCH FROM (NOW() - vs.stopped_at))
                )
            FROM video_stream vs
            WHERE vs.stream_id = $1
        """
        
        try:
            await self.db_manager.execute_query(query, (stream_id, retry_count))
        except Exception as e:
            logger.error(f"Error logging retry success: {e}")

# Global instance
retry_service = CameraRetryService()