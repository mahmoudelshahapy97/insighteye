# app/services/video_stream_service.py
import asyncpg
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from fastapi import HTTPException
import traceback
from app.services.database import db_manager

logger = logging.getLogger(__name__)


class VideoStreamService:
    """
    Service layer for video stream database operations.
    Encapsulates all video stream logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def get_video_stream_by_id(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Retrieve video stream by ID."""
        query = "SELECT * FROM video_stream WHERE stream_id = $1"
        return await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)

    async def get_workspace_streams(
        self, 
        workspace_id: UUID, 
        status: Optional[str] = None,
        stream_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all video streams for a workspace with optional filters."""
        base_query = "SELECT * FROM video_stream WHERE workspace_id = $1"
        params = [workspace_id]
        conditions = []
        
        if status:
            conditions.append(f"status = ${len(params) + 1}")
            params.append(status)
        
        if stream_type:
            conditions.append(f"type = ${len(params) + 1}")
            params.append(stream_type)
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        base_query += " ORDER BY created_at DESC"
        
        return await self.db_manager.execute_query(base_query, tuple(params), fetch_all=True)

    async def get_user_streams(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> List[Dict[str, Any]]:
        """Get all video streams created by a user."""
        if workspace_id:
            query = """
                SELECT * FROM video_stream 
                WHERE user_id = $1 AND workspace_id = $2
                ORDER BY created_at DESC
            """
            params = (user_id, workspace_id)
        else:
            query = """
                SELECT * FROM video_stream 
                WHERE user_id = $1
                ORDER BY created_at DESC
            """
            params = (user_id,)
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def update_stream_status(
        self,
        stream_id: UUID,
        status: str,
        is_streaming: Optional[bool] = None,
        last_activity: Optional[datetime] = None
    ) -> bool:
        """
        Update stream status in database.
        
        CRITICAL: is_streaming should ONLY be set to False when explicitly requested,
        NOT automatically when status becomes 'error'.
        """
        try:
            if last_activity is None:
                last_activity = datetime.now(ZoneInfo("Africa/Cairo"))

            if is_streaming is not None:
                # Explicit is_streaming value provided
                query = """
                    UPDATE video_stream 
                    SET status = $1, is_streaming = $2, updated_at = $3, last_activity = $3
                    WHERE stream_id = $4
                """
                await self.db_manager.execute_query(query, (status, is_streaming, last_activity, stream_id))
                logger.info(f"Updated stream {stream_id}: status={status}, is_streaming={is_streaming}")
            else:
                # Only update status, leave is_streaming unchanged
                query = """
                    UPDATE video_stream 
                    SET status = $1, updated_at = NOW(), last_activity = NOW()
                    WHERE stream_id = $2
                """
                await self.db_manager.execute_query(query, (status, stream_id))
                logger.info(f"Updated stream {stream_id}: status={status} (is_streaming unchanged)")
            
            return True
        except Exception as e:
            logger.error(f"Error updating stream status: {e}", exc_info=True)
            return False

    async def search_streams(
        self,
        workspace_id: UUID,
        search_term: str
    ) -> List[Dict[str, Any]]:
        """Search streams by name, location, or other fields."""
        query = """
            SELECT * FROM video_stream
            WHERE workspace_id = $1
            AND (
                name ILIKE $2
                OR location ILIKE $2
                OR area ILIKE $2
                OR building ILIKE $2
                OR zone ILIKE $2
            )
            ORDER BY name
        """
        search_pattern = f"%{search_term}%"
        return await self.db_manager.execute_query(
            query, 
            (workspace_id, search_pattern), 
            fetch_all=True
        )

    async def record_camera_stop(
        self,
        stream_id: UUID,
        stop_reason: str,
        stopped_by: Optional[UUID] = None,
        additional_context: Optional[str] = None
    ) -> bool:
        """
        Record why and when a camera was stopped.
        
        ✅ ENHANCED: Prevents race conditions and state conflicts
        """
        valid_reasons = [
            'user_action', 'connection_error', 'system_error', 
            'timeout', 'manual_restart'
        ]
        
        if stop_reason not in valid_reasons:
            logger.error(f"Invalid stop_reason: {stop_reason}")
            return False
        
        try:
            # ✅ CRITICAL: Check current state FIRST
            current_state = await self.db_manager.execute_query(
                """SELECT 
                    stop_reason, 
                    is_streaming, 
                    status, 
                    retry_count,
                    auto_retry_enabled
                FROM video_stream 
                WHERE stream_id = $1
                FOR UPDATE""",  # ✅ Lock row to prevent race conditions
                (stream_id,),
                fetch_one=True
            )
            
            if not current_state:
                logger.error(f"Camera {stream_id} not found")
                return False
            
            # ✅ CRITICAL: If already user-stopped, NEVER overwrite
            if current_state['stop_reason'] == 'user_action':
                logger.info(
                    f"⏭️ Camera {stream_id} already user-stopped. "
                    f"Ignoring record_camera_stop with reason '{stop_reason}'"
                )
                return True
            
            if stop_reason == 'user_action':
                # ✅ User stop: Complete shutdown, disable retries
                query = """
                    UPDATE video_stream 
                    SET stop_reason = $1,
                        stopped_by = $2,
                        stopped_at = NOW(),
                        status = 'inactive',
                        is_streaming = FALSE,
                        retry_count = 0,
                        next_retry_at = NULL,
                        last_retry_at = NULL,
                        auto_retry_enabled = FALSE,
                        locked_by_server = NULL,
                        server_heartbeat = NULL,
                        updated_at = NOW()
                    WHERE stream_id = $3
                    RETURNING is_streaming, stop_reason, auto_retry_enabled
                """
                
                result = await self.db_manager.execute_query(
                    query, 
                    (stop_reason, stopped_by, stream_id),
                    fetch_one=True
                )
                
                # ✅ Verify the update worked
                if result and result['is_streaming'] == False:
                    logger.info(
                        f"✅ User stop recorded: {stream_id} -> "
                        f"is_streaming=FALSE, auto_retry=FALSE, will NOT restart"
                    )
                else:
                    logger.error(
                        f"❌ CRITICAL: User stop failed to set is_streaming=FALSE! "
                        f"Result: {result}"
                    )
                    return False
                
            else:
                # ✅ System error: Keep is_streaming=TRUE, enable retries
                current_retry = current_state.get('retry_count', 0)
                
                query = """
                    UPDATE video_stream 
                    SET stop_reason = $1,
                        stopped_by = $2,
                        stopped_at = NOW(),
                        status = 'error',
                        is_streaming = TRUE,
                        retry_count = $3 + 1,
                        last_retry_at = NOW(),
                        auto_retry_enabled = TRUE,
                        updated_at = NOW()
                    WHERE stream_id = $4
                    RETURNING is_streaming, stop_reason, retry_count, auto_retry_enabled
                """
                
                result = await self.db_manager.execute_query(
                    query, 
                    (stop_reason, stopped_by, current_retry, stream_id),
                    fetch_one=True
                )
                
                # ✅ Verify the update worked
                if result and result['is_streaming'] == True:
                    logger.warning(
                        f"⚠️ System error recorded: {stream_id} -> "
                        f"is_streaming=TRUE, retry_count={result['retry_count']}, "
                        f"auto_retry=TRUE, will retry (reason: {stop_reason})"
                    )
                else:
                    logger.error(
                        f"❌ CRITICAL: System error failed to keep is_streaming=TRUE! "
                        f"Result: {result}"
                    )
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording camera stop: {e}", exc_info=True)
            return False

video_stream_service = VideoStreamService()
