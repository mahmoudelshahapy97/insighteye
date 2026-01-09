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

    async def create_video_stream(
        self,
        workspace_id: UUID,
        user_id: UUID,
        name: str,
        path: str,
        stream_type: str = "local",
        **kwargs,
    ) -> Dict[str, Any]:
        """Create a new video stream."""
        query = """
            INSERT INTO video_stream 
            (workspace_id, user_id, name, path, type, location, area, building, 
             floor_level, zone, latitude, longitude, count_threshold_greater, 
             count_threshold_less, alert_enabled)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING *
        """
        params = (
            workspace_id,
            user_id,
            name,
            path,
            stream_type,
            kwargs.get("location"),
            kwargs.get("area"),
            kwargs.get("building"),
            kwargs.get("floor_level"),
            kwargs.get("zone"),
            kwargs.get("latitude"),
            kwargs.get("longitude"),
            kwargs.get("count_threshold_greater"),
            kwargs.get("count_threshold_less"),
            kwargs.get("alert_enabled", False),
        )
        
        try:
            result = await self.db_manager.execute_query(query, params, fetch_one=True)
            if not result:
                raise HTTPException(status_code=500, detail="Failed to create video stream")
            
            logger.info(f"Created video stream {result['stream_id']} ({name})")
            return result
            
        except asyncpg.UniqueViolationError:
            logger.error(f"Video stream with name '{name}' already exists in workspace {workspace_id}")
            raise HTTPException(status_code=409, detail="Video stream with this name already exists")
        except asyncpg.PostgresError as e:
            logger.error(f"Database error creating video stream: {e}")
            raise HTTPException(status_code=500, detail="Database error")

    async def get_video_stream_by_id(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Retrieve video stream by ID."""
        query = "SELECT * FROM video_stream WHERE stream_id = $1"
        return await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)

    async def get_video_stream_by_name(
        self,
        name: str,
        workspace_id: UUID
    ) -> Optional[Dict[str, Any]]:
        """Retrieve video stream by name within a workspace."""
        query = """
            SELECT * FROM video_stream 
            WHERE name = $1 AND workspace_id = $2
        """
        return await self.db_manager.execute_query(query, (name, workspace_id), fetch_one=True)

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

    async def get_active_streams(
        self,
        workspace_id: Optional[UUID] = None
    ) -> List[Dict[str, Any]]:
        """Get all currently active/streaming video streams."""
        if workspace_id:
            query = """
                SELECT * FROM video_stream 
                WHERE workspace_id = $1 
                AND status = 'active' 
                AND is_streaming = TRUE
                ORDER BY last_activity DESC
            """
            params = (workspace_id,)
        else:
            query = """
                SELECT * FROM video_stream 
                WHERE status = 'active' 
                AND is_streaming = TRUE
                ORDER BY last_activity DESC
            """
            params = ()
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def update_video_stream(
        self,
        stream_id: UUID,
        **updates
    ) -> Optional[Dict[str, Any]]:
        """Update video stream fields."""
        if not updates:
            return None
        
        # Build dynamic update query
        set_clause = ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates.keys())])
        query = f"""
            UPDATE video_stream 
            SET {set_clause}, last_activity = CURRENT_TIMESTAMP
            WHERE stream_id = $1
            RETURNING *
        """
        params = (stream_id, *updates.values())
        
        try:
            result = await self.db_manager.execute_query(query, params, fetch_one=True)
            if not result:
                raise HTTPException(status_code=404, detail="Video stream not found")
            
            logger.debug(f"Updated video stream {stream_id}")
            return result
            
        except asyncpg.PostgresError as e:
            logger.error(f"Database error updating video stream: {e}")
            raise HTTPException(status_code=500, detail="Database error")

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
        
    async def update_stream_alert_config(
        self,
        stream_id: UUID,
        alert_enabled: Optional[bool] = None,
        count_threshold_greater: Optional[int] = None,
        count_threshold_less: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update alert configuration for a video stream."""
        updates = {}
        if alert_enabled is not None:
            updates["alert_enabled"] = alert_enabled
        if count_threshold_greater is not None:
            updates["count_threshold_greater"] = count_threshold_greater
        if count_threshold_less is not None:
            updates["count_threshold_less"] = count_threshold_less

        if not updates:
            return None

        set_clause = ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates.keys())])
        query = f"UPDATE video_stream SET {set_clause} WHERE stream_id = $1 RETURNING *"
        params = (stream_id, *updates.values())

        try:
            result = await self.db_manager.execute_query(query, params, fetch_one=True)
            if not result:
                raise HTTPException(status_code=404, detail="Video stream not found")
            
            logger.info(f"Updated alert config for stream {stream_id}")
            return result
            
        except asyncpg.PostgresError as e:
            logger.error(f"Database error updating alert config: {e}")
            raise HTTPException(status_code=500, detail="Database error")

    async def update_stream_location(
        self,
        stream_id: UUID,
        location: Optional[str] = None,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        """Update location information for a video stream."""
        updates = {}
        if location is not None:
            updates["location"] = location
        if area is not None:
            updates["area"] = area
        if building is not None:
            updates["building"] = building
        if floor_level is not None:
            updates["floor_level"] = floor_level
        if zone is not None:
            updates["zone"] = zone
        if latitude is not None:
            updates["latitude"] = latitude
        if longitude is not None:
            updates["longitude"] = longitude
        
        if not updates:
            return None
        
        return await self.update_video_stream(stream_id, **updates)

    async def delete_video_stream(self, stream_id: UUID) -> bool:
        """Delete a video stream."""
        query = "DELETE FROM video_stream WHERE stream_id = $1"
        rows = await self.db_manager.execute_query(query, (stream_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted video stream {stream_id}")
        
        return rows > 0

    async def delete_workspace_streams(self, workspace_id: UUID) -> int:
        """Delete all video streams in a workspace."""
        query = "DELETE FROM video_stream WHERE workspace_id = $1"
        rows = await self.db_manager.execute_query(query, (workspace_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} video streams from workspace {workspace_id}")
        
        return rows

    async def get_streams_with_alerts_enabled(
        self, 
        workspace_id: UUID
    ) -> List[Dict[str, Any]]:
        """Get all streams with alerts enabled in a workspace."""
        query = """
            SELECT * FROM video_stream 
            WHERE workspace_id = $1 AND alert_enabled = TRUE
            ORDER BY name
        """
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def get_inactive_streams(
        self,
        workspace_id: Optional[UUID] = None,
        minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """Get streams that haven't had activity in specified minutes."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        if workspace_id:
            query = """
                SELECT * FROM video_stream
                WHERE workspace_id = $1
                AND (last_activity < $2 OR last_activity IS NULL)
                AND is_streaming = TRUE
                ORDER BY last_activity DESC
            """
            params = (workspace_id, cutoff_time)
        else:
            query = """
                SELECT * FROM video_stream
                WHERE (last_activity < $1 OR last_activity IS NULL)
                AND is_streaming = TRUE
                ORDER BY last_activity DESC
            """
            params = (cutoff_time,)
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def get_workspace_stream_summary(self, workspace_id: UUID) -> Dict[str, Any]:
        """Get summary statistics for streams in a workspace."""
        query = """
            SELECT 
                COUNT(*) as total_streams,
                COUNT(CASE WHEN status = 'active' THEN 1 END) as active_streams,
                COUNT(CASE WHEN is_streaming = TRUE THEN 1 END) as streaming_now,
                COUNT(CASE WHEN alert_enabled = TRUE THEN 1 END) as alerts_enabled,
                COUNT(CASE WHEN type = 'rtsp' THEN 1 END) as rtsp_streams,
                COUNT(CASE WHEN type = 'local' THEN 1 END) as local_streams,
                MAX(last_activity) as last_activity_time
            FROM video_stream
            WHERE workspace_id = $1
        """
        result = await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)
        
        if not result:
            return {
                "total_streams": 0,
                "active_streams": 0,
                "streaming_now": 0,
                "alerts_enabled": 0,
                "rtsp_streams": 0,
                "local_streams": 0,
                "last_activity_time": None
            }
        
        return {
            "total_streams": result.get("total_streams", 0),
            "active_streams": result.get("active_streams", 0),
            "streaming_now": result.get("streaming_now", 0),
            "alerts_enabled": result.get("alerts_enabled", 0),
            "rtsp_streams": result.get("rtsp_streams", 0),
            "local_streams": result.get("local_streams", 0),
            "last_activity_time": result.get("last_activity_time")
        }

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
        
        ✅ FIXED: Proper state transitions for all stop reasons
        """
        valid_reasons = [
            'user_action', 'connection_error', 'system_error', 
            'timeout', 'manual_restart'
        ]
        
        if stop_reason not in valid_reasons:
            logger.error(f"Invalid stop_reason: {stop_reason}")
            return False
        
        try:
            # ✅ Check current state
            current_state = await self.db_manager.execute_query(
                """SELECT stop_reason, is_streaming, status, retry_count
                FROM video_stream 
                WHERE stream_id = $1""",
                (stream_id,),
                fetch_one=True
            )
            
            if not current_state:
                logger.error(f"Camera {stream_id} not found")
                return False
            
            # ✅ If already user-stopped, NEVER overwrite
            if current_state['stop_reason'] == 'user_action':
                logger.info(
                    f"⏭️ Camera {stream_id} already user-stopped. "
                    f"Ignoring record_camera_stop with reason '{stop_reason}'"
                )
                return True
            
            if stop_reason == 'user_action':
                # ✅ User stop: Complete shutdown
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
                """
                
                await self.db_manager.execute_query(
                    query, 
                    (stop_reason, stopped_by, stream_id)
                )
                
                logger.info(
                    f"✅ User stop recorded: {stream_id} -> "
                    f"is_streaming=FALSE, will NOT restart"
                )
                
            else:
                # ✅ System error: Keep running, schedule retry
                current_retry = current_state.get('retry_count', 0)
                
                query = """
                    UPDATE video_stream 
                    SET stop_reason = $1,
                        stopped_by = $2,
                        stopped_at = NOW(),
                        status = 'error',
                        is_streaming = TRUE,
                        retry_count = $3,
                        last_retry_at = NOW(),
                        updated_at = NOW()
                    WHERE stream_id = $4
                """
                
                await self.db_manager.execute_query(
                    query, 
                    (stop_reason, stopped_by, current_retry, stream_id)
                )
                
                # ✅ Trigger retry scheduling via database trigger
                # (auto_schedule_retry trigger will set next_retry_at)
                
                logger.warning(
                    f"⚠️ System error recorded: {stream_id} -> "
                    f"is_streaming=TRUE, will retry (reason: {stop_reason}, "
                    f"attempt #{current_retry + 1})"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording camera stop: {e}", exc_info=True)
            return False

    async def _log_camera_issue(
        self, 
        stream_id: UUID, 
        issue_type: str, 
        context: Optional[str]
    ):
        """Log camera issues for monitoring"""
        query = """
            INSERT INTO security_events (event_id, user_id, workspace_id, event_type, severity, event_data, created_at)
            SELECT 
                uuid_generate_v4(),
                user_id,
                workspace_id,
                $1,
                'medium',
                jsonb_build_object(
                    'stream_id', $2::text,
                    'issue_type', $1,
                    'context', $3,
                    'camera_name', name
                ),
                NOW()
            FROM video_stream
            WHERE stream_id = $2
        """
        
        await self.db_manager.execute_query(
            query,
            (f'camera_{issue_type}', stream_id, context)
        )


    async def get_frequently_failing_cameras(
        self,
        workspace_id: UUID,
        hours: int = 24,
        min_failures: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Get cameras that frequently have connection issues.
        Useful for identifying problematic cameras.
        """
        query = """
            SELECT 
                stream_id,
                name,
                location,
                COUNT(*) as failure_count,
                ARRAY_AGG(stopped_at ORDER BY stopped_at DESC) as failure_times,
                ARRAY_AGG(stop_reason) as failure_reasons,
                MAX(stopped_at) as last_failure
            FROM video_stream
            WHERE workspace_id = $1
            AND stopped_at > NOW() - INTERVAL '$2 hours'
            AND stop_reason IN ('connection_error', 'timeout', 'system_error')
            GROUP BY stream_id, name, location
            HAVING COUNT(*) >= $3
            ORDER BY failure_count DESC, last_failure DESC
        """
        
        return await self.db_manager.execute_query(
            query,
            (workspace_id, hours, min_failures),
            fetch_all=True
        )
        
video_stream_service = VideoStreamService()
