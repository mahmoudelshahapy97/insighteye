# app/services/fire_detection_service.py
import logging
from typing import Any, Dict, Optional, List, Union
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from app.services.database import db_manager

logger = logging.getLogger(__name__)


class FireDetectionService:
    """
    Service layer for fire detection database operations.
    Encapsulates all fire detection logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def create_or_update_fire_detection_state(
        self,
        stream_id: UUID,
        fire_status: str,
        last_detection_time: Optional[datetime] = None,
        last_notification_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Create or update fire detection state."""
        # query = """
        #     INSERT INTO fire_detection_state 
        #     (stream_id, fire_status, last_detection_time, last_notification_time, updated_at)
        #     VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
        #     ON CONFLICT (stream_id)
        #     DO UPDATE SET
        #         fire_status = EXCLUDED.fire_status,
        #         last_detection_time = EXCLUDED.last_detection_time,
        #         last_notification_time = EXCLUDED.last_notification_time,
        #         updated_at = CURRENT_TIMESTAMP
        #     RETURNING *
        # """

        query = """
            INSERT INTO fire_detection_state 
            (stream_id, fire_status, last_detection_time, last_notification_time, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (stream_id)
            DO UPDATE SET
                fire_status = EXCLUDED.fire_status,
                last_detection_time = EXCLUDED.last_detection_time,
                last_notification_time = CASE 
                    WHEN EXCLUDED.last_notification_time IS NOT NULL 
                    THEN EXCLUDED.last_notification_time 
                    ELSE fire_detection_state.last_notification_time  -- ← Keep existing!
                END,
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
        """
        result = await self.db_manager.execute_query(
            query,
            (stream_id, fire_status, last_detection_time, last_notification_time),
            fetch_one=True,
        )
        
        if result:
            logger.debug(f"Fire detection state updated for stream {stream_id}: {fire_status}")
        
        return result


    async def get_fire_detection_state(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Get fire detection state for a stream."""
        query = "SELECT * FROM fire_detection_state WHERE stream_id = $1"
        return await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)

    async def get_all_fire_detection_states(
        self, 
        workspace_id: Optional[UUID] = None,
        active_only: bool = False
    ) -> List[Dict[str, Any]]:
        """Get all fire detection states, optionally filtered by workspace or active status."""
        if workspace_id and active_only:
            query = """
                SELECT fds.*, vs.name as camera_name, vs.workspace_id
                FROM fire_detection_state fds
                JOIN video_stream vs ON fds.stream_id = vs.stream_id
                WHERE vs.workspace_id = $1 
                AND fds.fire_status IN ('fire', 'smoke')
                ORDER BY fds.last_detection_time DESC
            """
            params = (workspace_id,)
        elif workspace_id:
            query = """
                SELECT fds.*, vs.name as camera_name, vs.workspace_id
                FROM fire_detection_state fds
                JOIN video_stream vs ON fds.stream_id = vs.stream_id
                WHERE vs.workspace_id = $1
                ORDER BY fds.updated_at DESC
            """
            params = (workspace_id,)
        elif active_only:
            query = """
                SELECT fds.*, vs.name as camera_name, vs.workspace_id
                FROM fire_detection_state fds
                JOIN video_stream vs ON fds.stream_id = vs.stream_id
                WHERE fds.fire_status IN ('fire', 'smoke')
                ORDER BY fds.last_detection_time DESC
            """
            params = ()
        else:
            query = """
                SELECT fds.*, vs.name as camera_name, vs.workspace_id
                FROM fire_detection_state fds
                JOIN video_stream vs ON fds.stream_id = vs.stream_id
                ORDER BY fds.updated_at DESC
            """
            params = ()
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def delete_fire_detection_state(self, stream_id: UUID) -> bool:
        """Delete fire detection state for a stream."""
        query = "DELETE FROM fire_detection_state WHERE stream_id = $1"
        rows = await self.db_manager.execute_query(query, (stream_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted fire detection state for stream {stream_id}")
        
        return rows > 0

    async def upsert_fire_detection_state(
        self,
        stream_id: Union[str, UUID],
        fire_status: str,
        last_detection_time: Optional[datetime] = None,
        last_notification_time: Optional[datetime] = None
    ) -> bool:
        """Create or update fire detection state"""
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        detection_time = last_detection_time or now
        
        query = """
            INSERT INTO fire_detection_state
            (stream_id, fire_status, last_detection_time, last_notification_time, 
             created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (stream_id) DO UPDATE SET
                fire_status = EXCLUDED.fire_status,
                last_detection_time = EXCLUDED.last_detection_time,
                last_notification_time = CASE 
                    WHEN EXCLUDED.last_notification_time IS NOT NULL 
                    THEN EXCLUDED.last_notification_time 
                    ELSE fire_detection_state.last_notification_time 
                END,
                updated_at = EXCLUDED.updated_at
        """
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (stream_id_obj, fire_status, detection_time, last_notification_time, now, now),
            return_rowcount=True
        )
        return rows_affected > 0
    
    async def clear_fire_detection_state(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Clear/reset fire detection state to 'no detection'."""
        query = """
            UPDATE fire_detection_state
            SET fire_status = 'no detection',
                last_detection_time = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE stream_id = $1
            RETURNING *
        """
        result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
        
        if result:
            logger.info(f"Cleared fire detection state for stream {stream_id}")
        
        return result

    async def cleanup_old_fire_states(self):
        """Clean up old fire detection states"""
        try:
            await self.db_manager.execute_query(
                """DELETE FROM fire_detection_state 
                WHERE stream_id NOT IN (SELECT stream_id FROM video_stream)"""
            )
            
            day_ago = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(days=1)
            await self.db_manager.execute_query(
                """UPDATE fire_detection_state 
                SET fire_status = 'no detection', last_notification_time = NULL 
                WHERE last_notification_time < $1""",
                (day_ago,)
            )
            
            logger.debug("Cleaned up old fire detection states")
            
        except Exception as e:
            logger.error(f"Error cleaning up fire states: {e}")

    async def get_fire_detection_history(
        self,
        stream_id: UUID,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get fire detection history for a stream within a time range."""
        if start_time and end_time:
            query = """
                SELECT * FROM fire_detection_state
                WHERE stream_id = $1
                AND last_detection_time BETWEEN $2 AND $3
                ORDER BY last_detection_time DESC
            """
            params = (stream_id, start_time, end_time)
        elif start_time:
            query = """
                SELECT * FROM fire_detection_state
                WHERE stream_id = $1
                AND last_detection_time >= $2
                ORDER BY last_detection_time DESC
            """
            params = (stream_id, start_time)
        else:
            query = """
                SELECT * FROM fire_detection_state
                WHERE stream_id = $1
                AND last_detection_time IS NOT NULL
                ORDER BY last_detection_time DESC
            """
            params = (stream_id,)
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def get_workspace_fire_summary(self, workspace_id: UUID) -> Dict[str, Any]:
        """Get summary of fire detection for a workspace."""
        query = """
            SELECT 
                COUNT(*) as total_streams,
                COUNT(CASE WHEN fds.fire_status = 'fire' THEN 1 END) as active_fires,
                COUNT(CASE WHEN fds.fire_status = 'smoke' THEN 1 END) as smoke_detections,
                COUNT(CASE WHEN fds.fire_status = 'no detection' THEN 1 END) as clear_streams,
                MAX(fds.last_detection_time) as last_detection_time
            FROM fire_detection_state fds
            JOIN video_stream vs ON fds.stream_id = vs.stream_id
            WHERE vs.workspace_id = $1
        """
        result = await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)
        
        if not result:
            return {
                "total_streams": 0,
                "active_fires": 0,
                "smoke_detections": 0,
                "clear_streams": 0,
                "last_detection_time": None
            }
        
        return {
            "total_streams": result.get("total_streams", 0),
            "active_fires": result.get("active_fires", 0),
            "smoke_detections": result.get("smoke_detections", 0),
            "clear_streams": result.get("clear_streams", 0),
            "last_detection_time": result.get("last_detection_time")
        }

    async def load_fire_state_on_stream_start(self, stream_id: UUID) -> Dict[str, Any]:
        """Load persistent fire state when stream starts - returns state info for stream manager"""
        stream_id_str = str(stream_id)
        try:
            # Load from database
            persistent_state = await self.get_fire_detection_state(stream_id)
            
            # Calculate cooldown info
            cooldown_info = {
                'fire_status': persistent_state['fire_status'],
                'should_restore_cooldown': False,
                'cooldown_remaining': 0,
                'last_notification_timestamp': None
            }
            
            if persistent_state['last_notification_time']:
                last_notification_timestamp = persistent_state['last_notification_time'].timestamp()
                import time
                current_time = time.time()
                time_since_last = current_time - last_notification_timestamp
                
                # Check if within cooldown period (5 minutes = 300 seconds)
                fire_cooldown_duration = 300.0
                if time_since_last < fire_cooldown_duration:
                    cooldown_info['should_restore_cooldown'] = True
                    cooldown_info['cooldown_remaining'] = fire_cooldown_duration - time_since_last
                    cooldown_info['last_notification_timestamp'] = last_notification_timestamp
                    
                    logger.info(f"Fire cooldown should be restored for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_remaining={cooldown_info['cooldown_remaining']/60:.1f}min")
                else:
                    logger.info(f"Fire state loaded for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_expired")
            else:
                logger.info(f"Fire state loaded for {stream_id_str}: "
                        f"fire_status='{persistent_state['fire_status']}', "
                        f"no_previous_notifications")
            
            return cooldown_info
            
        except Exception as e:
            logger.error(f"Error loading fire state for {stream_id}: {e}")
            return {
                'fire_status': 'no detection',
                'should_restore_cooldown': False,
                'cooldown_remaining': 0,
                'last_notification_timestamp': None
            }

fire_detection_service = FireDetectionService()