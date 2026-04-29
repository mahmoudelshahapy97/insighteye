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

fire_detection_service = FireDetectionService()