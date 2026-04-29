# app/services/people_count_service.py
import asyncpg
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
from datetime import datetime

from app.services.database import db_manager

logger = logging.getLogger(__name__)


class PeopleCountService:
    """
    Service layer for people count alert database operations.
    Encapsulates all people count logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def create_or_update_people_count_alert_state(
        self,
        stream_id: UUID,
        last_count: Optional[int] = None,
        last_threshold_type: Optional[str] = None,
        last_notification_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Create or update people count alert state."""
        query = """
            INSERT INTO people_count_alert_state 
            (stream_id, last_count, last_threshold_type, last_notification_time, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (stream_id)
            DO UPDATE SET
                last_count = EXCLUDED.last_count,
                last_threshold_type = EXCLUDED.last_threshold_type,
                last_notification_time = EXCLUDED.last_notification_time,
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
        """
        result = await self.db_manager.execute_query(
            query,
            (stream_id, last_count, last_threshold_type, last_notification_time),
            fetch_one=True,
        )
        
        if result:
            logger.debug(f"People count alert state updated for stream {stream_id}: count={last_count}")
        
        return result

    async def get_people_count_alert_state(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Get people count alert state for a stream."""
        query = "SELECT * FROM people_count_alert_state WHERE stream_id = $1"
        return await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)

people_count_service = PeopleCountService()
