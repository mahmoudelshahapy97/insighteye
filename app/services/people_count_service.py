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

    async def get_all_people_count_alert_states(
        self,
        workspace_id: Optional[UUID] = None,
        alert_active: bool = False
    ) -> List[Dict[str, Any]]:
        """Get all people count alert states, optionally filtered by workspace or alert status."""
        if workspace_id and alert_active:
            query = """
                SELECT pcas.*, vs.name as camera_name, vs.workspace_id,
                       vs.count_threshold_greater, vs.count_threshold_less,
                       vs.alert_enabled
                FROM people_count_alert_state pcas
                JOIN video_stream vs ON pcas.stream_id = vs.stream_id
                WHERE vs.workspace_id = $1 
                AND vs.alert_enabled = TRUE
                AND (
                    (vs.count_threshold_greater IS NOT NULL AND pcas.last_count > vs.count_threshold_greater)
                    OR (vs.count_threshold_less IS NOT NULL AND pcas.last_count < vs.count_threshold_less)
                )
                ORDER BY pcas.last_notification_time DESC
            """
            params = (workspace_id,)
        elif workspace_id:
            query = """
                SELECT pcas.*, vs.name as camera_name, vs.workspace_id,
                       vs.count_threshold_greater, vs.count_threshold_less,
                       vs.alert_enabled
                FROM people_count_alert_state pcas
                JOIN video_stream vs ON pcas.stream_id = vs.stream_id
                WHERE vs.workspace_id = $1
                ORDER BY pcas.updated_at DESC
            """
            params = (workspace_id,)
        elif alert_active:
            query = """
                SELECT pcas.*, vs.name as camera_name, vs.workspace_id,
                       vs.count_threshold_greater, vs.count_threshold_less,
                       vs.alert_enabled
                FROM people_count_alert_state pcas
                JOIN video_stream vs ON pcas.stream_id = vs.stream_id
                WHERE vs.alert_enabled = TRUE
                AND (
                    (vs.count_threshold_greater IS NOT NULL AND pcas.last_count > vs.count_threshold_greater)
                    OR (vs.count_threshold_less IS NOT NULL AND pcas.last_count < vs.count_threshold_less)
                )
                ORDER BY pcas.last_notification_time DESC
            """
            params = ()
        else:
            query = """
                SELECT pcas.*, vs.name as camera_name, vs.workspace_id,
                       vs.count_threshold_greater, vs.count_threshold_less,
                       vs.alert_enabled
                FROM people_count_alert_state pcas
                JOIN video_stream vs ON pcas.stream_id = vs.stream_id
                ORDER BY pcas.updated_at DESC
            """
            params = ()
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def delete_people_count_alert_state(self, stream_id: UUID) -> bool:
        """Delete people count alert state for a stream."""
        query = "DELETE FROM people_count_alert_state WHERE stream_id = $1"
        rows = await self.db_manager.execute_query(query, (stream_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted people count alert state for stream {stream_id}")
        
        return rows > 0

    async def clear_people_count_alert_state(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Clear/reset people count alert state."""
        query = """
            UPDATE people_count_alert_state
            SET last_count = NULL,
                last_threshold_type = NULL,
                last_notification_time = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE stream_id = $1
            RETURNING *
        """
        result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
        
        if result:
            logger.info(f"Cleared people count alert state for stream {stream_id}")
        
        return result

    async def get_people_count_history(
        self,
        stream_id: UUID,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get people count alert history for a stream within a time range."""
        if start_time and end_time:
            query = """
                SELECT * FROM people_count_alert_state
                WHERE stream_id = $1
                AND last_notification_time BETWEEN $2 AND $3
                ORDER BY last_notification_time DESC
            """
            params = (stream_id, start_time, end_time)
        elif start_time:
            query = """
                SELECT * FROM people_count_alert_state
                WHERE stream_id = $1
                AND last_notification_time >= $2
                ORDER BY last_notification_time DESC
            """
            params = (stream_id, start_time)
        else:
            query = """
                SELECT * FROM people_count_alert_state
                WHERE stream_id = $1
                AND last_notification_time IS NOT NULL
                ORDER BY last_notification_time DESC
            """
            params = (stream_id,)
        
        return await self.db_manager.execute_query(query, params, fetch_all=True)

    async def get_workspace_people_count_summary(self, workspace_id: UUID) -> Dict[str, Any]:
        """Get summary of people count alerts for a workspace."""
        query = """
            SELECT 
                COUNT(*) as total_streams,
                COUNT(CASE WHEN vs.alert_enabled = TRUE THEN 1 END) as alerts_enabled,
                COUNT(CASE WHEN 
                    vs.alert_enabled = TRUE AND (
                        (vs.count_threshold_greater IS NOT NULL AND pcas.last_count > vs.count_threshold_greater)
                        OR (vs.count_threshold_less IS NOT NULL AND pcas.last_count < vs.count_threshold_less)
                    ) THEN 1 END) as active_alerts,
                AVG(pcas.last_count) as avg_count,
                MAX(pcas.last_count) as max_count,
                MAX(pcas.last_notification_time) as last_alert_time
            FROM people_count_alert_state pcas
            JOIN video_stream vs ON pcas.stream_id = vs.stream_id
            WHERE vs.workspace_id = $1
        """
        result = await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)
        
        if not result:
            return {
                "total_streams": 0,
                "alerts_enabled": 0,
                "active_alerts": 0,
                "avg_count": 0,
                "max_count": 0,
                "last_alert_time": None
            }
        
        return {
            "total_streams": result.get("total_streams", 0),
            "alerts_enabled": result.get("alerts_enabled", 0),
            "active_alerts": result.get("active_alerts", 0),
            "avg_count": float(result.get("avg_count", 0)) if result.get("avg_count") else 0,
            "max_count": result.get("max_count", 0),
            "last_alert_time": result.get("last_alert_time")
        }

    async def get_streams_exceeding_thresholds(
        self,
        workspace_id: UUID
    ) -> List[Dict[str, Any]]:
        """Get all streams currently exceeding their thresholds."""
        query = """
            SELECT 
                pcas.*,
                vs.name as camera_name,
                vs.count_threshold_greater,
                vs.count_threshold_less,
                vs.alert_enabled,
                CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL AND pcas.last_count > vs.count_threshold_greater 
                    THEN 'greater_than'
                    WHEN vs.count_threshold_less IS NOT NULL AND pcas.last_count < vs.count_threshold_less 
                    THEN 'less_than'
                    ELSE NULL
                END as threshold_violated
            FROM people_count_alert_state pcas
            JOIN video_stream vs ON pcas.stream_id = vs.stream_id
            WHERE vs.workspace_id = $1
            AND vs.alert_enabled = TRUE
            AND (
                (vs.count_threshold_greater IS NOT NULL AND pcas.last_count > vs.count_threshold_greater)
                OR (vs.count_threshold_less IS NOT NULL AND pcas.last_count < vs.count_threshold_less)
            )
            ORDER BY pcas.last_count DESC
        """
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

people_count_service = PeopleCountService()
