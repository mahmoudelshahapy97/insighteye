# app/services/notification_service.py
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.services.database import db_manager

logger = logging.getLogger(__name__)


class NotificationService:
    """
    Service layer for notification database operations.
    Encapsulates all notification logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def create_notification(
        self,
        workspace_id: UUID,
        user_id: UUID,
        status: str,
        message: str,
        stream_id: Optional[UUID] = None,
        camera_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new notification."""
        query = """
            INSERT INTO notifications 
            (workspace_id, user_id, stream_id, camera_name, status, message)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        """
        result = await self.db_manager.execute_query(
            query,
            (workspace_id, user_id, stream_id, camera_name, status, message),
            fetch_one=True,
        )
        
        if result:
            logger.debug(f"Notification created for user {user_id}: {message[:50]}...")
        
        return result

    async def get_notification_by_id(self, notification_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a specific notification by ID."""
        query = "SELECT * FROM notifications WHERE notification_id = $1"
        return await self.db_manager.execute_query(query, (notification_id,), fetch_one=True)

    async def get_user_notifications(
        self, 
        user_id: UUID, 
        workspace_id: Optional[UUID] = None, 
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get notifications for a user with optional filters."""
        base_query = "SELECT * FROM notifications WHERE user_id = $1"
        params = [user_id]
        conditions = []
        
        if workspace_id:
            conditions.append(f"workspace_id = ${len(params) + 1}")
            params.append(workspace_id)
        
        if unread_only:
            conditions.append("is_read = FALSE")
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        base_query += " ORDER BY timestamp DESC"
        
        if limit:
            base_query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(base_query, tuple(params), fetch_all=True)

    async def mark_notification_as_read(self, notification_id: UUID) -> bool:
        """Mark a notification as read."""
        query = "UPDATE notifications SET is_read = TRUE WHERE notification_id = $1"
        rows = await self.db_manager.execute_query(query, (notification_id,), return_rowcount=True)
        
        if rows > 0:
            logger.debug(f"Marked notification {notification_id} as read")
        
        return rows > 0

    async def get_unread_count(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Get count of unread notifications for a user."""
        if workspace_id:
            query = """
                SELECT COUNT(*) as count FROM notifications 
                WHERE user_id = $1 AND workspace_id = $2 AND is_read = FALSE
            """
            params = (user_id, workspace_id)
        else:
            query = """
                SELECT COUNT(*) as count FROM notifications 
                WHERE user_id = $1 AND is_read = FALSE
            """
            params = (user_id,)
        
        result = await self.db_manager.execute_query(query, params, fetch_one=True)
        return result.get("count", 0) if result else 0

notification_service = NotificationService()
