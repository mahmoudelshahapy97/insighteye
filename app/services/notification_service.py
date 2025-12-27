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

    async def get_workspace_notifications(
        self,
        workspace_id: UUID,
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all notifications for a workspace."""
        if unread_only:
            query = """
                SELECT * FROM notifications 
                WHERE workspace_id = $1 AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications 
                WHERE workspace_id = $1
                ORDER BY timestamp DESC
            """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def get_stream_notifications(
        self,
        stream_id: UUID,
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all notifications for a specific stream."""
        if unread_only:
            query = """
                SELECT * FROM notifications 
                WHERE stream_id = $1 AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications 
                WHERE stream_id = $1
                ORDER BY timestamp DESC
            """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(query, (stream_id,), fetch_all=True)

    async def mark_notification_as_read(self, notification_id: UUID) -> bool:
        """Mark a notification as read."""
        query = "UPDATE notifications SET is_read = TRUE WHERE notification_id = $1"
        rows = await self.db_manager.execute_query(query, (notification_id,), return_rowcount=True)
        
        if rows > 0:
            logger.debug(f"Marked notification {notification_id} as read")
        
        return rows > 0

    async def mark_all_notifications_as_read(
        self, 
        user_id: UUID, 
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Mark all notifications as read for a user."""
        if workspace_id:
            query = """
                UPDATE notifications SET is_read = TRUE 
                WHERE user_id = $1 AND workspace_id = $2 AND is_read = FALSE
            """
            params = (user_id, workspace_id)
        else:
            query = """
                UPDATE notifications SET is_read = TRUE 
                WHERE user_id = $1 AND is_read = FALSE
            """
            params = (user_id,)

        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Marked {rows} notifications as read for user {user_id}")
        
        return rows

    async def mark_stream_notifications_as_read(
        self,
        stream_id: UUID,
        user_id: Optional[UUID] = None
    ) -> int:
        """Mark all notifications for a stream as read."""
        if user_id:
            query = """
                UPDATE notifications SET is_read = TRUE
                WHERE stream_id = $1 AND user_id = $2 AND is_read = FALSE
            """
            params = (stream_id, user_id)
        else:
            query = """
                UPDATE notifications SET is_read = TRUE
                WHERE stream_id = $1 AND is_read = FALSE
            """
            params = (stream_id,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Marked {rows} notifications as read for stream {stream_id}")
        
        return rows

    async def delete_notification(self, notification_id: UUID) -> bool:
        """Delete a notification."""
        query = "DELETE FROM notifications WHERE notification_id = $1"
        rows = await self.db_manager.execute_query(query, (notification_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted notification {notification_id}")
        
        return rows > 0

    async def delete_user_notifications(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Delete all notifications for a user."""
        if workspace_id:
            query = "DELETE FROM notifications WHERE user_id = $1 AND workspace_id = $2"
            params = (user_id, workspace_id)
        else:
            query = "DELETE FROM notifications WHERE user_id = $1"
            params = (user_id,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications for user {user_id}")
        
        return rows

    async def delete_stream_notifications(self, stream_id: UUID) -> int:
        """Delete all notifications for a stream."""
        query = "DELETE FROM notifications WHERE stream_id = $1"
        rows = await self.db_manager.execute_query(query, (stream_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications for stream {stream_id}")
        
        return rows

    async def delete_old_notifications(
        self,
        days: int = 30,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Delete notifications older than specified days."""
        cutoff_date = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(days=days)
        
        if workspace_id:
            query = """
                DELETE FROM notifications 
                WHERE workspace_id = $1 AND timestamp < $2
            """
            params = (workspace_id, cutoff_date)
        else:
            query = "DELETE FROM notifications WHERE timestamp < $1"
            params = (cutoff_date,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications older than {days} days")
        
        return rows

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

    async def get_notification_summary(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Get summary of notifications for a user."""
        if workspace_id:
            query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_read = FALSE THEN 1 END) as unread,
                    COUNT(CASE WHEN status = 'urgent' THEN 1 END) as urgent,
                    MAX(timestamp) as latest_notification
                FROM notifications
                WHERE user_id = $1 AND workspace_id = $2
            """
            params = (user_id, workspace_id)
        else:
            query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_read = FALSE THEN 1 END) as unread,
                    COUNT(CASE WHEN status = 'urgent' THEN 1 END) as urgent,
                    MAX(timestamp) as latest_notification
                FROM notifications
                WHERE user_id = $1
            """
            params = (user_id,)
        
        result = await self.db_manager.execute_query(query, params, fetch_one=True)
        
        if not result:
            return {
                "total": 0,
                "unread": 0,
                "urgent": 0,
                "latest_notification": None
            }
        
        return {
            "total": result.get("total", 0),
            "unread": result.get("unread", 0),
            "urgent": result.get("urgent", 0),
            "latest_notification": result.get("latest_notification")
        }

    async def get_urgent_notifications(
        self,
        workspace_id: UUID,
        unread_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Get all urgent notifications for a workspace."""
        if unread_only:
            query = """
                SELECT * FROM notifications
                WHERE workspace_id = $1 
                AND status = 'urgent' 
                AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications
                WHERE workspace_id = $1 
                AND status = 'urgent'
                ORDER BY timestamp DESC
            """
        
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def create_fire_alert_notification(
        self,
        workspace_id: UUID,
        stream_id: UUID,
        camera_name: str,
        fire_status: str,
        location_info: Optional[Dict[str, Any]] = None,
        broadcast_to_workspace: bool = True
    ) -> Dict[str, Any]:
        """
        Create and broadcast a critical fire alert notification.
        
        Returns:
            Dictionary with notification details and broadcast results
        """
        try:
            # Build location text
            location_text = self._build_location_text(location_info)
            
            # Get stream owner
            from app.services.video_stream_service import video_stream_service
            stream_info = await video_stream_service.get_video_stream_by_id(stream_id)
            if not stream_info:
                raise ValueError(f"Stream {stream_id} not found")
            
            owner_id = stream_info['user_id']
            alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
            message = f"🔥 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
            
            # Create database notification
            notification = await self.create_notification(
                workspace_id=workspace_id,
                user_id=owner_id,
                status="urgent",
                message=message,
                stream_id=stream_id,
                camera_name=camera_name
            )
            
            if not notification:
                raise ValueError("Failed to create notification in database")
            
            # Build popup payload
            popup_payload = {
                "type": "fire_alert_popup",
                "alert": {
                    "id": str(notification.get("notification_id")),
                    "severity": "critical",
                    "alert_type": alert_type.lower(),
                    "status": fire_status,
                    "camera_name": camera_name,
                    "camera_id": str(stream_id),
                    "location": location_text,
                    "location_details": location_info,
                    "message": message,
                    "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                    "workspace_id": str(workspace_id),
                    "user_id": str(owner_id),
                    "requires_acknowledgment": True,
                    "sound_alert": True,
                    "auto_dismiss_seconds": 30,
                    "actions": [
                        {
                            "label": "View Camera",
                            "action": "navigate",
                            "url": f"/dashboard/streams/{str(stream_id)}"
                        },
                        {
                            "label": "Acknowledge",
                            "action": "acknowledge"
                        },
                        {
                            "label": "Dismiss",
                            "action": "dismiss"
                        }
                    ]
                }
            }
            
            # Broadcast to recipients
            recipients = []
            
            if broadcast_to_workspace:
                # Get all workspace members
                from app.services.workspace_service import workspace_service
                members = await workspace_service.get_workspace_members(
                    workspace_id=workspace_id,
                    current_user_id=workspace_id,
                    is_admin=True
                )
                recipients = [str(m['user_id']) for m in members]
            else:
                recipients = [str(owner_id)]
            
            # Broadcast via StreamManager
            from app.services.stream_service import stream_manager
            broadcast_results = []
            
            for user_id_str in recipients:
                try:
                    await stream_manager.broadcast_notification(user_id_str, popup_payload)
                    broadcast_results.append({
                        "user_id": user_id_str,
                        "status": "success"
                    })
                    logger.info(f"✅ Fire popup sent to user {user_id_str}")
                except Exception as e:
                    broadcast_results.append({
                        "user_id": user_id_str,
                        "status": "failed",
                        "error": str(e)
                    })
                    logger.error(f"❌ Failed to send fire popup to user {user_id_str}: {e}")
            
            success_count = sum(1 for r in broadcast_results if r['status'] == 'success')
            
            logger.warning(
                f"🔥 Fire alert notification created and broadcasted: "
                f"{success_count}/{len(recipients)} recipients"
            )
            
            return {
                "notification_id": str(notification.get("notification_id")),
                "message": message,
                "alert_type": alert_type,
                "location": location_text,
                "recipients_count": len(recipients),
                "successful_broadcasts": success_count,
                "broadcast_results": broadcast_results
            }
            
        except Exception as e:
            logger.error(f"Error creating fire alert notification: {e}", exc_info=True)
            raise

    def _build_location_text(self, location_info: Optional[Dict[str, Any]]) -> str:
        """Build a readable location string from location info."""
        if not location_info:
            return "Unknown Location"
        
        location_parts = []
        
        # Priority order: Building > Floor > Zone > Area
        if location_info.get('building'):
            location_parts.append(location_info['building'])
        
        if location_info.get('floor_level'):
            floor = location_info['floor_level']
            location_parts.append(f"Floor {floor}")
        
        if location_info.get('zone'):
            location_parts.append(location_info['zone'])
        
        if location_info.get('area'):
            location_parts.append(location_info['area'])
        
        # If we have parts, join them
        if location_parts:
            return " - ".join(location_parts)
        
        # Fallback to simple location
        return location_info.get('location', 'Unknown Location')

notification_service = NotificationService()
