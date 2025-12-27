# app/services/log_service.py
import logging
from typing import Any, Dict, Optional
from uuid import UUID
from app.services.database import db_manager

logger = logging.getLogger(__name__)

class LogService:
    """
    Service layer for database operations.
    Encapsulates all database logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def log_action(
        self,
        action_type: str,
        content: str,
        status: str = "success",
        user_id: Optional[UUID] = None,
        workspace_id: Optional[UUID] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        """Log an action."""
        query = """
            INSERT INTO logs 
            (user_id, workspace_id, action_type, status, content, ip_address, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        await self.db_manager.execute_query(
            query,
            (user_id, workspace_id, action_type, status, content, ip_address, user_agent),
        )

    async def log_security_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        severity: str = "low",
        user_id: Optional[UUID] = None,
        workspace_id: Optional[UUID] = None,
        ip_address: Optional[str] = None,
    ) -> None:
        """Log a security event."""
        import json

        query = """
            INSERT INTO security_events 
            (user_id, workspace_id, event_type, severity, ip_address, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        await self.db_manager.execute_query(
            query,
            (user_id, workspace_id, event_type, severity, ip_address, json.dumps(event_data)),
        )

log_service = LogService()