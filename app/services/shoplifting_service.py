import logging
from typing import List, Dict, Any, Optional
from uuid import UUID
from datetime import datetime

from app.services.database import db_manager

logger = logging.getLogger(__name__)

class ShopliftingService:
    def __init__(self):
        self.db = db_manager

    async def get_events(
        self,
        workspace_id: UUID,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch paginated shoplifting events for a workspace."""
        query = """
            SELECT * FROM get_shoplifting_events($1, $2, $3, $4)
        """
        try:
            return await self.db.execute_query(
                query,
                (workspace_id, status, limit, offset),
                fetch_all=True
            )
        except Exception as e:
            logger.error(f"Error fetching shoplifting events: {e}")
            raise

    async def count_events(
        self,
        workspace_id: UUID,
        status: Optional[str] = None
    ) -> int:
        """Count total shoplifting events for pagination."""
        query = """
            SELECT COUNT(*) as total 
            FROM shoplifting_events 
            WHERE workspace_id = $1
        """
        params = [workspace_id]
        if status:
            query += " AND status = $2"
            params.append(status)
            
        try:
            result = await self.db.execute_query(
                query,
                tuple(params),
                fetch_one=True
            )
            return result.get('total', 0) if result else 0
        except Exception as e:
            logger.error(f"Error counting shoplifting events: {e}")
            return 0

    async def resolve_event(
        self,
        event_id: int,
        status: str,
        action_taken: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """Resolve a shoplifting event (updates status, action_taken, etc.)."""
        query = """
            SELECT resolve_shoplifting_event($1, $2, $3, $4)
        """
        try:
            await self.db.execute_query(
                query,
                (event_id, status, action_taken, description)
            )
            return True
        except Exception as e:
            logger.error(f"Error resolving shoplifting event {event_id}: {e}")
            raise

    async def get_active_dashboard(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        """Fetch active shoplifting events summary."""
        query = """
            SELECT * FROM v_active_shoplifting_events
            WHERE workspace_id = $1
            ORDER BY event_timestamp DESC
        """
        try:
            return await self.db.execute_query(
                query,
                (workspace_id,),
                fetch_all=True
            )
        except Exception as e:
            logger.error(f"Error fetching active shoplifting dashboard for workspace {workspace_id}: {e}")
            raise

    async def get_daily_summary(self, workspace_id: UUID, limit: int = 30) -> List[Dict[str, Any]]:
        """Fetch daily shoplifting summaries."""
        query = """
            SELECT * FROM v_shoplifting_daily_summary
            WHERE workspace_id = $1
            ORDER BY event_date DESC
            LIMIT $2
        """
        try:
            return await self.db.execute_query(
                query,
                (workspace_id, limit),
                fetch_all=True
            )
        except Exception as e:
            logger.error(f"Error fetching shoplifting daily summary for workspace {workspace_id}: {e}")
            raise

    async def insert_surveillance_frame(
        self,
        session_id: UUID,
        stream_id: UUID,
        workspace_id: UUID,
        user_id: UUID,
        is_shoplifting: bool,
        behavior_state: Optional[str] = None,
        behavior_category: Optional[str] = None,
        confidence: Optional[float] = None,
        objects_detected: Optional[List[str]] = None,
        image_path: Optional[str] = None
    ) -> Optional[int]:
        """Insert a frame into the surveillance_data table."""
        metadata_query = """
            INSERT INTO surveillance_data (
                session_id, stream_id, workspace_id, user_id, 
                is_shoplifting, behavior_state, behavior_category,
                detection_confidence, objects_detected, image_path
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            ) RETURNING observation_id
        """
        try:
            result = await self.db.execute_query(
                metadata_query,
                (
                    session_id, stream_id, workspace_id, user_id, 
                    is_shoplifting, behavior_state, behavior_category,
                    confidence, objects_detected, image_path
                ),
                fetch_one=True
            )
            return result.get('observation_id') if result else None
        except Exception as e:
            logger.error(f"Error inserting surveillance frame for stream {stream_id}: {e}")
            return None

shoplifting_service = ShopliftingService()
