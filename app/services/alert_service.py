# app/services/alert_service.py
import logging
from typing import Any, Dict, Optional, List
from uuid import UUID
from app.services.database import db_manager

logger = logging.getLogger(__name__)

class Alertervice:
    """
    Service layer for database operations.
    Encapsulates all database logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager


    async def get_alert_statistics(self, workspace_id: UUID) -> Optional[Dict[str, Any]]:
        """Get alert statistics for a workspace."""
        # First refresh the materialized view
        await self.db_manager.execute_query("SELECT refresh_alert_statistics()")

        query = "SELECT * FROM alert_statistics WHERE workspace_id = $1"
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)

    async def get_cameras_with_alerts(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        """Get all cameras with alert configurations."""
        query = "SELECT * FROM get_cameras_with_alerts($1)"
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def get_cameras_needing_attention(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        """Get cameras that need attention."""
        query = "SELECT * FROM get_cameras_needing_alert_config($1)"
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def bulk_update_alert_settings(
        self,
        workspace_id: UUID,
        camera_ids: List[UUID],
        count_threshold_greater: Optional[int] = None,
        count_threshold_less: Optional[int] = None,
        alert_enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Bulk update alert settings for multiple cameras."""
        query = "SELECT * FROM bulk_update_alert_settings($1, $2, $3, $4, $5)"
        result = await self.db_manager.execute_query(
            query,
            (workspace_id, camera_ids, count_threshold_greater, count_threshold_less, alert_enabled),
            fetch_one=True,
        )
        logger.info(
            f"Bulk updated {result['updated_count']} cameras in workspace {workspace_id}"
        )
        return result

alert_service = Alertervice()