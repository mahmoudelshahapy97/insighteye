# app/services/parameter_service.py
from typing import Optional, Dict, List, Tuple
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import logging
import asyncpg
from fastapi import HTTPException, status

from app.services.database import db_manager
from app.schemas import CameraStreamQueryParams

logger = logging.getLogger(__name__)


class ParameterService:
    """
    Service layer for camera parameter database operations.
    Encapsulates all parameter logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def create_or_update_workspace_params(
        self,
        params: CameraStreamQueryParams,
        user_id: UUID,
        workspace_id: UUID
    ) -> Dict[str, str]:
        """Create or update workspace camera parameters."""
        try:
            param_id = uuid4()
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))

            query = """
                INSERT INTO param_stream 
                (param_id, user_id, workspace_id, frame_delay, frame_skip, conf, created_at, updated_at) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (workspace_id) DO UPDATE SET
                    frame_delay = EXCLUDED.frame_delay,
                    frame_skip = EXCLUDED.frame_skip,
                    conf = EXCLUDED.conf,
                    user_id = EXCLUDED.user_id,
                    updated_at = EXCLUDED.updated_at
                RETURNING param_id, created_at, updated_at;
            """
            
            result = await self.db_manager.execute_query(
                query,
                params=(
                    param_id, user_id, workspace_id, 
                    params.frame_delay, params.frame_skip, params.conf, 
                    now_utc, now_utc
                ),
                fetch_one=True
            )
            
            action = "created" if result['created_at'] == result['updated_at'] else "updated"
            logger.info(f"Workspace parameters {action} for workspace {workspace_id}")
            
            return {
                "param_id": str(result['param_id']),
                "workspace_id": str(workspace_id),
                "action": action
            }
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error saving param_stream: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred while saving parameters."
            )

    async def get_workspace_params(
        self,
        workspace_id: Optional[UUID]
    ) -> Dict:
        """Get workspace parameters with defaults if not set."""
        default_params = {
            "last_modifier_username": None,
            "frame_delay": 0.0,
            "frame_skip": 300,
            "conf": 0.4,
            "created_at": None,
            "updated_at": None,
        }
        
        try:
            if not workspace_id:
                default_params["message"] = "No active workspace or parameters not set. Returning defaults."
                return default_params
            
            query = """
                SELECT 
                    ps.param_id, ps.workspace_id, ps.user_id, 
                    u.username as last_modifier_username, 
                    ps.frame_delay, ps.frame_skip, ps.conf, 
                    ps.created_at, ps.updated_at 
                FROM param_stream ps
                JOIN users u ON ps.user_id = u.user_id
                WHERE ps.workspace_id = $1
            """
            params_data = await self.db_manager.execute_query(
                query, 
                params=(workspace_id,), 
                fetch_one=True
            )
            
            if not params_data:
                return default_params

            return {
                "param_id": str(params_data["param_id"]),
                "workspace_id": str(params_data["workspace_id"]),
                "last_modifier_id": str(params_data["user_id"]),
                "last_modifier_username": params_data["last_modifier_username"],
                "frame_delay": params_data["frame_delay"],
                "frame_skip": params_data["frame_skip"],
                "conf": params_data["conf"],
                "created_at": params_data["created_at"].isoformat() if params_data["created_at"] else None,
                "updated_at": params_data["updated_at"].isoformat() if params_data["updated_at"] else None,
            }
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error retrieving workspace params: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )

    async def get_params_by_id(self, param_id: UUID) -> Optional[Dict]:
        """Get parameters by param_id."""
        try:
            query = """
                SELECT 
                    ps.param_id, ps.workspace_id, ps.user_id,
                    u.username as last_modifier_username,
                    w.name as workspace_name,
                    ps.frame_delay, ps.frame_skip, ps.conf,
                    ps.created_at, ps.updated_at
                FROM param_stream ps
                JOIN users u ON ps.user_id = u.user_id
                JOIN workspaces w ON ps.workspace_id = w.workspace_id
                WHERE ps.param_id = $1
            """
            result = await self.db_manager.execute_query(query, params=(param_id,), fetch_one=True)
            
            if not result:
                return None
            
            return {
                "param_id": str(result["param_id"]),
                "workspace_id": str(result["workspace_id"]),
                "workspace_name": result["workspace_name"],
                "last_modifier_id": str(result["user_id"]),
                "last_modifier_username": result["last_modifier_username"],
                "frame_delay": result["frame_delay"],
                "frame_skip": result["frame_skip"],
                "conf": result["conf"],
                "created_at": result["created_at"].isoformat() if result["created_at"] else None,
                "updated_at": result["updated_at"].isoformat() if result["updated_at"] else None
            }
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error retrieving params by id: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred."
            )

    async def delete_workspace_params(
        self,
        workspace_id: UUID
    ) -> int:
        """Delete workspace parameters. Returns number of rows affected."""
        try:
            query = "DELETE FROM param_stream WHERE workspace_id = $1"
            rows_affected = await self.db_manager.execute_query(
                query, 
                params=(workspace_id,), 
                return_rowcount=True
            )
            
            if rows_affected and rows_affected > 0:
                logger.info(f"Deleted parameters for workspace {workspace_id}")
            
            return rows_affected if rows_affected is not None else 0
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error deleting workspace parameters: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )
        except Exception as e:
            logger.error(f"Unexpected error deleting workspace parameters: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to delete workspace parameters."
            )

    async def get_all_workspace_params(self) -> List[Dict]:
        """Get parameters for all workspaces (admin only)."""
        try:
            query = """
                SELECT 
                    ps.param_id, ps.workspace_id, w.name as workspace_name, 
                    ps.user_id as last_modifier_id, u.username as last_modifier_username,
                    ps.frame_delay, ps.frame_skip, ps.conf, 
                    ps.created_at, ps.updated_at 
                FROM param_stream ps
                JOIN workspaces w ON ps.workspace_id = w.workspace_id
                JOIN users u ON ps.user_id = u.user_id
                ORDER BY w.name, ps.created_at DESC
            """
            params_list_data = await self.db_manager.execute_query(query, fetch_all=True)
            
            return [
                {
                    "param_id": str(p["param_id"]),
                    "workspace_id": str(p["workspace_id"]),
                    "workspace_name": p["workspace_name"],
                    "last_modifier_id": str(p["last_modifier_id"]),
                    "last_modifier_username": p["last_modifier_username"],
                    "frame_delay": p["frame_delay"],
                    "frame_skip": p["frame_skip"],
                    "conf": p["conf"],
                    "created_at": p["created_at"].isoformat() if p["created_at"] else None,
                    "updated_at": p["updated_at"].isoformat() if p["updated_at"] else None
                } for p in params_list_data
            ] if params_list_data else []
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error retrieving all workspace params: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )

    async def update_all_workspace_params(
        self,
        params_list: List[CameraStreamQueryParams],
        admin_user_id: UUID
    ) -> Tuple[List[str], List[str]]:
        """
        Update parameters for multiple workspaces (admin only).
        Returns (updated_workspace_ids, failed_workspace_ids).
        """
        updated_ids = []
        failed_ids = []
        
        for params_item in params_list:
            if params_item.workspace_id is None:
                failed_ids.append("Unknown (missing workspace_id)")
                continue

            try:
                workspace_id_uuid = UUID(str(params_item.workspace_id))
            except (ValueError, AttributeError) as e:
                logger.error(f"Invalid workspace_id format: {params_item.workspace_id}. Error: {e}")
                failed_ids.append(f"{params_item.workspace_id} (invalid UUID)")
                continue
            
            # Verify workspace exists
            workspace_query = "SELECT workspace_id FROM workspaces WHERE workspace_id = $1"
            workspace_exists = await self.db_manager.execute_query(
                workspace_query,
                params=(workspace_id_uuid,),
                fetch_one=True
            )
            
            if not workspace_exists:
                failed_ids.append(f"{params_item.workspace_id} (workspace not found)")
                continue

            query_upsert = """
                INSERT INTO param_stream 
                (param_id, user_id, workspace_id, frame_delay, frame_skip, conf, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (workspace_id) DO UPDATE SET
                    frame_delay = EXCLUDED.frame_delay,
                    frame_skip = EXCLUDED.frame_skip,
                    conf = EXCLUDED.conf,
                    user_id = EXCLUDED.user_id,
                    updated_at = EXCLUDED.updated_at
                RETURNING param_id
            """
            
            param_id_new = uuid4()
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            
            try:
                result = await self.db_manager.execute_query(
                    query_upsert,
                    params=(
                        param_id_new,
                        admin_user_id,
                        workspace_id_uuid,
                        params_item.frame_delay,
                        params_item.frame_skip,
                        params_item.conf,
                        now_utc,
                        now_utc
                    ),
                    fetch_one=True
                )
                
                if result and result.get('param_id'):
                    updated_ids.append(str(params_item.workspace_id))
                else:
                    logger.warning(f"Upsert returned no result for workspace {params_item.workspace_id}")
                    failed_ids.append(f"{params_item.workspace_id} (DB operation returned no result)")
                    
            except asyncpg.PostgresError as db_err_item:
                logger.error(
                    f"Database error updating param_stream for workspace {params_item.workspace_id}: {db_err_item}", 
                    exc_info=True
                )
                failed_ids.append(f"{params_item.workspace_id} (database error)")
                continue
            except Exception as e:
                logger.error(f"Unexpected error updating workspace {params_item.workspace_id}: {e}", exc_info=True)
                failed_ids.append(f"{params_item.workspace_id} (unexpected error)")
                continue
        
        return updated_ids, failed_ids

    async def delete_all_workspace_params(self, workspace_id) -> int:
        """Delete all workspace parameters (admin only). Returns rows affected."""
        try:
            query = "DELETE FROM param_stream WHERE workspace_id = $1"
            rows_affected = await self.db_manager.execute_query(query, params=(workspace_id,), return_rowcount=True)
            
            if rows_affected and rows_affected > 0:
                logger.warning(f"Deleted ALL workspace parameters ({rows_affected} rows)")
            
            return rows_affected if rows_affected is not None else 0
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error deleting all workspace params: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )

    async def delete_all_workspaces_params(self) -> int:
        """Delete all workspace parameters (admin only). Returns rows affected."""
        try:
            query = "DELETE FROM param_stream"
            rows_affected = await self.db_manager.execute_query(query, return_rowcount=True)
            
            if rows_affected and rows_affected > 0:
                logger.warning(f"Deleted ALL workspaces parameters ({rows_affected} rows)")
            
            return rows_affected if rows_affected is not None else 0
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error deleting all workspace params: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )

    async def get_workspace_param_sync_status(
        self,
        workspace_id: UUID
    ) -> Dict:
        """Get workspace parameter sync status including member info."""
        try:
            # Get workspace parameters
            ws_params_query = """
                SELECT 
                    ps.param_id, ps.frame_delay, ps.frame_skip, ps.conf, 
                    ps.updated_at as params_last_updated, 
                    u.username as last_modifier_username
                FROM param_stream ps
                JOIN users u ON ps.user_id = u.user_id
                WHERE ps.workspace_id = $1
            """
            workspace_params_db = await self.db_manager.execute_query(
                ws_params_query,
                params=(workspace_id,),
                fetch_one=True
            )

            if workspace_params_db:
                current_workspace_parameters = {
                    "param_id": str(workspace_params_db["param_id"]),
                    "frame_delay": workspace_params_db["frame_delay"],
                    "frame_skip": workspace_params_db["frame_skip"],
                    "conf": workspace_params_db["conf"],
                    "last_updated_at": (
                        workspace_params_db["params_last_updated"].isoformat() 
                        if workspace_params_db["params_last_updated"] else None
                    ),
                    "last_modified_by": workspace_params_db["last_modifier_username"]
                }
            else:
                current_workspace_parameters = {
                    "param_id": None,
                    "frame_delay": 0.0,
                    "frame_skip": 300,
                    "conf": 0.4,
                    "last_updated_at": None,
                    "last_modified_by": None,
                    "is_default": True,
                    "message": "No parameters configured, using defaults"
                }
            
            # Get workspace members
            members_query = """
                SELECT u.user_id, u.username, wm.role as workspace_role
                FROM workspace_members wm
                JOIN users u ON wm.user_id = u.user_id
                WHERE wm.workspace_id = $1 
                ORDER BY u.username
            """
            members_db = await self.db_manager.execute_query(
                members_query,
                params=(workspace_id,),
                fetch_all=True
            )
            
            workspace_members_info = [
                {
                    "user_id": str(m["user_id"]),
                    "username": m["username"],
                    "workspace_role": m["workspace_role"]
                } for m in (members_db or []) 
            ]
            
            return {
                "workspace_id": str(workspace_id),
                "current_parameters": current_workspace_parameters,
                "members": workspace_members_info,
                "total_members": len(workspace_members_info)
            }
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error getting workspace param status: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database error occurred."
            )

    async def get_parameter_history(
        self,
        workspace_id: UUID,
        limit: int = 10
    ) -> List[Dict]:
        """Get parameter change history for a workspace (requires audit table)."""
        try:
            current_params = await self.get_workspace_params(workspace_id)
            if current_params and current_params.get("param_id"):
                return [current_params]
            return []
        except Exception as e:
            logger.error(f"Error retrieving parameter history: {e}", exc_info=True)
            return []

parameter_service = ParameterService()
