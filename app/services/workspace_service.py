# app/services/workspace_service.py
from typing import List, Optional, Dict, Tuple, Any, Union
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import logging
import asyncpg
from fastapi import HTTPException, status
from app.services.database import db_manager
from app.schemas import WorkspaceCreate, WorkspaceUpdate, WorkspaceMemberCreate, WorkspaceMemberUpdate
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)

class WorkspaceService:
    def __init__(self):
        self.db_manager = db_manager

    def is_system_admin(self, user_data: Dict) -> bool:
        """Check if user has system admin role."""
        return user_data and user_data.get("role") == "admin"

    def is_superadmin(self, user_data: Dict) -> bool:
        """Check if user has the superadmin role."""
        return bool(user_data and user_data.get("role") == "superadmin")

    async def get_workspace_by_id(self, workspace_id: UUID, check_active: bool = True) -> dict:
        """Retrieve workspace by ID with optional active status check."""
        query = """
            SELECT workspace_id, name, description, created_at, updated_at, is_active
            FROM workspaces 
            WHERE workspace_id = $1
        """
        params_tuple = (workspace_id,)
        if check_active:
            query += " AND is_active = TRUE"
            
        workspace_row = await self.db_manager.execute_query(query, params_tuple, fetch_one=True)
        
        if not workspace_row:
            detail = "Workspace not found"
            if check_active:
                detail += " or is inactive"
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
        
        return {
            "workspace_id": workspace_row["workspace_id"],
            "name": workspace_row["name"],
            "description": workspace_row["description"],
            "created_at": workspace_row["created_at"],
            "updated_at": workspace_row["updated_at"],
            "is_active": workspace_row["is_active"]
        }

    async def check_workspace_membership_and_get_role(
        self, 
        user_id: UUID, 
        workspace_id: UUID, 
        required_role: Optional[str] = None
    ) -> dict:
        """Verify workspace membership and optionally check role requirements."""
        query = """
            SELECT wm.membership_id, wm.workspace_id, wm.user_id, u.username, wm.role, 
                   wm.created_at, wm.updated_at
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.user_id = $1 AND wm.workspace_id = $2
        """
        membership_row = await self.db_manager.execute_query(query, (user_id, workspace_id), fetch_one=True)
        
        if not membership_row:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are not a member of this workspace."
            )
        
        user_actual_role = membership_row["role"]
        
        if required_role and user_actual_role != required_role and user_actual_role != 'admin':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires '{required_role}' or 'admin' role in the workspace. Your role: '{user_actual_role}'."
            )
        
        return {
            "membership_id": membership_row["membership_id"],
            "workspace_id": membership_row["workspace_id"],
            "user_id": membership_row["user_id"],
            "username": membership_row["username"],
            "role": user_actual_role,
            "created_at": membership_row["created_at"],
            "updated_at": membership_row["updated_at"]
        }

    async def get_user_and_workspace(self, username: str) -> Tuple[Optional[UUID], Optional[UUID]]:
        """Retrieve user ID and their active workspace."""
        try:
            if not username:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail="Username is required"
                )

            # Get user ID
            query_user = "SELECT user_id FROM users WHERE username = $1"
            user_result = await self.db_manager.execute_query(
                query_user, (username,), fetch_one=True
            )

            if not user_result or not user_result.get("user_id"):
                logger.warning(f"User ID lookup failed for username: {username}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail="User not found"
                )

            user_id = UUID(str(user_result["user_id"]))

            # Simplified workspace lookup with single query using COALESCE
            workspace_query = """
                WITH session_workspace AS (
                    SELECT ut.workspace_id, ut.updated_at as last_used
                    FROM user_tokens ut
                    WHERE ut.user_id = $1 AND ut.is_active = TRUE
                    ORDER BY ut.updated_at DESC
                    LIMIT 1
                ),
                member_workspaces AS (
                    SELECT wm.workspace_id, wm.created_at as joined_at
                    FROM workspace_members wm
                    JOIN workspaces w ON wm.workspace_id = w.workspace_id
                    WHERE wm.user_id = $1 AND w.is_active = TRUE
                    ORDER BY wm.created_at ASC
                    LIMIT 1
                )
                SELECT COALESCE(
                    (SELECT workspace_id FROM session_workspace),
                    (SELECT workspace_id FROM member_workspaces)
                ) as workspace_id
            """
            
            ws_result = await self.db_manager.execute_query(
                workspace_query, (user_id,), fetch_one=True
            )

            workspace_id: Optional[UUID] = None
            if ws_result and ws_result.get("workspace_id"):
                workspace_id = UUID(str(ws_result["workspace_id"]))
            else:
                logger.warning(f"User {username} (ID: {user_id}) has no active workspace available.")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No active or available workspace found for user."
                )
            
            return user_id, workspace_id

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving user_id and workspace for username '{username}': {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve user and workspace information."
            )

    async def get_active_workspace(self, user_id: Union[str, UUID]) -> Optional[Dict]:
        """Get user's active workspace, preferring session-based then first joined."""
        user_id_obj = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        
        # Try to get from active session/token
        query_session_ws = """
            SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, 
                w.is_active, wm.role as member_role
            FROM workspaces w
            JOIN user_tokens ut ON w.workspace_id = ut.workspace_id 
            JOIN workspace_members wm ON w.workspace_id = wm.workspace_id 
                AND ut.user_id = wm.user_id
            WHERE ut.user_id = $1 AND ut.is_active = TRUE AND w.is_active = TRUE
            ORDER BY ut.updated_at DESC LIMIT 1 
        """
        workspace_data = await self.db_manager.execute_query(
            query_session_ws, (user_id_obj,), fetch_one=True
        )
        
        # Fallback to first workspace if no active session
        if not workspace_data:
            query_default_ws = """
                SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, 
                    w.is_active, wm.role as member_role
                FROM workspaces w 
                JOIN workspace_members wm ON w.workspace_id = wm.workspace_id
                WHERE wm.user_id = $1 AND w.is_active = TRUE
                ORDER BY wm.created_at ASC LIMIT 1 
            """
            workspace_data = await self.db_manager.execute_query(
                query_default_ws, (user_id_obj,), fetch_one=True
            )

        if workspace_data:
            return {
                "workspace_id": workspace_data["workspace_id"],
                "name": workspace_data["name"],
                "description": workspace_data["description"],
                "created_at": workspace_data["created_at"],
                "updated_at": workspace_data["updated_at"],
                "is_active": workspace_data["is_active"],
                "member_role": workspace_data["member_role"]
            }
        
        logger.warning(f"No active workspace found for user {user_id_obj}")
        return None

    async def create_workspace(
        self,
        workspace_data: WorkspaceCreate,
        current_user_id: UUID,
        username: str
    ) -> Dict:
        """Create a new workspace and add creator as admin in a transaction."""
        workspace_id = uuid4()
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        
        try:
            async with self.db_manager.transaction() as conn:
                # Check for existing workspace
                existing_ws_query = "SELECT workspace_id FROM workspaces WHERE name = $1"
                existing_ws = await self.db_manager.execute_query(
                    existing_ws_query, 
                    (workspace_data.name,), 
                    fetch_one=True,
                    connection=conn
                )
                if existing_ws:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="A workspace with this name already exists."
                    )

                # Create workspace
                await self.db_manager.execute_query(
                    """INSERT INTO workspaces 
                    (workspace_id, name, description, created_at, updated_at, is_active) 
                    VALUES ($1, $2, $3, $4, $5, $6)""",
                    (workspace_id, workspace_data.name, workspace_data.description, 
                    now, now, True),
                    connection=conn
                )
                
                # Add creator as admin
                membership_id = uuid4()
                await self.db_manager.execute_query(
                    """INSERT INTO workspace_members 
                    (membership_id, workspace_id, user_id, role, created_at, updated_at) 
                    VALUES ($1, $2, $3, $4, $5, $6)""",
                    (membership_id, workspace_id, current_user_id, "admin", now, now),
                    connection=conn
                )
            
            return {
                "workspace_id": str(workspace_id),
                "name": workspace_data.name,
                "username": username
            }
            
        except HTTPException:
            raise
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error creating workspace: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred while creating workspace."
            )
        except Exception as e:
            logger.error(f"Error creating workspace by user {current_user_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create workspace."
            )        

    async def get_user_workspaces(
        self,
        current_user_id: UUID,
        include_inactive: bool = False
    ) -> List[dict]:
        """Get all workspaces for a user."""
        query = """
            SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, 
                   w.is_active, wm.role as member_role
            FROM workspaces w
            JOIN workspace_members wm ON w.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        params_list = [current_user_id]
        if not include_inactive:
            query += " AND w.is_active = TRUE"
        query += " ORDER BY w.created_at DESC"
        
        try:
            workspaces_rows = await self.db_manager.execute_query(
                query, 
                tuple(params_list), 
                fetch_all=True
            )
            return [
                {
                    "workspace_id": str(w_row["workspace_id"]),
                    "name": w_row["name"],
                    "description": w_row["description"],
                    "created_at": w_row["created_at"].isoformat() if w_row["created_at"] else None,
                    "updated_at": w_row["updated_at"].isoformat() if w_row["updated_at"] else None,
                    "is_active": w_row["is_active"],
                    "member_role": w_row["member_role"]
                } for w_row in workspaces_rows
            ] if workspaces_rows else []
        except Exception as e:
            logger.error(f"Error retrieving workspaces for user {current_user_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve workspaces."
            )

    async def get_workspace_details(
        self,
        workspace_id: UUID,
        current_user_id: UUID,
        is_admin: bool = False
    ) -> dict:
        """Get detailed information about a workspace."""
        try:
            if not is_admin:
                await check_workspace_access(
                    self.db_manager,
                    current_user_id,
                    workspace_id,
                )

            workspace_details = await self.get_workspace_by_id(workspace_id, check_active=False)
            
            count_result = await self.db_manager.execute_query(
                "SELECT COUNT(*) as count FROM workspace_members WHERE workspace_id = $1",
                (workspace_id,),
                fetch_one=True
            )
            workspace_details["member_count"] = count_result["count"] if count_result else 0
            
            workspace_details["workspace_id"] = str(workspace_details["workspace_id"])
            workspace_details["created_at"] = workspace_details["created_at"].isoformat() if workspace_details["created_at"] else None
            workspace_details["updated_at"] = workspace_details["updated_at"].isoformat() if workspace_details["updated_at"] else None
            
            return workspace_details
        except Exception as e:
            logger.error(f"Error retrieving details for workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve workspace details."
            )

    async def update_workspace(
        self,
        workspace_id: UUID,
        workspace_update_data: WorkspaceUpdate,
        current_user_id: UUID,
        is_admin: bool = False
    ) -> Dict:
        """Update workspace information."""
        try:
            # Import here to avoid circular dependency
            from app.services.user_service import user_manager
            
            # Verify permissions
            if not is_admin:
                membership_info = await check_workspace_access(
                    self.db_manager,
                    current_user_id,
                    workspace_id,
                    required_role="admin",
                )
                username = membership_info.get('username')
            else:
                admin_user_info = await user_manager.get_user_by_id(str(current_user_id))
                username = admin_user_info.get("username", "SystemAdmin") if admin_user_info else "SystemAdmin"

            # Build update query
            update_fields_clauses, params_list = [], []
            param_idx = 1
            
            # Check name uniqueness if changing
            if workspace_update_data.name is not None:
                current_workspace_details = await self.get_workspace_by_id(
                    workspace_id, check_active=False
                )
                if workspace_update_data.name != current_workspace_details["name"]:
                    existing_ws_query = """
                        SELECT workspace_id FROM workspaces 
                        WHERE name = $1 AND workspace_id != $2
                    """
                    existing_ws = await self.db_manager.execute_query(
                        existing_ws_query,
                        (workspace_update_data.name, workspace_id),
                        fetch_one=True
                    )
                    if existing_ws:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="A workspace with this name already exists."
                        )
                update_fields_clauses.append(f"name = ${param_idx}")
                params_list.append(workspace_update_data.name)
                param_idx += 1

            if workspace_update_data.description is not None:
                update_fields_clauses.append(f"description = ${param_idx}")
                params_list.append(workspace_update_data.description)
                param_idx += 1
                
            if workspace_update_data.is_active is not None:
                update_fields_clauses.append(f"is_active = ${param_idx}")
                params_list.append(workspace_update_data.is_active)
                param_idx += 1
            
            if not update_fields_clauses:
                return {
                    "updated": False, 
                    "username": username,
                    "message": "No changes to apply"
                }

            # Add updated_at timestamp
            update_fields_clauses.append(f"updated_at = ${param_idx}")
            params_list.append(datetime.now(ZoneInfo("Africa/Cairo")))
            param_idx += 1
            
            # Add WHERE clause
            params_list.append(workspace_id)
            
            query = f"""
                UPDATE workspaces 
                SET {', '.join(update_fields_clauses)} 
                WHERE workspace_id = ${param_idx}
            """
            
            rows_affected = await self.db_manager.execute_query(
                query,
                tuple(params_list),
                return_rowcount=True
            )

            if rows_affected == 0:
                # Double-check workspace exists
                await self.get_workspace_by_id(workspace_id, check_active=False)
                logger.warning(
                    f"Workspace {workspace_id} update affected 0 rows, though it exists. "
                    f"Possible no-op update."
                )

            return {
                "updated": rows_affected > 0,
                "username": username,
                "update_data": workspace_update_data.model_dump(exclude_unset=True),
                "rows_affected": rows_affected
            }
            
        except HTTPException:
            raise
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error updating workspace {workspace_id}: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred while updating workspace."
            )
        except Exception as e:
            logger.error(f"Error updating workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update workspace."
            )

    async def get_workspace_members(
        self,
        workspace_id: UUID,
        current_user_id: UUID,
        is_admin: bool = False
    ) -> List[dict]:
        """Get all members of a workspace."""
        try:
            if not is_admin:
                await check_workspace_access(
                    self.db_manager,
                    current_user_id,
                    workspace_id,
                )
            
            await self.get_workspace_by_id(workspace_id, check_active=False)

            query = """
                SELECT wm.membership_id, wm.workspace_id, wm.user_id, u.username, wm.role, 
                       wm.created_at, wm.updated_at
                FROM workspace_members wm
                JOIN users u ON wm.user_id = u.user_id
                WHERE wm.workspace_id = $1 ORDER BY u.username
            """
            members_rows = await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)
            return [
                {
                    "membership_id": str(m_row["membership_id"]),
                    "workspace_id": str(m_row["workspace_id"]),
                    "user_id": str(m_row["user_id"]),
                    "username": m_row["username"],
                    "role": m_row["role"],
                    "created_at": m_row["created_at"].isoformat() if m_row["created_at"] else None,
                    "updated_at": m_row["updated_at"].isoformat() if m_row["updated_at"] else None
                } for m_row in members_rows
            ] if members_rows else []
        except Exception as e:
            logger.error(f"Error retrieving members for workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve workspace members."
            )

    async def add_workspace_member(
        self,
        workspace_id: UUID,
        member_data: WorkspaceMemberCreate,
        current_user_id: UUID,
        admin_username: str,
        is_admin: bool = False
    ) -> Dict:
        """Add a new member to a workspace."""
        try:
            # Import here to avoid circular dependency
            from app.services.user_service import user_manager
            
            async with self.db_manager.transaction() as conn:
                # Verify permissions
                if not is_admin:
                    await check_workspace_access(
                        self.db_manager,
                        current_user_id,
                        workspace_id,
                        required_role="admin",
                    )
                else:
                    await self.get_workspace_by_id(workspace_id, check_active=True)
                
                # Validate target user exists
                target_user_id_obj = member_data.user_id
                target_user_info = await user_manager.get_user_by_id(str(target_user_id_obj))
                if not target_user_info:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"User with ID '{target_user_id_obj}' not found."
                    )

                # Check if already a member
                existing_member = await self.db_manager.execute_query(
                    """SELECT membership_id FROM workspace_members 
                    WHERE workspace_id = $1 AND user_id = $2""",
                    (workspace_id, target_user_id_obj),
                    fetch_one=True,
                    connection=conn
                )
                if existing_member:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="User is already a member of this workspace."
                    )
                
                # Validate role
                valid_roles = ['member', 'admin', 'owner']
                if member_data.role not in valid_roles:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}"
                    )
                
                # Add membership
                membership_id = uuid4()
                now = datetime.now(ZoneInfo("Africa/Cairo"))
                await self.db_manager.execute_query(
                    """INSERT INTO workspace_members 
                    (membership_id, workspace_id, user_id, role, created_at, updated_at) 
                    VALUES ($1, $2, $3, $4, $5, $6)""",
                    (membership_id, workspace_id, target_user_id_obj, member_data.role, now, now),
                    connection=conn
                )
            
            return {
                "membership_id": str(membership_id),
                "user_id": str(target_user_id_obj),
                "role": member_data.role,
                "target_username": target_user_info['username'],
                "admin_username": admin_username
            }
            
        except HTTPException:
            raise
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error adding member to workspace {workspace_id}: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred while adding member."
            )
        except Exception as e:
            logger.error(f"Error adding member to workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to add member to workspace."
            )
        
    async def update_workspace_member_role(
        self,
        workspace_id: UUID,
        target_user_id: UUID,
        member_update_data: WorkspaceMemberUpdate,
        current_user_id: UUID,
        admin_username: str,
        is_admin: bool = False
    ) -> Dict:
        """Update a workspace member's role."""
        try:
            if not is_admin:
                await check_workspace_access(
                    self.db_manager,
                    current_user_id,
                    workspace_id,
                    required_role="admin",
                )
            else:
                await self.get_workspace_by_id(workspace_id, check_active=True)

            if target_user_id == current_user_id and member_update_data.role != "admin":
                if not is_admin:
                    other_admins_result = await self.db_manager.execute_query(
                        """SELECT COUNT(*) as count FROM workspace_members 
                           WHERE workspace_id = $1 AND role = 'admin' AND user_id != $2""",
                        (workspace_id, current_user_id),
                        fetch_one=True
                    )
                    if not other_admins_result or other_admins_result["count"] == 0:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="Cannot demote the only admin in the workspace."
                        )

            target_member_info = await self.db_manager.execute_query(
                """SELECT u.username FROM workspace_members wm 
                   JOIN users u ON wm.user_id = u.user_id 
                   WHERE wm.workspace_id = $1 AND wm.user_id = $2""",
                (workspace_id, target_user_id),
                fetch_one=True
            )
            if not target_member_info:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Target user is not a member of this workspace."
                )
            target_username = target_member_info["username"]

            rows_affected = await self.db_manager.execute_query(
                """UPDATE workspace_members SET role = $1, updated_at = $2 
                   WHERE workspace_id = $3 AND user_id = $4""",
                (member_update_data.role, datetime.now(ZoneInfo("Africa/Cairo")), workspace_id, target_user_id),
                return_rowcount=True
            )
            if rows_affected == 0:
                if not await self.db_manager.execute_query(
                    "SELECT membership_id FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
                    (workspace_id, target_user_id),
                    fetch_one=True
                ):
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Member not found for update."
                    )

            return {
                "admin_username": admin_username,
                "target_username": target_username,
                "new_role": member_update_data.role
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating member role in workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update member role."
            )

    async def remove_workspace_member(
        self,
        workspace_id: UUID,
        target_user_id: UUID,
        current_user_id: UUID,
        admin_username: str,
        is_admin: bool = False
    ) -> Dict:
        """Remove a member from a workspace."""
        try:
            # Import here to avoid circular dependency
            from app.services.user_service import user_manager
            
            async with self.db_manager.transaction() as conn:
                # Verify permissions
                if not is_admin:
                    await check_workspace_access(
                        self.db_manager,
                        current_user_id,
                        workspace_id,
                        required_role="admin",
                    )
                else:
                    await self.get_workspace_by_id(workspace_id, check_active=False)

                # Check target member exists and get role
                target_member_role_info = await self.db_manager.execute_query(
                    """SELECT role FROM workspace_members 
                    WHERE workspace_id = $1 AND user_id = $2""",
                    (workspace_id, target_user_id),
                    fetch_one=True,
                    connection=conn
                )
                
                if not target_member_role_info:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Target user is not a member of this workspace."
                    )

                # Prevent removing last admin
                if target_member_role_info["role"] == 'admin':
                    other_admins_result = await self.db_manager.execute_query(
                        """SELECT COUNT(*) as count FROM workspace_members 
                        WHERE workspace_id = $1 AND role = 'admin' AND user_id != $2""",
                        (workspace_id, target_user_id),
                        fetch_one=True,
                        connection=conn
                    )
                    if not other_admins_result or other_admins_result["count"] == 0:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="Cannot remove the only admin from the workspace."
                        )

                # Get target username
                target_user_info = await user_manager.get_user_by_id(str(target_user_id))
                target_username = target_user_info.get("username", "UnknownUser") if target_user_info else "UnknownUser"

                # Delete membership
                rows_affected = await self.db_manager.execute_query(
                    "DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
                    (workspace_id, target_user_id),
                    return_rowcount=True,
                    connection=conn
                )
                
                if rows_affected == 0:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Member not found for deletion."
                    )

            return {
                "admin_username": admin_username,
                "target_username": target_username,
                "rows_affected": rows_affected
            }
            
        except HTTPException:
            raise
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error removing member from workspace {workspace_id}: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred while removing member."
            )
        except Exception as e:
            logger.error(f"Error removing member from workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to remove member."
            )

    async def activate_workspace(
        self,
        workspace_id: UUID,
        current_user_id: UUID
    ) -> Dict:
        """Set a workspace as active for the current user."""
        try:
            await check_workspace_access(
                self.db_manager,
                current_user_id,
                workspace_id,
            )
            workspace_details = await self.get_workspace_by_id(workspace_id, check_active=True)

            await self.db_manager.execute_query(
                """UPDATE user_tokens SET workspace_id = $1, updated_at = $2 
                   WHERE user_id = $3 AND is_active = TRUE""",
                (workspace_id, datetime.now(ZoneInfo("Africa/Cairo")), current_user_id)
            )

            return {"workspace_name": workspace_details['name']}
        except Exception as e:
            logger.error(f"Error activating workspace {workspace_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to activate workspace."
            )

    async def get_all_workspaces(self, include_inactive: bool = False) -> List[dict]:
        """Get all workspaces (admin only)."""
        query = """
            SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, w.is_active,
                   (SELECT COUNT(*) FROM workspace_members wm WHERE wm.workspace_id = w.workspace_id) as member_count
            FROM workspaces w
        """
        if not include_inactive:
            query += " WHERE w.is_active = TRUE"
        query += " ORDER BY w.name"

        try:
            workspaces_rows = await self.db_manager.execute_query(query, fetch_all=True)
            return [
                {
                    "workspace_id": str(ws_row["workspace_id"]),
                    "name": ws_row["name"],
                    "description": ws_row["description"],
                    "created_at": ws_row["created_at"].isoformat() if ws_row["created_at"] else None,
                    "updated_at": ws_row["updated_at"].isoformat() if ws_row["updated_at"] else None,
                    "is_active": ws_row["is_active"],
                    "member_count": ws_row["member_count"]
                } for ws_row in workspaces_rows
            ] if workspaces_rows else []
        except Exception as e:
            logger.error(f"SysAdmin error retrieving all workspaces: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve all workspaces."
            )
    
    async def get_workspace_statistics(
        self,
        workspace_id: Union[str, UUID]
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a workspace"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        stats = {
            'workspace_id': str(workspace_id_obj),
            'members': {'total': 0, 'by_role': {}},
            'streams': {'total': 0, 'active': 0, 'streaming': 0, 'by_status': {}},
            'notifications': {'total': 0, 'unread': 0},
            'recent_activity': []
        }
        
        # Member counts
        member_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE role = 'owner') as owners,
                   COUNT(*) FILTER (WHERE role = 'admin') as admins,
                   COUNT(*) FILTER (WHERE role = 'member') as members
            FROM workspace_members
            WHERE workspace_id = $1
        """
        member_stats = await self.db_manager.execute_query(
            member_query, (workspace_id_obj,), fetch_one=True
        )
        
        if member_stats:
            stats['members']['total'] = member_stats['total']
            stats['members']['by_role'] = {
                'owner': member_stats['owners'],
                'admin': member_stats['admins'],
                'member': member_stats['members']
            }
        
        # Stream counts
        stream_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE status = 'active') as active,
                   COUNT(*) FILTER (WHERE is_streaming = TRUE) as streaming,
                   COUNT(*) FILTER (WHERE status = 'inactive') as inactive,
                   COUNT(*) FILTER (WHERE status = 'error') as error
            FROM video_stream
            WHERE workspace_id = $1
        """
        stream_stats = await self.db_manager.execute_query(
            stream_query, (workspace_id_obj,), fetch_one=True
        )
        
        if stream_stats:
            stats['streams']['total'] = stream_stats['total']
            stats['streams']['active'] = stream_stats['active']
            stats['streams']['streaming'] = stream_stats['streaming']
            stats['streams']['by_status'] = {
                'active': stream_stats['active'],
                'inactive': stream_stats['inactive'],
                'error': stream_stats['error']
            }
        
        # Notification counts
        notif_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE is_read = FALSE) as unread
            FROM notifications
            WHERE workspace_id = $1
        """
        notif_stats = await self.db_manager.execute_query(
            notif_query, (workspace_id_obj,), fetch_one=True
        )
        
        if notif_stats:
            stats['notifications']['total'] = notif_stats['total']
            stats['notifications']['unread'] = notif_stats['unread']
        
        return stats
    
workspace_service = WorkspaceService()
