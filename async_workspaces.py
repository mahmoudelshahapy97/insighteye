# async_workspaces.py
from fastapi import APIRouter, HTTPException, Depends, status, Request, Query
from typing import List, Optional, Dict
from uuid import UUID, uuid4
from datetime import datetime, timezone 
import logging
from async_session_manager import SessionManager # Methods will be async
from async_database import DatabaseManager # Assuming this is AsyncDatabaseManager
from async_user_manager import UserManager # Methods will be async
from schemas_models import WorkspaceCreate, WorkspaceUpdate, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceResponse, WorkspaceMemberResponse, UserResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["workspaces"])
session_manager = SessionManager()
user_manager = UserManager()
db_manager = DatabaseManager()

# Helper functions
async def get_current_user_full_data_dependency(username: str = Depends(session_manager.get_current_user)) -> Dict:
    """Dependency to get the current authenticated user's full data including role."""
    user_data = await user_manager.get_user_by_username(username)
    if not user_data or "user_id" not in user_data or "role" not in user_data: # Check for role
        logger.error(f"User data, user_id, or role not found for username: {username} in dependency.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user session or user data incomplete.")
    
    try:
        current_user_id_value = user_data.get("user_id")

        if current_user_id_value is None:
            logger.error(f"User ID is None for username: {username}, which is unexpected here.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User ID is unexpectedly None.")

        if not isinstance(current_user_id_value, UUID):
            # Attempt conversion only if it's not already a UUID object
            user_data['user_id'] = UUID(current_user_id_value)
        # If it's already a UUID object, user_data['user_id'] is already correct.
        
        return user_data
    except (ValueError, TypeError) as e: 
        logger.error(f"Error converting user ID '{user_data.get('user_id')}' to UUID for username '{username}'. Error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User ID format or type error.")

async def get_current_user_id_dependency(username: str = Depends(session_manager.get_current_user)) -> Dict :
    """Dependency to get the current authenticated user's ID."""
    user_data = await user_manager.get_user_by_username(username)
    if not user_data or "user_id" not in user_data:
        logger.error(f"User data or user_id not found for username: {username} in dependency.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user session or user not found.")
    
    try:
        current_user_id_value = user_data.get("user_id")

        if current_user_id_value is None:
            logger.error(f"User ID is None for username: {username} in get_current_user_id_dependency.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User ID is unexpectedly None.")

        if not isinstance(current_user_id_value, UUID):
            user_data['user_id'] = UUID(current_user_id_value)
        
        return user_data
    except (ValueError, TypeError) as e:
        logger.error(f"Error converting user ID '{user_data.get('user_id')}' to UUID for username '{username}' in get_current_user_id_dependency. Error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User ID format or type error.")

def is_system_admin(user_data: Dict) -> bool:
    return user_data and user_data.get("role") == "admin"

async def get_workspace_by_id(workspace_id: UUID, check_active: bool = True) -> dict:
    query = """
        SELECT workspace_id, name, description, created_at, updated_at, is_active
        FROM workspaces 
        WHERE workspace_id = $1
    """
    params_tuple = (workspace_id,) 
    if check_active:
        query += " AND is_active = TRUE"
        
    workspace_row = await db_manager.execute_query(query, params_tuple, fetch_one=True)
    
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

async def check_workspace_membership_and_get_role(user_id: UUID, workspace_id: UUID, required_role: Optional[str] = None) -> dict:
    query = """
        SELECT wm.membership_id, wm.workspace_id, wm.user_id, u.username, wm.role, 
               wm.created_at, wm.updated_at
        FROM workspace_members wm
        JOIN users u ON wm.user_id = u.user_id
        WHERE wm.user_id = $1 AND wm.workspace_id = $2
    """
    membership_row = await db_manager.execute_query(query, (user_id, workspace_id), fetch_one=True)
    
    if not membership_row:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not a member of this workspace."
        )
    
    user_actual_role = membership_row["role"]
    
    if required_role and user_actual_role != required_role and user_actual_role != 'admin': # Workspace admin
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"This action requires '{required_role}' or 'admin' role in the workspace. Your role: '{user_actual_role}'."
        )
    
    return {
        "membership_id": membership_row["membership_id"], "workspace_id": membership_row["workspace_id"], 
        "user_id": membership_row["user_id"], "username": membership_row["username"], 
        "role": user_actual_role, 
        "created_at": membership_row["created_at"], "updated_at": membership_row["updated_at"]
    }

async def get_user_and_workspace(username: str) -> tuple[Optional[UUID], Optional[UUID]]: # Return type made more precise
    try:
        if not username:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is required")

        query_user = "SELECT user_id FROM users WHERE username = $1"
        user_result = await db_manager.execute_query(query_user, (username,), fetch_one=True)

        if not user_result or not user_result.get("user_id"):
            logger.warning(f"User ID lookup failed for username: {username}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        user_id = UUID(str(user_result["user_id"]))

        workspace_query = """
            SELECT workspace_id FROM sessions
            WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1
        """
        ws_result = await db_manager.execute_query(workspace_query, (user_id,), fetch_one=True)

        workspace_id: Optional[UUID] = None
        if ws_result and ws_result.get("workspace_id"):
            workspace_id = UUID(str(ws_result["workspace_id"]))
        else:
            default_ws_query = """
                SELECT wm.workspace_id FROM workspace_members wm
                JOIN workspaces w ON wm.workspace_id = w.workspace_id
                WHERE wm.user_id = $1 AND w.is_active = TRUE
                ORDER BY wm.created_at ASC LIMIT 1
            """
            default_ws_result = await db_manager.execute_query(default_ws_query, (user_id,), fetch_one=True)
            if default_ws_result and default_ws_result.get("workspace_id"):
                workspace_id = UUID(str(default_ws_result["workspace_id"]))
            else:
                any_ws_query = """
                    SELECT wm.workspace_id FROM workspace_members wm
                    JOIN workspaces w ON wm.workspace_id = w.workspace_id
                    WHERE wm.user_id = $1 AND w.is_active = TRUE
                    ORDER BY w.name ASC LIMIT 1
                """
                any_ws_result = await db_manager.execute_query(any_ws_query, (user_id,), fetch_one=True)
                if any_ws_result and any_ws_result.get("workspace_id"):
                    workspace_id = UUID(str(any_ws_result["workspace_id"]))
                    logger.info(f"User {username} has no specific active workspace in session or default; using first available active workspace: {workspace_id}")
                else:
                    logger.warning(f"User {username} (ID: {user_id}) has no active workspace, and no other active workspaces found to assign.")
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active or available workspace found for user. Please activate or create a workspace.")
        
        return user_id, workspace_id

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user_id and workspace for username '{username}': {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                           detail="Failed to retrieve user and workspace information.")
    
@router.post("/workspaces", status_code=status.HTTP_201_CREATED, response_model=dict)
async def create_workspace(
    workspace_data: WorkspaceCreate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    workspace_id = uuid4()
    now = datetime.now(timezone.utc)
    current_user_id = current_user_data['user_id']
    username_for_log = current_user_data['username']
    
    try:
        existing_ws_query = "SELECT workspace_id FROM workspaces WHERE name = $1"
        existing_ws = await db_manager.execute_query(existing_ws_query, (workspace_data.name,), fetch_one=True)
        if existing_ws:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A workspace with this name already exists.")

        await db_manager.execute_query(
            "INSERT INTO workspaces (workspace_id, name, description, created_at, updated_at, is_active) VALUES ($1, $2, $3, $4, $5, $6)",
            (workspace_id, workspace_data.name, workspace_data.description, now, now, True)
        )
        
        membership_id = uuid4()
        await db_manager.execute_query(
            "INSERT INTO workspace_members (membership_id, workspace_id, user_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)",
            (membership_id, workspace_id, current_user_id, "admin", now, now)
        )
        
        await session_manager.log_action(
            content=f"User '{username_for_log}' created workspace '{workspace_data.name}' (ID: {workspace_id})",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id), 
            action_type="Workspace_Created",
            ip_address=request.client.host if request.client else "N/A", 
            user_agent=request.headers.get("user-agent")
        )
        return {"message": "Workspace created successfully", "workspace_id": str(workspace_id), "name": workspace_data.name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating workspace by user {current_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create workspace.")

@router.get("/workspaces/user", response_model=List[dict])
async def get_user_workspaces(
    include_inactive: bool = Query(False, description="Include inactive workspaces"),
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    current_user_id = current_user_data['user_id']
    query = """
        SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, w.is_active, wm.role as member_role
        FROM workspaces w
        JOIN workspace_members wm ON w.workspace_id = wm.workspace_id
        WHERE wm.user_id = $1
    """
    params_list = [current_user_id] 
    if not include_inactive:
        query += " AND w.is_active = TRUE"
    query += " ORDER BY w.name"
    
    try:
        workspaces_rows = await db_manager.execute_query(query, tuple(params_list), fetch_all=True)
        return [
            {
                "workspace_id": str(w_row["workspace_id"]), "name": w_row["name"], "description": w_row["description"],
                "created_at": w_row["created_at"].isoformat() if w_row["created_at"] else None,
                "updated_at": w_row["updated_at"].isoformat() if w_row["updated_at"] else None,
                "is_active": w_row["is_active"], "member_role": w_row["member_role"]
            } for w_row in workspaces_rows
        ] if workspaces_rows else []
    except Exception as e:
        logger.error(f"Error retrieving workspaces for user {current_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve workspaces.")

@router.get("/workspaces/{workspace_id_str}", response_model=dict)
async def get_workspace_details(
    workspace_id_str: str,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        if not is_system_admin(current_user_data):
            await check_workspace_membership_and_get_role(current_user_id, workspace_id)

        workspace_details = await get_workspace_by_id(workspace_id, check_active=False)
        
        count_result = await db_manager.execute_query("SELECT COUNT(*) as count FROM workspace_members WHERE workspace_id = $1", (workspace_id,), fetch_one=True)
        workspace_details["member_count"] = count_result["count"] if count_result else 0
        
        workspace_details["workspace_id"] = str(workspace_details["workspace_id"]) # Already UUID, convert to str for response
        workspace_details["created_at"] = workspace_details["created_at"].isoformat() if workspace_details["created_at"] else None
        workspace_details["updated_at"] = workspace_details["updated_at"].isoformat() if workspace_details["updated_at"] else None
        
        return workspace_details
    except ValueError: 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving details for workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve workspace details.")

@router.put("/workspaces/{workspace_id_str}", response_model=dict)
async def update_workspace(
    workspace_id_str: str,
    workspace_update_data: WorkspaceUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']

        membership_info = None
        if not is_system_admin(current_user_data):
            membership_info = await check_workspace_membership_and_get_role(current_user_id, workspace_id, required_role="admin")
        else:
            admin_user_info = await user_manager.get_user_by_id(str(current_user_id))
            membership_info = {"username": admin_user_info.get("username", "SystemAdmin")}

        update_fields_clauses, params_list = [], []
        param_idx = 1
        
        if workspace_update_data.name is not None:
            current_workspace_details = await get_workspace_by_id(workspace_id, check_active=False)
            if workspace_update_data.name != current_workspace_details["name"]:
                existing_ws_query = "SELECT workspace_id FROM workspaces WHERE name = $1 AND workspace_id != $2"
                existing_ws = await db_manager.execute_query(existing_ws_query, (workspace_update_data.name, workspace_id), fetch_one=True)
                if existing_ws:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A workspace with this name already exists.")
            update_fields_clauses.append(f"name = ${param_idx}"); params_list.append(workspace_update_data.name); param_idx += 1

        if workspace_update_data.description is not None:
            update_fields_clauses.append(f"description = ${param_idx}"); params_list.append(workspace_update_data.description); param_idx += 1
        if workspace_update_data.is_active is not None:
            update_fields_clauses.append(f"is_active = ${param_idx}"); params_list.append(workspace_update_data.is_active); param_idx += 1
        
        if not update_fields_clauses:
            return {"message": "No update data provided."}

        update_fields_clauses.append(f"updated_at = ${param_idx}"); params_list.append(datetime.now(timezone.utc)); param_idx +=1
        params_list.append(workspace_id) 
        
        query = f"UPDATE workspaces SET {', '.join(update_fields_clauses)} WHERE workspace_id = ${param_idx}"
        rows_affected = await db_manager.execute_query(query, tuple(params_list), return_rowcount=True)

        if rows_affected == 0 and update_fields_clauses:
            await get_workspace_by_id(workspace_id, check_active=False) 
            logger.warning(f"Workspace {workspace_id} update by admin {current_user_id} affected 0 rows, though it exists and fields were provided.")

        log_username = membership_info.get('username', current_user_data.get('username', 'UnknownUser'))
        await session_manager.log_action(
            content=f"User '{log_username}' updated workspace (ID: {workspace_id}). Fields: {workspace_update_data.model_dump(exclude_unset=True)}",
            user_id=str(current_user_id), workspace_id=str(workspace_id), action_type="Workspace_Updated",
            ip_address=request.client.host if request.client else "N/A", user_agent=request.headers.get("user-agent")
        )
        return {"message": "Workspace updated successfully"}
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update workspace.")

@router.get("/workspaces/{workspace_id_str}/members", response_model=List[dict])
async def get_workspace_members(
    workspace_id_str: str,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']

        if not is_system_admin(current_user_data):
            await check_workspace_membership_and_get_role(current_user_id, workspace_id)
        
        await get_workspace_by_id(workspace_id, check_active=False) # Ensure workspace exists

        query = """
            SELECT wm.membership_id, wm.workspace_id, wm.user_id, u.username, wm.role, 
                   wm.created_at, wm.updated_at
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.workspace_id = $1 ORDER BY u.username
        """
        members_rows = await db_manager.execute_query(query, (workspace_id,), fetch_all=True)
        return [
            {
                "membership_id": str(m_row["membership_id"]), "workspace_id": str(m_row["workspace_id"]), 
                "user_id": str(m_row["user_id"]), "username": m_row["username"], "role": m_row["role"],
                "created_at": m_row["created_at"].isoformat() if m_row["created_at"] else None,
                "updated_at": m_row["updated_at"].isoformat() if m_row["updated_at"] else None
            } for m_row in members_rows
        ] if members_rows else []
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving members for workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve workspace members.")

@router.post("/workspaces/{workspace_id_str}/members", status_code=status.HTTP_201_CREATED, response_model=dict)
async def add_workspace_member(
    workspace_id_str: str,
    member_data: WorkspaceMemberCreate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']

        if not is_system_admin(current_user_data):
            await check_workspace_membership_and_get_role(current_user_id, workspace_id, required_role="admin")
        else:
            await get_workspace_by_id(workspace_id, check_active=True)
        
        target_user_id_obj = member_data.user_id
        target_user_info = await user_manager.get_user_by_id(str(target_user_id_obj))
        if not target_user_info:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{target_user_id_obj}' not found.")

        existing_member = await db_manager.execute_query(
            "SELECT membership_id FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
            (workspace_id, target_user_id_obj), fetch_one=True
        )
        if existing_member:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User is already a member of this workspace.")
            
        membership_id = uuid4()
        now = datetime.now(timezone.utc)
        await db_manager.execute_query(
            "INSERT INTO workspace_members (membership_id, workspace_id, user_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)",
            (membership_id, workspace_id, target_user_id_obj, member_data.role, now, now)
        )
        
        await session_manager.log_action(
            content=f"User '{admin_username}' added user '{target_user_info['username']}' to workspace (ID: {workspace_id}) with role '{member_data.role}'.",
            user_id=str(current_user_id), workspace_id=str(workspace_id), action_type="Workspace_Member_Added",
            ip_address=request.client.host if request.client else "N/A", user_agent=request.headers.get("user-agent")
        )
        return {"message": "Member added successfully", "membership_id": str(membership_id), "user_id": str(target_user_id_obj), "role": member_data.role}
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding member to workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to add member to workspace.")

@router.put("/workspaces/{workspace_id_str}/members/{target_user_id_str}", response_model=dict)
async def update_workspace_member_role(
    workspace_id_str: str,
    target_user_id_str: str,
    member_update_data: WorkspaceMemberUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        target_user_id = UUID(target_user_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']

        if not is_system_admin(current_user_data):
            await check_workspace_membership_and_get_role(current_user_id, workspace_id, required_role="admin")
        else:
            await get_workspace_by_id(workspace_id, check_active=True)

        if target_user_id == current_user_id and member_update_data.role != "admin":
            if not is_system_admin(current_user_data):
                other_admins_result = await db_manager.execute_query(
                    "SELECT COUNT(*) as count FROM workspace_members WHERE workspace_id = $1 AND role = 'admin' AND user_id != $2",
                    (workspace_id, current_user_id), fetch_one=True
                )
                if not other_admins_result or other_admins_result["count"] == 0:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot demote the only admin in the workspace.")

        target_member_info = await db_manager.execute_query(
            "SELECT u.username FROM workspace_members wm JOIN users u ON wm.user_id = u.user_id WHERE wm.workspace_id = $1 AND wm.user_id = $2",
            (workspace_id, target_user_id), fetch_one=True
        )
        if not target_member_info:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target user is not a member of this workspace.")
        target_username_for_log = target_member_info["username"]

        rows_affected = await db_manager.execute_query(
            "UPDATE workspace_members SET role = $1, updated_at = $2 WHERE workspace_id = $3 AND user_id = $4",
            (member_update_data.role, datetime.now(timezone.utc), workspace_id, target_user_id), return_rowcount=True
        )
        if rows_affected == 0:
            if not await db_manager.execute_query(
                "SELECT membership_id FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
                (workspace_id, target_user_id), fetch_one=True):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found for update.")

        await session_manager.log_action(
            content=f"User '{admin_username}' updated role of '{target_username_for_log}' to '{member_update_data.role}' in workspace (ID: {workspace_id}).",
            user_id=str(current_user_id), workspace_id=str(workspace_id), action_type="Workspace_Member_Updated",
            ip_address=request.client.host if request.client else "N/A", user_agent=request.headers.get("user-agent")
        )
        return {"message": "Member role updated successfully."}
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating member role in workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update member role.")
    
@router.delete("/workspaces/{workspace_id_str}/members/{target_user_id_str}", response_model=dict)
async def remove_workspace_member(
    workspace_id_str: str,
    target_user_id_str: str,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        target_user_id = UUID(target_user_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']

        if not is_system_admin(current_user_data):
            await check_workspace_membership_and_get_role(current_user_id, workspace_id, required_role="admin")
        else:
            await get_workspace_by_id(workspace_id, check_active=False)

        target_member_role_info = await db_manager.execute_query(
            "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
            (workspace_id, target_user_id), fetch_one=True
        )
        if not target_member_role_info:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target user is not a member of this workspace.")

        if target_member_role_info["role"] == 'admin':
            other_admins_result = await db_manager.execute_query(
                "SELECT COUNT(*) as count FROM workspace_members WHERE workspace_id = $1 AND role = 'admin' AND user_id != $2",
                (workspace_id, target_user_id), fetch_one=True
            )
            if not other_admins_result or other_admins_result["count"] == 0:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot remove the only admin from the workspace. Assign admin role to another member first or delete the workspace.")

        target_user_info = await user_manager.get_user_by_id(str(target_user_id))
        target_username_for_log = target_user_info.get("username", "UnknownUser") if target_user_info else "UnknownUser"

        rows_affected = await db_manager.execute_query(
            "DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
            (workspace_id, target_user_id), return_rowcount=True
        )
        if rows_affected == 0:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found for deletion (or was already removed).")

        await session_manager.log_action(
            content=f"User '{admin_username}' removed user '{target_username_for_log}' from workspace (ID: {workspace_id}).",
            user_id=str(current_user_id), workspace_id=str(workspace_id), action_type="Workspace_Member_Removed",
            ip_address=request.client.host if request.client else "N/A", user_agent=request.headers.get("user-agent")
        )
        return {"message": "Member removed successfully."}
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error removing member from workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to remove member.")

@router.post("/workspaces/{workspace_id_str}/activate", response_model=dict)
async def activate_workspace(
    workspace_id_str: str,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        username_for_log = current_user_data['username']

        await check_workspace_membership_and_get_role(current_user_id, workspace_id)
        workspace_details = await get_workspace_by_id(workspace_id, check_active=True)

        await db_manager.execute_query(
            "UPDATE user_tokens SET workspace_id = $1, updated_at = $2 WHERE user_id = $3 AND is_active = TRUE",
            (workspace_id, datetime.now(timezone.utc), current_user_id)
        )

        await session_manager.log_action(
            content=f"User '{username_for_log}' activated workspace '{workspace_details['name']}' (ID: {workspace_id}).",
            user_id=str(current_user_id), workspace_id=str(workspace_id), action_type="Workspace_Activated",
            ip_address=request.client.host if request.client else "N/A", user_agent=request.headers.get("user-agent")
        )
        return {"message": f"Workspace '{workspace_details['name']}' is now active."}
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error activating workspace {workspace_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to activate workspace.")

@router.post("/workspaces/migrate")
async def migrate_to_workspace_model(
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    if not is_system_admin(current_user_data):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only system admin users can perform migration."
        )
    try:
        await db_manager.execute_query("SELECT migrate_data_to_workspace_model()")

        await session_manager.log_action(
            content=f"Admin user '{current_user_data['username']}' performed migration to workspace model",
            user_id=str(current_user_data['user_id']),
            action_type="Workspace_Migration",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent"),
            status="success" # Assuming 'status' is a valid field for log_action
        )
        return {"message": "Migration to workspace model completed successfully"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in migration: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                           detail="Failed to migrate to workspace model")

@router.get("/admin/all-workspaces", response_model=List[dict])
async def admin_get_all_workspaces(
    include_inactive: bool = Query(False, description="Include inactive workspaces"),
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    if not is_system_admin(current_user_data):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="This action requires system admin privileges.")

    query = """
        SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, w.is_active,
               (SELECT COUNT(*) FROM workspace_members wm WHERE wm.workspace_id = w.workspace_id) as member_count
        FROM workspaces w
    """
    # No parameters are needed for the main query part with asyncpg if using string formatting for the optional WHERE clause.
    # If parameters were needed, they'd be $1, $2, etc.
    if not include_inactive:
        query += " WHERE w.is_active = TRUE"
    query += " ORDER BY w.name"

    try:
        # If `include_inactive` was a parameter to the query, it would be:
        # params = () if include_inactive else (True,) 
        # await db_manager.execute_query(query, params, fetch_all=True)
        # But since it's conditionally added to the query string, no params are needed here for that part.
        workspaces_rows = await db_manager.execute_query(query, fetch_all=True) 
        return [
            {
                "workspace_id": str(ws_row["workspace_id"]), "name": ws_row["name"], "description": ws_row["description"],
                "created_at": ws_row["created_at"].isoformat() if ws_row["created_at"] else None,
                "updated_at": ws_row["updated_at"].isoformat() if ws_row["updated_at"] else None,
                "is_active": ws_row["is_active"], "member_count": ws_row["member_count"]
            } for ws_row in workspaces_rows
        ] if workspaces_rows else []
    except Exception as e:
        logger.error(f"SysAdmin error retrieving all workspaces: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve all workspaces.")

@router.get("/admin/all-users", response_model=List[UserResponse])
async def admin_get_all_users(
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    if not is_system_admin(current_user_data):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="This action requires system admin privileges.")

    try:
        all_users_data = await user_manager.get_all_users()
        response_users = []
        for user_dict in all_users_data:
            response_users.append(UserResponse(
                user_id=str(user_dict["user_id"]),
                username=user_dict["username"],
                email=user_dict["email"]
            ))
        return response_users
    except Exception as e:
        logger.error(f"SysAdmin error retrieving all users: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve all users.")
        