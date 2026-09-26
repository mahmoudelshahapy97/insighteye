# app/routes/workspace_router.py
from fastapi import APIRouter, HTTPException, Depends, status, Request, Query
from typing import List, Dict, Literal
from uuid import UUID
import logging
from pydantic import BaseModel
from app.services.session_service import session_manager
from app.services.user_service import user_manager
from app.services.workspace_service import workspace_service
from app.services.feature_service import (
    FEATURES, feature_service, current_workspace_id,
)
from app.schemas import (
    WorkspaceCreate, 
    WorkspaceUpdate, 
    WorkspaceMemberCreate,
    WorkspaceMemberUpdate,
    UserResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["workspaces"])

FeatureKey = Literal[FEATURES]


class FeaturesUpdate(BaseModel):
    features: List[FeatureKey]

# Helper dependencies
async def get_current_user_full_data_dependency(
    username: str = Depends(session_manager.get_current_user)
) -> Dict:
    """Dependency to get the current authenticated user's full data including role."""
    user_data = await user_manager.get_user_by_username(username)
    if not user_data or "user_id" not in user_data or "role" not in user_data:
        logger.error(f"User data, user_id, or role not found for username: {username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user session or user data incomplete."
        )
    
    try:
        current_user_id_value = user_data.get("user_id")
        if current_user_id_value is None:
            logger.error(f"User ID is None for username: {username}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="User ID is unexpectedly None."
            )

        if not isinstance(current_user_id_value, UUID):
            user_data['user_id'] = UUID(current_user_id_value)
        
        return user_data
    except (ValueError, TypeError) as e:
        logger.error(f"Error converting user ID for username '{username}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User ID format or type error."
        )


async def get_current_user_id_dependency(
    username: str = Depends(session_manager.get_current_user)
) -> Dict:
    """Dependency to get the current authenticated user's ID."""
    user_data = await user_manager.get_user_by_username(username)
    if not user_data or "user_id" not in user_data:
        logger.error(f"User data or user_id not found for username: {username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user session or user not found."
        )
    
    try:
        current_user_id_value = user_data.get("user_id")
        if current_user_id_value is None:
            logger.error(f"User ID is None for username: {username}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="User ID is unexpectedly None."
            )

        if not isinstance(current_user_id_value, UUID):
            user_data['user_id'] = UUID(current_user_id_value)
        
        return user_data
    except (ValueError, TypeError) as e:
        logger.error(f"Error converting user ID for username '{username}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User ID format or type error."
        )


# Workspace endpoints
@router.post("/workspaces", status_code=status.HTTP_201_CREATED, response_model=dict)
async def create_workspace(
    workspace_data: WorkspaceCreate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Create a new workspace."""
    current_user_id = current_user_data['user_id']
    username = current_user_data['username']
    
    result = await workspace_service.create_workspace(
        workspace_data,
        current_user_id,
        username
    )
    
    await session_manager.log_action(
        content=f"User '{username}' created workspace '{result['name']}' (ID: {result['workspace_id']})",
        user_id=str(current_user_id),
        workspace_id=result['workspace_id'],
        action_type="Workspace_Created",
        ip_address=request.client.host if request.client else "N/A",
        user_agent=request.headers.get("user-agent")
    )
    
    return {
        "message": "Workspace created successfully",
        "workspace_id": result['workspace_id'],
        "name": result['name']
    }


@router.get("/workspaces/user", response_model=List[dict])
async def get_user_workspaces(
    include_inactive: bool = Query(False, description="Include inactive workspaces"),
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Get all workspaces for the current user (superadmin: every workspace)."""
    if workspace_service.is_superadmin(current_user_data):
        return await workspace_service.get_all_workspaces(include_inactive)
    current_user_id = current_user_data['user_id']
    return await workspace_service.get_user_workspaces(current_user_id, include_inactive)


@router.get("/workspaces/active")
async def get_active_workspace(
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """The workspace the current user is working in, or null."""
    ws = await workspace_service.get_active_workspace(current_user_data['user_id'])
    if not ws:
        return None
    return {
        "workspace_id": str(ws["workspace_id"]),
        "name": ws["name"],
        "member_role": ws["member_role"],
    }


@router.get("/features/me")
async def get_my_features(
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Features the current user may use in their active workspace."""
    workspace_id = await current_workspace_id(current_user_data)
    features = await feature_service.effective_features(current_user_data, workspace_id)
    return {
        "workspace_id": str(workspace_id) if workspace_id else None,
        "features": [f for f in FEATURES if f in features],
    }


@router.get("/workspaces/{workspace_id_str}", response_model=dict)
async def get_workspace_details(
    workspace_id_str: str,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Get detailed information about a specific workspace."""
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        return await workspace_service.get_workspace_details(
            workspace_id,
            current_user_id,
            is_admin
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace ID format."
        )


@router.put("/workspaces/{workspace_id_str}", response_model=dict)
async def update_workspace(
    workspace_id_str: str,
    workspace_update_data: WorkspaceUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Update workspace information."""
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        result = await workspace_service.update_workspace(
            workspace_id,
            workspace_update_data,
            current_user_id,
            is_admin
        )
        
        if not result['updated']:
            return {"message": "No update data provided."}
        
        await session_manager.log_action(
            content=f"User '{result['username']}' updated workspace (ID: {workspace_id}). Fields: {result['update_data']}",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id),
            action_type="Workspace_Updated",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent")
        )
        
        return {"message": "Workspace updated successfully"}
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace ID format."
        )


@router.get("/workspaces/{workspace_id_str}/members", response_model=List[dict])
async def get_workspace_members(
    workspace_id_str: str,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Get all members of a workspace."""
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        return await workspace_service.get_workspace_members(
            workspace_id,
            current_user_id,
            is_admin
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace ID format."
        )


@router.post("/workspaces/{workspace_id_str}/members", status_code=status.HTTP_201_CREATED, response_model=dict)
async def add_workspace_member(
    workspace_id_str: str,
    member_data: WorkspaceMemberCreate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_id_dependency)
):
    """Add a new member to a workspace."""
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        result = await workspace_service.add_workspace_member(
            workspace_id,
            member_data,
            current_user_id,
            admin_username,
            is_admin
        )
        
        await session_manager.log_action(
            content=f"User '{result['admin_username']}' added user '{result['target_username']}' to workspace (ID: {workspace_id}) with role '{result['role']}'.",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id),
            action_type="Workspace_Member_Added",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent")
        )
        
        return {
            "message": "Member added successfully",
            "membership_id": result['membership_id'],
            "user_id": result['user_id'],
            "role": result['role']
        }
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace ID format."
        )


@router.put("/workspaces/{workspace_id_str}/members/{target_user_id_str}", response_model=dict)
async def update_workspace_member_role(
    workspace_id_str: str,
    target_user_id_str: str,
    member_update_data: WorkspaceMemberUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Update a workspace member's role."""
    try:
        workspace_id = UUID(workspace_id_str)
        target_user_id = UUID(target_user_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        result = await workspace_service.update_workspace_member_role(
            workspace_id,
            target_user_id,
            member_update_data,
            current_user_id,
            admin_username,
            is_admin
        )
        
        await session_manager.log_action(
            content=f"User '{result['admin_username']}' updated role of '{result['target_username']}' to '{result['new_role']}' in workspace (ID: {workspace_id}).",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id),
            action_type="Workspace_Member_Updated",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent")
        )
        
        return {"message": "Member role updated successfully."}
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID format."
        )


@router.delete("/workspaces/{workspace_id_str}/members/{target_user_id_str}", response_model=dict)
async def remove_workspace_member(
    workspace_id_str: str,
    target_user_id_str: str,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Remove a member from a workspace."""
    try:
        workspace_id = UUID(workspace_id_str)
        target_user_id = UUID(target_user_id_str)
        current_user_id = current_user_data['user_id']
        admin_username = current_user_data['username']
        is_admin = workspace_service.is_system_admin(current_user_data)
        
        result = await workspace_service.remove_workspace_member(
            workspace_id,
            target_user_id,
            current_user_id,
            admin_username,
            is_admin
        )
        
        await session_manager.log_action(
            content=f"User '{result['admin_username']}' removed user '{result['target_username']}' from workspace (ID: {workspace_id}).",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id),
            action_type="Workspace_Member_Removed",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent")
        )
        
        return {"message": "Member removed successfully."}
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID format."
        )


@router.post("/workspaces/{workspace_id_str}/activate", response_model=dict)
async def activate_workspace(
    workspace_id_str: str,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Activate a workspace for the current user."""
    try:
        workspace_id = UUID(workspace_id_str)
        current_user_id = current_user_data['user_id']
        username = current_user_data['username']
        
        result = await workspace_service.activate_workspace(
            workspace_id,
            current_user_id,
            current_user_data.get('role')
        )
        
        await session_manager.log_action(
            content=f"User '{username}' activated workspace '{result['workspace_name']}' (ID: {workspace_id}).",
            user_id=str(current_user_id),
            workspace_id=str(workspace_id),
            action_type="Workspace_Activated",
            ip_address=request.client.host if request.client else "N/A",
            user_agent=request.headers.get("user-agent")
        )
        
        return {"message": f"Workspace '{result['workspace_name']}' is now active."}
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace ID format."
        )


@router.get("/admin/all-workspaces", response_model=List[dict])
async def admin_get_all_workspaces(
    include_inactive: bool = Query(False, description="Include inactive workspaces"),
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Get all workspaces (superadmin only)."""
    if not workspace_service.is_superadmin(current_user_data):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires superadmin privileges."
        )

    return await workspace_service.get_all_workspaces(include_inactive)


@router.get("/admin/all-users", response_model=List[UserResponse])
async def admin_get_all_users(
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Get all users (superadmin only)."""
    if not workspace_service.is_superadmin(current_user_data):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires superadmin privileges."
        )

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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve all users."
        )


def _require_superadmin(current_user_data: Dict) -> None:
    if not workspace_service.is_superadmin(current_user_data):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires superadmin privileges."
        )


@router.put("/admin/users/{user_id}/features", response_model=dict)
async def admin_set_user_features(
    user_id: UUID,
    body: FeaturesUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Open/close features for a user (superadmin only)."""
    _require_superadmin(current_user_data)
    username = await feature_service.set_user_features(user_id, body.features)
    if username is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    await session_manager.log_action(
        content=f"User '{current_user_data['username']}' set features of user '{username}' to {sorted(set(body.features))}.",
        user_id=str(current_user_data['user_id']),
        action_type="User_Features_Updated",
        ip_address=request.client.host if request.client else "N/A",
        user_agent=request.headers.get("user-agent")
    )
    return {"message": "User features updated.", "features": sorted(set(body.features))}


@router.put("/admin/workspaces/{workspace_id}/features", response_model=dict)
async def admin_set_workspace_features(
    workspace_id: UUID,
    body: FeaturesUpdate,
    request: Request,
    current_user_data: Dict = Depends(get_current_user_full_data_dependency)
):
    """Open/close features for a workspace (superadmin only)."""
    _require_superadmin(current_user_data)
    name = await feature_service.set_workspace_features(workspace_id, body.features)
    if name is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found.")

    await session_manager.log_action(
        content=f"User '{current_user_data['username']}' set features of workspace '{name}' to {sorted(set(body.features))}.",
        user_id=str(current_user_data['user_id']),
        workspace_id=str(workspace_id),
        action_type="Workspace_Features_Updated",
        ip_address=request.client.host if request.client else "N/A",
        user_agent=request.headers.get("user-agent")
    )
    return {"message": "Workspace features updated.", "features": sorted(set(body.features))}
