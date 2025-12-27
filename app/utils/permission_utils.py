# app/utils/permission_utils.py
from typing import Optional, Dict
from uuid import UUID
from fastapi import HTTPException, status
import logging

logger = logging.getLogger(__name__)

async def check_workspace_access(
    db_manager,
    user_id: UUID,
    workspace_id: UUID,
    required_role: Optional[str] = None,
    system_role: Optional[str] = None
) -> Dict[str, str]:
    """
    Centralized workspace permission check.
    
    Args:
        db_manager: Database manager instance
        user_id: User UUID
        workspace_id: Workspace UUID
        required_role: Required workspace role (member/admin/owner)
        system_role: User's system role (user/admin)
        
    Returns:
        Dict with user's workspace role
        
    Raises:
        HTTPException: If access denied
    """
    # System admins bypass checks
    if system_role == "admin":
        return {"role": "admin", "system_override": True}
    
    # Check membership
    query = """
        SELECT role FROM workspace_members 
        WHERE user_id = $1 AND workspace_id = $2
    """
    member_info = await db_manager.execute_query(
        query, (user_id, workspace_id), fetch_one=True
    )
    
    if not member_info:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: Not a workspace member"
        )
    
    user_role = member_info['role']
    
    # Check required role
    if required_role:
        role_hierarchy = {'owner': 3, 'admin': 2, 'member': 1}
        
        user_level = role_hierarchy.get(user_role, 0)
        required_level = role_hierarchy.get(required_role, 0)
        
        if user_level < required_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required role: {required_role}, your role: {user_role}"
            )
    
    return {"role": user_role}

def check_resource_ownership(
    resource_owner_id: UUID,
    requesting_user_id: UUID,
    workspace_role: Optional[str] = None,
    system_role: Optional[str] = None
) -> bool:
    """
    Check if user can access a resource.
    
    Returns:
        True if access granted, False otherwise
    """
    # System admin
    if system_role == "admin":
        return True
    
    # Workspace admin
    if workspace_role in ["admin", "owner"]:
        return True
    
    # Resource owner
    if resource_owner_id == requesting_user_id:
        return True
    
    return False
