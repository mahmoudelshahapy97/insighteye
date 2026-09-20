# app/utils/permission_utils.py
from typing import Optional, Dict
from uuid import UUID
from fastapi import HTTPException, status
import logging

logger = logging.getLogger(__name__)

# System roles (users.role) are hierarchical: user < admin < superadmin.
# A superadmin must pass every system-admin check; comparing against the
# literal "admin" alone locks superadmins out.
#
# Do NOT use this for workspace membership roles (workspace_members.role:
# owner/admin/member). That is a separate namespace that happens to also
# contain the string "admin".
SYSTEM_ADMIN_ROLES = ("admin", "superadmin")


def is_system_admin_role(role: Optional[str]) -> bool:
    """True if a *system* role (users.role) carries admin privileges."""
    return role in SYSTEM_ADMIN_ROLES


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
        system_role: User's system role (user/admin/superadmin)

    Returns:
        Dict with user's workspace role

    Raises:
        HTTPException: If access denied
    """
    # System admins (including superadmins) bypass checks
    if is_system_admin_role(system_role):
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
