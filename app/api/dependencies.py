# app/api/dependencies.py
"""Shared FastAPI dependencies for route-level authorization."""
import logging
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request, status

from app.services.session_service import session_manager
from app.utils.permission_utils import is_system_admin_role

logger = logging.getLogger(__name__)


async def require_system_admin(
    request: Request,
    current_user: Dict[str, Any] = Depends(session_manager.get_current_user_full_data_dependency),
) -> Dict[str, Any]:
    """Authenticated user whose *system* role is admin or superadmin."""
    if not is_system_admin_role(current_user.get("role")):
        logger.warning(
            "Non-admin user '%s' denied access to %s",
            current_user.get("username", "unknown"),
            request.url.path,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="System admin privileges required.",
        )
    return current_user
