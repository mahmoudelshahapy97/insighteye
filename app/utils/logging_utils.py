# app/utils/logging_utils.py
import logging
from typing import Optional, Any, Dict
from uuid import UUID

def get_service_logger(name: str) -> logging.Logger:
    """Get logger configured for service."""
    return logging.getLogger(f"app.services.{name}")

def log_user_action(
    logger: logging.Logger,
    action: str,
    user_id: Optional[UUID] = None,
    username: Optional[str] = None,
    workspace_id: Optional[UUID] = None,
    details: Optional[Dict[str, Any]] = None,
    level: str = "info"
):
    """
    Standardized user action logging.
    
    Example:
        log_user_action(
            logger, "camera_created",
            user_id=user_id,
            username=username,
            details={"camera_id": camera_id, "name": name}
        )
    """
    parts = [f"ACTION: {action}"]
    
    if username:
        parts.append(f"USER: {username}")
    elif user_id:
        parts.append(f"USER_ID: {str(user_id)}")
    
    if workspace_id:
        parts.append(f"WS: {str(workspace_id)}")
    
    if details:
        detail_str = ", ".join([f"{k}={v}" for k, v in details.items()])
        parts.append(f"DETAILS: {detail_str}")
    
    message = " | ".join(parts)
    
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)

def log_error(
    logger: logging.Logger,
    error: Exception,
    context: str,
    user_id: Optional[UUID] = None,
    additional_info: Optional[Dict] = None
):
    """
    Standardized error logging.
    
    Example:
        log_error(
            logger, e, "camera_creation_failed",
            user_id=user_id,
            additional_info={"camera_name": name}
        )
    """
    parts = [f"ERROR in {context}: {type(error).__name__}: {str(error)}"]
    
    if user_id:
        parts.append(f"USER_ID: {str(user_id)}")
    
    if additional_info:
        info_str = ", ".join([f"{k}={v}" for k, v in additional_info.items()])
        parts.append(f"INFO: {info_str}")
    
    logger.error(" | ".join(parts), exc_info=True)