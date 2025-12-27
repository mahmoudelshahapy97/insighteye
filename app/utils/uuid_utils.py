# app/utils/uuid_utils.py
from typing import Any, Optional, Union
from uuid import UUID
import logging

logger = logging.getLogger(__name__)

def ensure_uuid(value: Any) -> Optional[UUID]:
    """
    Convert any value to UUID object.
    
    Args:
        value: String, UUID, or None
        
    Returns:
        UUID object or None
        
    Raises:
        ValueError: If value is not a valid UUID
    """
    if value is None:
        return None
    
    if isinstance(value, UUID):
        return value
    
    try:
        return UUID(str(value))
    except (ValueError, AttributeError) as e:
        logger.error(f"Invalid UUID value: {value}")
        raise ValueError(f"Invalid UUID format: {value}") from e

def ensure_uuid_str(value: Any) -> Optional[str]:
    """
    Convert any value to UUID string.
    
    Args:
        value: String, UUID, or None
        
    Returns:
        UUID string or None
    """
    uuid_obj = ensure_uuid(value)
    return str(uuid_obj) if uuid_obj else None

def validate_uuid_list(values: list) -> list[UUID]:
    """
    Validate and convert list of values to UUID objects.
    
    Args:
        values: List of UUID strings or objects
        
    Returns:
        List of UUID objects
        
    Raises:
        ValueError: If any value is invalid
    """
    result = []
    for val in values:
        if val:  # Skip None/empty
            uuid_obj = ensure_uuid(val)
            if uuid_obj:
                result.append(uuid_obj)
    return result