# app/schemas/utils.py
from typing import Dict, Any, Optional, List, Union
import re
from datetime import datetime

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_camera_alert_thresholds(
    greater_threshold: Optional[int], 
    less_threshold: Optional[int]
) -> bool:
    """
    Validate that alert thresholds are logically consistent.
    
    Args:
        greater_threshold: Alert when count is greater than this value
        less_threshold: Alert when count is less than this value
    
    Returns:
        bool: True if thresholds are valid, False otherwise
    
    Example:
        >>> validate_camera_alert_thresholds(10, 5)
        True
        >>> validate_camera_alert_thresholds(5, 10)
        False
    """
    if greater_threshold is not None and less_threshold is not None:
        if less_threshold >= greater_threshold:
            return False
    return True

# ============================================================================
# SANITIZATION FUNCTIONS
# ============================================================================

def sanitize_string(value: Any) -> Optional[str]:
    """
    Sanitize string value by trimming whitespace and handling None/empty.
    
    Args:
        value: Value to sanitize
    
    Returns:
        Optional[str]: Sanitized string or None
    """
    if value is None:
        return None
    
    str_value = str(value).strip()
    
    # Return None for empty strings or common null representations
    if not str_value or str_value.lower() in ('none', 'null', 'n/a', 'na', ''):
        return None
    
    return str_value
