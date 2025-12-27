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


def validate_location_hierarchy(data: Dict[str, Any]) -> bool:
    """
    Validate location hierarchy consistency.
    Ensures that more specific levels have their parent levels defined.
    
    Args:
        data: Dictionary containing location hierarchy fields
    
    Returns:
        bool: True if hierarchy is valid
    
    Hierarchy levels (from general to specific):
        location -> area -> building -> floor_level -> zone
    """
    # Extract hierarchy fields
    location = data.get('location')
    area = data.get('area')
    building = data.get('building')
    floor_level = data.get('floor_level')
    zone = data.get('zone')
    
    # Zone requires floor_level
    if zone and not floor_level:
        return False
    
    # Floor_level requires building
    if floor_level and not building:
        return False
    
    # Building requires area
    if building and not area:
        return False
    
    # Area requires location
    if area and not location:
        return False
    
    return True


def validate_coordinates(latitude: Optional[float], longitude: Optional[float]) -> bool:
    """
    Validate GPS coordinates.
    
    Args:
        latitude: Latitude value (-90 to 90)
        longitude: Longitude value (-180 to 180)
    
    Returns:
        bool: True if coordinates are valid
    """
    if latitude is not None:
        if not -90 <= latitude <= 90:
            return False
    
    if longitude is not None:
        if not -180 <= longitude <= 180:
            return False
    
    return True


def validate_email_format(email: str) -> bool:
    """
    Validate email format using regex.
    
    Args:
        email: Email address to validate
    
    Returns:
        bool: True if email format is valid
    """
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email))


def validate_camera_path(path: str, camera_type: str) -> bool:
    """
    Validate camera path based on camera type.
    
    Args:
        path: Camera path/URL
        camera_type: Type of camera (rtsp, http, local, etc.)
    
    Returns:
        bool: True if path is valid for the given type
    """
    path = path.strip()
    
    if camera_type == 'rtsp':
        return path.startswith('rtsp://')
    elif camera_type == 'http':
        return path.startswith(('http://', 'https://'))
    elif camera_type == 'local':
        return path.lower() == 'local' or path.startswith(('/dev/', 'COM', '/'))
    elif camera_type == 'video file':
        return path.endswith(('.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv'))
    
    return True  # 'other' type accepts any path


def validate_datetime_range(start: Optional[str], end: Optional[str]) -> bool:
    """
    Validate that start datetime is before end datetime.
    
    Args:
        start: Start datetime string (ISO format)
        end: End datetime string (ISO format)
    
    Returns:
        bool: True if range is valid
    """
    if start and end:
        try:
            start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
            return start_dt < end_dt
        except (ValueError, AttributeError):
            return False
    return True


# ============================================================================
# SANITIZATION FUNCTIONS
# ============================================================================

def sanitize_camera_data_with_alerts(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize camera data including alert fields.
    
    Args:
        data: Raw camera data dictionary
    
    Returns:
        Dict[str, Any]: Sanitized camera data
    """
    sanitized = {}
    
    # Basic fields
    sanitized['name'] = sanitize_string(data.get('name', ''))
    sanitized['path'] = sanitize_string(data.get('path', ''))
    sanitized['type'] = sanitize_string(data.get('type', 'local')).lower()
    sanitized['status'] = sanitize_string(data.get('status', 'inactive')).lower()
    
    # Boolean fields
    sanitized['is_streaming'] = parse_boolean(data.get('is_streaming', False))
    sanitized['alert_enabled'] = parse_boolean(data.get('alert_enabled', False))
    
    # Location fields
    location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
    for field in location_fields:
        value = data.get(field)
        sanitized[field] = sanitize_string(value) if value else None
    
    # Coordinate fields
    sanitized['latitude'] = parse_float(data.get('latitude'))
    sanitized['longitude'] = parse_float(data.get('longitude'))
    
    # Alert threshold fields
    sanitized['count_threshold_greater'] = parse_int(data.get('count_threshold_greater'))
    sanitized['count_threshold_less'] = parse_int(data.get('count_threshold_less'))
    
    return sanitized


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


def sanitize_location_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize location hierarchy data.
    
    Args:
        data: Raw location data dictionary
    
    Returns:
        Dict[str, Any]: Sanitized location data
    """
    sanitized = {}
    
    # Location hierarchy fields
    location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'location_name', 'area_name']
    for field in location_fields:
        if field in data:
            sanitized[field] = sanitize_string(data.get(field))
    
    # Coordinate fields
    sanitized['latitude'] = parse_float(data.get('latitude'))
    sanitized['longitude'] = parse_float(data.get('longitude'))
    
    # Description field
    if 'description' in data:
        sanitized['description'] = sanitize_string(data.get('description'))
    
    # Boolean fields
    if 'is_active' in data:
        sanitized['is_active'] = parse_boolean(data.get('is_active'))
    
    return sanitized


def sanitize_bulk_assignment(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize bulk location assignment data.
    
    Args:
        data: Raw bulk assignment data
    
    Returns:
        Dict[str, Any]: Sanitized bulk assignment data
    """
    sanitized = {}
    
    # Camera IDs (required)
    camera_ids = data.get('camera_ids', [])
    if isinstance(camera_ids, str):
        camera_ids = [camera_ids]
    sanitized['camera_ids'] = [sanitize_string(cid) for cid in camera_ids if cid]
    
    # Location fields
    location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
    for field in location_fields:
        sanitized[field] = sanitize_string(data.get(field))
    
    # Coordinates
    sanitized['latitude'] = parse_float(data.get('latitude'))
    sanitized['longitude'] = parse_float(data.get('longitude'))
    
    # Alert settings
    sanitized['count_threshold_greater'] = parse_int(data.get('count_threshold_greater'))
    sanitized['count_threshold_less'] = parse_int(data.get('count_threshold_less'))
    sanitized['alert_enabled'] = parse_boolean(data.get('alert_enabled'))
    
    return sanitized


# ============================================================================
# PARSING FUNCTIONS
# ============================================================================

def parse_boolean(value: Any) -> bool:
    """
    Parse various representations of boolean values.
    
    Args:
        value: Value to parse as boolean
    
    Returns:
        bool: Parsed boolean value
    """
    if isinstance(value, bool):
        return value
    
    if value is None:
        return False
    
    str_value = str(value).lower().strip()
    return str_value in ('true', '1', 'yes', 'on', 't', 'y')


def parse_int(value: Any) -> Optional[int]:
    """
    Parse integer value with error handling.
    
    Args:
        value: Value to parse as integer
    
    Returns:
        Optional[int]: Parsed integer or None
    """
    if value is None or value == '':
        return None
    
    try:
        return int(float(value))  # Handle strings like "10.0"
    except (ValueError, TypeError):
        return None


def parse_float(value: Any) -> Optional[float]:
    """
    Parse float value with error handling.
    
    Args:
        value: Value to parse as float
    
    Returns:
        Optional[float]: Parsed float or None
    """
    if value is None or value == '':
        return None
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_list(value: Any, delimiter: str = ',') -> List[str]:
    """
    Parse comma-separated string into list.
    
    Args:
        value: Value to parse (string or list)
        delimiter: Delimiter for splitting string
    
    Returns:
        List[str]: Parsed list
    """
    if isinstance(value, list):
        return [sanitize_string(v) for v in value if v]
    
    if isinstance(value, str):
        return [sanitize_string(v) for v in value.split(delimiter) if v.strip()]
    
    return []


# ============================================================================
# FORMATTING FUNCTIONS
# ============================================================================

def format_camera_status(status: str) -> str:
    """
    Normalize camera status values.
    
    Args:
        status: Raw status value
    
    Returns:
        str: Normalized status (active, inactive, error, processing)
    """
    status = sanitize_string(status).lower() if status else 'inactive'
    
    valid_statuses = ['active', 'inactive', 'error', 'processing']
    return status if status in valid_statuses else 'inactive'


def format_camera_type(camera_type: str) -> str:
    """
    Normalize camera type values.
    
    Args:
        camera_type: Raw camera type value
    
    Returns:
        str: Normalized type (rtsp, http, local, other, video file)
    """
    camera_type = sanitize_string(camera_type).lower() if camera_type else 'local'
    
    valid_types = ['rtsp', 'http', 'local', 'other', 'video file']
    return camera_type if camera_type in valid_types else 'local'


def format_coordinate(coord: Optional[float], coord_type: str = 'latitude') -> Optional[float]:
    """
    Format and validate coordinate value.
    
    Args:
        coord: Coordinate value
        coord_type: Type of coordinate ('latitude' or 'longitude')
    
    Returns:
        Optional[float]: Formatted coordinate or None
    """
    if coord is None:
        return None
    
    try:
        coord = float(coord)
        
        if coord_type == 'latitude':
            return max(-90.0, min(90.0, coord))  # Clamp to valid range
        elif coord_type == 'longitude':
            return max(-180.0, min(180.0, coord))  # Clamp to valid range
        
        return coord
    except (ValueError, TypeError):
        return None


# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def extract_location_fields(data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract and sanitize location fields from data.
    
    Args:
        data: Dictionary containing location data
    
    Returns:
        Dict[str, Optional[str]]: Extracted location fields
    """
    return {
        'location': sanitize_string(data.get('location')),
        'area': sanitize_string(data.get('area')),
        'building': sanitize_string(data.get('building')),
        'floor_level': sanitize_string(data.get('floor_level')),
        'zone': sanitize_string(data.get('zone')),
        'latitude': parse_float(data.get('latitude')),
        'longitude': parse_float(data.get('longitude')),
    }


def extract_alert_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and sanitize alert fields from data.
    
    Args:
        data: Dictionary containing alert data
    
    Returns:
        Dict[str, Any]: Extracted alert fields
    """
    return {
        'count_threshold_greater': parse_int(data.get('count_threshold_greater')),
        'count_threshold_less': parse_int(data.get('count_threshold_less')),
        'alert_enabled': parse_boolean(data.get('alert_enabled')),
    }


# ============================================================================
# COMPARISON FUNCTIONS
# ============================================================================

def has_location_changes(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> bool:
    """
    Check if location data has changed.
    
    Args:
        old_data: Original location data
        new_data: Updated location data
    
    Returns:
        bool: True if location data has changed
    """
    location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']
    
    for field in location_fields:
        if old_data.get(field) != new_data.get(field):
            return True
    
    return False


def has_alert_changes(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> bool:
    """
    Check if alert settings have changed.
    
    Args:
        old_data: Original alert settings
        new_data: Updated alert settings
    
    Returns:
        bool: True if alert settings have changed
    """
    alert_fields = ['count_threshold_greater', 'count_threshold_less', 'alert_enabled']
    
    for field in alert_fields:
        if old_data.get(field) != new_data.get(field):
            return True
    
    return False


# ============================================================================
# FILTER FUNCTIONS
# ============================================================================

def filter_none_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove keys with None values from dictionary.
    
    Args:
        data: Dictionary to filter
    
    Returns:
        Dict[str, Any]: Filtered dictionary
    """
    return {k: v for k, v in data.items() if v is not None}


def filter_empty_strings(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove keys with empty string values from dictionary.
    
    Args:
        data: Dictionary to filter
    
    Returns:
        Dict[str, Any]: Filtered dictionary
    """
    return {k: v for k, v in data.items() if v != ''}


def merge_non_none(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge update dictionary into base, only updating non-None values.
    
    Args:
        base: Base dictionary
        updates: Updates to apply
    
    Returns:
        Dict[str, Any]: Merged dictionary
    """
    result = base.copy()
    for key, value in updates.items():
        if value is not None:
            result[key] = value
    return result