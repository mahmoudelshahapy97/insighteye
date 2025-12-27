# app/utils/parser_utils.py
"""Utilities for parsing and validating input parameters."""

import json
import logging
import re
from typing import Optional, Union, List
from datetime import datetime, time as dt_time, date as dt_date

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


def parse_string_or_list(value: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
    """
    Parse a parameter that can be either a string or list of strings.
    If it's a string containing commas, split it into a list.
    If it's a list, check each item for commas and split if needed.
    If it's None or empty, return None.
    
    Args:
        value: The input value (string, list of strings, or None)
        
    Returns:
        List of strings or None
        
    Examples:
        parse_string_or_list("area1,area2,area3") -> ["area1", "area2", "area3"]
        parse_string_or_list(["area1", "area2"]) -> ["area1", "area2"]
        parse_string_or_list(["area1,area2", "area3"]) -> ["area1", "area2", "area3"]
        parse_string_or_list("single_area") -> ["single_area"]
        parse_string_or_list(None) -> None
        parse_string_or_list("") -> None
    """
    if value is None:
        return None
    
    if isinstance(value, list):
        all_items = []
        for item in value:
            if item and isinstance(item, str) and item.strip():
                if ',' in item:
                    split_items = [sub_item.strip() for sub_item in item.split(',') if sub_item.strip()]
                    all_items.extend(split_items)
                else:
                    all_items.append(item.strip())
        
        return all_items if all_items else None
    
    if isinstance(value, str):
        if not value.strip():
            return None
        
        if ',' in value:
            items = [item.strip() for item in value.split(',') if item.strip()]
            return items if items else None
        else:
            return [value.strip()]
    
    return None


def parse_camera_ids(camera_id_input: Optional[Union[str, List[str]]]) -> List[str]:
    """
    Parse and validate camera_id from query parameters.
    Returns a list of cleaned camera IDs.
    Handles direct comma-separated strings, JSON-like strings, and lists.
    """
    if not camera_id_input:
        return []
    
    camera_ids_list: List[str] = []
    
    if isinstance(camera_id_input, str):
        # Attempt to parse as JSON array if it looks like one
        if camera_id_input.strip().startswith('[') and camera_id_input.strip().endswith(']'):
            try:
                parsed_json = json.loads(camera_id_input)
                if isinstance(parsed_json, list):
                    camera_ids_list = [str(item).strip() for item in parsed_json if str(item).strip()]
                else:
                    cleaned_str = camera_id_input.strip()[1:-1]
                    camera_ids_list = [cam_id.strip() for cam_id in cleaned_str.split(',') if cam_id.strip()]
            except json.JSONDecodeError:
                cleaned_str = camera_id_input.strip()[1:-1]
                camera_ids_list = [cam_id.strip() for cam_id in cleaned_str.split(',') if cam_id.strip()]
        else:
            camera_ids_list = [cam_id.strip() for cam_id in camera_id_input.split(',') if cam_id.strip()]
    elif isinstance(camera_id_input, (list, tuple)):
        camera_ids_list = [str(item).strip() for item in camera_id_input if str(item).strip()]
    
    return [cid.strip("'\"") for cid in camera_ids_list if cid.strip("'\"")]


def parse_date_format(date_str: str) -> dt_date: 
    """
    Try multiple date formats to parse the input string.
    Returns a datetime.date object if successful, raises ValueError if all formats fail.
    """
    formats = [
        "%Y-%m-%d",    # Standard ISO format: 2025-02-23
        "%d-%m-%Y",    # European format: 23-02-2025
        "%m-%d-%Y",    # US format: 02-23-2025
        "%d/%m/%Y",    # European with slashes: 23/02/2025
        "%m/%d/%Y",    # US with slashes: 02/23/2025
        "%Y/%m/%d"     # ISO with slashes: 2025/02/23
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # Handle year only (assume January 1st)
    if len(date_str) == 4 and date_str.isdigit():
        try: 
            return dt_date(int(date_str), 1, 1) # Use dt_date constructor
        except ValueError: 
            pass # Invalid year, will fall through to raise error
        
    raise ValueError(f"Could not parse date string: '{date_str}' with known formats.")


def parse_iso_date(date_str: str) -> dt_date:
    """Ensures the date is in YYYY-MM-DD format before parsing. Returns datetime.date."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.")


def parse_time_string(time_str: Optional[str], default_time: dt_time) -> dt_time:
    """Parses HH:MM or HH:MM:SS format, returns default on failure or None input."""
    if time_str is None:
        return default_time
    try:
        parts_str = time_str.split(':')
        parts = [int(p) for p in parts_str]
        
        hour = parts[0]
        minute = parts[1] if len(parts) > 1 else 0
        second = parts[2] if len(parts) > 2 else 0
        
        if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
            raise ValueError("Time component out of range")
        return dt_time(hour, minute, second)
    except (ValueError, IndexError) as e:
        logger.warning(f"Could not parse time string '{time_str}': {e}. Using default: {default_time.isoformat()}")
        return default_time


def validate_prompt(prompt: str) -> str:
    """Clean and validate prompt to match utils.py (using re)."""
    if not prompt:
        return ""
    # Using re.sub as in utils.py for consistency
    prompt = re.sub(r'[^\w\s,.!?-]', '', prompt)
    return prompt.strip()

def validate_text(text: str) -> str:
    """Validate and clean text (e.g., for TTS), ensuring it's printable."""
    if not text:
        raise ValueError("Text cannot be empty") # Match utils.py
    
    # Remove non-printable characters
    text = ''.join(char for char in text if char.isprintable())
    return text.strip()
    