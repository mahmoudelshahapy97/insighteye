# utils.py
from fastapi import HTTPException, status
import json
from config import config
import logging
import os
import asyncio
from typing import List, Dict, Tuple, Union, Optional, Any
import cv2
import base64
import numpy as np
import pandas as pd
import xgboost as xgb
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, time as dt_time, timezone, date as dt_date # Added timezone, date
from zoneinfo import ZoneInfo
from uuid import UUID
import re # Added for validate_prompt

# Qdrant specific imports
from qdrant_client import QdrantClient, models as qdrant_models

logger = logging.getLogger(__name__)

_models: Dict[str, Any] = {} # Added type hint for _models

# --- QDRANT UTILITIES ---
BASE_QDRANT_COLLECTION_NAME = config.get("qdrant_collection_name", "person_counts")
_workspace_collection_init_cache: Dict[str, bool] = {} # Cache for workspace collection initialization status

def get_workspace_qdrant_collection_name(workspace_id: Union[str, UUID]) -> str:
    """
    Generates the Qdrant collection name for a given workspace.
    All data related to a workspace will be stored in this single collection.
    Example: person_counts_ws_xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx
    """
    return f"{BASE_QDRANT_COLLECTION_NAME}_ws_{str(workspace_id).replace('-', '_')}"

async def ensure_workspace_qdrant_collection_exists(client: QdrantClient, workspace_id: Union[str, UUID]):
    """
    Ensures that a Qdrant collection for the given workspace_id exists.
    Creates it if it doesn't, with appropriate payload indexes.
    Uses executor for synchronous Qdrant client calls.
    """
    collection_name = get_workspace_qdrant_collection_name(workspace_id)
    
    if collection_name in _workspace_collection_init_cache:
        return

    loop = asyncio.get_event_loop()
    try:
        try:
            await loop.run_in_executor(None, client.get_collection, collection_name)
            logger.debug(f"Qdrant collection '{collection_name}' already exists for workspace {str(workspace_id)}.")
            _workspace_collection_init_cache[collection_name] = True
            return
        except Exception as e:
            # Broader check for "not found" conditions from Qdrant client
            if "not found" in str(e).lower() or \
               (hasattr(e, 'status_code') and e.status_code == 404) or \
               "NOT_FOUND" in str(e).upper() or \
               (hasattr(e, 'message') and "doesn't exist" in str(getattr(e, 'message', '')).lower()): # More robust check
                logger.info(f"Qdrant collection '{collection_name}' not found for workspace {str(workspace_id)}. Will create.")
            else:
                logger.error(f"Error checking Qdrant collection '{collection_name}' for ws {str(workspace_id)}: {e}", exc_info=True)
                raise # Re-raise unexpected error during check

        # Vector size 1 is unusual but matches utils.py. Consider if this is intended.
        # A more typical size would be configured, e.g., config.get("qdrant_vector_size", 128)
        vector_size_for_collection = 1 # Matching utils.py;
        
        await loop.run_in_executor(
            None,
            client.create_collection,
            collection_name,
            qdrant_models.VectorParams(size=vector_size_for_collection, distance=qdrant_models.Distance.DOT)
        )
        logger.info(f"Created Qdrant collection: '{collection_name}' for workspace {str(workspace_id)}")

        payload_indexes_map = {
            "timestamp": qdrant_models.PayloadSchemaType.FLOAT,
            "camera_id": qdrant_models.PayloadSchemaType.KEYWORD,
            "username": qdrant_models.PayloadSchemaType.KEYWORD,
            "date": qdrant_models.PayloadSchemaType.KEYWORD,
        }
        for field, schema_type in payload_indexes_map.items():
            try:
                await loop.run_in_executor(
                    None,
                    client.create_payload_index,
                    collection_name,
                    field, 
                    schema_type 
                )
                logger.info(f"Created payload index for field '{field}' in collection '{collection_name}'.")
            except Exception as e_idx:
                if "already exists" in str(e_idx).lower() or "already present" in str(e_idx).lower():
                     logger.info(f"Payload index for field '{field}' already exists in '{collection_name}'.")
                else:
                    logger.warning(f"Could not create payload index for '{field}' in '{collection_name}': {e_idx}")
        
        _workspace_collection_init_cache[collection_name] = True
    except Exception as e:
        logger.error(f"Error ensuring Qdrant collection '{collection_name}' exists for workspace {str(workspace_id)}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to initialize Qdrant workspace collection: {collection_name}")

def parse_string_or_list_v0(value: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
    """
    Parse a parameter that can be either a string or list of strings.
    If it's a string containing commas, split it into a list.
    If it's already a list, return as-is.
    If it's None or empty, return None.
    
    Args:
        value: The input value (string, list of strings, or None)
        
    Returns:
        List of strings or None
        
    Examples:
        parse_string_or_list("area1,area2,area3") -> ["area1", "area2", "area3"]
        parse_string_or_list(["area1", "area2"]) -> ["area1", "area2"]
        parse_string_or_list("single_area") -> ["single_area"]
        parse_string_or_list(None) -> None
        parse_string_or_list("") -> None
        parse_string_or_list("area1, area2 , area3") -> ["area1", "area2", "area3"]
    """
    if value is None:
        return None
    
    if isinstance(value, list):
        # Filter out empty strings and strip whitespace
        filtered_list = [item.strip() for item in value if item and item.strip()]
        return filtered_list if filtered_list else None
    
    if isinstance(value, str):
        # Handle empty string
        if not value.strip():
            return None
        
        # Check if string contains commas (indicating multiple values)
        if ',' in value:
            # Split by comma and strip whitespace from each item
            items = [item.strip() for item in value.split(',') if item.strip()]
            return items if items else None
        else:
            # Single string value
            return [value.strip()]
    
    return None


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
        parse_string_or_list("area1, area2 , area3") -> ["area1", "area2", "area3"]
    """
    if value is None:
        return None
    
    if isinstance(value, list):
        # Process each item in the list, splitting by commas if needed
        all_items = []
        for item in value:
            if item and isinstance(item, str) and item.strip():
                if ',' in item:
                    # Split comma-separated values within list items
                    split_items = [sub_item.strip() for sub_item in item.split(',') if sub_item.strip()]
                    all_items.extend(split_items)
                else:
                    all_items.append(item.strip())
        
        return all_items if all_items else None
    
    if isinstance(value, str):
        # Handle empty string
        if not value.strip():
            return None
        
        # Check if string contains commas (indicating multiple values)
        if ',' in value:
            # Split by comma and strip whitespace from each item
            items = [item.strip() for item in value.split(',') if item.strip()]
            return items if items else None
        else:
            # Single string value
            return [value.strip()]
    
    return None


encoded_string = ""
try:
    image_path = os.path.join(os.path.dirname(__file__), "images", "base64_1.jpg")
    if os.path.exists(image_path):
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    else:
        logger.warning(f"Default image '{image_path}' not found. Static base64 image will be empty.")
except Exception as e:
    logger.error(f"Error loading default image: {e}", exc_info=True)


def handle_exceptions(func): 
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            # Log with func name for better debugging
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True) 
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                # Provide a more generic message to the client for unexpected errors
                detail=f"An unexpected error occurred."
            )
    return wrapper

def datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO format string."""
    return dt.isoformat()

def iso_to_datetime(iso_str: str) -> datetime:
    """Convert ISO format string to datetime with timezone handling."""
    # Ensure 'Z' is handled correctly for UTC timezone awareness
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))

def timestamp_to_iso(timestamp: Union[int, float]) -> str:
    """Convert Unix timestamp to ISO format string (UTC)."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

def iso_to_timestamp(iso_str: str) -> float:
    """Convert ISO format string to Unix timestamp (float for precision)."""
    return iso_to_datetime(iso_str).timestamp()

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
                else: # Parsed as JSON, but not a list. Fallback to splitting the content.
                    cleaned_str = camera_id_input.strip()[1:-1] # Remove brackets
                    camera_ids_list = [cam_id.strip() for cam_id in cleaned_str.split(',') if cam_id.strip()]
            except json.JSONDecodeError:
                # JSON parsing failed, assume comma-separated content within brackets
                cleaned_str = camera_id_input.strip()[1:-1] # Remove brackets
                camera_ids_list = [cam_id.strip() for cam_id in cleaned_str.split(',') if cam_id.strip()]
        else:
            # Not bracketed, assume simple comma-separated string
            camera_ids_list = [cam_id.strip() for cam_id in camera_id_input.split(',') if cam_id.strip()]
    elif isinstance(camera_id_input, (list, tuple)): # Explicitly handle tuple as well
        camera_ids_list = [str(item).strip() for item in camera_id_input if str(item).strip()]
    
    # Final cleaning: remove surrounding single/double quotes from each ID
    # and filter out any empty strings that might have resulted from parsing.
    return [cid.strip("'\"") for cid in camera_ids_list if cid.strip("'\"")]

def parse_date_format(date_str: str) -> dt_date: # Changed to dt_date for clarity
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

def parse_iso_date(date_str: str) -> dt_date: # Renamed back and return type changed
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

def paginate_list_get_page(data: List[Any], page: int, per_page: int) -> List[Any]:
    """Returns a single page of data from a list."""
    if page < 1: page = 1
    start = (page - 1) * per_page
    end = start + per_page
    return data[start:end]

def paginate_list_all_pages(data: List[Any], per_page: int) -> List[List[Any]]:
    """Splits data into multiple pages (lists of items). Matches utils.py paginate_list."""
    if per_page <= 0:
        return [data] if data else [[]] # Avoid division by zero, return all data in one page or empty
    return [data[i : i + per_page] for i in range(0, len(data), per_page)]

# --- Email Utilities (Async Wrappers for smtplib) ---
async def send_email(recipient_email: str, subject: str, body: str, html_body: Optional[str] = None):
    loop = asyncio.get_event_loop()
    # Run the synchronous _send_email_sync function in a separate thread
    return await loop.run_in_executor(None, _send_email_sync, recipient_email, subject, body, html_body)

def _send_email_sync(recipient_email: str, subject: str, body: str, html_body: Optional[str] = None) -> bool:
    """Synchronous helper for sending email. To be run in an executor."""
    app_sender_email = config.get("otp_sender_email", config.get("sender_email")) # Fallback for sender_email
    app_sender_password = config.get("smtp_password", config.get("sender_password")) # Fallback for sender_password
    smtp_server_host = config.get("smtp_server")
    smtp_server_port = int(config.get("smtp_port", 587)) # Ensure port is int, default 587

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP server, port, or credentials not fully configured for send_email.")
        return False

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = app_sender_email
    message["To"] = recipient_email
    
    # Ensure UTF-8 encoding for broader character support
    message.attach(MIMEText(body, "plain", _charset="utf-8"))
    if html_body:
        message.attach(MIMEText(html_body, "html", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        # Added timeout from config
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.get("smtp_timeout", 30)) as server:
            server.ehlo_or_helo_if_needed() # Improved compatibility
            if server.has_extn('STARTTLS'): # Check if STARTTLS is supported
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed() # Re-EHLO after STARTTLS
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, recipient_email, message.as_string())
        logger.info(f"Email sent successfully to {recipient_email} with subject '{subject}'")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email}. Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for email to {recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp: # Catch broader SMTP errors
        logger.error(f"SMTP Error sending email to {recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_sync for {recipient_email}: {e}", exc_info=True)
        return False

async def send_email_from_client_to_admin(
    admin_recipient_email: str, subject: str, body: str, reply_to_email: Optional[str] = None
):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _send_email_from_client_to_admin_sync, admin_recipient_email, subject, body, reply_to_email)

def _send_email_from_client_to_admin_sync(
    admin_recipient_email: str, subject: str, body: str, reply_to_email: Optional[str] = None
) -> bool:
    """Synchronous helper for sending admin notification email."""
    app_sender_email = config.get("otp_sender_email", config.get("sender_email"))
    app_sender_password = config.get("smtp_password", config.get("sender_password"))
    smtp_server_host = config.get("smtp_server")
    smtp_server_port = int(config.get("smtp_port", 587))

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP config incomplete for send_email_from_client_to_admin.")
        return False

    message = MIMEMultipart()
    # More descriptive From header
    message["From"] = f"InsightEye System <{app_sender_email}>" 
    message["To"] = admin_recipient_email
    message["Subject"] = subject
    if reply_to_email:
        message.add_header('Reply-To', reply_to_email)
    
    message.attach(MIMEText(body, "plain", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.get("smtp_timeout", 30)) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed()
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, admin_recipient_email, message.as_string())
        logger.info(f"Admin notification email sent to {admin_recipient_email} regarding '{subject}' from {reply_to_email or 'system'}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email} (admin notification). Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for admin notification email to {admin_recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp:
        logger.error(f"SMTP Error sending admin notification to {admin_recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_from_client_to_admin_sync for {admin_recipient_email}: {e}", exc_info=True)
        return False

async def send_fire_alert_email(user_email: str, camera_name: str, fire_status: str, 
                            location_info: Optional[Dict[str, Any]] = None) -> bool:
    """Send fire/smoke alert email to user"""
    try:
        logger.info(f"Preparing to send fire alert email to {user_email} for {fire_status} detected at {camera_name}")
        
        # Build location context
        location_text = ""
        if location_info:
            location_parts = []
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        # Create timestamp
        timestamp = datetime.now(ZoneInfo("Africa/Cairo")).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Email content
        alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
        subject = f"🚨 URGENT: {alert_type} DETECTED - {camera_name}"
        
        body = f"""URGENT ALERT: {alert_type} DETECTED

Camera: {camera_name}
Status: {fire_status.upper()}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system. 
Please verify the situation immediately and take appropriate action.

If this is a false alarm, please check your camera positioning and detection settings.

---
InsightEye Surveillance System
This alert will not be sent again for the next 10 minutes to prevent spam.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 URGENT ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type} DETECTED</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #dc3545; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Status:</strong> <span style="color: #dc3545; font-weight: bold;">{fire_status.upper()}</span></li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in (location_parts if location_info else [])) + '</ul>' if location_info and location_parts else ''}
        </div>
        
        <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #856404; margin-top: 0;">⚠️ Immediate Action Required</h4>
            <p style="margin-bottom: 0; color: #856404;">Please verify the situation immediately and take appropriate safety measures.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>If this is a false alarm, please check your camera positioning and detection settings.</p>
            <p><strong>Note:</strong> This alert will not be sent again for the next 10 minutes to prevent spam.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body)
        if success:
            logger.info(f"Fire alert email sent successfully to {user_email} for {fire_status} at {camera_name}")
        else:
            logger.error(f"Failed to send fire alert email to {user_email} for {fire_status} at {camera_name}")
        return success
        
    except Exception as e:
        logger.error(f"Error sending fire alert email to {user_email}: {e}", exc_info=True)
        return False

async def send_people_count_alert_email(user_email: str, camera_name: str, 
                                       person_count: int, threshold_settings: Dict[str, Any],
                                       location_info: Optional[Dict[str, Any]] = None) -> bool:
    """Send people count threshold alert email to user"""
    try:
        logger.info(f"Preparing to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        
        # Build location context
        location_text = ""
        if location_info:
            location_parts = []
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        # Create timestamp
        timestamp = datetime.now(ZoneInfo("Africa/Cairo")).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Determine alert type and message
        greater_than = threshold_settings.get("greater_than")
        less_than = threshold_settings.get("less_than")
        
        if greater_than is not None and person_count > greater_than:
            alert_type = "HIGH OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: >{greater_than})"
            severity_color = "#dc3545"  # Red
        elif less_than is not None and person_count < less_than:
            alert_type = "LOW OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: <{less_than})"
            severity_color = "#fd7e14"  # Orange
        else:
            alert_type = "OCCUPANCY"
            threshold_info = f"Detected: {person_count} people"
            severity_color = "#6f42c1"  # Purple
        
        # Email content
        subject = f"🚨 {alert_type} ALERT - {camera_name}"
        
        body = f"""{alert_type} ALERT

Camera: {camera_name}
{threshold_info}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system.
The people count has exceeded your configured threshold settings.

Please review the situation and adjust thresholds if necessary.

---
InsightEye Surveillance System
Configure your alert settings in the camera management section.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: {severity_color}; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 OCCUPANCY ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type}</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: {severity_color}; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Count:</strong> <span style="color: {severity_color}; font-weight: bold;">{person_count} people</span></li>
                <li style="padding: 5px 0;"><strong>Threshold:</strong> {f'>{greater_than}' if greater_than is not None and person_count > greater_than else f'<{less_than}' if less_than is not None and person_count < less_than else 'N/A'}</li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in (location_parts if location_info else [])) + '</ul>' if location_info and location_parts else ''}
        </div>
        
        <div style="background-color: #e7f3ff; border: 1px solid #b3d9ff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #0056b3; margin-top: 0;">📊 Occupancy Monitoring</h4>
            <p style="margin-bottom: 0; color: #0056b3;">This alert indicates that the people count has triggered your configured threshold. Please review the situation and adjust settings if needed.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>You can configure alert thresholds in the camera management section of your dashboard.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body)
        if success:
            logger.info(f"People count alert email sent successfully to {user_email} for {person_count} people at {camera_name}")
        else:
            logger.error(f"Failed to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        return success
        
    except Exception as e:
        logger.error(f"Error sending people count alert email to {user_email}: {e}", exc_info=True)
        return False
        
# --- Image Utilities (CPU-bound, remain synchronous) ---
def frame_to_base64(frame: np.ndarray) -> str:
    """Converts a OpenCV frame (numpy array) to a base64 encoded string."""
    if frame is None: 
        return ""
    # Added configurable JPEG quality
    success, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), config.get("jpeg_quality", 85)])
    if not success:
        logger.error("cv2.imencode failed during frame_to_base64 conversion.")
        return ""
    return base64.b64encode(buffer).decode('utf-8')

def base64_to_frame(base64_string: str) -> Optional[np.ndarray]:
  """Converts a base64 encoded string back to an OpenCV frame."""
  if not base64_string: 
      return None
  try:
    img_bytes = base64.b64decode(base64_string)
    nparr = np.frombuffer(img_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    return frame
  except Exception as e:
     # Log with full exception info for better diagnostics
     logger.error(f"Error decoding base64 string to frame: {e}", exc_info=True)
     return None

def make_prediction(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Makes predictions using an XGBoost model based on historical data.
    Returns predictions for next hour, day, and week.
    Falls back to average-based prediction if models aren't available or data is insufficient.
    """
    if not data or len(data) == 0:
        logging.info(f"No data available for camera {camera_id}. Returning zero predictions via fallback.")
        return use_fallback_prediction(pd.DataFrame()) # Pass empty DF to fallback
    
    # Extract metadata, assuming 'metadata' key holds the relevant fields
    records = []
    for item in data:
        if 'metadata' in item and isinstance(item['metadata'], dict):
            # Ensure essential keys are present before adding
            if "timestamp" in item["metadata"] and "person_count" in item["metadata"]:
                 records.append(item["metadata"])
            else:
                logging.warning(f"Skipping record for camera {camera_id} due to missing timestamp or person_count in metadata: {item}")
        else:
            logging.warning(f"Skipping record for camera {camera_id} due to missing or invalid metadata: {item}")

    if not records:
        logging.warning(f"No valid records with metadata found for camera {camera_id}. Using fallback.")
        return use_fallback_prediction(pd.DataFrame())
        
    df = pd.DataFrame(records)
    
    # Ensure 'person_count' and 'timestamp' columns exist after DataFrame creation
    if 'person_count' not in df.columns or 'timestamp' not in df.columns:
        logging.warning(f"DataFrame for camera {camera_id} missing 'person_count' or 'timestamp' columns. Using fallback.")
        return use_fallback_prediction(df if 'person_count' in df.columns and 'timestamp' in df.columns else pd.DataFrame())

    model_path_base = f"models/{camera_id}" # Corrected variable name
    hourly_model_path = f"{model_path_base}_hourly.model"
    daily_model_path = f"{model_path_base}_daily.model"
    weekly_model_path = f"{model_path_base}_weekly.model"

    import os
    if not all(os.path.exists(p) for p in [hourly_model_path, daily_model_path, weekly_model_path]):
        logging.info(f"XGBoost models not found for camera {camera_id}. Using fallback prediction method.")
        return use_fallback_prediction(df)

    try:
        hourly_model = xgb.Booster()
        hourly_model.load_model(hourly_model_path)
        daily_model = xgb.Booster()
        daily_model.load_model(daily_model_path)
        weekly_model = xgb.Booster()
        weekly_model.load_model(weekly_model_path)

        df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
        df["hour"] = df["datetime"].dt.hour
        df["day_of_week"] = df["datetime"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
        
        df = df.sort_values("timestamp")
        # Added fillna(0) for robustness if rolling window doesn't have enough periods
        df["rolling_mean_3h"] = df["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        df["rolling_mean_24h"] = df["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        
        if df.empty: # Should not happen if records were present, but as a safeguard
             logging.warning(f"DataFrame became empty after processing for camera {camera_id}. Using fallback.")
             return use_fallback_prediction(df)

        latest_data = df.iloc[-1]
        
        # Use .get() for robustness in case a feature column was unexpectedly dropped or missing
        features_np = np.array([
            latest_data.get("hour", 0), 
            latest_data.get("day_of_week", 0), 
            latest_data.get("is_weekend", 0),
            latest_data.get("rolling_mean_3h", 0), 
            latest_data.get("rolling_mean_24h", 0),
            latest_data.get("person_count", 0)
        ]).reshape(1, -1)
        
        dmatrix = xgb.DMatrix(features_np)
        
        pred_h = max(0, round(float(hourly_model.predict(dmatrix)[0])))
        pred_d = max(0, round(float(daily_model.predict(dmatrix)[0])))
        pred_w = max(0, round(float(weekly_model.predict(dmatrix)[0])))
        
        logging.info(f"Successfully used XGBoost models for camera {camera_id}")
        return {"next_hour": int(pred_h), "next_day": int(pred_d), "next_week": int(pred_w)} # Return int
    
    except Exception as e:
        logger.error(f"Error using XGBoost models for camera {camera_id}: {e}", exc_info=True)
        return use_fallback_prediction(df)

def use_fallback_prediction(df: pd.DataFrame) -> Dict[str, int]:
    """Fallback prediction method based on simple averages. Returns integers."""
    if df.empty or 'person_count' not in df.columns:
        logging.info("Fallback prediction: DataFrame is empty or missing 'person_count'. Returning zeros.")
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    
    # Ensure person_count is numeric, coercing errors to NaN, then fillna with 0
    df['person_count'] = pd.to_numeric(df['person_count'], errors='coerce').fillna(0)
    
    avg_count = df["person_count"].mean()
    if pd.isna(avg_count): avg_count = 0.0 # Should be handled by fillna(0) above

    if len(df) >= 4 and 'timestamp' in df.columns:
        df_sorted = df.sort_values("timestamp").reset_index(drop=True) # Ensure clean index
        half_point = len(df_sorted) // 2
        
        older_half = df_sorted.iloc[:half_point]
        newer_half = df_sorted.iloc[half_point:]

        older_avg = older_half["person_count"].mean() if not older_half.empty else avg_count
        newer_avg = newer_half["person_count"].mean() if not newer_half.empty else avg_count
        
        if pd.isna(newer_avg): newer_avg = avg_count
        if pd.isna(older_avg) or older_avg == 0: older_avg = avg_count if avg_count > 0 else 1.0 # Avoid division by zero
        
        trend_factor = newer_avg / older_avg if older_avg != 0 else 1.0
        trend_factor = max(0.8, min(trend_factor, 1.3)) # Cap trend factor
        
        recent_count = df_sorted.iloc[-1]["person_count"] if not df_sorted.empty else avg_count
        if pd.isna(recent_count): recent_count = avg_count

        return {
            "next_hour": round(max(0, recent_count * 1.05)),
            "next_day": round(max(0, newer_avg * trend_factor)),
            "next_week": round(max(0, avg_count * trend_factor))
        }
    else:
        # Not enough data for trend or timestamp missing
        logging.info("Fallback prediction: Not enough data for trend analysis or timestamp missing. Using simple averages.")
        return {
            "next_hour": round(max(0, avg_count * 1.1)),
            "next_day": round(max(0, avg_count * 1.05)),
            "next_week": round(max(0, avg_count))
        }

def make_prediction_default(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """ Placeholder function to simulate making a prediction. Matches utils.py. """
    if data and len(data) > 0:
        # Ensure metadata and person_count exist and are valid
        counts = [item["metadata"]["person_count"] 
                  for item in data 
                  if "metadata" in item and isinstance(item["metadata"], dict) and "person_count" in item["metadata"]
                  and isinstance(item["metadata"]["person_count"], (int, float))]
        if not counts:
            average_count = 100 # Default if no valid counts
        else:
            total_count = sum(counts)
            average_count = total_count / len(counts)
    else:
        average_count = 100 # Default if no data
    
    return {
        "next_hour": round(average_count * 1.2),
        "next_day": round(average_count * 1.1),
        "next_week": round(average_count * 1.05)
    }

def make_prediction_all_cameras(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Makes predictions using an XGBoost model based on historical data. Matches utils.py.
    For "all" camera_id, it combines data from all cameras.
    """
    if not data or len(data) == 0:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    
    records = []
    for item in data:
        if 'metadata' in item and isinstance(item['metadata'], dict):
            # Ensure essential keys are present
            if all(k in item['metadata'] for k in ["timestamp", "person_count", "camera_id"]):
                records.append({
                    "timestamp": item["metadata"]["timestamp"],
                    "person_count": item["metadata"]["person_count"],
                    "camera_id": item["metadata"]["camera_id"]
                })
    
    if not records:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}

    df = pd.DataFrame(records)
    df['person_count'] = pd.to_numeric(df['person_count'], errors='coerce').fillna(0)
    
    df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df = df.sort_values("timestamp")

    latest_data_source_df = df # Default to use the full df for individual camera logic path
    model_id_for_path = camera_id # Default model path based on camera_id

    if camera_id.lower() == "all":
        model_id_for_path = "all_cameras"
        # Group by timestamp and aggregate
        grouped_df = df.groupby("timestamp", as_index=False).agg(
            person_count=("person_count", "sum"),
            hour=("hour", "first"),
            day_of_week=("day_of_week", "first"),
            is_weekend=("is_weekend", "first")
            # datetime will be implicitly recalculated if needed, or use .first() if required by features
        )
        if grouped_df.empty:
            logging.warning("Grouped DataFrame for 'all_cameras' is empty. Using fallback averages.")
            avg_count = df["person_count"].mean() if not df.empty else 0
            return {
                "next_hour": round(avg_count * 1.2), "next_day": round(avg_count * 1.1), "next_week": round(avg_count * 1.05)
            }

        grouped_df["rolling_mean_3h"] = grouped_df["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        grouped_df["rolling_mean_24h"] = grouped_df["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        latest_data_source_df = grouped_df
    else:
        # For individual cameras, filter and calculate camera-specific rolling stats
        df_camera = df[df["camera_id"] == camera_id].copy() # Use .copy() to avoid SettingWithCopyWarning
        if df_camera.empty:
            return {"next_hour": 0, "next_day": 0, "next_week": 0}
        
        df_camera["rolling_mean_3h"] = df_camera["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        df_camera["rolling_mean_24h"] = df_camera["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        latest_data_source_df = df_camera

    # Load models
    model_path = f"models/{model_id_for_path}"
    import os
    try:
        if not (os.path.exists(f"{model_path}_hourly.model") and \
                os.path.exists(f"{model_path}_daily.model") and \
                os.path.exists(f"{model_path}_weekly.model")):
            raise FileNotFoundError("One or more model files not found.")
            
        hourly_model = xgb.Booster(); hourly_model.load_model(f"{model_path}_hourly.model")
        daily_model = xgb.Booster(); daily_model.load_model(f"{model_path}_daily.model")
        weekly_model = xgb.Booster(); weekly_model.load_model(f"{model_path}_weekly.model")
    except (FileNotFoundError, xgb.core.XGBoostError) as e: # xgb.core.XGBoostError for loading issues
        logging.error(f"Failed to load XGBoost models for '{model_id_for_path}': {e}")
        # Fallback for this specific camera_id or 'all'
        avg_val_df = latest_data_source_df if not latest_data_source_df.empty else df
        average_count = avg_val_df["person_count"].mean() if not avg_val_df.empty else 0
        return {
            "next_hour": round(average_count * 1.2),
            "next_day": round(average_count * 1.1),
            "next_week": round(average_count * 1.05)
        }
    
    if latest_data_source_df.empty:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    latest_data = latest_data_source_df.iloc[-1]
    
    features = np.array([
        latest_data.get("hour", 0), latest_data.get("day_of_week", 0), latest_data.get("is_weekend", 0),
        latest_data.get("rolling_mean_3h", 0), latest_data.get("rolling_mean_24h", 0),
        latest_data.get("person_count", 0)
    ]).reshape(1, -1)
    
    dmatrix = xgb.DMatrix(features)
    
    next_hour = max(0, round(float(hourly_model.predict(dmatrix)[0])))
    next_day = max(0, round(float(daily_model.predict(dmatrix)[0])))
    next_week = max(0, round(float(weekly_model.predict(dmatrix)[0])))
    
    return {"next_hour": int(next_hour), "next_day": int(next_day), "next_week": int(next_week)}


# --- LLM Utilities ---
from langchain_community.chat_models import ChatOpenAI # Already imported but good for section clarity
from langchain_ollama import ChatOllama # Already imported

def get_ChatOpenAI_model(model_id: str, base_url: str, temperature: float, num_predict: int, format_: str):
    """Get or create ChatOpenAI model instance."""
    # This key combines more parameters to differentiate models if needed
    cache_key = f"openai_{model_id}_{base_url}_{temperature}_{num_predict}_{format_}"
    if cache_key not in _models:
        _models[cache_key] = ChatOpenAI(
            base_url=base_url,
            model=model_id,
            api_key="na", # Assuming API key is set elsewhere or not needed for local/proxy
            max_tokens=num_predict,
            temperature=temperature,
            top_p=1, # As in utils.py
            frequency_penalty=1.1, # As in utils.py
            # model_kwargs={"format": format_} if format_ else {} # Example if API supports format directly
        )
    return _models[cache_key]

def get_ChatOllama_model(model_id: str, temperature: float, num_predict: int, format_: str):
    """Get or create ChatOllama model instance."""
    cache_key = f"ollama_{model_id}_{temperature}_{num_predict}_{format_}"
    if cache_key not in _models:
        _models[cache_key] = ChatOllama(
            model=model_id,
            temperature=temperature,
            num_predict=num_predict,
            format=format_ if format_ else None # Pass None if format_ is empty string
        )
    return _models[cache_key]

async def generate_chat_response(
    prompt: str,
    history: List[dict], # Assuming history is List[Dict[str, str]]
    context: str,
    system_prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.7,
    stream: bool = False,
    format_: str = "", # json, etc.
    image: Optional[str] = "", # Base64 encoded image string
):
    """Core logic for generating a chat response using Ollama models."""
    
    model_config = config['models']['chat'] # Main chat model config
    
    if not image:
        model_id = model_config.get('llama', 'llama3') # Default to llama3 if not specified
        # base_url = model_config.get('base_url') # Not used for ChatOllama directly here
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_)
        logger.info(f"Using Ollama model {model_id} for text chat.")

        messages = format_chat_history(history, system_prompt=system_prompt, context=context)
        # Role 'human' is often used by Langchain for user messages with Ollama/OpenAI
        messages.append({"role": "human", "content": prompt}) 
    else:
        model_id = model_config.get('llava', 'llava') # Default to llava if not specified
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_) # LLaVA may not use 'format'
        logger.info(f"Using Ollama model {model_id} for image chat.")

        messages = format_chat_history(history, system_prompt=system_prompt, context=context)
        
        content_parts = []
        text_part = {"type": "text", "text": prompt}
        # Ensure image is a non-empty string before creating image part
        if image: 
            image_part = {
                "type": "image_url",
                "image_url": f"data:image/jpeg;base64,{image}",
            }
            content_parts.append(image_part)
        content_parts.append(text_part)
        
        # Role 'user' is standard for multimodal messages with content parts
        messages.append({"role": "user", "content": content_parts})

    if stream:
        async def generate_response_stream(): # Renamed for clarity
            async for chunk in model.astream(messages):
                yield chunk.content
                # Minimal sleep to allow other tasks to run, might not be strictly necessary
                # depending on Langchain's astream implementation.
                await asyncio.sleep(0.001) 
        return generate_response_stream()

    response = await model.ainvoke(messages)
    return response.content

def format_chat_history(
    history: Union[str, List[Dict[str, str]]],
    system_prompt: str,
    context: Optional[str] = None
) -> List[Dict[str, str]]:
    """Format chat history with system prompt and context."""
    if isinstance(history, str):
        try:
            history = json.loads(history)
            if not isinstance(history, list): # Ensure it's a list after loading
                logger.warning(f"Loaded history from JSON string is not a list: {history}. Treating as empty.")
                history = []
        except json.JSONDecodeError:
            logger.error(f"Failed to parse history string as JSON: {history}. Treating as empty.")
            history = []
    
    messages: List[Dict[str, str]] = []
    
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    if context:
        # Prepending "Context: " as in utils.py, ensures it's clear
        messages.append({"role": "system", "content": f"Context: {context}"}) 
    
    valid_history_messages = []
    if isinstance(history, list): # Check if history is a list before iterating
        for msg in history:
            if isinstance(msg, dict) and "role" in msg and "content" in msg:
                valid_history_messages.append(msg)
            else:
                logger.warning(f"Skipping invalid history message (must be dict with 'role' and 'content'): {msg}")
    messages.extend(valid_history_messages)
    return messages

def get_user_message(messages: List[Dict[str, str]]) -> List[str]: # Return List[str]
    """Get user messages (human or user role) from messages list."""
    # Content can be complex (list of parts for multimodal), so convert to str
    return [str(msg['content']) for msg in messages if msg.get('role') in ['human', 'user']]

def get_assistant_message(messages: List[Dict[str, str]]) -> List[str]: # Return List[str]
    """Get assistant messages from messages list."""
    return [str(msg['content']) for msg in messages if msg.get('role') == 'assistant']

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
    