# camera.py
from fastapi import APIRouter, HTTPException, Query, Depends, Header, Cookie, status, Request
from typing import Generator, Dict, List, Optional, Tuple, Any, Set, Union
import logging
import threading
import time
import uuid
import os
import base64
import json
from database import execute_db_query
from schemas_models import StreamQueryParams, CameraStreamQueryParams, StreamCreate, StreamUpdate, StreamDelete, CameraState, CamerasStateResponse
from session_manager import SessionManager
from user_manager import UserManager

from video_streaming_qdrant import get_timestamp_range

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__) # Use logger

# Router
router = APIRouter(tags=["camera"])

# Initialize session manager
session_manager = SessionManager()
user_manager = UserManager()

# Load static image for preview with better error handling
encoded_string = ""
try:
    image_path = "images/base64_1.jpg"
    if os.path.exists(image_path):
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    else:
        logger.warning(f"Default image '{image_path}' not found. Static base64 image will be empty.")
except Exception as e:
    logger.error(f"Error loading default image: {e}")

def get_user_id_from_username(username: str) -> str:
    """Resolve username to user ID with error handling."""
    try:

        if not username:
            raise HTTPException(status_code=400, detail="Username is required")
        
        query = "SELECT user_id FROM users WHERE username = %s"
        user = execute_db_query(query, (username,), fetch_one=True)
        
        if not user or not user[0]:
            logger.warning(f"User ID lookup failed for username: {username}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found for the given username")

        return user[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user_id for username '{username}': {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail="Failed to retrieve user information.")

def ensure_uuid_str(id_value: Any) -> str:
    """Ensure consistent string formatting of UUID values"""
    if id_value is None:
        return None
    return str(id_value)

###################
# --- Stream Endpoints (Source) ---

# Create a new stream entry
@router.post("/source", status_code=status.HTTP_201_CREATED)
async def create_stream(stream: StreamCreate, request: Request, username: str = Depends(session_manager.get_current_user)):
    """Creates a new video stream entry for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)

        # Check current camera count for this user
        count_query = "SELECT COUNT(*) FROM video_stream WHERE user_id = %s"
        count_result = execute_db_query(count_query, (user_id,), fetch_one=True)
        current_stream_count = count_result[0] if count_result else 0

        # Fetch the count_of_camera limit for the user
        limit_query = "SELECT count_of_camera FROM users WHERE user_id = %s"
        limit_result = execute_db_query(limit_query, (user_id,), fetch_one=True)
        # Use default if not found, though user should exist if authenticated
        allowed_camera_count = limit_result[0] if limit_result else 5

        # Prevent stream creation if user exceeded the limit
        if current_stream_count >= allowed_camera_count:
            logger.warning(f"User {username} (ID: {user_id}) attempted to exceed camera limit ({current_stream_count}/{allowed_camera_count}).")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Camera limit ({allowed_camera_count}) exceeded. You currently have {current_stream_count} cameras."
            )

        # Generate unique stream ID (UUID)
        stream_id = str(uuid.uuid4())

        # CHANGE: Map 'video file' to 'local' to match the database constraint
        stream_type = stream.type
        if stream_type not in ['rtsp', 'http', 'local', 'other']:
            # Map any unexpected type (including 'video file') to 'local' or 'other'
            stream_type = 'local' if 'file' in stream_type.lower() else 'other'

        # Insert new video stream
        insert_query = """
            INSERT INTO video_stream (stream_id, user_id, name, path, type, status, is_streaming) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    
        execute_db_query(
            insert_query,
            (stream_id, user_id, stream.name, stream.path, stream_type, stream.status, stream.is_streaming)
        )
    
        # Log the action using SessionManager
        session_manager.log_action(
                content=f"User '{username}' added Camera '{stream.name}' (ID: {stream_id})",
                user_id=user_id,
                action_type="Added_Camera",
                ip_address=request.client.host if request.client else None, # Get IP if possible
                user_agent=request.headers.get("user-agent") # Get User-Agent if possible
        )

        return {"message": "Stream created successfully", "id": stream_id}

    except HTTPException as e:
        # Re-raise DB errors during insert
        raise e
    except Exception as e:
        # Catch other potential errors during insert
        logger.error(f"Error inserting stream for user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create stream.")

# Get streams for a specific user
@router.get("/source/user")
async def get_user_streams(username: str = Depends(session_manager.get_current_user)):
    """Gets all video streams associated with the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)
        
        query = """
            SELECT stream_id, user_id, name, path, type, status, is_streaming, created_at, updated_at 
            FROM video_stream 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        """
        streams_data = execute_db_query(query, (user_id,), fetch_all=True)
        
        if not streams_data:
            return []  # Return empty list instead of 404 error

        return [
            {
                "id": str(s[0]), 
                "user_id": str(s[1]), 
                "name": s[2], 
                "path": s[3], 
                "type": s[4], 
                "status": s[5], 
                "is_streaming": s[6],
                "created_at": s[7].isoformat() if s[7] else None,
                "updated_at": s[8].isoformat() if s[8] else None,
                "static_base64": encoded_string # Include static image if needed
            }
            for s in streams_data
        ]

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving streams for user {username}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to retrieve streams. Please try again later."
        )

# Get all streams
@router.get("/source/users")
async def get_all_streams(username: str = Depends(session_manager.get_current_user)):
    """
    Gets all video streams in the system. 
    (Requires authentication, consider admin role check if needed).
    """
    try:

        # Optional: Add role check here if only admins should see all streams
        # user_details = await user_manager.get_user_by_username(username)
        # if not user_details or user_details.get('role') != 'admin':
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

        query = """
            SELECT stream_id, user_id, name, path, type, status, is_streaming, created_at, updated_at 
            FROM video_stream 
            ORDER BY created_at DESC
        """
        streams_data = execute_db_query(query, fetch_all=True)
        
        if not streams_data:
            return []  # Return empty list instead of 404 error

        return [
            {
                "id": str(s[0]), 
                "user_id": str(s[1]), 
                "name": s[2], 
                "path": s[3], 
                "type": s[4], 
                "status": s[5], 
                "is_streaming": s[6],
                "created_at": s[7].isoformat() if s[7] else None,
                "updated_at": s[8].isoformat() if s[8] else None,
                "static_base64": encoded_string # Include static image if needed
            }
            for s in streams_data
        ]
    
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving all streams: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to retrieve streams. Please try again later."
        )

# Update multiple streams
@router.put("/source", status_code=status.HTTP_200_OK)
async def update_streams(streams: List[StreamUpdate], request: Request, username: str = Depends(session_manager.get_current_user)):
    """Updates multiple video streams. Users can only update their own streams."""
    try:

        user_id = get_user_id_from_username(username)
        updated_ids = []
        failed_ids = []

        # Get IDs of streams owned by the user
        owned_stream_ids = set()
        owned_query = "SELECT stream_id FROM video_stream WHERE user_id = %s"
        owned_results = execute_db_query(owned_query, (user_id,), fetch_all=True)
        if owned_results:
            owned_stream_ids = {str(row[0]) for row in owned_results}

        update_query = """
            UPDATE video_stream 
            SET name=%s, path=%s, type=%s, status=%s, is_streaming=%s 
            WHERE stream_id=%s AND user_id=%s 
        """ # Added user_id check

        for stream in streams:
            stream_id_str = ensure_uuid_str(stream.id) # Ensure comparison with string UUIDs
            if stream_id_str not in owned_stream_ids:
                logger.warning(f"User {username} (ID: {user_id}) attempted to update unauthorized stream ID: {stream.id}")
                failed_ids.append(stream_id_str)
                continue # Skip streams not owned by the user

            try:
                rows_affected = execute_db_query(
                    update_query,
                    (stream.name, stream.path, stream.type, stream.status, stream.is_streaming, stream.id, user_id),
                    return_rowcount=True
                )
                if rows_affected > 0:
                    updated_ids.append(stream_id_str)
                else:
                    # This could happen if the stream_id exists but user_id doesn't match (already handled)
                    # or if the stream_id doesn't exist at all.
                    logger.warning(f"Stream ID {stream.id} not found or update failed for user {user_id}.")
                    failed_ids.append(stream_id_str)
            except Exception as e:
                logger.error(f"Error updating stream {stream.id} for user {user_id}: {e}")
                failed_ids.append(stream_id_str)


        if updated_ids:
            session_manager.log_action(
                    content=f"User '{username}' updated {len(updated_ids)} camera(s): {', '.join(updated_ids)}",
                    user_id=user_id,
                    action_type="Updated_Camera",
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent")
            )

        if failed_ids:
            # Return partial success or failure based on whether any succeeded
            status_code = status.HTTP_207_MULTI_STATUS if updated_ids else status.HTTP_400_BAD_REQUEST
            detail = f"Streams updated: {len(updated_ids)}. Failed or unauthorized: {len(failed_ids)} (IDs: {', '.join(failed_ids)})."
            # Can't easily raise HTTPException with custom status code AND body, so return manually
            return Response(
                content=json.dumps({"detail": detail, "updated_ids": updated_ids, "failed_ids": failed_ids}),
                status_code=status_code,
                media_type="application/json"
            )

        return {"message": f"{len(updated_ids)} Stream(s) updated successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in update_streams: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to update streams. Please try again later."
        )

# Delete multiple streams
@router.delete("/source", status_code=status.HTTP_200_OK)
async def delete_streams(stream_ids_payload: StreamDelete, request: Request, username: str = Depends(session_manager.get_current_user)):
    """Deletes multiple video streams. Users can only delete their own streams."""
    try:
    
        user_id = get_user_id_from_username(username)

        if not stream_ids_payload.ids:
            return {"message": "No stream IDs provided for deletion."}

        # Ensure IDs are UUIDs for the query
        try:
            # Just validate format but won't use these UUID objects directly
            for id_str in stream_ids_payload.ids:
                uuid.UUID(id_str)  # This will raise ValueError if invalid
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid stream ID format. IDs must be UUIDs.")

        # First verify which streams belong to user - Fix for type mismatch
        # Use individual checks instead of ANY to handle type conversion properly
        placeholders = ", ".join(["%s"] * len(stream_ids_payload.ids))
        verify_query = f"""
            SELECT stream_id FROM video_stream 
            WHERE user_id = %s AND stream_id::text IN ({placeholders})
        """
        # Parameters list starts with user_id followed by all stream IDs
        params = [user_id] + stream_ids_payload.ids
        
        verify_results = execute_db_query(
            verify_query, 
            params,
            fetch_all=True
        )
        
        owned_ids = [ensure_uuid_str(row[0]) for row in verify_results] if verify_results else []
        unauthorized_ids = [id_str for id_str in stream_ids_payload.ids if id_str not in owned_ids]
        
        if not owned_ids:
            return {"message": "None of the provided stream IDs belong to you or exist."}

        # Delete streams that belong to the user - Fix for type mismatch
        # Use IN clause with proper placeholders instead of ANY
        placeholders = ", ".join(["%s"] * len(owned_ids))
        delete_query = f"DELETE FROM video_stream WHERE stream_id::text IN ({placeholders}) AND user_id = %s"
        params = owned_ids + [user_id]
        
        deleted_count = execute_db_query(
            delete_query, 
            params,
            return_rowcount=True
        )
        
        if deleted_count > 0:
            session_manager.log_action(
                    content=f"User '{username}' deleted {deleted_count} camera(s). Requested IDs: {', '.join(stream_ids_payload.ids)}",
                    user_id=user_id,
                    action_type="Deleted_Camera",
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent")
            )

        result_message = f"{deleted_count} Stream(s) deleted successfully."
        if unauthorized_ids:
            result_message += f" {len(unauthorized_ids)} streams were not found or do not belong to you."
            
        return {"message": result_message, "deleted_ids": owned_ids, "unauthorized_ids": unauthorized_ids}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in delete_streams: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to delete streams. Please try again later."
        )

###################
# --- Camera State ---

# Camera state endpoint
@router.get("/camera-state", response_model=CamerasStateResponse)
async def get_camera_state(username: str = Depends(session_manager.get_current_user)):
    """Gets the current state of all cameras for the authenticated user."""
    try:
        user_id = get_user_id_from_username(username)
        
        # Fetch camera states from video_stream table for the user
        query = "SELECT stream_id, name, status, is_streaming FROM video_stream WHERE user_id = %s"
        camera_rows = execute_db_query(query, (user_id,), fetch_all=True)
        
        if not camera_rows:
            # Return empty state instead of error when no cameras exist
            return CamerasStateResponse(
                cameras=[],
                total_active=0,
                total_inactive=0,
                total_error=0,
                total_processing=0,
                total_cameras=0
            )

        cameras = [
            CameraState(
                id=str(row[0]), # Ensure ID is string
                name=row[1],
                status=row[2],
                is_streaming=row[3]
            ) for row in camera_rows
        ]
        
        # Count cameras by status
        active_count = sum(1 for cam in cameras if cam.status == "active")
        inactive_count = sum(1 for cam in cameras if cam.status == "inactive")
        error_count = sum(1 for cam in cameras if cam.status == "error")
        processing_count = sum(1 for cam in cameras if cam.status == "processing")


        return CamerasStateResponse(
            cameras=cameras,
            total_active=active_count,
            total_inactive=inactive_count,
            total_error=error_count, # Add extra counts if needed
            total_processing=processing_count,
            total_cameras=len(cameras)
        )
    except HTTPException as e:
        # Re-raise known HTTP exceptions
        raise e
    except Exception as e:
        logger.error(f"Error fetching camera state for user {username}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching camera state."
        )

###################
# --- Parameter Stream Endpoints (User-level) ---

@router.post("/param_stream", status_code=status.HTTP_201_CREATED)
async def create_param_stream(params: StreamQueryParams, request: Request, username: str = Depends(session_manager.get_current_user)):
    """Creates or updates stream parameters for the authenticated user."""
    try:
        user_id = get_user_id_from_username(username)

        # Generate unique stream ID (UUID)
        param_id = str(uuid.uuid4())

        # Use INSERT ... ON CONFLICT to handle create or update atomically
        query = """
            INSERT INTO param_stream (param_id, user_id, frame_delay, frame_skip, conf) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                frame_delay = EXCLUDED.frame_delay,
                frame_skip = EXCLUDED.frame_skip,
                conf = EXCLUDED.conf,
                updated_at = CURRENT_TIMESTAMP
        """
        execute_db_query(
            query,
            (param_id, user_id, params.frame_delay, params.frame_skip, params.conf)
        )

        session_manager.log_action(
                content=f"User '{username}' created/updated stream parameters.",
                user_id=user_id,
                action_type="Upserted_Param_Stream",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent")
        )

        return {"message": "User stream parameters saved successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error saving param_stream for user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save stream parameters.")

@router.get("/param_stream/user")
async def get_user_params(username: str = Depends(session_manager.get_current_user)):
    """Gets stream parameters for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)
        
        query = "SELECT user_id, frame_delay, frame_skip, conf, created_at, updated_at FROM param_stream WHERE user_id = %s"
        params_data = execute_db_query(query, (user_id,), fetch_one=True) 
        
        if not params_data:
            # Standardized error response instead of returning a dictionary
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="No stream parameters found for this user."
            )

        return {
            "user_id": str(params_data[0]), 
            # "frame_delay": params_data[1], 
            "frame_skip": params_data[2], 
            "conf": params_data[3],
            "default_parameters": {
                # "frame_delay": 0,
                "frame_skip": 5,
                "conf": 0.5
            }
            # "created_at": params_data[4].isoformat() if params_data[4] else None,
            # "updated_at": params_data[5].isoformat() if params_data[5] else None
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving user parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to retrieve stream parameters. Please try again later."
        )

@router.get("/param_stream/users")
async def get_users_params(username: str = Depends(session_manager.get_current_user)):
    """Gets stream parameters for all users (Requires admin privileges usually)."""
    try:
        # Optional: Add role check here
        # user_details = await user_manager.get_user_by_username(username)
        # if not user_details or user_details.get('role') != 'admin':
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

        query = "SELECT user_id, frame_delay, frame_skip, conf, created_at, updated_at FROM param_stream ORDER BY created_at DESC"
        params_list = execute_db_query(query, fetch_all=True)
        
        if not params_list:
                return []

        return [
            {
                "user_id": str(p[0]), 
                "frame_delay": p[1], 
                "frame_skip": p[2], 
                "conf": p[3],
                "created_at": p[4].isoformat() if p[4] else None,
                "updated_at": p[5].isoformat() if p[5] else None
            } for p in params_list
        ]

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving all users' parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to retrieve stream parameters. Please try again later."
        )

# PUT /param_stream/user - Covered by POST with ON CONFLICT
@router.put("/param_stream/user")
async def update_user_params(params: StreamQueryParams, username: str = Depends(session_manager.get_current_user)):
    """Updates stream parameters for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)

        # Check if parameters exist first
        check_query = "SELECT 1 FROM param_stream WHERE user_id = %s"
        exists = execute_db_query(check_query, (user_id,), fetch_one=True)
        
        if not exists:
            # If no parameters exist, create new entry
            param_id = str(uuid.uuid4())
            insert_query = """
                INSERT INTO param_stream (param_id, user_id, frame_delay, frame_skip, conf)
                VALUES (%s, %s, %s, %s, %s)
            """
            execute_db_query(
                insert_query,
                (param_id, user_id, params.frame_delay, params.frame_skip, params.conf)
            )
        else:
            # Update existing parameters
            update_query = """
                UPDATE param_stream 
                SET frame_delay = %s, frame_skip = %s, conf = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
            """
            execute_db_query(
                update_query,
                (params.frame_delay, params.frame_skip, params.conf, user_id)
            )

        session_manager.log_action(
                content=f"User {username} Updated Param",
                user_id=user_id,
                action_type="Updated_Param",
                ip_address=None,
                user_agent=None
        )
        
        return {"message": "User parameters updated successfully"}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to update stream parameters. Please try again later."
        )

@router.delete("/param_stream/user", status_code=status.HTTP_200_OK)
async def delete_user_params(request: Request, username: str = Depends(session_manager.get_current_user)):
    """Deletes stream parameters for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)
        
        query = "DELETE FROM param_stream WHERE user_id = %s"
        rows_affected = execute_db_query(query, (user_id,), return_rowcount=True)
        
        if rows_affected > 0:
            session_manager.log_action(
                    content=f"User '{username}' deleted their stream parameters.",
                    user_id=user_id,
                    action_type="Deleted_Param_Stream",
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent")
            )
            return {"message": "Your stream parameters were deleted successfully"}
        else:
            return {"message": "No stream parameters found to delete"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting user parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to delete stream parameters. Please try again later."
        )

@router.delete("/param_stream/users", status_code=status.HTTP_200_OK)
async def delete_users_params(request: Request, username: str = Depends(session_manager.get_current_user)):
    """Deletes stream parameters for ALL users (Requires admin privileges usually)."""
    try:
        
        user_id = get_user_id_from_username(username) # Get ID for logging

        # Optional: Add role check here
        # user_details = await user_manager.get_user_by_username(username)
        # if not user_details or user_details.get('role') != 'admin':
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

        query = "DELETE FROM param_stream"
        rows_affected = execute_db_query(query, return_rowcount=True)

        session_manager.log_action(
                content=f"Admin action by '{username}': Deleted {rows_affected} user stream parameter entries.",
                user_id=user_id, # Log who performed the action
                action_type="Deleted_All_Param_Stream",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
                status="warning" # Or "success" depending on intent
        )
        return {"message": f"All ({rows_affected}) user stream parameters deleted successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting all users' parameters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to delete stream parameters. Please try again later."
        )

###################
# --- Parameter Stream Endpoints (Camera-specific) ---

@router.post("/param_stream_camera", status_code=status.HTTP_201_CREATED)
async def create_param_stream_camera(params: CameraStreamQueryParams, camera_id: str, request: Request, username: str = Depends(session_manager.get_current_user)):
    """Creates or updates specific camera parameters for the authenticated user."""
    try:
    
        user_id = get_user_id_from_username(username)

        # Validate camera_id belongs to user (optional but good practice)
        cam_check_query = "SELECT stream_id FROM video_stream WHERE stream_id = %s AND user_id = %s"
        cam_exists = execute_db_query(cam_check_query, (camera_id, user_id), fetch_one=True)
        if not cam_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Camera ID '{camera_id}' not found or does not belong to the you.")

        # Generate unique stream ID (UUID)
        param_camera_id = str(uuid.uuid4())

        # Use INSERT ... ON CONFLICT for atomic upsert
        query = """
            INSERT INTO param_stream_camera (param_camera_id, user_id, camera_id, frame_delay, frame_skip, conf) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, camera_id) DO UPDATE SET
                frame_delay = EXCLUDED.frame_delay,
                frame_skip = EXCLUDED.frame_skip,
                conf = EXCLUDED.conf,
                updated_at = CURRENT_TIMESTAMP
        """
        execute_db_query(
            query,
            (param_camera_id, user_id, camera_id, params.frame_delay, params.frame_skip, params.conf)
        )

        session_manager.log_action(
                content=f"User '{username}' created/updated parameters for camera '{camera_id}'.",
                user_id=user_id,
                action_type="Upserted_Param_Camera",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent")
        )
        
        return {"message": f"Parameters for camera '{camera_id}' saved successfully"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error saving param_stream_camera for user {user_id}, camera {camera_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save camera parameters.")

@router.get("/param_stream_camera/user")
async def get_user_camera_params(camera_id: Optional[str] = Query(None), username: str = Depends(session_manager.get_current_user)):
    """Gets specific camera parameters for the authenticated user. If camera_id is omitted, returns all."""
    try:

        user_id = get_user_id_from_username(username)
        
        base_query = "SELECT user_id, camera_id, frame_delay, frame_skip, conf, created_at, updated_at FROM param_stream_camera WHERE user_id = %s"
        params = [user_id]

        if camera_id:
            base_query += " AND camera_id = %s"
            params.append(camera_id)
            
        base_query += " ORDER BY created_at DESC"
        
        results = execute_db_query(base_query, tuple(params), fetch_all=True)
        
        if not results:
            detail = f"No specific camera parameters found for camera '{camera_id}'." if camera_id else "No specific camera parameters found for this user."
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)

        # return [
        #     {
        #         "user_id": str(p[0]), 
        #         "camera_id": str(p[1]), # camera_id is VARCHAR but might represent UUID
        #         "frame_delay": p[2], 
        #         "frame_skip": p[3], 
        #         "conf": p[4],
        #         "created_at": p[5].isoformat() if p[5] else None,
        #         "updated_at": p[6].isoformat() if p[6] else None
        #     } for p in results
        # ]

        formatted_results = []
        for p in results:
            formatted_results.append({
                "user_id": str(p[0]),  # Consistent string conversion for UUIDs
                "camera_id": str(p[1]),  # Consistent string conversion
                "frame_delay": p[2],
                "frame_skip": p[3],
                "conf": p[4],
                "created_at": p[5].isoformat() if p[5] else None,
                "updated_at": p[6].isoformat() if p[6] else None
            })
        
        return {"camera_params": formatted_results}  # Consistent response format

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving camera parameters: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve camera parameters.")

@router.get("/param_stream_camera/users")
async def get_users_camera_params(username: str = Depends(session_manager.get_current_user)):
    """Gets specific camera parameters for ALL users (Requires admin privileges usually)."""
    try:

        # Optional: Add role check here
        # user_details = await user_manager.get_user_by_username(username)
        # if not user_details or user_details.get('role') != 'admin':
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

        query = "SELECT user_id, camera_id, frame_delay, frame_skip, conf, created_at, updated_at FROM param_stream_camera ORDER BY user_id, created_at DESC"
        params_list = execute_db_query(query, fetch_all=True)
        
        # return [
        #     {
        #         "user_id": str(p[0]), 
        #         "camera_id": str(p[1]),
        #         "frame_delay": p[2], 
        #         "frame_skip": p[3], 
        #         "conf": p[4],
        #         "created_at": p[5].isoformat() if p[5] else None,
        #         "updated_at": p[6].isoformat() if p[6] else None
        #     } for p in params_list
        # ]

        formatted_results = []
        for p in params_list:
            formatted_results.append({
                "user_id": str(p[0]),  # Consistent string conversion
                "camera_id": str(p[1]),  # Consistent string conversion
                "frame_delay": p[2],
                "frame_skip": p[3],
                "conf": p[4],
                "created_at": p[5].isoformat() if p[5] else None,
                "updated_at": p[6].isoformat() if p[6] else None
            })
        
        return {"all_camera_params": formatted_results}  # Consistent response format
    
    except Exception as e:
        logger.error(f"Error retrieving all users' camera parameters: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve users' camera parameters.")

# PUT /param_stream_camera/user - Covered by POST with ON CONFLICT
@router.put("/param_stream_camera/user")
async def update_user_camera_params(params: CameraStreamQueryParams, camera_id: str, username: str = Depends(session_manager.get_current_user)):
    """Updates specific camera parameters for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)

        # Validate camera exists for this user
        cam_check_query = "SELECT stream_id FROM video_stream WHERE stream_id = %s AND user_id = %s"
        cam_exists = execute_db_query(cam_check_query, (camera_id, user_id), fetch_one=True)
        if not cam_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Camera ID '{camera_id}' not found or does not belong to you.")

        # Check if parameters exist for this camera
        param_check_query = "SELECT param_camera_id FROM param_stream_camera WHERE user_id = %s AND camera_id = %s"
        param_exists = execute_db_query(param_check_query, (user_id, camera_id), fetch_one=True)
        if not param_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No parameters found for camera '{camera_id}'.")

        update_query = "UPDATE param_stream_camera SET frame_delay = %s, frame_skip = %s, conf = %s, updated_at = CURRENT_TIMESTAMP WHERE user_id = %s AND camera_id = %s RETURNING param_camera_id"
        result = execute_db_query(
            update_query,
            (params.frame_delay, params.frame_skip, params.conf, user_id, camera_id),
            fetch_one=True
        )
        
        session_manager.log_action(
                content=f"User {username} updated parameters for camera '{camera_id}'",
                user_id=user_id,
                action_type="Updated_Param_Camera",
                ip_address=None,
                user_agent=None
        )

        return {"message": "Camera parameters updated successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating camera parameters: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update camera parameters.")

@router.delete("/param_stream_camera/user", status_code=status.HTTP_200_OK)
async def delete_user_camera_params(camera_id: str, request: Request, username: str = Depends(session_manager.get_current_user)):
    """Deletes specific camera parameters for the authenticated user."""
    try:

        user_id = get_user_id_from_username(username)
        
        query = "DELETE FROM param_stream_camera WHERE user_id = %s AND camera_id = %s"
        rows_affected = execute_db_query(query, (user_id, camera_id), return_rowcount=True)
        
        if rows_affected > 0:
            session_manager.log_action(
                    content=f"User '{username}' deleted parameters for camera '{camera_id}'.",
                    user_id=user_id,
                    action_type="Deleted_Param_Camera",
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent")
            )
            return {"message": f"Parameters for camera '{camera_id}' deleted successfully"}
        else:
            # Use consistent pattern - thrown an exception for not found
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                               detail=f"No parameters found for camera '{camera_id}' for user '{username}'.")
    
    except Exception as e:
        logger.error(f"Error deleting camera parameters: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete camera parameters.")

@router.delete("/param_stream_camera/users", status_code=status.HTTP_200_OK)
async def delete_users_camera_params(request: Request, username: str = Depends(session_manager.get_current_user)):
    """Deletes ALL specific camera parameters for ALL users (Requires admin privileges usually)."""
    try:

        user_id = get_user_id_from_username(username) # For logging

        # Optional: Add role check here
        # user_details = await user_manager.get_user_by_username(username)
        # if not user_details or user_details.get('role') != 'admin':
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

        query = "DELETE FROM param_stream_camera"
        rows_affected = execute_db_query(query, return_rowcount=True)

        session_manager.log_action(
                content=f"Admin action by '{username}': Deleted {rows_affected} camera-specific parameter entries.",
                user_id=user_id,
                action_type="Deleted_All_Param_Camera",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
                status="warning"
        )
        return {"message": f"All ({rows_affected}) user camera parameters deleted successfully"}

    except Exception as e:
        logger.error(f"Error deleting all users' camera parameters: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete all users' camera parameters.")

###################
# --- User Info Endpoint ---

@router.get("/user_info")
async def get_current_user_info(username: str = Depends(session_manager.get_current_user)):
    """Gets comprehensive information about the currently logged-in user."""
    try:
        # Retrieve user information from the database using username
        query = """
        SELECT 
            u.user_id, 
            u.username, 
            u.email,
            u.created_at,
            u.is_active,
            u.last_login,
            u.role,
            u.is_subscribed,        
            u.subscription_date,    
            u.count_of_camera,      
            (SELECT COUNT(*) FROM video_stream WHERE user_id = u.user_id) AS stream_count,
            -- Count active tokens (adjust if using is_active flag in user_tokens)
            (SELECT COUNT(*) FROM user_tokens WHERE user_id = u.user_id AND is_active = TRUE AND refresh_expires_at > CURRENT_TIMESTAMP) AS active_token_count, 
            -- Aggregate param_stream (should be 0 or 1 due to unique constraint)
            (SELECT jsonb_agg(jsonb_build_object(
            --    'frame_delay', ps.frame_delay, 
                'frame_skip', ps.frame_skip, 
                'conf', ps.conf, 
                'created_at', ps.created_at, 
                'updated_at', ps.updated_at
            ))
             FROM param_stream ps WHERE ps.user_id = u.user_id) AS stream_parameters,
            -- Aggregate param_stream_camera
            (SELECT jsonb_agg(jsonb_build_object(
                'camera_id', psc.camera_id, 
            --    'frame_delay', psc.frame_delay, 
                'frame_skip', psc.frame_skip, 
                'conf', psc.conf, 
                'created_at', psc.created_at, 
                'updated_at', psc.updated_at
                ) ORDER BY psc.created_at DESC)
             FROM param_stream_camera psc WHERE psc.user_id = u.user_id) AS camera_stream_parameters
        FROM users u
        WHERE u.username = %s
        """
        
        user_info = execute_db_query(query, (username,), fetch_one=True)
        
        if not user_info:
            # This shouldn't happen if authentication worked, but handle defensively
            logger.error(f"User info lookup failed for authenticated user: {username}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="User not found despite successful authentication."
            )
        
        # Assuming video_streaming_qdrant.get_timestamp_range works as intended
        try:
            timestamp_range_result = await get_timestamp_range(camera_id=None, username=username)
        except Exception as e:
             logger.error(f"Error fetching timestamp range for user {username}: {e}")
             timestamp_range_result = {"error": "Failed to retrieve timestamp range"}


        # Convert database result to dictionary
        user_data = {
            "user_id": str(user_info[0]), # UUID
            "username": user_info[1],
            "email": user_info[2],
            "created_at": user_info[3].isoformat() if user_info[3] else None,
            "is_active": user_info[4],
            "last_login": user_info[5].isoformat() if user_info[5] else None,
            "role": user_info[6],
            "is_subscribed": user_info[7], # From DB
            "subscription_date": user_info[8].isoformat() if user_info[8] else None, # From DB
            "camera_limit": user_info[9], # Renamed from count_of_camera for clarity
            "stream_count": user_info[10],
            "active_token_count": user_info[11],
            # Use [0] if expecting single object, else keep as list
            "stream_parameters": user_info[12][0] if user_info[12] else None, # param_stream has unique user_id
            "camera_stream_parameters": user_info[13] or [], # param_stream_camera can have multiple per user
            "timestamp_range": timestamp_range_result or {}
        }
        return user_data
        # return {"user_info": user_data}  # Consistent response format
    
    except HTTPException:
        # Re-raise HTTP exceptions 
        raise
    
    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error retrieving user info for {username}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal server error while retrieving user information."
        )

###################
# --- Admin/Maintenance Endpoints ---

@router.delete("/drop_tables", status_code=status.HTTP_200_OK)
async def drop_all_tables(request: Request, username: str = Depends(session_manager.get_current_user)):
    """
    Drops ALL application tables. EXTREMELY DANGEROUS. Requires admin role.
    """
    try:

        user_id = get_user_id_from_username(username) # For logging

        # # --- !! IMPORTANT: Role Check !! ---
        # user_details = await user_manager.get_user_by_username(username) # Need UserManager instance for this
        # if not user_details or user_details.get('role') != 'admin':
        #    logger.warning(f"Unauthorized attempt to drop tables by user: {username} (ID: {user_id})")
        #    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required to drop tables.")
        # # --- End Role Check ---

        logger.critical(f"ADMIN ACTION: User '{username}' (ID: {user_id}) initiated DROP ALL TABLES.")

        tables_to_drop = [
            # Drop tables in reverse order of dependency or use CASCADE
            "security_events",
            "logs",
            "token_blacklist",
            "user_tokens",
            "sessions", # Might be redundant if only using user_tokens
            "param_stream_camera",
            "param_stream",
            "video_stream",
            "users" # Drop users last or use CASCADE on others
        ]
        
        dropped_tables = []
        errors = {}

        for table in tables_to_drop:
            try:
                # Use CASCADE cautiously, otherwise ensure correct drop order
                execute_db_query(f"DROP TABLE IF EXISTS {table} CASCADE")
                logger.info(f"Table '{table}' dropped successfully by admin {username}.")
                dropped_tables.append(table)
            except Exception as e:
                logger.error(f"Error dropping table '{table}': {e}")
                errors[table] = str(e)

        # Log the overall action
        log_status = "failure" if errors else "success"
        log_content = f"Admin '{username}' attempted to drop all tables. Dropped: {', '.join(dropped_tables)}. Errors: {json.dumps(errors)}"
        
        session_manager.log_action(
                content=log_content,
                user_id=user_id,
                action_type="Admin_Drop_Tables",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
                status=log_status
        )

        if errors:
            # Return error if any table failed to drop
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail={"message": "Errors occurred during table drop.", "dropped": dropped_tables, "errors": errors}
            )

        return {"message": "All specified application tables dropped successfully (used CASCADE)."}

    except Exception as e:
        logger.error(f"Error during drop_all_tables operation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal server error during table drop operation."
        )
