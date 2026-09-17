# app/routes/camera_router.py
from fastapi import APIRouter, HTTPException, Depends, status, Request, Response, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Union
import csv
import io
import json
import logging
from uuid import UUID
from uuid import uuid4
import asyncpg
from zoneinfo import ZoneInfo
from datetime import timezone
from datetime import datetime
from app.schemas import (
    StreamCreate, StreamUpdate, StreamDelete, CameraStreamQueryParams,
    CamerasStateResponse, BulkLocationAssignment, BulkLocationAssignmentResult,
    LocationHierarchyResponse, CameraAlertSettings, CameraGroupResponse,
    LocationStatsResponseWithAlerts, CameraCSVRecordWithLocationAndAlerts,
    ThresholdSettings, UserCameraCountUpdate, UserCameraCountResponse
)
from app.services.session_service import session_manager 
from app.services.user_service import user_manager
from app.services.workspace_service import workspace_service
from app.services.camera_service import camera_service
from app.services.parameter_service import parameter_service
from app.services.location_service import location_service
from app.services.database import db_manager
from app.services.postgres_service import  postgres_service
from app.utils import ensure_uuid_str, check_workspace_access, parse_string_or_list, encoded_string


logger = logging.getLogger(__name__)

router = APIRouter(tags=["camera"])

# ==================== CAMERA CRUD ENDPOINTS ====================

@router.post("/source", status_code=status.HTTP_201_CREATED)
async def create_stream(
    stream: StreamCreate,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Create a new camera stream."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        member_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )

        stream_id = await camera_service.create_camera(stream, user_id_obj, workspace_id_obj, username)

        await session_manager.log_action(
            content=f"User '{username}' added Camera '{stream.name}' (ID: {stream_id}) to workspace (ID: {workspace_id_obj})",
            user_id=str(user_id_obj),
            workspace_id=str(workspace_id_obj),
            action_type="Added_Camera",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        
        return {"message": "Stream created successfully", "id": stream_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating stream: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while creating stream.")

@router.get("/source/user")
async def get_user_streams(
    return_base64: bool = Query(True, description="Return static_base64 field (yes/no)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras for the current user in their active workspace."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return []

        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")

        return await camera_service.get_user_cameras(user_id_obj, workspace_id_obj, user_role, encoded_string, return_base64)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving streams: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while retrieving streams.")

@router.get("/source/users")
async def get_all_streams(
    workspace_id_str: Optional[str] = Query(None, alias="workspaceId", description="ID of the workspace"),
    return_base64: bool = Query(True, description="Return static_base64 field (yes/no)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all streams (admin only, optionally scoped to workspace)."""
    user_id_obj = current_user_data["user_id"]
    user_system_role = current_user_data.get("role")
    
    try:
        # Build query based on workspace filter
        query_base = """
            SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.created_at, vs.updated_at, vs.workspace_id, w.name as workspace_name,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                   vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                   vs.is_shoplifting_camera, vs.is_blocked_exit_camera, vs.is_no_entry_zone_camera,
                   vs.is_people_counting_camera, vs.detection_models
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
        """
        
        params = []
        conditions = []
        
        if workspace_id_str:
            target_workspace_id = ensure_uuid_str(workspace_id_str)
            
            # Check permissions for workspace
            if user_system_role != "admin":
                membership_details = await check_workspace_access(
                    db_manager,
                    user_id_obj,
                    UUID(target_workspace_id),
                    required_role="admin",
                )
            conditions.append(f"vs.workspace_id = $1")
            params.append(target_workspace_id)
            order_by = " ORDER BY u.username, vs.created_at DESC"
        else:
            # No workspace filter - system admin only
            if user_system_role != "admin":
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required.")
            order_by = " ORDER BY w.name, u.username, vs.created_at DESC"
        
        final_query = query_base
        if conditions:
            final_query += " WHERE " + " AND ".join(conditions)
        final_query += order_by
        
        streams_data = await db_manager.execute_query(final_query, params=tuple(params), fetch_all=True)
        
        return [
            {
                "id": str(s["stream_id"]), "user_id": str(s["user_id"]), "owner_username": s["owner_username"],
                "name": s["name"], "path": s["path"], "type": s["type"], "status": s["status"],
                "is_streaming": s["is_streaming"],
                "location": s["location"], "area": s["area"], "building": s["building"],
                "floor_level": s["floor_level"], "zone": s["zone"],
                "latitude": float(s["latitude"]) if s["latitude"] else None,
                "longitude": float(s["longitude"]) if s["longitude"] else None,
                "count_threshold_greater": s["count_threshold_greater"],
                "count_threshold_less": s["count_threshold_less"],
                "alert_enabled": s["alert_enabled"],
                "is_shoplifting_camera": s["is_shoplifting_camera"],
                "is_blocked_exit_camera": s["is_blocked_exit_camera"],
                "is_no_entry_zone_camera": s["is_no_entry_zone_camera"],
                "is_people_counting_camera": s["is_people_counting_camera"],
                "detection_models": s["detection_models"],
                "created_at": s["created_at"].isoformat() if s["created_at"] else None,
                "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
                "workspace_id": str(s["workspace_id"]) if s["workspace_id"] else None,
                "workspace_name": s["workspace_name"],
                "static_base64": encoded_string if return_base64 else ""
            } for s in streams_data
        ] if streams_data else []
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving all streams: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.put("/source", status_code=status.HTTP_200_OK)
async def update_streams(
    streams: List[StreamUpdate],
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update multiple camera streams."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_role = current_user_data.get("role")
    
    try:
        _, active_workspace_id_obj = await workspace_service.get_user_and_workspace(username)

        updated_ids = []
        failed_ids = []

        for stream_update in streams:
            success, error = await camera_service.update_camera(stream_update, user_id_obj, user_role)
            if success:
                updated_ids.append(ensure_uuid_str(stream_update.id))
            else:
                failed_ids.append(f"{stream_update.id} ({error})")

        # Log action
        log_content = f"User '{username}' attempted to update {len(streams)} camera(s)."
        if updated_ids:
            log_content += f" Successfully updated: {', '.join(updated_ids)}."
        if failed_ids:
            log_content += f" Failed: {', '.join(failed_ids)}."
        
        await session_manager.log_action(
            content=log_content,
            user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Updated_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        response_status = status.HTTP_200_OK
        message = f"Updated: {len(updated_ids)}"
        if failed_ids:
            response_status = status.HTTP_207_MULTI_STATUS if updated_ids else status.HTTP_400_BAD_REQUEST
            message += f", Failed: {len(failed_ids)}"
        
        return Response(
            content=json.dumps({
                "detail": message,
                "updated_ids": updated_ids,
                "failed_ids": failed_ids
            }),
            status_code=response_status,
            media_type="application/json"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in batch update: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during stream updates.")

@router.delete("/source", status_code=status.HTTP_200_OK)
async def delete_streams(
    stream_ids_payload: StreamDelete,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete multiple camera streams."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_role = current_user_data.get("role")
    
    try:
        _, active_workspace_id_obj = await workspace_service.get_user_and_workspace(username)

        if not stream_ids_payload.ids:
            return {"message": "No stream IDs provided for deletion."}

        deleted_ids, unauthorized_ids, not_found_ids = await camera_service.delete_cameras(
            stream_ids_payload.ids,
            user_id_obj,
            user_role
        )

        # Log action
        log_content = f"User '{username}' attempted to delete {len(stream_ids_payload.ids)} camera(s)."
        if deleted_ids:
            log_content += f" Deleted: {', '.join(deleted_ids)}."
        if unauthorized_ids:
            log_content += f" Unauthorized: {', '.join(unauthorized_ids)}."
        if not_found_ids:
            log_content += f" Not found: {', '.join(not_found_ids)}."

        await session_manager.log_action(
            content=log_content,
            user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Deleted_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        response_message = f"Deleted: {len(deleted_ids)}"

        return {
            "message": response_message,
            "deleted_ids": deleted_ids,
            "unauthorized_ids": unauthorized_ids,
            "not_found_ids": not_found_ids
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in batch deletion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during stream deletion.")

# ==================== Threshold Management ====================

@router.post("/{stream_id}/thresholds")
async def update_stream_thresholds(
    stream_id: str,
    settings: ThresholdSettings,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Verify stream exists and user has access
        stream_info = await db_manager.execute_query(
            "SELECT workspace_id, name FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            stream_info['workspace_id'],
        )
        
        # Validate thresholds
        if (settings.count_threshold_greater is not None and 
            settings.count_threshold_less is not None and 
            settings.count_threshold_greater <= settings.count_threshold_less):
            raise HTTPException(status_code=400, detail="Greater threshold must be higher than less threshold")
        
        # Update thresholds
        await db_manager.execute_query(
            """UPDATE video_stream 
               SET count_threshold_greater = $1, count_threshold_less = $2, 
                   alert_enabled = $3, updated_at = NOW() 
               WHERE stream_id = $4""",
            (settings.count_threshold_greater, settings.count_threshold_less,
             settings.alert_enabled, stream_id_uuid)
        )
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Thresholds updated for stream '{stream_info['name']}'",
            "settings": {
                "count_threshold_greater": settings.count_threshold_greater,
                "count_threshold_less": settings.count_threshold_less,
                "alert_enabled": settings.alert_enabled
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error updating stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{stream_id}/thresholds")
async def get_stream_thresholds(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Get stream info with thresholds
        stream_info = await db_manager.execute_query(
            """SELECT vs.workspace_id, vs.name, vs.count_threshold_greater, 
                      vs.count_threshold_less, vs.alert_enabled
               FROM video_stream vs WHERE vs.stream_id = $1""",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            stream_info['workspace_id'],
        )
        
        return JSONResponse(content={
            "status": "success",
            "stream_id": stream_id,
            "name": stream_info['name'],
            "thresholds": {
                "count_threshold_greater": stream_info.get('count_threshold_greater'),
                "count_threshold_less": stream_info.get('count_threshold_less'),
                "alert_enabled": stream_info.get('alert_enabled', True)
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error getting stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/streams")
async def get_all_workspace_streams_endpoint(
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all streams accessible to the user."""
    try:
        user_id_str = str(current_user_data["user_id"])
        result = await camera_service.get_workspace_streams(user_id_str, workspace_id)
        return JSONResponse(content={"status": "success", "data": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.error(f"Error getting workspace streams: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

# ==================== CAMERA STATE & INFO ====================

@router.get("/camera-state", response_model=CamerasStateResponse)
async def get_camera_state(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get camera state summary for the workspace."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return CamerasStateResponse(
                cameras=[], total_active=0, total_inactive=0,
                total_error=0, total_processing=0, total_cameras=0
            )

        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )

        return await camera_service.get_camera_state(workspace_id_obj)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching camera state: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred fetching camera state.")

@router.get("/user_info")
async def get_current_user_info(
    current_user_data_dep: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get current user information including workspace details."""
    user_id_obj = current_user_data_dep["user_id"]
    username = current_user_data_dep["username"]
    
    try:
        user_base_info = await user_manager.get_user_by_id(str(user_id_obj))
        if not user_base_info:
            raise HTTPException(status_code=404, detail="User not found in DB.")

        active_workspace_params = None
        active_workspace_id_str = None
        stream_count = 0
        
        try:
            _, active_ws_id_obj = await workspace_service.get_user_and_workspace(username)
            if active_ws_id_obj:
                active_workspace_id_str = str(active_ws_id_obj)
                
                # Get workspace params
                active_workspace_params = await parameter_service.get_workspace_params(active_ws_id_obj)
                
                # Get stream count
                stream_count_q = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1 AND workspace_id = $2"
                sc_res = await db_manager.execute_query(stream_count_q, params=(user_id_obj, active_ws_id_obj), fetch_one=True)
                stream_count = sc_res['count'] if sc_res else 0
        except HTTPException as e_ws:
            logger.warning(f"Could not determine active workspace for {username}: {e_ws.detail}")
        
        # Get active token count
        token_count_q = "SELECT COUNT(*) as count FROM user_tokens WHERE user_id = $1 AND is_active = TRUE AND refresh_expires_at > $2"
        from datetime import datetime
        tc_res = await db_manager.execute_query(token_count_q, params=(user_id_obj, datetime.now(ZoneInfo("Africa/Cairo"))), fetch_one=True)
        active_token_count = tc_res['count'] if tc_res else 0

        # Get timestamp range
        timestamp_range_result = {}
        if active_workspace_id_str:
            try:
                timestamp_range_result = await postgres_service.get_timestamp_range(
                    camera_ids=None,
                    workspace_id=active_workspace_id_str,
                    requesting_username=username,
                    user_system_role=None,
                    user_workspace_role=None,
                )
                if hasattr(timestamp_range_result, 'model_dump'):
                    timestamp_range_result = timestamp_range_result.model_dump(exclude_none=True)
            except Exception as e_ts:
                logger.error(f"Error fetching timestamp range: {e_ts}", exc_info=True)
                timestamp_range_result = {"error": f"Failed to retrieve timestamp range: {str(e_ts)}"}
        
        return {
            "user_id": str(user_base_info["user_id"]),
            "username": user_base_info["username"],
            "email": user_base_info["email"],
            "created_at": user_base_info["created_at"].isoformat() if user_base_info["created_at"] else None,
            "is_active": user_base_info["is_active"],
            "last_login": user_base_info["last_login"].isoformat() if user_base_info["last_login"] else None,
            "role": user_base_info["role"],
            "is_subscribed": user_base_info["is_subscribed"],
            "subscription_date": user_base_info["subscription_date"].isoformat() if user_base_info["subscription_date"] else None,
            "camera_limit": user_base_info["count_of_camera"],
            "active_workspace_id": active_workspace_id_str,
            "active_workspace_stream_count": stream_count,
            "active_token_count": active_token_count,
            "active_workspace_stream_parameters": active_workspace_params,
            "active_workspace_data_timestamp_range": timestamp_range_result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving user info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while retrieving user information.")

# ==================== PARAMETER ENDPOINTS ====================

@router.post("/param_stream", status_code=status.HTTP_201_CREATED)
async def create_or_update_workspace_camera_params_post(
    params: CameraStreamQueryParams,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Create or update workspace camera parameters (POST)."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Cannot save parameters.")

        is_sys_admin = current_user_data.get("role") == "admin"
        if not is_sys_admin:
            membership_details = await check_workspace_access(
                db_manager,
                user_id_obj,
                workspace_id_obj,
                required_role=None,
            )
        
        result = await parameter_service.create_or_update_workspace_params(params, user_id_obj, workspace_id_obj)
        
        action_type = "Workspace_Param_Created" if result["action"] == "created" else "Workspace_Param_Updated"
        await session_manager.log_action(
            content=f"User '{username}' {result['action']} parameters for workspace '{workspace_id_obj}'. Values: {params.model_dump()}",
            user_id=str(user_id_obj),
            workspace_id=str(workspace_id_obj),
            action_type=action_type,
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        
        return {
            "message": f"Parameters for workspace '{workspace_id_obj}' saved successfully.",
            "param_id": result["param_id"],
            "workspace_id": result["workspace_id"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error saving param_stream: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while saving parameters.")

@router.put("/param_stream/user", status_code=status.HTTP_201_CREATED)
async def create_or_update_workspace_camera_params_put(
    params: CameraStreamQueryParams,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Create or update workspace camera parameters (PUT)."""
    return await create_or_update_workspace_camera_params_post(params, request, current_user_data)

@router.get("/param_stream/user")
async def get_active_workspace_params(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get parameters for the active workspace."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        
        if workspace_id_obj:
            membership_details = await check_workspace_access(
                db_manager,
                user_id_obj,
                workspace_id_obj,
                required_role=None,
            )
        
        return await parameter_service.get_workspace_params(workspace_id_obj)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving workspace params: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.delete("/param_stream/user", status_code=status.HTTP_200_OK)
async def delete_workspace_params(
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete workspace stream parameters."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found to delete parameters for.")

        is_sys_admin = current_user_data.get("role") == "admin"
        if not is_sys_admin:
            membership_details = await check_workspace_access(
                db_manager,
                user_id_obj,
                workspace_id_obj,
                required_role="admin",
            )
        
        rows_affected = await parameter_service.delete_workspace_params(workspace_id_obj)
        
        if rows_affected > 0:
            await session_manager.log_action(
                content=f"User '{username}' deleted stream parameters for workspace (ID: {workspace_id_obj}).",
                user_id=str(user_id_obj),
                workspace_id=str(workspace_id_obj),
                action_type="Deleted_Workspace_Param_Stream",
                ip_address=request.client.host if request.client else "Unknown",
                user_agent=request.headers.get("user-agent", "Unknown")
            )
            return {"message": "Workspace stream parameters deleted successfully"}
        else:
            return {"message": "No stream parameters found for the active workspace to delete"}
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting workspace parameters: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.get("/param_stream/users")
async def get_all_workspace_params(
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get parameters for all workspaces (admin only)."""
    try:
        if current_admin_data.get("role") != "admin":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required.")
        
        return await parameter_service.get_all_workspace_params()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving all workspace params: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.put("/param_stream/users", status_code=status.HTTP_200_OK)
async def update_all_workspace_params(
    params_list_payload: List[CameraStreamQueryParams],
    request: Request,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update parameters for all workspaces (admin only)."""
    try:
        if current_admin_data.get("role") != 'admin':
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin privileges required.")
        
        user_id_obj = current_admin_data["user_id"]
        username = current_admin_data["username"]
        
        updated_ids, failed_ids = await parameter_service.update_all_workspace_params(
            params_list_payload,
            user_id_obj
        )

        log_content = f"Admin '{username}' attempted to update stream parameters for {len(params_list_payload)} workspace(s)."
        if updated_ids:
            log_content += f" Updated: {', '.join(updated_ids)}."
        if failed_ids:
            log_content += f" Failed: {', '.join(failed_ids)}."

        await session_manager.log_action(
            content=log_content,
            user_id=str(user_id_obj),
            action_type="Updated_All_Workspace_Params",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown"),
            status="warning" if failed_ids else "info"
        )

        response_status = status.HTTP_200_OK
        message = f"Updated: {len(updated_ids)}"
        if failed_ids:
            response_status = status.HTTP_207_MULTI_STATUS if updated_ids else status.HTTP_400_BAD_REQUEST
            message += f", Failed: {len(failed_ids)}"

        return Response(
            content=json.dumps({
                "detail": message,
                "updated_workspace_ids": updated_ids,
                "failed_workspace_ids": failed_ids
            }),
            status_code=response_status,
            media_type="application/json"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in batch update: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during batch update.")

@router.delete("/param_stream/users", status_code=status.HTTP_200_OK)
async def delete_all_workspaces_params(
    request: Request,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete all workspace stream parameters (admin only)."""
    try:
        if current_admin_data.get("role") != "admin":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required.")

        user_id_obj = current_admin_data["user_id"]
        username = current_admin_data["username"]

        rows_affected = await parameter_service.delete_all_workspaces_params()

        await session_manager.log_action(
            content=f"Admin action by '{username}': Deleted all {rows_affected} workspace stream parameter entries.",
            user_id=str(user_id_obj),
            action_type="Deleted_All_Workspace_Params",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown"),
            status="warning"# or "info" depending on your constraint
        )
        
        return {"message": f"All ({rows_affected}) workspace stream parameters deleted successfully."}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting all workspace params: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.get("/param_stream/workspace/status")
async def get_workspace_param_sync_status(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get workspace parameter synchronization status."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {
                "workspace_id": None,
                "message": "No active workspace found.",
                "current_parameters": None,
                "members": [],
                "total_members": 0
            }

        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )

        return await parameter_service.get_workspace_param_sync_status(workspace_id_obj)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting workspace param status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get workspace parameter status.")

# ==================== LOCATION ENDPOINTS ====================

@router.get("/locations/hierarchy", response_model=LocationHierarchyResponse)
async def get_location_hierarchy(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get the location hierarchy for the workspace."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return LocationHierarchyResponse(
                workspace_id="",
                hierarchy=[],
                total_locations=0,
                buildings=[],
                zones=[],
                areas=[],
                locations=[]
            )
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        return await camera_service.get_location_hierarchy(workspace_id_obj)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting location hierarchy: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location hierarchy.")

@router.get("/locations/stats", response_model=List[LocationStatsResponseWithAlerts])
async def get_location_statistics(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get statistics for all locations in the workspace."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return []
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        stats_query = """
            SELECT 
                vs.workspace_id, w.name as workspace_name, vs.location, vs.area, vs.building,
                vs.floor_level, vs.zone,
                COUNT(*) as total_cameras,
                COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_cameras,
                COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_cameras,
                COUNT(CASE WHEN vs.status = 'error' THEN 1 END) as error_cameras,
                COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_cameras,
                COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_cameras,
                ARRAY_AGG(vs.stream_id) as camera_ids,
                ARRAY_AGG(vs.name) as camera_names,
                ARRAY_AGG(
                    CASE WHEN vs.alert_enabled = true THEN
                        JSON_BUILD_OBJECT(
                            'camera_id', vs.stream_id::text,
                            'camera_name', vs.name,
                            'count_threshold_greater', vs.count_threshold_greater,
                            'count_threshold_less', vs.count_threshold_less
                        )
                    END
                ) FILTER (WHERE vs.alert_enabled = true) as alert_configurations
            FROM video_stream vs
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.workspace_id = $1
            GROUP BY vs.workspace_id, w.name, vs.location, vs.area, vs.building, vs.floor_level, vs.zone
            ORDER BY vs.floor_level, vs.building, vs.zone, vs.area, vs.location
        """
        
        stats_results = await db_manager.execute_query(stats_query, (workspace_id_obj,), fetch_all=True)
        
        return [
            LocationStatsResponseWithAlerts(
                workspace_id=str(stat["workspace_id"]),
                workspace_name=stat["workspace_name"],
                location=stat["location"],
                area=stat["area"],
                building=stat["building"],
                floor_level=stat["floor_level"],
                zone=stat["zone"],
                total_cameras=stat["total_cameras"],
                active_cameras=stat["active_cameras"],
                inactive_cameras=stat["inactive_cameras"],
                error_cameras=stat["error_cameras"],
                streaming_cameras=stat["streaming_cameras"],
                alert_enabled_cameras=stat["alert_enabled_cameras"],
                camera_ids=[str(cid) for cid in stat["camera_ids"]] if stat["camera_ids"] else [],
                camera_names=stat["camera_names"] if stat["camera_names"] else [],
                alert_configurations=stat["alert_configurations"] if stat["alert_configurations"] else []
            ) for stat in stats_results
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting location statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location statistics.")

@router.get("/locations/list")
async def get_locations(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations for the current user."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"locations": [], "total_count": 0}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        if user_role == "admin":
            query = """
                SELECT DISTINCT location, COUNT(*) as camera_count
                FROM video_stream 
                WHERE workspace_id = $1 AND location IS NOT NULL AND location != ''
                GROUP BY location ORDER BY location
            """
            params = (workspace_id_obj,)
        else:
            query = """
                SELECT DISTINCT location, COUNT(*) as camera_count
                FROM video_stream 
                WHERE workspace_id = $1 AND user_id = $2 AND location IS NOT NULL AND location != ''
                GROUP BY location ORDER BY location
            """
            params = (workspace_id_obj, user_id_obj)
        
        results = await db_manager.execute_query(query, params, fetch_all=True)
        
        return {
            "locations": [{"location": r["location"], "camera_count": r["camera_count"]} for r in results],
            "total_count": len(results)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve locations.")

@router.get("/areas/list")
async def get_areas(
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique areas, optionally filtered by location(s)."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        
        locations = parse_string_or_list(locations)
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"areas": [], "total_count": 0, "filtered_by_locations": locations}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        
        conditions = ["workspace_id = $1", "area IS NOT NULL", "area != ''"]
        params = [workspace_id_obj]
        param_count = 1
        
        if user_role != "admin":
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id_obj)
        
        if locations:
            if isinstance(locations, str):
                locations = [locations]
            if locations:
                param_count += 1
                placeholders = ", ".join([f"${param_count + i}" for i in range(len(locations))])
                conditions.append(f"location IN ({placeholders})")
                params.extend(locations)
        
        query = f"""
            SELECT DISTINCT area, location, COUNT(*) as camera_count
            FROM video_stream 
            WHERE {' AND '.join(conditions)}
            GROUP BY area, location
            ORDER BY area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        return {
            "areas": [
                {"area": r["area"], "location": r["location"], "camera_count": r["camera_count"]}
                for r in results
            ],
            "total_count": len(results),
            "filtered_by_locations": locations
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve areas.")

@router.get("/buildings/list")
async def get_buildings(
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique buildings, optionally filtered by area(s)."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        areas = parse_string_or_list(areas)
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"buildings": [], "total_count": 0, "filtered_by_areas": areas}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        if isinstance(areas, str):
            areas = [areas] if areas else None
        
        result = await location_service.get_location_values(
            workspace_id_obj,
            user_id_obj,
            user_role,
            "building",
            "area" if areas else None,
            areas
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve buildings.")

@router.get("/floor-levels/list")
async def get_floor_levels(
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique floor levels, optionally filtered by building(s)."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        buildings = parse_string_or_list(buildings)
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"floor_levels": [], "total_count": 0, "filtered_by_buildings": buildings}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        if isinstance(buildings, str):
            buildings = [buildings] if buildings else None
        
        result = await location_service.get_location_values(
            workspace_id_obj,
            user_id_obj,
            user_role,
            "floor_level",
            "building" if buildings else None,
            buildings
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve floor levels.")

@router.get("/zones/list")
async def get_zones(
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique zones, optionally filtered by floor level(s)."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        floor_levels = parse_string_or_list(floor_levels)
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"zones": [], "total_count": 0, "filtered_by_floor_levels": floor_levels}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        if isinstance(floor_levels, str):
            floor_levels = [floor_levels] if floor_levels else None
        
        result = await location_service.get_location_values(
            workspace_id_obj,
            user_id_obj,
            user_role,
            "zone",
            "floor_level" if floor_levels else None,
            floor_levels
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting zones: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve zones.")

@router.get("/cameras/by-location")
async def get_cameras_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    group_by: str = Query("location", pattern="^(location|area|building|floor_level|zone)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras grouped by location criteria."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return CameraGroupResponse(groups=[], total_groups=0, total_cameras=0, group_type=group_by)
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        return await location_service.get_cameras_by_location(
            workspace_id_obj,
            user_id_obj,
            user_role,
            group_by,
            location,
            area,
            building,
            floor_level,
            zone
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cameras by location: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve cameras by location.")
    
@router.get("/streams/locations/search")
async def search_streams_by_location_filters(
    q: Optional[str] = Query(None, description="Search query for location names"),
    location_type: Optional[Union[str, List[str]]] = Query(None, description="Filter by location type(s): building, floor_level, zone, area, location"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific zone(s)"),
    workspace_id: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search for streams using location-based filters and text search."""
    
    # Parse all filter parameters
    location_type = parse_string_or_list(location_type)
    locations = parse_string_or_list(locations)
    areas = parse_string_or_list(areas)
    buildings = parse_string_or_list(buildings)
    floor_levels = parse_string_or_list(floor_levels)
    zones = parse_string_or_list(zones)
    
    user_id_obj = current_user_data["user_id"]
    username = current_user_data.get("username", "unknown")
    
    try:
        # Get workspace ID
        workspace_id_obj = None
        if workspace_id:
            workspace_id_obj = UUID(workspace_id)
        else:
            _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        
        # Call the service method
        results = await camera_service.search_cameras_by_filters(
            user_id=user_id_obj,
            workspace_id=workspace_id_obj,
            q=q,
            location_type=location_type,
            locations=locations,
            areas=areas,
            buildings=buildings,
            floor_levels=floor_levels,
            zones=zones,
            encoded_string=encoded_string
        )
        
        return results
        
    except ValueError as ve:
        logger.error(f"Invalid data for location search for user {username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching streams by location for user {username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while searching streams.")

# ==================== ALERT ENDPOINTS ====================

@router.put("/cameras/{camera_id}/alert-settings")
async def update_camera_alert_settings(
    camera_id: str,
    alert_data: CameraAlertSettings,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update alert settings for a specific camera."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        result = await camera_service.update_alert_settings(camera_id, alert_data, user_id_obj, workspace_id_obj)
        
        await session_manager.log_action(
            content=f"User '{username}' updated alert settings for camera '{result['camera_name']}'. "
                   f"Alert enabled: {alert_data.alert_enabled}, "
                   f"Greater threshold: {alert_data.count_threshold_greater}, "
                   f"Less threshold: {alert_data.count_threshold_less}",
            user_id=str(user_id_obj),
            workspace_id=str(workspace_id_obj),
            action_type="Camera_Alert_Settings_Updated",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        
        return {
            "message": "Alert settings updated successfully",
            **result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating camera alert settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update camera alert settings.")

@router.get("/cameras/alert-summary")
async def get_alert_summary(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get a summary of all cameras with alert settings."""
    username = current_user_data["username"]
    user_id_obj = current_user_data["user_id"]
    
    try:
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"total_cameras": 0, "alert_enabled_cameras": 0, "cameras": []}
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        if user_role == "admin":
            query = """
                SELECT vs.stream_id, vs.name, vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                       vs.status, vs.is_streaming, vs.alert_enabled,
                       vs.count_threshold_greater, vs.count_threshold_less, u.username as owner
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.workspace_id = $1
                ORDER BY vs.alert_enabled DESC, vs.name
            """
            params = (workspace_id_obj,)
        else:
            query = """
                SELECT vs.stream_id, vs.name, vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                       vs.status, vs.is_streaming, vs.alert_enabled,
                       vs.count_threshold_greater, vs.count_threshold_less, u.username as owner
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.workspace_id = $1 AND vs.user_id = $2
                ORDER BY vs.alert_enabled DESC, vs.name
            """
            params = (workspace_id_obj, user_id_obj)
        
        cameras = await db_manager.execute_query(query, params, fetch_all=True)
        
        alert_enabled_count = sum(1 for camera in cameras if camera["alert_enabled"])
        
        return {
            "total_cameras": len(cameras),
            "alert_enabled_cameras": alert_enabled_count,
            "cameras": [
                {
                    "camera_id": str(c["stream_id"]),
                    "name": c["name"],
                    "location": c["location"],
                    "area": c["area"],
                    "building": c["building"],
                    "floor_level": c["floor_level"],
                    "zone": c["zone"],
                    "status": c["status"],
                    "is_streaming": c["is_streaming"],
                    "alert_enabled": c["alert_enabled"],
                    "count_threshold_greater": c["count_threshold_greater"],
                    "count_threshold_less": c["count_threshold_less"],
                    "owner": c["owner"]
                } for c in cameras
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting alert summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve alert summary.")

@router.put("/cameras/bulk-location-assignment", response_model=BulkLocationAssignmentResult)
async def bulk_assign_camera_locations(
    assignment_data: BulkLocationAssignment,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Bulk assign location data and alert thresholds to multiple cameras."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_role = membership_details.get("role")
        
        updated_cameras, failed_cameras, errors = await camera_service.bulk_assign_locations(
            assignment_data.camera_ids,
            assignment_data,
            user_id_obj,
            workspace_id_obj,
            user_role
        )
        
        await session_manager.log_action(
            content=f"User '{username}' bulk assigned locations and alert settings to {len(assignment_data.camera_ids)} cameras. "
                   f"Successful: {len(updated_cameras)}, Failed: {len(failed_cameras)}",
            user_id=str(user_id_obj),
            workspace_id=str(workspace_id_obj),
            action_type="Bulk_Location_Assignment",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        
        return BulkLocationAssignmentResult(
            total_processed=len(assignment_data.camera_ids),
            successful_assignments=len(updated_cameras),
            failed_assignments=len(failed_cameras),
            updated_cameras=updated_cameras,
            failed_cameras=failed_cameras,
            errors=errors
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in bulk location assignment: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to perform bulk location assignment.")

# ==================== BULK UPLOAD ENDPOINTS ====================

@router.get("/source/bulk-upload-with-location/template")
async def download_camera_csv_template_with_location(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Download a CSV template for bulk camera upload with location data and alert settings."""
    template_content = """name,path,type,status,is_streaming,location,area,building,floor_level,zone,latitude,longitude,count_threshold_greater,count_threshold_less,alert_enabled
Main Entrance Camera,rtsp://192.168.1.100/stream,rtsp,active,true,Main Entrance,Lobby,Building A,Ground Floor,Security Zone,40.7128,-74.0060,10,2,true
Parking Camera 1,rtsp://192.168.1.101/stream,rtsp,active,false,Parking Lot,Exterior,Building A,Ground Floor,Parking Zone,40.7130,-74.0065,5,1,true
Office Camera 1,/path/to/office1.mp4,video file,inactive,false,Office 101,East Wing,Building A,First Floor,Office Zone,,,,,false
Cafeteria Camera,http://192.168.1.102/stream,http,active,true,Cafeteria,Central Area,Building A,Ground Floor,Common Zone,,,15,3,true
Server Room Camera,rtsp://192.168.1.103/stream,rtsp,active,true,Server Room,IT Wing,Building B,Basement,Restricted Zone,40.7125,-74.0055,1,,true
"""
    
    return Response(
        content=template_content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=camera_upload_template_with_location_and_alerts.csv"}
    )

@router.post("/source/bulk-upload-with-location", status_code=status.HTTP_200_OK)
async def bulk_upload_cameras_with_location(
    file: UploadFile = File(..., description="CSV file containing camera data"),
    request: Request = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Bulk upload cameras from CSV file with location data and alert thresholds."""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        
        
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")

        membership_details = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )

        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV file")

        contents = await file.read()
        
        try:
            csv_content = contents.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            required_columns = ['name', 'path']
            if not all(col in csv_reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
                raise HTTPException(status_code=400, detail=f"Missing required columns: {missing_cols}")
            
            rows = list(csv_reader)
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File encoding error")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing CSV: {str(e)}")

        if not rows:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Check camera limits
        user_db_details = await user_manager.get_user_by_id(str(user_id_obj))
        allowed_camera_count = user_db_details.get("count_of_camera", 5)
        user_system_role = user_db_details.get("role", "user")

        current_stream_count = 0
        if user_system_role != 'admin':
            count_query = "SELECT COUNT(*) as stream_count FROM video_stream WHERE user_id = $1 AND workspace_id = $2"
            count_result = await db_manager.execute_query(count_query, params=(user_id_obj, workspace_id_obj), fetch_one=True)
            current_stream_count = count_result['stream_count'] if count_result else 0

        successful_cameras = []
        failed_cameras = []
        errors = []
        processed_count = 0

        for row_idx, row in enumerate(rows, start=1):
            processed_count += 1
            row_errors = []
            
            try:
                # Clean row data
                cleaned_row = {key: str(value).strip() if value is not None else "" for key, value in row.items()}

                # Set defaults
                cleaned_row.setdefault('type', 'local')
                cleaned_row.setdefault('status', 'inactive')
                cleaned_row.setdefault('is_streaming', 'false')
                cleaned_row.setdefault('alert_enabled', 'false')
                
                # Location fields
                for field in ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']:
                    cleaned_row.setdefault(field, None)
                
                # Alert threshold fields
                cleaned_row.setdefault('count_threshold_greater', None)
                cleaned_row.setdefault('count_threshold_less', None)

                # Convert boolean fields
                is_streaming_str = cleaned_row['is_streaming'].lower()
                cleaned_row['is_streaming'] = is_streaming_str in ['true', '1', 'yes', 'on']

                alert_enabled_str = cleaned_row['alert_enabled'].lower()
                cleaned_row['alert_enabled'] = alert_enabled_str in ['true', '1', 'yes', 'on']

                # Validate required fields
                if not cleaned_row.get('name'):
                    row_errors.append("Name is required")
                if not cleaned_row.get('path'):
                    row_errors.append("Path is required")

                # Clean empty fields
                for field in ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']:
                    if cleaned_row.get(field) == '':
                        cleaned_row[field] = None

                # Clean and validate threshold fields
                for field in ['count_threshold_greater', 'count_threshold_less']:
                    if cleaned_row.get(field) == '':
                        cleaned_row[field] = None
                    elif cleaned_row.get(field) is not None:
                        try:
                            cleaned_row[field] = int(cleaned_row[field])
                        except ValueError:
                            row_errors.append(f"Invalid {field} value: {cleaned_row[field]}")

                # Validate using Pydantic model
                try:
                    camera_record = CameraCSVRecordWithLocationAndAlerts(**cleaned_row)
                except Exception as validation_error:
                    row_errors.append(f"Validation error: {str(validation_error)}")

                if row_errors:
                    failed_cameras.append({"row": row_idx, "data": cleaned_row, "errors": row_errors})
                    errors.extend([f"Row {row_idx}: {error}" for error in row_errors])
                    continue

                # Check camera limit
                if user_system_role != 'admin':
                    if current_stream_count + len(successful_cameras) >= allowed_camera_count:
                        failed_cameras.append({
                            "row": row_idx,
                            "data": cleaned_row,
                            "errors": [f"Camera limit ({allowed_camera_count}) exceeded"]
                        })
                        errors.append(f"Row {row_idx}: Camera limit exceeded")
                        continue

                # Check for duplicate names
                existing_camera_query = "SELECT stream_id FROM video_stream WHERE workspace_id = $1 AND name = $2"
                existing_camera = await db_manager.execute_query(
                    existing_camera_query,
                    params=(workspace_id_obj, cleaned_row['name']),
                    fetch_one=True
                )
                
                if existing_camera:
                    failed_cameras.append({
                        "row": row_idx,
                        "data": cleaned_row,
                        "errors": [f"Camera with name '{cleaned_row['name']}' already exists"]
                    })
                    errors.append(f"Row {row_idx}: Duplicate camera name")
                    continue

                # Insert camera
                stream_id = uuid4()
                now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
                
                insert_query = """
                    INSERT INTO video_stream 
                    (stream_id, user_id, workspace_id, name, path, type, status, is_streaming, 
                     location, area, building, floor_level, zone, latitude, longitude,
                     count_threshold_greater, count_threshold_less, alert_enabled,
                     created_at, updated_at, last_activity) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                """
                
                await db_manager.execute_query(
                    insert_query,
                    params=(stream_id, user_id_obj, workspace_id_obj, cleaned_row['name'],
                           cleaned_row['path'], cleaned_row['type'], cleaned_row['status'],
                           cleaned_row['is_streaming'], cleaned_row['location'], cleaned_row['area'],
                           cleaned_row['building'], cleaned_row['floor_level'], cleaned_row['zone'],
                           cleaned_row['latitude'], cleaned_row['longitude'],
                           cleaned_row['count_threshold_greater'], cleaned_row['count_threshold_less'],
                           cleaned_row['alert_enabled'], now_utc, now_utc, now_utc)
                )
                
                successful_cameras.append({
                    "row": row_idx,
                    "stream_id": str(stream_id),
                    "name": cleaned_row['name'],
                    "path": cleaned_row['path'],
                    "location": cleaned_row['location'],
                    "area": cleaned_row['area'],
                    "building": cleaned_row['building'],
                    "alert_enabled": cleaned_row['alert_enabled']
                })

            except Exception as e:
                logger.error(f"Error processing row {row_idx}: {e}", exc_info=True)
                failed_cameras.append({
                    "row": row_idx,
                    "data": row,
                    "errors": [f"Processing error: {str(e)}"]
                })
                errors.append(f"Row {row_idx}: Processing error")

        # Log the bulk upload
        await session_manager.log_action(
            content=f"User '{username}' performed bulk camera upload with location and alert data. "
                   f"Total: {processed_count}, Successful: {len(successful_cameras)}, Failed: {len(failed_cameras)}",
            user_id=str(user_id_obj),
            workspace_id=str(workspace_id_obj),
            action_type="Bulk_Camera_Upload_With_Location_And_Alerts",
            ip_address=request.client.host if request and request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown") if request else "Unknown"
        )

        return {
            "total_processed": processed_count,
            "successful_uploads": len(successful_cameras),
            "failed_uploads": len(failed_cameras),
            "successful_cameras": successful_cameras,
            "failed_cameras": failed_cameras,
            "errors": errors
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in bulk upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to perform bulk upload.")

# ==================== CAMERA LIMIT MANAGEMENT ENDPOINTS ====================

@router.put("/admin/user-camera-limit", status_code=status.HTTP_200_OK)
async def update_user_camera_limit(
    update_data: UserCameraCountUpdate,
    request: Request,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Update the camera count limit for a specific user.
    Only system admins can update camera limits.
    """
    admin_user_id = None
    admin_username = "unknown"
    
    try:
        # Verify admin privileges
        admin_user_id = current_admin_data["user_id"]
        admin_username = current_admin_data["username"]
        
        if current_admin_data.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="System admin privileges required to update camera limits."
            )
        
        # Validate and convert user_id
        target_user_id = ensure_uuid_str(update_data.user_id)
        
        # Get current user information
        user_query = """
            SELECT user_id, username, count_of_camera, role
            FROM users
            WHERE user_id = $1
        """
        user_info = await db_manager.execute_query(
            user_query,
            params=(UUID(target_user_id),),
            fetch_one=True
        )
        
        if not user_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with ID {target_user_id} not found."
            )
        
        previous_count = user_info["count_of_camera"]
        target_username = user_info["username"]
        
        # Prevent modifying another admin's camera count (optional security check)
        if user_info["role"] == "admin" and str(admin_user_id) != target_user_id:
            logger.warning(
                f"Admin {admin_username} attempted to modify camera limit for another admin {target_username}"
            )
            # Uncomment to prevent admins from modifying other admins
            # raise HTTPException(
            #     status_code=status.HTTP_403_FORBIDDEN,
            #     detail="Cannot modify camera limits for other admin users."
            # )
        
        # Update the camera count
        update_query = """
            UPDATE users
            SET count_of_camera = $1
            WHERE user_id = $2
            RETURNING count_of_camera
        """
        
        result = await db_manager.execute_query(
            update_query,
            params=(update_data.count_of_camera, UUID(target_user_id)),
            fetch_one=True
        )
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update camera count."
            )
        
        # Log the action
        await session_manager.log_action(
            content=f"Admin '{admin_username}' updated camera limit for user '{target_username}' (ID: {target_user_id}) from {previous_count} to {update_data.count_of_camera}",
            user_id=str(admin_user_id),
            action_type="Updated_User_Camera_Limit",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown"),
            status="info"
        )
        
        return UserCameraCountResponse(
            user_id=target_user_id,
            username=target_username,
            count_of_camera=result["count_of_camera"],
            previous_count=previous_count,
            message=f"Camera limit updated successfully from {previous_count} to {result['count_of_camera']}"
        )
        
    except asyncpg.PostgresError as db_err:
        logger.error(
            f"Database error updating camera limit by admin {admin_username}: {db_err}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Database error occurred while updating camera limit."
        )
    except ValueError as ve:
        logger.error(
            f"Invalid data in update_user_camera_limit by admin {admin_username}: {ve}",
            exc_info=True
        )
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(
            f"Unexpected error in update_user_camera_limit by admin {admin_username}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while updating camera limit."
        )

@router.put("/admin/batch-user-camera-limit", status_code=status.HTTP_200_OK)
async def batch_update_user_camera_limits(
    updates: List[UserCameraCountUpdate],
    request: Request,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Update camera count limits for multiple users in batch.
    Only system admins can update camera limits.
    """
    admin_user_id = None
    admin_username = "unknown"
    
    try:
        # Verify admin privileges
        admin_user_id = current_admin_data["user_id"]
        admin_username = current_admin_data["username"]
        
        if current_admin_data.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="System admin privileges required to update camera limits."
            )
        
        if not updates:
            return {
                "message": "No updates provided.",
                "updated_users": [],
                "failed_users": []
            }
        
        updated_users = []
        failed_users = []
        
        for update_data in updates:
            try:
                # Validate user_id
                target_user_id = ensure_uuid_str(update_data.user_id)
                
                # Get current user info
                user_query = """
                    SELECT user_id, username, count_of_camera
                    FROM users
                    WHERE user_id = $1
                """
                user_info = await db_manager.execute_query(
                    user_query,
                    params=(UUID(target_user_id),),
                    fetch_one=True
                )
                
                if not user_info:
                    failed_users.append({
                        "user_id": target_user_id,
                        "reason": "User not found"
                    })
                    continue
                
                previous_count = user_info["count_of_camera"]
                
                # Update camera count
                update_query = """
                    UPDATE users
                    SET count_of_camera = $1
                    WHERE user_id = $2
                    RETURNING count_of_camera
                """
                
                result = await db_manager.execute_query(
                    update_query,
                    params=(update_data.count_of_camera, UUID(target_user_id)),
                    fetch_one=True
                )
                
                if result:
                    updated_users.append({
                        "user_id": target_user_id,
                        "username": user_info["username"],
                        "previous_count": previous_count,
                        "new_count": result["count_of_camera"]
                    })
                else:
                    failed_users.append({
                        "user_id": target_user_id,
                        "reason": "Update failed"
                    })
                    
            except Exception as e_user:
                logger.error(
                    f"Error updating camera limit for user {update_data.user_id}: {e_user}",
                    exc_info=True
                )
                failed_users.append({
                    "user_id": update_data.user_id,
                    "reason": str(e_user)
                })
        
        # Log the batch action
        log_content = f"Admin '{admin_username}' batch updated camera limits for {len(updates)} user(s). "
        log_content += f"Successful: {len(updated_users)}, Failed: {len(failed_users)}."
        
        await session_manager.log_action(
            content=log_content,
            user_id=str(admin_user_id),
            action_type="Batch_Updated_User_Camera_Limits",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown"),
            status="info" if not failed_users else "warning"
        )
        
        response_status = status.HTTP_200_OK
        if failed_users and not updated_users:
            response_status = status.HTTP_400_BAD_REQUEST
        elif failed_users:
            response_status = status.HTTP_207_MULTI_STATUS
        
        return Response(
            content=json.dumps({
                "message": f"Batch update completed. Updated: {len(updated_users)}, Failed: {len(failed_users)}",
                "updated_users": updated_users,
                "failed_users": failed_users
            }),
            status_code=response_status,
            media_type="application/json"
        )
        
    except asyncpg.PostgresError as db_err:
        logger.error(
            f"Database error in batch camera limit update by admin {admin_username}: {db_err}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Database error occurred during batch update."
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(
            f"Unexpected error in batch camera limit update by admin {admin_username}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during batch update."
        )

@router.get("/admin/user-camera-limit/{user_id}", status_code=status.HTTP_200_OK)
async def get_user_camera_limit(
    user_id: str,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the current camera count limit for a specific user.
    System admins can view any user's limit. Regular users can only view their own.
    """
    try:
        requesting_user_id = current_admin_data["user_id"]
        requesting_user_role = current_admin_data.get("role")
        
        # Validate user_id
        target_user_id = ensure_uuid_str(user_id)
        
        # Check permissions
        if requesting_user_role != "admin" and str(requesting_user_id) != target_user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only view your own camera limit."
            )
        
        # Get user information
        query = """
            SELECT user_id, username, email, count_of_camera, role, 
                   is_active, is_subscribed, created_at
            FROM users
            WHERE user_id = $1
        """
        user_info = await db_manager.execute_query(
            query,
            params=(UUID(target_user_id),),
            fetch_one=True
        )
        
        if not user_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with ID {target_user_id} not found."
            )
        
        # Get current camera count across all workspaces
        camera_count_query = """
            SELECT COUNT(*) as total_cameras
            FROM video_stream
            WHERE user_id = $1
        """
        camera_count = await db_manager.execute_query(
            camera_count_query,
            params=(UUID(target_user_id),),
            fetch_one=True
        )
        
        # Get camera counts per workspace
        workspace_camera_query = """
            SELECT vs.workspace_id, w.name as workspace_name, COUNT(*) as camera_count
            FROM video_stream vs
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.user_id = $1
            GROUP BY vs.workspace_id, w.name
            ORDER BY w.name
        """
        workspace_cameras = await db_manager.execute_query(
            workspace_camera_query,
            params=(UUID(target_user_id),),
            fetch_all=True
        )
        
        workspace_breakdown = []
        if workspace_cameras:
            workspace_breakdown = [
                {
                    "workspace_id": str(wc["workspace_id"]),
                    "workspace_name": wc["workspace_name"],
                    "camera_count": wc["camera_count"]
                }
                for wc in workspace_cameras
            ]
        
        return {
            "user_id": str(user_info["user_id"]),
            "username": user_info["username"],
            "email": user_info["email"],
            "count_of_camera": user_info["count_of_camera"],
            "current_cameras": camera_count["total_cameras"] if camera_count else 0,
            "remaining_limit": max(0, user_info["count_of_camera"] - (camera_count["total_cameras"] if camera_count else 0)),
            "workspace_breakdown": workspace_breakdown,
            "role": user_info["role"],
            "is_active": user_info["is_active"],
            "is_subscribed": user_info["is_subscribed"],
            "created_at": user_info["created_at"].isoformat() if user_info["created_at"] else None
        }
        
    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error getting user camera limit: {db_err}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Database error occurred while retrieving camera limit."
        )
    except ValueError as ve:
        logger.error(f"Invalid data in get_user_camera_limit: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in get_user_camera_limit: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while retrieving camera limit."
        )

@router.get("/admin/all-users-camera-limits", status_code=status.HTTP_200_OK)
async def get_all_users_camera_limits(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page"),
    sort_by: str = Query("username", pattern="^(username|email|count_of_camera|current_cameras|remaining_limit)$"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$"),
    filter_role: Optional[str] = Query(None, pattern="^(user|admin)$"),
    filter_subscribed: Optional[bool] = Query(None),
    search: Optional[str] = Query(None, description="Search by username or email"),
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get camera limits for all users with pagination, sorting, and filtering.
    Only system admins can access this endpoint.
    """
    try:
        # Verify admin privileges
        if current_admin_data.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="System admin privileges required."
            )
        
        # Build base query with camera counts
        conditions = []
        params = []
        param_count = 0
        
        # Apply filters
        if filter_role:
            param_count += 1
            conditions.append(f"u.role = ${param_count}")
            params.append(filter_role)
        
        if filter_subscribed is not None:
            param_count += 1
            conditions.append(f"u.is_subscribed = ${param_count}")
            params.append(filter_subscribed)
        
        if search:
            param_count += 1
            conditions.append(f"(u.username ILIKE ${param_count} OR u.email ILIKE ${param_count})")
            params.append(f"%{search}%")
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total
            FROM users u
            WHERE {where_clause}
        """
        total_result = await db_manager.execute_query(
            count_query,
            params=tuple(params),
            fetch_one=True
        )
        total_count = total_result["total"] if total_result else 0
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        offset = (page - 1) * per_page
        
        # Map sort_by to actual columns
        sort_column_map = {
            "username": "u.username",
            "email": "u.email",
            "count_of_camera": "u.count_of_camera",
            "current_cameras": "current_cameras",
            "remaining_limit": "remaining_limit"
        }
        sort_column = sort_column_map.get(sort_by, "u.username")
        
        # Get paginated data with camera counts
        data_query = f"""
            SELECT 
                u.user_id,
                u.username,
                u.email,
                u.count_of_camera,
                u.role,
                u.is_active,
                u.is_subscribed,
                u.created_at,
                COALESCE(cam_counts.total_cameras, 0) as current_cameras,
                GREATEST(0, u.count_of_camera - COALESCE(cam_counts.total_cameras, 0)) as remaining_limit
            FROM users u
            LEFT JOIN (
                SELECT user_id, COUNT(*) as total_cameras
                FROM video_stream
                GROUP BY user_id
            ) cam_counts ON u.user_id = cam_counts.user_id
            WHERE {where_clause}
            ORDER BY {sort_column} {sort_order.upper()}
            LIMIT ${param_count + 1} OFFSET ${param_count + 2}
        """
        params.extend([per_page, offset])
        
        users_data = await db_manager.execute_query(
            data_query,
            params=tuple(params),
            fetch_all=True
        )
        
        # Format response
        users_list = []
        if users_data:
            for user in users_data:
                users_list.append({
                    "user_id": str(user["user_id"]),
                    "username": user["username"],
                    "email": user["email"],
                    "count_of_camera": user["count_of_camera"],
                    "current_cameras": user["current_cameras"],
                    "remaining_limit": user["remaining_limit"],
                    "utilization_percentage": round(
                        (user["current_cameras"] / user["count_of_camera"] * 100) 
                        if user["count_of_camera"] > 0 else 0, 
                        2
                    ),
                    "role": user["role"],
                    "is_active": user["is_active"],
                    "is_subscribed": user["is_subscribed"],
                    "created_at": user["created_at"].isoformat() if user["created_at"] else None
                })
        
        return {
            "users": users_list,
            "pagination": {
                "current_page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "total_count": total_count
            },
            "sorting": {
                "sort_by": sort_by,
                "sort_order": sort_order
            },
            "filters": {
                "role": filter_role,
                "subscribed": filter_subscribed,
                "search": search
            }
        }
        
    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error getting all users camera limits: {db_err}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Database error occurred while retrieving camera limits."
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in get_all_users_camera_limits: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while retrieving camera limits."
        )

@router.get("/user/my-camera-limit", status_code=status.HTTP_200_OK)
async def get_my_camera_limit(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the current user's own camera limit and usage.
    Any authenticated user can access this endpoint for their own data.
    """
    try:
        user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        # Get user information
        query = """
            SELECT user_id, username, email, count_of_camera, role, 
                   is_active, is_subscribed, created_at
            FROM users
            WHERE user_id = $1
        """
        user_info = await db_manager.execute_query(
            query,
            params=(user_id,),
            fetch_one=True
        )
        
        if not user_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found."
            )
        
        # Get current camera count across all workspaces
        camera_count_query = """
            SELECT COUNT(*) as total_cameras
            FROM video_stream
            WHERE user_id = $1
        """
        camera_count = await db_manager.execute_query(
            camera_count_query,
            params=(user_id,),
            fetch_one=True
        )
        
        # Get camera counts per workspace
        workspace_camera_query = """
            SELECT vs.workspace_id, w.name as workspace_name, COUNT(*) as camera_count
            FROM video_stream vs
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.user_id = $1
            GROUP BY vs.workspace_id, w.name
            ORDER BY w.name
        """
        workspace_cameras = await db_manager.execute_query(
            workspace_camera_query,
            params=(user_id,),
            fetch_all=True
        )
        
        workspace_breakdown = []
        if workspace_cameras:
            workspace_breakdown = [
                {
                    "workspace_id": str(wc["workspace_id"]),
                    "workspace_name": wc["workspace_name"],
                    "camera_count": wc["camera_count"]
                }
                for wc in workspace_cameras
            ]
        
        current_cameras = camera_count["total_cameras"] if camera_count else 0
        camera_limit = user_info["count_of_camera"]
        remaining = max(0, camera_limit - current_cameras)
        
        return {
            "user_id": str(user_info["user_id"]),
            "username": user_info["username"],
            "email": user_info["email"],
            "count_of_camera": camera_limit,
            "current_cameras": current_cameras,
            "remaining_limit": remaining,
            "utilization_percentage": round(
                (current_cameras / camera_limit * 100) if camera_limit > 0 else 0, 
                2
            ),
            "workspace_breakdown": workspace_breakdown,
            "can_add_camera": remaining > 0,
            "is_active": user_info["is_active"],
            "is_subscribed": user_info["is_subscribed"]
        }
        
    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error getting user camera limit: {db_err}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Database error occurred while retrieving camera limit."
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in get_my_camera_limit: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while retrieving camera limit."
        )
    