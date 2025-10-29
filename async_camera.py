# async_camera.py 
################################
from fastapi import APIRouter, HTTPException, Depends, status, Request, Response, Query
from fastapi import UploadFile, File
import csv
import io
import pandas as pd
from typing import List, Optional, Any, Dict, Union
import logging
from uuid import UUID, uuid4
import os
import asyncpg # For asyncpg.PostgresError
import base64
import json
from schemas_models import (
    CameraStreamQueryParams, StreamCreate, StreamUpdate, StreamDelete, CameraState, 
    CamerasStateResponse, CameraBulkUploadResult, CameraCSVRecord, UserCameraCountUpdate, UserCameraCountResponse
)
from schemas_models import (
    LocationCreate, LocationUpdate, LocationResponse, 
    LocationSearchQuery, LocationHierarchyResponse, 
    CameraLocationGroup, CameraGroupResponse,
    StreamCreateWithLocation, StreamUpdateWithLocation,
    CameraCSVRecordWithLocation, BulkLocationAssignment,
    BulkLocationAssignmentResult, LocationStatsResponse
)
from schemas_models import (
    CameraAlertSettings, CameraCSVRecordWithLocationAndAlerts, BulkLocationAssignmentWithAlerts, 
    CameraLocationGroupWithAlerts, CameraGroupResponseWithAlerts, 
    LocationStatsResponseWithAlerts, CameraDetailedResponse,
    AlertSummaryResponse, CameraAlertUpdateResponse
)
from async_session_manager import SessionManager
from async_user_manager import UserManager
from async_database import DatabaseManager 
from async_video_streaming_qdrant import get_timestamp_range_, get_qdrant_client, get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists
from qdrant_client.http import models as qdrant_models # For Qdrant integration
from async_workspaces import get_user_and_workspace, check_workspace_membership_and_get_role 
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from async_utils import parse_string_or_list, encoded_string

logger = logging.getLogger(__name__)

router = APIRouter(tags=["camera"])

db_manager = DatabaseManager() 
session_manager = SessionManager() 
user_manager = UserManager() 

def ensure_uuid_str(id_value: Any) -> Optional[str]:
    if id_value is None: return None
    if isinstance(id_value, UUID): return str(id_value)
    if isinstance(id_value, str):
        try:
            return str(UUID(id_value)) # Validate and standardize
        except ValueError as ve:
            logger.error(f"Invalid UUID string format: {id_value}: {ve}", exc_info=True) 
            raise HTTPException(status_code=400, detail=f"Invalid ID format: {ve}")
        except Exception as e: 
            logger.error(f"Unexpected error validating UUID {id_value}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected error occurred during ID validation.")
    raise HTTPException(status_code=400, detail=f"Unsupported ID type: {type(id_value)}")

@router.post("/source", status_code=status.HTTP_201_CREATED)
async def create_stream(stream: StreamCreate, request: Request, current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None 
    workspace_id_obj = None
    username = "unknown"
    try:
        user_id_obj = current_user_data["user_id"] 
        username = current_user_data["username"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username) 
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None) 

        user_db_details = await user_manager.get_user_by_id(str(user_id_obj)) 
        allowed_camera_count = user_db_details.get("count_of_camera", 5)
        user_system_role = user_db_details.get("role", "user")

        if user_system_role != 'admin':
            count_query = "SELECT COUNT(*) as stream_count FROM video_stream WHERE user_id = $1 AND workspace_id = $2"
            count_result = await db_manager.execute_query(count_query, params=(user_id_obj, workspace_id_obj), fetch_one=True) 
            current_stream_count = count_result['stream_count'] if count_result else 0
            if current_stream_count >= allowed_camera_count:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Camera limit ({allowed_camera_count}) in workspace '{workspace_id_obj}' exceeded. You currently have {current_stream_count} cameras."
                )

        stream_id = uuid4()
        
        # Updated insert query to include all fields
        insert_query = """
            INSERT INTO video_stream 
            (stream_id, user_id, workspace_id, name, path, type, status, is_streaming, 
             location, area, building, floor_level, zone, latitude, longitude,
             count_threshold_greater, count_threshold_less, alert_enabled,
             created_at, updated_at, last_activity) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
        """
        
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        
        # Execute insert with all fields
        await db_manager.execute_query( 
            insert_query,
            params=(
                stream_id, 
                user_id_obj, 
                workspace_id_obj, 
                stream.name, 
                stream.path, 
                stream.type, 
                stream.status, 
                stream.is_streaming,
                getattr(stream, 'location', None),
                getattr(stream, 'area', None),
                getattr(stream, 'building', None),
                getattr(stream, 'floor_level', None),
                getattr(stream, 'zone', None),
                getattr(stream, 'latitude', None),
                getattr(stream, 'longitude', None),
                getattr(stream, 'count_threshold_greater', None),
                getattr(stream, 'count_threshold_less', None),
                getattr(stream, 'alert_enabled', False),
                now_utc, 
                now_utc, 
                now_utc
            )
        )
    
        await session_manager.log_action( 
                content=f"User '{username}' added Camera '{stream.name}' (ID: {stream_id}) to workspace (ID: {workspace_id_obj})",
                user_id=str(user_id_obj), 
                workspace_id=str(workspace_id_obj), 
                action_type="Added_Camera",
                ip_address=request.client.host if request.client else "Unknown",
                user_agent=request.headers.get("user-agent", "Unknown")
        )
        
        return {"message": "Stream created successfully", "id": str(stream_id)}

    except asyncpg.PostgresError as db_err:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Database error creating stream for user {log_user_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred while creating stream.")
    except ValueError as ve: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Invalid data for create stream for user {log_user_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data provided: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Unexpected error creating stream for user {log_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while creating stream.")
    
@router.get("/source/user")
async def get_user_streams(current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        
        if not workspace_id_obj:
             return [] 

        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")

        query_sql_base = """
            SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, 
                   vs.type, vs.status, vs.is_streaming, vs.created_at, vs.updated_at,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                   vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                   w.name as workspace_name 
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
        """
        params_sql_list = []
        
        if user_role_in_workspace == "admin":
            query_sql = query_sql_base + " WHERE vs.workspace_id = $1 ORDER BY u.username, vs.created_at DESC"
            params_sql_list.append(workspace_id_obj)
        else:
            query_sql = query_sql_base + " WHERE vs.user_id = $1 AND vs.workspace_id = $2 ORDER BY u.username, vs.created_at DESC"
            params_sql_list.extend([user_id_obj, workspace_id_obj])
            
        streams_data = await db_manager.execute_query(query_sql, params=tuple(params_sql_list), fetch_all=True)
        
        return [
            {
                "id": str(s["stream_id"]), 
                "user_id": str(s["user_id"]), 
                "owner_username": s["owner_username"],
                "name": s["name"], 
                "path": s["path"], 
                "type": s["type"], 
                "status": s["status"], 
                "is_streaming": s["is_streaming"],
                "location": s["location"],
                "area": s["area"],
                "building": s["building"],
                "floor_level": s["floor_level"],
                "zone": s["zone"],
                "latitude": float(s["latitude"]) if s["latitude"] else None,
                "longitude": float(s["longitude"]) if s["longitude"] else None,
                "count_threshold_greater": s["count_threshold_greater"],
                "count_threshold_less": s["count_threshold_less"],
                "alert_enabled": s["alert_enabled"],
                "created_at": s["created_at"].isoformat() if s["created_at"] else None,
                "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
                "workspace_id": str(workspace_id_obj), 
                "workspace_name": s["workspace_name"],
                "static_base64": encoded_string 
            } for s in streams_data
        ] if streams_data else []

    except asyncpg.PostgresError as db_err:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Database error retrieving streams for user {log_username}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred while retrieving streams.")
    except ValueError as ve:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Invalid data for get user streams for user {log_username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Unexpected error retrieving streams for user {log_username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while retrieving streams.")

@router.get("/source/users") 
async def get_all_streams(
    workspace_id_str: Optional[str] = Query(None, alias="workspaceId", description="ID of the workspace to get streams from. If provided, user must be admin of this workspace or a system admin."),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        user_system_role = current_user_data.get("role") 

        query_base = """
            SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, vs.type, vs.status, vs.is_streaming, 
                   vs.created_at, vs.updated_at, vs.workspace_id, w.name as workspace_name,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                   vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
        """
        params_list = []
        query_conditions = []
        order_by_clause = ""
        
        if workspace_id_str:
            target_workspace_id = ensure_uuid_str(workspace_id_str) 

            can_access_specific_workspace_streams = False
            if user_system_role == "admin":
                can_access_specific_workspace_streams = True
            else:
                try:
                    await check_workspace_membership_and_get_role(user_id_obj, UUID(target_workspace_id), required_role="admin")
                    can_access_specific_workspace_streams = True
                except HTTPException as e: 
                    raise e 

            if not can_access_specific_workspace_streams: 
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Access denied to streams in workspace {workspace_id_str}.")

            query_conditions.append(f"vs.workspace_id = ${len(params_list) + 1}")
            params_list.append(target_workspace_id)
            order_by_clause = " ORDER BY u.username, vs.created_at DESC"
        
        else: 
            if user_system_role != "admin":
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required to list all streams across all workspaces.")
            order_by_clause = " ORDER BY w.name, u.username, vs.created_at DESC"

        final_query = query_base
        if query_conditions:
            final_query += " WHERE " + " AND ".join(query_conditions)
        final_query += order_by_clause
        
        streams_data = await db_manager.execute_query(final_query, params=tuple(params_list), fetch_all=True)
        
        return [
            {
                "id": str(s["stream_id"]), "user_id": str(s["user_id"]), "owner_username": s["owner_username"],
                "name": s["name"], "path": s["path"], "type": s["type"], "status": s["status"], 
                "is_streaming": s["is_streaming"],
                "location": s["location"],
                "area": s["area"],
                "building": s["building"],
                "floor_level": s["floor_level"],
                "zone": s["zone"],
                "latitude": float(s["latitude"]) if s["latitude"] else None,
                "longitude": float(s["longitude"]) if s["longitude"] else None,
                "count_threshold_greater": s["count_threshold_greater"],
                "count_threshold_less": s["count_threshold_less"],
                "alert_enabled": s["alert_enabled"],
                "created_at": s["created_at"].isoformat() if s["created_at"] else None,
                "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
                "workspace_id": str(s["workspace_id"]) if s["workspace_id"] else None,
                "workspace_name": s["workspace_name"],
                "static_base64": encoded_string 
            } for s in streams_data
        ] if streams_data else []

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error retrieving all streams (admin/scoped): {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve: 
        logger.error(f"Invalid data in get_all_streams: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error retrieving all streams (admin/scoped): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.put("/source", status_code=status.HTTP_200_OK)
async def update_streams(streams: List[StreamUpdate], request: Request, current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    active_workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, active_workspace_id_obj = await get_user_and_workspace(username) 

        updated_ids, failed_ids = [], []

        for stream_update_data in streams:
            stream_id_to_update_str = ensure_uuid_str(stream_update_data.id) 
            
            stream_info_query = "SELECT user_id, workspace_id FROM video_stream WHERE stream_id = $1"
            stream_info = await db_manager.execute_query(stream_info_query, params=(UUID(stream_id_to_update_str),), fetch_one=True) 

            if not stream_info:
                logger.warning(f"Stream ID {stream_id_to_update_str} not found for update by {username}.")
                failed_ids.append(stream_id_to_update_str)
                continue
            
            stream_owner_id_obj = stream_info["user_id"] 
            stream_workspace_id_obj = stream_info["workspace_id"]

            can_update = False
            if stream_owner_id_obj == user_id_obj:
                can_update = True
            else:
                try:
                    await check_workspace_membership_and_get_role(user_id_obj, stream_workspace_id_obj, required_role="admin") 
                    can_update = True
                except HTTPException: 
                    pass 
            
            if not can_update and current_user_data.get("role") == "admin": 
                can_update = True

            if not can_update:
                logger.warning(f"User {username} (ID: {user_id_obj}) unauthorized to update stream ID: {stream_id_to_update_str}")
                failed_ids.append(stream_id_to_update_str)
                continue
            
            set_clauses_list, update_params_list = [], []
            param_idx = 1
            if stream_update_data.name is not None: 
                set_clauses_list.append(f"name = ${param_idx}"); 
                update_params_list.append(stream_update_data.name); 
                param_idx += 1
            if stream_update_data.path is not None: 
                set_clauses_list.append(f"path = ${param_idx}"); 
                update_params_list.append(stream_update_data.path); 
                param_idx += 1
            if stream_update_data.type is not None: 
                set_clauses_list.append(f"type = ${param_idx}"); 
                update_params_list.append(stream_update_data.type); 
                param_idx += 1
            if stream_update_data.status is not None: 
                set_clauses_list.append(f"status = ${param_idx}"); 
                update_params_list.append(stream_update_data.status); 
                param_idx += 1
            if stream_update_data.is_streaming is not None: 
                set_clauses_list.append(f"is_streaming = ${param_idx}"); 
                update_params_list.append(stream_update_data.is_streaming); 
                param_idx += 1
            
            # Add location fields if they exist in the update data
            if hasattr(stream_update_data, 'location') and stream_update_data.location is not None:
                set_clauses_list.append(f"location = ${param_idx}")
                update_params_list.append(stream_update_data.location)
                param_idx += 1
            if hasattr(stream_update_data, 'area') and stream_update_data.area is not None:
                set_clauses_list.append(f"area = ${param_idx}")
                update_params_list.append(stream_update_data.area)
                param_idx += 1
            if hasattr(stream_update_data, 'building') and stream_update_data.building is not None:
                set_clauses_list.append(f"building = ${param_idx}")
                update_params_list.append(stream_update_data.building)
                param_idx += 1
            if hasattr(stream_update_data, 'floor_level') and stream_update_data.floor_level is not None:
                set_clauses_list.append(f"floor_level = ${param_idx}")
                update_params_list.append(stream_update_data.floor_level)
                param_idx += 1
            if hasattr(stream_update_data, 'zone') and stream_update_data.zone is not None:
                set_clauses_list.append(f"zone = ${param_idx}")
                update_params_list.append(stream_update_data.zone)
                param_idx += 1
            if hasattr(stream_update_data, 'latitude') and stream_update_data.latitude is not None:
                set_clauses_list.append(f"latitude = ${param_idx}")
                update_params_list.append(stream_update_data.latitude)
                param_idx += 1
            if hasattr(stream_update_data, 'longitude') and stream_update_data.longitude is not None:
                set_clauses_list.append(f"longitude = ${param_idx}")
                update_params_list.append(stream_update_data.longitude)
                param_idx += 1
            
            # Add alert fields if they exist in the update data
            if hasattr(stream_update_data, 'count_threshold_greater') and stream_update_data.count_threshold_greater is not None:
                set_clauses_list.append(f"count_threshold_greater = ${param_idx}")
                update_params_list.append(stream_update_data.count_threshold_greater)
                param_idx += 1
            if hasattr(stream_update_data, 'count_threshold_less') and stream_update_data.count_threshold_less is not None:
                set_clauses_list.append(f"count_threshold_less = ${param_idx}")
                update_params_list.append(stream_update_data.count_threshold_less)
                param_idx += 1
            if hasattr(stream_update_data, 'alert_enabled') and stream_update_data.alert_enabled is not None:
                set_clauses_list.append(f"alert_enabled = ${param_idx}")
                update_params_list.append(stream_update_data.alert_enabled)
                param_idx += 1
            
            if not set_clauses_list: 
                logger.info(f"No update clauses for stream {stream_id_to_update_str}. Skipping DB update.")
                continue

            set_clauses_list.append(f"updated_at = ${param_idx}"); 
            update_params_list.append(datetime.now(ZoneInfo("Africa/Cairo"))); 
            param_idx += 1
            update_query_str = f"UPDATE video_stream SET {', '.join(set_clauses_list)} WHERE stream_id = ${param_idx}"
            update_params_list.append(UUID(stream_id_to_update_str)) 

            try:
                rows_affected = await db_manager.execute_query(update_query_str, params=tuple(update_params_list), return_rowcount=True) 
                if rows_affected > 0: 
                    updated_ids.append(stream_id_to_update_str)

                    # UPDATE QDRANT WITH ALL CHANGED FIELDS
                    qdrant_payload = {}
                    qdrant_update_needed = False
                    
                    # Check for name update
                    if stream_update_data.name is not None:
                        qdrant_payload["name"] = stream_update_data.name
                        qdrant_update_needed = True
                    
                    # Check for location field updates
                    if hasattr(stream_update_data, 'location') and stream_update_data.location is not None:
                        qdrant_payload["location"] = stream_update_data.location
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'area') and stream_update_data.area is not None:
                        qdrant_payload["area"] = stream_update_data.area
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'building') and stream_update_data.building is not None:
                        qdrant_payload["building"] = stream_update_data.building
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'floor_level') and stream_update_data.floor_level is not None:
                        qdrant_payload["floor_level"] = stream_update_data.floor_level
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'zone') and stream_update_data.zone is not None:
                        qdrant_payload["zone"] = stream_update_data.zone
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'latitude') and stream_update_data.latitude is not None:
                        qdrant_payload["latitude"] = float(stream_update_data.latitude)
                        qdrant_update_needed = True
                    if hasattr(stream_update_data, 'longitude') and stream_update_data.longitude is not None:
                        qdrant_payload["longitude"] = float(stream_update_data.longitude)
                        qdrant_update_needed = True
                    
                    # Update Qdrant if any relevant fields changed
                    if qdrant_update_needed:
                        try:
                            qdrant_client = get_qdrant_client()  #await 
                            collection_name = get_workspace_qdrant_collection_name(str(stream_workspace_id_obj)) 
                            await ensure_workspace_qdrant_collection_exists(qdrant_client, str(stream_workspace_id_obj)) #await 
                            
                            qdrant_client.set_payload( #await 
                                collection_name=collection_name,
                                payload=qdrant_payload, 
                                points=qdrant_models.Filter( 
                                must=[
                                    qdrant_models.FieldCondition(
                                        key="camera_id", 
                                        match=qdrant_models.MatchValue(value=stream_id_to_update_str)
                                    )
                                ]
                                )
                            )
                            
                            updated_fields = list(qdrant_payload.keys())
                            logger.info(f"Synchronized fields {updated_fields} in Qdrant for camera_id {stream_id_to_update_str} in collection '{collection_name}'.")
                        except Exception as e_qdrant:
                            logger.error(f"Failed to update Qdrant collection for stream {stream_id_to_update_str} field changes: {e_qdrant}", exc_info=True)
                            if stream_id_to_update_str in updated_ids: updated_ids.remove(stream_id_to_update_str) 
                            if stream_id_to_update_str not in failed_ids: failed_ids.append(stream_id_to_update_str) 
                            continue 
                else: 
                    logger.warning(f"Update to video_stream for ID {stream_id_to_update_str} affected 0 rows. Stream info was: {stream_info}")
                    if stream_id_to_update_str not in failed_ids: failed_ids.append(stream_id_to_update_str)
            except Exception as e_update: 
                logger.error(f"Error updating stream {stream_id_to_update_str} in DB: {e_update}", exc_info=True)
                if stream_id_to_update_str not in failed_ids: failed_ids.append(stream_id_to_update_str)

        log_content = f"User '{username}' attempted to update {len(streams)} camera(s)."
        if updated_ids: log_content += f" Successfully updated: {', '.join(list(set(updated_ids)))}." 
        if failed_ids: log_content += f" Failed/unauthorized for IDs: {', '.join(list(set(failed_ids)))}."
        
        await session_manager.log_action( 
            content=log_content, user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Updated_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        final_updated_ids = list(set(updated_ids))
        final_failed_ids = list(set(failed_ids))

        response_status_code = status.HTTP_200_OK
        response_detail_msg = f"Streams update attempt finished. Updated: {len(final_updated_ids)}."
        if final_failed_ids:
            response_status_code = status.HTTP_207_MULTI_STATUS if final_updated_ids else status.HTTP_400_BAD_REQUEST
            response_detail_msg += f" Failed/unauthorized for IDs: {', '.join(final_failed_ids)}."
        
        return Response(
            content=json.dumps({"detail": response_detail_msg, "updated_ids": final_updated_ids, "failed_ids": final_failed_ids}),
            status_code=response_status_code, media_type="application/json"
        )

    except asyncpg.PostgresError as db_err: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Database error in update_streams batch for user {log_user_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred during stream updates.")
    except ValueError as ve: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Invalid data in update_streams batch for user {log_user_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Unexpected error in update_streams batch for user {log_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during stream updates.")

@router.delete("/source", status_code=status.HTTP_200_OK)
async def delete_streams(stream_ids_payload: StreamDelete, request: Request, current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    active_workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, active_workspace_id_obj = await get_user_and_workspace(username) 

        if not stream_ids_payload.ids: return {"message": "No stream IDs provided for deletion."}

        valid_ids_to_delete, unauthorized_ids, not_found_ids = [], [], []
        # Store workspace info for Qdrant deletion
        workspace_camera_mapping = {}  # {workspace_id: [camera_ids]}

        for id_str_raw in stream_ids_payload.ids:
            try:
                id_to_delete_str = ensure_uuid_str(id_str_raw)
            except HTTPException: 
                unauthorized_ids.append(f"{id_str_raw} (invalid format)") 
                continue

            stream_info = await db_manager.execute_query("SELECT user_id, workspace_id FROM video_stream WHERE stream_id = $1", params=(UUID(id_to_delete_str),), fetch_one=True) 

            if not stream_info:
                not_found_ids.append(id_to_delete_str)
                continue
            
            stream_owner_id_obj = stream_info["user_id"]
            stream_workspace_id_obj = stream_info["workspace_id"]
            
            can_delete = False
            if stream_owner_id_obj == user_id_obj: 
                can_delete = True
            else:
                try:
                    await check_workspace_membership_and_get_role(user_id_obj, stream_workspace_id_obj, required_role="admin") 
                    can_delete = True
                except HTTPException: pass
            
            if not can_delete and current_user_data.get("role") == "admin": 
                can_delete = True

            if can_delete:
                valid_ids_to_delete.append(id_to_delete_str)
                # Group by workspace for efficient Qdrant deletion
                workspace_str = str(stream_workspace_id_obj)
                if workspace_str not in workspace_camera_mapping:
                    workspace_camera_mapping[workspace_str] = []
                workspace_camera_mapping[workspace_str].append(id_to_delete_str)
            else:
                unauthorized_ids.append(id_to_delete_str)
        
        deleted_count = 0
        qdrant_deletion_failures = []
        
        if valid_ids_to_delete:
            # Delete from PostgreSQL first
            placeholders = ", ".join([f"${i+1}" for i in range(len(valid_ids_to_delete))])
            delete_query = f"DELETE FROM video_stream WHERE stream_id IN ({placeholders})"
            
            uuid_params_for_delete = tuple(UUID(id_str) for id_str in valid_ids_to_delete) 
            deleted_count = await db_manager.execute_query(delete_query, params=uuid_params_for_delete, return_rowcount=True) 
            
            # Delete from Qdrant collections if PostgreSQL deletion was successful
            if deleted_count > 0:
                try:
                    qdrant_client = get_qdrant_client()
                    
                    for workspace_id_str, camera_ids in workspace_camera_mapping.items():
                        try:
                            collection_name = get_workspace_qdrant_collection_name(workspace_id_str)
                            
                            # Delete points for each camera_id in this workspace
                            for camera_id in camera_ids:
                                try:
                                    qdrant_client.delete(
                                        collection_name=collection_name,
                                        points_selector=qdrant_models.Filter(
                                            must=[
                                                qdrant_models.FieldCondition(
                                                    key="camera_id",
                                                    match=qdrant_models.MatchValue(value=camera_id)
                                                )
                                            ]
                                        ),
                                        wait=True
                                    )
                                    logger.info(f"Deleted Qdrant points for camera_id {camera_id} from collection '{collection_name}'.")
                                except Exception as e_camera:
                                    logger.error(f"Failed to delete Qdrant points for camera_id {camera_id} in collection '{collection_name}': {e_camera}", exc_info=True)
                                    qdrant_deletion_failures.append(camera_id)
                                    
                        except Exception as e_workspace:
                            logger.error(f"Failed to access Qdrant collection for workspace {workspace_id_str}: {e_workspace}", exc_info=True)
                            qdrant_deletion_failures.extend(camera_ids)
                            
                except Exception as e_qdrant:
                    logger.error(f"Failed to get Qdrant client for stream deletion: {e_qdrant}", exc_info=True)
                    qdrant_deletion_failures.extend(valid_ids_to_delete)
        
        log_content = f"User '{username}' attempted to delete {len(stream_ids_payload.ids)} camera(s)."
        if deleted_count > 0: log_content += f" Successfully deleted from DB: {', '.join(valid_ids_to_delete)}." 
        if qdrant_deletion_failures: log_content += f" Qdrant deletion failed for: {', '.join(qdrant_deletion_failures)}."
        if unauthorized_ids: log_content += f" Unauthorized/invalid for IDs: {', '.join(unauthorized_ids)}."
        if not_found_ids: log_content += f" Not found IDs: {', '.join(not_found_ids)}."

        await session_manager.log_action( 
            content=log_content, user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Deleted_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        response_message = f"Deletion attempt finished. Deleted from DB: {deleted_count}."
        if qdrant_deletion_failures:
            response_message += f" Warning: Qdrant deletion failed for {len(qdrant_deletion_failures)} camera(s)."

        return {
            "message": response_message,
            "deleted_ids": valid_ids_to_delete, 
            "unauthorized_ids": unauthorized_ids, 
            "not_found_ids": not_found_ids,
            "qdrant_deletion_failures": qdrant_deletion_failures
        }

    except asyncpg.PostgresError as db_err:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Database error in delete_streams batch for user {log_user_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred during stream deletion.")
    except ValueError as ve: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Invalid data in delete_streams batch for user {log_user_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Unexpected error in delete_streams batch for user {log_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during stream deletion.")

@router.delete("/source_v2", status_code=status.HTTP_200_OK)
async def delete_streams_v2(stream_ids_payload: StreamDelete, request: Request, current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    active_workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, active_workspace_id_obj = await get_user_and_workspace(username) 

        if not stream_ids_payload.ids: return {"message": "No stream IDs provided for deletion."}

        valid_ids_to_delete, unauthorized_ids, not_found_ids = [], [], []
        # Store workspace info for Qdrant deletion
        workspace_camera_mapping = {}  # {workspace_id: [camera_ids]}

        for id_str_raw in stream_ids_payload.ids:
            try:
                id_to_delete_str = ensure_uuid_str(id_str_raw)
            except HTTPException: 
                unauthorized_ids.append(f"{id_str_raw} (invalid format)") 
                continue

            stream_info = await db_manager.execute_query("SELECT user_id, workspace_id FROM video_stream WHERE stream_id = $1", params=(UUID(id_to_delete_str),), fetch_one=True) 

            if not stream_info:
                not_found_ids.append(id_to_delete_str)
                continue
            
            stream_owner_id_obj = stream_info["user_id"]
            stream_workspace_id_obj = stream_info["workspace_id"]
            
            can_delete = False
            if stream_owner_id_obj == user_id_obj: 
                can_delete = True
            else:
                try:
                    await check_workspace_membership_and_get_role(user_id_obj, stream_workspace_id_obj, required_role="admin") 
                    can_delete = True
                except HTTPException: pass
            
            if not can_delete and current_user_data.get("role") == "admin": 
                can_delete = True

            if can_delete:
                valid_ids_to_delete.append(id_to_delete_str)
                # Group by workspace for efficient Qdrant deletion
                workspace_str = str(stream_workspace_id_obj)
                if workspace_str not in workspace_camera_mapping:
                    workspace_camera_mapping[workspace_str] = []
                workspace_camera_mapping[workspace_str].append(id_to_delete_str)
            else:
                unauthorized_ids.append(id_to_delete_str)
        
        deleted_count = 0
        qdrant_deletion_failures = []
        successfully_deleted_from_postgres = []
        
        if valid_ids_to_delete:
            # First, try to delete from Qdrant to ensure data consistency
            # If Qdrant deletion fails, we'll log it but proceed with PostgreSQL deletion
            try:
                qdrant_client = get_qdrant_client()
                
                for workspace_id_str, camera_ids in workspace_camera_mapping.items():
                    try:
                        collection_name = get_workspace_qdrant_collection_name(workspace_id_str)
                        
                        # Delete points for each camera_id in this workspace
                        for camera_id in camera_ids:
                            try:
                                # Use async-friendly deletion approach
                                delete_result = qdrant_client.delete(
                                    collection_name=collection_name,
                                    points_selector=qdrant_models.Filter(
                                        must=[
                                            qdrant_models.FieldCondition(
                                                key="camera_id",
                                                match=qdrant_models.MatchValue(value=camera_id)
                                            )
                                        ]
                                    ),
                                    wait=True  # Wait for completion
                                )
                                
                                # Check if deletion was successful
                                if hasattr(delete_result, 'status') and delete_result.status == qdrant_models.UpdateStatus.COMPLETED:
                                    logger.info(f"Successfully deleted Qdrant points for camera_id {camera_id} from collection '{collection_name}'.")
                                else:
                                    logger.warning(f"Qdrant deletion status unclear for camera_id {camera_id}: {delete_result}")
                                    
                            except Exception as e_camera:
                                logger.error(f"Failed to delete Qdrant points for camera_id {camera_id} in collection '{collection_name}': {e_camera}", exc_info=True)
                                qdrant_deletion_failures.append(camera_id)
                                
                    except Exception as e_workspace:
                        logger.error(f"Failed to access Qdrant collection for workspace {workspace_id_str}: {e_workspace}", exc_info=True)
                        qdrant_deletion_failures.extend(camera_ids)
                        
            except Exception as e_qdrant:
                logger.error(f"Failed to get Qdrant client for stream deletion: {e_qdrant}", exc_info=True)
                qdrant_deletion_failures.extend(valid_ids_to_delete)
            
            # Now delete from PostgreSQL
            placeholders = ", ".join([f"${i+1}" for i in range(len(valid_ids_to_delete))])
            delete_query = f"DELETE FROM video_stream WHERE stream_id IN ({placeholders})"
            
            uuid_params_for_delete = tuple(UUID(id_str) for id_str in valid_ids_to_delete) 
            deleted_count = await db_manager.execute_query(delete_query, params=uuid_params_for_delete, return_rowcount=True) 
            
            if deleted_count > 0:
                successfully_deleted_from_postgres = valid_ids_to_delete.copy()
                logger.info(f"Successfully deleted {deleted_count} cameras from PostgreSQL")
            else:
                logger.warning("No cameras were deleted from PostgreSQL despite valid IDs being provided")
        
        # Prepare logging content
        log_content = f"User '{username}' attempted to delete {len(stream_ids_payload.ids)} camera(s)."
        if successfully_deleted_from_postgres: 
            log_content += f" Successfully deleted from PostgreSQL: {', '.join(successfully_deleted_from_postgres)}." 
        if qdrant_deletion_failures: 
            log_content += f" Qdrant deletion failed for: {', '.join(set(qdrant_deletion_failures))}."
        if unauthorized_ids: 
            log_content += f" Unauthorized/invalid for IDs: {', '.join(unauthorized_ids)}."
        if not_found_ids: 
            log_content += f" Not found IDs: {', '.join(not_found_ids)}."

        await session_manager.log_action( 
            content=log_content, user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Deleted_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        # Prepare response message
        response_message = f"Deletion attempt finished. Deleted from PostgreSQL: {deleted_count}."
        if qdrant_deletion_failures:
            unique_failures = list(set(qdrant_deletion_failures))
            response_message += f" Warning: Qdrant deletion failed for {len(unique_failures)} camera(s): {', '.join(unique_failures)}."

        # Determine response status
        response_status = status.HTTP_200_OK
        if qdrant_deletion_failures and not successfully_deleted_from_postgres:
            response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        elif qdrant_deletion_failures:
            response_status = status.HTTP_207_MULTI_STATUS  # Partial success

        return Response(
            content=json.dumps({
                "message": response_message,
                "deleted_from_postgres": successfully_deleted_from_postgres, 
                "postgres_deleted_count": deleted_count,
                "qdrant_deletion_failures": list(set(qdrant_deletion_failures)),
                "unauthorized_ids": unauthorized_ids, 
                "not_found_ids": not_found_ids,
                "summary": {
                    "total_requested": len(stream_ids_payload.ids),
                    "postgres_success": len(successfully_deleted_from_postgres),
                    "qdrant_failures": len(set(qdrant_deletion_failures)),
                    "unauthorized": len(unauthorized_ids),
                    "not_found": len(not_found_ids)
                }
            }),
            status_code=response_status,
            media_type="application/json"
        )

    except asyncpg.PostgresError as db_err:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Database error in delete_streams batch for user {log_user_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred during stream deletion.")
    except ValueError as ve: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Invalid data in delete_streams batch for user {log_user_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Unexpected error in delete_streams batch for user {log_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during stream deletion.")

@router.post("/param_stream", status_code=status.HTTP_201_CREATED) 
async def create_or_update_workspace_camera_params_post( 
    params: CameraStreamQueryParams, 
    request: Request, 
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username) 
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Cannot save parameters.")

        is_sys_admin = current_user_data.get("role") == "admin"
        if not is_sys_admin:
            await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        
        param_id = uuid4()
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))

        query = """
            INSERT INTO param_stream (param_id, user_id, workspace_id, frame_delay, frame_skip, conf, created_at, updated_at) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (workspace_id) DO UPDATE SET
                frame_delay = EXCLUDED.frame_delay,
                frame_skip = EXCLUDED.frame_skip,
                conf = EXCLUDED.conf,
                user_id = EXCLUDED.user_id,
                updated_at = EXCLUDED.updated_at
            RETURNING param_id, created_at, updated_at;
        """
        result = await db_manager.execute_query( 
            query,
            params=(param_id, user_id_obj, workspace_id_obj, params.frame_delay, params.frame_skip, params.conf, now_utc, now_utc),
            fetch_one=True
        )
        
        action_type_log = "Workspace_Param_Created" if result['created_at'] == result['updated_at'] else "Workspace_Param_Updated"

        await session_manager.log_action( 
            content=f"User '{username}' {action_type_log.split('_')[-1].lower()} parameters for workspace '{workspace_id_obj}'. Values: {params.model_dump()}",
            user_id=str(user_id_obj), workspace_id=str(workspace_id_obj), action_type=action_type_log,
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        return {
            "message": f"Parameters for workspace '{workspace_id_obj}' saved successfully.", 
            "param_id": str(result['param_id']),
            "workspace_id": str(workspace_id_obj)
        }

    except asyncpg.PostgresError as db_err:
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        log_user_id = str(user_id_obj) if user_id_obj else "unknown_user"
        logger.error(f"Database error saving param_stream for user {log_user_id}, ws {log_ws_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred while saving parameters.")
    except ValueError as ve: 
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        log_user_id = str(user_id_obj) if user_id_obj else "unknown_user"
        logger.error(f"Invalid data for param_stream for user {log_user_id}, ws {log_ws_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        log_user_id = str(user_id_obj) if user_id_obj else "unknown_user"
        logger.error(f"Unexpected error saving param_stream for user {log_user_id}, ws {log_ws_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while saving parameters.")

@router.put("/param_stream/user", status_code=status.HTTP_201_CREATED) 
async def create_or_update_workspace_camera_params_put( 
    params: CameraStreamQueryParams, 
    request: Request, 
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Cannot save parameters.")

        is_sys_admin = current_user_data.get("role") == "admin" 
        if not is_sys_admin:
            await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj)
        
        param_id = uuid4() 
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))

        query = """
            INSERT INTO param_stream (param_id, user_id, workspace_id, frame_delay, frame_skip, conf, created_at, updated_at) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (workspace_id) DO UPDATE SET
                frame_delay = EXCLUDED.frame_delay,
                frame_skip = EXCLUDED.frame_skip,
                conf = EXCLUDED.conf,
                user_id = EXCLUDED.user_id,
                updated_at = EXCLUDED.updated_at
            RETURNING param_id, created_at, updated_at;
        """ 
        result = await db_manager.execute_query(
            query,
            params=(param_id, user_id_obj, workspace_id_obj, params.frame_delay, params.frame_skip, params.conf, now_utc, now_utc),
            fetch_one=True
        )
        
        action_type_log = "Workspace_Param_Created" if result['created_at'] == result['updated_at'] else "Workspace_Param_Updated"

        await session_manager.log_action(
            content=f"User '{username}' {action_type_log.split('_')[-1].lower()} parameters for workspace '{workspace_id_obj}'. Values: {params.model_dump()}",
            user_id=str(user_id_obj), workspace_id=str(workspace_id_obj), action_type=action_type_log,
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent")
        )
        return {
            "message": f"Parameters for workspace '{workspace_id_obj}' saved successfully.", 
            "param_id": str(result['param_id']),
            "workspace_id": str(workspace_id_obj)
        }
    except ValueError: 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        log_ws_id = str(workspace_id_obj) if 'workspace_id_obj' in locals() and workspace_id_obj else "unknown_ws"
        log_user_id = str(user_id_obj) if 'user_id_obj' in locals() and user_id_obj else "unknown_user"
        logger.error(f"Error saving param_stream for user {log_user_id}, ws {log_ws_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save camera parameters.")

@router.get("/param_stream/user") 
async def get_active_workspace_params(current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username) 
        
        if not workspace_id_obj: 
            return { 
                "last_modifier_username": None,
                "frame_skip": 300, "conf": 0.4, 
                "created_at": None, "updated_at": None,
                "message": "No active workspace or parameters not set for active workspace. Returning system defaults."
            }
        
        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None) 
        
        query = """
            SELECT ps.param_id, ps.workspace_id, ps.user_id, u.username as last_modifier_username, 
                   ps.frame_delay, ps.frame_skip, ps.conf, ps.created_at, ps.updated_at 
            FROM param_stream ps
            JOIN users u ON ps.user_id = u.user_id
            WHERE ps.workspace_id = $1
        """
        params_data = await db_manager.execute_query(query, params=(workspace_id_obj,), fetch_one=True) 
        
        if not params_data: 
            return { 
                "last_modifier_username": None,
                "frame_skip": 300, "conf": 0.4, 
                "created_at": None, "updated_at": None,
            }

        return {
            "last_modifier_username": params_data["last_modifier_username"],
            "frame_skip": params_data["frame_skip"], 
            "conf": params_data["conf"],
            "created_at": params_data["created_at"].isoformat() if params_data["created_at"] else None,
            "updated_at": params_data["updated_at"].isoformat() if params_data["updated_at"] else None,
        }

    except asyncpg.PostgresError as db_err:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        logger.error(f"Database error retrieving workspace params for {log_username} (ws: {log_ws_id}): {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Invalid data for get active workspace params for {log_username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        logger.error(f"Unexpected error retrieving workspace params for {log_username} (ws: {log_ws_id}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.delete("/param_stream/user", status_code=status.HTTP_200_OK) 
async def delete_workspace_params(request: Request, current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)):
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username) 
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found to delete parameters for.")

        is_sys_admin = current_user_data.get("role") == "admin"
        if not is_sys_admin:
            await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role="admin") 
        
        query = "DELETE FROM param_stream WHERE workspace_id = $1"
        rows_affected = await db_manager.execute_query(query, params=(workspace_id_obj,), return_rowcount=True) 
        
        if rows_affected > 0:
            await session_manager.log_action( 
                content=f"User '{username}' deleted stream parameters for workspace (ID: {workspace_id_obj}).",
                user_id=str(user_id_obj), workspace_id=str(workspace_id_obj),
                action_type="Deleted_Workspace_Param_Stream",
                ip_address=request.client.host if request.client else "Unknown",
                user_agent=request.headers.get("user-agent", "Unknown")
            )
            return {"message": "Workspace stream parameters deleted successfully"}
        else:
            return {"message": "No stream parameters found for the active workspace to delete"}

    except asyncpg.PostgresError as db_err:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Database error deleting workspace parameters for user {log_user_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve: 
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Invalid data for delete workspace params for user {log_user_id}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_user_id = str(user_id_obj) if user_id_obj else "unknown"
        logger.error(f"Unexpected error deleting workspace parameters for user {log_user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

# Continue with remaining endpoints...
@router.put("/param_stream/users", status_code=status.HTTP_200_OK)
async def update_all_workspace_params(
    params_list_payload: List[CameraStreamQueryParams],
    request: Request,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_str_for_log = "unknown_admin"
    username_for_log = "unknown_admin_user"
    try:
        if current_admin_data.get("role") != 'admin':
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin privileges required.")
    
        user_id_str_for_log = str(current_admin_data["user_id"])
        username_for_log = current_admin_data["username"]
        updated_ids, failed_ids = [], []
        
        for params_item in params_list_payload: 
            if params_item.workspace_id is None:
                failed_ids.append(f"Unknown (missing workspace_id in item: {params_item.model_dump(exclude={'workspace_id'})})")
                continue

            workspace_id_str_item = ensure_uuid_str(params_item.workspace_id) 
            
            workspace_query = "SELECT workspace_id FROM workspaces WHERE workspace_id = $1"
            workspace_exists = await db_manager.execute_query(workspace_query, params=(UUID(workspace_id_str_item),), fetch_one=True)
            if not workspace_exists:
                failed_ids.append(f"{workspace_id_str_item} (workspace not found)")
                continue

            query_upsert = """
                INSERT INTO param_stream (param_id, user_id, workspace_id, frame_delay, frame_skip, conf, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (workspace_id) DO UPDATE SET
                    frame_delay = EXCLUDED.frame_delay,
                    frame_skip = EXCLUDED.frame_skip,
                    conf = EXCLUDED.conf,
                    user_id = EXCLUDED.user_id,
                    updated_at = EXCLUDED.updated_at
                RETURNING param_id
            """
            param_id_new = uuid4()
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            
            try:
                result = await db_manager.execute_query(
                    query_upsert,
                    params=(param_id_new, current_admin_data["user_id"], UUID(workspace_id_str_item), params_item.frame_delay, 
                            params_item.frame_skip, params_item.conf, now_utc, now_utc),
                    fetch_one=True
                )
                if result:
                    updated_ids.append(workspace_id_str_item)
                else: 
                    failed_ids.append(f"{workspace_id_str_item} (DB operation did not return ID)")
            except asyncpg.PostgresError as db_err_item: 
                logger.error(f"Database error updating param_stream for workspace {workspace_id_str_item} by admin {username_for_log}: {db_err_item}", exc_info=True)
                failed_ids.append(f"{workspace_id_str_item} (database error)")
                continue 

        log_content = f"Admin '{username_for_log}' attempted to update stream parameters for {len(params_list_payload)} workspace(s)."
        if updated_ids: log_content += f" Successfully updated: {', '.join(updated_ids)}."
        if failed_ids: log_content += f" Failed: {', '.join(failed_ids)}."

        await session_manager.log_action(
            content=log_content,
            user_id=user_id_str_for_log,
            action_type="Updated_All_Workspace_Params",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown"),
            status="critical" if failed_ids else "info" 
        )

        response_status_code = status.HTTP_200_OK
        response_detail_msg = f"Workspace parameters update attempt finished. Updated: {len(updated_ids)}."
        if failed_ids:
            response_status_code = status.HTTP_207_MULTI_STATUS if updated_ids else status.HTTP_400_BAD_REQUEST
            response_detail_msg += f" Failed for workspace IDs: {', '.join(failed_ids)}."

        return Response(
            content=json.dumps({
                "detail": response_detail_msg,
                "updated_workspace_ids": updated_ids,
                "failed_workspace_ids": failed_ids
            }),
            status_code=response_status_code,
            media_type="application/json"
        )

    except asyncpg.PostgresError as db_err: 
        logger.error(f"Database error in update_all_workspace_params for admin {username_for_log}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred during batch update.")
    except ValueError as ve: 
        logger.error(f"Invalid data in update_all_workspace_params for admin {username_for_log}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in update_all_workspace_params for admin {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during batch update.")

@router.get("/param_stream/users") 
async def get_all_workspace_params(current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_str_for_log = "unknown_admin"
    try:
        user_system_role = current_admin_data.get("role")
        if user_system_role != "admin":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required.")
        
        user_id_str_for_log = str(current_admin_data["user_id"]) 

        query = """
            SELECT ps.param_id, ps.workspace_id, w.name as workspace_name, 
                   ps.user_id as last_modifier_id, u.username as last_modifier_username,
                   ps.frame_delay, ps.frame_skip, ps.conf, ps.created_at, ps.updated_at 
            FROM param_stream ps
            JOIN workspaces w ON ps.workspace_id = w.workspace_id
            JOIN users u ON ps.user_id = u.user_id
            ORDER BY w.name, ps.created_at DESC
        """
        params_list_data = await db_manager.execute_query(query, fetch_all=True)
        
        return [
            {
                "param_id": str(p["param_id"]), "workspace_id": str(p["workspace_id"]),
                "workspace_name": p["workspace_name"],
                "last_modifier_id": str(p["last_modifier_id"]), "last_modifier_username": p["last_modifier_username"],
                "frame_delay": p["frame_delay"], "frame_skip": p["frame_skip"], "conf": p["conf"],
                "created_at": p["created_at"].isoformat() if p["created_at"] else None,
                "updated_at": p["updated_at"].isoformat() if p["updated_at"] else None
            } for p in params_list_data
        ] if params_list_data else []

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error retrieving all workspace params for admin {user_id_str_for_log}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve: 
        logger.error(f"Value error processing all workspace params for admin {user_id_str_for_log}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data processing: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error retrieving all workspace params for admin {user_id_str_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.delete("/param_stream/users", status_code=status.HTTP_200_OK) 
async def delete_all_workspace_params(request: Request, current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_str_for_log = "unknown_admin"
    username_for_log = "unknown_admin_user"
    try:
        user_system_role = current_admin_data.get("role")
        if user_system_role != "admin":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin role required.")

        user_id_str_for_log = str(current_admin_data["user_id"])
        username_for_log = current_admin_data["username"]

        query = "DELETE FROM param_stream" 
        rows_affected = await db_manager.execute_query(query, return_rowcount=True)

        await session_manager.log_action(
                content=f"Admin action by '{username_for_log}': Deleted all {rows_affected} workspace stream parameter entries.",
                user_id=user_id_str_for_log, action_type="Deleted_All_Workspace_Params", 
                ip_address=request.client.host if request.client else "Unknown",
                user_agent=request.headers.get("user-agent", "Unknown"), status="critical" 
        )
        return {"message": f"All ({rows_affected}) workspace stream parameters deleted successfully."}

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error deleting all workspace params for admin {username_for_log}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error deleting all workspace params for admin {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@router.get("/camera-state", response_model=CamerasStateResponse)
async def get_camera_state(current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return CamerasStateResponse(cameras=[], total_active=0, total_inactive=0, total_error=0, total_processing=0, total_cameras=0)

        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None) 

        query = """
            SELECT vs.stream_id, vs.name, vs.status, vs.is_streaming, u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1
        """
        camera_rows = await db_manager.execute_query(query, params=(workspace_id_obj,), fetch_all=True)
        
        cameras = [ CameraState(id=str(row["stream_id"]), name=f"{row['name']} (Owner: {row['owner_username']})", 
                                status=row["status"], is_streaming=row["is_streaming"]) 
                    for row in camera_rows ] if camera_rows else []
        
        active_count = sum(1 for cam in cameras if cam.status == "active")
        inactive_count = sum(1 for cam in cameras if cam.is_streaming is False or cam.status == "inactive") 
        error_count = sum(1 for cam in cameras if cam.status == "error")
        processing_count = sum(1 for cam in cameras if cam.status == "processing")

        return CamerasStateResponse(
            cameras=cameras, total_active=active_count, total_inactive=inactive_count,
            total_error=error_count, total_processing=processing_count, total_cameras=len(cameras)
        )

    except asyncpg.PostgresError as db_err:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        logger.error(f"Database error fetching camera state for {log_username} in {log_ws_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error fetching camera state.")
    except ValueError as ve: 
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Value error fetching camera state for {log_username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data processing camera state: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        log_ws_id = str(workspace_id_obj) if workspace_id_obj else "unknown_ws"
        logger.error(f"Unexpected error fetching camera state for {log_username} in {log_ws_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred fetching camera state.")

@router.get("/user_info")
async def get_current_user_info(current_user_data_dep: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_obj = None
    username = "unknown"
    active_workspace_id_for_params_str = None 
    try:
        user_id_obj = current_user_data_dep["user_id"] 
        username = current_user_data_dep["username"]
        
        user_base_info = await user_manager.get_user_by_id(str(user_id_obj)) 
        if not user_base_info:
            raise HTTPException(status_code=404, detail="User not found in DB.")

        active_workspace_params = None
        active_ws_id_obj_for_params_uuid = None 
        try:
            _user_id_from_ws_check, active_ws_id_obj_for_params_uuid = await get_user_and_workspace(username)
            if active_ws_id_obj_for_params_uuid:
                active_workspace_id_for_params_str = str(active_ws_id_obj_for_params_uuid)
                params_query_sql = """
                    SELECT frame_delay, frame_skip, conf, created_at, updated_at 
                    FROM param_stream WHERE workspace_id = $1
                """
                ws_params_db = await db_manager.execute_query(params_query_sql, params=(active_ws_id_obj_for_params_uuid,), fetch_one=True)
                if ws_params_db:
                    active_workspace_params = {
                        "frame_delay": ws_params_db["frame_delay"],
                        "frame_skip": ws_params_db["frame_skip"],
                        "conf": ws_params_db["conf"],
                        "created_at": ws_params_db["created_at"].isoformat() if ws_params_db["created_at"] else None,
                        "updated_at": ws_params_db["updated_at"].isoformat() if ws_params_db["updated_at"] else None,
                    }
        except HTTPException as e_ws: 
            logger.warning(f"Could not determine active workspace for {username} to fetch params: {e_ws.detail}")
        
        stream_count_for_active_ws = 0
        if active_workspace_id_for_params_str and active_ws_id_obj_for_params_uuid: 
            stream_count_q_sql = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1 AND workspace_id = $2"
            sc_res = await db_manager.execute_query(stream_count_q_sql, params=(user_id_obj, active_ws_id_obj_for_params_uuid), fetch_one=True)
            stream_count_for_active_ws = sc_res['count'] if sc_res else 0
        
        token_count_q_sql = "SELECT COUNT(*) as count FROM user_tokens WHERE user_id = $1 AND is_active = TRUE AND refresh_expires_at > $2"
        tc_res = await db_manager.execute_query(token_count_q_sql, params=(user_id_obj, datetime.now(ZoneInfo("Africa/Cairo"))), fetch_one=True)
        active_token_count = tc_res['count'] if tc_res else 0

        timestamp_range_result = {}
        if active_workspace_id_for_params_str: 
            try:
                timestamp_range_result = await get_timestamp_range_(
                    camera_id_param=None, 
                    workspace_id_for_search=active_workspace_id_for_params_str, 
                    current_user_data_for_filter=current_user_data_dep, 
                )
                if hasattr(timestamp_range_result, 'model_dump'): 
                    timestamp_range_result = timestamp_range_result.model_dump(exclude_none=True)
            except Exception as e_ts:
                 logger.error(f"Error fetching timestamp range for user {username} (active_ws: {active_workspace_id_for_params_str}): {e_ts}", exc_info=True)
                 timestamp_range_result = {"error": f"Failed to retrieve timestamp range: {str(e_ts)}"}
        
        user_data_response = {
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
            "active_workspace_id": active_workspace_id_for_params_str, 
            "active_workspace_stream_count": stream_count_for_active_ws, 
            "active_token_count": active_token_count,
            "active_workspace_stream_parameters": active_workspace_params, 
            "active_workspace_data_timestamp_range": timestamp_range_result
        }
        return user_data_response

    except asyncpg.PostgresError as db_err:
        log_username = username if username != "unknown" else current_user_data_dep.get("username", "unknown")
        logger.error(f"Database error retrieving user info for {log_username}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve: 
        log_username = username if username != "unknown" else current_user_data_dep.get("username", "unknown")
        logger.error(f"Value error retrieving user info for {log_username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_username = username if username != "unknown" else current_user_data_dep.get("username", "unknown")
        logger.error(f"Unexpected error retrieving user info for {log_username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while retrieving user information.")
                
@router.delete("/drop_tables", status_code=status.HTTP_200_OK) 
async def drop_all_tables_endpoint(request: Request, current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_str_for_log = "unknown_admin"
    username_for_log = "unknown_admin_user"
    try:
        if current_admin_data.get("role") != 'admin':
           logger.warning(f"Unauthorized attempt to drop tables by user: {current_admin_data.get('username', 'unknown_user')}")
           raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="System admin privileges required.")
    
        user_id_str_for_log = str(current_admin_data["user_id"])
        username_for_log = current_admin_data["username"]
        logger.critical(f"ADMIN ACTION: User '{username_for_log}' (ID: {user_id_str_for_log}) initiated DROP ALL TABLES.")
    
        from async_database import drop_all_tables as db_drop_all_tables_func 
        result = await db_drop_all_tables_func() 
        return result

    except asyncpg.PostgresError as db_err: 
        logger.error(f"Database error during drop_all_tables for admin {username_for_log}: {db_err}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error during table drop.")
    except HTTPException as e: 
        raise e
    except Exception as e:
        logger.error(f"Error during drop_all_tables endpoint operation by {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal error during table drop.")

@router.get("/param_stream/workspace/status") 
async def get_workspace_param_sync_status(current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"workspace_id": None, "message": "No active workspace found.", "current_parameters": None, "members": [], "total_members": 0} 

        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None) 

        ws_params_query_sql = """
            SELECT ps.param_id, ps.frame_delay, ps.frame_skip, ps.conf, 
                   ps.updated_at as params_last_updated, u.username as last_modifier_username
            FROM param_stream ps
            JOIN users u ON ps.user_id = u.user_id
            WHERE ps.workspace_id = $1
        """
        workspace_params_db = await db_manager.execute_query(ws_params_query_sql, params=(workspace_id_obj,), fetch_one=True) 

        current_workspace_parameters = None
        if workspace_params_db:
            current_workspace_parameters = { 
                "param_id": str(workspace_params_db["param_id"]),
                "frame_delay": workspace_params_db["frame_delay"],
                "frame_skip": workspace_params_db["frame_skip"],
                "conf": workspace_params_db["conf"],
                "last_updated_at": workspace_params_db["params_last_updated"].isoformat() if workspace_params_db["params_last_updated"] else None,
                "last_modified_by": workspace_params_db["last_modifier_username"]
            }
        else: 
             current_workspace_parameters = {
                "param_id": None, "frame_delay": 0.0, "frame_skip": 300, "conf": 0.4,
                "last_updated_at": None, "last_modified_by": None, "is_default": True
            }
        
        members_query_sql = """
            SELECT u.user_id, u.username, wm.role as workspace_role
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.workspace_id = $1 ORDER BY u.username
        """
        members_db = await db_manager.execute_query(members_query_sql, params=(workspace_id_obj,), fetch_all=True) 
        
        workspace_members_info = [{
            "user_id": str(m["user_id"]), "username": m["username"], "workspace_role": m["workspace_role"]
        } for m in members_db] if members_db else []
        
        return { 
            "workspace_id": str(workspace_id_obj),
            "current_parameters": current_workspace_parameters,
            "members": workspace_members_info,
            "total_members": len(workspace_members_info)
        }

    except asyncpg.PostgresError as db_err:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Database error getting workspace param status for {log_username}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except ValueError as ve: 
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Value error getting workspace param status for {log_username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data processing: {ve}")
    except HTTPException as he:
        raise he
    except Exception as e:
        log_username = username if username != "unknown" else current_user_data.get("username", "unknown")
        logger.error(f"Unexpected error getting workspace param status for {log_username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get workspace parameter status.")

# === CSV TEMPLATE WITH LOCATION AND ALERTS ===

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

# === ENHANCED BULK UPLOAD WITH LOCATION AND ALERT THRESHOLDS ===

@router.post("/source/bulk-upload-with-location", status_code=status.HTTP_200_OK)
async def bulk_upload_cameras_with_location(
    file: UploadFile = File(..., description="CSV file containing camera data with location and alert settings"),
    request: Request = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Bulk upload cameras from CSV file with location data and alert thresholds.
    
    Expected CSV format:
    name,path,type,status,is_streaming,location,area,building,floor_level,zone,latitude,longitude,count_threshold_greater,count_threshold_less,alert_enabled
    """
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")

        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)

        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV file")

        contents = await file.read()
        
        try:
            csv_content = contents.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            required_columns = ['name', 'path']
            if not all(col in csv_reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
                raise HTTPException(
                    status_code=400, 
                    detail=f"Missing required columns: {missing_cols}"
                )
            
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
                cleaned_row = {}
                for key, value in row.items():
                    cleaned_row[key] = str(value).strip() if value is not None else ""

                # Set defaults
                cleaned_row.setdefault('type', 'local')
                cleaned_row.setdefault('status', 'inactive')
                cleaned_row.setdefault('is_streaming', 'false')
                cleaned_row.setdefault('alert_enabled', 'false')
                
                # Location fields (optional)
                location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']
                for field in location_fields:
                    cleaned_row.setdefault(field, None)
                
                # Alert threshold fields (optional)
                cleaned_row.setdefault('count_threshold_greater', None)
                cleaned_row.setdefault('count_threshold_less', None)

                # Convert boolean fields
                is_streaming_str = cleaned_row['is_streaming'].lower()
                if is_streaming_str in ['true', '1', 'yes', 'on']:
                    cleaned_row['is_streaming'] = True
                elif is_streaming_str in ['false', '0', 'no', 'off', '']:
                    cleaned_row['is_streaming'] = False
                else:
                    row_errors.append(f"Invalid is_streaming value: {cleaned_row['is_streaming']}")

                alert_enabled_str = cleaned_row['alert_enabled'].lower()
                if alert_enabled_str in ['true', '1', 'yes', 'on']:
                    cleaned_row['alert_enabled'] = True
                elif alert_enabled_str in ['false', '0', 'no', 'off', '']:
                    cleaned_row['alert_enabled'] = False
                else:
                    row_errors.append(f"Invalid alert_enabled value: {cleaned_row['alert_enabled']}")

                # Validate required fields
                if not cleaned_row.get('name'):
                    row_errors.append("Name is required")
                if not cleaned_row.get('path'):
                    row_errors.append("Path is required")

                # Clean empty fields
                for field in location_fields:
                    if cleaned_row.get(field) == '':
                        cleaned_row[field] = None

                # Clean and validate threshold fields
                threshold_fields = ['count_threshold_greater', 'count_threshold_less']
                for field in threshold_fields:
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
                    failed_cameras.append({
                        "row": row_idx,
                        "data": cleaned_row,
                        "errors": row_errors
                    })
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
                existing_camera_query = """
                    SELECT stream_id FROM video_stream 
                    WHERE workspace_id = $1 AND name = $2
                """
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

                # Insert camera with location data and alert thresholds
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

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error in bulk upload with location and alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to perform bulk upload with location and alerts.")

# === BULK LOCATION ASSIGNMENT ===

@router.put("/cameras/bulk-location-assignment", response_model=BulkLocationAssignmentResult)
async def bulk_assign_camera_locations(
    assignment_data: BulkLocationAssignment,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Bulk assign location data to multiple cameras with alert threshold support."""
    user_id_obj = None
    username = "unknown"
    workspace_id_obj = None
    
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        
        updated_cameras = []
        failed_cameras = []
        errors = []
        
        for camera_id_str in assignment_data.camera_ids:
            try:
                # Verify camera exists and user has permission
                camera_check_query = """
                    SELECT vs.stream_id, vs.user_id, vs.name 
                    FROM video_stream vs
                    WHERE vs.stream_id = $1 AND vs.workspace_id = $2
                """
                camera_info = await db_manager.execute_query(
                    camera_check_query, 
                    (UUID(camera_id_str), workspace_id_obj), 
                    fetch_one=True
                )
                
                if not camera_info:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Camera not found in workspace"
                    })
                    continue
                
                # Check if user owns camera or is workspace admin
                user_role = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj)
                if camera_info["user_id"] != user_id_obj and user_role.get("role") != "admin":
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Permission denied"
                    })
                    continue
                
                # Build update query
                update_fields = []
                params = []
                param_count = 0
                
                if assignment_data.location is not None:
                    param_count += 1
                    update_fields.append(f"location = ${param_count}")
                    params.append(assignment_data.location)
                
                if assignment_data.area is not None:
                    param_count += 1
                    update_fields.append(f"area = ${param_count}")
                    params.append(assignment_data.area)
                
                if assignment_data.building is not None:
                    param_count += 1
                    update_fields.append(f"building = ${param_count}")
                    params.append(assignment_data.building)
                
                if assignment_data.floor_level is not None:
                    param_count += 1
                    update_fields.append(f"floor_level = ${param_count}")
                    params.append(assignment_data.floor_level)
                
                if assignment_data.zone is not None:
                    param_count += 1
                    update_fields.append(f"zone = ${param_count}")
                    params.append(assignment_data.zone)
                
                if assignment_data.latitude is not None:
                    param_count += 1
                    update_fields.append(f"latitude = ${param_count}")
                    params.append(assignment_data.latitude)
                
                if assignment_data.longitude is not None:
                    param_count += 1
                    update_fields.append(f"longitude = ${param_count}")
                    params.append(assignment_data.longitude)
                
                # Handle alert threshold fields
                if hasattr(assignment_data, 'count_threshold_greater') and assignment_data.count_threshold_greater is not None:
                    param_count += 1
                    update_fields.append(f"count_threshold_greater = ${param_count}")
                    params.append(assignment_data.count_threshold_greater)
                
                if hasattr(assignment_data, 'count_threshold_less') and assignment_data.count_threshold_less is not None:
                    param_count += 1
                    update_fields.append(f"count_threshold_less = ${param_count}")
                    params.append(assignment_data.count_threshold_less)
                
                if hasattr(assignment_data, 'alert_enabled') and assignment_data.alert_enabled is not None:
                    param_count += 1
                    update_fields.append(f"alert_enabled = ${param_count}")
                    params.append(assignment_data.alert_enabled)
                
                if not update_fields:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "No data provided for update"
                    })
                    continue
                
                # Add updated_at
                param_count += 1
                update_fields.append(f"updated_at = ${param_count}")
                params.append(datetime.now(ZoneInfo("Africa/Cairo")))
                
                # Add WHERE clause parameters
                param_count += 1
                params.append(UUID(camera_id_str))
                param_count += 1
                params.append(workspace_id_obj)
                
                update_query = f"""
                    UPDATE video_stream 
                    SET {', '.join(update_fields)}
                    WHERE stream_id = ${param_count - 1} AND workspace_id = ${param_count}
                """
                
                rows_affected = await db_manager.execute_query(
                    update_query, 
                    tuple(params), 
                    return_rowcount=True
                )
                
                if rows_affected > 0:
                    updated_cameras.append(camera_id_str)
                else:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Update failed"
                    })
                    
            except Exception as e:
                logger.error(f"Error updating camera {camera_id_str}: {e}", exc_info=True)
                failed_cameras.append({
                    "camera_id": camera_id_str,
                    "error": f"Update error: {str(e)}"
                })
                errors.append(f"Camera {camera_id_str}: {str(e)}")
        
        # Log the bulk assignment
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
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error in bulk location assignment: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to perform bulk location assignment.")

# === LOCATION MANAGEMENT ENDPOINTS ===

@router.get("/cameras/by-location")
async def get_cameras_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    group_by: str = Query("location", regex="^(location|area|building|floor_level|zone)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras grouped by location criteria."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return CameraGroupResponse(groups=[], total_groups=0, total_cameras=0, group_type=group_by)
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build query conditions
        conditions = ["vs.workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        if location:
            param_count += 1
            conditions.append(f"vs.location = ${param_count}")
            params.append(location)
        if area:
            param_count += 1
            conditions.append(f"vs.area = ${param_count}")
            params.append(area)
        if building:
            param_count += 1
            conditions.append(f"vs.building = ${param_count}")
            params.append(building)
        if zone:
            param_count += 1
            conditions.append(f"vs.zone = ${param_count}")
            params.append(zone)
        if floor_level:
            param_count += 1
            conditions.append(f"vs.floor_level = ${param_count}")
            params.append(floor_level)
        
        # Add user permission filter if not admin
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"vs.user_id = ${param_count}")
            params.append(user_id_obj)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                vs.{group_by} as group_name,
                COUNT(*) as camera_count,
                COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_count,
                COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_count,
                COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_count,
                COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_count,
                ARRAY_AGG(
                    JSON_BUILD_OBJECT(
                        'id', vs.stream_id::text,
                        'name', vs.name,
                        'status', vs.status,
                        'is_streaming', vs.is_streaming,
                        'location', vs.location,
                        'area', vs.area,
                        'building', vs.building,
                        'floor_level', vs.floor_level,
                        'zone', vs.zone,
                        'alert_enabled', vs.alert_enabled,
                        'count_threshold_greater', vs.count_threshold_greater,
                        'count_threshold_less', vs.count_threshold_less,
                        'owner_username', u.username
                    ) ORDER BY vs.name
                ) as cameras
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE {where_clause} AND vs.{group_by} IS NOT NULL
            GROUP BY vs.{group_by}
            ORDER BY vs.{group_by}
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        groups = []
        total_cameras = 0
        
        for result in results:
            cameras_data = result["cameras"] or []
            parsed_cameras = []
            
            for camera_json in cameras_data:
                if isinstance(camera_json, str):
                    try:
                        parsed_camera = json.loads(camera_json)
                        parsed_cameras.append(parsed_camera)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse camera JSON: {camera_json}")
                        continue
                elif isinstance(camera_json, dict):
                    parsed_cameras.append(camera_json)
                else:
                    logger.warning(f"Unexpected camera data type: {type(camera_json)}")
                    continue
            
            group = CameraLocationGroup(
                group_type=group_by,
                group_name=result["group_name"] or "Unknown",
                camera_count=result["camera_count"],
                cameras=parsed_cameras,
                active_count=result["active_count"] or 0,
                inactive_count=result["inactive_count"] or 0,
                streaming_count=result["streaming_count"] or 0,
                alert_enabled_count=result["alert_enabled_count"] or 0
            )
            groups.append(group)
            total_cameras += group.camera_count
        
        return CameraGroupResponse(
            groups=groups,
            total_groups=len(groups),
            total_cameras=total_cameras,
            group_type=group_by
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting cameras by location: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve cameras by location.")

@router.get("/locations/hierarchy", response_model=LocationHierarchyResponse)
async def get_location_hierarchy(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get the location hierarchy for the workspace."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return LocationHierarchyResponse(
                workspace_id="", hierarchy=[], total_locations=0,
                buildings=[], zones=[], areas=[], locations=[]
            )
        
        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        
        # Get hierarchy using the database function
        hierarchy_query = "SELECT * FROM get_location_hierarchy($1)"
        hierarchy_results = await db_manager.execute_query(hierarchy_query, (workspace_id_obj,), fetch_all=True)
        
        # Get unique values for each level
        floor_level = set()
        buildings = set()
        zones = set()
        areas = set()
        locations = set()
        
        hierarchy = []
        for row in hierarchy_results:
            hierarchy.append({
                "building": row["building"],
                "floor_level": row["floor_level"],
                "zone": row["zone"],
                "area": row["area"],
                "location": row["location"],
                "camera_count": row["camera_count"]
            })
            
            if row["floor_level"]:
                floor_level.add(row["floor_level"])
            if row["building"]:
                buildings.add(row["building"])
            if row["zone"]:
                zones.add(row["zone"])
            if row["area"]:
                areas.add(row["area"])
            if row["location"]:
                locations.add(row["location"])
        
        return LocationHierarchyResponse(
            workspace_id=str(workspace_id_obj),
            hierarchy=hierarchy,
            total_locations=len(hierarchy),
            buildings=sorted(list(buildings)),
            floor_level=sorted(list(floor_level)),
            zones=sorted(list(zones)),
            areas=sorted(list(areas)),
            locations=sorted(list(locations))
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting location hierarchy: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location hierarchy.")

# === LOCATION STATISTICS WITH ALERT INFO ===

@router.get("/locations/stats", response_model=List[LocationStatsResponseWithAlerts])
async def get_location_statistics(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get statistics for all locations in the workspace including alert information."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return []
        
        await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        
        # Enhanced stats query to include alert information
        stats_query = """
            SELECT 
                vs.workspace_id,
                w.name as workspace_name,
                vs.location,
                vs.area,
                vs.building,
                vs.floor_level,
                vs.zone,
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
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting location statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location statistics.")

# === CAMERA ALERT MANAGEMENT ENDPOINTS ===

@router.put("/cameras/{camera_id}/alert-settings")
async def update_camera_alert_settings(
    camera_id: str,
    alert_data: CameraAlertSettings,
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update alert settings for a specific camera."""
    try:
        user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Verify camera exists and user has permission
        camera_check_query = """
            SELECT vs.stream_id, vs.user_id, vs.name 
            FROM video_stream vs
            WHERE vs.stream_id = $1 AND vs.workspace_id = $2
        """
        camera_info = await db_manager.execute_query(
            camera_check_query, 
            (UUID(camera_id), workspace_id_obj), 
            fetch_one=True
        )
        
        if not camera_info:
            raise HTTPException(status_code=404, detail="Camera not found in workspace")
        
        # Check if user owns camera or is workspace admin
        user_role = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj)
        if camera_info["user_id"] != user_id_obj and user_role.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Permission denied")
        
        # Update alert settings
        update_query = """
            UPDATE video_stream 
            SET count_threshold_greater = $1,
                count_threshold_less = $2,
                alert_enabled = $3,
                updated_at = $4
            WHERE stream_id = $5 AND workspace_id = $6
        """
        
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        
        rows_affected = await db_manager.execute_query(
            update_query,
            params=(alert_data.count_threshold_greater, alert_data.count_threshold_less,
                   alert_data.alert_enabled, now_utc, UUID(camera_id), workspace_id_obj),
            return_rowcount=True
        )
        
        if rows_affected == 0:
            raise HTTPException(status_code=404, detail="Camera not found or no changes made")
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' updated alert settings for camera '{camera_info['name']}'. "
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
            "camera_id": camera_id,
            "camera_name": camera_info["name"],
            "alert_settings": alert_data.model_dump()
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error updating camera alert settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update camera alert settings.")

@router.get("/cameras/alert-summary")
async def get_alert_summary(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get a summary of all cameras with alert settings in the workspace."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"total_cameras": 0, "alert_enabled_cameras": 0, "cameras": []}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build query conditions based on user role
        if user_role_in_workspace == "admin":
            # Admin can see all cameras
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
            # Regular users can only see their own cameras
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
        
        camera_list = [
            {
                "camera_id": str(camera["stream_id"]),
                "name": camera["name"],
                "location": camera["location"],
                "area": camera["area"],
                "building": camera["building"],
                "floor_level": camera["floor_level"],
                "zone": camera["zone"],
                "status": camera["status"],
                "is_streaming": camera["is_streaming"],
                "alert_enabled": camera["alert_enabled"],
                "count_threshold_greater": camera["count_threshold_greater"],
                "count_threshold_less": camera["count_threshold_less"],
                "owner": camera["owner"]
            } for camera in cameras
        ]
        
        return {
            "total_cameras": len(cameras),
            "alert_enabled_cameras": alert_enabled_count,
            "cameras": camera_list
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting alert summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve alert summary.")

@router.get("/locations/list")
async def get_locations(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations for the current user in their workspace."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"locations": [], "total_count": 0}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build query based on user role
        if user_role_in_workspace == "admin":
            # Admin can see all locations in workspace
            query = """
                SELECT DISTINCT location, COUNT(*) as camera_count
                FROM video_stream 
                WHERE workspace_id = $1 AND location IS NOT NULL AND location != ''
                GROUP BY location
                ORDER BY location
            """
            params = (workspace_id_obj,)
        else:
            # Regular users see only their own camera locations
            query = """
                SELECT DISTINCT location, COUNT(*) as camera_count
                FROM video_stream 
                WHERE workspace_id = $1 AND user_id = $2 AND location IS NOT NULL AND location != ''
                GROUP BY location
                ORDER BY location
            """
            params = (workspace_id_obj, user_id_obj)
        
        results = await db_manager.execute_query(query, params, fetch_all=True)
        
        locations = [
            {
                "location": result["location"],
                "camera_count": result["camera_count"]
            } for result in results
        ]
        
        return {
            "locations": locations,
            "total_count": len(locations)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve locations.")

@router.get("/areas/list")
async def get_areas(
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique areas for the current user, optionally filtered by location(s)."""
    try:

        locations = parse_string_or_list(locations)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"areas": [], "total_count": 0, "filtered_by_locations": locations}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build base conditions
        conditions = ["workspace_id = $1", "area IS NOT NULL", "area != ''"]
        params = [workspace_id_obj]
        param_count = 1
        
        # Add user filter if not admin
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id_obj)
        
        # Add location filter if provided
        if locations:
            if isinstance(locations, str):
                locations = [locations]
            
            if locations:
                param_count += 1
                location_placeholders = ", ".join([f"${param_count + i}" for i in range(len(locations))])
                conditions.append(f"location IN ({location_placeholders})")
                params.extend(locations)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT DISTINCT area, location, COUNT(*) as camera_count
            FROM video_stream 
            WHERE {where_clause}
            GROUP BY area, location
            ORDER BY area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        areas = [
            {
                "area": result["area"],
                "location": result["location"],
                "camera_count": result["camera_count"]
            } for result in results
        ]
        
        return {
            "areas": areas,
            "total_count": len(areas),
            "filtered_by_locations": locations
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve areas.")

@router.get("/buildings/list")
async def get_buildings(
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique buildings for the current user, optionally filtered by area(s)."""
    try:
        areas = parse_string_or_list(areas)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"buildings": [], "total_count": 0, "filtered_by_areas": areas}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build base conditions
        conditions = ["workspace_id = $1", "building IS NOT NULL", "building != ''"]
        params = [workspace_id_obj]
        param_count = 1
        
        # Add user filter if not admin
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id_obj)
        
        # Add area filter if provided
        if areas:
            if isinstance(areas, str):
                areas = [areas]
            
            if areas:
                param_count += 1
                area_placeholders = ", ".join([f"${param_count + i}" for i in range(len(areas))])
                conditions.append(f"area IN ({area_placeholders})")
                params.extend(areas)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT DISTINCT building, area, location, COUNT(*) as camera_count
            FROM video_stream 
            WHERE {where_clause}
            GROUP BY building, area, location
            ORDER BY building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        buildings = [
            {
                "building": result["building"],
                "area": result["area"],
                "location": result["location"],
                "camera_count": result["camera_count"]
            } for result in results
        ]
        
        return {
            "buildings": buildings,
            "total_count": len(buildings),
            "filtered_by_areas": areas
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve buildings.")

@router.get("/floor-levels/list")
async def get_floor_levels(
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique floor levels for the current user, optionally filtered by building(s)."""
    try:
        buildings = parse_string_or_list(buildings)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"floor_levels": [], "total_count": 0, "filtered_by_buildings": buildings}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build base conditions
        conditions = ["workspace_id = $1", "floor_level IS NOT NULL", "floor_level != ''"]
        params = [workspace_id_obj]
        param_count = 1
        
        # Add user filter if not admin
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id_obj)
        
        # Add building filter if provided
        if buildings:
            if isinstance(buildings, str):
                buildings = [buildings]
            
            if buildings:
                param_count += 1
                building_placeholders = ", ".join([f"${param_count + i}" for i in range(len(buildings))])
                conditions.append(f"building IN ({building_placeholders})")
                params.extend(buildings)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT DISTINCT floor_level, building, area, location, COUNT(*) as camera_count
            FROM video_stream 
            WHERE {where_clause}
            GROUP BY floor_level, building, area, location
            ORDER BY floor_level, building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        floor_levels = [
            {
                "floor_level": result["floor_level"],
                "building": result["building"],
                "area": result["area"],
                "location": result["location"],
                "camera_count": result["camera_count"]
            } for result in results
        ]
        
        return {
            "floor_levels": floor_levels,
            "total_count": len(floor_levels),
            "filtered_by_buildings": buildings
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve floor levels.")

@router.get("/zones/list")
async def get_zones(
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique zones for the current user, optionally filtered by floor level(s)."""
    try:
        floor_levels = parse_string_or_list(floor_levels)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"zones": [], "total_count": 0, "filtered_by_floor_levels": floor_levels}
        
        membership_details = await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build base conditions
        conditions = ["workspace_id = $1", "zone IS NOT NULL", "zone != ''"]
        params = [workspace_id_obj]
        param_count = 1
        
        # Add user filter if not admin
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id_obj)
        
        # Add floor_level filter if provided
        if floor_levels:
            if isinstance(floor_levels, str):
                floor_levels = [floor_levels]
            
            if floor_levels:
                param_count += 1
                floor_level_placeholders = ", ".join([f"${param_count + i}" for i in range(len(floor_levels))])
                conditions.append(f"floor_level IN ({floor_level_placeholders})")
                params.extend(floor_levels)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT DISTINCT zone, floor_level, building, area, location, COUNT(*) as camera_count
            FROM video_stream 
            WHERE {where_clause}
            GROUP BY zone, floor_level, building, area, location
            ORDER BY zone, floor_level, building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        zones = [
            {
                "zone": result["zone"],
                "floor_level": result["floor_level"],
                "building": result["building"],
                "area": result["area"],
                "location": result["location"],
                "camera_count": result["camera_count"]
            } for result in results
        ]
        
        return {
            "zones": zones,
            "total_count": len(zones),
            "filtered_by_floor_levels": floor_levels
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting zones: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve zones.")


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
            # You can uncomment this to prevent admins from modifying other admins
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
        
        return {
            "user_id": str(user_info["user_id"]),
            "username": user_info["username"],
            "email": user_info["email"],
            "count_of_camera": user_info["count_of_camera"],
            "current_cameras": camera_count["total_cameras"] if camera_count else 0,
            "remaining_limit": max(0, user_info["count_of_camera"] - (camera_count["total_cameras"] if camera_count else 0)),
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
