################################
# async_camera.py (Revised)
################################
from fastapi import APIRouter, HTTPException, Depends, status, Request, Response, Query
from typing import List, Optional, Any, Dict
import logging
from uuid import UUID, uuid4
import os
import asyncpg # For asyncpg.PostgresError
import base64
import json
from schemas_models import CameraStreamQueryParams, StreamCreate, StreamUpdate, StreamDelete, CameraState, CamerasStateResponse
from async_session_manager import SessionManager
from async_user_manager import UserManager
from async_database import DatabaseManager 
from async_video_streaming_qdrant import get_timestamp_range_, get_qdrant_client, get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists
from qdrant_client.http import models as qdrant_models # For Qdrant integration
from async_workspaces import get_user_and_workspace, check_workspace_membership_and_get_role 
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

router = APIRouter(tags=["camera"])

db_manager = DatabaseManager() 
session_manager = SessionManager() 
user_manager = UserManager() 

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
        insert_query = """
            INSERT INTO video_stream 
            (stream_id, user_id, workspace_id, name, path, type, status, is_streaming, created_at, updated_at, last_activity) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """
        now_utc = datetime.now(timezone.utc)
        await db_manager.execute_query( 
            insert_query,
            params=(stream_id, user_id_obj, workspace_id_obj, stream.name, stream.path, stream.type, 
                    stream.status, stream.is_streaming, now_utc, now_utc, now_utc)
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
                   vs.created_at, vs.updated_at, vs.workspace_id, w.name as workspace_name
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
            
            if not set_clauses_list: 
                logger.info(f"No update clauses for stream {stream_id_to_update_str}. Skipping DB update.")
                continue

            set_clauses_list.append(f"updated_at = ${param_idx}"); 
            update_params_list.append(datetime.now(timezone.utc)); 
            param_idx += 1
            update_query_str = f"UPDATE video_stream SET {', '.join(set_clauses_list)} WHERE stream_id = ${param_idx}"
            update_params_list.append(UUID(stream_id_to_update_str)) 

            try:
                rows_affected = await db_manager.execute_query(update_query_str, params=tuple(update_params_list), return_rowcount=True) 
                if rows_affected > 0: 
                    updated_ids.append(stream_id_to_update_str)

                    if stream_update_data.name is not None: 
                        try:
                            qdrant_client = get_qdrant_client()  #await 
                            collection_name = get_workspace_qdrant_collection_name(str(stream_workspace_id_obj)) 
                            ensure_workspace_qdrant_collection_exists(qdrant_client, str(stream_workspace_id_obj)) #await 
                            
                            qdrant_client.set_payload( #await 
                                collection_name=collection_name,
                                payload={"name": stream_update_data.name}, 
                                points=qdrant_models.Filter( 
                                must=[
                                    qdrant_models.FieldCondition(
                                        key="camera_id", 
                                        match=qdrant_models.MatchValue(value=stream_id_to_update_str)
                                    )
                                ]
                                )
                            )
                            logger.info(f"Synchronized name in Qdrant for camera_id {stream_id_to_update_str} to '{stream_update_data.name}' in collection '{collection_name}'.")
                        except Exception as e_qdrant:
                            logger.error(f"Failed to update Qdrant collection for stream {stream_id_to_update_str} name change: {e_qdrant}", exc_info=True)
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
            else:
                unauthorized_ids.append(id_to_delete_str)
        
        deleted_count = 0
        if valid_ids_to_delete:
            placeholders = ", ".join([f"${i+1}" for i in range(len(valid_ids_to_delete))])
            delete_query = f"DELETE FROM video_stream WHERE stream_id IN ({placeholders})"
            
            uuid_params_for_delete = tuple(UUID(id_str) for id_str in valid_ids_to_delete) 
            deleted_count = await db_manager.execute_query(delete_query, params=uuid_params_for_delete, return_rowcount=True) 
        
        log_content = f"User '{username}' attempted to delete {len(stream_ids_payload.ids)} camera(s)."
        if deleted_count > 0: log_content += f" Successfully deleted: {', '.join(valid_ids_to_delete)}." 
        if unauthorized_ids: log_content += f" Unauthorized/invalid for IDs: {', '.join(unauthorized_ids)}."
        if not_found_ids: log_content += f" Not found IDs: {', '.join(not_found_ids)}."

        await session_manager.log_action( 
            content=log_content, user_id=str(user_id_obj),
            workspace_id=str(active_workspace_id_obj) if active_workspace_id_obj else None,
            action_type="Deleted_Cameras_Batch",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )

        return {
            "message": f"Deletion attempt finished. Deleted: {deleted_count}.",
            "deleted_ids": valid_ids_to_delete, 
            "unauthorized_ids": unauthorized_ids, 
            "not_found_ids": not_found_ids
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
        now_utc = datetime.now(timezone.utc)

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

@router.put("/param_stream/user", status_code=status.HTTP_201_CREATED) # Changed to PUT to match original sync intent
async def create_or_update_workspace_camera_params_put( # Renamed to avoid conflict
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
            await check_workspace_membership_and_get_role(user_id_obj, workspace_id_obj)#, required_role="admin"
        
        param_id = uuid4() 
        now_utc = datetime.now(timezone.utc)

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
                "param_id": None, "workspace_id": None, "last_modified_by_user_id": None,
                "frame_delay": 0.0, "frame_skip": 2, "conf": 0.4, 
                "created_at": None, "updated_at": None, "is_default": True,
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
                # "param_id": None, "workspace_id": str(workspace_id_obj),"last_modified_by_user_id": None, 
                "last_modifier_username": None,
                # "frame_delay": 0.0, 
                "frame_skip": 2, "conf": 0.4, 
                "created_at": None, "updated_at": None, 
                # "is_default": True
            }

        return {
            # "param_id": str(params_data["param_id"]),
            # "workspace_id": str(params_data["workspace_id"]),
            # "last_modified_by_user_id": str(params_data["user_id"]),
            "last_modifier_username": params_data["last_modifier_username"],
            # "frame_delay": params_data["frame_delay"], 
            "frame_skip": params_data["frame_skip"], 
            "conf": params_data["conf"],
            "created_at": params_data["created_at"].isoformat() if params_data["created_at"] else None,
            "updated_at": params_data["updated_at"].isoformat() if params_data["updated_at"] else None,
            # "is_default": False 
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

@router.put("/param_stream/users", status_code=status.HTTP_200_OK)
async def update_all_workspace_params(
    params_list_payload: List[CameraStreamQueryParams], # Renamed to avoid conflict with params var used later
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
            if params_item.workspace_id is None: # Check if workspace_id is provided in the item
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
            now_utc = datetime.now(timezone.utc)
            
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
        params_list_data = await db_manager.execute_query(query, fetch_all=True) # No params for this query
        
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
        rows_affected = await db_manager.execute_query(query, return_rowcount=True) # No params

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
        tc_res = await db_manager.execute_query(token_count_q_sql, params=(user_id_obj, datetime.now(timezone.utc)), fetch_one=True)
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
        await session_manager.log_action(
            content=f"Admin '{username_for_log}' executed DROP ALL TABLES. Result: {result.get('message', 'No message')}",
            user_id=user_id_str_for_log, action_type="Admin_Drop_All_Tables", status="critical",
            ip_address=request.client.host if request.client else "Unknown", 
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        return result

    except asyncpg.PostgresError as db_err: 
        logger.error(f"Database error during drop_all_tables for admin {username_for_log}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content=f"Admin '{username_for_log}' attempt to DROP ALL TABLES failed with database error.",
            user_id=user_id_str_for_log, action_type="Admin_Drop_All_Tables_Error", status="critical",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error during table drop.")
    except HTTPException as e: 
        await session_manager.log_action(
            content=f"Admin '{username_for_log}' attempt to DROP ALL TABLES failed. Reason: {e.detail}",
            user_id=user_id_str_for_log, action_type="Admin_Drop_All_Tables_Failed", status="critical",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
        raise e
    except Exception as e:
        logger.error(f"Error during drop_all_tables endpoint operation by {username_for_log}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Admin '{username_for_log}' attempt to DROP ALL TABLES failed with unexpected error: {str(e)}",
            user_id=user_id_str_for_log, action_type="Admin_Drop_All_Tables_Error", status="critical",
            ip_address=request.client.host if request.client else "Unknown",
            user_agent=request.headers.get("user-agent", "Unknown")
        )
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
                "param_id": None, "frame_delay": 0.0, "frame_skip": 2, "conf": 0.4,
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
        