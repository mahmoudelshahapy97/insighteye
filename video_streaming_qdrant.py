# video_streaming_qdrant.py
from fastapi import APIRouter, HTTPException, Query, Request, Depends, status
from fastapi.responses import JSONResponse
from collections import defaultdict
from typing import Optional, List, Union, Dict, Any, Tuple, Set
from uuid import UUID, uuid4
import asyncio
from config import config
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models 
from datetime import datetime, time as dt_time, timezone 
from zoneinfo import ZoneInfo
import logging

# Updated imports for asynchronous managers
from user_manager import UserManager as AsyncUserManager
from session_manager import SessionManager as AsyncSessionManager
from database import DatabaseManager as AsyncDatabaseManager

from schemas_models import CreateCollectionRequest, SearchQuery, TimestampRangeResponse, CameraIdsResponse, DeleteDataRequest, LocationSearchQuery, CameraLocationGroup
from utils import (
    parse_camera_ids, make_prediction, parse_date_format, parse_time_string,
    get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists
)
from utils import parse_string_or_list

logger = logging.getLogger(__name__)
BASE_QDRANT_COLLECTION_NAME = config.get("qdrant_collection_name", "person_counts")
router = APIRouter(tags=["qdrant_data"]) 

# Initialize new asynchronous managers
session_manager_global_qdrant = AsyncSessionManager() 
user_manager_global_qdrant = AsyncUserManager() 
db_manager_global_qdrant = AsyncDatabaseManager() 

qdrant_client: Optional[QdrantClient] = None

def get_qdrant_client() -> QdrantClient:
    global qdrant_client
    if qdrant_client is None:
        qdrant_client = QdrantClient(
            url=config.get("qdrant_url", "localhost"),
            port=config.get("qdrant_port", 6333),
            timeout=config.get("qdrant_timeout", 60.0) 
        )
        logger.info("Qdrant client initialized.")
    return qdrant_client

@router.on_event("startup")
async def on_startup_qdrant_router():
    """
    Initializes the Qdrant client.
    Note: Database pool initialization (init_db_pool from database.py)
    should be handled by the main FastAPI application's startup event
    to resolve warnings like "DB pool not available".
    """
    get_qdrant_client()

async def get_user_and_workspace_refined(username: str, 
                                         user_manager: AsyncUserManager
                                         ) -> Tuple[Optional[Dict], Optional[UUID]]:
    """
    Gets user details and their active workspace ID.
    (Placeholder, ideally uses fully implemented UserManager.get_active_workspace)
    """
    user_details = await user_manager.get_user_by_username(username)
    if not user_details:
        logger.debug(f"User '{username}' not found by get_user_and_workspace_refined.")
        return None, None
    
    user_id_obj = user_details.get("user_id") # Should be UUID from user_manager
    if not user_id_obj:
        logger.error(f"User details for '{username}' missing user_id.")
        return user_details, None

    active_workspace_info = await user_manager.get_active_workspace(user_id_obj) # Pass UUID
    
    if active_workspace_info and isinstance(active_workspace_info.get("workspace_id"), UUID):
        return user_details, active_workspace_info["workspace_id"]
    logger.debug(f"No active workspace found for user '{username}' (ID: {user_id_obj}) by get_active_workspace.")
    return user_details, None

async def check_workspace_membership_and_get_role_async(
    user_id: UUID, 
    workspace_id: UUID, 
    db_manager: AsyncDatabaseManager, 
    required_role: Optional[str] = None
) -> Dict[str, str]:
    """
    Checks if a user is a member of a workspace and retrieves their role.
    Optionally checks if the user has a required role.
    (Placeholder, actual implementation would be in workspaces.py)
    """
    query = "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
    member_info = await db_manager.execute_query(query, (user_id, workspace_id), fetch_one=True)

    if not member_info:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: You are not a member of this workspace."
        )

    user_workspace_role = member_info['role']

    # Role hierarchy: owner > admin > member. 'editor', 'viewer' could also exist.
    # For simplicity, assume owner implies admin, and admin implies member capabilities.
    if required_role:
        has_permission = False
        if required_role == user_workspace_role:
            has_permission = True
        elif required_role == 'member' and user_workspace_role in ['admin', 'owner']:
            has_permission = True
        elif required_role == 'admin' and user_workspace_role == 'owner':
            has_permission = True
        # Add other role implications as needed by the application's RBAC model

        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: You do not have the required '{required_role}' role in this workspace. Your role: '{user_workspace_role}'."
            )
    return {"role": user_workspace_role}

# --- Qdrant Filter Building Logic (No changes needed for async DB) ---
def build_filter_from_query(
    query: SearchQuery,
    user_system_role: Optional[str] = None,
    user_workspace_role: Optional[str] = None,
    requesting_username: Optional[str] = None,
) -> Optional[qdrant_models.Filter]:
    must_conditions: List[Union[qdrant_models.FieldCondition, qdrant_models.Filter]] = []
    logger.debug(f"Building filter from query: {query.model_dump(exclude_none=True)}, sys_role: {user_system_role}, ws_role: {user_workspace_role}, user: {requesting_username}")

    if query.camera_id:
        if not isinstance(query.camera_id, list):
            logger.error(f"camera_id is not a list: {query.camera_id}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="camera_id must be a list")
        str_camera_ids = [str(cid) for cid in query.camera_id if cid] # Ensure UUIDs are strings for Qdrant match
        if not str_camera_ids: # If list was provided but all items were empty/None
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid camera_id provided in the list.")

        if len(str_camera_ids) == 1:
            must_conditions.append(qdrant_models.FieldCondition(key="camera_id", match=qdrant_models.MatchValue(value=str_camera_ids[0])))
        else:
            must_conditions.append(qdrant_models.Filter(should=[
                qdrant_models.FieldCondition(key="camera_id", match=qdrant_models.MatchValue(value=cam_id)) for cam_id in str_camera_ids
            ]))

    start_timestamp: Optional[float] = None
    end_timestamp: Optional[float] = None
    try:
        if query.start_date:
            start_date_obj = parse_date_format(query.start_date)
            start_time_obj = parse_time_string(query.start_time, dt_time.min)
            start_datetime = datetime.combine(start_date_obj, start_time_obj).replace(tzinfo=timezone.utc)
            start_timestamp = start_datetime.timestamp()
        if query.end_date:
            end_date_obj = parse_date_format(query.end_date)
            end_time_obj = parse_time_string(query.end_time, dt_time.max.replace(microsecond=0))
            end_datetime = datetime.combine(end_date_obj, end_time_obj).replace(tzinfo=timezone.utc)
            end_timestamp = end_datetime.timestamp()

        if start_timestamp is not None or end_timestamp is not None:
            timestamp_range_filter: Dict[str, Any] = {}
            if start_timestamp is not None: timestamp_range_filter["gte"] = start_timestamp
            if end_timestamp is not None: timestamp_range_filter["lte"] = end_timestamp
            if timestamp_range_filter:
                must_conditions.append(qdrant_models.FieldCondition(key="timestamp", range=qdrant_models.Range(**timestamp_range_filter)))
    except ValueError as date_err:
        logger.error(f"Invalid date format provided for filtering: {date_err}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid date/time format: {date_err}")

    apply_username_filter = True
    if user_system_role == 'admin':
        apply_username_filter = False
        logger.debug("System admin access: no username filter applied in Qdrant query.")
    elif user_workspace_role in ['admin', 'owner']: # Workspace admin/owner sees all in that workspace
        apply_username_filter = False
        logger.debug(f"Workspace admin/owner access: no username filter applied in Qdrant query for this workspace.")

    if apply_username_filter:
        if requesting_username:
            must_conditions.append(qdrant_models.FieldCondition(key="username", match=qdrant_models.MatchValue(value=requesting_username)))
            logger.debug(f"Regular member access: applying filter for username='{requesting_username}' in Qdrant query.")
        else: # Safeguard: non-admin context but no username provided
            logger.error("CRITICAL SAFEGUARD: Attempting to apply username filter for non-admin, but requesting_username is missing. Applying 'match nothing' filter.")
            must_conditions.append(qdrant_models.FieldCondition(key="username", match=qdrant_models.MatchValue(value=f"__IMPOSSIBLE_USERNAME_{uuid4()}__")))
    
    if not must_conditions: return None
    return qdrant_models.Filter(must=must_conditions)

@router.get("/workspace/search_results_v2")
async def workspace_search_results_v2(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1), per_page: Optional[str] = Query(None),
    base64: bool = Query(True),  # New parameter with default True
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    final_workspace_id_obj: Optional[UUID] = None
    try:
        requesting_user_id_obj = current_user_data["user_id"] # This is UUID from session_manager
        username = current_user_data["username"]
        user_db_info = await user_manager_global_qdrant.get_user_by_id(requesting_user_id_obj) # Pass UUID
        if not user_db_info: raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")

        if workspace_id_query:
            try: final_workspace_id_obj = UUID(workspace_id_query)
            except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
            if not active_ws_id_obj:
                 raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found. Please specify workspaceId or activate one.")
            final_workspace_id_obj = active_ws_id_obj

        workspace_specific_role = None
        if not is_system_admin: # Non-system admin
            try:
                member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, final_workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access: # Catches if not member
                logger.warning(f"Access denied for user {username} to workspace {final_workspace_id_obj}: {e_ws_access.detail}")
                raise e_ws_access
        else: # System admin, find their role in this specific workspace if they are a member
            try:
                ws_member_info_for_sysadmin = await db_manager_global_qdrant.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id_obj, final_workspace_id_obj), fetch_one=True # Pass UUIDs
                )
                if ws_member_info_for_sysadmin:
                    workspace_specific_role = ws_member_info_for_sysadmin['role']
            except Exception as e_role_fetch: # DB errors, etc.
                logger.debug(f"Could not fetch specific workspace role for system admin {username} in {final_workspace_id_obj}: {e_role_fetch}")
        
        # Handle per_page parameter - convert string to int or None
        processed_per_page: Optional[int] = None
        if per_page is not None and per_page.lower() not in ["none", "null", ""]:
            try:
                processed_per_page = int(per_page)
                if processed_per_page < 1 or processed_per_page > 100:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="per_page must be between 1 and 100")
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="per_page must be a valid integer or 'none'")
        elif per_page is None:
            # Default to 10 if no per_page parameter is provided
            processed_per_page = 10

        can_search_feature = user_db_info.get("is_search", False) or \
                             is_system_admin or \
                             (workspace_specific_role in ["admin", "owner"])
        if not can_search_feature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable for your account in this workspace context.")

        target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)
        
        search_query_model = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time
        )
        filter_obj = build_filter_from_query(search_query_model, system_role, workspace_specific_role, username)

        count_result = client.count(collection_name=target_collection_name, count_filter=filter_obj)
        total_count = count_result.count

        paginated_data: List[Dict[str, Any]] = []
        num_of_pages = 0
        points_for_current_page = []  # Initialize outside the conditional
        
        if total_count > 0:
            if processed_per_page is None:
                # Return all results without pagination - handle Qdrant limitations
                num_of_pages = 1
                
                # Qdrant scroll has limitations, so we need to batch the requests
                batch_size = 1000  # Qdrant's typical safe limit
                offset = 0
                
                while offset < total_count:
                    current_limit = min(batch_size, total_count - offset)
                    batch_points, _ = client.scroll(
                        collection_name=target_collection_name, 
                        scroll_filter=filter_obj,
                        limit=current_limit, 
                        offset=offset, 
                        with_payload=True, 
                        with_vectors=False
                    )
                    points_for_current_page.extend(batch_points)
                    offset += current_limit
                    
                    # Break if we got fewer results than expected (end of data)
                    if len(batch_points) < current_limit:
                        break
            else:
                # Use pagination
                num_of_pages = (total_count + processed_per_page - 1) // processed_per_page
                if page <= num_of_pages:
                    offset = (page - 1) * processed_per_page
                    points_for_current_page, _ = client.scroll(
                        collection_name=target_collection_name, scroll_filter=filter_obj,
                        limit=processed_per_page, offset=offset, with_payload=True, with_vectors=False
                    )
                else:
                    points_for_current_page = []
            
            # Process the points data (moved outside the pagination conditional)
            for point_item in points_for_current_page:
                if point_item.payload: 
                    # Conditionally include frame based on base64 parameter
                    frame_data = point_item.payload.get("frame_base64") if base64 else None
                    
                    paginated_data.append({
                        "id": str(point_item.id), 
                        "frame": frame_data,  # Will be None when base64=False
                        "metadata": {
                            "camera_id": point_item.payload.get("camera_id"),
                            "name": point_item.payload.get("name", "Unknown Camera"),
                            "timestamp": point_item.payload.get("timestamp"),
                            "date": point_item.payload.get("date"), 
                            "time": point_item.payload.get("time"),
                            "person_count": point_item.payload.get("person_count", 0),
                            "male_count": point_item.payload.get("male_count", 0),
                            "female_count": point_item.payload.get("female_count", 0),
                            "fire_status": point_item.payload.get("fire_status", "no detection"),
                            "owner_username": point_item.payload.get("username")
                        }
                    })

        return JSONResponse(content={
            "data": paginated_data, 
            "current_page": page if processed_per_page is not None else 1, 
            "num_of_pages": num_of_pages,
            "total_count": total_count, 
            "per_page": processed_per_page,
            "search_scope": {
                "workspace_id": str(final_workspace_id_obj), 
                "collection_queried": target_collection_name, 
                "filters_applied": search_query_model.model_dump(exclude_none=True),
                "access_level": "system_admin" if is_system_admin else (workspace_specific_role or "member_or_undefined"),
                "base64_frames_included": base64,  # Added to show what was requested
                "pagination_disabled": processed_per_page is None  # Added to show if pagination was disabled
            }
        })
    except HTTPException as e:
        raise e
    except Exception as e_search:
        final_ws_id_log = str(final_workspace_id_obj) if final_workspace_id_obj else 'unknown_workspace'
        logger.error(f"Workspace search error in {final_ws_id_log}: {e_search}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error during workspace data search.")
    
@router.get("/workspace/search_results_v2_ordered")
async def workspace_search_results_v2_ordered(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"), 
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1), per_page: int = Query(10, ge=1, le=100),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    final_workspace_id_obj: Optional[UUID] = None 
    try:
        requesting_user_id_obj = current_user_data["user_id"] # This is UUID
        username = current_user_data["username"]
        user_db_info = await user_manager_global_qdrant.get_user_by_id(requesting_user_id_obj) # Pass UUID
        if not user_db_info: raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")

        if workspace_id_query:
            try: final_workspace_id_obj = UUID(workspace_id_query)
            except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            # Using the refined async helper function
            _, active_ws_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
            if not active_ws_id_obj:
                 raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found. Please specify workspaceId or activate one.")
            final_workspace_id_obj = active_ws_id_obj

        workspace_specific_role = None
        if not is_system_admin:
            try:
                # Using the refined async helper function
                member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, final_workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                logger.warning(f"Access denied for user {username} to workspace {final_workspace_id_obj} (ordered search): {e_ws_access.detail}")
                raise e_ws_access
        else:
            try:
                ws_member_info_for_sysadmin = await db_manager_global_qdrant.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id_obj, final_workspace_id_obj), fetch_one=True # Pass UUIDs
                )
                if ws_member_info_for_sysadmin:
                    workspace_specific_role = ws_member_info_for_sysadmin['role']
            except Exception as e_role_fetch:
                 logger.debug(f"Could not fetch specific workspace role for system admin {username} in {final_workspace_id_obj} (ordered search): {e_role_fetch}")


        can_search_feature = user_db_info.get("is_search", False) or \
                             is_system_admin or \
                             (workspace_specific_role in ["admin", "owner"]) # Assuming 'owner' is also an admin-like role
        if not can_search_feature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable for your account in this workspace context.")

        target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)
        
        search_query_model = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time
        )
        filter_obj = build_filter_from_query(
            search_query_model,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username
        )

        count_result = client.count(collection_name=target_collection_name, count_filter=filter_obj)
        total_count = count_result.count

        paginated_data: List[Dict[str, Any]] = []
        num_of_pages = 0

        if total_count > 0:
            num_of_pages = (total_count + per_page - 1) // per_page

            if page <= num_of_pages:
                # When using order_by, Qdrant requires offset to be None.
                # We fetch all points up to the current page and then slice.
                fetch_limit_for_qdrant_batch = page * per_page
                
                points_batch, _ = client.scroll(
                    collection_name=target_collection_name,
                    scroll_filter=filter_obj,
                    limit=fetch_limit_for_qdrant_batch, # Fetch all points up to the end of the current page
                    offset=None,                        # Must be None when order_by is used
                    with_payload=True,
                    with_vectors=False,
                    order_by=qdrant_models.OrderBy(
                        key="timestamp", 
                        direction=qdrant_models.Direction.DESC
                    )
                )
                
                # Extract the data for the current page from the fetched batch
                start_index_for_slice = (page - 1) * per_page
                # Slicing: [start_index_for_slice : start_index_for_slice + per_page]
                # Python's list slicing handles the end index gracefully if it exceeds the list length.
                points_for_current_page = points_batch[start_index_for_slice : start_index_for_slice + per_page]
                
                logger.debug(f"Ordered search: Fetched {len(points_batch)} total points up to page {page}, "
                             f"sliced to {len(points_for_current_page)} points for current page.")
                
                for point_item in points_for_current_page:
                    if point_item.payload: 
                        paginated_data.append({
                            "id": str(point_item.id), 
                            "frame": point_item.payload.get("frame_base64"), 
                            "metadata": {
                                "camera_id": point_item.payload.get("camera_id"),
                                "name": point_item.payload.get("name", "Unknown Camera"),
                                "timestamp": point_item.payload.get("timestamp"),
                                "date": point_item.payload.get("date"), 
                                "time": point_item.payload.get("time"),
                                "person_count": point_item.payload.get("person_count", 0),
                                "male_count": point_item.payload.get("male_count", 0),
                                "female_count": point_item.payload.get("female_count", 0),
                                "fire_status": point_item.payload.get("fire_status", "no detection"),
                                "owner_username": point_item.payload.get("username")
                            }
                        })
        
        return JSONResponse(content={
            "data": paginated_data, 
            "current_page": page, 
            "num_of_pages": num_of_pages,
            "total_count": total_count, 
            "per_page": per_page,
            "search_scope": {
                "workspace_id": str(final_workspace_id_obj), 
                "collection_queried": target_collection_name, 
                "filters_applied": search_query_model.model_dump(exclude_none=True),
                "access_level": "system_admin" if is_system_admin else (workspace_specific_role if workspace_specific_role else "member_or_undefined")
            }
        })
    except HTTPException as e:
        # Log HTTPExceptions if they are not raised by us explicitly with enough context
        if e.status_code >= 500 : # Log server-side HTTPExceptions
             logger.error(f"HTTPException in ordered search for ws {final_workspace_id_obj or 'unknown'}: {e.detail}", exc_info=True)
        raise e
    except Exception as e_search: # Catch other non-HTTP exceptions (like Qdrant client errors)
        final_ws_id_log = str(final_workspace_id_obj) if final_workspace_id_obj else 'unknown_workspace'
        raw_qdrant_response = getattr(e_search, 'raw_response_content', None)
        if raw_qdrant_response:
            logger.error(f"Qdrant raw response content during ordered search: {raw_qdrant_response}")
        logger.error(f"Ordered search error in {final_ws_id_log}: {e_search}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error during ordered workspace data search.")

async def process_camera_prediction( 
    cam_id: str, collection_name: str, start_date: Optional[str], end_date: Optional[str],
    start_time: Optional[str], end_time: Optional[str],
    # Potentially add user context params if data in Qdrant is user-scoped and needs filtering here
    # user_system_role: Optional[str] = None, user_workspace_role: Optional[str] = None, requesting_username: Optional[str] = None
):
    try:
        client = get_qdrant_client() 
        cam_query = SearchQuery(camera_id=[cam_id], start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time)
        # Pass context if added to signature: build_filter_from_query(cam_query, user_system_role, ...)
        filter_obj = build_filter_from_query(cam_query) # Original call, no user context
        
        points, _ = client.scroll(
            collection_name=collection_name, limit=config.get("prediction_data_points_limit", 250), 
            with_payload=["timestamp", "person_count", "camera_id", "name"],
            with_vectors=False, scroll_filter=filter_obj, offset=None, # offset=None for DESC scroll from latest
            order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC) 
        )
        if not points: return None 
        camera_name = points[0].payload.get("name", "Unknown") if points[0].payload else "Unknown"
        search_results = [{"metadata": {"camera_id": p.payload.get("camera_id"), "timestamp": p.payload.get("timestamp"), "person_count": p.payload.get("person_count", 0)}} for p in points if p.payload]
        search_results.sort(key=lambda x: x['metadata']['timestamp']) # Sort ascending for prediction model
        prediction = make_prediction(cam_id, search_results) 
        return {"camera_id": cam_id, "name": camera_name, "prediction": prediction}
    except Exception as e:
        logger.error(f"Error processing prediction for cam {cam_id} in {collection_name}: {e}", exc_info=True)
        return None

async def get_timestamp_range_( 
    camera_id_param: Optional[str] = None, 
    workspace_id_for_search: Optional[Union[str, UUID]] = None,
    current_user_data_for_filter: Dict = None 
) -> TimestampRangeResponse:
    client = get_qdrant_client()
    final_workspace_id_obj: Optional[UUID] = None

    requesting_user_id_obj = current_user_data_for_filter["user_id"]
    username_context = current_user_data_for_filter["username"]
    system_role = current_user_data_for_filter.get("role") # From user_db_info.role via full_data_dependency
    is_system_admin = (system_role == "admin")

    if workspace_id_for_search:
        try: final_workspace_id_obj = UUID(str(workspace_id_for_search))
        except ValueError: raise HTTPException(status_code=400, detail="Invalid workspace_id_for_search format.")
    elif username_context:
        _, active_ws_id_obj = await get_user_and_workspace_refined(username_context, user_manager_global_qdrant)
        if not active_ws_id_obj:
            logger.warning(f"Timestamp range: User {username_context} has no active workspace. Returning empty range.")
            return TimestampRangeResponse() # Return empty if no active workspace and none specified
        final_workspace_id_obj = active_ws_id_obj
    else: # Should not be reached if current_user_data_for_filter always has username
        raise HTTPException(status_code=400, detail="Workspace context is required for timestamp range.")

    workspace_specific_role = None
    if not is_system_admin:
        try:
            member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, final_workspace_id_obj, db_manager_global_qdrant)
            workspace_specific_role = member_info.get("role")
        except HTTPException as e_ws_access:
            logger.warning(f"Timestamp range access denied for user {username_context} to workspace {final_workspace_id_obj}: {e_ws_access.detail}")
            raise e_ws_access
    else: # System admin - check their role in this specific workspace if they happen to be a member
        try:
            ws_member_info = await db_manager_global_qdrant.execute_query(
                "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                (requesting_user_id_obj, final_workspace_id_obj), fetch_one=True)
            if ws_member_info: workspace_specific_role = ws_member_info['role']
        except Exception: pass

    target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
    await ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)

    parsed_camera_ids_list = parse_camera_ids(camera_id_param) if camera_id_param else None
    filter_obj = build_filter_from_query(
        SearchQuery(camera_id=parsed_camera_ids_list),
        user_system_role=system_role, user_workspace_role=workspace_specific_role, requesting_username=username_context)

    min_ts, max_ts = None, None
    try:
        # ASC for min_ts
        points_asc, _ = client.scroll(collection_name=target_collection_name, scroll_filter=filter_obj, limit=1, offset=None, with_payload=["timestamp"], order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.ASC))
        if points_asc and points_asc[0].payload and "timestamp" in points_asc[0].payload:
            min_ts = float(points_asc[0].payload["timestamp"])
        # DESC for max_ts
        points_desc, _ = client.scroll(collection_name=target_collection_name, scroll_filter=filter_obj, limit=1, offset=None, with_payload=["timestamp"], order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC))
        if points_desc and points_desc[0].payload and "timestamp" in points_desc[0].payload:
            max_ts = float(points_desc[0].payload["timestamp"])
    except Exception as e_scroll: 
        logger.warning(f"Optimized timestamp range scroll failed for {target_collection_name} (filter: {filter_obj}): {e_scroll}. Falling back.")
        # Fallback logic from original code
        sample_points, _ = client.scroll(collection_name=target_collection_name, limit=100, offset=None, with_payload=["timestamp"], scroll_filter=filter_obj) # Fallback without order_by
        all_timestamps = [float(p.payload["timestamp"]) for p in sample_points if p.payload and p.payload.get("timestamp") is not None]
        if all_timestamps:
            min_ts = min(all_timestamps)
            max_ts = max(all_timestamps)

    response = TimestampRangeResponse()
    if min_ts is not None and max_ts is not None:
        response.first_timestamp, response.last_timestamp = min_ts, max_ts
        try:
            first_dt = datetime.fromtimestamp(min_ts, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(max_ts, tz=timezone.utc)
            response.first_datetime, response.last_datetime = first_dt.isoformat(), last_dt.isoformat()
            response.first_date, response.last_date = first_dt.date().isoformat(), last_dt.date().isoformat()
            response.first_time, response.last_time = first_dt.time().isoformat(timespec='seconds'), last_dt.time().isoformat(timespec='seconds')
        except Exception as dt_err: # Catch potential errors from timestamp conversion
            logger.error(f"Error converting timestamps in get_timestamp_range_ for {target_collection_name}: {dt_err}")
            response.first_timestamp = response.last_timestamp = None # Clear if conversion fails

    # Populate camera_id in response if it was used for filtering
    if parsed_camera_ids_list:
        response.camera_id = ", ".join(parsed_camera_ids_list) if len(parsed_camera_ids_list) > 1 else parsed_camera_ids_list[0]
    elif camera_id_param: # If camera_id_param was a single string not parsed into list (e.g. invalid format)
        response.camera_id = camera_id_param
    return response

async def process_single_camera_prediction(
    client: QdrantClient, collection_name: str, camera_id_str: str, camera_name: str,
    base_search_query: SearchQuery, user_system_role: Optional[str],
    user_workspace_role: Optional[str], requesting_username: Optional[str]
):
    try:
        filter_obj = build_filter_from_query(base_search_query, user_system_role, user_workspace_role, requesting_username)
        points, _ = client.scroll(
            collection_name=collection_name, scroll_filter=filter_obj, offset=None,
            limit=config.get("prediction_data_points_limit", 250), 
            with_payload=["timestamp", "person_count", "male_count", "female_count", "fire_status", "camera_id"], 
            with_vectors=False,
            order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC))
        if not points: return None 

        prediction_input_data = [
            {
                "metadata": {
                    "camera_id": p.payload.get("camera_id"), 
                    "timestamp": p.payload.get("timestamp"), 
                    "person_count": p.payload.get("person_count", 0),
                    "male_count": p.payload.get("male_count", 0),
                    "female_count": p.payload.get("female_count", 0),
                    "fire_status": p.payload.get("fire_status", "no detection")
                }
            } 
        for p in points if p.payload]
        
        if not prediction_input_data: return None
        
        prediction_input_data.sort(key=lambda x: x['metadata']['timestamp'])
        prediction_output = make_prediction(camera_id_str, prediction_input_data) 
        return {"camera_id": camera_id_str, "name": camera_name, "prediction": prediction_output}
    except Exception as e:
        logger.error(f"Error in process_single_camera_prediction for {camera_id_str}: {e}", exc_info=True)
        return {"camera_id": camera_id_str, "name": camera_name, "prediction": None, "error": str(e)} 

@router.get("/workspace/prediction_data")
async def workspace_prediction_endpoint(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"), 
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    final_workspace_id_obj: Optional[UUID] = None
    try:
        requesting_user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        user_db_info = await user_manager_global_qdrant.get_user_by_id(requesting_user_id_obj)
        if not user_db_info: raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")

        if workspace_id_query:
            try: final_workspace_id_obj = UUID(workspace_id_query)
            except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
            if not active_ws_id_obj: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id_obj = active_ws_id_obj
        
        workspace_specific_role = None
        if not is_system_admin:
            try:
                member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, final_workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access: raise e_ws_access
        else:
            try:
                ws_member_info = await db_manager_global_qdrant.execute_query("SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2", (requesting_user_id_obj, final_workspace_id_obj), fetch_one=True)
                if ws_member_info: workspace_specific_role = ws_member_info['role']
            except Exception: pass

        can_use_prediction = user_db_info.get("is_prediction", False) or is_system_admin or (workspace_specific_role in ["admin", "owner"])
        if not can_use_prediction: raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Prediction feature unavailable.")

        target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)

        parsed_camera_ids_for_pred: List[Tuple[str, str]] = [] 
        if camera_id_param:
            raw_parsed_ids = parse_camera_ids(camera_id_param) # Returns List[str]
            if raw_parsed_ids:
                # Assuming video_stream.stream_id is TEXT/VARCHAR for compatibility with raw_parsed_ids (List[str])
                # If stream_id in DB is UUID, raw_parsed_ids need conversion: `[UUID(cid) for cid in raw_parsed_ids]`
                # Using $N placeholders for asyncpg
                placeholders = ', '.join([f'${i+1}' for i in range(len(raw_parsed_ids))])
                cam_names_q = f"SELECT stream_id, name FROM video_stream WHERE stream_id IN ({placeholders}) AND workspace_id = ${len(raw_parsed_ids) + 1}"
                db_params = tuple(raw_parsed_ids + [final_workspace_id_obj]) # final_workspace_id_obj is UUID
                
                cam_names_db = await db_manager_global_qdrant.execute_query(cam_names_q, db_params, fetch_all=True)
                cam_details_map = {str(row['stream_id']): row['name'] for row in cam_names_db} # Ensure key is string
                parsed_camera_ids_for_pred = [(cid_str, cam_details_map.get(cid_str, f"Camera {cid_str[:8]}")) for cid_str in raw_parsed_ids]
        else: 
            db_cameras_q = "SELECT stream_id, name FROM video_stream WHERE workspace_id = $1 AND status = 'active'" 
            db_cameras = await db_manager_global_qdrant.execute_query(db_cameras_q, (final_workspace_id_obj,), fetch_all=True) # final_workspace_id_obj is UUID
            if not db_cameras: return JSONResponse({"predictions": [], "message": "No active cameras found in this workspace."})
            parsed_camera_ids_for_pred = [(str(cam['stream_id']), cam['name']) for cam in db_cameras] # stream_id from DB may be UUID

        if not parsed_camera_ids_for_pred: return JSONResponse({"predictions": [], "message": "No cameras selected or found."})
        
        prediction_tasks = [
            process_single_camera_prediction(client, target_collection_name, cam_id_str, cam_name_str,
                SearchQuery(camera_id=[cam_id_str], start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time),
                system_role, workspace_specific_role, username)
            for cam_id_str, cam_name_str in parsed_camera_ids_for_pred
        ]
        prediction_results = await asyncio.gather(*prediction_tasks)
        return JSONResponse({"predictions": [pred for pred in prediction_results if pred]})
    except HTTPException as e: raise e
    except Exception as e_pred:
        logger.error(f"Workspace prediction error for ws {final_workspace_id_obj or 'unknown'}: {e_pred}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error during workspace prediction.")

@router.get("/search_results") 
async def search_results_user_active_workspace(
    camera_id: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1), per_page: Optional[str] = Query(None),
    base64: bool = Query(True), 
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    username = current_user_data["username"]
    _, active_workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
    if not active_workspace_id_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found for user.")
    return await workspace_search_results_v2(workspace_id_query=str(active_workspace_id_obj), camera_id_param=camera_id, start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time, page=page, per_page=per_page, current_user_data=current_user_data, base64=base64)

@router.get("/prediction_data") 
async def prediction_data_user_active_workspace(
    camera_id: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    username = current_user_data["username"]
    _, active_workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
    if not active_workspace_id_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found for user.")
    return await workspace_prediction_endpoint(workspace_id_query=str(active_workspace_id_obj), camera_id_param=camera_id, start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time, current_user_data=current_user_data)

@router.get("/timestamp-range", response_model=TimestampRangeResponse)
async def get_timestamp_range_endpoint( 
    camera_id: Optional[str] = Query(None, alias="camera_id"), 
    workspace_id: Optional[str] = Query(None, description="Optional: Specify workspace ID. Defaults to active workspace."),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    try:
        if workspace_id and current_user_data.get("role") != "admin": # system role check
            target_ws_id_obj = UUID(workspace_id) # Can raise ValueError
            await check_workspace_membership_and_get_role_async(current_user_data["user_id"], target_ws_id_obj, db_manager_global_qdrant) # Checks membership
        return await get_timestamp_range_(camera_id_param=camera_id, workspace_id_for_search=workspace_id, current_user_data_for_filter=current_user_data)
    except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace_id format.")
    except HTTPException as e: raise e
    except Exception as e_ts_range:
        logger.error(f"Error getting timestamp range for {current_user_data['username']}: {e_ts_range}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error retrieving timestamp range.")

@router.get("/camera-ids-alt", response_model=CameraIdsResponse)
async def get_all_camera_ids_for_active_workspace(current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)): 
    try:
        username = current_user_data["username"]
        _, active_workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant) # active_workspace_id_obj is UUID
        if not active_workspace_id_obj:
            return CameraIdsResponse(camera_ids=[], count=0)

        query = "SELECT DISTINCT stream_id, name FROM video_stream WHERE workspace_id = $1 ORDER BY name"
        # Pass UUID active_workspace_id_obj directly, asyncpg handles it.
        camera_records = await db_manager_global_qdrant.execute_query(query, (active_workspace_id_obj,), fetch_all=True)
        
        camera_list = [{"id": str(rec["stream_id"]), "name": rec["name"]} for rec in camera_records] if camera_records else []
        return CameraIdsResponse(camera_ids=[cam["id"] for cam in camera_list], count=len(camera_list)) # Original code returned camera_ids=[cam["id"]...]
    except HTTPException as e: raise e
    except Exception as e:
        logger.error(f"Error retrieving camera IDs for active workspace of {current_user_data.get('username','unknown')}: {e}", exc_info=True)
        # This is where the original error likely occurred. The new async stack should resolve it.
        raise HTTPException(status_code=500, detail=f"Error retrieving camera IDs: {str(e)}")

@router.delete("/collections/{collection_name_to_delete}")
async def delete_qdrant_collection(
    collection_name_to_delete: str,
    current_admin_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    if current_admin_data.get("role") != "admin": 
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only system admins can delete collections this way.")
    try:
        client.get_collection(collection_name=collection_name_to_delete) # Check existence
        client.delete_collection(collection_name=collection_name_to_delete)
        logger.info(f"System Admin {current_admin_data['username']} deleted Qdrant collection: {collection_name_to_delete}")
        await session_manager_global_qdrant.log_action( # Await async log_action
            content=f"Admin deleted Qdrant collection: {collection_name_to_delete}",
            user_id=current_admin_data["user_id"], # This is UUID
            action_type="Qdrant_Collection_Deleted", status="critical"
        )
        return {"status": "success", "message": f"Collection '{collection_name_to_delete}' deleted successfully"}
    except Exception as e: 
        if "not found" in str(e).lower() or (hasattr(e, 'status_code') and e.status_code == 404): 
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Collection '{collection_name_to_delete}' not found.")
        logger.error(f"Failed to delete Qdrant collection {collection_name_to_delete}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to delete collection: {str(e)}")

@router.delete("/delete_data") 
async def delete_data_from_workspace_collection(
    request_body: DeleteDataRequest,
    workspace_id_to_target: str = Query(..., description="The ID of the workspace whose data should be targeted."),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    requesting_user_id_obj = current_user_data["user_id"] # UUID
    requesting_user_system_role = current_user_data["role"] # System role
    username = current_user_data["username"]

    try: target_ws_id_obj = UUID(workspace_id_to_target)
    except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace_id_to_target format.")

    workspace_specific_role_for_deleter = None
    if requesting_user_system_role != "admin": # Not a system admin
        # Must be at least admin/owner of the target workspace
        try:
            member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, target_ws_id_obj, db_manager_global_qdrant, required_role="admin") # or owner
            workspace_specific_role_for_deleter = member_info.get("role")
        except HTTPException as e_perm: # Not admin/owner of workspace
            raise e_perm
    # If system admin, workspace_specific_role_for_deleter can remain None (or be fetched if desired, but not strictly needed for filter building if sys_role is admin)

    target_collection_name = get_workspace_qdrant_collection_name(target_ws_id_obj)
    await ensure_workspace_qdrant_collection_exists(client, target_ws_id_obj)

    search_query_for_delete = SearchQuery(
        camera_id=parse_camera_ids(request_body.camera_id) if request_body.camera_id else None,
        start_date=request_body.start_date, end_date=request_body.end_date,
        start_time=request_body.start_time, end_time=request_body.end_time
    )
    filter_obj = build_filter_from_query(
        search_query_for_delete,
        user_system_role=requesting_user_system_role,
        user_workspace_role=workspace_specific_role_for_deleter, # This will be None for sys admin, or their role in WS
        requesting_username=username # Only applied if not sys/ws admin
    )

    if not filter_obj:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No specific filter criteria provided for data deletion. Deleting all data is not permitted without filters.")

    try:
        count_before_result = client.count(collection_name=target_collection_name, count_filter=filter_obj)
        count_before = count_before_result.count
        if count_before == 0:
            return JSONResponse(content={"status": "info", "message": "No data found matching the criteria to delete.", "deleted_count": 0})

        delete_op_result = client.delete(collection_name=target_collection_name, points_selector=qdrant_models.FilterSelector(filter=filter_obj))
        
        log_status_qdrant = "success" if delete_op_result.status == qdrant_models.UpdateStatus.COMPLETED else "failure"
        await session_manager_global_qdrant.log_action( # Await async log_action
            content=f"User '{username}' deleted data from workspace '{target_ws_id_obj}'. Filters: {search_query_for_delete.model_dump(exclude_none=True)}. Targeted {count_before} points. Qdrant status: {delete_op_result.status}",
            user_id=requesting_user_id_obj, workspace_id=target_ws_id_obj, # Pass UUIDs
            action_type="Qdrant_Data_Deleted", status=log_status_qdrant
        )
        if delete_op_result.status != qdrant_models.UpdateStatus.COMPLETED:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Qdrant delete operation status: {delete_op_result.status}")
        return JSONResponse(content={"status": "success", "message": f"Successfully targeted {count_before} data points for deletion.", "targeted_for_deletion_count": count_before})
    except HTTPException as e: raise e
    except Exception as e_del_data:
        logger.error(f"Failed to delete data from {target_collection_name}: {e_del_data}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to delete data: {str(e_del_data)}")

@router.delete("/delete_full_data") 
async def delete_full_data_from_workspace_collection(  # Changed function name to avoid duplicate
    workspace_id_to_target: str = Query(..., description="The ID of the workspace whose data should be targeted."),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    requesting_user_id_obj = current_user_data["user_id"]
    requesting_user_system_role = current_user_data["role"]
    username = current_user_data["username"]

    try: target_ws_id_obj = UUID(workspace_id_to_target)
    except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace_id_to_target format.")

    workspace_specific_role_for_deleter = None
    if requesting_user_system_role != "admin":
        try:
            member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, target_ws_id_obj, db_manager_global_qdrant, required_role="admin")
            workspace_specific_role_for_deleter = member_info.get("role")
        except HTTPException as e_perm:
            raise e_perm

    target_collection_name = get_workspace_qdrant_collection_name(target_ws_id_obj)
    await ensure_workspace_qdrant_collection_exists(client, target_ws_id_obj)

    try:
        count_before_result = client.count(collection_name=target_collection_name)
        count_before = count_before_result.count
        if count_before == 0:
            return JSONResponse(content={"status": "info", "message": "No data found to delete.", "deleted_count": 0})

        # Option 1: Use FilterSelector with must=[] to match all points
        delete_op_result = client.delete(
            collection_name=target_collection_name,
            points_selector=qdrant_models.FilterSelector(
                filter=qdrant_models.Filter(must=[])
            )
        )
        
        log_status_qdrant = "success" if delete_op_result.status == qdrant_models.UpdateStatus.COMPLETED else "failure"
        await session_manager_global_qdrant.log_action(
            content=f"User '{username}' deleted ALL data from workspace '{target_ws_id_obj}'. Targeted {count_before} points. Qdrant status: {delete_op_result.status}",
            user_id=requesting_user_id_obj, workspace_id=target_ws_id_obj,
            action_type="Qdrant_Full_Data_Deleted", status=log_status_qdrant
        )
        if delete_op_result.status != qdrant_models.UpdateStatus.COMPLETED:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Qdrant delete operation status: {delete_op_result.status}")
        return JSONResponse(content={"status": "success", "message": f"Successfully deleted all {count_before} data points.", "deleted_count": count_before})
    except HTTPException as e: raise e
    except Exception as e_del_data:
        logger.error(f"Failed to delete data from {target_collection_name}: {e_del_data}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to delete data: {str(e_del_data)}")

@router.get("/collections") 
async def list_collections(current_admin_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)): 
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")
    client = get_qdrant_client()
    try:
        collections_response = client.get_collections()
        collection_info_list = []
        for c in collections_response.collections:
            try:
                details = client.get_collection(collection_name=c.name)
                ws_id_from_name = None
                if c.name.startswith(BASE_QDRANT_COLLECTION_NAME + "_ws_"):
                    try:
                        ws_id_str = c.name.split("_ws_")[1].replace("_", "-") 
                        ws_id_from_name = str(UUID(ws_id_str))
                    except (IndexError, ValueError): 
                        logger.warning(f"Could not parse workspace ID from collection name: {c.name}")
                collection_info_list.append({
                    "name": c.name, "status": str(details.status), "optimizer_status": str(details.optimizer_status),
                    "points_count": details.points_count, "segments_count": details.segments_count,
                    "config": details.config.model_dump(exclude_none=True), 
                    "payload_schema": {k: str(v.data_type) for k, v in details.payload_schema.items()} if details.payload_schema else {},
                    "associated_workspace_id": ws_id_from_name
                })
            except Exception as e_detail:
                logger.warning(f"Could not get full details for collection {c.name}: {e_detail}")
                collection_info_list.append({"name": c.name, "status": "details_error", "error": str(e_detail)})
        return {"collections": collection_info_list}
    except Exception as e:
        logger.error(f"Failed to list Qdrant collections: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list collections: {str(e)}")

@router.get("/collections/{collection_name}/count")
async def get_collection_count(
    collection_name: str,
    camera_id: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    client = get_qdrant_client()
    workspace_id_of_collection: Optional[UUID] = None 

    requesting_user_id_obj = current_user_data["user_id"] # UUID
    system_role = current_user_data.get("role") # User's system role
    username = current_user_data["username"]
    is_system_admin = (system_role == "admin")
    workspace_specific_role = None # User's role within the workspace_id_of_collection (if applicable)

    try:
        if not collection_name.startswith(BASE_QDRANT_COLLECTION_NAME + "_ws_"): # Generic collection
            if not is_system_admin:
                raise HTTPException(status_code=403, detail="Access denied to this collection's count.")
            try: client.get_collection(collection_name=collection_name) # Check existence
            except Exception as e_gen_coll: # Qdrant client errors
                 if "not found" in str(e_gen_coll).lower() or (hasattr(e_gen_coll, 'status_code') and e_gen_coll.status_code == 404):
                    raise HTTPException(status_code=404, detail=f"Generic collection '{collection_name}' not found.")
                 raise HTTPException(status_code=500, detail=f"Error accessing generic collection '{collection_name}': {str(e_gen_coll)}")
        else: # Workspace-specific collection
            try:
                ws_id_str_from_coll = collection_name.split("_ws_")[1].replace("_", "-")
                workspace_id_of_collection = UUID(ws_id_str_from_coll)
            except (IndexError, ValueError):
                raise HTTPException(status_code=400, detail="Invalid workspace ID in collection name.")
            
            if not is_system_admin: # Non-system admin accessing a workspace collection
                try:
                    member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, workspace_id_of_collection, db_manager_global_qdrant)
                    workspace_specific_role = member_info.get("role")
                    # Add check: only ws admin/owner can count? Or any member? Assuming any member for now.
                except HTTPException as e_ws_access: raise e_ws_access
            else: # System admin accessing a workspace collection
                try: # Get their role in this specific workspace if they are also a member
                    ws_member_info = await db_manager_global_qdrant.execute_query(
                        "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                        (requesting_user_id_obj, workspace_id_of_collection), fetch_one=True)
                    if ws_member_info: workspace_specific_role = ws_member_info['role']
                except Exception: pass # Ignore if sysadmin is not a member or DB error
            await ensure_workspace_qdrant_collection_exists(client, workspace_id_of_collection)

        search_query_model = SearchQuery(camera_id=parse_camera_ids(camera_id) if camera_id else None, start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time)
        filter_obj = build_filter_from_query(search_query_model, system_role, workspace_specific_role, username)
        
        count_result = client.count(collection_name=collection_name, count_filter=filter_obj)
        
        access_level_desc = "system_admin" if is_system_admin else \
                           (workspace_specific_role if workspace_specific_role else \
                           ("member_or_undefined" if workspace_id_of_collection else "generic_collection_admin_only"))

        return {"collection_name": collection_name, "workspace_id_associated": str(workspace_id_of_collection) if workspace_id_of_collection else "N/A (Generic Collection)", "count": count_result.count, "filters_applied": search_query_model.model_dump(exclude_none=True), "access_level": access_level_desc}
    except HTTPException as e: raise e
    except Exception as e:
        logger.error(f"Failed to get collection count for '{collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get collection count: {str(e)}")

@router.post("/collections") 
async def create_qdrant_collection_generic(
    request_body: CreateCollectionRequest, 
    current_admin_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")
    client = get_qdrant_client()
    try:
        try: client.get_collection(collection_name=request_body.collection_name)
        except Exception: pass # Collection does not exist, proceed.
        else: return {"status": "info", "message": f"Collection '{request_body.collection_name}' already exists."}

        distance_map = {"DOT": qdrant_models.Distance.DOT, "COSINE": qdrant_models.Distance.COSINE, "EUCLID": qdrant_models.Distance.EUCLID}
        qdrant_distance = distance_map.get(request_body.distance.upper())
        if not qdrant_distance: raise HTTPException(status_code=400, detail="Invalid distance metric.")

        client.create_collection(collection_name=request_body.collection_name, vectors_config=qdrant_models.VectorParams(size=request_body.vector_size, distance=qdrant_distance))
        await session_manager_global_qdrant.log_action(content=f"Admin created Qdrant collection: {request_body.collection_name}", user_id=current_admin_data["user_id"], action_type="Qdrant_Collection_Created", status="success")
        return {"status": "success", "message": f"Collection '{request_body.collection_name}' created."}
    except Exception as e:
        logger.error(f"Failed to create Qdrant collection '{request_body.collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create collection: {str(e)}")

async def perform_search(query: SearchQuery, collection_name: str, 
                         user_system_role: Optional[str] = None,
                         user_workspace_role: Optional[str] = None,
                         requesting_username: Optional[str] = None):
    client = get_qdrant_client() 
    logger.info(f"Performing tool search in '{collection_name}' with query: {query.model_dump(exclude_none=True)}")
    try:
        filter_obj = build_filter_from_query(query, user_system_role, user_workspace_role, requesting_username)
        
        count_result = client.count(collection_name=collection_name, count_filter=filter_obj)
        total_count = count_result.count

        fetch_limit_for_tool = config.get("qdrant_tool_fetch_limit", 20) 
        all_points = []
        if total_count > 0:
            points, _ = client.scroll(
                collection_name=collection_name, limit=fetch_limit_for_tool, offset=None, 
                with_payload=True, with_vectors=False, scroll_filter=filter_obj,
                order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC))
            all_points.extend(points)

        search_results = [
            {
                "id": str(p.id), 
                "frame": p.payload.get("frame_base64"), 
                "metadata": {
                    "camera_id": p.payload.get("camera_id"), 
                    "name": p.payload.get("name", "Unknown"), 
                    "timestamp": p.payload.get("timestamp"), 
                    "date": p.payload.get("date"), 
                    "time": p.payload.get("time"), 
                    "person_count": p.payload.get("person_count", 0), 
                    "male_count": p.payload.get("male_count", 0),
                    "female_count": p.payload.get("female_count", 0),
                    "fire_status": p.payload.get("fire_status", "no detection"),
                    "owner_username": p.payload.get("username")
                }
            } 
            for p in all_points if p.payload]
        return {"results": search_results, "total_count": total_count, "fetched_for_tool": len(search_results)}
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Qdrant tool search error in '{collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Search operation failed: {str(e)}")

##############################################################################################################################
##############################################################################################################################

def build_filter_from_query_with_location(
    query: Union[SearchQuery, LocationSearchQuery],
    user_system_role: Optional[str] = None,
    user_workspace_role: Optional[str] = None,
    requesting_username: Optional[str] = None,
) -> Optional[qdrant_models.Filter]:
    """Enhanced filter building with location support."""
    must_conditions: List[Union[qdrant_models.FieldCondition, qdrant_models.Filter]] = []
    
    # Camera ID filter
    if hasattr(query, 'camera_id') and query.camera_id:
        if not isinstance(query.camera_id, list):
            logger.error(f"camera_id is not a list: {query.camera_id}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="camera_id must be a list")
        str_camera_ids = [str(cid) for cid in query.camera_id if cid]
        if not str_camera_ids:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid camera_id provided")

        if len(str_camera_ids) == 1:
            must_conditions.append(qdrant_models.FieldCondition(key="camera_id", match=qdrant_models.MatchValue(value=str_camera_ids[0])))
        else:
            must_conditions.append(qdrant_models.Filter(should=[
                qdrant_models.FieldCondition(key="camera_id", match=qdrant_models.MatchValue(value=cam_id)) for cam_id in str_camera_ids
            ]))

    # Location filters (if query is LocationSearchQuery)
    if hasattr(query, 'location') and query.location:
        must_conditions.append(qdrant_models.FieldCondition(key="location", match=qdrant_models.MatchValue(value=query.location)))
    
    if hasattr(query, 'area') and query.area:
        must_conditions.append(qdrant_models.FieldCondition(key="area", match=qdrant_models.MatchValue(value=query.area)))
    
    if hasattr(query, 'building') and query.building:
        must_conditions.append(qdrant_models.FieldCondition(key="building", match=qdrant_models.MatchValue(value=query.building)))
    
    if hasattr(query, 'floor_level') and query.floor_level:
        must_conditions.append(qdrant_models.FieldCondition(key="floor_level", match=qdrant_models.MatchValue(value=query.floor_level)))
        
    if hasattr(query, 'zone') and query.zone:
        must_conditions.append(qdrant_models.FieldCondition(key="zone", match=qdrant_models.MatchValue(value=query.zone)))

    # Timestamp filters
    start_timestamp: Optional[float] = None
    end_timestamp: Optional[float] = None
    try:
        if hasattr(query, 'start_date') and query.start_date:
            start_date_obj = parse_date_format(query.start_date)
            start_time_obj = parse_time_string(getattr(query, 'start_time', None), dt_time.min)
            start_datetime = datetime.combine(start_date_obj, start_time_obj).replace(tzinfo=timezone.utc)
            start_timestamp = start_datetime.timestamp()
            
        if hasattr(query, 'end_date') and query.end_date:
            end_date_obj = parse_date_format(query.end_date)
            end_time_obj = parse_time_string(getattr(query, 'end_time', None), dt_time.max.replace(microsecond=0))
            end_datetime = datetime.combine(end_date_obj, end_time_obj).replace(tzinfo=timezone.utc)
            end_timestamp = end_datetime.timestamp()

        if start_timestamp is not None or end_timestamp is not None:
            timestamp_range_filter: Dict[str, Any] = {}
            if start_timestamp is not None: 
                timestamp_range_filter["gte"] = start_timestamp
            if end_timestamp is not None: 
                timestamp_range_filter["lte"] = end_timestamp
            if timestamp_range_filter:
                must_conditions.append(qdrant_models.FieldCondition(key="timestamp", range=qdrant_models.Range(**timestamp_range_filter)))
    except ValueError as date_err:
        logger.error(f"Invalid date format provided for filtering: {date_err}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid date/time format: {date_err}")

    # User access control
    apply_username_filter = True
    if user_system_role == 'admin':
        apply_username_filter = False
    elif user_workspace_role in ['admin', 'owner']:
        apply_username_filter = False

    if apply_username_filter:
        if requesting_username:
            must_conditions.append(qdrant_models.FieldCondition(key="username", match=qdrant_models.MatchValue(value=requesting_username)))
        else:
            must_conditions.append(qdrant_models.FieldCondition(key="username", match=qdrant_models.MatchValue(value=f"__IMPOSSIBLE_USERNAME_{uuid4()}__")))
    
    if not must_conditions: 
        return None
    return qdrant_models.Filter(must=must_conditions)

@router.get("/workspace/search_results_with_location")
async def workspace_search_results_with_location(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    # Date/time filters
    start_date: Optional[str] = Query(None), 
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), 
    end_time: Optional[str] = Query(None),
    # Location filters
    location: Optional[str] = Query(None, description="Filter by location"),
    area: Optional[str] = Query(None, description="Filter by area"),
    building: Optional[str] = Query(None, description="Filter by building"),
    floor_level: Optional[str] = Query(None, description="Filter by floor_level"),
    zone: Optional[str] = Query(None, description="Filter by zone"),
    # Pagination
    page: int = Query(1, ge=1), 
    per_page: Optional[str] = Query(None),  # Changed to Optional[str] like v2
    base64: bool = Query(True),  # New parameter with default True
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    """Enhanced search with location-based filtering."""
    client = get_qdrant_client()
    final_workspace_id_obj: Optional[UUID] = None
    
    try:
        requesting_user_id_obj = current_user_data["user_id"]
        username = current_user_data["username"]
        user_db_info = await user_manager_global_qdrant.get_user_by_id(requesting_user_id_obj)
        if not user_db_info: 
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")

        # Determine workspace
        if workspace_id_query:
            try: 
                final_workspace_id_obj = UUID(workspace_id_query)
            except ValueError: 
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
            if not active_ws_id_obj:
                 raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id_obj = active_ws_id_obj

        # Check permissions
        workspace_specific_role = None
        if not is_system_admin:
            try:
                member_info = await check_workspace_membership_and_get_role_async(requesting_user_id_obj, final_workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        else:
            try:
                ws_member_info = await db_manager_global_qdrant.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id_obj, final_workspace_id_obj), fetch_one=True
                )
                if ws_member_info: 
                    workspace_specific_role = ws_member_info['role']
            except Exception: 
                pass

        # Handle per_page parameter - convert string to int or None (same logic as v2)
        processed_per_page: Optional[int] = None
        if per_page is not None and per_page.lower() not in ["none", "null", ""]:
            try:
                processed_per_page = int(per_page)
                if processed_per_page < 1 or processed_per_page > 100:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="per_page must be between 1 and 100")
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="per_page must be a valid integer or 'none'")
        elif per_page is None:
            # Default to 10 if no per_page parameter is provided
            processed_per_page = 10

        # Check search permissions
        can_search_feature = user_db_info.get("is_search", False) or \
                             is_system_admin or \
                             (workspace_specific_role in ["admin", "owner"])
        if not can_search_feature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable.")

        target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)
        
        # Build enhanced search query with location
        search_query_model = LocationSearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, area=area, building=building, floor_level=floor_level, zone=zone
        )
        
        filter_obj = build_filter_from_query_with_location(
            search_query_model, system_role, workspace_specific_role, username
        )

        # Get count
        count_result = client.count(collection_name=target_collection_name, count_filter=filter_obj)
        total_count = count_result.count

        paginated_data: List[Dict[str, Any]] = []
        num_of_pages = 0
        points_for_current_page = []  # Initialize outside the conditional
        
        if total_count > 0:
            if processed_per_page is None:
                # Return all results without pagination - handle Qdrant limitations (same as v2)
                num_of_pages = 1
                
                # Qdrant scroll has limitations, so we need to batch the requests
                batch_size = 1000  # Qdrant's typical safe limit
                offset = 0
                
                while offset < total_count:
                    current_limit = min(batch_size, total_count - offset)
                    batch_points, _ = client.scroll(
                        collection_name=target_collection_name, 
                        scroll_filter=filter_obj,
                        limit=current_limit, 
                        offset=offset, 
                        with_payload=True, 
                        with_vectors=False
                    )
                    points_for_current_page.extend(batch_points)
                    offset += current_limit
                    
                    # Break if we got fewer results than expected (end of data)
                    if len(batch_points) < current_limit:
                        break
            else:
                # Use pagination
                num_of_pages = (total_count + processed_per_page - 1) // processed_per_page
                if page <= num_of_pages:
                    offset = (page - 1) * processed_per_page
                    points_for_current_page, _ = client.scroll(
                        collection_name=target_collection_name, 
                        scroll_filter=filter_obj,
                        limit=processed_per_page, 
                        offset=offset, 
                        with_payload=True, 
                        with_vectors=False
                    )
                else:
                    points_for_current_page = []
            
            # Process the points data (moved outside the pagination conditional)
            for point_item in points_for_current_page:
                if point_item.payload: 
                    # Conditionally include frame based on base64 parameter
                    frame_data = point_item.payload.get("frame_base64") if base64 else None
                    
                    paginated_data.append({
                        "id": str(point_item.id), 
                        "frame": frame_data,  # Will be None when base64=False
                        "metadata": {
                            "camera_id": point_item.payload.get("camera_id"),
                            "name": point_item.payload.get("name", "Unknown Camera"),
                            "timestamp": point_item.payload.get("timestamp"),
                            "date": point_item.payload.get("date"), 
                            "time": point_item.payload.get("time"),
                            "person_count": point_item.payload.get("person_count", 0),
                            "male_count": point_item.payload.get("male_count", 0),
                            "female_count": point_item.payload.get("female_count", 0),
                            "fire_status": point_item.payload.get("fire_status", "no detection"),
                            "owner_username": point_item.payload.get("username"),
                            # Location metadata
                            "location": point_item.payload.get("location"),
                            "area": point_item.payload.get("area"),
                            "building": point_item.payload.get("building"),
                            "floor_level": point_item.payload.get("floor_level"),
                            "zone": point_item.payload.get("zone")
                        }
                    })
        
        return JSONResponse(content={
            "data": paginated_data, 
            "current_page": page if processed_per_page is not None else 1, 
            "num_of_pages": num_of_pages,
            "total_count": total_count, 
            "per_page": processed_per_page,
            "search_scope": {
                "workspace_id": str(final_workspace_id_obj), 
                "collection_queried": target_collection_name, 
                "filters_applied": search_query_model.model_dump(exclude_none=True),
                "access_level": "system_admin" if is_system_admin else (workspace_specific_role or "member_or_undefined"),
                "base64_frames_included": base64,  # Added to show what was requested
                "pagination_disabled": processed_per_page is None  # Added to show if pagination was disabled
            }
        })
        
    except HTTPException as e:
        raise e
    except Exception as e_search:
        final_ws_id_log = str(final_workspace_id_obj) if final_workspace_id_obj else 'unknown_workspace'
        logger.error(f"Location-based search error in {final_ws_id_log}: {e_search}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error during location-based search.")

async def store_data_with_location_in_qdrant(
    camera_id: str,
    frame_base64: str,
    person_count: int,
    timestamp: float,
    username: str,
    workspace_id: UUID,
    camera_name: str = "Unknown Camera",
    # Location data
    location: Optional[str] = None,
    area: Optional[str] = None,
    building: Optional[str] = None,
    floor_level: Optional[str] = None,
    zone: Optional[str] = None
):
    """Store detection data with location information in Qdrant."""
    try:
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        # Create point with location metadata
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        point_id = str(uuid4())
        
        payload = {
            "camera_id": camera_id,
            "name": camera_name,
            "username": username,
            "timestamp": timestamp,
            "date": dt.date().isoformat(),
            "time": dt.time().isoformat(timespec='seconds'),
            "person_count": person_count,
            "frame_base64": frame_base64,
            # Location fields
            "location": location,
            "area": area,
            "building": building,
            "floor_level": floor_level,
            "zone": zone
        }
        
        # Create a simple vector (Qdrant requires vectors)
        vector = [0.0] * config.get("qdrant_vector_size", 1)
        
        point = qdrant_models.PointStruct(
            id=point_id,
            vector=vector,
            payload=payload
        )
        
        client.upsert(collection_name=collection_name, points=[point])
        logger.debug(f"Stored data with location in Qdrant collection {collection_name}: {point_id}")
        
        return point_id
        
    except Exception as e:
        logger.error(f"Error storing data with location in Qdrant: {e}", exc_info=True)
        raise e

@router.get("/cameras/search-by-location")
async def search_cameras_by_location_criteria(
    location: Optional[str] = Query(None, description="Filter by exact location"),
    area: Optional[str] = Query(None, description="Filter by area"),
    building: Optional[str] = Query(None, description="Filter by building"),
    floor_level: Optional[str] = Query(None, description="Filter by floor_level"),
    zone: Optional[str] = Query(None, description="Filter by zone"),
    status: Optional[str] = Query(None, description="Filter by camera status"),
    search_term: Optional[str] = Query(None, description="Search in camera names"),
    group_by: str = Query("location", regex="^(location|area|building|floor_level|zone|none)$"),
    include_inactive: bool = Query(False, description="Include inactive cameras"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Advanced camera search with location-based grouping and filtering."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"cameras": [], "groups": [], "total_count": 0, "group_type": group_by}
        
        membership_details = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        # Build dynamic query
        conditions = ["vs.workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        # Location filters
        if location:
            param_count += 1
            conditions.append(f"vs.location ILIKE ${param_count}")
            params.append(f"%{location}%")
        if area:
            param_count += 1
            conditions.append(f"vs.area ILIKE ${param_count}")
            params.append(f"%{area}%")
        if building:
            param_count += 1
            conditions.append(f"vs.building ILIKE ${param_count}")
            params.append(f"%{building}%")
        if floor_level:
            param_count += 1
            conditions.append(f"vs.floor_level ILIKE ${param_count}")
            params.append(f"%{floor_level}%")
        if zone:
            param_count += 1
            conditions.append(f"vs.zone ILIKE ${param_count}")
            params.append(f"%{zone}%")
        
        # Status filter
        if status:
            param_count += 1
            conditions.append(f"vs.status = ${param_count}")
            params.append(status)
        elif not include_inactive:
            conditions.append("vs.status != 'inactive'")
        
        # Search term filter
        if search_term:
            param_count += 1
            conditions.append(f"vs.name ILIKE ${param_count}")
            params.append(f"%{search_term}%")
        
        # Permission filter
        if user_role_in_workspace != "admin":
            param_count += 1
            conditions.append(f"vs.user_id = ${param_count}")
            params.append(user_id_obj)
        
        where_clause = " AND ".join(conditions)
        
        if group_by == "none":
            # Return flat list without grouping
            query = f"""
                SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                       vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                       vs.latitude, vs.longitude, vs.created_at, vs.updated_at,
                       u.username as owner_username
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE {where_clause}
                ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name
            """
            
            cameras = await db_manager_global_qdrant.execute_query(query, tuple(params), fetch_all=True)
            
            camera_list = []
            for cam in cameras:
                camera_list.append({
                    "id": str(cam["stream_id"]),
                    "name": cam["name"],
                    "path": cam["path"],
                    "type": cam["type"],
                    "status": cam["status"],
                    "is_streaming": cam["is_streaming"],
                    "location": cam["location"],
                    "area": cam["area"],
                    "building": cam["building"],
                    "floor_level": cam["floor_level"],
                    "zone": cam["zone"],
                    "latitude": float(cam["latitude"]) if cam["latitude"] else None,
                    "longitude": float(cam["longitude"]) if cam["longitude"] else None,
                    "owner_username": cam["owner_username"],
                    "created_at": cam["created_at"].isoformat() if cam["created_at"] else None,
                    "updated_at": cam["updated_at"].isoformat() if cam["updated_at"] else None
                })
            
            return {
                "cameras": camera_list,
                "groups": [],
                "total_count": len(camera_list),
                "group_type": "none"
            }
        
        else:
            # Return grouped results
            group_field = f"vs.{group_by}"
            query = f"""
                SELECT 
                    {group_field} as group_name,
                    COUNT(*) as camera_count,
                    COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_count,
                    COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_count,
                    COUNT(CASE WHEN vs.status = 'error' THEN 1 END) as error_count,
                    COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_count,
                    ARRAY_AGG(
                        JSON_BUILD_OBJECT(
                            'id', vs.stream_id::text,
                            'name', vs.name,
                            'path', vs.path,
                            'type', vs.type,
                            'status', vs.status,
                            'is_streaming', vs.is_streaming,
                            'location', vs.location,
                            'area', vs.area,
                            'building', vs.building,
                            'floor_level', vs.floor_level,
                            'zone', vs.zone,
                            'latitude', vs.latitude,
                            'longitude', vs.longitude,
                            'owner_username', u.username,
                            'created_at', vs.created_at,
                            'updated_at', vs.updated_at
                        ) ORDER BY vs.name
                    ) as cameras
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE {where_clause} AND {group_field} IS NOT NULL
                GROUP BY {group_field}
                ORDER BY {group_field}
            """
            
            results = await db_manager_global_qdrant.execute_query(query, tuple(params), fetch_all=True)
            
            groups = []
            total_cameras = 0
            
            for result in results:
                # Process cameras in the group
                processed_cameras = []
                if result["cameras"]:
                    for cam in result["cameras"]:
                        cam_data = cam.copy()
                        if cam_data.get("created_at"):
                            cam_data["created_at"] = cam_data["created_at"].isoformat()
                        if cam_data.get("updated_at"):
                            cam_data["updated_at"] = cam_data["updated_at"].isoformat()
                        if cam_data.get("latitude"):
                            cam_data["latitude"] = float(cam_data["latitude"])
                        if cam_data.get("longitude"):
                            cam_data["longitude"] = float(cam_data["longitude"])
                        processed_cameras.append(cam_data)
                
                group = CameraLocationGroup(
                    group_type=group_by,
                    group_name=result["group_name"] or "Unknown",
                    camera_count=result["camera_count"],
                    cameras=processed_cameras,
                    active_count=result["active_count"] or 0,
                    inactive_count=result["inactive_count"] or 0,
                    streaming_count=result["streaming_count"] or 0
                )
                groups.append(group)
                total_cameras += group.camera_count
            
            return {
                "cameras": [],
                "groups": [group.model_dump() for group in groups],
                "total_count": total_cameras,
                "group_type": group_by
            }
            
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error in location-based camera search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to search cameras by location.")

@router.get("/locations/{location_name}/cameras")
async def get_cameras_in_location(
    location_name: str,
    status_filter: Optional[str] = Query(None, regex="^(active|inactive|error|processing)$"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all cameras in a specific location."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return []
        
        membership_details = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant, required_role=None)
        user_role_in_workspace = membership_details.get("role")
        
        conditions = ["vs.workspace_id = $1", "vs.location = $2"]
        params = [workspace_id_obj, location_name]
        
        if status_filter:
            conditions.append("vs.status = $3")
            params.append(status_filter)
        
        if user_role_in_workspace != "admin":
            param_count = len(params) + 1
            conditions.append(f"vs.user_id = ${param_count}")
            params.append(user_id_obj)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT vs.*, u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE {where_clause}
            ORDER BY vs.name
        """
        
        cameras = await db_manager_global_qdrant.execute_query(query, tuple(params), fetch_all=True)
        
        return [
            {
                "id": str(cam["stream_id"]),
                "name": cam["name"],
                "path": cam["path"],
                "type": cam["type"],
                "status": cam["status"],
                "is_streaming": cam["is_streaming"],
                "location": cam["location"],
                "area": cam["area"],
                "building": cam["building"],
                "floor_level": cam["floor_level"],
                "zone": cam["zone"],
                "latitude": float(cam["latitude"]) if cam["latitude"] else None,
                "longitude": float(cam["longitude"]) if cam["longitude"] else None,
                "owner_username": cam["owner_username"],
                "created_at": cam["created_at"].isoformat() if cam["created_at"] else None,
                "updated_at": cam["updated_at"].isoformat() if cam["updated_at"] else None
            } for cam in cameras
        ]
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting cameras in location {location_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get cameras in location.")

@router.get("/locations/export")
async def export_location_data(
    format_type: str = Query("csv", regex="^(csv|json)$"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Export camera location data."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            raise HTTPException(status_code=404, detail="No active workspace found")
        
        await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant, required_role=None)
        
        query = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                   vs.latitude, vs.longitude, vs.created_at, vs.updated_at,
                   u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1
            ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name
        """
        
        cameras = await db_manager_global_qdrant.execute_query(query, (workspace_id_obj,), fetch_all=True)
        
        if format_type == "csv":
            import io
            import csv
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                "camera_id", "name", "path", "type", "status", "is_streaming",
                "location", "area", "building", "floor_level", "zone",
                "latitude", "longitude", "owner_username", "created_at", "updated_at"
            ])
            
            # Write data
            for cam in cameras:
                writer.writerow([
                    str(cam["stream_id"]), cam["name"], cam["path"], cam["type"], 
                    cam["status"], cam["is_streaming"], cam["location"], cam["area"],
                    cam["building"], cam["floor_level"], cam["zone"],
                    cam["latitude"], cam["longitude"], cam["owner_username"],
                    cam["created_at"].isoformat() if cam["created_at"] else "",
                    cam["updated_at"].isoformat() if cam["updated_at"] else ""
                ])
            
            from fastapi.responses import Response
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=camera_locations_{workspace_id_obj}.csv"}
            )
        
        else:  # JSON format
            export_data = []
            for cam in cameras:
                export_data.append({
                    "camera_id": str(cam["stream_id"]),
                    "name": cam["name"],
                    "path": cam["path"],
                    "type": cam["type"],
                    "status": cam["status"],
                    "is_streaming": cam["is_streaming"],
                    "location": cam["location"],
                    "area": cam["area"],
                    "building": cam["building"],
                    "floor_level": cam["floor_level"],
                    "zone": cam["zone"],
                    "latitude": float(cam["latitude"]) if cam["latitude"] else None,
                    "longitude": float(cam["longitude"]) if cam["longitude"] else None,
                    "owner_username": cam["owner_username"],
                    "created_at": cam["created_at"].isoformat() if cam["created_at"] else None,
                    "updated_at": cam["updated_at"].isoformat() if cam["updated_at"] else None
                })
            
            return JSONResponse(
                content={
                    "workspace_id": str(workspace_id_obj),
                    "export_timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat(),
                    "total_cameras": len(export_data),
                    "cameras": export_data
                }
            )
            
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error exporting location data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export location data.")


# === QDRANT-BASED HIERARCHICAL LOCATION ENDPOINTS ===

async def get_qdrant_location_data(
    workspace_id_obj: UUID,
    user_system_role: str,
    user_workspace_role: Optional[str],
    requesting_username: str,
    location_field: Optional[str] = None,  # Which field to extract
    filter_conditions: Optional[Dict[str, Union[str, List[str]]]] = None  # Additional filters
) -> List[Dict[str, Any]]:
    """
    Helper function to extract location data from Qdrant collection.
    
    Args:
        workspace_id_obj: The workspace UUID
        user_system_role: User's system role
        user_workspace_role: User's role in the workspace
        requesting_username: Username for filtering
        location_field: Specific field to extract (location, area, building, etc.)
        filter_conditions: Additional filter conditions for hierarchical filtering
    
    Returns:
        List of unique location data with counts
    """
    try:
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build base filter for user permissions
        must_conditions = []
        
        # Apply user access control
        apply_username_filter = True
        if user_system_role == 'admin':
            apply_username_filter = False
        elif user_workspace_role in ['admin', 'owner']:
            apply_username_filter = False

        if apply_username_filter:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="username", 
                    match=qdrant_models.MatchValue(value=requesting_username)
                )
            )
        
        # Add hierarchical filter conditions
        if filter_conditions:
            for field_name, field_values in filter_conditions.items():
                if field_values:
                    if isinstance(field_values, str):
                        field_values = [field_values]
                    
                    if len(field_values) == 1:
                        must_conditions.append(
                            qdrant_models.FieldCondition(
                                key=field_name, 
                                match=qdrant_models.MatchValue(value=field_values[0])
                            )
                        )
                    else:
                        must_conditions.append(
                            qdrant_models.Filter(should=[
                                qdrant_models.FieldCondition(
                                    key=field_name, 
                                    match=qdrant_models.MatchValue(value=val)
                                ) for val in field_values
                            ])
                        )
        
        # Create filter object
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Fetch all points with location data
        # We need to get all points to properly count unique combinations
        all_points = []
        offset = None
        batch_size = 1000
        
        while True:
            points, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=batch_size,
                offset=offset,
                with_payload=["camera_id", "name", "location", "area", "building", "floor_level", "zone"],
                with_vectors=False
            )
            
            all_points.extend(points)
            
            if next_offset is None or len(points) < batch_size:
                break
            offset = next_offset
        
        # Process the points to extract location data
        location_data = defaultdict(lambda: {
            'count': 0, 
            'cameras': set(),
            'details': {}
        })
        
        for point in all_points:
            if not point.payload:
                continue
                
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            # Extract location fields
            location_info = {
                'location': point.payload.get('location'),
                'area': point.payload.get('area'),
                'building': point.payload.get('building'),
                'floor_level': point.payload.get('floor_level'),
                'zone': point.payload.get('zone'),
                'camera_name': point.payload.get('name', 'Unknown Camera')
            }
            
            # Create a key based on what we're grouping by
            if location_field:
                key_value = location_info.get(location_field)
                if key_value:
                    location_data[key_value]['cameras'].add(camera_id)
                    location_data[key_value]['details'] = location_info
            else:
                # If no specific field, group by all location data
                key = tuple(sorted([(k, v) for k, v in location_info.items() 
                                  if k != 'camera_name' and v is not None]))
                if key:
                    location_data[key]['cameras'].add(camera_id)
                    location_data[key]['details'] = location_info
        
        # Convert to final format
        result = []
        for key, data in location_data.items():
            if location_field:
                # Single field grouping
                result.append({
                    location_field: key,
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras']),
                    **{k: v for k, v in data['details'].items() 
                       if k != location_field and k != 'camera_name' and v is not None}
                })
            else:
                # Multi-field grouping
                details = data['details']
                result.append({
                    **details,
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting location data from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to extract location data: {str(e)}")

@router.get("/qdrant/locations/list")
async def get_qdrant_locations(
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all unique locations from Qdrant data for the current user in their workspace."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"locations": [], "total_count": 0}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        # Get location data from Qdrant
        location_data = await get_qdrant_location_data(
            workspace_id_obj=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username,
            location_field="location"
        )
        
        # Format response
        locations = [
            {
                "location": item["location"],
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in location_data if item.get("location")
        ]
        
        # Sort by location name
        locations.sort(key=lambda x: x["location"])
        
        return {
            "locations": locations,
            "total_count": len(locations),
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve locations from Qdrant.")

@router.get("/qdrant/areas/list")
async def get_qdrant_areas(
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all unique areas from Qdrant data, optionally filtered by location(s)."""
    try:
        locations = parse_string_or_list(locations)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"areas": [], "total_count": 0, "filtered_by_locations": locations}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        # Prepare filter conditions
        filter_conditions = {}
        if locations:
            if isinstance(locations, str):
                locations = [locations]
            filter_conditions["location"] = locations
        
        # Get area data from Qdrant
        area_data = await get_qdrant_location_data(
            workspace_id_obj=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username,
            location_field="area",
            filter_conditions=filter_conditions
        )
        
        # Format response
        areas = [
            {
                "area": item["area"],
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in area_data if item.get("area")
        ]
        
        # Sort by area name
        areas.sort(key=lambda x: (x["area"], x.get("location", "")))
        
        return {
            "areas": areas,
            "total_count": len(areas),
            "filtered_by_locations": locations,
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant areas: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve areas from Qdrant.")

@router.get("/qdrant/buildings/list")
async def get_qdrant_buildings(
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all unique buildings from Qdrant data, optionally filtered by area(s)."""
    try:
        areas = parse_string_or_list(areas)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"buildings": [], "total_count": 0, "filtered_by_areas": areas}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        # Prepare filter conditions
        filter_conditions = {}
        if areas:
            if isinstance(areas, str):
                areas = [areas]
            filter_conditions["area"] = areas
        
        # Get building data from Qdrant
        building_data = await get_qdrant_location_data(
            workspace_id_obj=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username,
            location_field="building",
            filter_conditions=filter_conditions
        )
        
        # Format response
        buildings = [
            {
                "building": item["building"],
                "area": item.get("area"),
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in building_data if item.get("building")
        ]
        
        # Sort by building name
        buildings.sort(key=lambda x: (x["building"], x.get("area", ""), x.get("location", "")))
        
        return {
            "buildings": buildings,
            "total_count": len(buildings),
            "filtered_by_areas": areas,
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant buildings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve buildings from Qdrant.")

@router.get("/qdrant/floor-levels/list")
async def get_qdrant_floor_levels(
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all unique floor levels from Qdrant data, optionally filtered by building(s)."""
    try:
        buildings = parse_string_or_list(buildings)

        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"floor_levels": [], "total_count": 0, "filtered_by_buildings": buildings}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        # Prepare filter conditions
        filter_conditions = {}
        if buildings:
            if isinstance(buildings, str):
                buildings = [buildings]
            filter_conditions["building"] = buildings
        
        # Get floor level data from Qdrant
        floor_level_data = await get_qdrant_location_data(
            workspace_id_obj=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username,
            location_field="floor_level",
            filter_conditions=filter_conditions
        )
        
        # Format response
        floor_levels = [
            {
                "floor_level": item["floor_level"],
                "building": item.get("building"),
                "area": item.get("area"),
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in floor_level_data if item.get("floor_level")
        ]
        
        # Sort by floor level
        floor_levels.sort(key=lambda x: (x["floor_level"], x.get("building", ""), x.get("area", ""), x.get("location", "")))
        
        return {
            "floor_levels": floor_levels,
            "total_count": len(floor_levels),
            "filtered_by_buildings": buildings,
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant floor levels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve floor levels from Qdrant.")

@router.get("/qdrant/zones/list")
async def get_qdrant_zones(
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all unique zones from Qdrant data, optionally filtered by floor level(s)."""
    try:
        floor_levels = parse_string_or_list(floor_levels)
        
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"zones": [], "total_count": 0, "filtered_by_floor_levels": floor_levels}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        # Prepare filter conditions
        filter_conditions = {}
        if floor_levels:
            if isinstance(floor_levels, str):
                floor_levels = [floor_levels]
            filter_conditions["floor_level"] = floor_levels
        
        # Get zone data from Qdrant
        zone_data = await get_qdrant_location_data(
            workspace_id_obj=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_specific_role,
            requesting_username=username,
            location_field="zone",
            filter_conditions=filter_conditions
        )
        
        # Format response
        zones = [
            {
                "zone": item["zone"],
                "floor_level": item.get("floor_level"),
                "building": item.get("building"),
                "area": item.get("area"),
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in zone_data if item.get("zone")
        ]
        
        # Sort by zone name
        zones.sort(key=lambda x: (x["zone"], x.get("floor_level", ""), x.get("building", ""), x.get("area", ""), x.get("location", "")))
        
        return {
            "zones": zones,
            "total_count": len(zones),
            "filtered_by_floor_levels": floor_levels,
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant zones: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve zones from Qdrant.")

# === QDRANT LOCATION ANALYTICS ENDPOINTS ===

@router.get("/qdrant/locations/analytics")
async def get_qdrant_location_analytics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    group_by: str = Query("location", regex="^(location|area|building|floor_level|zone)$"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get analytics data grouped by location hierarchy from Qdrant."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"analytics": [], "total_count": 0, "group_by": group_by}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build search query with date/time filters
        search_query = SearchQuery(
            camera_id=None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        filter_obj = build_filter_from_query(
            search_query, system_role, workspace_specific_role, username
        )
        
        # Fetch all matching points
        all_points = []
        offset = None
        batch_size = 1000
        
        while True:
            points, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=batch_size,
                offset=offset,
                with_payload=["camera_id", "name", "location", "area", "building", 
                             "floor_level", "zone", "person_count", "timestamp"],
                with_vectors=False
            )
            
            all_points.extend(points)
            
            if next_offset is None or len(points) < batch_size:
                break
            offset = next_offset
        
        # Group data by specified field
        analytics_data = defaultdict(lambda: {
            'data_points': 0,
            'total_person_count': 0,
            'average_person_count': 0,
            'unique_cameras': set(),
            'time_range': {'earliest': None, 'latest': None}
        })
        
        for point in all_points:
            if not point.payload:
                continue
            
            group_key = point.payload.get(group_by)
            if not group_key:
                continue
            
            person_count = point.payload.get('person_count', 0)
            timestamp = point.payload.get('timestamp')
            camera_id = point.payload.get('camera_id')
            
            data = analytics_data[group_key]
            data['data_points'] += 1
            data['total_person_count'] += person_count
            
            if camera_id:
                data['unique_cameras'].add(camera_id)
            
            if timestamp:
                if data['time_range']['earliest'] is None or timestamp < data['time_range']['earliest']:
                    data['time_range']['earliest'] = timestamp
                if data['time_range']['latest'] is None or timestamp > data['time_range']['latest']:
                    data['time_range']['latest'] = timestamp
        
        # Calculate averages and format response
        analytics = []
        for group_name, data in analytics_data.items():
            if data['data_points'] > 0:
                data['average_person_count'] = data['total_person_count'] / data['data_points']
            
            # Convert timestamps to readable format
            time_range = {}
            if data['time_range']['earliest']:
                time_range['earliest'] = datetime.fromtimestamp(
                    data['time_range']['earliest'], tz=timezone.utc
                ).isoformat()
            if data['time_range']['latest']:
                time_range['latest'] = datetime.fromtimestamp(
                    data['time_range']['latest'], tz=timezone.utc
                ).isoformat()
            
            analytics.append({
                group_by: group_name,
                'data_points': data['data_points'],
                'total_person_count': data['total_person_count'],
                'average_person_count': round(data['average_person_count'], 2),
                'unique_cameras': len(data['unique_cameras']),
                'camera_ids': list(data['unique_cameras']),
                'time_range': time_range
            })
        
        # Sort by total person count (descending)
        analytics.sort(key=lambda x: x['total_person_count'], reverse=True)
        
        return {
            "analytics": analytics,
            "total_groups": len(analytics),
            "group_by": group_by,
            "filters_applied": search_query.model_dump(exclude_none=True),
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant location analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location analytics from Qdrant.")

# === ADDITIONAL UTILITY ENDPOINTS FOR QDRANT LOCATION DATA ===

@router.get("/qdrant/locations/summary")
async def get_qdrant_location_summary(
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get a comprehensive summary of all location data from Qdrant."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {
                "summary": {
                    "locations": 0, "areas": 0, "buildings": 0, 
                    "floor_levels": 0, "zones": 0, "total_cameras": 0
                },
                "hierarchy": []
            }
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter for user permissions
        must_conditions = []
        apply_username_filter = True
        if system_role == 'admin':
            apply_username_filter = False
        elif workspace_specific_role in ['admin', 'owner']:
            apply_username_filter = False

        if apply_username_filter:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="username", 
                    match=qdrant_models.MatchValue(value=username)
                )
            )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Get all points with location data
        all_points = []
        offset = None
        batch_size = 1000
        
        while True:
            points, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=batch_size,
                offset=offset,
                with_payload=["camera_id", "name", "location", "area", "building", "floor_level", "zone"],
                with_vectors=False
            )
            
            all_points.extend(points)
            
            if next_offset is None or len(points) < batch_size:
                break
            offset = next_offset
        
        # Process the data to create summary
        unique_values = {
            'locations': set(),
            'areas': set(),
            'buildings': set(),
            'floor_levels': set(),
            'zones': set(),
            'cameras': set()
        }
        
        hierarchy_map = defaultdict(lambda: {
            'cameras': set(),
            'details': {}
        })
        
        for point in all_points:
            if not point.payload:
                continue
                
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            unique_values['cameras'].add(camera_id)
            
            # Extract location fields
            location = point.payload.get('location')
            area = point.payload.get('area')
            building = point.payload.get('building')
            floor_level = point.payload.get('floor_level')
            zone = point.payload.get('zone')
            
            if location:
                unique_values['locations'].add(location)
            if area:
                unique_values['areas'].add(area)
            if building:
                unique_values['buildings'].add(building)
            if floor_level:
                unique_values['floor_levels'].add(floor_level)
            if zone:
                unique_values['zones'].add(zone)
            
            # Create hierarchy key
            hierarchy_key = (building or 'Unknown Building', 
                           floor_level or 'Unknown Floor', 
                           zone or 'Unknown Zone', 
                           area or 'Unknown Area', 
                           location or 'Unknown Location')
            
            hierarchy_map[hierarchy_key]['cameras'].add(camera_id)
            hierarchy_map[hierarchy_key]['details'] = {
                'building': building,
                'floor_level': floor_level,
                'zone': zone,
                'area': area,
                'location': location
            }
        
        # Create hierarchy list
        hierarchy = []
        for key, data in hierarchy_map.items():
            hierarchy.append({
                **data['details'],
                'camera_count': len(data['cameras']),
                'camera_ids': list(data['cameras'])
            })
        
        # Sort hierarchy by building, floor, zone, area, location
        # Handle None values by converting them to empty strings for sorting
        hierarchy.sort(key=lambda x: (
            x.get('building') or '', 
            x.get('floor_level') or '', 
            x.get('zone') or '', 
            x.get('area') or '', 
            x.get('location') or ''
        ))
        
        summary = {
            'locations': len(unique_values['locations']),
            'areas': len(unique_values['areas']),
            'buildings': len(unique_values['buildings']),
            'floor_levels': len(unique_values['floor_levels']),
            'zones': len(unique_values['zones']),
            'total_cameras': len(unique_values['cameras']),
            'unique_cameras': list(unique_values['cameras'])
        }
        
        return {
            "summary": summary,
            "hierarchy": hierarchy,
            "workspace_id": str(workspace_id_obj),
            "data_source": "qdrant"
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant location summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve location summary from Qdrant.")

@router.get("/qdrant/locations/cameras/{location_name}")
async def get_qdrant_cameras_in_location(
    location_name: str,
    include_recent_data: bool = Query(False, description="Include recent detection data"),
    limit: int = Query(100, ge=1, le=1000, description="Limit number of results"),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Get all cameras and optionally their recent data for a specific location from Qdrant."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"cameras": [], "location": location_name}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter conditions
        must_conditions = [
            qdrant_models.FieldCondition(
                key="location", 
                match=qdrant_models.MatchValue(value=location_name)
            )
        ]
        
        # Apply user access control
        apply_username_filter = True
        if system_role == 'admin':
            apply_username_filter = False
        elif workspace_specific_role in ['admin', 'owner']:
            apply_username_filter = False

        if apply_username_filter:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="username", 
                    match=qdrant_models.MatchValue(value=username)
                )
            )
        
        filter_obj = qdrant_models.Filter(must=must_conditions)
        
        # Get points for this location
        points, _ = client.scroll(
            collection_name=collection_name,
            scroll_filter=filter_obj,
            limit=limit,
            offset=None,
            with_payload=True,
            with_vectors=False,
            order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC)
        )
        
        if not include_recent_data:
            # Just return unique cameras
            unique_cameras = {}
            for point in points:
                if point.payload:
                    camera_id = point.payload.get('camera_id')
                    if camera_id and camera_id not in unique_cameras:
                        unique_cameras[camera_id] = {
                            'camera_id': camera_id,
                            'name': point.payload.get('name', 'Unknown Camera'),
                            'location': point.payload.get('location'),
                            'area': point.payload.get('area'),
                            'building': point.payload.get('building'),
                            'floor_level': point.payload.get('floor_level'),
                            'zone': point.payload.get('zone')
                        }
            
            return {
                "cameras": list(unique_cameras.values()),
                "location": location_name,
                "total_unique_cameras": len(unique_cameras),
                "include_recent_data": False
            }
        
        else:
            # Return cameras with their recent detection data
            camera_data = defaultdict(list)
            
            for point in points:
                if point.payload:
                    camera_id = point.payload.get('camera_id')
                    if camera_id:
                        timestamp = point.payload.get('timestamp')
                        dt = None
                        if timestamp:
                            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        
                        camera_data[camera_id].append({
                            'id': str(point.id),
                            'timestamp': timestamp,
                            'datetime': dt.isoformat() if dt else None,
                            'person_count': point.payload.get('person_count', 0),
                            'camera_name': point.payload.get('name', 'Unknown Camera'),
                            'location_details': {
                                'location': point.payload.get('location'),
                                'area': point.payload.get('area'),
                                'building': point.payload.get('building'),
                                'floor_level': point.payload.get('floor_level'),
                                'zone': point.payload.get('zone')
                            }
                        })
            
            # Format the response
            cameras_with_data = []
            for camera_id, detections in camera_data.items():
                # Sort detections by timestamp (most recent first)
                detections.sort(key=lambda x: x['timestamp'] or 0, reverse=True)
                
                # Get camera info from most recent detection
                latest_detection = detections[0]
                
                cameras_with_data.append({
                    'camera_id': camera_id,
                    'name': latest_detection['camera_name'],
                    'location_details': latest_detection['location_details'],
                    'recent_detections': detections[:10],  # Limit to 10 most recent
                    'total_detections': len(detections),
                    'latest_detection_time': latest_detection['datetime']
                })
            
            # Sort cameras by latest detection time
            cameras_with_data.sort(key=lambda x: x['latest_detection_time'] or '', reverse=True)
            
            return {
                "cameras": cameras_with_data,
                "location": location_name,
                "total_unique_cameras": len(cameras_with_data),
                "include_recent_data": True,
                "data_limit": limit
            }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Qdrant cameras in location {location_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve cameras in location from Qdrant.")

@router.get("/qdrant/locations/search")
async def search_qdrant_location_data(
    search_term: str = Query(..., min_length=1, description="Search term for location names"),
    search_fields: List[str] = Query(
        default=["location", "area", "building", "floor_level", "zone"], 
        description="Fields to search in"
    ),
    exact_match: bool = Query(False, description="Use exact match instead of partial"),
    limit: int = Query(50, ge=1, le=200),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency)
):
    """Search location data in Qdrant collection."""
    try:
        username = current_user_data["username"]
        user_id_obj = current_user_data["user_id"]
        
        user_db_info = await user_manager_global_qdrant.get_user_by_id(user_id_obj)
        if not user_db_info:
            raise HTTPException(status_code=401, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _user_id_from_ws_check, workspace_id_obj = await get_user_and_workspace_refined(username, user_manager_global_qdrant)
        if not workspace_id_obj:
            return {"results": [], "search_term": search_term}
        
        # Check workspace permissions
        workspace_specific_role = None
        if system_role != "admin":
            try:
                member_info = await check_workspace_membership_and_get_role_async(user_id_obj, workspace_id_obj, db_manager_global_qdrant)
                workspace_specific_role = member_info.get("role")
            except HTTPException as e_ws_access:
                raise e_ws_access
        
        client = get_qdrant_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build search conditions
        search_conditions = []
        valid_fields = ["location", "area", "building", "floor_level", "zone"]
        
        for field in search_fields:
            if field in valid_fields:
                if exact_match:
                    search_conditions.append(
                        qdrant_models.FieldCondition(
                            key=field, 
                            match=qdrant_models.MatchValue(value=search_term)
                        )
                    )
                else:
                    # For partial matching, we'll need to get all data and filter in Python
                    # since Qdrant doesn't support LIKE operations in filters
                    pass
        
        # Base filter for user permissions
        must_conditions = []
        apply_username_filter = True
        if system_role == 'admin':
            apply_username_filter = False
        elif workspace_specific_role in ['admin', 'owner']:
            apply_username_filter = False

        if apply_username_filter:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="username", 
                    match=qdrant_models.MatchValue(value=username)
                )
            )
        
        if exact_match and search_conditions:
            # Add search conditions to must_conditions for exact match
            must_conditions.append(
                qdrant_models.Filter(should=search_conditions)
            )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Get points
        all_points = []
        offset = None
        batch_size = 1000
        
        while len(all_points) < limit * 10:  # Get more than needed for filtering
            points, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=min(batch_size, limit * 10 - len(all_points)),
                offset=offset,
                with_payload=["camera_id", "name", "location", "area", "building", "floor_level", "zone"],
                with_vectors=False
            )
            
            all_points.extend(points)
            
            if next_offset is None or len(points) < batch_size:
                break
            offset = next_offset
        
        # Filter results based on search term
        matching_locations = defaultdict(lambda: {
            'cameras': set(),
            'details': {},
            'match_fields': set()
        })
        
        search_term_lower = search_term.lower()
        
        for point in all_points:
            if not point.payload:
                continue
            
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            # Check if any of the search fields match
            match_found = False
            matched_fields = []
            
            for field in search_fields:
                if field in valid_fields:
                    field_value = point.payload.get(field)
                    if field_value:
                        if exact_match:
                            if field_value.lower() == search_term_lower:
                                match_found = True
                                matched_fields.append(field)
                        else:
                            if search_term_lower in field_value.lower():
                                match_found = True
                                matched_fields.append(field)
            
            if match_found:
                # Create a key for grouping
                location_key = (
                    point.payload.get('location', ''),
                    point.payload.get('area', ''),
                    point.payload.get('building', ''),
                    point.payload.get('floor_level', ''),
                    point.payload.get('zone', '')
                )
                
                matching_locations[location_key]['cameras'].add(camera_id)
                matching_locations[location_key]['details'] = {
                    'location': point.payload.get('location'),
                    'area': point.payload.get('area'),
                    'building': point.payload.get('building'),
                    'floor_level': point.payload.get('floor_level'),
                    'zone': point.payload.get('zone')
                }
                matching_locations[location_key]['match_fields'].update(matched_fields)
        
        # Format results
        results = []
        for key, data in list(matching_locations.items())[:limit]:
            results.append({
                **data['details'],
                'camera_count': len(data['cameras']),
                'camera_ids': list(data['cameras']),
                'matched_fields': list(data['match_fields'])
            })
        
        # Sort by relevance (number of matching fields, then camera count)
        results.sort(key=lambda x: (len(x['matched_fields']), x['camera_count']), reverse=True)
        
        return {
            "results": results,
            "search_term": search_term,
            "search_fields": search_fields,
            "exact_match": exact_match,
            "total_matches": len(results),
            "workspace_id": str(workspace_id_obj)
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error searching Qdrant location data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to search location data in Qdrant.")

