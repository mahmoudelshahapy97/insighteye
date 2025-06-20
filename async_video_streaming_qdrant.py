# async_video_streaming_qdrant.py
from fastapi import APIRouter, HTTPException, Query, Request, Depends, status
from fastapi.responses import JSONResponse
from typing import Optional, List, Union, Dict, Any, Tuple
from uuid import UUID, uuid4
import asyncio
from async_config import config
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models 
from datetime import datetime, time as dt_time, timezone 
import logging

# Updated imports for asynchronous managers
from async_user_manager import UserManager as AsyncUserManager
from async_session_manager import SessionManager as AsyncSessionManager
from async_database import DatabaseManager as AsyncDatabaseManager

from schemas_models import CreateCollectionRequest, SearchQuery, TimestampRangeResponse, CameraIdsResponse, DeleteDataRequest
from utils import (
    parse_camera_ids, make_prediction, parse_date_format, paginate_list, parse_time_string,
    get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists
)
# The following utility functions are assumed to be updated to be async
# and use the AsyncDatabaseManager if they interact with the database.
# For this conversion, placeholders/refined versions are provided below.

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
    Note: Database pool initialization (init_db_pool from async_database.py)
    should be handled by the main FastAPI application's startup event
    to resolve warnings like "DB pool not available".
    """
    get_qdrant_client()

# --- Placeholder/Refined Async Helper Functions ---
# These would typically reside in utils.py or workspaces.py and be imported.
# They are crucial for the application's RBAC and workspace logic.

async def get_user_and_workspace_async_refined(username: str, 
                                         user_manager: AsyncUserManager
                                         ) -> Tuple[Optional[Dict], Optional[UUID]]:
    """
    Gets user details and their active workspace ID.
    (Placeholder, ideally uses fully implemented UserManager.get_active_workspace)
    """
    user_details = await user_manager.get_user_by_username(username)
    if not user_details:
        logger.debug(f"User '{username}' not found by get_user_and_workspace_async_refined.")
        return None, None
    
    user_id_obj = user_details.get("user_id") # Should be UUID from async_user_manager
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


# --- API Endpoints (Refactored for Async) ---

@router.get("/workspace/search_results_v2")
async def workspace_search_results_v2(
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
        requesting_user_id_obj = current_user_data["user_id"] # This is UUID from async_session_manager
        username = current_user_data["username"]
        user_db_info = await user_manager_global_qdrant.get_user_by_id(requesting_user_id_obj) # Pass UUID
        if not user_db_info: raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")

        if workspace_id_query:
            try: final_workspace_id_obj = UUID(workspace_id_query)
            except ValueError: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant)
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
        
        can_search_feature = user_db_info.get("is_search", False) or \
                             is_system_admin or \
                             (workspace_specific_role in ["admin", "owner"])
        if not can_search_feature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable for your account in this workspace context.")

        target_collection_name = get_workspace_qdrant_collection_name(final_workspace_id_obj)
        ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)
        
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
        if total_count > 0:
            num_of_pages = (total_count + per_page - 1) // per_page
            if page <= num_of_pages:
                offset = (page - 1) * per_page
                points_for_current_page, _ = client.scroll(
                    collection_name=target_collection_name, scroll_filter=filter_obj,
                    limit=per_page, offset=offset, with_payload=True, with_vectors=False
                )
                for point_item in points_for_current_page:
                    if point_item.payload: 
                        paginated_data.append({
                            "id": str(point_item.id), "frame": point_item.payload.get("frame_base64"), 
                            "metadata": {
                                "camera_id": point_item.payload.get("camera_id"),
                                "name": point_item.payload.get("name", "Unknown Camera"),
                                "timestamp": point_item.payload.get("timestamp"),
                                "date": point_item.payload.get("date"), "time": point_item.payload.get("time"),
                                "person_count": point_item.payload.get("person_count", 0),
                                "owner_username": point_item.payload.get("username")
                            }
                        })
        return JSONResponse(content={
            "data": paginated_data, "current_page": page, "num_of_pages": num_of_pages,
            "total_count": total_count, "per_page": per_page,
            "search_scope": {
                "workspace_id": str(final_workspace_id_obj), "collection_queried": target_collection_name, 
                "filters_applied": search_query_model.model_dump(exclude_none=True),
                "access_level": "system_admin" if is_system_admin else (workspace_specific_role or "member_or_undefined")
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
            _, active_ws_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant)
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
        ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)
        
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

# process_camera_prediction: No direct DB manager calls, uses Qdrant.
# If its `build_filter_from_query` needs user context for security, it should be passed.
# The current usage in the file (not shown) would determine this.
# For now, keeping its signature as is, assuming context is handled by caller or not needed here.
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
        _, active_ws_id_obj = await get_user_and_workspace_async_refined(username_context, user_manager_global_qdrant)
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
    ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)

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

# process_single_camera_prediction: No direct DB manager calls.
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
            with_payload=["timestamp", "person_count", "camera_id"], with_vectors=False,
            order_by=qdrant_models.OrderBy(key="timestamp", direction=qdrant_models.Direction.DESC))
        if not points: return None 
        prediction_input_data = [{"metadata": {"camera_id": p.payload.get("camera_id"), "timestamp": p.payload.get("timestamp"), "person_count": p.payload.get("person_count", 0)}} for p in points if p.payload]
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
            _, active_ws_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant)
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
        ensure_workspace_qdrant_collection_exists(client, final_workspace_id_obj)

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
    page: int = Query(1, ge=1), per_page: int = Query(10, ge=1, le=100),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    username = current_user_data["username"]
    _, active_workspace_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant)
    if not active_workspace_id_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found for user.")
    return await workspace_search_results_v2(workspace_id_query=str(active_workspace_id_obj), camera_id_param=camera_id, start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time, page=page, per_page=per_page, current_user_data=current_user_data)

@router.get("/prediction_data") 
async def prediction_data_user_active_workspace(
    camera_id: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global_qdrant.get_current_user_full_data_dependency) 
):
    username = current_user_data["username"]
    _, active_workspace_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant)
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
        _, active_workspace_id_obj = await get_user_and_workspace_async_refined(username, user_manager_global_qdrant) # active_workspace_id_obj is UUID
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
    ensure_workspace_qdrant_collection_exists(client, target_ws_id_obj)

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

# list_collections: No DB interaction.
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
            ensure_workspace_qdrant_collection_exists(client, workspace_id_of_collection)

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

# perform_search: This function might be used by AI tools or internal systems.
# It should also respect user context if the data it queries is user-scoped.
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

        search_results = [{"id": str(p.id), "frame": p.payload.get("frame_base64"), "metadata": {"camera_id": p.payload.get("camera_id"), "name": p.payload.get("name", "Unknown"), "timestamp": p.payload.get("timestamp"), "date": p.payload.get("date"), "time": p.payload.get("time"), "person_count": p.payload.get("person_count", 0), "owner_username": p.payload.get("username")}} for p in all_points if p.payload]
        return {"results": search_results, "total_count": total_count, "fetched_for_tool": len(search_results)}
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Qdrant tool search error in '{collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Search operation failed: {str(e)}")
