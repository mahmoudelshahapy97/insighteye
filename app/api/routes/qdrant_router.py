# app/routes/qdrant_router.py
from fastapi import APIRouter, HTTPException, Query, Body, Depends, status, Response
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Union, Any
from uuid import UUID
import logging
import io
import csv

from app.services.qdrant_service import qdrant_service
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.schemas import (
    SearchQuery, LocationSearchQuery, DeleteDataRequest, StreamUpdate,
    CreateCollectionRequest, TimestampRangeResponse, CameraIdsResponse
)
from app.utils import parse_camera_ids, parse_string_or_list, get_workspace_qdrant_collection_name

logger = logging.getLogger(__name__)
router = APIRouter(tags=["qdrant_data"], prefix="/qdrant")

# ========== Search Endpoints ==========

@router.get("/workspace/search_results_v2")
async def workspace_search_results_v2(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: Optional[str] = Query(None),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data with optional filters"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await qdrant_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Check search feature access
        can_search = user_db_info.get("is_search", False) or \
                    is_system_admin or \
                    (workspace_role in ["admin", "owner"])
        
        if not can_search:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Search feature unavailable for your account."
            )
        
        # Process per_page parameter
        processed_per_page = None
        if per_page and per_page.lower() not in ["none", "null", ""]:
            try:
                processed_per_page = int(per_page)
                if processed_per_page < 1 or processed_per_page > 100:
                    raise ValueError("Invalid range")
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="per_page must be between 1 and 100"
                )
        elif per_page is None:
            processed_per_page = 10
        
        # Build search query
        search_query = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute search
        results = await qdrant_service.search_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=processed_per_page,
            base64=base64
        )
        
        # Add search scope info
        results["search_scope"] = {
            "workspace_id": str(final_workspace_id),
            "collection_queried": f"person_counts_ws_{str(final_workspace_id).replace('-', '_')}",
            "filters_applied": search_query.model_dump(exclude_none=True),
            "access_level": "system_admin" if is_system_admin else (workspace_role or "member"),
            "base64_frames_included": base64,
            "pagination_disabled": processed_per_page is None
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Workspace search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during workspace data search."
        )

@router.get("/workspace/search_results_v2_ordered")
async def workspace_search_results_v2_ordered(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data with timestamp ordering"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await qdrant_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Build search query
        search_query = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute ordered search
        results = await qdrant_service.search_ordered_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=per_page
        )
        
        results["search_scope"] = {
            "workspace_id": str(final_workspace_id),
            "filters_applied": search_query.model_dump(exclude_none=True),
            "access_level": "system_admin" if is_system_admin else (workspace_role or "member")
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ordered search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during ordered workspace data search."
        )

@router.get("/workspace/search_results_with_location")
async def workspace_search_results_with_location(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: Optional[str] = Query(None),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Enhanced search with location-based filtering"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            final_workspace_id = UUID(workspace_id_query)
        else:
            _, active_ws_id = await qdrant_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(
                requesting_user_id, final_workspace_id
            )
            workspace_role = member_info.get("role")
        
        # Process per_page
        processed_per_page = None
        if per_page and per_page.lower() not in ["none", "null", ""]:
            processed_per_page = int(per_page)
        elif per_page is None:
            processed_per_page = 10
        
        # Build location search query
        search_query = LocationSearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            location=location,
            area=area,
            building=building,
            floor_level=floor_level,
            zone=zone
        )
        
        results = await qdrant_service.search_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=processed_per_page,
            base64=base64
        )

        if "search_scope" not in results:
            results["search_scope"] = {}

        results["search_scope"]["location_filters"] = {
            "location": location,
            "area": area,
            "building": building,
            "floor_level": floor_level,
            "zone": zone
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Location search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during location-based search."
        )

@router.get("/search_results")
async def search_results_user_active_workspace(
    camera_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: Optional[str] = Query(None),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search in user's active workspace"""
    username = current_user_data["username"]
    _, active_workspace_id = await qdrant_service.get_user_workspace_info(username)
    
    if not active_workspace_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active workspace found for user."
        )
    
    return await workspace_search_results_v2(
        workspace_id_query=str(active_workspace_id),
        camera_id_param=camera_id,
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        page=page,
        per_page=per_page,
        base64=base64,
        current_user_data=current_user_data
    )

# ========== Prediction Endpoints ==========

@router.get("/workspace/prediction_data")
async def workspace_prediction_endpoint(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get prediction data for cameras"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            final_workspace_id = UUID(workspace_id_query)
        else:
            _, active_ws_id = await qdrant_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_info = await qdrant_service.check_workspace_membership(
                requesting_user_id, final_workspace_id
            )
            workspace_role = member_info.get("role")
        
        # Check prediction feature access
        can_predict = user_db_info.get("is_prediction", False) or \
                     is_system_admin or \
                     (workspace_role in ["admin", "owner"])
        
        if not can_predict:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Prediction feature unavailable."
            )
        
        # Get camera IDs
        parsed_camera_ids = []
        if camera_id_param:
            raw_ids = parse_camera_ids(camera_id_param)
            if raw_ids:
                placeholders = ', '.join([f'${i+1}' for i in range(len(raw_ids))])
                query = f"""
                    SELECT stream_id, name FROM video_stream 
                    WHERE stream_id IN ({placeholders}) AND workspace_id = ${len(raw_ids) + 1}
                """
                params = tuple(raw_ids + [final_workspace_id])
                cam_names_db = await db_manager.execute_query(query, params, fetch_all=True)
                cam_details_map = {str(row['stream_id']): row['name'] for row in cam_names_db}
                parsed_camera_ids = [(cid, cam_details_map.get(cid, f"Camera {cid[:8]}")) for cid in raw_ids]
        else:
            query = "SELECT stream_id, name FROM video_stream WHERE workspace_id = $1 AND status = 'active'"
            db_cameras = await db_manager.execute_query(query, (final_workspace_id,), fetch_all=True)
            if db_cameras:
                parsed_camera_ids = [(str(cam['stream_id']), cam['name']) for cam in db_cameras]
        
        if not parsed_camera_ids:
            return JSONResponse({"predictions": [], "message": "No cameras found."})
        
        search_query = SearchQuery(
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        predictions = await qdrant_service.get_prediction_data(
            workspace_id=final_workspace_id,
            camera_ids=parsed_camera_ids,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse({"predictions": predictions})
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during prediction."
        )

@router.get("/prediction_data")
async def prediction_data_user_active_workspace(
    camera_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get predictions for user's active workspace"""
    username = current_user_data["username"]
    _, active_workspace_id = await qdrant_service.get_user_workspace_info(username)
    
    if not active_workspace_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active workspace found."
        )
    
    return await workspace_prediction_endpoint(
        workspace_id_query=str(active_workspace_id),
        camera_id_param=camera_id,
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        current_user_data=current_user_data
    )

# ========== Timestamp and Camera Endpoints ==========

@router.get("/timestamp-range", response_model=TimestampRangeResponse)
async def get_timestamp_range_endpoint(
    camera_id: Optional[str] = Query(None),
    workspace_id: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get timestamp range for data"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        system_role = current_user_data.get("role")
        
        # Determine workspace
        if workspace_id:
            target_ws_id = UUID(workspace_id)
            if system_role != "admin":
                await qdrant_service.check_workspace_membership(requesting_user_id, target_ws_id)
        else:
            _, target_ws_id = await qdrant_service.get_user_workspace_info(username)
            if not target_ws_id:
                return TimestampRangeResponse()
        
        # Get workspace role
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(
                requesting_user_id, target_ws_id
            )
            workspace_role = member_info.get("role")
        
        parsed_camera_ids = parse_camera_ids(camera_id) if camera_id else None
        
        return await qdrant_service.get_timestamp_range(
            workspace_id=target_ws_id,
            camera_ids=parsed_camera_ids,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspace_id format.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting timestamp range: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving timestamp range."
        )

@router.get("/camera-ids-alt", response_model=CameraIdsResponse)
async def get_all_camera_ids_for_active_workspace(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all camera IDs for active workspace"""
    try:
        username = current_user_data["username"]
        _, active_workspace_id = await qdrant_service.get_user_workspace_info(username)
        
        if not active_workspace_id:
            return CameraIdsResponse(camera_ids=[], count=0)
        
        cameras = await qdrant_service.get_camera_ids_for_workspace(active_workspace_id)
        
        return CameraIdsResponse(
            camera_ids=[cam["id"] for cam in cameras],
            count=len(cameras)
        )
        
    except Exception as e:
        logger.error(f"Error retrieving camera IDs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving camera IDs."
        )

# ========== Collection Management Endpoints ==========

@router.get("/collections")
async def list_collections(
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """List all Qdrant collections (admin only)"""
    if current_admin_data.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required."
        )
    
    try:
        return await qdrant_service.list_collections()
    except Exception as e:
        logger.error(f"Failed to list collections: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list collections: {str(e)}"
        )

@router.post("/collections")
async def create_qdrant_collection_generic(
    request_body: CreateCollectionRequest,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Create a new Qdrant collection (admin only)"""
    if current_admin_data.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required."
        )
    
    try:
        result = await qdrant_service.create_collection(
            collection_name=request_body.collection_name,
            vector_size=request_body.vector_size,
            distance=request_body.distance
        )
        
        await session_manager.log_action(
            content=f"Admin created Qdrant collection: {request_body.collection_name}",
            user_id=current_admin_data["user_id"],
            action_type="Qdrant_Collection_Created",
            status="success"
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create collection: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create collection: {str(e)}"
        )

@router.delete("/collections/{collection_name}")
async def delete_qdrant_collection(
    collection_name: str,
    current_admin_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete a Qdrant collection (admin only)"""
    if current_admin_data.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required."
        )
    
    try:
        result = await qdrant_service.delete_collection(collection_name)
        
        await session_manager.log_action(
            content=f"Admin deleted Qdrant collection: {collection_name}",
            user_id=current_admin_data["user_id"],
            action_type="Qdrant_Collection_Deleted",
            status="warning"
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete collection: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete collection: {str(e)}"
        )

@router.get("/collections/{collection_name}/count")
async def get_collection_count(
    collection_name: str,
    camera_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get count of points in collection"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        system_role = current_user_data.get("role")
        is_system_admin = (system_role == "admin")
        
        workspace_id = None
        workspace_role = None
        
        # Check if it's a workspace collection
        if collection_name.startswith("person_counts_ws_"):
            try:
                ws_id_str = collection_name.split("_ws_")[1].replace("_", "-")
                workspace_id = UUID(ws_id_str)
            except (IndexError, ValueError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid workspace ID in collection name."
                )
            
            if not is_system_admin:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, workspace_id
                )
                workspace_role = member_info.get("role")
        elif not is_system_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this collection."
            )
        
        search_query = None
        if any([camera_id, start_date, end_date, start_time, end_time]):
            search_query = SearchQuery(
                camera_id=parse_camera_ids(camera_id) if camera_id else None,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time
            )
        
        result = await qdrant_service.get_collection_count(
            collection_name=collection_name,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        result["workspace_id"] = str(workspace_id) if workspace_id else "N/A"
        result["access_level"] = "system_admin" if is_system_admin else (workspace_role or "generic_collection_admin")
        
        return result
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to get collection count: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get collection count: {str(e)}"
        )

# ========== Data Management Endpoints ==========

@router.delete("/delete_data")
async def delete_data_from_workspace_collection(
    request_body: DeleteDataRequest,
    workspace_id_to_target: str = Query(..., description="Target workspace ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete data from workspace collection"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            target_workspace_id = UUID(workspace_id_to_target)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions - must be workspace admin/owner or system admin
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, target_workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        else:
            # Get workspace role for system admin if they're a member
            try:
                ws_member_info = await db_manager.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id, target_workspace_id), fetch_one=True
                )
                if ws_member_info:
                    workspace_role = ws_member_info['role']
            except Exception:
                pass
        
        # Execute deletion
        result = await qdrant_service.delete_data(
            workspace_id=target_workspace_id,
            delete_request=request_body,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' deleted data from workspace '{target_workspace_id}'. "
                   f"Filters: {request_body.model_dump(exclude_none=True)}. "
                   f"Deleted {result.get('deleted_count', 0)} points.",
            user_id=requesting_user_id,
            workspace_id=target_workspace_id,
            action_type="Qdrant_Data_Deleted",
            status="success" if result.get('status') == 'success' else "failure"
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data deletion error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during data deletion."
        )

@router.delete("/workspace/delete_all_data")
async def delete_all_workspace_data(
    workspace_id_to_target: str = Query(..., description="Target workspace ID"),
    confirm: bool = Query(False, description="Confirmation flag - must be true"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Delete ALL data from a workspace collection.
    Requires workspace admin/owner or system admin role.
    Requires explicit confirmation flag.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        # Safety check: require explicit confirmation
        if not confirm:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Confirmation required. Set 'confirm=true' to delete all workspace data."
            )
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            target_workspace_id = UUID(workspace_id_to_target)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions - must be workspace admin/owner or system admin
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, target_workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        else:
            # Get workspace role for system admin if they're a member
            try:
                ws_member_info = await db_manager.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id, target_workspace_id), fetch_one=True
                )
                if ws_member_info:
                    workspace_role = ws_member_info['role']
            except Exception:
                pass
        
        # Get count before deletion
        collection_name = get_workspace_qdrant_collection_name(target_workspace_id)
        count_before = await qdrant_service.get_collection_total_count(collection_name)
        
        # Execute deletion
        result = await qdrant_service.delete_all_workspace_data(
            workspace_id=target_workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' deleted ALL data from workspace '{target_workspace_id}'. "
                   f"Deleted {count_before} total points.",
            user_id=requesting_user_id,
            workspace_id=target_workspace_id,
            action_type="Qdrant_All_Data_Deleted",
            status="warning"  # Use warning status for critical deletion
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting all workspace data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during workspace data deletion."
        )

@router.get("/workspace/preview_metadata_update")
async def preview_metadata_update_endpoint(
    workspace_id_query: str = Query(..., alias="workspaceId"),
    camera_ids: Optional[str] = Query(None, description="Comma-separated camera IDs to preview"),
    limit: int = Query(10, ge=1, le=100, description="Number of sample points to return"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Preview data points that would be affected by a metadata update.
    Shows sample data with current metadata values before executing bulk updates.
    
    This helps verify the scope and impact of metadata changes.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            workspace_id = UUID(workspace_id_query)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Parse camera IDs if provided
        parsed_camera_ids = None
        if camera_ids:
            parsed_camera_ids = parse_camera_ids(camera_ids)
        
        # Execute preview
        result = await qdrant_service.preview_metadata_update(
            workspace_id=workspace_id,
            camera_ids=parsed_camera_ids,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            limit=limit
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error previewing metadata update: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during metadata update preview."
        )

@router.post("/workspace/update_metadata_by_id")
async def bulk_update_metadata_by_camera_id(
    workspace_id_query: str = Query(..., alias="workspaceId"),
    stream_update: StreamUpdate = Body(..., description="Stream update with camera ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Bulk update metadata for a specific camera using StreamUpdate model with ID in body.
    More RESTful approach where the camera ID comes from the StreamUpdate.id field.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            workspace_id = UUID(workspace_id_query)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Extract camera ID from StreamUpdate
        camera_id = stream_update.id
        if not camera_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Camera ID is required in the StreamUpdate model"
            )
        
        # Convert StreamUpdate to dictionary
        metadata_dict = stream_update.model_dump(exclude_none=True, exclude_unset=True)
        
        # Remove fields that shouldn't be updated in Qdrant
        metadata_dict.pop('id', None)
        fields_to_exclude = ['path', 'type', 'status', 'is_streaming',
                            'count_threshold_greater', 'count_threshold_less', 'alert_enabled']
        for field in fields_to_exclude:
            metadata_dict.pop(field, None)
        
        if not metadata_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid metadata fields provided for update"
            )
        
        # Execute metadata update for single camera
        result = await qdrant_service.bulk_update_metadata(
            workspace_id=workspace_id,
            camera_ids=[camera_id],
            metadata_updates=metadata_dict,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' updated metadata for camera '{camera_id}' in workspace '{workspace_id}'. "
                   f"Fields updated: {', '.join(metadata_dict.keys())}. "
                   f"Affected {result.get('updated_count', 0)} data points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Qdrant_Metadata_Updated",
            status="success" if result.get('status') == 'success' else "failure"
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating metadata: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during metadata update."
        )

@router.post("/workspace/update_metadata")
async def bulk_update_metadata(
    workspace_id_query: str = Query(..., alias="workspaceId"),
    camera_ids: Optional[str] = Query(None, description="Comma-separated camera IDs to update"),
    metadata_updates: StreamUpdate = Body(..., description="Metadata fields to update"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Bulk update metadata for camera data in Qdrant using StreamUpdate model.
    NOW UPDATES ALL FIELDS including path, type, status, is_streaming, and alert settings.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            workspace_id = UUID(workspace_id_query)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Parse camera IDs if provided
        parsed_camera_ids = None
        if camera_ids:
            parsed_camera_ids = parse_camera_ids(camera_ids)
        
        # Convert StreamUpdate to dictionary with only non-None values
        metadata_dict = metadata_updates.model_dump(exclude_none=True, exclude_unset=True)
        
        # Remove 'id' field as it's not metadata we want to update in Qdrant
        metadata_dict.pop('id', None)
        
        # NO LONGER EXCLUDING ANY FIELDS - ALL fields can be updated
        
        if not metadata_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid metadata fields provided for update."
            )
        
        # Execute metadata update
        result = await qdrant_service.bulk_update_metadata(
            workspace_id=workspace_id,
            camera_ids=parsed_camera_ids,
            metadata_updates=metadata_dict,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        camera_info = f"cameras: {', '.join(parsed_camera_ids[:5])}" if parsed_camera_ids else "all cameras"
        if parsed_camera_ids and len(parsed_camera_ids) > 5:
            camera_info += f" and {len(parsed_camera_ids) - 5} more"
        
        await session_manager.log_action(
            content=f"User '{username}' updated metadata in workspace '{workspace_id}' for {camera_info}. "
                   f"Fields updated: {', '.join(metadata_dict.keys())}. "
                   f"Affected {result.get('updated_count', 0)} data points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Qdrant_Metadata_Updated",
            status="success" if result.get('status') == 'success' else "failure"
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating metadata: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during metadata update."
        )

@router.post("/workspace/batch_update_metadata")
async def batch_update_metadata_for_cameras(
    workspace_id_query: str = Query(..., alias="workspaceId"),
    stream_updates: List[StreamUpdate] = Body(..., description="List of stream updates"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Batch update metadata for multiple cameras, each with potentially different metadata.
    NOW UPDATES ALL FIELDS from StreamUpdate model.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Validate workspace ID
        try:
            workspace_id = UUID(workspace_id_query)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid workspace_id format."
            )
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await qdrant_service.check_workspace_membership(
                    requesting_user_id, workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        if not stream_updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No stream updates provided"
            )
        
        # Process each camera update
        results = []
        total_updated = 0
        
        for stream_update in stream_updates:
            camera_id = stream_update.id
            if not camera_id:
                results.append({
                    "camera_id": None,
                    "status": "error",
                    "message": "Missing camera ID",
                    "updated_count": 0
                })
                continue
            
            # Convert to metadata dict - NO FIELD EXCLUSIONS
            metadata_dict = stream_update.model_dump(exclude_none=True, exclude_unset=True)
            metadata_dict.pop('id', None)
            
            # NO LONGER EXCLUDING ANY FIELDS
            
            if not metadata_dict:
                results.append({
                    "camera_id": camera_id,
                    "status": "skipped",
                    "message": "No valid metadata fields to update",
                    "updated_count": 0
                })
                continue
            
            # Update metadata for this camera
            try:
                result = await qdrant_service.bulk_update_metadata(
                    workspace_id=workspace_id,
                    camera_ids=[camera_id],
                    metadata_updates=metadata_dict,
                    user_system_role=system_role,
                    user_workspace_role=workspace_role,
                    requesting_username=username
                )
                
                results.append({
                    "camera_id": camera_id,
                    "status": result.get("status"),
                    "message": result.get("message"),
                    "updated_count": result.get("updated_count", 0),
                    "updated_fields": list(metadata_dict.keys())
                })
                
                total_updated += result.get("updated_count", 0)
                
            except Exception as e:
                logger.error(f"Error updating metadata for camera {camera_id}: {e}")
                results.append({
                    "camera_id": camera_id,
                    "status": "error",
                    "message": str(e),
                    "updated_count": 0
                })
        
        # Log the batch action
        await session_manager.log_action(
            content=f"User '{username}' batch updated metadata for {len(stream_updates)} cameras "
                   f"in workspace '{workspace_id}'. Total data points affected: {total_updated}.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Qdrant_Batch_Metadata_Updated",
            status="success"
        )
        
        return JSONResponse(content={
            "batch_results": results,
            "total_cameras_processed": len(stream_updates),
            "total_data_points_updated": total_updated,
            "workspace_id": str(workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in batch metadata update: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during batch metadata update."
        )

# ========== Location Data Endpoints ==========

@router.get("/locations/list")
async def get_qdrant_locations(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations from Qdrant data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"locations": [], "total_count": 0}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get location data
        location_data = await qdrant_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
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
        
        locations.sort(key=lambda x: x["location"])
        
        return {
            "locations": locations,
            "total_count": len(locations),
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve locations."
        )

@router.get("/areas/list")
async def get_qdrant_areas(
    locations: Optional[str] = Query(None, description="Filter by location(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique areas, optionally filtered by location(s)."""
    try:
        parsed_locations = parse_string_or_list(locations)
        
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"areas": [], "total_count": 0, "filtered_by_locations": parsed_locations}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Prepare filter conditions
        filter_conditions = {}
        if parsed_locations:
            filter_conditions["location"] = parsed_locations if isinstance(parsed_locations, list) else [parsed_locations]
        
        # Get area data
        area_data = await qdrant_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
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
        
        areas.sort(key=lambda x: (x["area"], x.get("location", "")))
        
        return {
            "areas": areas,
            "total_count": len(areas),
            "filtered_by_locations": parsed_locations,
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve areas."
        )

@router.get("/buildings/list")
async def get_qdrant_buildings(
    areas: Optional[str] = Query(None, description="Filter by area(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique buildings, optionally filtered by area(s)."""
    try:
        parsed_areas = parse_string_or_list(areas)
        
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"buildings": [], "total_count": 0, "filtered_by_areas": parsed_areas}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Prepare filter conditions
        filter_conditions = {}
        if parsed_areas:
            filter_conditions["area"] = parsed_areas if isinstance(parsed_areas, list) else [parsed_areas]
        
        # Get building data
        building_data = await qdrant_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
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
        
        buildings.sort(key=lambda x: (x["building"], x.get("area", ""), x.get("location", "")))
        
        return {
            "buildings": buildings,
            "total_count": len(buildings),
            "filtered_by_areas": parsed_areas,
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve buildings."
        )

@router.get("/floor-levels/list")
async def get_qdrant_floor_levels(
    buildings: Optional[str] = Query(None, description="Filter by building(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique floor levels, optionally filtered by building(s)."""
    try:
        parsed_buildings = parse_string_or_list(buildings)
        
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"floor_levels": [], "total_count": 0, "filtered_by_buildings": parsed_buildings}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Prepare filter conditions
        filter_conditions = {}
        if parsed_buildings:
            filter_conditions["building"] = parsed_buildings if isinstance(parsed_buildings, list) else [parsed_buildings]
        
        # Get floor level data
        floor_data = await qdrant_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
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
            } for item in floor_data if item.get("floor_level")
        ]
        
        floor_levels.sort(key=lambda x: (
            x["floor_level"], 
            x.get("building", ""), 
            x.get("area", ""), 
            x.get("location", "")
        ))
        
        return {
            "floor_levels": floor_levels,
            "total_count": len(floor_levels),
            "filtered_by_buildings": parsed_buildings,
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve floor levels."
        )

@router.get("/zones/list")
async def get_qdrant_zones(
    floor_levels: Optional[str] = Query(None, description="Filter by floor level(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique zones, optionally filtered by floor level(s)."""
    try:
        parsed_floor_levels = parse_string_or_list(floor_levels)
        
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"zones": [], "total_count": 0, "filtered_by_floor_levels": parsed_floor_levels}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Prepare filter conditions
        filter_conditions = {}
        if parsed_floor_levels:
            filter_conditions["floor_level"] = parsed_floor_levels if isinstance(parsed_floor_levels, list) else [parsed_floor_levels]
        
        # Get zone data
        zone_data = await qdrant_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
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
        
        zones.sort(key=lambda x: (
            x["zone"], 
            x.get("floor_level", ""), 
            x.get("building", ""), 
            x.get("area", ""), 
            x.get("location", "")
        ))
        
        return {
            "zones": zones,
            "total_count": len(zones),
            "filtered_by_floor_levels": parsed_floor_levels,
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting zones: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve zones."
        )

@router.get("/locations/analytics")
async def get_location_analytics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    group_by: str = Query("location", regex="^(location|area|building|floor_level|zone)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get analytics data grouped by location hierarchy."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"analytics": [], "total_groups": 0, "group_by": group_by}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Build search query
        search_query = SearchQuery(
            camera_id=None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Get analytics
        result = await qdrant_service.get_location_analytics(
            workspace_id=workspace_id,
            search_query=search_query,
            group_by=group_by,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting location analytics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve location analytics."
        )

@router.get("/locations/summary")
async def get_location_summary(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive summary of all location data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {
                "summary": {
                    "locations": 0, "areas": 0, "buildings": 0,
                    "floor_levels": 0, "zones": 0, "total_cameras": 0
                },
                "hierarchy": []
            }
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get summary
        result = await qdrant_service.get_location_summary(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting location summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve location summary."
        )

@router.get("/locations/search")
async def search_location_data(
    search_term: str = Query(..., min_length=1, description="Search term"),
    search_fields: List[str] = Query(
        default=["location", "area", "building", "floor_level", "zone"],
        description="Fields to search in"
    ),
    exact_match: bool = Query(False, description="Use exact match"),
    limit: int = Query(50, ge=1, le=200),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search location data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await qdrant_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"results": [], "search_term": search_term}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await qdrant_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Search
        result = await qdrant_service.search_location_data(
            workspace_id=workspace_id,
            search_term=search_term,
            search_fields=search_fields,
            exact_match=exact_match,
            limit=limit,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error searching location data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search location data."
        )
