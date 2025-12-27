# app/routes/elasticsearch_router.py
from fastapi import APIRouter, HTTPException, Body, Query, Depends, status, Response, status as http_status
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Union
from uuid import UUID
import logging
import io
import csv

from app.services.elasticsearch_service import elasticsearch_service
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.schemas import (
    SearchQuery, LocationSearchQuery, DeleteDataRequest, StreamUpdate,
    CreateCollectionRequest, TimestampRangeResponse, CameraIdsResponse
)
from app.utils import parse_camera_ids, parse_string_or_list

logger = logging.getLogger(__name__)
router = APIRouter(tags=["elasticsearch_data"], prefix="/elasticsearch")

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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
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
        results = await elasticsearch_service.search_workspace_data(
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
            "index_queried": f"person_counts_ws_{str(final_workspace_id).replace('-', '_')}",
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
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
        results = await elasticsearch_service.search_ordered_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=per_page
        )
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ordered search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during ordered data search."
        )

@router.get("/workspace/location_search")
async def workspace_location_search(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    latitude: Optional[float] = Query(None),
    longitude: Optional[float] = Query(None),
    radius_km: Optional[float] = Query(None),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data by location with optional geo-radius filtering"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
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
            zone=zone,
            latitude=latitude,
            longitude=longitude,
            radius_km=radius_km
        )
        
        # Execute location search
        results = await elasticsearch_service.search_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=per_page,
            base64=base64
        )

        # Add location filter info
        if "search_scope" not in results:
            results["search_scope"] = {}
        
        results["search_scope"]["location_filters"] = {
            "location": location,
            "area": area,
            "building": building,
            "floor_level": floor_level,
            "zone": zone,
            "latitude": latitude,
            "longitude": longitude,
            "radius_km": radius_km
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Location search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during location search."
        )

# ========== Statistics Endpoints ==========

@router.get("/workspace/timestamp_range")
async def get_timestamp_range(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get earliest and latest timestamps in workspace data"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get timestamp range
        result = await elasticsearch_service.get_timestamp_range(
            workspace_id=final_workspace_id,
            camera_ids=parse_camera_ids(camera_id_param) if camera_id_param else None,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result.model_dump())
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting timestamp range: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve timestamp range."
        )

@router.get("/workspace/camera_ids")
async def get_camera_ids(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique camera IDs in workspace"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get camera IDs
        cameras = await elasticsearch_service.get_camera_ids_for_workspace(final_workspace_id)
        
        return JSONResponse(content={
            "camera_ids": cameras,
            "count": len(cameras),
            "workspace_id": str(final_workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting camera IDs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve camera IDs."
        )

@router.get("/workspace/statistics")
async def get_workspace_statistics(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive statistics for workspace data"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get statistics
        result = await elasticsearch_service.get_workspace_statistics(
            workspace_id=final_workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting workspace statistics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve workspace statistics."
        )

# ========== Data Management Endpoints ==========

@router.delete("/workspace/data")
async def delete_workspace_data(
    delete_request: DeleteDataRequest,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete data from workspace based on filters"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if delete_request.workspace_id:
            final_workspace_id = delete_request.workspace_id
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Only admins and workspace owners can delete data
        if not is_system_admin and workspace_role not in ["owner", "admin"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only workspace owners/admins can delete data."
            )
        
        # Execute deletion
        result = await elasticsearch_service.delete_workspace_data(
            workspace_id=final_workspace_id,
            delete_request=delete_request,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting workspace data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete workspace data."
        )

@router.get("/workspace/export/csv")
async def export_workspace_data_csv(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    include_frame: bool = Query(False),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Export workspace data as CSV"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
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
        
        # Get data for export
        csv_data, content_type, filename  = await elasticsearch_service.export_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            format='csv',
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            include_frame=include_frame
        )
        
        return Response(
            content=csv_data,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error exporting workspace data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export workspace data."
        )

# ========== Metadata Management Endpoints ==========

@router.delete("/workspace/delete_all_data")
async def delete_all_workspace_data(
    workspace_id_to_target: str = Query(..., description="Target workspace ID"),
    confirm: bool = Query(False, description="Confirmation flag - must be true"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Delete ALL data from a workspace Elasticsearch index.
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
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, target_workspace_id, required_role="admin"
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        else:
            # Get workspace role for system admin if they're a member
            try:
                from app.services.database import db_manager
                ws_member_info = await db_manager.execute_query(
                    "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
                    (requesting_user_id, target_workspace_id), fetch_one=True
                )
                if ws_member_info:
                    workspace_role = ws_member_info['role']
            except Exception:
                pass
        
        # Get count before deletion
        from app.utils import get_workspace_elasticsearch_index_name
        index_name = get_workspace_elasticsearch_index_name(target_workspace_id)
        count_before = await elasticsearch_service.get_index_total_count(index_name)
        
        # Execute deletion
        result = await elasticsearch_service.delete_all_workspace_data(
            workspace_id=target_workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' deleted ALL data from workspace '{target_workspace_id}'. "
                   f"Deleted {count_before} total documents.",
            user_id=requesting_user_id,
            workspace_id=target_workspace_id,
            action_type="Elasticsearch_All_Data_Deleted",
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
    limit: int = Query(10, ge=1, le=100, description="Number of sample documents to return"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Preview data documents that would be affected by a metadata update.
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
                member_info = await elasticsearch_service.check_workspace_membership(
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
        result = await elasticsearch_service.preview_metadata_update(
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
    Updates ALL fields from StreamUpdate model.
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
                member_info = await elasticsearch_service.check_workspace_membership(
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
        
        # Execute metadata update for single camera
        result = await elasticsearch_service.update_camera_metadata_by_id(
            workspace_id=workspace_id,
            camera_id=str(camera_id),
            stream_update=stream_update,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        updated_fields = [k for k, v in stream_update.model_dump(exclude_none=True, exclude={'id'}).items()]
        await session_manager.log_action(
            content=f"User '{username}' updated metadata for camera '{camera_id}' in workspace '{workspace_id}'. "
                   f"Fields updated: {', '.join(updated_fields)}. "
                   f"Affected {result.get('updated_count', 0)} documents.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Elasticsearch_Metadata_Updated",
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
    Bulk update metadata for camera data in Elasticsearch using StreamUpdate model.
    Updates ALL fields from StreamUpdate including path, type, status, is_streaming, and alert settings.
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
                member_info = await elasticsearch_service.check_workspace_membership(
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
        
        # Remove 'id' field as it's not metadata we want to update
        metadata_dict.pop('id', None)
        
        if not metadata_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid metadata fields provided for update."
            )
        
        # Execute metadata update
        result = await elasticsearch_service.bulk_update_metadata(
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
                   f"Affected {result.get('updated_count', 0)} documents.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Elasticsearch_Metadata_Updated",
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
    Updates ALL fields from StreamUpdate model.
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
                member_info = await elasticsearch_service.check_workspace_membership(
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
            
            # Update metadata for this camera
            try:
                result = await elasticsearch_service.update_camera_metadata_by_id(
                    workspace_id=workspace_id,
                    camera_id=str(camera_id),
                    stream_update=stream_update,
                    user_system_role=system_role,
                    user_workspace_role=workspace_role,
                    requesting_username=username
                )
                
                results.append({
                    "camera_id": str(camera_id),
                    "status": result.get("status"),
                    "message": result.get("message"),
                    "updated_count": result.get("updated_count", 0),
                    "updated_fields": result.get("updated_fields", [])
                })
                
                total_updated += result.get("updated_count", 0)
                
            except Exception as e:
                logger.error(f"Error updating metadata for camera {camera_id}: {e}")
                results.append({
                    "camera_id": str(camera_id),
                    "status": "error",
                    "message": str(e),
                    "updated_count": 0
                })
        
        # Log the batch action
        await session_manager.log_action(
            content=f"User '{username}' batch updated metadata for {len(stream_updates)} cameras "
                   f"in workspace '{workspace_id}'. Total documents affected: {total_updated}.",
            user_id=requesting_user_id,
            workspace_id=workspace_id,
            action_type="Elasticsearch_Batch_Metadata_Updated",
            status="success"
        )
        
        return JSONResponse(content={
            "batch_results": results,
            "total_cameras_processed": len(stream_updates),
            "total_documents_updated": total_updated,
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
    
# ========== Camera Endpoints ==========

@router.get("/workspace/cameras")
async def get_workspace_cameras(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search_term: Optional[str] = Query(None),
    group_by: Optional[str] = Query(None, regex="^(location|area|building|floor_level|zone)$"),
    include_inactive: bool = Query(False),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras in workspace with optional filtering and grouping"""
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
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        location_filters = {
            'location': location,
            'area': area,
            'building': building,
            'floor_level': floor_level,
            'zone': zone
        }

        # Get cameras
        result = await elasticsearch_service.get_workspace_cameras(
            workspace_id=final_workspace_id,
            location_filters=location_filters,
            status=status,
            search_term=search_term,
            group_by=group_by,
            include_inactive=include_inactive,
            user_id=requesting_user_id,
            user_role=system_role
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting workspace cameras: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve workspace cameras."
        )

# ========== Location Hierarchy Endpoints ==========

@router.get("/locations")
async def get_locations(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations in workspace"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"locations": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get locations
        result = await elasticsearch_service.get_unique_locations(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve locations."
        )

@router.get("/areas")
async def get_areas(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique areas, optionally filtered by location"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"areas": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")

        result = await elasticsearch_service.get_unique_areas(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve areas."
        )

@router.get("/buildings")
async def get_buildings(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique buildings, optionally filtered by location/area"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"buildings": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        result = await elasticsearch_service.get_unique_buildings(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve buildings."
        )

@router.get("/floor_levels")
async def get_floor_levels(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique floor levels, optionally filtered by location/area/building"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"floor_levels": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get floor levels
        result = await elasticsearch_service.get_unique_floor_levels(
            workspace_id=workspace_id,
            location=location,
            area=area,
            building=building,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve floor levels."
        )

@router.get("/zones")
async def get_zones(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    floor_level: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique zones with their associated location information"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"zones": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Parse floor_level parameter
        parsed_floor_levels = None
        if floor_level:
            parsed_floor_levels = parse_string_or_list(floor_level)
        
        # Get zones
        result = await elasticsearch_service.get_unique_zones(
            workspace_id=workspace_id,
            floor_levels=parsed_floor_levels,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
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
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"analytics": [], "total_groups": 0, "group_by": group_by}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
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
        result = await elasticsearch_service.get_location_analytics(
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
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
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
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get summary
        result = await elasticsearch_service.get_location_summary(
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
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"results": [], "search_term": search_term}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Search
        result = await elasticsearch_service.search_location_data(
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