# app/routes/postgres_router.py
from fastapi import APIRouter, HTTPException, Query, Body, Depends, status, Response
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, Dict, List
import asyncio
import logging
import io

from app.services.postgres_service import postgres_service
from app.services.detection_data_service import detection_data_service
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.schemas import (
    SearchQuery, LocationSearchQuery, DeleteDataRequest, StreamUpdate,
    TimestampRangeResponse, CameraIdsResponse
)
from app.utils import parse_camera_ids, parse_string_or_list
from app.services.s3_service import s3_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["postgres_data"])#, prefix="/postgres"

# ========== Search Endpoints ==========

@router.get("/workspace/search_results")
async def workspace_search_results(
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: Optional[str] = Query(None),
    include_frame: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data using detection_data_service (PostgreSQL metadata)"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
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
        
        # Parse camera IDs
        parsed_camera_ids = parse_camera_ids(camera_id_param) if camera_id_param else None
        
        # ===== USE DETECTION_DATA_SERVICE INSTEAD =====
        results = await detection_data_service.retrieve_detection_data(
            workspace_id=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            camera_id=parsed_camera_ids,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            page=page,
            per_page=processed_per_page,
            include_frame=include_frame
        )
        
        # Add search scope info
        results["search_scope"] = {
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql",
            "filters_applied": {
                "camera_id": parsed_camera_ids,
                "start_date": start_date,
                "end_date": end_date,
                "start_time": start_time,
                "end_time": end_time
            },
            "access_level": "system_admin" if is_system_admin else (workspace_role or "member"),
            "frames_included": include_frame,
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

@router.get("/workspace/search_results_with_location")
async def workspace_search_results_with_location(
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
    include_frame: bool = Query(True),
    base64: Optional[bool] = Query(None, description="Alias for include_frame used by the frame-search UI"),
    full_data: Optional[bool] = Query(None, description="Accepted for compatibility; no effect beyond per_page=null"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Enhanced search with location-based filtering using detection_data_service"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Process per_page
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

        if full_data:
            processed_per_page = 100
        
        # Parse camera IDs
        parsed_camera_ids = parse_camera_ids(camera_id_param) if camera_id_param else None

        # `base64`, when explicitly passed, is the frame-search UI's alias for include_frame
        effective_include_frame = include_frame if base64 is None else base64

        # ===== USE DETECTION_DATA_SERVICE WITH LOCATION FILTERS =====
        results = await detection_data_service.retrieve_detection_data(
            workspace_id=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            camera_id=parsed_camera_ids,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            location=location,
            area=area,
            building=building,
            floor_level=floor_level,
            zone=zone,
            page=page,
            per_page=processed_per_page,
            include_frame=effective_include_frame
        )

        # Resolve S3 image paths to presigned URLs in each result item (concurrently)
        s3_items = [
            item for item in results.get("data", [])
            if isinstance(item.get("frame"), str) and item["frame"].startswith("s3://")
        ]
        if s3_items:
            presigned_urls = await asyncio.gather(
                *(s3_service.get_presigned_url(item["frame"]) for item in s3_items)
            )
            for item, url in zip(s3_items, presigned_urls):
                item["frame"] = url

        # Add location filters to search scope
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

# ========== Prediction Endpoints ==========

@router.get("/workspace/prediction_data")
async def workspace_prediction_endpoint(
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
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
                params = tuple(raw_ids + [workspace_id_obj])
                cam_names_db = await db_manager.execute_query(query, params, fetch_all=True)
                cam_details_map = {str(row['stream_id']): row['name'] for row in cam_names_db}
                parsed_camera_ids = [(cid, cam_details_map.get(cid, f"Camera {cid[:8]}")) for cid in raw_ids]
        else:
            query = "SELECT stream_id, name FROM video_stream WHERE workspace_id = $1 AND status = 'active'"
            db_cameras = await db_manager.execute_query(query, (workspace_id_obj,), fetch_all=True)
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
        
        predictions = await postgres_service.get_prediction_data(
            workspace_id=workspace_id_obj,
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
    return await workspace_prediction_endpoint(
        camera_id_param=camera_id,
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        current_user_data=current_user_data
    )

# ========== Statistics Endpoints ==========

@router.get("/workspace/statistics")
async def get_workspace_statistics(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive statistics for a workspace"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Get statistics
        stats = await postgres_service.get_workspace_statistics(
            workspace_id=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=stats)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting statistics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving workspace statistics."
        )

# ========== Export Endpoints ==========

@router.get("/workspace/export")
async def export_workspace_data(
    format: str = Query("csv", pattern="^(csv|json)$"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    include_frame: bool = Query(False),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Export workspace data in CSV or JSON format"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build search query if filters provided
        search_query = None
        if any([camera_id_param, start_date, end_date, start_time, end_time]):
            search_query = SearchQuery(
                camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time
            )
        
        # Execute export
        file_data, media_type, filename = await postgres_service.export_workspace_data(
            workspace_id=workspace_id_obj,
            search_query=search_query,
            format=format,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            include_frame=include_frame
        )
        
        # Log export action
        await session_manager.log_action(
            content=f"User '{username}' exported workspace data in {format.upper()} format",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="Data_Exported",
            status="success"
        )
        
        return StreamingResponse(
            io.BytesIO(file_data),
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during data export."
        )

# ========== Timestamp and Camera Endpoints ==========

@router.get("/timestamp-range", response_model=TimestampRangeResponse)
async def get_timestamp_range_endpoint(
    camera_id: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get timestamp range for data"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        system_role = current_user_data.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return TimestampRangeResponse()
        
        # Get workspace role
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            workspace_role = member_result['role'] if member_result else None
        
        parsed_camera_ids = parse_camera_ids(camera_id) if camera_id else None
        
        return await postgres_service.get_timestamp_range(
            workspace_id=workspace_id_obj,
            camera_ids=parsed_camera_ids,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting timestamp range: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving timestamp range."
        )

@router.get("/camera-ids", response_model=CameraIdsResponse)
async def get_all_camera_ids_for_active_workspace(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all camera IDs for workspace"""
    try:
        username = current_user_data["username"]
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return CameraIdsResponse(camera_ids=[], count=0)
        
        cameras = await postgres_service.get_camera_ids_for_workspace(workspace_id_obj)
        
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

# ========== Location Endpoints ==========

@router.get("/workspace/locations")
async def get_unique_locations(
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations with filtering options"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            return {"locations": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        result = await postgres_service.get_unique_locations(
            workspace_id=workspace_id_obj,
            area=area,
            building=building,
            floor_level=floor_level,
            zone=zone,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving locations."
        )

# ========== Data Management Endpoints ==========

@router.delete("/workspace/delete_data")
async def delete_data_from_workspace(
    request_body: DeleteDataRequest,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete data from workspace"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions - must be workspace admin/owner or system admin
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
            
            if workspace_role not in ['admin', 'owner']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only workspace admins can delete data."
                )
        
        # Execute deletion
        result = await postgres_service.delete_data(
            workspace_id=workspace_id_obj,
            delete_request=request_body,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' deleted data from workspace '{workspace_id_obj}'. "
                   f"Filters: {request_body.model_dump(exclude_none=True)}. "
                   f"Deleted {result.get('deleted_count', 0)} points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="PostgreSQL_Data_Deleted",
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
    confirm: bool = Query(False, description="Confirmation flag - must be true"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Delete ALL data from a workspace.
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
            
            if workspace_role not in ['admin', 'owner']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only workspace admins can delete all data."
                )
        
        # Execute deletion
        result = await postgres_service.delete_all_workspace_data(
            workspace_id=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' deleted ALL data from workspace '{workspace_id_obj}'. "
                   f"Deleted {result.get('deleted_count', 0)} total points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="PostgreSQL_All_Data_Deleted",
            status="warning"
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

# ========== Metadata Update Endpoints ==========

@router.get("/workspace/preview_metadata_update")
async def preview_metadata_update_endpoint(
    camera_ids: Optional[str] = Query(None, description="Comma-separated camera IDs to preview"),
    limit: int = Query(10, ge=1, le=100, description="Number of sample points to return"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Preview data points that would be affected by a metadata update.
    Shows sample data with current metadata values before executing bulk updates.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Parse camera IDs if provided
        parsed_camera_ids = None
        if camera_ids:
            parsed_camera_ids = parse_camera_ids(camera_ids)
        
        # Execute preview
        result = await postgres_service.preview_metadata_update(
            workspace_id=workspace_id_obj,
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
async def update_camera_metadata_by_id(
    stream_update: StreamUpdate = Body(..., description="Stream update with camera ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Update metadata for a specific camera using StreamUpdate model with ID in body.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
            
            if workspace_role not in ['admin', 'owner']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only workspace admins can update metadata."
                )
        
        # Extract camera ID from StreamUpdate
        camera_id = stream_update.id
        if not camera_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Camera ID is required in the StreamUpdate model"
            )
        
        # Execute metadata update
        result = await postgres_service.update_camera_metadata_by_id(
            workspace_id=workspace_id_obj,
            camera_id=camera_id,
            stream_update=stream_update,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Log the action
        await session_manager.log_action(
            content=f"User '{username}' updated metadata for camera '{camera_id}' in workspace '{workspace_id_obj}'. "
                   f"Affected {result.get('updated_count', 0)} data points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="PostgreSQL_Metadata_Updated",
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
    camera_ids: Optional[str] = Query(None, description="Comma-separated camera IDs to update"),
    metadata_updates: StreamUpdate = Body(..., description="Metadata fields to update"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Bulk update metadata for camera data in PostgreSQL using StreamUpdate model.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
            
            if workspace_role not in ['admin', 'owner']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only workspace admins can update metadata."
                )
        
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
        result = await postgres_service.bulk_update_metadata(
            workspace_id=workspace_id_obj,
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
            content=f"User '{username}' updated metadata in workspace '{workspace_id_obj}' for {camera_info}. "
                   f"Fields updated: {', '.join(metadata_dict.keys())}. "
                   f"Affected {result.get('updated_count', 0)} data points.",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="PostgreSQL_Metadata_Updated",
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
    stream_updates: List[StreamUpdate] = Body(..., description="List of stream updates"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Batch update metadata for multiple cameras, each with potentially different metadata.
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
            
            if workspace_role not in ['admin', 'owner']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only workspace admins can update metadata."
                )
        
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
                result = await postgres_service.update_camera_metadata_by_id(
                    workspace_id=workspace_id_obj,
                    camera_id=camera_id,
                    stream_update=stream_update,
                    user_system_role=system_role,
                    user_workspace_role=workspace_role,
                    requesting_username=username
                )
                
                results.append({
                    "camera_id": camera_id,
                    "status": result.get("status"),
                    "message": result.get("message"),
                    "updated_count": result.get("updated_count", 0)
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
                   f"in workspace '{workspace_id_obj}'. Total data points affected: {total_updated}.",
            user_id=requesting_user_id,
            workspace_id=workspace_id_obj,
            action_type="PostgreSQL_Batch_Metadata_Updated",
            status="success"
        )
        
        return JSONResponse(content={
            "batch_results": results,
            "total_cameras_processed": len(stream_updates),
            "total_data_points_updated": total_updated,
            "workspace_id": str(workspace_id_obj)
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
async def get_postgres_locations(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations from PostgreSQL data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Get location data
        result = await postgres_service.get_unique_locations(
            workspace_id=workspace_id_obj,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
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

@router.get("/areas/list")
async def get_postgres_areas(
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build query with location filter
        conditions = ["workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        if parsed_locations:
            param_count += 1
            if isinstance(parsed_locations, list):
                conditions.append(f"location = ANY(${param_count})")
                params.append(parsed_locations)
            else:
                conditions.append(f"location = ${param_count}")
                params.append(parsed_locations)
        
        # User access control
        if system_role != 'admin' and workspace_role not in ['admin', 'owner']:
            param_count += 1
            conditions.append(f"username = ${param_count}")
            params.append(username)
        
        where_clause = " AND ".join(conditions)
        
        # Get areas grouped by location
        query = f"""
            SELECT 
                area, location,
                COUNT(DISTINCT camera_id) as camera_count,
                ARRAY_AGG(DISTINCT camera_id) as camera_ids
            FROM stream_results
            WHERE {where_clause} AND area IS NOT NULL
            GROUP BY area, location
            ORDER BY area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        areas = []
        if results:
            for row in results:
                areas.append({
                    'area': row['area'],
                    'location': row['location'],
                    'camera_count': row['camera_count'],
                    'camera_ids': row['camera_ids']
                })
        
        return JSONResponse(content={
            "areas": areas,
            "total_count": len(areas),
            "filtered_by_locations": parsed_locations,
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql"
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve areas."
        )

@router.get("/buildings/list")
async def get_postgres_buildings(
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build query with area filter
        conditions = ["workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        if parsed_areas:
            param_count += 1
            if isinstance(parsed_areas, list):
                conditions.append(f"area = ANY(${param_count})")
                params.append(parsed_areas)
            else:
                conditions.append(f"area = ${param_count}")
                params.append(parsed_areas)
        
        # User access control
        if system_role != 'admin' and workspace_role not in ['admin', 'owner']:
            param_count += 1
            conditions.append(f"username = ${param_count}")
            params.append(username)
        
        where_clause = " AND ".join(conditions)
        
        # Get buildings grouped by area and location
        query = f"""
            SELECT 
                building, area, location,
                COUNT(DISTINCT camera_id) as camera_count,
                ARRAY_AGG(DISTINCT camera_id) as camera_ids
            FROM stream_results
            WHERE {where_clause} AND building IS NOT NULL
            GROUP BY building, area, location
            ORDER BY building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        buildings = []
        if results:
            for row in results:
                buildings.append({
                    'building': row['building'],
                    'area': row['area'],
                    'location': row['location'],
                    'camera_count': row['camera_count'],
                    'camera_ids': row['camera_ids']
                })
        
        return JSONResponse(content={
            "buildings": buildings,
            "total_count": len(buildings),
            "filtered_by_areas": parsed_areas,
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql"
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve buildings."
        )

@router.get("/floor-levels/list")
async def get_postgres_floor_levels(
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build query with building filter
        conditions = ["workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        if parsed_buildings:
            param_count += 1
            if isinstance(parsed_buildings, list):
                conditions.append(f"building = ANY(${param_count})")
                params.append(parsed_buildings)
            else:
                conditions.append(f"building = ${param_count}")
                params.append(parsed_buildings)
        
        # User access control
        if system_role != 'admin' and workspace_role not in ['admin', 'owner']:
            param_count += 1
            conditions.append(f"username = ${param_count}")
            params.append(username)
        
        where_clause = " AND ".join(conditions)
        
        # Get floor levels
        query = f"""
            SELECT 
                floor_level, building, area, location,
                COUNT(DISTINCT camera_id) as camera_count,
                ARRAY_AGG(DISTINCT camera_id) as camera_ids
            FROM stream_results
            WHERE {where_clause} AND floor_level IS NOT NULL
            GROUP BY floor_level, building, area, location
            ORDER BY floor_level, building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        floor_levels = []
        if results:
            for row in results:
                floor_levels.append({
                    'floor_level': row['floor_level'],
                    'building': row['building'],
                    'area': row['area'],
                    'location': row['location'],
                    'camera_count': row['camera_count'],
                    'camera_ids': row['camera_ids']
                })
        
        return JSONResponse(content={
            "floor_levels": floor_levels,
            "total_count": len(floor_levels),
            "filtered_by_buildings": parsed_buildings,
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql"
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve floor levels."
        )

@router.get("/zones/list")
async def get_postgres_zones(
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
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build query with floor level filter
        conditions = ["workspace_id = $1"]
        params = [workspace_id_obj]
        param_count = 1
        
        if parsed_floor_levels:
            param_count += 1
            if isinstance(parsed_floor_levels, list):
                conditions.append(f"floor_level = ANY(${param_count})")
                params.append(parsed_floor_levels)
            else:
                conditions.append(f"floor_level = ${param_count}")
                params.append(parsed_floor_levels)
        
        # User access control
        if system_role != 'admin' and workspace_role not in ['admin', 'owner']:
            param_count += 1
            conditions.append(f"username = ${param_count}")
            params.append(username)
        
        where_clause = " AND ".join(conditions)
        
        # Get zones
        query = f"""
            SELECT 
                zone, floor_level, building, area, location,
                COUNT(DISTINCT camera_id) as camera_count,
                ARRAY_AGG(DISTINCT camera_id) as camera_ids
            FROM stream_results
            WHERE {where_clause} AND zone IS NOT NULL
            GROUP BY zone, floor_level, building, area, location
            ORDER BY zone, floor_level, building, area, location
        """
        
        results = await db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        zones = []
        if results:
            for row in results:
                zones.append({
                    'zone': row['zone'],
                    'floor_level': row['floor_level'],
                    'building': row['building'],
                    'area': row['area'],
                    'location': row['location'],
                    'camera_count': row['camera_count'],
                    'camera_ids': row['camera_ids']
                })
        
        return JSONResponse(content={
            "zones": zones,
            "total_count": len(zones),
            "filtered_by_floor_levels": parsed_floor_levels,
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql"
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting zones: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve zones."
        )

@router.get("/locations/summary")
async def get_location_summary_postgres(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive summary of all location data from PostgreSQL."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (user_id, workspace_id_obj), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Build base conditions
        conditions = ["workspace_id = $1"]
        params = [workspace_id_obj]
        
        # User access control
        if system_role != 'admin' and workspace_role not in ['admin', 'owner']:
            conditions.append("username = $2")
            params.append(username)
        
        where_clause = " AND ".join(conditions)
        
        # Get summary counts
        summary_query = f"""
            SELECT 
                COUNT(DISTINCT location) FILTER (WHERE location IS NOT NULL) as locations,
                COUNT(DISTINCT area) FILTER (WHERE area IS NOT NULL) as areas,
                COUNT(DISTINCT building) FILTER (WHERE building IS NOT NULL) as buildings,
                COUNT(DISTINCT floor_level) FILTER (WHERE floor_level IS NOT NULL) as floor_levels,
                COUNT(DISTINCT zone) FILTER (WHERE zone IS NOT NULL) as zones,
                COUNT(DISTINCT camera_id) as total_cameras
            FROM stream_results
            WHERE {where_clause}
        """
        
        summary_result = await db_manager.execute_query(
            summary_query, tuple(params), fetch_one=True
        )
        
        # Get hierarchy
        hierarchy_query = f"""
            SELECT 
                building, floor_level, zone, area, location,
                COUNT(DISTINCT camera_id) as camera_count,
                ARRAY_AGG(DISTINCT camera_id) as camera_ids
            FROM stream_results
            WHERE {where_clause}
            GROUP BY building, floor_level, zone, area, location
            ORDER BY building, floor_level, zone, area, location
        """
        
        hierarchy_results = await db_manager.execute_query(
            hierarchy_query, tuple(params), fetch_all=True
        )
        
        hierarchy = []
        if hierarchy_results:
            for row in hierarchy_results:
                hierarchy.append({
                    'building': row['building'],
                    'floor_level': row['floor_level'],
                    'zone': row['zone'],
                    'area': row['area'],
                    'location': row['location'],
                    'camera_count': row['camera_count'],
                    'camera_ids': row['camera_ids']
                })
        
        return JSONResponse(content={
            "summary": {
                "locations": summary_result['locations'],
                "areas": summary_result['areas'],
                "buildings": summary_result['buildings'],
                "floor_levels": summary_result['floor_levels'],
                "zones": summary_result['zones'],
                "total_cameras": summary_result['total_cameras']
            },
            "hierarchy": hierarchy,
            "workspace_id": str(workspace_id_obj),
            "database": "postgresql",
            "data_source": "postgresql"
        })
        
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
    """Search location data in PostgreSQL."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Get workspace using workspace_service
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            try:
                query = "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
                member_info = await db_manager.execute_query(
                    query, (user_id, workspace_id_obj), fetch_one=True
                )
                if not member_info:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="You are not a member of this workspace"
                    )
                workspace_role = member_info['role']
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error checking workspace membership: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to verify workspace access"
                )
        
        # Search
        result = await postgres_service.search_location_data(
            workspace_id=workspace_id_obj,
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
