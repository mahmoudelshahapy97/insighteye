# app/routes/unified_detection_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, status
from fastapi.responses import JSONResponse
from typing import Optional, Dict
from uuid import UUID
import logging

from app.services.detection_data_service import detection_data_service
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.utils import parse_camera_ids

logger = logging.getLogger(__name__)
router = APIRouter(tags=["unified_detection"], prefix="/unified")

# ========== Unified Search Endpoints ==========

@router.get("/workspace/search_unified")
async def workspace_search_unified(
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
    include_frame: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Search detection data using unified service (PostgreSQL metadata + Qdrant frames).
    
    This endpoint provides the same filtering capabilities as workspace_search_results_with_location
    but retrieves metadata from PostgreSQL and frames from Qdrant for optimal performance.
    
    Query Parameters:
    - workspaceId: Optional workspace UUID (defaults to user's active workspace)
    - camera_id: Camera ID(s) to filter (comma-separated for multiple)
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - start_time: Start time (HH:MM:SS)
    - end_time: End time (HH:MM:SS)
    - location: Location name filter
    - area: Area name filter
    - building: Building name filter
    - floor_level: Floor level filter
    - zone: Zone name filter
    - page: Page number (default: 1)
    - per_page: Results per page (default: 10, use 'none' for all)
    - include_frame: Include frame data from Qdrant (default: true)
    
    Returns:
        Combined results with metadata from PostgreSQL and frames from Qdrant
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        # Get user info
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, 
                detail="User not found"
            )
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail="Invalid workspaceId format."
                )
        else:
            # Get user's active workspace
            ws_query = """
                SELECT workspace_id FROM workspace_members 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT 1
            """
            ws_result = await db_manager.execute_query(
                ws_query, (requesting_user_id,), fetch_one=True
            )
            if not ws_result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail="No active workspace found."
                )
            final_workspace_id = ws_result['workspace_id']
        
        # Check workspace membership and permissions
        workspace_role = None
        if not is_system_admin:
            member_query = """
                SELECT role FROM workspace_members 
                WHERE user_id = $1 AND workspace_id = $2
            """
            member_result = await db_manager.execute_query(
                member_query, (requesting_user_id, final_workspace_id), fetch_one=True
            )
            if not member_result:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You are not a member of this workspace."
                )
            workspace_role = member_result['role']
        
        # Check search feature access
        can_search = (
            user_db_info.get("is_search", False) or 
            is_system_admin or 
            (workspace_role in ["admin", "owner"])
        )
        
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
        camera_ids = parse_camera_ids(camera_id_param) if camera_id_param else None
        
        # Execute unified search
        results = await detection_data_service.retrieve_detection_data_unified(
            workspace_id=final_workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            camera_id=camera_ids,
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
            include_frame=include_frame
        )
        
        # Add additional metadata
        results["workspace_id"] = str(final_workspace_id)
        results["access_level"] = "system_admin" if is_system_admin else (workspace_role or "member")
        results["pagination_disabled"] = processed_per_page is None
        
        return JSONResponse(content=results)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unified search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during unified data search."
        )


@router.get("/workspace/retrieve_single")
async def retrieve_single_detection(
    result_id: str = Query(..., description="Result ID to retrieve"),
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    include_frame: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Retrieve a single detection result by ID from unified storage.
    
    Query Parameters:
    - result_id: UUID of the detection result (required)
    - workspaceId: Optional workspace UUID (defaults to user's active workspace)
    - include_frame: Include frame data from Qdrant (default: true)
    
    Returns:
        Single detection result with metadata and optional frame
    """
    try:
        requesting_user_id = current_user_data["user_id"]
        
        # Validate result_id format
        try:
            result_uuid = UUID(result_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid result_id format."
            )
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid workspaceId format."
                )
        else:
            ws_query = """
                SELECT workspace_id FROM workspace_members 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT 1
            """
            ws_result = await db_manager.execute_query(
                ws_query, (requesting_user_id,), fetch_one=True
            )
            if not ws_result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No active workspace found."
                )
            final_workspace_id = ws_result['workspace_id']
        
        # Retrieve detection data
        result = await detection_data_service.retrieve_single_detection_unified(
            result_id=result_uuid,
            workspace_id=final_workspace_id,
            include_frame=include_frame
        )
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Detection result {result_id} not found in workspace."
            )
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving single detection: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving detection result."
        )


@router.get("/search_unified")
async def search_unified_user_active_workspace(
    camera_id: Optional[str] = Query(None),
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
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Shorthand endpoint to search in user's active workspace using unified service.
    
    Automatically uses the requesting user's active workspace.
    """
    return await workspace_search_unified(
        workspace_id_query=None,
        camera_id_param=camera_id,
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
        per_page=per_page,
        include_frame=include_frame,
        current_user_data=current_user_data
    )