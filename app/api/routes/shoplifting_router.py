from fastapi import APIRouter, HTTPException, Depends, Query, status
from typing import Dict, Any, List
from uuid import UUID

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.shoplifting_service import shoplifting_service
from app.schemas.shoplifting_schema import (
    ShopliftingEventResponse,
    ShopliftingEventListResponse,
    ResolveEventRequest,
    ShopliftingDailySummary,
    ActiveShopliftingEventSummary
)
from app.services.s3_service import s3_service

router = APIRouter(prefix="/shoplifting", tags=["Shoplifting"])

async def get_workspace_id_for_user(username: str) -> UUID:
    """
    Helper function to get workspace_id for a user.
    Raises HTTPException if no active workspace found.
    """
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400, 
            detail="No active workspace. Please set an active workspace."
        )
    return workspace_id_obj

@router.get("/events", response_model=ShopliftingEventListResponse)
async def get_shoplifting_events(
    status_filter: str = Query(None, description="Filter by status (e.g., 'detected', 'confirmed')"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get a paginated list of shoplifting events for the current workspace."""
    username = current_user["username"]
    workspace_id = await get_workspace_id_for_user(username)
    
    offset = (page - 1) * limit
    
    events = await shoplifting_service.get_events(
        workspace_id=workspace_id,
        status=status_filter,
        limit=limit,
        offset=offset
    )
    
    
    # Process S3 URLs
    for event in events:
        if event.get("image_path") and str(event["image_path"]).startswith("s3://"):
            event["image_path"] = await s3_service.get_presigned_url(event["image_path"])
            
    total = await shoplifting_service.count_events(workspace_id=workspace_id, status=status_filter)
    
    return ShopliftingEventListResponse(
        items=events,
        total=total,
        limit=limit,
        offset=offset
    )

@router.patch("/events/{event_id}/resolve")
async def resolve_shoplifting_event(
    event_id: int,
    request: ResolveEventRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Update the status of a shoplifting event.
    Status can be 'confirmed', 'dismissed', or 'resolved'.
    """
    username = current_user["username"]
    # Ensures the user has an active workspace
    await get_workspace_id_for_user(username)
    
    success = await shoplifting_service.resolve_event(
        event_id=event_id,
        status=request.status,
        action_taken=request.action_taken,
        description=request.description
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to resolve event")
        
    return {"message": f"Event {event_id} status updated to {request.status}"}

@router.get("/dashboard/active", response_model=List[ActiveShopliftingEventSummary])
async def get_active_shoplifting_dashboard(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all active/unresolved shoplifting events for the dashboard."""
    username = current_user["username"]
    workspace_id = await get_workspace_id_for_user(username)
    
    events = await shoplifting_service.get_active_dashboard(workspace_id=workspace_id)
    
    # Process S3 URLs
    for event in events:
        if event.get("image_path") and str(event["image_path"]).startswith("s3://"):
            event["image_path"] = await s3_service.get_presigned_url(event["image_path"])
            
    return events

@router.get("/dashboard/summary", response_model=List[ShopliftingDailySummary])
async def get_shoplifting_daily_summary(
    limit: int = Query(30, ge=1, le=365, description="Number of days to return"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get daily summary rollup of shoplifting statistics."""
    username = current_user["username"]
    workspace_id = await get_workspace_id_for_user(username)
    
    return await shoplifting_service.get_daily_summary(workspace_id=workspace_id, limit=limit)
