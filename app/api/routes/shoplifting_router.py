# app/routes/shoplifting_router.py
"""
Shoplifting Detection API
=========================
Endpoints covering all 10 dashboard features.

Page 1 – Incident Videos  : /events, /incidents/videos, /incidents/behavior-sequences
Page 2 – Visualizations   : /dashboard/executive-summary, /dashboard/hotspots,
                            /dashboard/high-value-analysis, /dashboard/session-insights,
                            /dashboard/model-accuracy, /dashboard/regional-risk,
                            /dashboard/summary
Page 3 – Camera / Ops     : /dashboard/camera-health, /dashboard/real-time-activity,
                            /dashboard/operational-efficiency
"""

import logging

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
from uuid import UUID

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.shoplifting_service import shoplifting_service
from app.services.s3_service import s3_service
from app.schemas.shoplifting_schema import (
    ShopliftingEventResponse,
    ShopliftingEventListResponse,
    ResolveEventRequest,
    ShopliftingDailySummary,
    ActiveShopliftingEventSummary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/shoplifting", tags=["Shoplifting"])


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def get_workspace_id_for_user(username: str) -> UUID:
    """Return the active workspace UUID for the given user."""
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400,
            detail="No active workspace. Please set an active workspace.",
        )
    return workspace_id_obj


async def resolve_s3_paths(paths: Optional[List[str]]) -> Optional[List[str]]:
    if not paths:
        return paths
    resolved = []
    for path in paths:
        if isinstance(path, str) and path.startswith("s3://"):
            lower = path.lower()
            for old_ext in (".jpg", ".jpeg", ".png"):
                if lower.endswith(old_ext):
                    path = path[: -len(old_ext)] + ".webp"
                    break
            url = await s3_service.get_presigned_url(path)
            resolved.append(url)
        else:
            resolved.append(path)
    return resolved


async def enrich_event(event: dict) -> dict:
    if event.get("evidence_paths"):
        event["evidence_paths"] = await resolve_s3_paths(event["evidence_paths"])
    if event.get("video_path") and str(event["video_path"]).startswith("s3://"):
        event["video_path"] = await s3_service.get_presigned_url(event["video_path"])
    return event


# Shared filter dependency – same 8 params on every filterable endpoint
def _filters(
    start_date: Optional[str]   = Query(None, description="ISO-8601 start datetime"),
    end_date: Optional[str]     = Query(None, description="ISO-8601 end datetime"),
    start_time: Optional[str]   = Query(None, description="Time-of-day lower bound (HH:MM)"),
    end_time: Optional[str]     = Query(None, description="Time-of-day upper bound (HH:MM)"),
    location: Optional[str]     = Query(None, description="Filter by camera location"),
    building: Optional[str]     = Query(None, description="Filter by building"),
    floor_level: Optional[str]  = Query(None, description="Filter by floor level"),
    zone: Optional[str]         = Query(None, description="Filter by zone"),
):
    return dict(
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        location=location, building=building,
        floor_level=floor_level, zone=zone,
    )


# ─────────────────────────────────────────────
# Core Events  (Page 1 / Page 2 shared)
# ─────────────────────────────────────────────

@router.get("/events", response_model=ShopliftingEventListResponse)
async def get_shoplifting_events(
    status_filter: Optional[str] = Query(None, description="detected | confirmed | dismissed | resolved"),
    camera_id: Optional[str]     = Query(None, description="Filter by stream/camera UUID"),
    f: dict = Depends(_filters),
    page: int  = Query(1,  ge=1),
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Paginated list of shoplifting events. Returns evidence_paths as pre-signed S3 URLs."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        offset = (page - 1) * limit

        events = await shoplifting_service.get_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id,
            limit=limit, offset=offset, **f,
        )
        for ev in events:
            await enrich_event(ev)

        total = await shoplifting_service.count_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id, **f,
        )
        return ShopliftingEventListResponse(items=events, total=total, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_shoplifting_events error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving shoplifting events.")


@router.patch("/events/{event_id}/resolve")
async def resolve_shoplifting_event(
    event_id: int,
    request: ResolveEventRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 6 – Operational action.
    action_taken values: 'intervened' | 'police_called' | 'no_action' (or any free text).
    """
    await get_workspace_id_for_user(current_user["username"])
    success = await shoplifting_service.resolve_event(
        event_id=event_id, status=request.status,
        action_taken=request.action_taken, description=request.description,
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to resolve event")
    return {"message": f"Event {event_id} updated to '{request.status}'"}


@router.post("/test-alert/{camera_id}")
async def trigger_test_alert(camera_id: str):
    """Test Endpoint: Force a shoplifting alert on the specified camera stream."""
    from app.services.shoplifting_inference import shoplifting_engine
    shoplifting_engine.force_alert(camera_id)
    return {"message": f"Simulated shoplifting alert triggered on camera {camera_id}. Please wait ~10 seconds for the buffer to save to S3."}


# ─────────────────────────────────────────────
# Active dashboard (legacy / quick view)
# ─────────────────────────────────────────────

@router.get("/dashboard/active", response_model=List[ActiveShopliftingEventSummary])
async def get_active_shoplifting_dashboard(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """All detected / under-review events (live view)."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        events = await shoplifting_service.get_active_dashboard(workspace_id=workspace_id)
        for ev in events:
            await enrich_event(ev)
        return events
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_active_shoplifting_dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving active dashboard.")


@router.get("/dashboard/summary", response_model=List[ShopliftingDailySummary])
async def get_shoplifting_daily_summary(
    limit: int = Query(30, ge=1, le=365, description="Days to return"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Daily rollup summary."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return await shoplifting_service.get_daily_summary(workspace_id=workspace_id, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_shoplifting_daily_summary error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving daily summary.")


# ─────────────────────────────────────────────
# PAGE 1  –  Incident Videos
# ─────────────────────────────────────────────

@router.get("/incidents/videos")
async def get_incident_videos(
    camera_id: Optional[str] = Query(None, description="Filter by camera UUID"),
    f: dict = Depends(_filters),
    page: int  = Query(1,  ge=1),
    limit: int = Query(10, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 4 – Behavioral Sequence Analysis (video tab).
    Returns the most recent incident videos as S3 pre-signed URLs.
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        offset = (page - 1) * limit
        videos = await shoplifting_service.get_incident_videos(
            workspace_id=workspace_id, camera_id=camera_id,
            limit=limit, offset=offset, **f,
        )
        for v in videos:
            await enrich_event(v)
        return JSONResponse(content={"items": videos, "page": page, "limit": limit})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_incident_videos error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving incident videos.")


@router.get("/incidents/behavior-sequences")
async def get_behavior_sequences(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 4 – Behavioral Sequence Analysis (stats).
    State frequencies and which states correlate with shoplifting.
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_behavior_sequences(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_behavior_sequences error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving behavior sequences.")


@router.get("/incidents/confidence-audit")
async def get_confidence_audit(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 5 – Model Accuracy & Confidence Audit (incident video tab).
    Per-event confidence scores + evidence URLs.
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        data = await shoplifting_service.get_model_accuracy(workspace_id=workspace_id, **f)
        for ev in data.get("confidence_per_event", []):
            await enrich_event(ev)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_confidence_audit error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving confidence audit.")


@router.patch("/incidents/{event_id}/action")
async def set_incident_action(
    event_id: int,
    request: ResolveEventRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 6 – Dropdown action for each incident (Page 1).
    action_taken: 'intervened' | 'police_called' | 'no_action'
    """
    await get_workspace_id_for_user(current_user["username"])
    await shoplifting_service.resolve_event(
        event_id=event_id, status=request.status,
        action_taken=request.action_taken, description=request.description,
    )
    return {"message": f"Action recorded for event {event_id}"}


# ─────────────────────────────────────────────
# PAGE 2  –  Visualisations
# ─────────────────────────────────────────────

@router.get("/dashboard/executive-summary")
async def get_executive_summary(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 1 – Executive Loss Prevention Summary.
    Returns:
      • incidents_by_location  → bar chart (# incidents per location)
      • lost_items_by_camera   → bar chart (# lost items per camera + date)
      • trend                  → line chart (shoplifting events over time)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_executive_summary(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_executive_summary error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving executive summary.")


@router.get("/dashboard/hotspots")
async def get_hotspots(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 3 – Hotspot & Risk Mapping.
    Returns:
      • heatmap               → incidents per camera zone & name per day
      • event_type_breakdown  → event_type distribution by location
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_hotspots(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_hotspots error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving hotspots.")


@router.get("/dashboard/high-value-analysis")
async def get_high_value_analysis(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 7 – High-Value Target & Item Analysis.
    Returns:
      • items_stolen           → bar chart (items count per zone + date)
      • total_estimated_value  → bar chart (total value per date + severity)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_high_value_analysis(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_high_value_analysis error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving high-value analysis.")


@router.get("/dashboard/session-insights")
async def get_session_insights(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 9 – Store Traffic & Session Insights.
    Returns:
      • avg_behavior_per_session  → bar chart (avg # behaviors per session
                                    broken down by camera / zone / location)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_session_insights(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_session_insights error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving session insights.")


@router.get("/dashboard/model-accuracy")
async def get_model_accuracy(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 5 – Model Accuracy & Confidence Audit (chart tab).
    Returns:
      • confidence_per_event  → confidence score per incident video
      • average_per_camera    → bar chart (avg confidence per camera)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        data = await shoplifting_service.get_model_accuracy(workspace_id=workspace_id, **f)
        for ev in data.get("confidence_per_event", []):
            await enrich_event(ev)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_model_accuracy error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving model accuracy.")


@router.get("/dashboard/regional-risk")
async def get_regional_risk(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 10 – Regional Risk Benchmark.
    Returns:
      • incidents_per_1000_sessions  → bar chart (normalized incident rate)
      • top_risk_areas               → top-5 highest risk locations (bar chart)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_regional_risk(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_regional_risk error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving regional risk.")


# ─────────────────────────────────────────────
# PAGE 3  –  Camera Information
# ─────────────────────────────────────────────

@router.get("/dashboard/camera-health")
async def get_camera_health(
    camera_name: Optional[str] = Query(None, description="Filter by camera name (partial match)"),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 8 – Camera Health & Coverage.
    Returns camera status (active/inactive), observation counts,
    installation date, and location hierarchy.
    Date/time filters narrow the observation count window.
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_camera_health(
            workspace_id=workspace_id, camera_name=camera_name, **f,
        ))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_camera_health error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving camera health.")


@router.get("/dashboard/real-time-activity")
async def get_realtime_activity(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 2 – Real-Time Store Activity Monitor.
    Returns sessions observed in the last hour, sorted by
    detection_confidence descending, with full camera location hierarchy.
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_realtime_activity(workspace_id=workspace_id))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_realtime_activity error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving real-time activity.")


@router.get("/dashboard/operational-efficiency")
async def get_operational_efficiency(
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 6 – Operational Efficiency & Response.
    Returns:
      • resolution_times  → time from detection to resolution per event
      • action_breakdown  → counts of each action_taken value
      • open_events       → unresolved events (for status table)
    """
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return JSONResponse(content=await shoplifting_service.get_operational_efficiency(workspace_id=workspace_id, **f))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_operational_efficiency error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving operational efficiency.")
