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

from fastapi import APIRouter, HTTPException, Depends, Query
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
    """
    Convert s3:// URIs to pre-signed HTTPS URLs.
    Also normalises image extensions to .webp as per spec.
    """
    if not paths:
        return paths
    resolved = []
    for path in paths:
        if isinstance(path, str) and path.startswith("s3://"):
            # Normalise image extension
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
    """Resolve evidence_paths and video_path S3 URLs in place."""
    if event.get("evidence_paths"):
        event["evidence_paths"] = await resolve_s3_paths(event["evidence_paths"])
    if event.get("video_path") and str(event["video_path"]).startswith("s3://"):
        event["video_path"] = await s3_service.get_presigned_url(event["video_path"])
    return event


# ─────────────────────────────────────────────
# Core Events  (Page 1 / Page 2 shared)
# ─────────────────────────────────────────────

@router.get("/events", response_model=ShopliftingEventListResponse)
async def get_shoplifting_events(
    status_filter: Optional[str] = Query(None, description="detected | confirmed | dismissed | resolved"),
    camera_id: Optional[str]     = Query(None, description="Filter by stream/camera UUID"),
    start_date: Optional[str]    = Query(None, description="ISO-8601 start datetime"),
    end_date: Optional[str]      = Query(None, description="ISO-8601 end datetime"),
    page: int  = Query(1,  ge=1),
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Paginated list of shoplifting events.
    Returns evidence_paths as pre-signed S3 URLs.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    offset = (page - 1) * limit

    events = await shoplifting_service.get_events(
        workspace_id=workspace_id,
        status=status_filter,
        camera_id=camera_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )

    for ev in events:
        await enrich_event(ev)

    total = await shoplifting_service.count_events(workspace_id=workspace_id, status=status_filter)

    return ShopliftingEventListResponse(items=events, total=total, limit=limit, offset=offset)


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
        event_id=event_id,
        status=request.status,
        action_taken=request.action_taken,
        description=request.description,
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to resolve event")

    return {"message": f"Event {event_id} updated to '{request.status}'"}


@router.post("/test-alert/{camera_id}")
async def trigger_test_alert(camera_id: str):
    """
    Test Endpoint: Force a shoplifting alert on the specified camera stream.
    This bypasses the ML model and triggers the S3 video buffering and Postgres save pipeline.
    """
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
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    events = await shoplifting_service.get_active_dashboard(workspace_id=workspace_id)
    for ev in events:
        await enrich_event(ev)
    return events


@router.get("/dashboard/summary", response_model=List[ShopliftingDailySummary])
async def get_shoplifting_daily_summary(
    limit: int = Query(30, ge=1, le=365, description="Days to return"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Daily rollup summary (existing view)."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_daily_summary(workspace_id=workspace_id, limit=limit)


# ─────────────────────────────────────────────
# PAGE 1  –  Incident Videos
# ─────────────────────────────────────────────

@router.get("/incidents/videos")
async def get_incident_videos(
    camera_id: Optional[str]  = Query(None, description="Filter by camera UUID"),
    start_date: Optional[str] = Query(None, description="ISO-8601 start datetime"),
    end_date: Optional[str]   = Query(None, description="ISO-8601 end datetime"),
    page: int  = Query(1,  ge=1),
    limit: int = Query(10, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 4 – Behavioral Sequence Analysis (video tab).
    Returns the most recent incident videos as S3 pre-signed URLs.
    No filter  → latest 10 incidents.
    With filter → paginated by camera hierarchy and/or date range.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    offset = (page - 1) * limit

    videos = await shoplifting_service.get_incident_videos(
        workspace_id=workspace_id,
        camera_id=camera_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )

    for v in videos:
        await enrich_event(v)

    return {"items": videos, "page": page, "limit": limit}


@router.get("/incidents/behavior-sequences")
async def get_behavior_sequences(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 4 – Behavioral Sequence Analysis (stats).
    State frequencies and which states correlate with shoplifting.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_behavior_sequences(workspace_id=workspace_id)


@router.get("/incidents/confidence-audit")
async def get_confidence_audit(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 5 – Model Accuracy & Confidence Audit (incident video tab).
    Per-event confidence scores + evidence URLs.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    data = await shoplifting_service.get_model_accuracy(workspace_id=workspace_id)
    # enrich evidence paths in per-event list
    for ev in data.get("confidence_per_event", []):
        await enrich_event(ev)
    return data


@router.patch("/incidents/{event_id}/action")
async def set_incident_action(
    event_id: int,
    request: ResolveEventRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 6 – Dropdown action for each incident (Page 1).
    Convenience alias for resolve endpoint.
    action_taken: 'intervened' | 'police_called' | 'no_action'
    """
    await get_workspace_id_for_user(current_user["username"])
    await shoplifting_service.resolve_event(
        event_id=event_id,
        status=request.status,
        action_taken=request.action_taken,
        description=request.description,
    )
    return {"message": f"Action recorded for event {event_id}"}


# ─────────────────────────────────────────────
# PAGE 2  –  Visualisations
# ─────────────────────────────────────────────

@router.get("/dashboard/executive-summary")
async def get_executive_summary(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 1 – Executive Loss Prevention Summary.
    Returns:
      • incidents_by_location  → bar chart (# incidents per location)
      • lost_items_by_camera   → bar chart (# lost items per camera + date)
      • trend                  → line chart (shoplifting events over time)
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_executive_summary(workspace_id=workspace_id)


@router.get("/dashboard/hotspots")
async def get_hotspots(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 3 – Hotspot & Risk Mapping.
    Returns:
      • heatmap               → incidents per camera zone & name per day
      • event_type_breakdown  → event_type distribution by location
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_hotspots(workspace_id=workspace_id)


@router.get("/dashboard/high-value-analysis")
async def get_high_value_analysis(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 7 – High-Value Target & Item Analysis.
    Returns:
      • items_stolen           → bar chart (items count per zone + date)
      • total_estimated_value  → bar chart (total value per date + severity)
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_high_value_analysis(workspace_id=workspace_id)


@router.get("/dashboard/session-insights")
async def get_session_insights(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 9 – Store Traffic & Session Insights.
    Returns:
      • avg_behavior_per_session  → bar chart (avg # behaviors per session
                                    broken down by camera / zone / location)
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_session_insights(workspace_id=workspace_id)


@router.get("/dashboard/model-accuracy")
async def get_model_accuracy(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 5 – Model Accuracy & Confidence Audit (chart tab).
    Returns:
      • confidence_per_event  → confidence score per incident video
      • average_per_camera    → bar chart (avg confidence per camera)
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    data = await shoplifting_service.get_model_accuracy(workspace_id=workspace_id)
    for ev in data.get("confidence_per_event", []):
        await enrich_event(ev)
    return data


@router.get("/dashboard/regional-risk")
async def get_regional_risk(
    zone: Optional[str] = Query(None, description="Filter by zone name"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 10 – Regional Risk Benchmark.
    Returns:
      • incidents_per_1000_sessions  → bar chart (normalized incident rate)
      • top_risk_areas               → top-5 highest risk locations (bar chart)
    No time filter; optional zone filter.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_regional_risk(workspace_id=workspace_id, zone=zone)


# ─────────────────────────────────────────────
# PAGE 3  –  Camera Information
# ─────────────────────────────────────────────

@router.get("/dashboard/camera-health")
async def get_camera_health(
    camera_name: Optional[str] = Query(None, description="Filter by camera name (partial match)"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 8 – Camera Health & Coverage.
    Returns camera status (active/inactive), observation counts,
    installation date, and location hierarchy.
    Filterable by camera name only.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_camera_health(
        workspace_id=workspace_id,
        camera_name=camera_name,
    )


@router.get("/dashboard/real-time-activity")
async def get_realtime_activity(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 2 – Real-Time Store Activity Monitor.
    Returns sessions observed in the last hour, sorted by
    detection_confidence descending, with full camera location hierarchy.
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_realtime_activity(workspace_id=workspace_id)


@router.get("/dashboard/operational-efficiency")
async def get_operational_efficiency(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """
    Feature 6 – Operational Efficiency & Response.
    Returns:
      • resolution_times  → time from detection to resolution per event
      • action_breakdown  → counts of each action_taken value
      • open_events       → unresolved events (for status table)
    """
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await shoplifting_service.get_operational_efficiency(workspace_id=workspace_id)
