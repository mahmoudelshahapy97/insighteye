# app/routes/blocked_exit_router.py
"""
Blocked Exit Detection API
===========================
Monitors emergency exits for obstructions and reports exit accessibility.

/status                          - live state of every blocked-exit camera
/events                          - list / filter events (episodes)
/events/{event_id}               - one event with evidence snapshot URLs
/events/{event_id}/resolve       - acknowledge / resolve an event
/events/{event_id}/acknowledge   - shorthand for resolve(status=acknowledged)
/dashboard/active                - live unresolved events
/dashboard/summary               - daily rollup
/analytics/*                     - summary, most-blocked, daily-report, hourly, timeline
/configs                         - every door-polygon config in the workspace
/cameras/{stream_id}/door-polygon - set/update door polygon calibration
/cameras/{stream_id}/config      - read / patch settings / delete a config
/test-alert/{stream_id}          - send a test notification
/workspace/delete_data           - filtered delete
/workspace/delete_all_data       - delete all (confirm required)
"""

import asyncio
import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends, Query, Response
from typing import Dict, Any, List, Optional

from app.services.session_service import session_manager
from app.services.feature_service import require_feature
from app.services.workspace_service import workspace_service
from app.services.blocked_exit_service import blocked_exit_service
from app.services.user_service import user_manager
from app.services.database import db_manager
from app.services.notification_service import notification_service
from app.services import camera_runtime
from app.services.nez import rtsp_probe
from app.utils.permission_utils import check_workspace_access
from app.schemas.blocked_exit_schema import (
    BlockedExitEventResponse,
    BlockedExitEventDetail,
    BlockedExitEventListResponse,
    ResolveBlockedExitRequest,
    BlockedExitDailySummary,
    ActiveBlockedExitEventSummary,
    ExitZoneConfigRequest,
    ExitZoneConfigUpdate,
    ExitZoneConfigResponse,
    BlockedExitCameraStatus,
    BlockedExitAnalyticsSummary,
    BlockedExitCameraStat,
    BlockedExitDailyReport,
    BlockedExitHeatmapCell,
    BlockedExitTimelineEntry,
    BlockedExitCamera,
    ProbeResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/blocked-exit", tags=["Blocked Exit"],
                   dependencies=[Depends(require_feature("blocked_exit"))])


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


async def _require_camera(stream_id: UUID, workspace_id: UUID) -> Dict[str, Any]:
    """404 unless the camera belongs to the caller's workspace."""
    camera = await blocked_exit_service.get_camera(workspace_id, stream_id)
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    return camera


def _hot_reload(stream_id: UUID, cfg: Optional[Dict[str, Any]]) -> None:
    try:
        from app.services.blocked_exit_inference import blocked_exit_engine
        blocked_exit_engine.set_zone_config(str(stream_id), cfg)
    except Exception as e:
        logger.warning(f"Failed to hot-reload door polygon for {stream_id}: {e}")


def _to_json_safe(obj):
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_safe(i) for i in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, UUID):
        return str(obj)
    return obj


def _filters(
    start_date: Optional[str] = Query(None, description="ISO-8601 start datetime"),
    end_date: Optional[str] = Query(None, description="ISO-8601 end datetime"),
    start_time: Optional[str] = Query(None, description="Time-of-day lower bound (HH:MM)"),
    end_time: Optional[str] = Query(None, description="Time-of-day upper bound (HH:MM)"),
    location: Optional[str] = Query(None, description="Filter by camera location"),
    building: Optional[str] = Query(None, description="Filter by building"),
    floor_level: Optional[str] = Query(None, description="Filter by floor level"),
    zone: Optional[str] = Query(None, description="Filter by zone"),
):
    return dict(
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        location=location, building=building,
        floor_level=floor_level, zone=zone,
    )


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

@router.get("/events", response_model=BlockedExitEventListResponse)
async def get_blocked_exit_events(
    status_filter: Optional[str] = Query(
        None, pattern="^(detected|acknowledged|resolved)$", description="detected | acknowledged | resolved",
    ),
    camera_id: Optional[UUID] = Query(None, description="Filter by stream/camera UUID"),
    state: Optional[str] = Query(None, pattern="^(partially_blocked|blocked)$", description="Worst state reached"),
    risk_level: Optional[str] = Query(None, pattern="^(low|medium|high|critical)$"),
    open_only: bool = Query(False, description="Only detected / acknowledged events"),
    sort: str = Query("time", pattern="^(time|risk)$"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Paginated list of blocked-exit events."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        offset = (page - 1) * limit
        extra = dict(
            state=state, risk_level=risk_level, open_only=open_only,
            camera_id=str(camera_id) if camera_id else None,
        )

        events = await blocked_exit_service.get_events(
            workspace_id=workspace_id, status=status_filter,
            limit=limit, offset=offset, order_by_risk=(sort == "risk"), **extra, **f,
        )
        total = await blocked_exit_service.count_events(
            workspace_id=workspace_id, status=status_filter, **extra, **f,
        )
        return BlockedExitEventListResponse(items=events, total=total, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_blocked_exit_events error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving blocked-exit events.")


@router.get("/events/{event_id}", response_model=BlockedExitEventDetail)
async def get_blocked_exit_event(
    event_id: int,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """One event, with presigned URLs for its evidence snapshots."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    event = await blocked_exit_service.get_event(workspace_id, event_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    urls = []
    if event.get("evidence_paths"):
        from app.services.s3_service import s3_service
        for path in event["evidence_paths"]:
            url = await s3_service.get_presigned_url(path)
            if url and url.startswith("http"):
                urls.append(url)
    return {**event, "snapshot_urls": urls}


async def _set_event_status(event_id: int, status: str, description: Optional[str], current_user: Dict):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    success = await blocked_exit_service.resolve_event(
        workspace_id=workspace_id, event_id=event_id, status=status,
        description=description, user_id=current_user.get("user_id"),
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return {"message": f"Event {event_id} updated to '{status}'"}


@router.post("/events/{event_id}/resolve")
async def resolve_blocked_exit_event(
    event_id: int,
    request: ResolveBlockedExitRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    return await _set_event_status(event_id, request.status, request.description, current_user)


@router.post("/events/{event_id}/acknowledge")
async def acknowledge_blocked_exit_event(
    event_id: int,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    return await _set_event_status(event_id, "acknowledged", None, current_user)


@router.get("/dashboard/active", response_model=List[ActiveBlockedExitEventSummary])
async def get_active_blocked_exit_dashboard(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return await blocked_exit_service.get_active_dashboard(workspace_id=workspace_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_active_blocked_exit_dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving active dashboard.")


@router.get("/dashboard/summary", response_model=List[BlockedExitDailySummary])
async def get_blocked_exit_daily_summary(
    limit: int = Query(30, ge=1, le=365, description="Days to return"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return await blocked_exit_service.get_daily_summary(workspace_id=workspace_id, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_blocked_exit_daily_summary error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving daily summary.")


# ─────────────────────────────────────────────
# Zone / door-polygon configuration
# ─────────────────────────────────────────────

@router.put("/cameras/{stream_id}/door-polygon", response_model=ExitZoneConfigResponse)
async def set_door_polygon(
    stream_id: UUID,
    request: ExitZoneConfigRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        await _require_camera(stream_id, workspace_id)
        row = await blocked_exit_service.set_door_polygon(
            stream_id=stream_id,
            workspace_id=workspace_id,
            door_polygon=request.door_polygon,
            calibration_frame_w=request.calibration_frame_w,
            calibration_frame_h=request.calibration_frame_h,
            detector_strategy=request.detector_strategy,
            min_accessibility_pct=request.min_accessibility_pct,
            debounce_seconds=request.debounce_seconds,
            is_active=request.is_active,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
        _hot_reload(stream_id, row)
        return row
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"set_door_polygon error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error saving door polygon.")


@router.get("/cameras/{stream_id}/config", response_model=Optional[ExitZoneConfigResponse])
async def get_zone_config(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    return await blocked_exit_service.get_zone_config(stream_id, workspace_id)


@router.patch("/cameras/{stream_id}/config", response_model=ExitZoneConfigResponse)
async def update_zone_config(
    stream_id: UUID,
    request: ExitZoneConfigUpdate,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Change detector settings (threshold, debounce, strategy, active) without redrawing."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    row = await blocked_exit_service.update_config(
        stream_id=stream_id, workspace_id=workspace_id, fields=request.model_dump(exclude_unset=True),
    )
    if not row:
        raise HTTPException(status_code=404, detail="This camera has no door polygon yet.")
    _hot_reload(stream_id, row)
    if row.get("is_active") is False:
        await blocked_exit_service.close_open_episodes(stream_id)
    return row


@router.delete("/cameras/{stream_id}/config")
async def delete_zone_config(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Remove the door polygon; monitoring for this camera stops and any open episode closes."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    if not await blocked_exit_service.delete_config(stream_id=stream_id, workspace_id=workspace_id):
        raise HTTPException(status_code=404, detail="This camera has no door polygon.")
    _hot_reload(stream_id, None)
    await blocked_exit_service.close_open_episodes(stream_id)
    return {"message": "Door polygon removed"}


@router.get("/configs", response_model=List[ExitZoneConfigResponse])
async def list_zone_configs(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await blocked_exit_service.list_configs(workspace_id)


# ─────────────────────────────────────────────
# Cameras: connection status, stills, connection checks
# The source path can embed credentials; it is only ever used server-side and
# shown to clients as source_host (masked).
# ─────────────────────────────────────────────

@router.get("/permissions")
async def get_permissions(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """What the caller may do here, so the UI can hide actions that would 403."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])

    async def allowed(role: str) -> bool:
        try:
            await check_workspace_access(
                db_manager, current_user["user_id"], workspace_id,
                required_role=role, system_role=current_user.get("role"),
            )
            return True
        except HTTPException:
            return False

    return {"can_resolve": await allowed("member"), "can_manage": await allowed("admin")}


async def _with_health(row: Dict[str, Any], source: Optional[str]) -> Dict[str, Any]:
    health = await camera_runtime.camera_health(
        row["stream_id"], source, locked_by_server=row.get("locked_by_server"),
    )
    return {
        **{k: v for k, v in row.items() if k != "locked_by_server"},
        "running_elsewhere": health["live_status"] == "running_on_other_server",
        "source_host": camera_runtime.source_host(source),
        "health": health,
    }


async def _source_for(stream_id: UUID, workspace_id: UUID) -> Dict[str, Any]:
    cam = await blocked_exit_service.get_camera_source(stream_id, workspace_id)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    return cam


@router.get("/cameras", response_model=List[BlockedExitCamera])
async def list_cameras(
    enabled_only: bool = Query(True, description="Only cameras with Blocked Exit enabled"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    rows = await blocked_exit_service.list_cameras(workspace_id, enabled_only=enabled_only)
    sources = {
        s["stream_id"]: s["path"]
        for s in await _all_sources(workspace_id, enabled_only)
    }
    return [await _with_health(r, sources.get(r["stream_id"])) for r in rows]


async def _all_sources(workspace_id: UUID, enabled_only: bool) -> List[Dict[str, Any]]:
    if enabled_only:
        return await blocked_exit_service.get_camera_sources(workspace_id)
    return await db_manager.execute_query(
        "SELECT stream_id, name, path, type FROM video_stream WHERE workspace_id = $1",
        (workspace_id,),
        fetch_all=True,
    ) or []


@router.post("/cameras/probe-all", response_model=List[ProbeResult])
async def probe_all_cameras(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Test the connection to every Blocked Exit camera. Runs concurrently, but the
    probe module caps how many sources are opened at once."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    cams = await blocked_exit_service.get_camera_sources(workspace_id)

    async def one(cam: Dict[str, Any]) -> Dict[str, Any]:
        info = await rtsp_probe.probe(cam["path"])
        return {**info, "stream_id": cam["stream_id"], "camera_name": cam["name"]}

    return await asyncio.gather(*(one(c) for c in cams))


@router.get("/cameras/{stream_id}", response_model=BlockedExitCamera)
async def get_camera(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    source = await _source_for(stream_id, workspace_id)
    rows = await blocked_exit_service.list_cameras(workspace_id, enabled_only=False, stream_id=stream_id)
    if not rows:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    return await _with_health(rows[0], source["path"])


@router.get("/cameras/{stream_id}/snapshot", response_class=Response)
async def get_camera_snapshot(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """A JPEG still: the running stream's latest frame, or a one-off grab from the
    camera when it isn't streaming (so a door can be drawn on a stopped camera)."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    source = await _source_for(stream_id, workspace_id)
    jpeg, info = await camera_runtime.live_or_grab_jpeg(source["path"])
    if jpeg is None:
        raise HTTPException(status_code=502, detail=f"Camera unreachable: {info['error']}")
    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-store",
            "X-Frame-Source": info["frame_source"],
            "X-Frame-Width": str(info["width"]),
            "X-Frame-Height": str(info["height"]),
        },
    )


@router.post("/cameras/{stream_id}/probe", response_model=ProbeResult)
async def probe_camera(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    source = await _source_for(stream_id, workspace_id)
    info = await rtsp_probe.probe(source["path"])
    return {**info, "stream_id": stream_id, "camera_name": source["name"]}


# ─────────────────────────────────────────────
# Live status
# ─────────────────────────────────────────────

@router.get("/status", response_model=List[BlockedExitCameraStatus])
async def get_blocked_exit_status(
    stream_id: Optional[UUID] = Query(None),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Every blocked-exit camera with its calibration and ongoing blockage, worst first."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        return await blocked_exit_service.get_camera_status(workspace_id, stream_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_blocked_exit_status error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving blocked-exit status.")


@router.post("/test-alert/{stream_id}")
async def send_test_alert(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Send a clearly-labelled test notification for a camera. Records no event.
    Workspace admins/owners only."""
    _, workspace_id = await _require_admin_or_owner(current_user)
    camera = await _require_camera(stream_id, workspace_id)
    message = f"🚪 [TEST] Blocked-exit alert test for '{camera['name']}'"
    notification = await notification_service.create_notification(
        workspace_id=workspace_id,
        user_id=current_user["user_id"],
        status="warning",
        message=message,
        stream_id=stream_id,
        camera_name=camera["name"],
    )
    if not notification:
        raise HTTPException(status_code=500, detail="Could not create the test notification.")
    try:
        from app.services.stream_service import stream_manager
        await stream_manager.broadcast_notification(
            str(current_user["user_id"]),
            {
                "type": "new_notification",
                "notification": {
                    "id": str(notification.get("notification_id")),
                    "user_id": str(notification.get("user_id")),
                    "workspace_id": str(notification.get("workspace_id")),
                    "stream_id": str(stream_id),
                    "camera_name": camera["name"],
                    "status": notification.get("status"),
                    "message": message,
                    "timestamp": (notification.get("timestamp") or datetime.now(ZoneInfo("Africa/Cairo"))).timestamp(),
                    "read": False,
                },
            },
        )
    except Exception as e:
        logger.warning(f"test-alert websocket broadcast failed: {e}")
    return {"message": f"Test alert sent for '{camera['name']}'"}


# ─────────────────────────────────────────────
# Analytics
# ─────────────────────────────────────────────

@router.get("/analytics/summary", response_model=BlockedExitAnalyticsSummary)
async def analytics_summary(
    days: int = Query(7, ge=1, le=365),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await blocked_exit_service.analytics_summary(workspace_id, days)


@router.get("/analytics/most-blocked", response_model=List[BlockedExitCameraStat])
async def analytics_most_blocked(
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(10, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await blocked_exit_service.most_blocked(workspace_id, days, limit)


@router.get("/analytics/daily-report", response_model=BlockedExitDailyReport)
async def analytics_daily_report(
    report_date: Optional[date] = Query(None, description="Defaults to today (Africa/Cairo)"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    report_date = report_date or datetime.now(ZoneInfo("Africa/Cairo")).date()
    return await blocked_exit_service.daily_report(workspace_id, report_date)


@router.get("/analytics/hourly", response_model=List[BlockedExitHeatmapCell])
async def analytics_hourly(
    days: int = Query(30, ge=1, le=365),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await blocked_exit_service.hourly_heatmap(workspace_id, days)


@router.get("/analytics/timeline/{stream_id}", response_model=List[BlockedExitTimelineEntry])
async def analytics_timeline(
    stream_id: UUID,
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(200, ge=1, le=1000),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    return await blocked_exit_service.timeline(workspace_id, stream_id, days, limit)


# ─────────────────────────────────────────────
# Data Management – delete events
# ─────────────────────────────────────────────

async def _require_admin_or_owner(current_user: Dict) -> tuple:
    requesting_user_id = current_user["user_id"]
    username = current_user["username"]

    user_db_info = await user_manager.get_user_by_id(requesting_user_id)
    if not user_db_info:
        raise HTTPException(status_code=401, detail="User not found")

    system_role = user_db_info.get("role")
    is_system_admin = system_role in ("admin", "superadmin")

    workspace_id = await get_workspace_id_for_user(username)

    if not is_system_admin:
        member = await db_manager.execute_query(
            "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2",
            (requesting_user_id, workspace_id),
            fetch_one=True,
        )
        if not member:
            raise HTTPException(status_code=403, detail="You are not a member of this workspace.")
        if member["role"] not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Only workspace admins can delete data.")

    return user_db_info, workspace_id


@router.delete("/workspace/delete_data")
async def delete_blocked_exit_data(
    camera_id: Optional[str] = Query(None, description="Filter by camera UUID"),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        _, workspace_id = await _require_admin_or_owner(current_user)
        result = await blocked_exit_service.delete_events(
            workspace_id=workspace_id, camera_id=camera_id, **f,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"delete_blocked_exit_data error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error deleting blocked-exit data.")


@router.delete("/workspace/delete_all_data")
async def delete_all_blocked_exit_data(
    confirm: bool = Query(False, description="Must be true to confirm deletion of all events"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set 'confirm=true' to delete all blocked-exit data.",
        )
    try:
        _, workspace_id = await _require_admin_or_owner(current_user)
        result = await blocked_exit_service.delete_all_events(workspace_id=workspace_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"delete_all_blocked_exit_data error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error deleting all blocked-exit data.")
