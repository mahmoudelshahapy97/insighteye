# app/routes/no_entry_zone_router.py
"""
No-Entry Zone Detection API
=============================
Restricted-polygon intrusion detection.

/overview                         - KPI cards + latest incidents
/permissions                      - what the caller may change (drives the UI)
/cameras                          - cameras enabled for no-entry-zone, with zone counts
/incidents                        - list / filter incidents (events grouped by incident_id)
/incidents/{incident_id}/resolve  - acknowledge / resolve every event of an incident
/events                           - list / filter violation events
/events/{event_id}                - one event
/events/{event_id}/evidence       - short-lived URLs for the snapshot and clip
/events/{event_id}/resolve        - acknowledge / resolve an event
/analytics/summary                - time series + breakdowns
/dashboard/active                 - live unresolved events
/dashboard/summary                - daily rollup
/zones                            - list / create zones
/zones/{zone_id}                  - get / update / delete a zone
/workspace/delete_data            - filtered delete
/workspace/delete_all_data        - delete all (confirm required)

Reading is open to every workspace member; acknowledging/resolving needs member (not
viewer); zone changes and data deletion need workspace admin. System admins bypass.
"""

import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncio

import cv2
from fastapi import APIRouter, HTTPException, Depends, Query, Response
from typing import Dict, List, Literal, Optional

from app.services.session_service import session_manager
from app.services.feature_service import require_feature
from app.services.workspace_service import workspace_service
from app.services.no_entry_zone_service import no_entry_zone_service
from app.services.database import db_manager
from app.utils.permission_utils import check_workspace_access
from app.services.nez import rtsp_probe
from app.schemas.no_entry_zone_schema import (
    NoEntryEventListResponse,
    NoEntryEventResponse,
    NoEntryIncidentListResponse,
    ResolveIncidentResponse,
    ResolveNoEntryRequest,
    NoEntryDailySummary,
    ActiveNoEntryEventSummary,
    OverviewResponse,
    AnalyticsSummary,
    EvidenceUrls,
    NoEntryCamera,
    NoEntryCameraDetail,
    ProbeResult,
    EventStatus,
    ZoneCreateRequest,
    ZoneUpdateRequest,
    ZoneResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/no-entry-zone", tags=["No Entry Zone"],
                   dependencies=[Depends(require_feature("no_entry_zone"))])

EVIDENCE_URL_TTL = 900


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def get_workspace_id_for_user(username: str) -> UUID:
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400,
            detail="No active workspace. Please set an active workspace.",
        )
    return workspace_id_obj


async def _workspace_with_role(current_user: Dict, required_role: Optional[str]) -> UUID:
    """Resolve the caller's active workspace and enforce a workspace role."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await check_workspace_access(
        db_manager, current_user["user_id"], workspace_id,
        required_role=required_role, system_role=current_user.get("role"),
    )
    return workspace_id


def _filters(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    start_time: Optional[str] = Query(None, description="Time-of-day lower bound (HH:MM)"),
    end_time: Optional[str] = Query(None, description="Time-of-day upper bound (HH:MM)"),
    location: Optional[str] = Query(None, description="Filter by camera location"),
    building: Optional[str] = Query(None, description="Filter by building"),
    floor_level: Optional[str] = Query(None, description="Filter by floor level"),
    zone: Optional[str] = Query(None, description="Filter by the camera's location zone (not a no-entry zone)"),
):
    return dict(
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        location=location, building=building,
        floor_level=floor_level, zone=zone,
    )


async def _refresh_engine(stream_id) -> None:
    """Push a camera's zones into the in-process engine (applied on its next frame).
    Streams in other processes pick the change up on their periodic zone refresh."""
    from app.services.no_entry_zone_inference import no_entry_zone_engine
    zones = await no_entry_zone_service.list_zones_for_stream(stream_id)
    no_entry_zone_engine.set_zones(str(stream_id), zones)


# ─────────────────────────────────────────────
# Overview / cameras / permissions
# ─────────────────────────────────────────────

@router.get("/overview", response_model=OverviewResponse)
async def get_overview(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    try:
        return await no_entry_zone_service.overview(workspace_id)
    except Exception as e:
        logger.error(f"no-entry-zone overview error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving no-entry-zone overview.")


@router.get("/permissions")
async def get_permissions(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
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

    return {"can_resolve": await allowed("member"), "can_manage_zones": await allowed("admin")}


HEALTHY_FRAME_AGE_S = 30  # same rule as StreamManager's health check


def _redact_error(message: Optional[str], source: Optional[str]) -> Optional[str]:
    if not message:
        return None
    if source:
        message = message.replace(source, rtsp_probe.redact_url(source))
    return message[:300]


async def _with_health(rows: List[Dict]) -> List[Dict]:
    """Merge this server's in-memory stream state into camera rows and drop ``path``.

    Streams are processed by whichever server holds their lock; a stream running on
    another server is reported as such rather than as stopped.
    """
    from app.services.stream_service import stream_manager
    from app.services.no_entry_zone_inference import no_entry_zone_engine

    async with stream_manager._lock:
        memory = {
            sid: {k: info.get(k) for k in ("status", "last_frame_time", "source")}
            for sid, info in stream_manager.active_streams.items()
        }
    shared_streams = getattr(stream_manager.video_file_manager, "shared_streams", {}) or {}
    now = datetime.now(timezone.utc)
    now_epoch = now.timestamp()

    out = []
    for row in rows:
        cam = dict(row)
        path = cam.pop("path", None)
        locked_by = cam.pop("locked_by_server", None)
        cam["source_display"] = rtsp_probe.redact_url(path) if path else None
        sid = str(cam["stream_id"])
        mem = memory.get(sid)
        health: Dict = {"live_status": "stopped"}
        if mem:
            last = mem.get("last_frame_time")
            health.update(
                live_status=mem.get("status") or "unknown",
                last_frame_at=last,
                healthy=bool(last and (now - last).total_seconds() < HEALTHY_FRAME_AGE_S),
            )
            shared = shared_streams.get(mem.get("source") or path)
            if shared is not None:
                m = shared.metrics
                uptime = now_epoch - m.uptime_start if m.uptime_start else None
                frame = getattr(shared, "latest_frame", None)
                health.update(
                    capture_state=getattr(shared.state, "value", str(shared.state)),
                    total_frames=m.total_frames,
                    uptime_s=round(uptime, 1) if uptime else None,
                    avg_fps=round(m.total_frames / uptime, 2) if uptime and uptime > 1 else None,
                    connection_attempts=m.connection_attempts,
                    consecutive_errors=m.consecutive_errors,
                    last_error=_redact_error(m.last_error, path),
                    resolution=f"{frame.shape[1]}x{frame.shape[0]}" if frame is not None else None,
                )
            health["zones_loaded"] = no_entry_zone_engine.zones_loaded(sid)
        elif cam.get("is_streaming") and locked_by:
            health["running_elsewhere"] = True
            health["live_status"] = "running_on_other_server"
        cam["health"] = health
        out.append(cam)
    return out


async def _camera_or_404(stream_id: UUID, workspace_id: UUID) -> Dict:
    rows = await no_entry_zone_service.list_cameras(workspace_id, enabled_only=False, stream_id=stream_id)
    if not rows:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace")
    return rows[0]


@router.get("/cameras", response_model=List[NoEntryCamera])
async def list_cameras(
    enabled_only: bool = Query(True, description="Only cameras with no-entry-zone detection enabled"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Cameras with their zone counts and live stream health (source URLs redacted)."""
    workspace_id = await _workspace_with_role(current_user, None)
    rows = await no_entry_zone_service.list_cameras(workspace_id, enabled_only=enabled_only)
    return await _with_health(rows)


@router.post("/cameras/probe-all", response_model=List[ProbeResult])
async def probe_all_cameras(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Open every no-entry-zone camera once and report whether a frame could be read.
    Probes run a few at a time; a stream already running keeps running."""
    workspace_id = await _workspace_with_role(current_user, None)
    rows = await no_entry_zone_service.list_cameras(workspace_id, enabled_only=True)

    async def one(row):
        info = await rtsp_probe.probe(row["path"]) if row.get("path") else {"reachable": False, "error": "no source"}
        return {"stream_id": row["stream_id"], "name": row["name"], **info}

    return await asyncio.gather(*(one(r) for r in rows))


@router.get("/cameras/{stream_id}", response_model=NoEntryCameraDetail)
async def get_camera(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    row = await _camera_or_404(stream_id, workspace_id)
    cam = (await _with_health([row]))[0]
    cam["zones"] = await no_entry_zone_service.list_zones(workspace_id, stream_id=stream_id)
    return cam


@router.post("/cameras/{stream_id}/probe", response_model=ProbeResult)
async def probe_camera(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    row = await _camera_or_404(stream_id, workspace_id)
    info = await rtsp_probe.probe(row["path"]) if row.get("path") else {"reachable": False, "error": "no source"}
    return {"stream_id": row["stream_id"], "name": row["name"], **info}


@router.get("/cameras/{stream_id}/snapshot")
async def camera_snapshot(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """A still to draw zones on. The live (un-annotated) frame when the stream is being
    processed here, otherwise a frame grabbed directly from the camera."""
    from app.services.stream_service import stream_manager

    workspace_id = await _workspace_with_role(current_user, None)
    row = await _camera_or_404(stream_id, workspace_id)
    frame, source = None, "grab"
    async with stream_manager._lock:
        mem = stream_manager.active_streams.get(str(stream_id)) or {}
        mem_source = mem.get("source")
    shared = (getattr(stream_manager.video_file_manager, "shared_streams", {}) or {}).get(mem_source or row.get("path"))
    if shared is not None and getattr(shared, "latest_frame", None) is not None:
        frame, source = shared.latest_frame.copy(), "live"
    if frame is None:
        if not row.get("path"):
            raise HTTPException(status_code=502, detail="Camera has no source configured")
        frame, info = await rtsp_probe.snapshot(row["path"])
        if frame is None:
            raise HTTPException(status_code=502, detail=f"Camera unreachable: {info.get('error') or 'no frame'}")
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
    if not ok:
        raise HTTPException(status_code=500, detail="Could not encode the frame")
    return Response(
        content=buf.tobytes(),
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-store",
            "X-Frame-Source": source,
            "X-Frame-Size": f"{frame.shape[1]}x{frame.shape[0]}",
            "Access-Control-Expose-Headers": "X-Frame-Source, X-Frame-Size",
        },
    )


# ─────────────────────────────────────────────
# Incidents
# ─────────────────────────────────────────────

@router.get("/incidents", response_model=NoEntryIncidentListResponse)
async def list_incidents(
    status_filter: Optional[EventStatus] = Query(None, description="detected | acknowledged | resolved"),
    camera_id: Optional[UUID] = Query(None, description="Filter by stream/camera UUID"),
    zone_id: Optional[UUID] = Query(None, description="Filter by zone UUID"),
    target_class: Optional[str] = Query(None, description="Filter by object class"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    offset = (page - 1) * limit
    try:
        kw = dict(camera_id=camera_id, zone_id=zone_id, target_class=target_class, **f)
        items = await no_entry_zone_service.list_incidents(
            workspace_id, status=status_filter, limit=limit, offset=offset, **kw,
        )
        total = await no_entry_zone_service.count_incidents(workspace_id, status=status_filter, **kw)
        return NoEntryIncidentListResponse(items=items, total=total, limit=limit, offset=offset, page=page)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"list_incidents error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving no-entry-zone incidents.")


@router.post("/incidents/{incident_id}/resolve", response_model=ResolveIncidentResponse)
async def resolve_incident(
    incident_id: UUID,
    request: ResolveNoEntryRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "member")
    updated = await no_entry_zone_service.resolve_incident(
        incident_id=incident_id, workspace_id=workspace_id, status=request.status,
        description=request.description, user_id=current_user["user_id"],
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return ResolveIncidentResponse(
        message=f"Incident {incident_id} updated to '{request.status}'", updated_events=updated,
    )


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

@router.get("/events", response_model=NoEntryEventListResponse)
async def get_no_entry_events(
    status_filter: Optional[EventStatus] = Query(None, description="detected | acknowledged | resolved"),
    camera_id: Optional[UUID] = Query(None, description="Filter by stream/camera UUID"),
    zone_id: Optional[UUID] = Query(None, description="Filter by zone UUID"),
    incident_id: Optional[UUID] = Query(None, description="Only the events of one incident"),
    target_class: Optional[str] = Query(None, description="Filter by object class"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    offset = (page - 1) * limit
    try:
        kw = dict(
            status=status_filter, camera_id=camera_id, zone_id=zone_id,
            incident_id=incident_id, target_class=target_class, **f,
        )
        events = await no_entry_zone_service.get_events(workspace_id, limit=limit, offset=offset, **kw)
        total = await no_entry_zone_service.count_events(workspace_id, **kw)
        return NoEntryEventListResponse(items=events, total=total, limit=limit, offset=offset, page=page)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"get_no_entry_events error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving no-entry-zone events.")


@router.get("/events/{event_id}", response_model=NoEntryEventResponse)
async def get_no_entry_event(
    event_id: int,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    event = await no_entry_zone_service.get_event(event_id, workspace_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return event


@router.get("/events/{event_id}/evidence", response_model=EvidenceUrls)
async def get_no_entry_event_evidence(
    event_id: int,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Presigned URLs, so <img>/<video> can load the evidence without a bearer header."""
    workspace_id = await _workspace_with_role(current_user, None)
    event = await no_entry_zone_service.get_event(event_id, workspace_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    from app.services.s3_service import s3_service

    async def sign(path: Optional[str]) -> Optional[str]:
        if not path:
            return None
        return await s3_service.get_presigned_url(path, expiration=EVIDENCE_URL_TTL)

    return EvidenceUrls(
        snapshot_url=await sign(event.get("snapshot_path")),
        clip_url=await sign(event.get("clip_path")),
        expires_in=EVIDENCE_URL_TTL,
    )


@router.post("/events/{event_id}/resolve")
async def resolve_no_entry_event(
    event_id: int,
    request: ResolveNoEntryRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "member")
    success = await no_entry_zone_service.resolve_event(
        event_id=event_id, workspace_id=workspace_id, status=request.status,
        description=request.description, user_id=current_user["user_id"],
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return {"message": f"Event {event_id} updated to '{request.status}'"}


# ─────────────────────────────────────────────
# Analytics / dashboard
# ─────────────────────────────────────────────

@router.get("/analytics/summary", response_model=AnalyticsSummary)
async def get_analytics_summary(
    range_hours: int = Query(24, ge=1, le=24 * 90, description="Window ending now, in hours"),
    bucket: Optional[Literal["hour", "day"]] = Query(None, description="Defaults to hour for <=48h, else day"),
    camera_id: Optional[UUID] = Query(None),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    until = datetime.now(timezone.utc)
    since = until - timedelta(hours=range_hours)
    try:
        return await no_entry_zone_service.analytics_summary(
            workspace_id, since=since, until=until,
            bucket=bucket or ("hour" if range_hours <= 48 else "day"), stream_id=camera_id,
        )
    except Exception as e:
        logger.error(f"no-entry-zone analytics error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving no-entry-zone analytics.")


@router.get("/dashboard/active", response_model=List[ActiveNoEntryEventSummary])
async def get_active_no_entry_dashboard(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await _workspace_with_role(current_user, None)
        return await no_entry_zone_service.get_active_dashboard(workspace_id=workspace_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_active_no_entry_dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving active dashboard.")


@router.get("/dashboard/summary", response_model=List[NoEntryDailySummary])
async def get_no_entry_daily_summary(
    limit: int = Query(30, ge=1, le=365, description="Days to return"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await _workspace_with_role(current_user, None)
        return await no_entry_zone_service.get_daily_summary(workspace_id=workspace_id, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_no_entry_daily_summary error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving daily summary.")


# ─────────────────────────────────────────────
# Zone CRUD
# ─────────────────────────────────────────────

@router.get("/zones", response_model=List[ZoneResponse])
async def list_zones(
    stream_id: Optional[UUID] = Query(None, description="Filter by camera UUID"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    return await no_entry_zone_service.list_zones(workspace_id, stream_id=stream_id)


@router.get("/zones/{zone_id}", response_model=ZoneResponse)
async def get_zone(
    zone_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, None)
    zone = await no_entry_zone_service.get_zone(zone_id, workspace_id)
    if not zone:
        raise HTTPException(status_code=404, detail="Zone not found")
    return zone


@router.post("/zones", response_model=ZoneResponse, status_code=201)
async def create_zone(
    request: ZoneCreateRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "admin")
    if not await no_entry_zone_service.stream_in_workspace(request.stream_id, workspace_id):
        raise HTTPException(status_code=404, detail="Camera not found in this workspace")
    try:
        zone = await no_entry_zone_service.create_zone(workspace_id, request.model_dump())
    except HTTPException as e:
        # db_manager maps unique violations to 409; the only unique key here is (camera, name).
        if e.status_code == 409:
            raise HTTPException(status_code=409, detail="A zone with this name already exists on this camera")
        raise
    except Exception as e:
        logger.error(f"create_zone error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error creating zone.")
    try:
        await _refresh_engine(request.stream_id)
    except Exception as e:
        logger.warning(f"Failed to hot-reload zones for {request.stream_id}: {e}")
    return zone


@router.patch("/zones/{zone_id}", response_model=ZoneResponse)
async def update_zone(
    zone_id: UUID,
    request: ZoneUpdateRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "admin")
    try:
        updated = await no_entry_zone_service.update_zone(
            zone_id, workspace_id, request.model_dump(exclude_unset=True)
        )
    except HTTPException as e:
        # db_manager maps unique violations to 409; the only unique key here is (camera, name).
        if e.status_code == 409:
            raise HTTPException(status_code=409, detail="A zone with this name already exists on this camera")
        raise
    except Exception as e:
        logger.error(f"update_zone error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error updating zone.")
    if not updated:
        raise HTTPException(status_code=404, detail="Zone not found")
    try:
        await _refresh_engine(updated["stream_id"])
    except Exception as e:
        logger.warning(f"Failed to hot-reload zones for {updated['stream_id']}: {e}")
    return updated


@router.delete("/zones/{zone_id}")
async def delete_zone(
    zone_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "admin")
    stream_id = await no_entry_zone_service.delete_zone(zone_id, workspace_id)
    if not stream_id:
        raise HTTPException(status_code=404, detail="Zone not found")
    try:
        await _refresh_engine(stream_id)
    except Exception as e:
        logger.warning(f"Failed to hot-reload zones for {stream_id}: {e}")
    return {"message": f"Zone {zone_id} deleted"}


# ─────────────────────────────────────────────
# Data Management – delete events
# ─────────────────────────────────────────────

@router.delete("/workspace/delete_data")
async def delete_no_entry_data(
    camera_id: Optional[UUID] = Query(None, description="Filter by camera UUID"),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await _workspace_with_role(current_user, "admin")
    try:
        return await no_entry_zone_service.delete_events(workspace_id=workspace_id, camera_id=camera_id, **f)
    except Exception as e:
        logger.error(f"delete_no_entry_data error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error deleting no-entry-zone data.")


@router.delete("/workspace/delete_all_data")
async def delete_all_no_entry_data(
    confirm: bool = Query(False, description="Must be true to confirm deletion of all events"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set 'confirm=true' to delete all no-entry-zone data.",
        )
    workspace_id = await _workspace_with_role(current_user, "admin")
    try:
        return await no_entry_zone_service.delete_all_events(workspace_id=workspace_id)
    except Exception as e:
        logger.error(f"delete_all_no_entry_data error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error deleting all no-entry-zone data.")
