# app/api/routes/lost_found_router.py
"""
Lost & Found API
================
Abandoned-object detection, the found-items gallery, lost-item reports and matching.

/cameras                          - workspace cameras with their lost & found config
/cameras/{stream_id}/polygon      - get / replace / clear the region of interest (shared polygon-editor shape)
/cameras/{stream_id}/settings     - abandonment rule overrides (+ server defaults)
/cameras/{stream_id}/snapshot     - JPEG still for drawing the ROI
/cameras/{stream_id}/live         - latest annotated frame (tracked objects coloured by status)
/cameras/{stream_id}/status       - tracked objects and their abandonment status
/events                           - abandoned events (IncidentVideosTab contract)
/events/stats                     - counters for the tab header
/events/{event_id}                - one event with presigned media
/events/{event_id}/resolve        - acknowledge / resolve / false positive
/found-items                      - the found gallery: camera, date/time, type, colour, status filters
/found-items/{found_id}           - one item; PATCH sets its status
/lost-items                       - file (multipart, photo and/or description) / list lost reports
/lost-items/{lost_id}             - one report; PATCH sets its status
/lost-items/{lost_id}/matches     - ranked found-item candidates (rule-based)
/matches/confirm, /matches        - confirm / reject a pairing
/analytics/summary                - KPIs, trend, heatmap, by camera / class, funnel
/analytics/incidents-per-camera   - shared chart: events per camera
/analytics/confidence-audit       - shared chart: model confidence audit

The ROI and settings can only be changed on cameras with Lost & Found enabled.
"""

import asyncio
import logging
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Response, UploadFile

from app.config.settings import config
from app.schemas.evidence_schema import CameraPolygonRequest
from app.schemas.lost_found_schema import (
    ConfirmMatchRequest,
    EventStatus,
    FoundStatus,
    FoundStatusRequest,
    LostStatus,
    LostStatusRequest,
    LostFoundSettingsRequest,
    ResolveLostFoundEventRequest,
)
from app.services import camera_runtime
from app.services.database import db_manager
from app.services.feature_service import require_feature
from app.services.lost_found.abandonment import AbandonmentParams
from app.services.lost_found.imaging import decode_jpeg
from app.services.lost_found_service import lost_found_service
from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.utils.permission_utils import check_workspace_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lost-found", tags=["Lost & Found"],
                   dependencies=[Depends(require_feature("lost_found"))])

EVIDENCE_URL_TTL = 900
_UPLOAD_TYPES = {"image/jpeg", "image/png", "image/webp", "image/bmp"}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def get_workspace_id_for_user(username: str) -> UUID:
    """Return the active workspace UUID for the given user."""
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")
    return workspace_id_obj


async def _require_camera(stream_id: UUID, workspace_id: UUID) -> Dict[str, Any]:
    """404 unless the camera belongs to the caller's workspace."""
    camera = await lost_found_service.get_camera(workspace_id, stream_id)
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    return camera


async def _require_enabled_camera(stream_id: UUID, workspace_id: UUID) -> Dict[str, Any]:
    """404 outside the workspace; 409 when Lost & Found is not enabled on the camera."""
    camera = await _require_camera(stream_id, workspace_id)
    if not camera.get("is_lost_found_camera"):
        raise HTTPException(status_code=409, detail="Lost & Found is not enabled on this camera.")
    return camera


async def _require_role(current_user: Dict, workspace_id: UUID, role: str) -> None:
    await check_workspace_access(
        db_manager, current_user["user_id"], workspace_id,
        required_role=role, system_role=current_user.get("role"),
    )


async def _sign(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    from app.services.s3_service import s3_service
    return await s3_service.get_presigned_url(path, expiration=EVIDENCE_URL_TTL)


async def _signed(row: Dict[str, Any], *columns: str) -> Dict[str, Any]:
    """``row`` with each ``*_path`` media column replaced by a presigned URL."""
    row = dict(row)
    for column in columns:
        row[column] = await _sign(row.get(column))
    return row


def _hot_reload(stream_id: UUID, cfg: Optional[Dict[str, Any]]) -> None:
    try:
        from app.services.lost_found_inference import lost_found_engine
        lost_found_engine.reload_config(str(stream_id), cfg)
    except Exception as e:
        logger.warning(f"Failed to hot-reload lost & found config for {stream_id}: {e}")


def _to_json_safe(obj):
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items() if not isinstance(v, (bytes, bytearray, memoryview))}
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
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    start_time: Optional[str] = Query(None, description="Time-of-day lower bound (HH:MM)"),
    end_time: Optional[str] = Query(None, description="Time-of-day upper bound (HH:MM)"),
    start: Optional[datetime] = Query(None, description="Exact ISO-8601 lower bound"),
    end: Optional[datetime] = Query(None, description="Exact ISO-8601 upper bound"),
    location: Optional[str] = Query(None, description="Camera location (comma-separated for several)"),
    area: Optional[str] = Query(None, description="Camera area (comma-separated for several)"),
    building: Optional[str] = Query(None, description="Building (comma-separated for several)"),
    floor_level: Optional[str] = Query(None, description="Floor level (comma-separated for several)"),
    zone: Optional[str] = Query(None, description="Zone (comma-separated for several)"),
):
    return dict(
        start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time,
        start=start, end=end, location=location, area=area, building=building,
        floor_level=floor_level, zone=zone,
    )


def _polygon_response(stream_id: UUID, camera_name: Optional[str], cfg: Optional[Dict[str, Any]]) -> Dict:
    roi = (cfg or {}).get("roi")
    return {
        "stream_id": stream_id, "camera_name": camera_name, "id": stream_id,
        "polygon": roi or None,
        "ref_width": (cfg or {}).get("ref_width") if roi else None,
        "ref_height": (cfg or {}).get("ref_height") if roi else None,
        "point_count": len(roi or []),
        "is_active": (cfg or {}).get("is_active"),
        "updated_at": (cfg or {}).get("updated_at"),
    }


def _settings_response(cfg: Optional[Dict[str, Any]]) -> Dict:
    return {
        "settings": (cfg or {}).get("settings") or {},
        "is_active": (cfg or {}).get("is_active", True),
        "defaults": asdict(AbandonmentParams()),
        "updated_at": (cfg or {}).get("updated_at"),
    }


# ─────────────────────────────────────────────
# Cameras & configuration
# ─────────────────────────────────────────────

@router.get("/cameras")
async def list_cameras(
    enabled_only: bool = Query(True, description="Only cameras with Lost & Found enabled"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return _to_json_safe(await lost_found_service.list_cameras(workspace_id, enabled_only=enabled_only))


@router.get("/cameras/{stream_id}/polygon")
async def get_camera_polygon(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """The camera's region of interest in the shared editor shape; ``polygon`` is null
    when detection covers the whole frame."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    camera = await _require_camera(stream_id, workspace_id)
    cfg = await lost_found_service.get_config(stream_id, workspace_id)
    return _to_json_safe(_polygon_response(stream_id, camera.get("name"), cfg))


@router.put("/cameras/{stream_id}/polygon")
async def set_camera_polygon(
    stream_id: UUID,
    request: CameraPolygonRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Replace the region of interest (settings are kept). Workspace admins only, and
    only on cameras with Lost & Found enabled."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_role(current_user, workspace_id, "admin")
    camera = await _require_enabled_camera(stream_id, workspace_id)
    row = await lost_found_service.upsert_config(
        stream_id=stream_id, workspace_id=workspace_id, roi=request.polygon,
        ref_width=request.ref_width, ref_height=request.ref_height, settings=None,
        is_active=request.is_active, replace_roi=True, replace_settings=False,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    _hot_reload(stream_id, row)
    return _to_json_safe(_polygon_response(stream_id, camera.get("name"), row))


@router.delete("/cameras/{stream_id}/polygon")
async def clear_camera_polygon(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Remove the region of interest: detection covers the whole frame again."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_role(current_user, workspace_id, "admin")
    camera = await _require_enabled_camera(stream_id, workspace_id)
    row = await lost_found_service.upsert_config(
        stream_id=stream_id, workspace_id=workspace_id, roi=None, ref_width=None, ref_height=None,
        settings=None, is_active=None, replace_roi=True, replace_settings=False,
    )
    _hot_reload(stream_id, row)
    return _to_json_safe(_polygon_response(stream_id, camera.get("name"), row))


@router.get("/cameras/{stream_id}/settings")
async def get_camera_settings(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """The camera's abandonment rule overrides, plus the server defaults they replace."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    return _to_json_safe(_settings_response(await lost_found_service.get_config(stream_id, workspace_id)))


@router.put("/cameras/{stream_id}/settings")
async def set_camera_settings(
    stream_id: UUID,
    request: LostFoundSettingsRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Replace the camera's rule overrides (the ROI is kept). Workspace admins only."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_role(current_user, workspace_id, "admin")
    await _require_enabled_camera(stream_id, workspace_id)
    row = await lost_found_service.upsert_config(
        stream_id=stream_id, workspace_id=workspace_id, roi=None, ref_width=None, ref_height=None,
        settings=request.settings, is_active=request.is_active, replace_roi=False, replace_settings=True,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    _hot_reload(stream_id, row)
    return _to_json_safe(_settings_response(row))


@router.get("/cameras/{stream_id}/snapshot", response_class=Response)
async def get_camera_snapshot(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """A JPEG still: the running stream's latest frame, or a one-off grab from the
    camera when it isn't streaming (so an ROI can be drawn on a stopped camera)."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    source = await lost_found_service.get_camera_source(stream_id, workspace_id)
    if not source:
        raise HTTPException(status_code=404, detail="Camera not found in this workspace.")
    if not source.get("path"):
        raise HTTPException(status_code=502, detail="Camera has no source configured")
    jpeg, info = await camera_runtime.live_or_grab_jpeg(source["path"])
    if jpeg is None:
        raise HTTPException(status_code=502, detail=f"Camera unreachable: {info['error']}")
    # Same headers as the blocked-exit / no-entry-zone snapshot, so the UI shares one editor.
    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-store",
            "X-Frame-Source": info["frame_source"],
            "X-Frame-Size": f"{info['width']}x{info['height']}",
            "X-Frame-Width": str(info["width"]),
            "X-Frame-Height": str(info["height"]),
            "Access-Control-Expose-Headers": "X-Frame-Source, X-Frame-Size, X-Frame-Width, X-Frame-Height",
        },
    )


@router.get("/cameras/{stream_id}/live", response_class=Response)
async def get_camera_live(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """The latest annotated frame: every tracked object coloured by status (moving,
    attended, unattended, abandoned). 404 unless detection runs on this server."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    await _require_camera(stream_id, workspace_id)
    from app.services.lost_found_inference import lost_found_engine
    jpeg = await asyncio.to_thread(lost_found_engine.live_jpeg, str(stream_id))
    if jpeg is None:
        raise HTTPException(status_code=404, detail="Lost & Found is not running on this camera.")
    return Response(content=jpeg, media_type="image/jpeg", headers={"Cache-Control": "no-store"})


@router.get("/cameras/{stream_id}/status")
async def get_camera_status(
    stream_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Tracked objects and their abandonment status (``running`` false when detection
    does not run on this server)."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    camera = await _require_camera(stream_id, workspace_id)
    from app.services.lost_found_inference import lost_found_engine
    objects = lost_found_engine.tracked_objects(str(stream_id))
    return _to_json_safe({
        "stream_id": stream_id,
        "camera_name": camera.get("name"),
        "is_streaming": camera.get("is_streaming"),
        "is_lost_found_camera": camera.get("is_lost_found_camera"),
        "running": objects is not None,
        "tracked_objects": objects or [],
    })


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

def _event_out(event: Dict[str, Any]) -> Dict[str, Any]:
    """Field names IncidentVideosTab reads (``dwell_seconds`` is the unattended time)."""
    unattended = event.get("unattended_seconds")
    return {**event, "dwell_seconds": round(unattended) if unattended is not None else None}


@router.get("/events")
async def list_events(
    status_filter: Optional[EventStatus] = Query(None, description="open | acknowledged | resolved | false_positive"),
    camera_id: Optional[UUID] = Query(None, description="Filter by stream/camera UUID"),
    target_class: Optional[str] = Query(None, description="Object type, e.g. bag"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Paginated abandoned events, newest first, with presigned snapshot and clip URLs."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    kw = dict(status=status_filter, camera_id=str(camera_id) if camera_id else None, class_name=target_class, **f)
    try:
        events = await lost_found_service.get_events(workspace_id, limit=limit, offset=(page - 1) * limit, **kw)
        total = await lost_found_service.count_events(workspace_id, **kw)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    items = [_event_out(await _signed(ev, "snapshot_path", "video_path", "crop_path")) for ev in events]
    return _to_json_safe({"items": items, "total": total, "limit": limit, "offset": (page - 1) * limit,
                          "page": page, "expires_in": EVIDENCE_URL_TTL})


@router.get("/events/stats")
async def event_stats(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return _to_json_safe(await lost_found_service.event_stats(workspace_id))


@router.get("/events/{event_id}")
async def get_event(
    event_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    event = await lost_found_service.get_event(workspace_id, event_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return _to_json_safe(_event_out(await _signed(event, "snapshot_path", "video_path", "crop_path")))


@router.post("/events/{event_id}/resolve")
async def resolve_event(
    event_id: UUID,
    request: ResolveLostFoundEventRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Acknowledge / resolve / mark a false positive (which also discards the found item)."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    ok = await lost_found_service.resolve_event(
        workspace_id=workspace_id, event_id=event_id, status=request.status,
        action_taken=request.action_taken, description=request.description,
        user_id=current_user.get("user_id"),
    )
    if not ok:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return {"message": f"Event {event_id} updated to '{request.status}'", "event_id": str(event_id),
            "status": request.status}


# ─────────────────────────────────────────────
# Found items
# ─────────────────────────────────────────────

@router.get("/found-items")
async def list_found_items(
    status: Optional[FoundStatus] = Query(None),
    camera_id: Optional[UUID] = Query(None),
    class_name: Optional[str] = Query(None),
    color: Optional[str] = Query(None),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    per_page: int = Query(24, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """The found gallery and its search: every filter is optional."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    try:
        result = await lost_found_service.list_found_items(
            workspace_id, page=page, per_page=per_page, status=status,
            camera_id=str(camera_id) if camera_id else None, class_name=class_name, color=color, **f,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    result["items"] = [await _signed(r, "crop_path", "snapshot_path", "video_path") for r in result["items"]]
    return _to_json_safe(result)


@router.get("/found-items/{found_id}")
async def get_found_item(
    found_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    item = await lost_found_service.get_found_item(workspace_id, found_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Found item {found_id} not found")
    return _to_json_safe(await _signed(item, "crop_path", "snapshot_path", "video_path"))


@router.patch("/found-items/{found_id}")
async def set_found_item_status(
    found_id: UUID,
    request: FoundStatusRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    if not await lost_found_service.set_found_status(workspace_id, found_id, request.status):
        raise HTTPException(status_code=404, detail=f"Found item {found_id} not found")
    return {"found_id": str(found_id), "status": request.status}


# ─────────────────────────────────────────────
# Lost reports & matching
# ─────────────────────────────────────────────

@router.post("/lost-items", status_code=201)
async def report_lost_item(
    photo: Optional[UploadFile] = File(None),
    description: Optional[str] = Form(None),
    reporter_name: Optional[str] = Form(None),
    reporter_email: Optional[str] = Form(None),
    class_name: Optional[str] = Form(None),
    lost_after: Optional[datetime] = Form(None),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """File a lost-item report from a photo and/or a description."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    image = None
    if photo is not None and photo.filename:
        if photo.content_type not in _UPLOAD_TYPES:
            raise HTTPException(status_code=415, detail="Photo must be a JPEG, PNG, WebP or BMP image.")
        data = await photo.read(int(config.lost_found_max_upload_bytes) + 1)
        if len(data) > int(config.lost_found_max_upload_bytes):
            raise HTTPException(status_code=413, detail="Photo is too large.")
        image = await asyncio.to_thread(decode_jpeg, data)
        if image is None:
            raise HTTPException(status_code=422, detail="Photo could not be decoded.")
    description = (description or "").strip() or None
    if image is None and not description:
        raise HTTPException(status_code=422, detail="Provide a photo or a description.")
    report = await lost_found_service.create_lost_item(
        workspace_id, image=image, description=description,
        reporter_name=(reporter_name or "").strip() or None,
        reporter_email=(reporter_email or "").strip() or None,
        class_name=(class_name or "").strip() or None, lost_after=lost_after,
        reported_by=current_user.get("user_id"),
    )
    if not report:
        raise HTTPException(status_code=500, detail="Could not save the report.")
    return _to_json_safe(await _signed(report, "photo_path"))


@router.get("/lost-items")
async def list_lost_items(
    status: Optional[LostStatus] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    result = await lost_found_service.list_lost_items(workspace_id, status=status, page=page, per_page=per_page)
    result["items"] = [await _signed(r, "photo_path") for r in result["items"]]
    return _to_json_safe(result)


@router.get("/lost-items/{lost_id}")
async def get_lost_item(
    lost_id: UUID,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    report = await lost_found_service.get_lost_item(workspace_id, lost_id)
    if not report:
        raise HTTPException(status_code=404, detail=f"Lost report {lost_id} not found")
    return _to_json_safe(await _signed(report, "photo_path"))


@router.patch("/lost-items/{lost_id}")
async def set_lost_item_status(
    lost_id: UUID,
    request: LostStatusRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    if not await lost_found_service.set_lost_status(workspace_id, lost_id, request.status):
        raise HTTPException(status_code=404, detail=f"Lost report {lost_id} not found")
    return {"lost_id": str(lost_id), "status": request.status}


@router.get("/lost-items/{lost_id}/matches")
async def get_matches(
    lost_id: UUID,
    top_k: int = Query(10, ge=1, le=50),
    min_score: Optional[float] = Query(None, ge=0.0, le=1.0),
    persist: bool = Query(True, description="Keep the shortlist as match suggestions"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Unclaimed found items ranked against the report: same type, compatible colour,
    found soon after the loss, and (with a photo) visual likeness."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    matches = await lost_found_service.find_matches(
        workspace_id, lost_id, top_k=top_k, min_score=min_score, persist=persist,
    )
    if matches is None:
        raise HTTPException(status_code=404, detail=f"Lost report {lost_id} not found")
    matches = [await _signed(m, "crop_path") for m in matches]
    return _to_json_safe({"lost_id": lost_id, "count": len(matches), "matches": matches})


@router.post("/matches/confirm")
async def confirm_match(
    request: ConfirmMatchRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Confirm a pairing: the report is resolved, the item claimed, its event resolved."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    match = await lost_found_service.confirm_match(
        workspace_id, request.lost_id, request.found_id, request.score, user_id=current_user.get("user_id"),
    )
    if not match:
        raise HTTPException(status_code=404, detail="Lost report or found item not found in this workspace.")
    return _to_json_safe(match)


@router.delete("/matches")
async def reject_match(
    lost_id: UUID = Query(...),
    found_id: UUID = Query(...),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    if not await lost_found_service.reject_match(workspace_id, lost_id, found_id):
        raise HTTPException(status_code=404, detail="No such match suggestion.")
    return {"message": "Match suggestion removed."}


# ─────────────────────────────────────────────
# Analytics
# ─────────────────────────────────────────────

@router.get("/analytics/summary")
async def analytics_summary(
    days: int = Query(7, ge=1, le=90, description="Window length; 1 = last 24 hours, hourly"),
    camera_id: Optional[UUID] = Query(None),
    tz: str = Query("Africa/Cairo", max_length=64, description="IANA timezone for the buckets"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    try:
        return _to_json_safe(await lost_found_service.summary(workspace_id, days=days, camera_id=camera_id, tz=tz))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.get("/analytics/incidents-per-camera")
async def analytics_incidents_per_camera(
    camera_id: Optional[UUID] = Query(None),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Shared chart: abandoned events per camera."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    try:
        return _to_json_safe(await lost_found_service.incidents_per_camera(
            workspace_id, camera_id=str(camera_id) if camera_id else None, **f,
        ))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.get("/analytics/confidence-audit")
async def analytics_confidence_audit(
    camera_id: Optional[UUID] = Query(None),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Shared chart: model accuracy & confidence audit of the abandoned-object detections."""
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    try:
        return _to_json_safe(await lost_found_service.confidence_audit(
            workspace_id, camera_id=str(camera_id) if camera_id else None, **f,
        ))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
