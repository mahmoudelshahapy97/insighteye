# app/routes/blocked_exit_router.py
"""
Blocked Exit Detection API
===========================
Monitors emergency exits for obstructions and reports exit accessibility.

/events                          - list / filter events
/events/{event_id}/resolve       - acknowledge / resolve an event
/dashboard/active                - live unresolved events
/dashboard/summary               - daily rollup
/cameras/{stream_id}/door-polygon - set/update door polygon calibration
/cameras/{stream_id}/config      - read current zone config
/workspace/delete_data           - filtered delete
/workspace/delete_all_data       - delete all (confirm required)
"""

import logging
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any, List, Optional

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.blocked_exit_service import blocked_exit_service
from app.services.user_service import user_manager
from app.services.database import db_manager
from app.schemas.blocked_exit_schema import (
    BlockedExitEventResponse,
    BlockedExitEventListResponse,
    ResolveBlockedExitRequest,
    BlockedExitDailySummary,
    ActiveBlockedExitEventSummary,
    ExitZoneConfigRequest,
    ExitZoneConfigResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/blocked-exit", tags=["Blocked Exit"])


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
    status_filter: Optional[str] = Query(None, description="detected | acknowledged | resolved"),
    camera_id: Optional[str] = Query(None, description="Filter by stream/camera UUID"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    """Paginated list of blocked-exit events."""
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        offset = (page - 1) * limit

        events = await blocked_exit_service.get_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id,
            limit=limit, offset=offset, **f,
        )
        total = await blocked_exit_service.count_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id, **f,
        )
        return BlockedExitEventListResponse(items=events, total=total, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_blocked_exit_events error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving blocked-exit events.")


@router.post("/events/{event_id}/resolve")
async def resolve_blocked_exit_event(
    event_id: int,
    request: ResolveBlockedExitRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    await get_workspace_id_for_user(current_user["username"])
    success = await blocked_exit_service.resolve_event(
        event_id=event_id, status=request.status, description=request.description,
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return {"message": f"Event {event_id} updated to '{request.status}'"}


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
        row = await blocked_exit_service.set_door_polygon(
            stream_id=stream_id,
            workspace_id=workspace_id,
            door_polygon=request.door_polygon,
            calibration_frame_w=request.calibration_frame_w,
            calibration_frame_h=request.calibration_frame_h,
            detector_strategy=request.detector_strategy,
            min_accessibility_pct=request.min_accessibility_pct,
            debounce_seconds=request.debounce_seconds,
        )
        try:
            from app.services.blocked_exit_inference import blocked_exit_engine
            blocked_exit_engine.set_zone_config(str(stream_id), row)
        except Exception as e:
            logger.warning(f"Failed to hot-reload door polygon for {stream_id}: {e}")
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
    await get_workspace_id_for_user(current_user["username"])
    return await blocked_exit_service.get_zone_config(stream_id)


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
