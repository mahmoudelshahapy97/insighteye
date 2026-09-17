# app/routes/no_entry_zone_router.py
"""
No-Entry Zone Detection API
=============================
Restricted-polygon intrusion detection.

/events                          - list / filter violation events
/events/{event_id}/resolve       - acknowledge / resolve an event
/dashboard/active                - live unresolved events
/dashboard/summary               - daily rollup
/zones                           - list / create zones
/zones/{zone_id}                 - update / delete a zone
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
from app.services.no_entry_zone_service import no_entry_zone_service
from app.services.user_service import user_manager
from app.services.database import db_manager
from app.schemas.no_entry_zone_schema import (
    NoEntryEventListResponse,
    ResolveNoEntryRequest,
    NoEntryDailySummary,
    ActiveNoEntryEventSummary,
    ZoneCreateRequest,
    ZoneUpdateRequest,
    ZoneResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/no-entry-zone", tags=["No Entry Zone"])


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

@router.get("/events", response_model=NoEntryEventListResponse)
async def get_no_entry_events(
    status_filter: Optional[str] = Query(None, description="detected | acknowledged | resolved"),
    camera_id: Optional[str] = Query(None, description="Filter by stream/camera UUID"),
    zone_id: Optional[str] = Query(None, description="Filter by zone UUID"),
    f: dict = Depends(_filters),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        offset = (page - 1) * limit

        events = await no_entry_zone_service.get_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id,
            zone_id=zone_id, limit=limit, offset=offset, **f,
        )
        total = await no_entry_zone_service.count_events(
            workspace_id=workspace_id, status=status_filter, camera_id=camera_id, zone_id=zone_id, **f,
        )
        return NoEntryEventListResponse(items=events, total=total, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_no_entry_events error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving no-entry-zone events.")


@router.post("/events/{event_id}/resolve")
async def resolve_no_entry_event(
    event_id: int,
    request: ResolveNoEntryRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    await get_workspace_id_for_user(current_user["username"])
    success = await no_entry_zone_service.resolve_event(
        event_id=event_id, status=request.status, description=request.description,
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to resolve event")
    return {"message": f"Event {event_id} updated to '{request.status}'"}


@router.get("/dashboard/active", response_model=List[ActiveNoEntryEventSummary])
async def get_active_no_entry_dashboard(
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
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
        workspace_id = await get_workspace_id_for_user(current_user["username"])
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
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    return await no_entry_zone_service.list_zones(workspace_id, stream_id=stream_id)


@router.post("/zones", response_model=ZoneResponse)
async def create_zone(
    request: ZoneCreateRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        zone = await no_entry_zone_service.create_zone(workspace_id, request.model_dump())
        try:
            from app.services.no_entry_zone_inference import no_entry_zone_engine
            zones = await no_entry_zone_service.list_zones(workspace_id, stream_id=request.stream_id)
            no_entry_zone_engine.set_zones(str(request.stream_id), zones)
        except Exception as e:
            logger.warning(f"Failed to hot-reload zones for {request.stream_id}: {e}")
        return zone
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_zone error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error creating zone.")


@router.patch("/zones/{zone_id}", response_model=ZoneResponse)
async def update_zone(
    zone_id: UUID,
    request: ZoneUpdateRequest,
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        workspace_id = await get_workspace_id_for_user(current_user["username"])
        updated = await no_entry_zone_service.update_zone(
            zone_id, workspace_id, request.model_dump(exclude_unset=True)
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Zone not found")
        # Hot-reload the running stream worker's zone cache, mirroring the
        # prototype's manager.reload_zones(camera_id) behavior: refetch this
        # camera's active zones and push them into the in-process engine cache.
        try:
            from app.services.no_entry_zone_inference import no_entry_zone_engine
            zones = await no_entry_zone_service.list_zones(workspace_id, stream_id=updated["stream_id"])
            no_entry_zone_engine.set_zones(str(updated["stream_id"]), zones)
        except Exception as e:
            logger.warning(f"Failed to hot-reload zones for {updated['stream_id']}: {e}")
        return updated
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"update_zone error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error updating zone.")


@router.delete("/zones/{zone_id}")
async def delete_zone(
    zone_id: UUID,
    stream_id: Optional[UUID] = Query(None, description="Camera UUID, used to refresh the live zone cache"),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    workspace_id = await get_workspace_id_for_user(current_user["username"])
    deleted = await no_entry_zone_service.delete_zone(zone_id, workspace_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Zone not found")
    if stream_id:
        try:
            from app.services.no_entry_zone_inference import no_entry_zone_engine
            zones = await no_entry_zone_service.list_zones(workspace_id, stream_id=stream_id)
            no_entry_zone_engine.set_zones(str(stream_id), zones)
        except Exception as e:
            logger.warning(f"Failed to hot-reload zones for {stream_id}: {e}")
    return {"message": f"Zone {zone_id} deleted"}


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
async def delete_no_entry_data(
    camera_id: Optional[str] = Query(None, description="Filter by camera UUID"),
    f: dict = Depends(_filters),
    current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
):
    try:
        _, workspace_id = await _require_admin_or_owner(current_user)
        result = await no_entry_zone_service.delete_events(
            workspace_id=workspace_id, camera_id=camera_id, **f,
        )
        return result
    except HTTPException:
        raise
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
    try:
        _, workspace_id = await _require_admin_or_owner(current_user)
        result = await no_entry_zone_service.delete_all_events(workspace_id=workspace_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"delete_all_no_entry_data error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error deleting all no-entry-zone data.")
