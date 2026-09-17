import json
import logging
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from app.services.database import db_manager
from app.utils.parser_utils import parse_date_format, parse_time_string

_TZ = ZoneInfo("Africa/Cairo")

logger = logging.getLogger(__name__)


def _decode_zone_config_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """asyncpg returns JSONB columns as raw text; decode door_polygon/reference_bbox."""
    if row is None:
        return None
    for key in ("door_polygon", "reference_bbox"):
        value = row.get(key)
        if isinstance(value, str):
            try:
                row[key] = json.loads(value)
            except (TypeError, ValueError):
                pass
    return row


class BlockedExitService:
    def __init__(self):
        self.db = db_manager

    # =========================================================
    # Internal helpers
    # =========================================================

    def _build_event_conditions(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        table_alias: str = "be",
        vs_alias: str = "vs",
    ):
        conditions = [f"{table_alias}.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if status:
            p += 1
            conditions.append(f"{table_alias}.status = ${p}")
            params.append(status)
        if camera_id:
            p += 1
            conditions.append(f"{table_alias}.stream_id = ${p}::uuid")
            params.append(camera_id)
        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            p += 1
            conditions.append(f"{table_alias}.event_timestamp >= ${p}")
            params.append(datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            t = parse_time_string(start_time, dt_time.min)
            p += 1
            conditions.append(f"{table_alias}.event_timestamp::time >= ${p}")
            params.append(t)
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            p += 1
            conditions.append(f"{table_alias}.event_timestamp <= ${p}")
            params.append(datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time and not start_date:
            t = parse_time_string(end_time, dt_time(23, 59, 59))
            p += 1
            conditions.append(f"{table_alias}.event_timestamp::time <= ${p}")
            params.append(t)
        if location:
            p += 1
            conditions.append(f"{vs_alias}.location = ${p}")
            params.append(location)
        if building:
            p += 1
            conditions.append(f"{vs_alias}.building = ${p}")
            params.append(building)
        if floor_level:
            p += 1
            conditions.append(f"{vs_alias}.floor_level = ${p}")
            params.append(floor_level)
        if zone:
            p += 1
            conditions.append(f"{vs_alias}.zone = ${p}")
            params.append(zone)

        return conditions, params, p

    # =========================================================
    # Events
    # =========================================================

    async def get_events(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        **filters,
    ) -> List[Dict[str, Any]]:
        conditions, params, p = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id, **filters
        )
        p += 1
        limit_idx = p
        p += 1
        offset_idx = p
        params.extend([limit, offset])

        query = f"""
            SELECT be.event_id, be.event_timestamp, be.state, be.accessibility_pct,
                   be.blocking_objects, be.risk_score, be.risk_level,
                   be.recommended_action, be.status, be.description, be.evidence_paths,
                   be.resolved_at, vs.name AS camera_name, vs.location AS camera_location,
                   vs.building, vs.floor_level, vs.zone
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON be.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
            ORDER BY be.event_timestamp DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """
        return await self.db.execute_query(query, tuple(params), fetch_all=True)

    async def count_events(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        **filters,
    ) -> int:
        conditions, params, _ = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id, **filters
        )
        query = f"""
            SELECT COUNT(*) AS total
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON be.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return row["total"] if row else 0

    async def resolve_event(
        self, *, event_id: int, status: str, description: Optional[str] = None
    ) -> bool:
        row = await self.db.execute_query(
            "SELECT resolve_blocked_exit_event($1, $2, $3) AS found",
            (event_id, status, description),
            fetch_one=True,
        )
        return bool(row and row.get("found"))

    async def get_active_dashboard(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            "SELECT * FROM v_active_blocked_exit_events WHERE workspace_id = $1",
            (workspace_id,),
            fetch_all=True,
        )

    async def get_daily_summary(self, workspace_id: UUID, limit: int = 30) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT * FROM v_blocked_exit_daily_summary
            WHERE workspace_id = $1
            ORDER BY event_date DESC
            LIMIT $2
            """,
            (workspace_id, limit),
            fetch_all=True,
        )

    async def record_event(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        state: str,
        accessibility_pct: Optional[float],
        blocking_objects: Optional[List[str]],
        risk_score: Optional[float],
        risk_level: Optional[str],
        recommended_action: Optional[str],
        evidence_paths: Optional[List[str]] = None,
        dedup_window_seconds: int = 300,
    ) -> Optional[int]:
        """Insert a new event unless an open one for this stream+state was created within
        dedup_window_seconds (mirrors shoplifting's auto-create dedup window)."""
        existing = await self.db.execute_query(
            """
            SELECT event_id FROM blocked_exit_events
            WHERE stream_id = $1 AND state = $2 AND status = 'detected'
              AND event_timestamp > NOW() - ($3 || ' seconds')::interval
            ORDER BY event_timestamp DESC LIMIT 1
            """,
            (stream_id, state, dedup_window_seconds),
            fetch_one=True,
        )
        if existing:
            return None

        row = await self.db.execute_query(
            """
            INSERT INTO blocked_exit_events (
                stream_id, workspace_id, state, accessibility_pct, blocking_objects,
                risk_score, risk_level, recommended_action, evidence_paths
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING event_id
            """,
            (
                stream_id, workspace_id, state, accessibility_pct, blocking_objects,
                risk_score, risk_level, recommended_action, evidence_paths,
            ),
            fetch_one=True,
        )
        return row["event_id"] if row else None

    # =========================================================
    # Zone / door-polygon configuration
    # =========================================================

    async def get_zone_config(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        row = await self.db.execute_query(
            "SELECT * FROM exit_zone_config WHERE stream_id = $1",
            (stream_id,),
            fetch_one=True,
        )
        return _decode_zone_config_row(row)

    async def set_door_polygon(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        door_polygon: List[List[float]],
        calibration_frame_w: Optional[int] = None,
        calibration_frame_h: Optional[int] = None,
        detector_strategy: str = "zone_based",
        min_accessibility_pct: Optional[float] = None,
        debounce_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        row = await self.db.execute_query(
            """
            INSERT INTO exit_zone_config (
                stream_id, workspace_id, door_polygon, calibration_frame_w,
                calibration_frame_h, detector_strategy, min_accessibility_pct, debounce_seconds
            ) VALUES ($1, $2, $3::jsonb, $4, $5, $6,
                      COALESCE($7, 50.00), COALESCE($8, 5.00))
            ON CONFLICT (stream_id) DO UPDATE SET
                door_polygon = EXCLUDED.door_polygon,
                calibration_frame_w = EXCLUDED.calibration_frame_w,
                calibration_frame_h = EXCLUDED.calibration_frame_h,
                detector_strategy = EXCLUDED.detector_strategy,
                min_accessibility_pct = COALESCE(EXCLUDED.min_accessibility_pct, exit_zone_config.min_accessibility_pct),
                debounce_seconds = COALESCE(EXCLUDED.debounce_seconds, exit_zone_config.debounce_seconds),
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
            """,
            (
                stream_id, workspace_id, json.dumps(door_polygon), calibration_frame_w,
                calibration_frame_h, detector_strategy, min_accessibility_pct, debounce_seconds,
            ),
            fetch_one=True,
        )
        return _decode_zone_config_row(row)

    # =========================================================
    # Data management
    # =========================================================

    async def delete_events(self, workspace_id: UUID, *, camera_id: Optional[str] = None, **filters) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(workspace_id, camera_id=camera_id, **filters)
        query = f"""
            DELETE FROM blocked_exit_events
            WHERE event_id IN (
                SELECT be.event_id
                FROM blocked_exit_events be
                LEFT JOIN video_stream vs ON be.stream_id = vs.stream_id
                WHERE {' AND '.join(conditions)}
            )
        """
        affected = await self.db.execute_query(query, tuple(params), return_rowcount=True)
        return {"deleted": affected}

    async def delete_all_events(self, workspace_id: UUID) -> Dict[str, Any]:
        affected = await self.db.execute_query(
            "DELETE FROM blocked_exit_events WHERE workspace_id = $1",
            (workspace_id,),
            return_rowcount=True,
        )
        return {"deleted": affected}


blocked_exit_service = BlockedExitService()
