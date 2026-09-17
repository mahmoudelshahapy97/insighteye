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


def _decode_zone_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """asyncpg returns JSONB columns as raw text; decode polygon/schedule."""
    if row is None:
        return None
    for key in ("polygon", "schedule"):
        value = row.get(key)
        if isinstance(value, str):
            try:
                row[key] = json.loads(value)
            except (TypeError, ValueError):
                pass
    return row


class NoEntryZoneService:
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
        zone_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        table_alias: str = "ne",
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
        if zone_id:
            p += 1
            conditions.append(f"{table_alias}.zone_id = ${p}::uuid")
            params.append(zone_id)
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
        zone_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        **filters,
    ) -> List[Dict[str, Any]]:
        conditions, params, p = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id, zone_id=zone_id, **filters
        )
        p += 1
        limit_idx = p
        p += 1
        offset_idx = p
        params.extend([limit, offset])

        query = f"""
            SELECT ne.event_id, ne.incident_id, ne.event_timestamp, ne.camera_name,
                   ne.zone_name, ne.target_class, ne.dwell_seconds, ne.status,
                   ne.description, ne.evidence_paths, ne.resolved_at,
                   vs.location AS camera_location, vs.building, vs.floor_level
            FROM no_entry_events ne
            LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
            ORDER BY ne.event_timestamp DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """
        return await self.db.execute_query(query, tuple(params), fetch_all=True)

    async def count_events(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        zone_id: Optional[str] = None,
        **filters,
    ) -> int:
        conditions, params, _ = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id, zone_id=zone_id, **filters
        )
        query = f"""
            SELECT COUNT(*) AS total
            FROM no_entry_events ne
            LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return row["total"] if row else 0

    async def resolve_event(
        self, *, event_id: int, status: str, description: Optional[str] = None
    ) -> bool:
        row = await self.db.execute_query(
            "SELECT resolve_no_entry_event($1, $2, $3) AS found",
            (event_id, status, description),
            fetch_one=True,
        )
        return bool(row and row.get("found"))

    async def get_active_dashboard(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            "SELECT * FROM v_active_no_entry_events WHERE workspace_id = $1",
            (workspace_id,),
            fetch_all=True,
        )

    async def get_daily_summary(self, workspace_id: UUID, limit: int = 30) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT * FROM v_no_entry_daily_summary
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
        zone_id: UUID,
        stream_id: UUID,
        workspace_id: UUID,
        camera_name: Optional[str],
        zone_name: Optional[str],
        target_class: str,
        dwell_seconds: Optional[float],
        evidence_paths: Optional[List[str]] = None,
        incident_window_seconds: int = 300,
    ) -> Dict[str, Any]:
        """Insert a violation, grouping into an existing open incident for this zone
        within incident_window_seconds (mirrors the prototype's incident-grouping logic)."""
        existing = await self.db.execute_query(
            """
            SELECT incident_id FROM no_entry_events
            WHERE zone_id = $1 AND status = 'detected'
              AND event_timestamp > NOW() - ($2 || ' seconds')::interval
            ORDER BY event_timestamp DESC LIMIT 1
            """,
            (zone_id, incident_window_seconds),
            fetch_one=True,
        )
        incident_id = existing["incident_id"] if existing else None

        row = await self.db.execute_query(
            """
            INSERT INTO no_entry_events (
                incident_id, zone_id, stream_id, workspace_id, camera_name, zone_name,
                target_class, dwell_seconds, evidence_paths
            ) VALUES (COALESCE($1, uuid_generate_v4()), $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING event_id, incident_id
            """,
            (
                incident_id, zone_id, stream_id, workspace_id, camera_name, zone_name,
                target_class, dwell_seconds, evidence_paths,
            ),
            fetch_one=True,
        )
        return row

    # =========================================================
    # Zone CRUD
    # =========================================================

    async def list_zones(self, workspace_id: UUID, stream_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        if stream_id:
            rows = await self.db.execute_query(
                "SELECT * FROM no_entry_zones WHERE workspace_id = $1 AND stream_id = $2 ORDER BY created_at DESC",
                (workspace_id, stream_id),
                fetch_all=True,
            )
        else:
            rows = await self.db.execute_query(
                "SELECT * FROM no_entry_zones WHERE workspace_id = $1 ORDER BY created_at DESC",
                (workspace_id,),
                fetch_all=True,
            )
        return [_decode_zone_row(r) for r in rows]

    async def create_zone(self, workspace_id: UUID, data: Dict[str, Any]) -> Dict[str, Any]:
        row = await self.db.execute_query(
            """
            INSERT INTO no_entry_zones (
                stream_id, workspace_id, name, polygon, ref_width, ref_height,
                target_classes, min_dwell_seconds, schedule
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6,
                      COALESCE($7, '{"person"}'), COALESCE($8, 1.00), $9::jsonb)
            RETURNING *
            """,
            (
                data["stream_id"], workspace_id, data["name"], json.dumps(data["polygon"]),
                data["ref_width"], data["ref_height"], data.get("target_classes"),
                data.get("min_dwell_seconds"),
                json.dumps(data["schedule"]) if data.get("schedule") is not None else None,
            ),
            fetch_one=True,
        )
        return _decode_zone_row(row)

    async def update_zone(self, zone_id: UUID, workspace_id: UUID, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fields = []
        params: list = []
        p = 0
        for col in ("name", "ref_width", "ref_height", "target_classes", "min_dwell_seconds", "is_active"):
            if data.get(col) is not None:
                p += 1
                fields.append(f"{col} = ${p}")
                params.append(data[col])
        if data.get("polygon") is not None:
            p += 1
            fields.append(f"polygon = ${p}::jsonb")
            params.append(json.dumps(data["polygon"]))
        if "schedule" in data:
            # Explicit null in the request body clears the schedule (zone becomes
            # always-active); omitting the field entirely leaves it untouched.
            p += 1
            fields.append(f"schedule = ${p}::jsonb")
            params.append(json.dumps(data["schedule"]) if data["schedule"] is not None else None)
        if not fields:
            row = await self.db.execute_query(
                "SELECT * FROM no_entry_zones WHERE zone_id = $1 AND workspace_id = $2",
                (zone_id, workspace_id),
                fetch_one=True,
            )
            return _decode_zone_row(row)
        p += 1
        zone_idx = p
        p += 1
        ws_idx = p
        params.extend([zone_id, workspace_id])
        query = f"""
            UPDATE no_entry_zones SET {', '.join(fields)}
            WHERE zone_id = ${zone_idx} AND workspace_id = ${ws_idx}
            RETURNING *
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return _decode_zone_row(row)

    async def delete_zone(self, zone_id: UUID, workspace_id: UUID) -> bool:
        affected = await self.db.execute_query(
            "DELETE FROM no_entry_zones WHERE zone_id = $1 AND workspace_id = $2",
            (zone_id, workspace_id),
            return_rowcount=True,
        )
        return affected > 0

    # =========================================================
    # Data management
    # =========================================================

    async def delete_events(self, workspace_id: UUID, *, camera_id: Optional[str] = None, **filters) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(workspace_id, camera_id=camera_id, **filters)
        query = f"""
            DELETE FROM no_entry_events
            WHERE event_id IN (
                SELECT ne.event_id
                FROM no_entry_events ne
                LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
                WHERE {' AND '.join(conditions)}
            )
        """
        affected = await self.db.execute_query(query, tuple(params), return_rowcount=True)
        return {"deleted": affected}

    async def delete_all_events(self, workspace_id: UUID) -> Dict[str, Any]:
        affected = await self.db.execute_query(
            "DELETE FROM no_entry_events WHERE workspace_id = $1",
            (workspace_id,),
            return_rowcount=True,
        )
        return {"deleted": affected}


no_entry_zone_service = NoEntryZoneService()
