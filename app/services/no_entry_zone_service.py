import json
import logging
from datetime import datetime, timedelta, time as dt_time
from typing import List, Dict, Any, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from app.config.settings import config
from app.services.database import db_manager
from app.utils.parser_utils import parse_date_format, parse_time_string

_TZ_NAME = getattr(config, "no_entry_zone_timezone", "Africa/Cairo")
_TZ = ZoneInfo(_TZ_NAME)

logger = logging.getLogger(__name__)

# Columns returned for an event everywhere (list, detail, dashboard).
_EVENT_COLUMNS = """
    ne.event_id, ne.incident_id, ne.zone_id, ne.stream_id, ne.event_timestamp,
    ne.entered_at, ne.exited_at, ne.camera_name, ne.zone_name, ne.target_class,
    ne.track_id, ne.confidence, ne.dwell_seconds, ne.status, ne.description,
    ne.snapshot_path, ne.clip_path, ne.evidence_paths,
    ne.acknowledged_at, ne.resolved_at,
    vs.location AS camera_location, vs.building, vs.floor_level, vs.zone AS camera_zone
"""

# Nullable per-zone overrides: an explicit null in a PATCH resets them to the default.
_NULLABLE_ZONE_COLUMNS = ("consecutive_frames", "cooldown_seconds", "anchor")


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
        incident_id: Optional[str] = None,
        target_class: Optional[str] = None,
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

        def add(sql: str, value: Any) -> None:
            nonlocal p
            p += 1
            conditions.append(sql.replace("$?", f"${p}"))
            params.append(value)

        if status:
            add(f"{table_alias}.status = $?", status)
        if camera_id:
            add(f"{table_alias}.stream_id = $?::uuid", str(camera_id))
        if zone_id:
            add(f"{table_alias}.zone_id = $?::uuid", str(zone_id))
        if incident_id:
            add(f"{table_alias}.incident_id = $?::uuid", str(incident_id))
        if target_class:
            add(f"{table_alias}.target_class = $?", target_class)

        # Time-of-day bounds are compared in the configured zone, the same zone the
        # date bounds are built in (the DB session zone may differ).
        local_time = f"({table_alias}.event_timestamp AT TIME ZONE '{_TZ_NAME}')::time"
        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            add(f"{table_alias}.event_timestamp >= $?", datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            add(f"{local_time} >= $?", parse_time_string(start_time, dt_time.min))
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            add(f"{table_alias}.event_timestamp <= $?", datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time and not start_date:
            add(f"{local_time} <= $?", parse_time_string(end_time, dt_time(23, 59, 59)))
        if location:
            add(f"{vs_alias}.location = $?", location)
        if building:
            add(f"{vs_alias}.building = $?", building)
        if floor_level:
            add(f"{vs_alias}.floor_level = $?", floor_level)
        if zone:
            add(f"{vs_alias}.zone = $?", zone)

        return conditions, params, p

    # =========================================================
    # Events
    # =========================================================

    async def get_events(
        self,
        workspace_id: UUID,
        *,
        limit: int = 10,
        offset: int = 0,
        **filters,
    ) -> List[Dict[str, Any]]:
        conditions, params, p = self._build_event_conditions(workspace_id, **filters)
        params.extend([limit, offset])
        query = f"""
            SELECT {_EVENT_COLUMNS}
            FROM no_entry_events ne
            LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
            ORDER BY ne.event_timestamp DESC, ne.event_id DESC
            LIMIT ${p + 1} OFFSET ${p + 2}
        """
        return await self.db.execute_query(query, tuple(params), fetch_all=True)

    async def count_events(self, workspace_id: UUID, **filters) -> int:
        conditions, params, _ = self._build_event_conditions(workspace_id, **filters)
        query = f"""
            SELECT COUNT(*) AS total
            FROM no_entry_events ne
            LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return row["total"] if row else 0

    async def get_event(self, event_id: int, workspace_id: UUID) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            f"""
            SELECT {_EVENT_COLUMNS}
            FROM no_entry_events ne
            LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
            WHERE ne.event_id = $1 AND ne.workspace_id = $2
            """,
            (event_id, workspace_id),
            fetch_one=True,
        )

    async def resolve_event(
        self,
        *,
        event_id: int,
        workspace_id: UUID,
        status: str,
        description: Optional[str] = None,
        user_id: Optional[UUID] = None,
    ) -> bool:
        affected = await self.db.execute_query(
            """
            UPDATE no_entry_events
            SET status          = $3::varchar,
                description     = COALESCE($4::text, description),
                acknowledged_at = CASE WHEN $3::varchar = 'acknowledged' THEN NOW() ELSE acknowledged_at END,
                acknowledged_by = CASE WHEN $3::varchar = 'acknowledged' THEN $5::uuid ELSE acknowledged_by END,
                resolved_at     = CASE WHEN $3::varchar = 'resolved' THEN NOW() ELSE resolved_at END,
                resolved_by     = CASE WHEN $3::varchar = 'resolved' THEN $5::uuid ELSE resolved_by END
            WHERE event_id = $1 AND workspace_id = $2
            """,
            (event_id, workspace_id, status, description, user_id),
            return_rowcount=True,
        )
        return affected > 0

    # =========================================================
    # Incidents (events sharing incident_id)
    # =========================================================

    def _incident_cte(self, conditions: List[str]) -> str:
        """Group the filtered events into incidents. The representative event is the
        first one with a snapshot (else the first one), which supplies the evidence."""
        return f"""
            WITH filtered AS (
                SELECT ne.*
                FROM no_entry_events ne
                LEFT JOIN video_stream vs ON ne.stream_id = vs.stream_id
                WHERE {' AND '.join(conditions)}
            ),
            agg AS (
                SELECT
                    incident_id,
                    MIN(event_timestamp)                         AS started_at,
                    MAX(COALESCE(exited_at, event_timestamp))    AS last_seen_at,
                    BOOL_OR(exited_at IS NULL AND status <> 'resolved') AS is_ongoing,
                    COUNT(*)                                     AS event_count,
                    MAX(confidence)                              AS max_confidence,
                    MAX(dwell_seconds)                           AS max_dwell_seconds,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT target_class), NULL) AS target_classes,
                    CASE
                        WHEN BOOL_OR(status = 'detected')     THEN 'detected'
                        WHEN BOOL_OR(status = 'acknowledged') THEN 'acknowledged'
                        ELSE 'resolved'
                    END                                          AS status
                FROM filtered
                GROUP BY incident_id
            ),
            rep AS (
                SELECT DISTINCT ON (incident_id)
                    incident_id, event_id, zone_id, stream_id, camera_name, zone_name,
                    snapshot_path, clip_path
                FROM filtered
                ORDER BY incident_id, (snapshot_path IS NULL), event_timestamp
            ),
            incidents AS (
                SELECT agg.*, rep.event_id, rep.zone_id, rep.stream_id, rep.camera_name,
                       rep.zone_name, rep.snapshot_path, rep.clip_path
                FROM agg JOIN rep USING (incident_id)
            )
        """

    async def list_incidents(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
        **filters,
    ) -> List[Dict[str, Any]]:
        # Status is an incident-level property (least-progressed of its events), so it
        # filters the grouped rows rather than the events.
        conditions, params, p = self._build_event_conditions(workspace_id, **filters)
        status_sql = ""
        if status:
            p += 1
            status_sql = f"WHERE status = ${p}"
            params.append(status)
        params.extend([limit, offset])
        query = f"""
            {self._incident_cte(conditions)}
            SELECT * FROM incidents
            {status_sql}
            ORDER BY started_at DESC
            LIMIT ${p + 1} OFFSET ${p + 2}
        """
        return await self.db.execute_query(query, tuple(params), fetch_all=True)

    async def count_incidents(self, workspace_id: UUID, *, status: Optional[str] = None, **filters) -> int:
        conditions, params, p = self._build_event_conditions(workspace_id, **filters)
        status_sql = ""
        if status:
            p += 1
            status_sql = f"WHERE status = ${p}"
            params.append(status)
        query = f"""
            {self._incident_cte(conditions)}
            SELECT COUNT(*) AS total FROM incidents {status_sql}
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return row["total"] if row else 0

    async def resolve_incident(
        self,
        *,
        incident_id: UUID,
        workspace_id: UUID,
        status: str,
        description: Optional[str] = None,
        user_id: Optional[UUID] = None,
    ) -> Optional[int]:
        """Apply the status to every event of the incident. Acknowledging never moves an
        already-resolved event backwards. Returns None when the incident does not exist
        in this workspace, else the number of events changed."""
        exists = await self.db.execute_query(
            "SELECT 1 FROM no_entry_events WHERE incident_id = $1 AND workspace_id = $2 LIMIT 1",
            (incident_id, workspace_id),
            fetch_one=True,
        )
        if not exists:
            return None
        return await self.db.execute_query(
            """
            UPDATE no_entry_events
            SET status          = $3::varchar,
                description     = COALESCE($4::text, description),
                acknowledged_at = CASE WHEN $3::varchar = 'acknowledged' THEN NOW() ELSE acknowledged_at END,
                acknowledged_by = CASE WHEN $3::varchar = 'acknowledged' THEN $5::uuid ELSE acknowledged_by END,
                resolved_at     = CASE WHEN $3::varchar = 'resolved' THEN NOW() ELSE resolved_at END,
                resolved_by     = CASE WHEN $3::varchar = 'resolved' THEN $5::uuid ELSE resolved_by END
            WHERE incident_id = $1 AND workspace_id = $2
              AND status <> 'resolved' AND status <> $3::varchar
            """,
            (incident_id, workspace_id, status, description, user_id),
            return_rowcount=True,
        )

    # =========================================================
    # Dashboard / analytics
    # =========================================================

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

    async def overview(self, workspace_id: UUID) -> Dict[str, Any]:
        since = datetime.now(_TZ) - timedelta(hours=24)
        counts = await self.db.execute_query(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_timestamp >= $2)                  AS events_24h,
                COUNT(DISTINCT incident_id) FILTER (WHERE event_timestamp >= $2) AS incidents_24h,
                COUNT(DISTINCT incident_id) FILTER (WHERE status <> 'resolved') AS open_incidents,
                COUNT(*) FILTER (WHERE status = 'detected')                    AS unacknowledged_events,
                COUNT(*) FILTER (WHERE exited_at IS NULL AND status <> 'resolved'
                                   AND event_timestamp >= $2)                  AS ongoing_events
            FROM no_entry_events
            WHERE workspace_id = $1
            """,
            (workspace_id, since),
            fetch_one=True,
        ) or {}
        cams = await self.db.execute_query(
            """
            SELECT
                COUNT(*) FILTER (WHERE vs.is_no_entry_zone_camera)                     AS nez_cameras,
                COUNT(*) FILTER (WHERE vs.is_no_entry_zone_camera AND vs.is_streaming) AS nez_cameras_streaming,
                (SELECT COUNT(*) FROM no_entry_zones z
                  WHERE z.workspace_id = $1 AND z.is_active)                           AS active_zones,
                (SELECT COUNT(*) FROM no_entry_zones z WHERE z.workspace_id = $1)      AS total_zones
            FROM video_stream vs
            WHERE vs.workspace_id = $1
            """,
            (workspace_id,),
            fetch_one=True,
        ) or {}
        latest = await self.list_incidents(workspace_id, limit=6, offset=0)
        return {**counts, **cams, "latest_incidents": latest}

    async def analytics_summary(
        self,
        workspace_id: UUID,
        *,
        since: datetime,
        until: datetime,
        bucket: str = "hour",
        stream_id: Optional[UUID] = None,
    ) -> Dict[str, Any]:
        if bucket not in ("hour", "day"):
            raise ValueError("bucket must be 'hour' or 'day'")
        step = "1 hour" if bucket == "hour" else "1 day"
        base_params: tuple = (workspace_id, since, until, str(stream_id) if stream_id else None)
        where = """
            ne.workspace_id = $1 AND ne.event_timestamp >= $2::timestamptz AND ne.event_timestamp < $3::timestamptz
            AND ($4::uuid IS NULL OR ne.stream_id = $4::uuid)
        """
        local_ts = f"(ne.event_timestamp AT TIME ZONE '{_TZ_NAME}')"

        totals = await self.db.execute_query(
            f"""
            SELECT COUNT(*) AS total_events,
                   COUNT(DISTINCT incident_id) AS total_incidents,
                   COUNT(*) FILTER (WHERE status = 'resolved') AS resolved_events,
                   AVG(dwell_seconds) AS avg_dwell_seconds
            FROM no_entry_events ne WHERE {where}
            """,
            base_params,
            fetch_one=True,
        ) or {}

        # Zero-filled buckets in the configured zone, so gaps show as gaps.
        series = await self.db.execute_query(
            f"""
            WITH buckets AS (
                SELECT generate_series(
                    date_trunc('{bucket}', $2::timestamptz AT TIME ZONE '{_TZ_NAME}'),
                    date_trunc('{bucket}', ($3::timestamptz - interval '1 microsecond') AT TIME ZONE '{_TZ_NAME}'),
                    interval '{step}'
                ) AS bucket
            ),
            counts AS (
                SELECT date_trunc('{bucket}', {local_ts}) AS bucket,
                       COUNT(*) AS events, COUNT(DISTINCT incident_id) AS incidents
                FROM no_entry_events ne WHERE {where}
                GROUP BY 1
            )
            SELECT b.bucket, COALESCE(c.events, 0) AS events, COALESCE(c.incidents, 0) AS incidents
            FROM buckets b LEFT JOIN counts c USING (bucket)
            ORDER BY b.bucket
            """,
            base_params,
            fetch_all=True,
        )

        async def breakdown(column: str) -> List[Dict[str, Any]]:
            return await self.db.execute_query(
                f"""
                SELECT COALESCE({column}, 'unknown') AS label,
                       COUNT(*) AS events, COUNT(DISTINCT incident_id) AS incidents
                FROM no_entry_events ne WHERE {where}
                GROUP BY 1 ORDER BY incidents DESC, events DESC LIMIT 20
                """,
                base_params,
                fetch_all=True,
            )

        by_hour = await self.db.execute_query(
            f"""
            SELECT h.hour, COALESCE(c.events, 0) AS events, COALESCE(c.incidents, 0) AS incidents
            FROM generate_series(0, 23) AS h(hour)
            LEFT JOIN (
                SELECT EXTRACT(HOUR FROM {local_ts})::int AS hour,
                       COUNT(*) AS events, COUNT(DISTINCT incident_id) AS incidents
                FROM no_entry_events ne WHERE {where}
                GROUP BY 1
            ) c USING (hour)
            ORDER BY h.hour
            """,
            base_params,
            fetch_all=True,
        )

        return {
            "bucket": bucket,
            "timezone": _TZ_NAME,
            "since": since,
            "until": until,
            **totals,
            "series": series,
            "by_zone": await breakdown("ne.zone_name"),
            "by_camera": await breakdown("ne.camera_name"),
            "by_class": await breakdown("ne.target_class"),
            "by_hour": by_hour,
        }

    # =========================================================
    # Detection-side writes (called from stream processing)
    # =========================================================

    async def create_open_event(
        self,
        *,
        incident_id: UUID,
        zone_id: UUID,
        stream_id: UUID,
        workspace_id: UUID,
        camera_name: Optional[str],
        zone_name: Optional[str],
        target_class: str,
        track_id: Optional[int],
        confidence: Optional[float],
        entered_at: Optional[datetime],
        alerted_at: Optional[datetime],
        dwell_seconds: Optional[float],
    ) -> Optional[int]:
        """Insert a confirmed violation. Incident grouping is decided by the engine
        (zone occupancy), not here. Returns the new event_id."""
        row = await self.db.execute_query(
            """
            INSERT INTO no_entry_events (
                incident_id, zone_id, stream_id, workspace_id, camera_name, zone_name,
                target_class, track_id, confidence, entered_at, event_timestamp, dwell_seconds
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, NOW()), $12)
            RETURNING event_id
            """,
            (
                incident_id, zone_id, stream_id, workspace_id, camera_name, zone_name,
                target_class, track_id, confidence, entered_at, alerted_at, dwell_seconds,
            ),
            fetch_one=True,
        )
        return row["event_id"] if row else None

    async def close_event(
        self, event_id: int, *, exited_at: datetime, dwell_seconds: Optional[float]
    ) -> None:
        await self.db.execute_query(
            """
            UPDATE no_entry_events
            SET exited_at = $2, dwell_seconds = GREATEST(COALESCE(dwell_seconds, 0), COALESCE($3, 0))
            WHERE event_id = $1 AND exited_at IS NULL
            """,
            (event_id, exited_at, dwell_seconds),
            return_rowcount=True,
        )

    async def set_evidence(
        self, event_id: int, *, snapshot_path: Optional[str] = None, clip_path: Optional[str] = None
    ) -> None:
        """Record an uploaded evidence file; evidence_paths is kept for older readers."""
        await self.db.execute_query(
            """
            UPDATE no_entry_events
            SET snapshot_path  = COALESCE($2, snapshot_path),
                clip_path      = COALESCE($3, clip_path),
                evidence_paths = ARRAY(
                    SELECT DISTINCT p FROM unnest(
                        COALESCE(evidence_paths, '{}') || ARRAY_REMOVE(ARRAY[$2, $3]::text[], NULL)
                    ) AS p
                )
            WHERE event_id = $1
            """,
            (event_id, snapshot_path, clip_path),
            return_rowcount=True,
        )

    async def is_camera_enabled(self, stream_id: UUID) -> bool:
        row = await self.db.execute_query(
            "SELECT is_no_entry_zone_camera FROM video_stream WHERE stream_id = $1",
            (stream_id,),
            fetch_one=True,
        )
        return bool(row and row.get("is_no_entry_zone_camera"))

    # =========================================================
    # Cameras
    # =========================================================

    async def list_cameras(
        self, workspace_id: UUID, *, enabled_only: bool = True, stream_id: Optional[UUID] = None,
    ) -> List[Dict[str, Any]]:
        """Workspace cameras with zone counts. Rows include ``path`` (the source URL, often
        with credentials) for server-side use only — the router strips it."""
        return await self.db.execute_query(
            """
            SELECT vs.stream_id, vs.name, vs.location, vs.building, vs.floor_level,
                   vs.status, vs.is_streaming, vs.is_no_entry_zone_camera,
                   vs.type, vs.path, vs.last_activity, vs.stop_reason, vs.retry_count,
                   vs.auto_retry_enabled, vs.locked_by_server,
                   COUNT(z.zone_id)                          AS zone_count,
                   COUNT(z.zone_id) FILTER (WHERE z.is_active) AS active_zone_count
            FROM video_stream vs
            LEFT JOIN no_entry_zones z ON z.stream_id = vs.stream_id
            WHERE vs.workspace_id = $1 AND ($2 = FALSE OR vs.is_no_entry_zone_camera)
              AND ($3::uuid IS NULL OR vs.stream_id = $3::uuid)
            GROUP BY vs.stream_id
            ORDER BY vs.name
            """,
            (workspace_id, enabled_only, str(stream_id) if stream_id else None),
            fetch_all=True,
        )

    async def stream_in_workspace(self, stream_id: UUID, workspace_id: UUID) -> bool:
        row = await self.db.execute_query(
            "SELECT 1 FROM video_stream WHERE stream_id = $1 AND workspace_id = $2",
            (stream_id, workspace_id),
            fetch_one=True,
        )
        return row is not None

    # =========================================================
    # Zone CRUD
    # =========================================================

    async def list_zones(self, workspace_id: UUID, stream_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        rows = await self.db.execute_query(
            """
            SELECT z.*, vs.name AS camera_name
            FROM no_entry_zones z
            LEFT JOIN video_stream vs ON vs.stream_id = z.stream_id
            WHERE z.workspace_id = $1 AND ($2::uuid IS NULL OR z.stream_id = $2::uuid)
            ORDER BY z.created_at DESC
            """,
            (workspace_id, str(stream_id) if stream_id else None),
            fetch_all=True,
        )
        return [_decode_zone_row(r) for r in rows]

    async def list_zones_for_stream(self, stream_id: UUID) -> List[Dict[str, Any]]:
        """Every zone of a camera, regardless of the caller's workspace (detection side)."""
        rows = await self.db.execute_query(
            "SELECT * FROM no_entry_zones WHERE stream_id = $1",
            (stream_id,),
            fetch_all=True,
        )
        return [_decode_zone_row(r) for r in rows]

    async def get_zone(self, zone_id: UUID, workspace_id: UUID) -> Optional[Dict[str, Any]]:
        row = await self.db.execute_query(
            """
            SELECT z.*, vs.name AS camera_name
            FROM no_entry_zones z
            LEFT JOIN video_stream vs ON vs.stream_id = z.stream_id
            WHERE z.zone_id = $1 AND z.workspace_id = $2
            """,
            (zone_id, workspace_id),
            fetch_one=True,
        )
        return _decode_zone_row(row)

    async def create_zone(self, workspace_id: UUID, data: Dict[str, Any]) -> Dict[str, Any]:
        row = await self.db.execute_query(
            """
            INSERT INTO no_entry_zones (
                stream_id, workspace_id, name, polygon, ref_width, ref_height,
                target_classes, min_dwell_seconds, schedule, is_active,
                consecutive_frames, cooldown_seconds, anchor
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6,
                      COALESCE($7::text[], ARRAY['person']::text[]), COALESCE($8::numeric, 1.00), $9::jsonb,
                      COALESCE($10::boolean, TRUE), $11::integer, $12::numeric, $13)
            RETURNING *
            """,
            (
                data["stream_id"], workspace_id, data["name"], json.dumps(data["polygon"]),
                data["ref_width"], data["ref_height"], data.get("target_classes"),
                data.get("min_dwell_seconds"),
                json.dumps(data["schedule"]) if data.get("schedule") is not None else None,
                data.get("is_active"),
                data.get("consecutive_frames"), data.get("cooldown_seconds"), data.get("anchor"),
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
        for col in _NULLABLE_ZONE_COLUMNS:
            if col in data:
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
            return await self.get_zone(zone_id, workspace_id)
        params.extend([zone_id, workspace_id])
        query = f"""
            UPDATE no_entry_zones SET {', '.join(fields)}
            WHERE zone_id = ${p + 1} AND workspace_id = ${p + 2}
            RETURNING *
        """
        row = await self.db.execute_query(query, tuple(params), fetch_one=True)
        return _decode_zone_row(row)

    async def delete_zone(self, zone_id: UUID, workspace_id: UUID) -> Optional[UUID]:
        """Returns the zone's stream_id (for the live cache refresh), or None if absent."""
        row = await self.db.execute_query(
            "DELETE FROM no_entry_zones WHERE zone_id = $1 AND workspace_id = $2 RETURNING stream_id",
            (zone_id, workspace_id),
            fetch_one=True,
        )
        return row["stream_id"] if row else None

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
