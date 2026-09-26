import json
import logging
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional, Tuple
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
        state: Optional[str] = None,
        risk_level: Optional[str] = None,
        open_only: bool = False,
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
        if state:
            p += 1
            conditions.append(f"COALESCE({table_alias}.peak_state, {table_alias}.state) = ${p}")
            params.append(state)
        if risk_level:
            p += 1
            conditions.append(f"{table_alias}.risk_level = ${p}")
            params.append(risk_level)
        if open_only:
            conditions.append(f"{table_alias}.status IN ('detected', 'acknowledged')")
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

    _EVENT_COLUMNS = """
        be.event_id, be.stream_id, be.event_timestamp, be.ended_at, be.duration_s,
        be.state, be.peak_state, be.accessibility_pct, be.min_accessibility_pct,
        be.blocking_objects, be.risk_score, be.peak_risk_score, be.risk_level,
        be.recommended_action, be.status, be.description, be.evidence_paths,
        be.acknowledged_at, be.resolved_at,
        COALESCE(vs.name, be.camera_name) AS camera_name, vs.location AS camera_location,
        vs.building, vs.floor_level, vs.zone,
        CASE WHEN be.ended_at IS NULL
             THEN EXTRACT(EPOCH FROM (NOW() - be.event_timestamp))
             ELSE be.duration_s END::float AS effective_duration_s
    """

    async def get_events(
        self,
        workspace_id: UUID,
        *,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        order_by_risk: bool = False,
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
        order = (
            "COALESCE(be.peak_risk_score, be.risk_score, 0) DESC, be.event_timestamp DESC"
            if order_by_risk else "be.event_timestamp DESC"
        )

        query = f"""
            SELECT {self._EVENT_COLUMNS}
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON be.stream_id = vs.stream_id
            WHERE {' AND '.join(conditions)}
            ORDER BY {order}
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

    async def get_event(self, workspace_id: UUID, event_id: int) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            f"""
            SELECT {self._EVENT_COLUMNS}
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON be.stream_id = vs.stream_id
            WHERE be.workspace_id = $1 AND be.event_id = $2
            """,
            (workspace_id, event_id),
            fetch_one=True,
        )

    async def resolve_event(
        self,
        *,
        workspace_id: UUID,
        event_id: int,
        status: str,
        description: Optional[str] = None,
        user_id: Optional[UUID] = None,
    ) -> bool:
        """Acknowledge or resolve an event. Scoped to the caller's workspace, so an
        event id from another workspace behaves as not found."""
        row = await self.db.execute_query(
            "SELECT resolve_blocked_exit_event($1, $2, $3, $4, $5) AS found",
            (workspace_id, event_id, status, description, user_id),
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

    # =========================================================
    # Episodes (written by the stream processor)
    # =========================================================

    async def open_episode(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        camera_name: Optional[str],
        state: str,
        accessibility_pct: Optional[float],
        blocking_objects: Optional[List[str]],
        risk_score: Optional[float],
        risk_level: Optional[str],
        recommended_action: Optional[str],
        evidence_paths: Optional[List[str]] = None,
    ) -> Tuple[Optional[int], bool]:
        """Start a blockage episode. If one is already open for the stream (e.g. the
        engine was reset mid-episode) that one is continued instead.
        Returns (event_id, created)."""
        existing = await self.db.execute_query(
            "SELECT event_id FROM blocked_exit_events WHERE stream_id = $1 AND ended_at IS NULL",
            (stream_id,),
            fetch_one=True,
        )
        if existing:
            await self.update_episode(
                event_id=existing["event_id"], state=state, accessibility_pct=accessibility_pct,
                blocking_objects=blocking_objects, risk_score=risk_score, risk_level=risk_level,
                recommended_action=recommended_action,
            )
            return existing["event_id"], False

        row = await self.db.execute_query(
            """
            INSERT INTO blocked_exit_events (
                stream_id, workspace_id, camera_name, config_id, state, peak_state,
                accessibility_pct, min_accessibility_pct, blocking_objects,
                risk_score, peak_risk_score, risk_level, recommended_action, evidence_paths
            )
            SELECT $1::uuid, $2::uuid, $3::varchar, cfg.config_id, $4::varchar, $4::varchar,
                   $5::numeric, $5::numeric, $6::text[], $7::numeric, $7::numeric,
                   $8::varchar, $9::text, $10::text[]
            FROM (SELECT 1) AS one
            LEFT JOIN exit_zone_config cfg ON cfg.stream_id = $1::uuid
            ON CONFLICT (stream_id) WHERE ended_at IS NULL DO NOTHING
            RETURNING event_id
            """,
            (
                stream_id, workspace_id, camera_name, state, accessibility_pct,
                blocking_objects, risk_score, risk_level, recommended_action, evidence_paths,
            ),
            fetch_one=True,
        )
        return (row["event_id"], True) if row else (None, False)

    async def update_episode(
        self,
        *,
        event_id: int,
        state: str,
        accessibility_pct: Optional[float],
        blocking_objects: Optional[List[str]],
        risk_score: Optional[float],
        risk_level: Optional[str],
        recommended_action: Optional[str],
    ) -> None:
        """Refresh an open episode with the latest reading, keeping the worst values seen.
        The risk level only ever rises during an episode."""
        await self.db.execute_query(
            """
            UPDATE blocked_exit_events
            SET state                 = $2::varchar,
                accessibility_pct     = $3::numeric,
                min_accessibility_pct = LEAST(COALESCE(min_accessibility_pct, $3::numeric), $3::numeric),
                peak_state            = CASE WHEN $2::varchar = 'blocked' OR peak_state = 'blocked'
                                             THEN 'blocked' ELSE COALESCE(peak_state, $2::varchar) END,
                blocking_objects      = COALESCE($4::text[], blocking_objects),
                risk_score            = $5::numeric,
                peak_risk_score       = GREATEST(COALESCE(peak_risk_score, $5::numeric), $5::numeric),
                risk_level            = CASE
                    WHEN array_position(ARRAY['low','medium','high','critical']::varchar[], $6::varchar)
                       > COALESCE(array_position(ARRAY['low','medium','high','critical']::varchar[], risk_level), 0)
                    THEN $6::varchar ELSE risk_level END,
                recommended_action    = CASE
                    WHEN array_position(ARRAY['low','medium','high','critical']::varchar[], $6::varchar)
                       > COALESCE(array_position(ARRAY['low','medium','high','critical']::varchar[], risk_level), 0)
                    THEN $7::text ELSE recommended_action END
            WHERE event_id = $1 AND ended_at IS NULL
            """,
            (event_id, state, accessibility_pct, blocking_objects, risk_score, risk_level, recommended_action),
        )

    async def close_open_episodes(self, stream_id: UUID) -> int:
        """Close the stream's open episode (exit confirmed clear, or stream stopped)."""
        return await self.db.execute_query(
            """
            UPDATE blocked_exit_events
            SET ended_at   = NOW(),
                duration_s = GREATEST(EXTRACT(EPOCH FROM (NOW() - event_timestamp)), 0),
                state      = 'clear'
            WHERE stream_id = $1 AND ended_at IS NULL
            """,
            (stream_id,),
            return_rowcount=True,
        )

    async def get_open_episode_id(self, stream_id: UUID) -> Optional[int]:
        row = await self.db.execute_query(
            "SELECT event_id FROM blocked_exit_events WHERE stream_id = $1 AND ended_at IS NULL",
            (stream_id,),
            fetch_one=True,
        )
        return row["event_id"] if row else None

    async def record_event(self, **kwargs) -> Optional[int]:
        """Backwards-compatible wrapper: opens (or continues) an episode."""
        event_id, created = await self.open_episode(**kwargs)
        return event_id if created else None

    # =========================================================
    # Cameras & live status
    # =========================================================

    async def get_camera(self, workspace_id: UUID, stream_id: UUID) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT stream_id, workspace_id, name, location, building, floor_level, zone,
                   status, is_streaming, is_blocked_exit_camera
            FROM video_stream
            WHERE stream_id = $1 AND workspace_id = $2
            """,
            (stream_id, workspace_id),
            fetch_one=True,
        )

    async def list_cameras(
        self, workspace_id: UUID, *, enabled_only: bool = True, stream_id: Optional[UUID] = None,
    ) -> List[Dict[str, Any]]:
        """Workspace cameras with stream bookkeeping, door config and any ongoing
        episode. Never selects the source path (see get_camera_source)."""
        params: list = [workspace_id]
        where = ["vs.workspace_id = $1"]
        if enabled_only:
            where.append("vs.is_blocked_exit_camera")
        if stream_id:
            params.append(stream_id)
            where.append(f"vs.stream_id = ${len(params)}")
        rows = await self.db.execute_query(
            f"""
            SELECT vs.stream_id, vs.name AS camera_name, vs.location AS camera_location,
                   vs.building, vs.floor_level, vs.zone, vs.type, vs.status AS stream_status,
                   vs.is_streaming, vs.is_blocked_exit_camera, vs.last_activity, vs.stop_reason,
                   vs.retry_count, vs.auto_retry_enabled, vs.locked_by_server,
                   s.config_id, COALESCE(s.is_calibrated, FALSE) AS is_calibrated,
                   COALESCE(s.config_active, FALSE) AS config_active,
                   s.threshold_pct, s.debounce_seconds, s.open_event_id,
                   COALESCE(s.current_state, 'clear') AS current_state,
                   s.accessibility_pct, s.risk_level, s.risk_score, s.blocking_objects,
                   s.event_status, s.blocked_since, s.blocked_for_s, s.last_event_at,
                   cfg.door_polygon, cfg.calibration_frame_w, cfg.calibration_frame_h
            FROM video_stream vs
            LEFT JOIN v_blocked_exit_camera_status s ON s.stream_id = vs.stream_id
            LEFT JOIN exit_zone_config cfg ON cfg.stream_id = vs.stream_id
            WHERE {' AND '.join(where)}
            ORDER BY vs.is_blocked_exit_camera DESC, (s.open_event_id IS NULL), vs.name
            """,
            tuple(params),
            fetch_all=True,
        ) or []
        return [_decode_zone_config_row(r) for r in rows]

    async def get_camera_source(self, stream_id: UUID, workspace_id: UUID) -> Optional[Dict[str, Any]]:
        """Internal only: the camera's source path, which may embed credentials.
        Callers must never return it to a client."""
        return await self.db.execute_query(
            """
            SELECT stream_id, name, path, type, locked_by_server, is_streaming
            FROM video_stream WHERE stream_id = $1 AND workspace_id = $2
            """,
            (stream_id, workspace_id),
            fetch_one=True,
        )

    async def get_camera_sources(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        """Internal only: every blocked-exit camera's source, for connection checks."""
        return await self.db.execute_query(
            """
            SELECT stream_id, name, path, type FROM video_stream
            WHERE workspace_id = $1 AND is_blocked_exit_camera
            ORDER BY name
            """,
            (workspace_id,),
            fetch_all=True,
        ) or []

    async def get_camera_status(
        self, workspace_id: UUID, stream_id: Optional[UUID] = None,
    ) -> List[Dict[str, Any]]:
        """Blocked-exit cameras, worst first: open episodes by risk, then clear, then uncalibrated."""
        params: list = [workspace_id]
        extra = ""
        if stream_id:
            params.append(stream_id)
            extra = "AND stream_id = $2"
        rows = await self.db.execute_query(
            f"""
            SELECT * FROM v_blocked_exit_camera_status
            WHERE workspace_id = $1 {extra}
            ORDER BY (open_event_id IS NULL), risk_score DESC NULLS LAST,
                     is_calibrated DESC, camera_name
            """,
            tuple(params),
            fetch_all=True,
        )
        return rows or []

    # =========================================================
    # Analytics
    # =========================================================

    async def analytics_summary(self, workspace_id: UUID, days: int = 7) -> Dict[str, Any]:
        row = await self.db.execute_query(
            """
            WITH ev AS (
                SELECT be.*,
                       COALESCE(be.duration_s, EXTRACT(EPOCH FROM (NOW() - be.event_timestamp))) AS dur
                FROM blocked_exit_events be
                WHERE be.workspace_id = $1
                  AND be.event_timestamp >= NOW() - make_interval(days => $2)
            )
            SELECT
                (SELECT COUNT(*) FROM ev)                                        AS total_events,
                (SELECT COUNT(*) FROM ev WHERE peak_state = 'blocked')           AS fully_blocked_events,
                (SELECT COUNT(*) FROM ev WHERE status <> 'resolved')             AS unresolved_events,
                (SELECT COUNT(*) FROM ev WHERE risk_level IN ('high','critical')) AS high_risk_events,
                (SELECT COALESCE(AVG(dur), 0)::float FROM ev)                    AS avg_duration_s,
                (SELECT COALESCE(MAX(dur), 0)::float FROM ev)                    AS max_duration_s,
                (SELECT COALESCE(SUM(dur), 0)::float FROM ev)                    AS total_blocked_s,
                (SELECT AVG(min_accessibility_pct)::float FROM ev)               AS avg_min_accessibility_pct,
                (SELECT COUNT(*) FROM v_blocked_exit_camera_status s
                  WHERE s.workspace_id = $1)                                     AS cameras_total,
                (SELECT COUNT(*) FROM v_blocked_exit_camera_status s
                  WHERE s.workspace_id = $1 AND s.is_calibrated AND s.config_active
                    AND s.is_streaming)                                          AS cameras_monitored,
                (SELECT COUNT(*) FROM v_blocked_exit_camera_status s
                  WHERE s.workspace_id = $1 AND s.open_event_id IS NOT NULL)     AS currently_blocked
            """,
            (workspace_id, days),
            fetch_one=True,
        )
        return {"days": days, **(row or {})}

    async def most_blocked(self, workspace_id: UUID, days: int = 7, limit: int = 10) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT be.stream_id,
                   COALESCE(vs.name, MAX(be.camera_name)) AS camera_name,
                   vs.location AS camera_location,
                   COUNT(*)                                  AS event_count,
                   COUNT(*) FILTER (WHERE be.peak_state = 'blocked') AS fully_blocked_count,
                   COALESCE(SUM(COALESCE(be.duration_s,
                       EXTRACT(EPOCH FROM (NOW() - be.event_timestamp)))), 0)::float AS total_blocked_s,
                   MIN(be.min_accessibility_pct)::float      AS worst_accessibility_pct,
                   MAX(be.peak_risk_score)::float            AS peak_risk_score,
                   MAX(be.event_timestamp)                   AS last_event_at
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON vs.stream_id = be.stream_id
            WHERE be.workspace_id = $1
              AND be.event_timestamp >= NOW() - make_interval(days => $2)
            GROUP BY be.stream_id, vs.name, vs.location
            ORDER BY total_blocked_s DESC, event_count DESC
            LIMIT $3
            """,
            (workspace_id, days, limit),
            fetch_all=True,
        ) or []

    async def daily_report(self, workspace_id: UUID, report_date) -> Dict[str, Any]:
        """Per-camera rollup for one calendar day (Africa/Cairo)."""
        rows = await self.db.execute_query(
            """
            SELECT be.stream_id,
                   COALESCE(vs.name, MAX(be.camera_name)) AS camera_name,
                   COUNT(*)                                             AS event_count,
                   COUNT(*) FILTER (WHERE be.peak_state = 'blocked')   AS fully_blocked_count,
                   COALESCE(SUM(COALESCE(be.duration_s,
                       EXTRACT(EPOCH FROM (NOW() - be.event_timestamp)))), 0)::float AS total_blocked_s,
                   MAX(COALESCE(be.duration_s,
                       EXTRACT(EPOCH FROM (NOW() - be.event_timestamp))))::float     AS longest_s,
                   MIN(be.min_accessibility_pct)::float                AS worst_accessibility_pct,
                   COUNT(*) FILTER (WHERE be.status = 'resolved')      AS resolved_count
            FROM blocked_exit_events be
            LEFT JOIN video_stream vs ON vs.stream_id = be.stream_id
            WHERE be.workspace_id = $1
              AND (be.event_timestamp AT TIME ZONE 'Africa/Cairo')::date = $2
            GROUP BY be.stream_id, vs.name
            ORDER BY total_blocked_s DESC
            """,
            (workspace_id, report_date),
            fetch_all=True,
        ) or []
        return {
            "report_date": report_date,
            "rows": rows,
            "total_events": sum(r["event_count"] for r in rows),
            "total_blocked_s": sum(r["total_blocked_s"] or 0 for r in rows),
        }

    async def hourly_heatmap(self, workspace_id: UUID, days: int = 30) -> List[Dict[str, Any]]:
        """Event counts by weekday (0=Sunday) x hour of day, Africa/Cairo."""
        return await self.db.execute_query(
            """
            SELECT EXTRACT(DOW  FROM be.event_timestamp AT TIME ZONE 'Africa/Cairo')::int AS weekday,
                   EXTRACT(HOUR FROM be.event_timestamp AT TIME ZONE 'Africa/Cairo')::int AS hour,
                   COUNT(*) AS event_count
            FROM blocked_exit_events be
            WHERE be.workspace_id = $1
              AND be.event_timestamp >= NOW() - make_interval(days => $2)
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            (workspace_id, days),
            fetch_all=True,
        ) or []

    async def timeline(
        self, workspace_id: UUID, stream_id: UUID, days: int = 7, limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT be.event_id, be.event_timestamp AS started_at, be.ended_at,
                   COALESCE(be.duration_s, EXTRACT(EPOCH FROM (NOW() - be.event_timestamp)))::float AS duration_s,
                   be.peak_state, be.min_accessibility_pct::float, be.peak_risk_score::float,
                   be.risk_level, be.status
            FROM blocked_exit_events be
            WHERE be.workspace_id = $1 AND be.stream_id = $2
              AND be.event_timestamp >= NOW() - make_interval(days => $3)
            ORDER BY be.event_timestamp DESC
            LIMIT $4
            """,
            (workspace_id, stream_id, days, limit),
            fetch_all=True,
        ) or []

    # =========================================================
    # Zone / door-polygon configuration
    # =========================================================

    async def get_zone_config(
        self, stream_id: UUID, workspace_id: Optional[UUID] = None,
    ) -> Optional[Dict[str, Any]]:
        """workspace_id is required from API callers; the stream processor, which
        already owns the stream, may omit it."""
        if workspace_id is None:
            row = await self.db.execute_query(
                "SELECT * FROM exit_zone_config WHERE stream_id = $1",
                (stream_id,),
                fetch_one=True,
            )
        else:
            row = await self.db.execute_query(
                "SELECT * FROM exit_zone_config WHERE stream_id = $1 AND workspace_id = $2",
                (stream_id, workspace_id),
                fetch_one=True,
            )
        return _decode_zone_config_row(row)

    async def list_configs(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        rows = await self.db.execute_query(
            """
            SELECT cfg.*, vs.name AS camera_name
            FROM exit_zone_config cfg
            JOIN video_stream vs ON vs.stream_id = cfg.stream_id
            WHERE cfg.workspace_id = $1
            ORDER BY vs.name
            """,
            (workspace_id,),
            fetch_all=True,
        ) or []
        return [_decode_zone_config_row(r) for r in rows]

    async def set_door_polygon(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        door_polygon: List[List[float]],
        calibration_frame_w: Optional[int],
        calibration_frame_h: Optional[int],
        detector_strategy: Optional[str] = None,
        min_accessibility_pct: Optional[float] = None,
        debounce_seconds: Optional[float] = None,
        is_active: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Create or replace the door polygon. Settings left as None keep their
        current value (or the column default for a new config)."""
        row = await self.db.execute_query(
            """
            INSERT INTO exit_zone_config (
                stream_id, workspace_id, door_polygon, calibration_frame_w, calibration_frame_h,
                detector_strategy, min_accessibility_pct, debounce_seconds, is_active
            ) VALUES (
                $1, $2, $3::jsonb, $4, $5,
                COALESCE($6, 'zone_based'), COALESCE($7, 50.00), COALESCE($8, 5.00), COALESCE($9, TRUE)
            )
            ON CONFLICT (stream_id) DO UPDATE SET
                door_polygon          = EXCLUDED.door_polygon,
                calibration_frame_w   = EXCLUDED.calibration_frame_w,
                calibration_frame_h   = EXCLUDED.calibration_frame_h,
                detector_strategy     = COALESCE($6, exit_zone_config.detector_strategy),
                min_accessibility_pct = COALESCE($7, exit_zone_config.min_accessibility_pct),
                debounce_seconds      = COALESCE($8, exit_zone_config.debounce_seconds),
                is_active             = COALESCE($9, exit_zone_config.is_active),
                updated_at            = NOW()
            WHERE exit_zone_config.workspace_id = EXCLUDED.workspace_id
            RETURNING *
            """,
            (
                stream_id, workspace_id, json.dumps(door_polygon), calibration_frame_w,
                calibration_frame_h, detector_strategy, min_accessibility_pct, debounce_seconds, is_active,
            ),
            fetch_one=True,
        )
        return _decode_zone_config_row(row)

    async def update_config(
        self, *, stream_id: UUID, workspace_id: UUID, fields: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Partial update of detector settings (not the polygon)."""
        allowed = ("detector_strategy", "min_accessibility_pct", "debounce_seconds", "is_active")
        sets, params = [], [stream_id, workspace_id]
        for key in allowed:
            if key in fields and fields[key] is not None:
                params.append(fields[key])
                sets.append(f"{key} = ${len(params)}")
        if not sets:
            return await self.get_zone_config(stream_id, workspace_id)
        row = await self.db.execute_query(
            f"""
            UPDATE exit_zone_config SET {', '.join(sets)}, updated_at = NOW()
            WHERE stream_id = $1 AND workspace_id = $2
            RETURNING *
            """,
            tuple(params),
            fetch_one=True,
        )
        return _decode_zone_config_row(row)

    async def delete_config(self, *, stream_id: UUID, workspace_id: UUID) -> bool:
        affected = await self.db.execute_query(
            "DELETE FROM exit_zone_config WHERE stream_id = $1 AND workspace_id = $2",
            (stream_id, workspace_id),
            return_rowcount=True,
        )
        return bool(affected)

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
