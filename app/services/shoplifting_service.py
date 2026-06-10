import asyncio
import logging
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from app.services.database import db_manager
from app.utils.parser_utils import parse_date_format, parse_time_string

_TZ = ZoneInfo("Africa/Cairo")

logger = logging.getLogger(__name__)


class ShopliftingService:
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
        table_alias: str = "se",
        vs_alias: str = "vs",
    ):
        """
        Build WHERE conditions + params for shoplifting_events queries.
        Returns (conditions_list, params_list, next_p).
        Caller is responsible for adding the video_stream JOIN when location
        filters are present.
        """
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

    def _build_surveillance_conditions(
        self,
        workspace_id: UUID,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        table_alias: str = "sd",
        vs_alias: str = "vs",
    ):
        """Build WHERE conditions + params for surveillance_data queries."""
        conditions = [f"{table_alias}.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            p += 1
            conditions.append(f"{table_alias}.timestamp >= ${p}")
            params.append(datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            t = parse_time_string(start_time, dt_time.min)
            p += 1
            conditions.append(f"{table_alias}.timestamp::time >= ${p}")
            params.append(t)
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            p += 1
            conditions.append(f"{table_alias}.timestamp <= ${p}")
            params.append(datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time and not start_date:
            t = parse_time_string(end_time, dt_time(23, 59, 59))
            p += 1
            conditions.append(f"{table_alias}.timestamp::time <= ${p}")
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
    # Core CRUD
    # =========================================================

    async def get_events(
        self,
        workspace_id: UUID,
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
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        conditions, params, p = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        p += 1; params.append(limit)
        p += 1; params.append(offset)

        query = f"""
            SELECT
                se.event_id,
                se.event_timestamp,
                se.severity,
                se.event_type,
                se.status,
                se.items_stolen,
                se.estimated_value,
                se.action_taken,
                se.description,
                se.evidence_paths,
                COALESCE(se.video_path, sd.video_path) AS video_path,
                se.resolved_at,
                se.detection_method,
                vs.name        AS camera_name,
                vs.zone,
                vs.building,
                vs.floor_level,
                vs.location    AS camera_location
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT video_path FROM surveillance_data
                WHERE stream_id = se.stream_id AND video_path IS NOT NULL
                  AND ABS(EXTRACT(EPOCH FROM (timestamp - se.event_timestamp))) < 600
                ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - se.event_timestamp)))
                LIMIT 1
            ) sd ON TRUE
            WHERE {where}
            ORDER BY se.event_timestamp DESC
            LIMIT ${p-1} OFFSET ${p}
        """
        try:
            rows = await self.db.execute_query(query, tuple(params), fetch_all=True)
            return rows or []
        except Exception as e:
            logger.error(f"get_events error: {e}")
            raise

    async def count_events(
        self,
        workspace_id: UUID,
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
    ) -> int:
        conditions, params, _ = self._build_event_conditions(
            workspace_id, status=status, camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        needs_join = any([location, building, floor_level, zone])
        join_clause = "LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id" if needs_join else ""
        query = f"SELECT COUNT(*) AS total FROM shoplifting_events se {join_clause} WHERE {where}"
        try:
            result = await self.db.execute_query(query, tuple(params), fetch_one=True)
            return result.get("total", 0) if result else 0
        except Exception as e:
            logger.error(f"count_events error: {e}")
            return 0

    async def resolve_event(
        self,
        event_id: int,
        status: str,
        action_taken: Optional[str] = None,
        description: Optional[str] = None,
    ) -> bool:
        query = """
            UPDATE shoplifting_events
            SET status       = $2::varchar,
                action_taken = COALESCE($3::varchar, action_taken),
                description  = COALESCE($4::text, description),
                resolved_at  = CASE WHEN $2::varchar IN ('confirmed', 'dismissed', 'resolved')
                                    THEN NOW() ELSE resolved_at END,
                updated_at   = NOW()
            WHERE event_id = $1
        """
        try:
            await self.db.execute_query(query, (event_id, status, action_taken, description))
            return True
        except Exception as e:
            logger.error(f"resolve_event {event_id} error: {e}")
            raise

    async def get_active_dashboard(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        query = "SELECT * FROM v_active_shoplifting_events WHERE workspace_id = $1 ORDER BY event_timestamp DESC LIMIT 100"
        try:
            rows = await self.db.execute_query(query, (workspace_id,), fetch_all=True)
            return rows or []
        except Exception as e:
            logger.error(f"get_active_dashboard error: {e}")
            raise

    async def get_daily_summary(self, workspace_id: UUID, limit: int = 30) -> List[Dict[str, Any]]:
        query = """
            SELECT * FROM v_shoplifting_daily_summary
            WHERE workspace_id = $1
            ORDER BY event_date DESC
            LIMIT $2
        """
        try:
            rows = await self.db.execute_query(query, (workspace_id, limit), fetch_all=True)
            return rows or []
        except Exception as e:
            logger.error(f"get_daily_summary error: {e}")
            raise

    # =========================================================
    # 1. Executive Loss Prevention Summary
    # =========================================================

    async def get_executive_summary(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        base_params = tuple(params)

        q_location = f"""
            SELECT
                vs.location,
                COUNT(se.event_id)      AS incident_count,
                SUM(se.estimated_value) AS total_loss_value
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.location
            ORDER BY incident_count DESC
        """
        q_items = f"""
            SELECT
                vs.name AS camera_name, vs.zone, vs.building,
                DATE(se.event_timestamp)::text AS event_date,
                item, COUNT(*) AS item_count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            CROSS JOIN LATERAL UNNEST(se.items_stolen) AS item
            WHERE {where}
            GROUP BY vs.name, vs.zone, vs.building, DATE(se.event_timestamp), item
            ORDER BY event_date DESC, item_count DESC
        """
        q_trend = f"""
            SELECT
                DATE(se.event_timestamp)::text AS date,
                COUNT(*)                       AS incident_count,
                SUM(se.estimated_value)         AS total_value
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY DATE(se.event_timestamp)
            ORDER BY date ASC
        """
        try:
            by_location = await self.db.execute_query(q_location, base_params, fetch_all=True)
            lost_items  = await self.db.execute_query(q_items,    base_params, fetch_all=True)
            trend       = await self.db.execute_query(q_trend,    base_params, fetch_all=True)
            return {
                "incidents_by_location": by_location or [],
                "lost_items_by_camera":  lost_items  or [],
                "trend":                 trend        or [],
            }
        except Exception as e:
            logger.error(f"get_executive_summary error: {e}")
            raise

    # =========================================================
    # 2. Real-Time Store Activity Monitor
    # =========================================================

    async def get_realtime_activity(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        query = """
            SELECT DISTINCT ON (sd.session_id)
                sd.session_id, sd.behavior_state, sd.behavior_category,
                sd.detection_confidence,
                sd.timestamp AS last_seen,
                vs.name      AS camera_name,
                vs.location, vs.building, vs.floor_level, vs.zone
            FROM surveillance_data sd
            JOIN video_stream vs ON sd.stream_id = vs.stream_id
            WHERE sd.workspace_id = $1
              AND sd.timestamp >= NOW() - INTERVAL '1 hour'
            ORDER BY sd.session_id, sd.timestamp DESC, sd.detection_confidence DESC NULLS LAST
        """
        try:
            rows = await self.db.execute_query(query, (workspace_id,), fetch_all=True)
            if rows:
                for r in rows:
                    if r.get("detection_confidence") is not None:
                        r["detection_confidence"] = float(r["detection_confidence"])
            return rows or []
        except Exception as e:
            logger.error(f"get_realtime_activity error: {e}")
            raise

    # =========================================================
    # 3. Hotspot & Risk Mapping
    # =========================================================

    async def get_hotspots(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        et_where = where + " AND se.event_type IS NOT NULL"
        base_params = tuple(params)

        q_heatmap = f"""
            SELECT
                vs.zone, vs.name AS camera_name,
                TO_CHAR(DATE(se.event_timestamp), 'YYYY-MM-DD') AS day,
                COUNT(*) AS incidents
            FROM shoplifting_events se
            JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.zone, vs.name, DATE(se.event_timestamp)
            ORDER BY day DESC, incidents DESC
        """
        q_event_type = f"""
            SELECT se.event_type, vs.location, COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {et_where}
            GROUP BY se.event_type, vs.location
            ORDER BY count DESC
        """
        try:
            heatmap    = await self.db.execute_query(q_heatmap,    base_params, fetch_all=True)
            event_type = await self.db.execute_query(q_event_type, base_params, fetch_all=True)
            return {
                "heatmap":              heatmap    or [],
                "event_type_breakdown": event_type or [],
            }
        except Exception as e:
            logger.error(f"get_hotspots error: {e}")
            raise

    # =========================================================
    # 4. Behavioral Sequence Analysis – incident videos
    # =========================================================

    async def get_incident_videos(
        self,
        workspace_id: UUID,
        camera_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> Dict[str, Any]:
        conditions, params, p = self._build_event_conditions(
            workspace_id, camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        filter_params = tuple(params)
        p += 1; params.append(limit)
        p += 1; params.append(offset)

        query = f"""
            SELECT
                se.event_id, se.event_timestamp, se.severity, se.status,
                se.evidence_paths, COALESCE(se.video_path, sd.video_path) AS video_path,
                vs.name       AS camera_name,
                vs.zone, vs.building, vs.floor_level,
                vs.location   AS camera_location
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT video_path FROM surveillance_data
                WHERE stream_id = se.stream_id AND video_path IS NOT NULL
                  AND ABS(EXTRACT(EPOCH FROM (timestamp - se.event_timestamp))) < 600
                ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - se.event_timestamp)))
                LIMIT 1
            ) sd ON TRUE
            WHERE {where}
            ORDER BY se.event_timestamp DESC
            LIMIT ${p-1} OFFSET ${p}
        """
        q_count = f"""
            SELECT COUNT(*) AS total
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
        """
        try:
            rows = await self.db.execute_query(query, tuple(params), fetch_all=True)
            count_row = await self.db.execute_query(q_count, filter_params, fetch_one=True)
            return {"items": rows or [], "total": count_row["total"] if count_row else 0}
        except Exception as e:
            logger.error(f"get_incident_videos error: {e}")
            raise

    async def get_behavior_sequences(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_surveillance_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        needs_join = any([location, building, floor_level, zone])
        join_clause = "JOIN video_stream vs ON sd.stream_id = vs.stream_id" if needs_join else ""
        where = " AND ".join(conditions)
        sl_where = where + " AND sd.is_shoplifting = TRUE"
        base_params = tuple(params)

        q_seq = f"""
            SELECT sd.behavior_state, COUNT(*) AS freq
            FROM surveillance_data sd
            {join_clause}
            WHERE {where}
              AND sd.behavior_state IS NOT NULL
            GROUP BY sd.behavior_state
            ORDER BY freq DESC
        """
        q_shoplifting_states = f"""
            SELECT sd.behavior_state, COUNT(*) AS shoplifting_count
            FROM surveillance_data sd
            {join_clause}
            WHERE {sl_where}
              AND sd.behavior_state IS NOT NULL
            GROUP BY sd.behavior_state
            ORDER BY shoplifting_count DESC
        """
        try:
            all_states         = await self.db.execute_query(q_seq,               base_params, fetch_all=True)
            shoplifting_states = await self.db.execute_query(q_shoplifting_states, base_params, fetch_all=True)
            return {
                "state_frequencies":  all_states         or [],
                "shoplifting_states": shoplifting_states or [],
            }
        except Exception as e:
            logger.error(f"get_behavior_sequences error: {e}")
            raise

    # =========================================================
    # 5. Model Accuracy & Confidence Audit
    # =========================================================

    async def get_model_accuracy(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        # per-event: filter on shoplifting_events timestamp + location via video_stream
        ev_conditions, ev_params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        ev_where = " AND ".join(ev_conditions)

        q_per_event = f"""
            SELECT
                se.event_id,
                TO_CHAR(se.event_timestamp AT TIME ZONE 'UTC', 'MM/DD HH24:MI') AS event_label,
                se.event_timestamp, se.status, se.evidence_paths,
                COALESCE(sd.avg_confidence, 0.0)::float AS avg_confidence,
                vs.name AS camera_name
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT AVG(detection_confidence)::float AS avg_confidence
                FROM surveillance_data
                WHERE stream_id = se.stream_id
                  AND detection_confidence IS NOT NULL
                  AND ABS(EXTRACT(EPOCH FROM (timestamp - se.event_timestamp))) < 600
            ) sd ON TRUE
            WHERE {ev_where}
            ORDER BY se.event_timestamp DESC
            LIMIT 200
        """

        # per-camera: filter on surveillance_data timestamp + location
        cam_conditions, cam_params, _ = self._build_surveillance_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        cam_conditions.append("sd.detection_confidence IS NOT NULL")
        cam_where = " AND ".join(cam_conditions)

        q_per_camera = f"""
            SELECT
                vs.name AS camera_name,
                AVG(sd.detection_confidence)::float AS avg_confidence,
                COUNT(sd.observation_id)            AS observation_count
            FROM surveillance_data sd
            JOIN video_stream vs ON sd.stream_id = vs.stream_id
            WHERE {cam_where}
            GROUP BY vs.name
            ORDER BY avg_confidence DESC
        """
        try:
            per_event  = await self.db.execute_query(q_per_event,  tuple(ev_params),  fetch_all=True)
            per_camera = await self.db.execute_query(q_per_camera, tuple(cam_params), fetch_all=True)
            return {
                "confidence_per_event": per_event  or [],
                "average_per_camera":   per_camera or [],
            }
        except Exception as e:
            logger.error(f"get_model_accuracy error: {e}")
            raise

    # =========================================================
    # 6. Operational Efficiency & Response
    # =========================================================

    async def get_operational_efficiency(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        needs_join = any([location, building, floor_level, zone])
        join_clause = "LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id" if needs_join else ""
        where = " AND ".join(conditions)
        base_params = tuple(params)

        q_resolution = f"""
            SELECT
                se.event_id, se.event_timestamp, se.resolved_at,
                se.status, se.action_taken,
                EXTRACT(EPOCH FROM (se.resolved_at - se.event_timestamp)) AS resolution_seconds
            FROM shoplifting_events se
            {join_clause}
            WHERE {where}
              AND se.resolved_at IS NOT NULL
            ORDER BY se.event_timestamp DESC
            LIMIT 500
        """
        q_actions = f"""
            SELECT
                COALESCE(se.action_taken, 'no_action') AS action_taken,
                COUNT(*) AS count
            FROM shoplifting_events se
            {join_clause}
            WHERE {where}
            GROUP BY se.action_taken
            ORDER BY count DESC
        """
        q_open = f"""
            SELECT se.event_id, se.event_timestamp, se.status, se.severity
            FROM shoplifting_events se
            {join_clause}
            WHERE {where}
              AND se.status IN ('detected', 'under_review')
            ORDER BY se.event_timestamp ASC
            LIMIT 200
        """
        try:
            resolution, actions, open_events = await asyncio.gather(
                self.db.execute_query(q_resolution, base_params, fetch_all=True),
                self.db.execute_query(q_actions,    base_params, fetch_all=True),
                self.db.execute_query(q_open,       base_params, fetch_all=True),
            )
            if resolution:
                for r in resolution:
                    if r.get("resolution_seconds") is not None:
                        r["resolution_seconds"] = float(r["resolution_seconds"])
            return {
                "resolution_times": resolution  or [],
                "action_breakdown": actions     or [],
                "open_events":      open_events or [],
            }
        except Exception as e:
            logger.error(f"get_operational_efficiency error: {e}")
            raise

    # =========================================================
    # action_taken Analytics
    # =========================================================

    async def get_action_summary(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> list:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        q = f"""
            SELECT COALESCE(se.action_taken, 'no_action') AS action_taken,
                   COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY se.action_taken
            ORDER BY count DESC
        """
        try:
            return await self.db.execute_query(q, tuple(params), fetch_all=True) or []
        except Exception as e:
            logger.error(f"get_action_summary error: {e}")
            raise

    async def get_action_by_camera(
        self,
        workspace_id: UUID,
        camera_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> list:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        q = f"""
            SELECT vs.name AS camera_name,
                   se.stream_id::text AS camera_id,
                   COALESCE(se.action_taken, 'no_action') AS action_taken,
                   COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.name, se.stream_id, se.action_taken
            ORDER BY vs.name, count DESC
        """
        try:
            return await self.db.execute_query(q, tuple(params), fetch_all=True) or []
        except Exception as e:
            logger.error(f"get_action_by_camera error: {e}")
            raise

    async def get_action_by_date(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> list:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        q = f"""
            SELECT DATE(se.event_timestamp AT TIME ZONE 'UTC') AS event_date,
                   COALESCE(se.action_taken, 'no_action') AS action_taken,
                   COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY event_date, se.action_taken
            ORDER BY event_date, count DESC
        """
        try:
            return await self.db.execute_query(q, tuple(params), fetch_all=True) or []
        except Exception as e:
            logger.error(f"get_action_by_date error: {e}")
            raise

    async def get_action_by_location(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> list:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        q = f"""
            SELECT vs.location,
                   vs.zone,
                   COALESCE(se.action_taken, 'no_action') AS action_taken,
                   COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.location, vs.zone, se.action_taken
            ORDER BY vs.location, vs.zone, count DESC
        """
        try:
            return await self.db.execute_query(q, tuple(params), fetch_all=True) or []
        except Exception as e:
            logger.error(f"get_action_by_location error: {e}")
            raise

    async def get_action_by_camera_date(
        self,
        workspace_id: UUID,
        camera_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> list:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        q = f"""
            SELECT vs.name AS camera_name,
                   se.stream_id::text AS camera_id,
                   DATE(se.event_timestamp AT TIME ZONE 'UTC') AS event_date,
                   COALESCE(se.action_taken, 'no_action') AS action_taken,
                   COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.name, se.stream_id, event_date, se.action_taken
            ORDER BY vs.name, event_date, count DESC
        """
        try:
            return await self.db.execute_query(q, tuple(params), fetch_all=True) or []
        except Exception as e:
            logger.error(f"get_action_by_camera_date error: {e}")
            raise

    # =========================================================
    # 7. High-Value Target & Item Analysis
    # =========================================================

    async def get_high_value_analysis(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)
        base_params = tuple(params)

        q_items = f"""
            SELECT
                COALESCE(i.item, COALESCE(vs.zone, 'Unknown Zone')) AS item,
                vs.zone, vs.name AS camera_name,
                DATE(se.event_timestamp)::text AS date,
                COUNT(*) AS item_count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT UNNEST(se.items_stolen) AS item
                WHERE se.items_stolen IS NOT NULL AND cardinality(se.items_stolen) > 0
            ) i ON TRUE
            WHERE {where}
            GROUP BY COALESCE(i.item, COALESCE(vs.zone, 'Unknown Zone')), vs.zone, vs.name, DATE(se.event_timestamp)
            ORDER BY item_count DESC
        """
        q_value = f"""
            SELECT
                DATE(se.event_timestamp)::text AS date,
                se.severity,
                COUNT(*) AS incidents,
                COALESCE(
                    SUM(se.estimated_value),
                    SUM(CASE se.severity
                        WHEN 'critical' THEN 1000 WHEN 'high' THEN 500
                        WHEN 'medium' THEN 200 ELSE 100
                    END)
                ) AS total_value,
                COALESCE(
                    AVG(se.estimated_value)::float,
                    AVG(CASE se.severity
                        WHEN 'critical' THEN 1000.0 WHEN 'high' THEN 500.0
                        WHEN 'medium' THEN 200.0 ELSE 100.0
                    END)::float
                ) AS avg_value
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY DATE(se.event_timestamp), se.severity
            ORDER BY date DESC
        """
        try:
            items  = await self.db.execute_query(q_items,  base_params, fetch_all=True)
            values = await self.db.execute_query(q_value,  base_params, fetch_all=True)
            return {
                "items_stolen":          items  or [],
                "total_estimated_value": values or [],
            }
        except Exception as e:
            logger.error(f"get_high_value_analysis error: {e}")
            raise

    # =========================================================
    # 8. Camera Health & Coverage
    # =========================================================

    async def get_camera_health(
        self,
        workspace_id: UUID,
        camera_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        conditions = ["vs.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if camera_name:
            p += 1
            conditions.append(f"vs.name ILIKE ${p}")
            params.append(f"%{camera_name}%")
        if location:
            p += 1
            conditions.append(f"vs.location = ${p}")
            params.append(location)
        if building:
            p += 1
            conditions.append(f"vs.building = ${p}")
            params.append(building)
        if floor_level:
            p += 1
            conditions.append(f"vs.floor_level = ${p}")
            params.append(floor_level)
        if zone:
            p += 1
            conditions.append(f"vs.zone = ${p}")
            params.append(zone)

        # Date/time filters narrow the observation_count sub-query
        obs_conditions = []
        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            p += 1
            obs_conditions.append(f"timestamp >= ${p}")
            params.append(datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            t = parse_time_string(start_time, dt_time.min)
            p += 1
            obs_conditions.append(f"timestamp::time >= ${p}")
            params.append(t)
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            p += 1
            obs_conditions.append(f"timestamp <= ${p}")
            params.append(datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time and not start_date:
            t = parse_time_string(end_time, dt_time(23, 59, 59))
            p += 1
            obs_conditions.append(f"timestamp::time <= ${p}")
            params.append(t)

        obs_where = ("WHERE " + " AND ".join(obs_conditions)) if obs_conditions else ""
        where = " AND ".join(conditions)

        query = f"""
            SELECT
                vs.stream_id::text AS stream_id,
                vs.name        AS camera_name,
                vs.status,
                vs.is_streaming,
                vs.location,
                vs.building,
                vs.floor_level,
                vs.zone,
                vs.created_at::text AS installed_at,
                COALESCE(obs.observation_count, 0) AS observation_count
            FROM video_stream vs
            LEFT JOIN (
                SELECT stream_id, COUNT(*) AS observation_count
                FROM surveillance_data
                {obs_where}
                GROUP BY stream_id
            ) obs ON obs.stream_id = vs.stream_id
            WHERE {where}
            ORDER BY vs.name
        """
        try:
            rows = await self.db.execute_query(query, tuple(params), fetch_all=True)
            return rows or []
        except Exception as e:
            logger.error(f"get_camera_health error: {e}")
            raise

    # =========================================================
    # 9. Store Traffic & Session Insights
    # =========================================================

    async def get_session_insights(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        conditions, params, _ = self._build_surveillance_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)

        q_avg = f"""
            SELECT
                vs.location, vs.zone, vs.name AS camera_name,
                COUNT(sd.observation_id)::float /
                    NULLIF(SUM(vs.session_count), 0) AS avg_behavior_per_session,
                COALESCE(SUM(vs.session_count), 0) AS session_count
            FROM surveillance_data sd
            JOIN video_stream vs ON sd.stream_id = vs.stream_id
            WHERE {where}
            GROUP BY vs.location, vs.zone, vs.name
            ORDER BY avg_behavior_per_session DESC NULLS LAST
        """
        try:
            rows = await self.db.execute_query(q_avg, tuple(params), fetch_all=True)
            if rows:
                for r in rows:
                    v = r.get("avg_behavior_per_session")
                    r["avg_behavior_per_session"] = float(v) if v is not None else 0.0
            return {"avg_behavior_per_session": rows or []}
        except Exception as e:
            logger.error(f"get_session_insights error: {e}")
            raise

    # =========================================================
    # 10. Regional Risk Benchmark
    # =========================================================

    async def get_regional_risk(
        self,
        workspace_id: UUID,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Filters on video_stream (base table)
        vs_conditions = ["vs.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if zone:
            p += 1; vs_conditions.append(f"vs.zone = ${p}"); params.append(zone)
        if location:
            p += 1; vs_conditions.append(f"vs.location = ${p}"); params.append(location)
        if building:
            p += 1; vs_conditions.append(f"vs.building = ${p}"); params.append(building)
        if floor_level:
            p += 1; vs_conditions.append(f"vs.floor_level = ${p}"); params.append(floor_level)

        # Date/time filters for events and sessions via inline JOIN conditions
        event_extra = ""
        session_extra = ""
        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            p += 1
            event_extra   += f" AND se.event_timestamp >= ${p}"
            session_extra += f" AND s.timestamp >= ${p}"
            params.append(datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            t = parse_time_string(start_time, dt_time.min)
            p += 1
            event_extra   += f" AND se.event_timestamp::time >= ${p}"
            session_extra += f" AND s.timestamp::time >= ${p}"
            params.append(t)
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            p += 1
            event_extra   += f" AND se.event_timestamp <= ${p}"
            session_extra += f" AND s.timestamp <= ${p}"
            params.append(datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time and not start_date:
            t = parse_time_string(end_time, dt_time(23, 59, 59))
            p += 1
            event_extra   += f" AND se.event_timestamp::time <= ${p}"
            session_extra += f" AND s.timestamp::time <= ${p}"
            params.append(t)

        vs_where = " AND ".join(vs_conditions)
        base_params = tuple(params)

        q_rate = f"""
            SELECT
                vs.location, vs.zone,
                COUNT(DISTINCT se.event_id)      AS incident_count,
                COALESCE(SUM(vs.session_count), 0) AS session_count,
                CASE
                    WHEN SUM(vs.session_count) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT se.event_id) * 1000.0
                        / NULLIF(SUM(vs.session_count), 0), 2
                    )
                END::float AS incidents_per_1000_sessions
            FROM video_stream vs
            LEFT JOIN shoplifting_events se
                ON se.stream_id = vs.stream_id AND se.workspace_id = $1{event_extra}
            WHERE {vs_where}
            GROUP BY vs.location, vs.zone
            ORDER BY incidents_per_1000_sessions DESC
        """

        # top-5 uses a simpler join on shoplifting_events
        se_conditions, se_params, _ = self._build_event_conditions(
            workspace_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        se_where = " AND ".join(se_conditions)
        q_top5 = f"""
            SELECT
                vs.location, vs.zone,
                COUNT(se.event_id)      AS incident_count,
                SUM(se.estimated_value) AS total_loss
            FROM shoplifting_events se
            JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE {se_where}
            GROUP BY vs.location, vs.zone
            ORDER BY incident_count DESC
            LIMIT 5
        """
        try:
            rate = await self.db.execute_query(q_rate,  base_params,       fetch_all=True)
            top5 = await self.db.execute_query(q_top5,  tuple(se_params),  fetch_all=True)
            return {
                "incidents_per_1000_sessions": rate or [],
                "top_risk_areas":              top5 or [],
            }
        except Exception as e:
            logger.error(f"get_regional_risk error: {e}")
            raise

    # =========================================================
    # Insert helpers (used by stream pipeline)
    # =========================================================

    async def insert_shoplifting_event(
        self,
        stream_id: UUID,
        workspace_id: UUID,
        confidence: Optional[float] = None,
        video_path: Optional[str] = None,
    ) -> Optional[int]:
        """
        Directly insert a detected shoplifting event into shoplifting_events.
        Used as a fallback alongside the DB trigger so the incidents endpoint
        always returns results.
        Skips insert if an 'detected' event for this stream already exists
        within the last 5 minutes (mirrors the trigger's dedup logic).
        """
        try:
            desc = f"Auto-created by stream pipeline. confidence={confidence:.2f}" if confidence else "Auto-created by stream pipeline."
            # Try to insert a new event
            insert_query = """
                INSERT INTO shoplifting_events (
                    stream_id, workspace_id,
                    event_timestamp, detection_method, severity, status, description, video_path
                )
                SELECT $1, $2, NOW(), 'ml_model', 'medium', 'detected', $3, $4
                WHERE NOT EXISTS (
                    SELECT 1 FROM shoplifting_events
                    WHERE stream_id = $1
                      AND workspace_id = $2
                      AND status = 'detected'
                      AND event_timestamp > NOW() - INTERVAL '5 minutes'
                )
                RETURNING event_id
            """
            result = await self.db.execute_query(
                insert_query, (stream_id, workspace_id, desc, video_path), fetch_one=True
            )
            if result:
                return result.get("event_id")
            # Deduped — update video_path on the existing event if it's still null
            if video_path:
                await self.db.execute_query(
                    """
                    UPDATE shoplifting_events
                    SET video_path = $3, updated_at = NOW()
                    WHERE event_id = (
                        SELECT event_id FROM shoplifting_events
                        WHERE stream_id = $1
                          AND workspace_id = $2
                          AND status = 'detected'
                          AND event_timestamp > NOW() - INTERVAL '5 minutes'
                        ORDER BY event_timestamp DESC
                        LIMIT 1
                    ) AND video_path IS NULL
                    """,
                    (stream_id, workspace_id, video_path),
                    fetch_one=False,
                )
            return None
        except Exception as e:
            logger.error(f"insert_shoplifting_event error: {e}")
            return None

    async def insert_surveillance_frame(
        self,
        session_id: UUID,
        stream_id: UUID,
        workspace_id: UUID,
        user_id: UUID,
        is_shoplifting: bool,
        behavior_state: Optional[str] = None,
        behavior_category: Optional[str] = None,
        confidence: Optional[float] = None,
        objects_detected: Optional[List[str]] = None,
        image_path: Optional[str] = None,
        video_path: Optional[str] = None,
    ) -> Optional[int]:
        query = """
            INSERT INTO surveillance_data (
                session_id, stream_id, workspace_id, user_id,
                is_shoplifting, behavior_state, behavior_category,
                detection_confidence, objects_detected, image_path, video_path
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            RETURNING observation_id
        """
        try:
            result = await self.db.execute_query(
                query,
                (session_id, stream_id, workspace_id, user_id,
                 is_shoplifting, behavior_state, behavior_category,
                 confidence, objects_detected, image_path, video_path),
                fetch_one=True,
            )
            return result.get("observation_id") if result else None
        except Exception as e:
            logger.error(f"insert_surveillance_frame error: {e}")
            return None


    # =========================================================
    # Delete Events (videos + DB rows)
    # =========================================================

    async def delete_events(
        self,
        workspace_id: UUID,
        *,
        camera_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        from app.services.s3_service import s3_service

        needs_join = any([location, building, floor_level, zone])
        join_clause = "LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id" if needs_join else ""

        conditions, params, _ = self._build_event_conditions(
            workspace_id,
            camera_id=camera_id,
            start_date=start_date, end_date=end_date,
            start_time=start_time, end_time=end_time,
            location=location, building=building,
            floor_level=floor_level, zone=zone,
        )
        where = " AND ".join(conditions)

        try:
            # Step 1: Fetch matching events – collect event_ids, session_ids, evidence_paths
            fetch_events_query = f"""
                SELECT se.event_id, se.session_id, se.evidence_paths
                FROM shoplifting_events se
                {join_clause}
                WHERE {where}
            """
            rows = await self.db.execute_query(fetch_events_query, tuple(params), fetch_all=True) or []

            if not rows:
                return {"status": "success", "deleted_events": 0, "deleted_s3_files": 0}

            event_ids = [row["event_id"] for row in rows]
            session_ids = list({row["session_id"] for row in rows if row.get("session_id")})

            # Collect evidence_paths (images stored on shoplifting_events)
            s3_paths: List[str] = []
            for row in rows:
                if row.get("evidence_paths"):
                    s3_paths.extend(p for p in row["evidence_paths"] if p)

            # Step 2: Collect ALL image_path + video_path from surveillance_data for those sessions
            if session_ids:
                sd_rows = await self.db.execute_query(
                    """SELECT image_path, video_path FROM surveillance_data
                       WHERE session_id = ANY($1) AND workspace_id = $2""",
                    (session_ids, workspace_id),
                    fetch_all=True,
                ) or []
                for row in sd_rows:
                    if row.get("image_path"):
                        s3_paths.append(row["image_path"])
                    if row.get("video_path"):
                        s3_paths.append(row["video_path"])

                # Delete surveillance_data rows from PostgreSQL
                await self.db.execute_query(
                    "DELETE FROM surveillance_data WHERE session_id = ANY($1) AND workspace_id = $2",
                    (session_ids, workspace_id),
                )

            # Step 3: Delete shoplifting_events rows (by id – avoids invalid alias/JOIN DELETE syntax)
            await self.db.execute_query(
                "DELETE FROM shoplifting_events WHERE event_id = ANY($1)",
                (event_ids,),
            )

            # Step 4: Delete unique S3 files
            unique_paths = list({p for p in s3_paths if p and isinstance(p, str) and p.startswith("s3://")})
            s3_deleted = await s3_service.delete_files(unique_paths)

            return {
                "status": "success",
                "deleted_events": len(event_ids),
                "deleted_s3_files": s3_deleted,
            }
        except Exception as e:
            logger.error(f"delete_events error: {e}")
            raise

    async def delete_all_events(self, workspace_id: UUID) -> Dict[str, Any]:
        from app.services.s3_service import s3_service

        try:
            # Step 1: Fetch all events for the workspace
            se_rows = await self.db.execute_query(
                "SELECT session_id, evidence_paths FROM shoplifting_events WHERE workspace_id = $1",
                (workspace_id,),
                fetch_all=True,
            ) or []

            if not se_rows:
                return {"status": "success", "deleted_events": 0, "deleted_s3_files": 0}

            deleted_count = len(se_rows)
            session_ids = list({row["session_id"] for row in se_rows if row.get("session_id")})

            # Collect evidence_paths (images on shoplifting_events)
            s3_paths: List[str] = []
            for row in se_rows:
                if row.get("evidence_paths"):
                    s3_paths.extend(p for p in row["evidence_paths"] if p)

            # Step 2: Collect ALL image_path + video_path from surveillance_data
            if session_ids:
                sd_rows = await self.db.execute_query(
                    """SELECT image_path, video_path FROM surveillance_data
                       WHERE session_id = ANY($1) AND workspace_id = $2""",
                    (session_ids, workspace_id),
                    fetch_all=True,
                ) or []
                for row in sd_rows:
                    if row.get("image_path"):
                        s3_paths.append(row["image_path"])
                    if row.get("video_path"):
                        s3_paths.append(row["video_path"])

                # Delete surveillance_data rows from PostgreSQL
                await self.db.execute_query(
                    "DELETE FROM surveillance_data WHERE session_id = ANY($1) AND workspace_id = $2",
                    (session_ids, workspace_id),
                )

            # Step 3: Delete shoplifting_events
            await self.db.execute_query(
                "DELETE FROM shoplifting_events WHERE workspace_id = $1",
                (workspace_id,),
            )

            # Step 4: Delete unique S3 files
            unique_paths = list({p for p in s3_paths if p and isinstance(p, str) and p.startswith("s3://")})
            s3_deleted = await s3_service.delete_files(unique_paths)

            return {
                "status": "success",
                "deleted_events": deleted_count,
                "deleted_s3_files": s3_deleted,
            }
        except Exception as e:
            logger.error(f"delete_all_events error: {e}")
            raise


shoplifting_service = ShopliftingService()
