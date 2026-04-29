import logging
from typing import List, Dict, Any, Optional
from uuid import UUID

from app.services.database import db_manager

logger = logging.getLogger(__name__)


class ShopliftingService:
    def __init__(self):
        self.db = db_manager

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
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Paginated shoplifting events with optional filters."""
        conditions = ["se.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if status:
            p += 1
            conditions.append(f"se.status = ${p}")
            params.append(status)
        if camera_id:
            p += 1
            conditions.append(f"se.stream_id = ${p}::uuid")
            params.append(camera_id)
        if start_date:
            p += 1
            conditions.append(f"se.event_timestamp >= ${p}::timestamptz")
            params.append(start_date)
        if end_date:
            p += 1
            conditions.append(f"se.event_timestamp <= ${p}::timestamptz")
            params.append(end_date)

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
                sd.video_path,
                se.resolved_at,
                se.detection_method,
                vs.name   AS camera_name,
                vs.zone,
                vs.building,
                vs.floor_level,
                vs.location AS camera_location
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT video_path
                FROM surveillance_data
                WHERE session_id = se.session_id
                  AND video_path IS NOT NULL
                ORDER BY timestamp DESC
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
    ) -> int:
        query = "SELECT COUNT(*) as total FROM shoplifting_events WHERE workspace_id = $1"
        params: list = [workspace_id]
        if status:
            query += " AND status = $2"
            params.append(status)
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
        """Update status / action on a shoplifting event."""
        query = "SELECT resolve_shoplifting_event($1, $2, $3, $4)"
        try:
            await self.db.execute_query(query, (event_id, status, action_taken, description))
            return True
        except Exception as e:
            logger.error(f"resolve_event {event_id} error: {e}")
            raise

    async def get_active_dashboard(self, workspace_id: UUID) -> List[Dict[str, Any]]:
        """Active (unresolved) events from the helper view."""
        query = """
            SELECT * FROM v_active_shoplifting_events
            WHERE workspace_id = $1
        """
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

    async def get_executive_summary(self, workspace_id: UUID) -> Dict[str, Any]:
        """
        Returns three datasets for the executive dashboard:
        - incidents_by_location  (bar chart)
        - lost_items_by_camera   (bar chart – items per camera + date)
        - trend                  (line chart – events per day)
        """
        q_location = """
            SELECT
                vs.location,
                vs.building,
                vs.zone,
                COUNT(se.event_id)        AS incident_count,
                SUM(se.estimated_value)   AS total_loss_value
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE se.workspace_id = $1
            GROUP BY vs.location, vs.building, vs.zone
            ORDER BY incident_count DESC
        """
        q_items = """
            SELECT
                vs.name   AS camera_name,
                vs.zone,
                vs.building,
                DATE(se.event_timestamp)::text AS date,
                item,
                COUNT(*)  AS item_count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id,
            UNNEST(se.items_stolen) AS item
            WHERE se.workspace_id = $1
            GROUP BY vs.name, vs.zone, vs.building, DATE(se.event_timestamp), item
            ORDER BY date DESC, item_count DESC
        """
        q_trend = """
            SELECT
                DATE(event_timestamp)::text AS date,
                COUNT(*)                   AS incident_count,
                SUM(estimated_value)        AS total_value
            FROM shoplifting_events
            WHERE workspace_id = $1
            GROUP BY DATE(event_timestamp)
            ORDER BY date ASC
        """
        try:
            by_location = await self.db.execute_query(q_location, (workspace_id,), fetch_all=True)
            lost_items  = await self.db.execute_query(q_items,    (workspace_id,), fetch_all=True)
            trend       = await self.db.execute_query(q_trend,    (workspace_id,), fetch_all=True)
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
        """
        High-confidence sessions in the last hour with camera hierarchy.
        """
        query = """
            SELECT DISTINCT ON (sd.session_id)
                sd.session_id,
                sd.behavior_state,
                sd.behavior_category,
                sd.detection_confidence,
                sd.timestamp            AS last_seen,
                vs.name                 AS camera_name,
                vs.location,
                vs.building,
                vs.floor_level,
                vs.zone
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

    async def get_hotspots(self, workspace_id: UUID) -> Dict[str, Any]:
        """
        Heatmap data: incidents per camera zone & name per calendar day.
        """
        q_heatmap = """
            SELECT
                vs.zone,
                vs.name  AS camera_name,
                TO_CHAR(DATE(se.event_timestamp), 'YYYY-MM-DD') AS day,
                COUNT(*)                                         AS incidents
            FROM shoplifting_events se
            JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE se.workspace_id = $1
            GROUP BY vs.zone, vs.name, DATE(se.event_timestamp)
            ORDER BY day DESC, incidents DESC
        """
        q_event_type = """
            SELECT
                se.event_type,
                vs.location,
                COUNT(*) AS count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE se.workspace_id = $1
              AND se.event_type IS NOT NULL
            GROUP BY se.event_type, vs.location
            ORDER BY count DESC
        """
        try:
            heatmap    = await self.db.execute_query(q_heatmap,    (workspace_id,), fetch_all=True)
            event_type = await self.db.execute_query(q_event_type, (workspace_id,), fetch_all=True)
            return {
                "heatmap":          heatmap    or [],
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
        limit: int = 10,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Returns the most recent incident video URLs from surveillance_data
        (or evidence_paths on shoplifting_events).
        """
        conditions = ["se.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1

        if camera_id:
            p += 1
            conditions.append(f"se.stream_id = ${p}::uuid")
            params.append(camera_id)
        if start_date:
            p += 1
            conditions.append(f"se.event_timestamp >= ${p}::timestamptz")
            params.append(start_date)
        if end_date:
            p += 1
            conditions.append(f"se.event_timestamp <= ${p}::timestamptz")
            params.append(end_date)

        where = " AND ".join(conditions)
        p += 1; params.append(limit)
        p += 1; params.append(offset)

        query = f"""
            SELECT
                se.event_id,
                se.event_timestamp,
                se.severity,
                se.status,
                se.evidence_paths,
                sd.video_path,
                vs.name       AS camera_name,
                vs.zone,
                vs.building,
                vs.floor_level,
                vs.location   AS camera_location
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN LATERAL (
                SELECT video_path
                FROM surveillance_data
                WHERE session_id = se.session_id
                  AND video_path IS NOT NULL
                ORDER BY timestamp DESC
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
            logger.error(f"get_incident_videos error: {e}")
            raise

    async def get_behavior_sequences(self, workspace_id: UUID) -> Dict[str, Any]:
        """Transition probabilities of behavior states."""
        q_seq = """
            SELECT
                behavior_state,
                COUNT(*) AS freq
            FROM surveillance_data
            WHERE workspace_id = $1
              AND behavior_state IS NOT NULL
            GROUP BY behavior_state
            ORDER BY freq DESC
        """
        q_shoplifting_states = """
            SELECT
                behavior_state,
                COUNT(*) AS shoplifting_count
            FROM surveillance_data
            WHERE workspace_id = $1
              AND is_shoplifting = TRUE
              AND behavior_state IS NOT NULL
            GROUP BY behavior_state
            ORDER BY shoplifting_count DESC
        """
        try:
            all_states       = await self.db.execute_query(q_seq,              (workspace_id,), fetch_all=True)
            shoplifting_states = await self.db.execute_query(q_shoplifting_states, (workspace_id,), fetch_all=True)
            return {
                "state_frequencies":      all_states         or [],
                "shoplifting_states":     shoplifting_states or [],
            }
        except Exception as e:
            logger.error(f"get_behavior_sequences error: {e}")
            raise

    # =========================================================
    # 5. Model Accuracy & Confidence Audit
    # =========================================================

    async def get_model_accuracy(self, workspace_id: UUID) -> Dict[str, Any]:
        """Confidence distribution and per-camera averages."""
        q_per_event = """
            SELECT
                se.event_id,
                se.event_timestamp,
                se.status,
                se.evidence_paths,
                AVG(sd.detection_confidence)::float AS avg_confidence,
                vs.name AS camera_name
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id
            LEFT JOIN surveillance_data sd ON sd.session_id = se.session_id
            WHERE se.workspace_id = $1
            GROUP BY se.event_id, se.event_timestamp, se.status, se.evidence_paths, vs.name
            ORDER BY se.event_timestamp DESC
            LIMIT 200
        """
        q_per_camera = """
            SELECT
                vs.name AS camera_name,
                AVG(sd.detection_confidence)::float  AS avg_confidence,
                COUNT(sd.observation_id)             AS observation_count
            FROM surveillance_data sd
            JOIN video_stream vs ON sd.stream_id = vs.stream_id
            WHERE sd.workspace_id = $1
              AND sd.detection_confidence IS NOT NULL
            GROUP BY vs.name
            ORDER BY avg_confidence DESC
        """
        try:
            per_event  = await self.db.execute_query(q_per_event,  (workspace_id,), fetch_all=True)
            per_camera = await self.db.execute_query(q_per_camera, (workspace_id,), fetch_all=True)
            return {
                "confidence_per_event":  per_event  or [],
                "average_per_camera":    per_camera or [],
            }
        except Exception as e:
            logger.error(f"get_model_accuracy error: {e}")
            raise

    # =========================================================
    # 6. Operational Efficiency & Response
    # =========================================================

    async def get_operational_efficiency(self, workspace_id: UUID) -> Dict[str, Any]:
        """Resolution times and action breakdown."""
        q_resolution = """
            SELECT
                event_id,
                event_timestamp,
                resolved_at,
                status,
                action_taken,
                EXTRACT(EPOCH FROM (resolved_at - event_timestamp)) AS resolution_seconds
            FROM shoplifting_events
            WHERE workspace_id = $1
              AND resolved_at IS NOT NULL
            ORDER BY event_timestamp DESC
        """
        q_actions = """
            SELECT
                COALESCE(action_taken, 'no_action') AS action_taken,
                COUNT(*) AS count
            FROM shoplifting_events
            WHERE workspace_id = $1
            GROUP BY action_taken
            ORDER BY count DESC
        """
        q_open = """
            SELECT
                event_id,
                event_timestamp,
                status,
                severity
            FROM shoplifting_events
            WHERE workspace_id = $1
              AND status IN ('detected', 'under_review')
            ORDER BY event_timestamp ASC
        """
        try:
            resolution = await self.db.execute_query(q_resolution, (workspace_id,), fetch_all=True)
            if resolution:
                for r in resolution:
                    if r.get("resolution_seconds") is not None:
                        r["resolution_seconds"] = float(r["resolution_seconds"])
            actions    = await self.db.execute_query(q_actions,    (workspace_id,), fetch_all=True)
            open_events= await self.db.execute_query(q_open,       (workspace_id,), fetch_all=True)
            return {
                "resolution_times": resolution  or [],
                "action_breakdown": actions     or [],
                "open_events":      open_events or [],
            }
        except Exception as e:
            logger.error(f"get_operational_efficiency error: {e}")
            raise

    # =========================================================
    # 7. High-Value Target & Item Analysis
    # =========================================================

    async def get_high_value_analysis(self, workspace_id: UUID) -> Dict[str, Any]:
        """Items stolen frequency and estimated value breakdown."""
        q_items = """
            SELECT
                item,
                vs.zone,
                vs.name AS camera_name,
                DATE(se.event_timestamp)::text AS date,
                COUNT(*) AS item_count
            FROM shoplifting_events se
            LEFT JOIN video_stream vs ON se.stream_id = vs.stream_id,
            UNNEST(se.items_stolen) AS item
            WHERE se.workspace_id = $1
            GROUP BY item, vs.zone, vs.name, DATE(se.event_timestamp)
            ORDER BY item_count DESC
        """
        q_value = """
            SELECT
                DATE(event_timestamp)::text AS date,
                severity,
                COUNT(*)                   AS incidents,
                SUM(estimated_value)        AS total_value,
                AVG(estimated_value)::float AS avg_value
            FROM shoplifting_events
            WHERE workspace_id = $1
            GROUP BY DATE(event_timestamp), severity
            ORDER BY date DESC
        """
        try:
            items  = await self.db.execute_query(q_items,  (workspace_id,), fetch_all=True)
            values = await self.db.execute_query(q_value,  (workspace_id,), fetch_all=True)
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
    ) -> List[Dict[str, Any]]:
        """Camera status, observation counts."""
        conditions = ["vs.workspace_id = $1"]
        params: list = [workspace_id]
        p = 1
        if camera_name:
            p += 1
            conditions.append(f"vs.name ILIKE ${p}")
            params.append(f"%{camera_name}%")

        where = " AND ".join(conditions)
        query = f"""
            SELECT
                vs.stream_id,
                vs.name        AS camera_name,
                vs.status,
                vs.is_streaming,
                vs.location,
                vs.building,
                vs.floor_level,
                vs.zone,
                vs.created_at  AS installed_at,
                COALESCE(obs.observation_count, 0) AS observation_count
            FROM video_stream vs
            LEFT JOIN (
                SELECT stream_id, COUNT(*) AS observation_count
                FROM surveillance_data
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

    async def get_session_insights(self, workspace_id: UUID) -> Dict[str, Any]:
        """Average behaviors per session by camera/zone/location."""
        q_avg = """
            SELECT
                vs.location,
                vs.zone,
                vs.name AS camera_name,
                COUNT(sd.observation_id)::float /
                    NULLIF(COUNT(DISTINCT sd.session_id), 0) AS avg_behavior_per_session,
                COUNT(DISTINCT sd.session_id) AS session_count
            FROM surveillance_data sd
            JOIN video_stream vs ON sd.stream_id = vs.stream_id
            WHERE sd.workspace_id = $1
            GROUP BY vs.location, vs.zone, vs.name
            ORDER BY avg_behavior_per_session DESC NULLS LAST
        """
        try:
            rows = await self.db.execute_query(q_avg, (workspace_id,), fetch_all=True)
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
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Incidents per 1 000 sessions and top-5 risk locations."""
        zone_cond = "AND vs.zone = $2" if zone else ""
        zone_params_rate = (workspace_id, zone) if zone else (workspace_id,)
        zone_params_top  = (workspace_id, zone) if zone else (workspace_id,)

        q_rate = f"""
            SELECT
                vs.location,
                vs.zone,
                COUNT(DISTINCT se.event_id)   AS incident_count,
                COUNT(DISTINCT s.session_id)  AS session_count,
                CASE
                    WHEN COUNT(DISTINCT s.session_id) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT se.event_id) * 1000.0
                        / COUNT(DISTINCT s.session_id), 2
                    )
                END::float AS incidents_per_1000_sessions
            FROM video_stream vs
            LEFT JOIN sessions         s  ON s.stream_id  = vs.stream_id
            LEFT JOIN shoplifting_events se ON se.stream_id = vs.stream_id
                                            AND se.workspace_id = $1
            WHERE vs.workspace_id = $1
              {zone_cond}
            GROUP BY vs.location, vs.zone
            ORDER BY incidents_per_1000_sessions DESC
        """
        q_top5 = f"""
            SELECT
                vs.location,
                vs.zone,
                COUNT(se.event_id)   AS incident_count,
                SUM(se.estimated_value) AS total_loss
            FROM shoplifting_events se
            JOIN video_stream vs ON se.stream_id = vs.stream_id
            WHERE se.workspace_id = $1
              {zone_cond}
            GROUP BY vs.location, vs.zone
            ORDER BY incident_count DESC
            LIMIT 5
        """
        try:
            rate = await self.db.execute_query(q_rate,  zone_params_rate, fetch_all=True)
            top5 = await self.db.execute_query(q_top5,  zone_params_top,  fetch_all=True)
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


shoplifting_service = ShopliftingService()
