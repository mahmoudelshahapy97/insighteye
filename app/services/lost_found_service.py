"""Persistence for lost & found: camera config, abandoned events, the found gallery,
lost reports, rule-based matching and the analytics page.

Every query is scoped by workspace. Images and clips live in S3 (``*_path`` columns
hold ``s3://`` URIs); routers turn them into presigned URLs.
"""

import base64
import json
import logging
from datetime import datetime, time as dt_time, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

import numpy as np

from app.config.settings import config
from app.services import evidence_analytics
from app.services.database import db_manager
from app.services.lost_found import visual
from app.services.lost_found.color import color_from_text, dominant_color
from app.services.lost_found.imaging import decode_jpeg, encode_jpeg
from app.services.lost_found.matching import collapse_repeat_sightings, score_candidate
from app.utils.parser_utils import parse_date_format, parse_time_string
from zoneinfo import ZoneInfo

_TZ_NAME = "Africa/Cairo"
_TZ = ZoneInfo(_TZ_NAME)

logger = logging.getLogger(__name__)

EVENT_STATUSES = ("open", "acknowledged", "resolved", "false_positive")
FOUND_STATUSES = ("unclaimed", "claimed", "discarded")
LOST_STATUSES = ("open", "matched", "resolved", "closed")

# Enough to cover a busy day of detections for one report; the rerank and the
# repeat-sighting collapse run in Python over these.
_MAX_CANDIDATES = 500


def _json(value: Any) -> Any:
    """JSONB comes back from asyncpg as text (no codec is registered)."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _decode(row: Optional[Dict[str, Any]], *columns: str) -> Optional[Dict[str, Any]]:
    if not row:
        return row
    row = dict(row)
    for column in columns:
        if column in row:
            row[column] = _json(row[column])
    return row


def _epoch(value: Optional[float]) -> Optional[datetime]:
    return datetime.fromtimestamp(value, tz=timezone.utc) if value else None


def s3_key(workspace_id: UUID, stream_id: Optional[Any], kind: str, object_id: Any, ext: str) -> str:
    """``lost-found/<workspace>/<camera>/<kind>/<id>.<ext>``; lost-report photos have no camera."""
    camera = str(stream_id) if stream_id else "unassigned"
    return f"lost-found/{workspace_id}/{camera}/{kind}/{object_id}.{ext}"


def lost_photo_key(workspace_id: UUID, lost_id: Any) -> str:
    return f"lost-found/{workspace_id}/lost/{lost_id}.jpg"


async def upload_jpeg(jpeg: Optional[bytes], key: str) -> Optional[str]:
    if not jpeg:
        return None
    from app.services.s3_service import s3_service
    try:
        return await s3_service.upload_image_base64_to_s3(base64.b64encode(jpeg).decode("ascii"), key) or None
    except Exception as e:
        logger.warning(f"[lost-found] upload of {key} failed: {e}")
        return None


class LostFoundService:
    def __init__(self) -> None:
        self.db = db_manager

    # =========================================================
    # Filters
    # =========================================================

    def _conditions(
        self,
        workspace_id: UUID,
        *,
        alias: str,
        time_column: str,
        status: Optional[str] = None,
        camera_id: Optional[str] = None,
        class_name: Optional[str] = None,
        color: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        location: Optional[str] = None,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        vs_alias: str = "vs",
    ) -> Tuple[List[str], list]:
        """WHERE parts shared by events and found items. Dates are the platform's
        ``start_date``/``start_time`` pickers (Cairo time) or exact ``start``/``end``."""
        conditions = [f"{alias}.workspace_id = $1"]
        params: list = [workspace_id]

        def add(sql: str, value: Any) -> None:
            params.append(value)
            conditions.append(sql.replace("?", f"${len(params)}"))

        if status:
            add(f"{alias}.status = ?", status)
        if camera_id:
            add(f"{alias}.stream_id = ?::uuid", str(camera_id))
        if class_name:
            add(f"{alias}.class_name = ?", class_name)
        if color:
            add(f"{alias}.dominant_color = ?", color)
        if start:
            add(f"{alias}.{time_column} >= ?", start)
        if end:
            add(f"{alias}.{time_column} <= ?", end)
        local_time = f"({alias}.{time_column} AT TIME ZONE '{_TZ_NAME}')::time"
        if start_date:
            sd = parse_date_format(start_date)
            st = parse_time_string(start_time, dt_time.min) if start_time else dt_time.min
            add(f"{alias}.{time_column} >= ?", datetime.combine(sd, st).replace(tzinfo=_TZ))
        elif start_time:
            add(f"{local_time} >= ?", parse_time_string(start_time, dt_time.min))
        if end_date:
            ed = parse_date_format(end_date)
            et = parse_time_string(end_time, dt_time(23, 59, 59)) if end_time else dt_time(23, 59, 59)
            add(f"{alias}.{time_column} <= ?", datetime.combine(ed, et).replace(tzinfo=_TZ))
        elif end_time:
            add(f"{local_time} <= ?", parse_time_string(end_time, dt_time(23, 59, 59)))
        for column, value in (("location", location), ("area", area), ("building", building),
                              ("floor_level", floor_level), ("zone", zone)):
            values = evidence_analytics.csv_values(value) if value else []
            if values:
                add(f"{vs_alias}.{column} = ANY(?::text[])", values)
        return conditions, params

    # =========================================================
    # Cameras & config
    # =========================================================

    async def get_camera(self, workspace_id: UUID, stream_id: UUID) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            """
            SELECT stream_id, workspace_id, name, location, area, building, floor_level, zone,
                   status, is_streaming, is_lost_found_camera
            FROM video_stream
            WHERE stream_id = $1 AND workspace_id = $2
            """,
            (stream_id, workspace_id),
            fetch_one=True,
        )

    async def get_camera_source(self, stream_id: UUID, workspace_id: UUID) -> Optional[Dict[str, Any]]:
        """Internal only: the camera's source path, which may embed credentials.
        Callers must never return it to a client."""
        return await self.db.execute_query(
            "SELECT stream_id, name, path, type FROM video_stream WHERE stream_id = $1 AND workspace_id = $2",
            (stream_id, workspace_id),
            fetch_one=True,
        )

    async def is_camera_enabled(self, stream_id: UUID) -> bool:
        row = await self.db.execute_query(
            "SELECT is_lost_found_camera FROM video_stream WHERE stream_id = $1",
            (stream_id,),
            fetch_one=True,
        )
        return bool(row and row.get("is_lost_found_camera"))

    async def list_cameras(self, workspace_id: UUID, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """Workspace cameras with their lost & found config and event counts."""
        where = "vs.workspace_id = $1" + (" AND vs.is_lost_found_camera" if enabled_only else "")
        rows = await self.db.execute_query(
            f"""
            SELECT vs.stream_id, vs.name AS camera_name, vs.location AS camera_location,
                   vs.area AS camera_area, vs.building, vs.floor_level, vs.zone,
                   vs.status AS stream_status, vs.is_streaming, vs.is_lost_found_camera,
                   cfg.roi, cfg.ref_width, cfg.ref_height, cfg.settings,
                   COALESCE(cfg.is_active, TRUE) AS config_active, cfg.updated_at AS config_updated_at,
                   (SELECT COUNT(*) FROM lost_found_events e
                     WHERE e.stream_id = vs.stream_id AND e.status = 'open')  AS open_events,
                   (SELECT MAX(e.event_timestamp) FROM lost_found_events e
                     WHERE e.stream_id = vs.stream_id)                         AS last_event_at
            FROM video_stream vs
            LEFT JOIN lost_found_camera_config cfg ON cfg.stream_id = vs.stream_id
            WHERE {where}
            ORDER BY vs.is_lost_found_camera DESC, vs.name
            """,
            (workspace_id,),
            fetch_all=True,
        ) or []
        return [_decode(r, "roi", "settings") for r in rows]

    async def get_config(self, stream_id: UUID, workspace_id: Optional[UUID] = None) -> Optional[Dict[str, Any]]:
        params: tuple = (stream_id,) if workspace_id is None else (stream_id, workspace_id)
        row = await self.db.execute_query(
            "SELECT stream_id, workspace_id, roi, ref_width, ref_height, settings, is_active, updated_at "
            "FROM lost_found_camera_config WHERE stream_id = $1"
            + ("" if workspace_id is None else " AND workspace_id = $2"),
            params,
            fetch_one=True,
        )
        return _decode(row, "roi", "settings")

    async def upsert_config(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        roi: Optional[List[List[float]]],
        ref_width: Optional[int],
        ref_height: Optional[int],
        settings: Optional[Dict[str, Any]],
        is_active: Optional[bool],
        replace_roi: bool = True,
        replace_settings: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Create or update a camera's config. ``replace_roi`` / ``replace_settings``
        false keeps the stored value, so the ROI editor and the threshold form can save
        independently."""
        row = await self.db.execute_query(
            """
            INSERT INTO lost_found_camera_config
                (stream_id, workspace_id, roi, ref_width, ref_height, settings, is_active)
            SELECT vs.stream_id, vs.workspace_id, $3::jsonb, $4::int, $5::int, COALESCE($6::jsonb, '{}'::jsonb),
                   COALESCE($7::boolean, TRUE)
            FROM video_stream vs WHERE vs.stream_id = $1 AND vs.workspace_id = $2
            ON CONFLICT (stream_id) DO UPDATE SET
                roi        = CASE WHEN $8::boolean THEN EXCLUDED.roi        ELSE lost_found_camera_config.roi END,
                ref_width  = CASE WHEN $8 THEN EXCLUDED.ref_width  ELSE lost_found_camera_config.ref_width END,
                ref_height = CASE WHEN $8 THEN EXCLUDED.ref_height ELSE lost_found_camera_config.ref_height END,
                settings   = CASE WHEN $9::boolean THEN EXCLUDED.settings   ELSE lost_found_camera_config.settings END,
                is_active  = COALESCE($7, lost_found_camera_config.is_active)
            RETURNING stream_id, workspace_id, roi, ref_width, ref_height, settings, is_active, updated_at
            """,
            (
                stream_id, workspace_id,
                json.dumps(roi) if roi else None, ref_width, ref_height,
                json.dumps(settings) if settings is not None else None, is_active,
                replace_roi, replace_settings,
            ),
            fetch_one=True,
        )
        return _decode(row, "roi", "settings")

    # =========================================================
    # Events (written by the stream processor)
    # =========================================================

    async def insert_event(
        self,
        *,
        stream_id: UUID,
        workspace_id: UUID,
        camera_name: Optional[str],
        record: Dict[str, Any],
    ) -> bool:
        """Store one abandoned event (``LostFoundEngine`` record). False when the
        (camera, track) pair was already recorded - recording is idempotent."""
        row = await self.db.execute_query(
            """
            INSERT INTO lost_found_events (
                event_id, stream_id, workspace_id, camera_name, track_key, class_name, confidence,
                bbox, owner_track_id, first_seen_at, static_since, event_timestamp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12)
            ON CONFLICT (stream_id, track_key) DO NOTHING
            RETURNING event_id
            """,
            (
                UUID(record["event_id"]), stream_id, workspace_id, camera_name,
                record["track_key"], record["class_name"], record.get("confidence"),
                json.dumps(record.get("bbox")) if record.get("bbox") else None,
                record.get("owner_track_id"),
                _epoch(record.get("first_seen_at")) or datetime.now(timezone.utc),
                _epoch(record.get("static_since")),
                _epoch(record.get("abandoned_at")) or datetime.now(timezone.utc),
            ),
            fetch_one=True,
        )
        return row is not None

    async def insert_found_item(
        self,
        *,
        event_id: UUID,
        stream_id: UUID,
        workspace_id: UUID,
        record: Dict[str, Any],
    ) -> Optional[UUID]:
        """Add the event's object to the found gallery."""
        row = await self.db.execute_query(
            """
            INSERT INTO lost_found_found_items (
                event_id, stream_id, workspace_id, class_name, dominant_color, phash, color_hist, found_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING found_id
            """,
            (
                event_id, stream_id, workspace_id, record["class_name"], record.get("dominant_color"),
                record.get("phash"), record.get("color_hist"),
                _epoch(record.get("abandoned_at")) or datetime.now(timezone.utc),
            ),
            fetch_one=True,
        )
        return row["found_id"] if row else None

    async def set_event_evidence(
        self, event_id: UUID, *, snapshot_path: Optional[str] = None, video_path: Optional[str] = None,
    ) -> None:
        await self.db.execute_query(
            """
            UPDATE lost_found_events
            SET snapshot_path = COALESCE(snapshot_path, $2),
                video_path    = COALESCE(video_path, $3)
            WHERE event_id = $1
            """,
            (event_id, snapshot_path, video_path),
            return_rowcount=True,
        )

    async def set_found_crop(self, found_id: UUID, crop_path: str) -> None:
        await self.db.execute_query(
            "UPDATE lost_found_found_items SET crop_path = COALESCE(crop_path, $2) WHERE found_id = $1",
            (found_id, crop_path),
            return_rowcount=True,
        )

    # =========================================================
    # Events (read / case management)
    # =========================================================

    _EVENT_COLUMNS = """
        e.event_id, e.stream_id, e.event_timestamp, e.track_key, e.class_name, e.class_name AS target_class,
        e.confidence::float AS confidence, e.bbox, e.owner_track_id, e.first_seen_at, e.static_since,
        EXTRACT(EPOCH FROM (e.event_timestamp - e.static_since))::float AS unattended_seconds,
        e.snapshot_path, e.video_path, e.status, e.action_taken, e.description,
        e.acknowledged_at, e.resolved_at, e.resolved_by,
        COALESCE(vs.name, e.camera_name) AS camera_name, vs.location AS camera_location,
        vs.area AS camera_area, vs.building, vs.floor_level, vs.zone,
        f.found_id, f.dominant_color, f.status AS found_status, f.crop_path
    """
    _EVENT_FROM = """
        lost_found_events e
        LEFT JOIN video_stream vs ON vs.stream_id = e.stream_id
        LEFT JOIN lost_found_found_items f ON f.event_id = e.event_id
    """

    async def get_events(
        self, workspace_id: UUID, *, limit: int = 10, offset: int = 0, **filters,
    ) -> List[Dict[str, Any]]:
        conditions, params = self._conditions(workspace_id, alias="e", time_column="event_timestamp", **filters)
        params.extend([limit, offset])
        rows = await self.db.execute_query(
            f"""
            SELECT {self._EVENT_COLUMNS}
            FROM {self._EVENT_FROM}
            WHERE {' AND '.join(conditions)}
            ORDER BY e.event_timestamp DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """,
            tuple(params),
            fetch_all=True,
        ) or []
        return [_decode(r, "bbox") for r in rows]

    async def count_events(self, workspace_id: UUID, **filters) -> int:
        conditions, params = self._conditions(workspace_id, alias="e", time_column="event_timestamp", **filters)
        row = await self.db.execute_query(
            f"""
            SELECT COUNT(*) AS total
            FROM lost_found_events e LEFT JOIN video_stream vs ON vs.stream_id = e.stream_id
            WHERE {' AND '.join(conditions)}
            """,
            tuple(params),
            fetch_one=True,
        )
        return row["total"] if row else 0

    async def get_event(self, workspace_id: UUID, event_id: UUID) -> Optional[Dict[str, Any]]:
        row = await self.db.execute_query(
            f"SELECT {self._EVENT_COLUMNS} FROM {self._EVENT_FROM} WHERE e.workspace_id = $1 AND e.event_id = $2",
            (workspace_id, event_id),
            fetch_one=True,
        )
        return _decode(row, "bbox")

    async def event_stats(self, workspace_id: UUID) -> Dict[str, Any]:
        row = await self.db.execute_query(
            """
            SELECT COUNT(*)                                                        AS total,
                   COUNT(*) FILTER (WHERE status = 'open')                         AS open,
                   COUNT(*) FILTER (WHERE event_timestamp >= NOW() - INTERVAL '24 hours') AS last_24h,
                   MAX(event_timestamp)                                            AS last_event_at
            FROM lost_found_events WHERE workspace_id = $1
            """,
            (workspace_id,),
            fetch_one=True,
        )
        return row or {"total": 0, "open": 0, "last_24h": 0, "last_event_at": None}

    async def resolve_event(
        self,
        *,
        workspace_id: UUID,
        event_id: UUID,
        status: str,
        action_taken: Optional[str] = None,
        description: Optional[str] = None,
        user_id: Optional[UUID] = None,
    ) -> bool:
        """Acknowledge / resolve / mark false positive. Scoped to the workspace, so an
        event of another workspace behaves as not found. A false positive also takes
        its object out of the found gallery."""
        found = await self.db.execute_query(
            """
            UPDATE lost_found_events
            SET status          = $3::varchar,
                action_taken    = COALESCE($4, action_taken),
                description     = COALESCE($5, description),
                acknowledged_at = CASE WHEN $3::varchar <> 'open' THEN COALESCE(acknowledged_at, NOW()) END,
                resolved_at     = CASE WHEN $3::varchar IN ('resolved', 'false_positive')
                                       THEN COALESCE(resolved_at, NOW()) END,
                resolved_by     = CASE WHEN $3::varchar IN ('resolved', 'false_positive') THEN $6::uuid
                                       ELSE resolved_by END
            WHERE workspace_id = $1 AND event_id = $2
            """,
            (workspace_id, event_id, status, action_taken, description, user_id),
            return_rowcount=True,
        )
        if found and status == "false_positive":
            await self.db.execute_query(
                "UPDATE lost_found_found_items SET status = 'discarded' "
                "WHERE event_id = $1 AND workspace_id = $2 AND status = 'unclaimed'",
                (event_id, workspace_id),
                return_rowcount=True,
            )
        return bool(found)

    # =========================================================
    # Found items
    # =========================================================

    _FOUND_COLUMNS = """
        f.found_id, f.event_id, f.stream_id, COALESCE(vs.name, e.camera_name) AS camera_name,
        vs.location AS camera_location, f.class_name, f.dominant_color, f.found_at, f.status,
        f.crop_path, e.snapshot_path, e.video_path, e.confidence::float AS confidence,
        e.status AS event_status
    """
    _FOUND_FROM = """
        lost_found_found_items f
        LEFT JOIN video_stream vs ON vs.stream_id = f.stream_id
        LEFT JOIN lost_found_events e ON e.event_id = f.event_id
    """

    async def list_found_items(
        self, workspace_id: UUID, *, page: int = 1, per_page: int = 24, **filters,
    ) -> Dict[str, Any]:
        """The found gallery and its search: camera, time range, type, colour, status."""
        conditions, params = self._conditions(workspace_id, alias="f", time_column="found_at", **filters)
        where = " AND ".join(conditions)
        total = await self.db.execute_query(
            f"SELECT COUNT(*) AS total FROM {self._FOUND_FROM} WHERE {where}", tuple(params), fetch_one=True,
        )
        params.extend([per_page, (page - 1) * per_page])
        rows = await self.db.execute_query(
            f"""
            SELECT {self._FOUND_COLUMNS}
            FROM {self._FOUND_FROM}
            WHERE {where}
            ORDER BY f.found_at DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """,
            tuple(params),
            fetch_all=True,
        ) or []
        return {"total": total["total"] if total else 0, "page": page, "per_page": per_page, "items": rows}

    async def get_found_item(self, workspace_id: UUID, found_id: UUID) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            f"SELECT {self._FOUND_COLUMNS} FROM {self._FOUND_FROM} WHERE f.workspace_id = $1 AND f.found_id = $2",
            (workspace_id, found_id),
            fetch_one=True,
        )

    async def set_found_status(self, workspace_id: UUID, found_id: UUID, status: str) -> bool:
        return bool(await self.db.execute_query(
            "UPDATE lost_found_found_items SET status = $3 WHERE workspace_id = $1 AND found_id = $2",
            (workspace_id, found_id, status),
            return_rowcount=True,
        ))

    # =========================================================
    # Lost reports
    # =========================================================

    _LOST_COLUMNS = """
        l.lost_id, l.reporter_name, l.reporter_email, l.description, l.class_name, l.dominant_color,
        l.photo_path, l.lost_after, l.reported_at, l.status, l.reported_by,
        u.username AS reported_by_username,
        (SELECT COUNT(*) FROM lost_found_matches m WHERE m.lost_id = l.lost_id)                 AS suggestions,
        (SELECT m.found_id FROM lost_found_matches m WHERE m.lost_id = l.lost_id AND m.confirmed
          ORDER BY m.created_at DESC LIMIT 1)                                                   AS confirmed_found_id
    """

    async def create_lost_item(
        self,
        workspace_id: UUID,
        *,
        image: Optional[np.ndarray] = None,
        description: Optional[str] = None,
        reporter_name: Optional[str] = None,
        reporter_email: Optional[str] = None,
        class_name: Optional[str] = None,
        lost_after: Optional[datetime] = None,
        reported_by: Optional[UUID] = None,
    ) -> Optional[Dict[str, Any]]:
        """Register a lost item from a photo and/or a text description. The colour used
        for matching comes from the photo, or failing that from a colour word in the
        description ("black leather bag")."""
        color = dominant_color(image) if image is not None else None
        if color is None:
            color = color_from_text(description)
        features = visual.extract(image, keep=float(config.lost_found_match_photo_keep))

        row = await self.db.execute_query(
            """
            INSERT INTO lost_found_lost_items (
                workspace_id, reported_by, reporter_name, reporter_email, description, class_name,
                dominant_color, lost_after, phash, color_hist
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING lost_id
            """,
            (
                workspace_id, reported_by, reporter_name, reporter_email, description, class_name, color,
                lost_after,
                features.phash if features else None,
                features.histogram_bytes if features else None,
            ),
            fetch_one=True,
        )
        if not row:
            return None
        if image is not None:
            path = await upload_jpeg(encode_jpeg(image), lost_photo_key(workspace_id, row["lost_id"]))
            if path:
                await self.db.execute_query(
                    "UPDATE lost_found_lost_items SET photo_path = $2 WHERE lost_id = $1",
                    (row["lost_id"], path),
                    return_rowcount=True,
                )
        return await self.get_lost_item(workspace_id, row["lost_id"])

    async def get_lost_item(self, workspace_id: UUID, lost_id: UUID) -> Optional[Dict[str, Any]]:
        return await self.db.execute_query(
            f"""
            SELECT {self._LOST_COLUMNS}
            FROM lost_found_lost_items l LEFT JOIN users u ON u.user_id = l.reported_by
            WHERE l.workspace_id = $1 AND l.lost_id = $2
            """,
            (workspace_id, lost_id),
            fetch_one=True,
        )

    async def list_lost_items(
        self, workspace_id: UUID, *, status: Optional[str] = None, page: int = 1, per_page: int = 20,
    ) -> Dict[str, Any]:
        params: list = [workspace_id]
        where = "l.workspace_id = $1"
        if status:
            params.append(status)
            where += " AND l.status = $2"
        total = await self.db.execute_query(
            f"SELECT COUNT(*) AS total FROM lost_found_lost_items l WHERE {where}", tuple(params), fetch_one=True,
        )
        params.extend([per_page, (page - 1) * per_page])
        rows = await self.db.execute_query(
            f"""
            SELECT {self._LOST_COLUMNS}
            FROM lost_found_lost_items l LEFT JOIN users u ON u.user_id = l.reported_by
            WHERE {where}
            ORDER BY l.reported_at DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """,
            tuple(params),
            fetch_all=True,
        ) or []
        return {"total": total["total"] if total else 0, "page": page, "per_page": per_page, "items": rows}

    async def set_lost_status(self, workspace_id: UUID, lost_id: UUID, status: str) -> bool:
        return bool(await self.db.execute_query(
            "UPDATE lost_found_lost_items SET status = $3 WHERE workspace_id = $1 AND lost_id = $2",
            (workspace_id, lost_id, status),
            return_rowcount=True,
        ))

    # =========================================================
    # Matching
    # =========================================================

    async def find_matches(
        self,
        workspace_id: UUID,
        lost_id: UUID,
        *,
        top_k: Optional[int] = None,
        min_score: Optional[float] = None,
        persist: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """Rank unclaimed found items against a stored lost report (None: no such report)."""
        report = await self.db.execute_query(
            """
            SELECT class_name, dominant_color, lost_after, phash, color_hist
            FROM lost_found_lost_items WHERE workspace_id = $1 AND lost_id = $2
            """,
            (workspace_id, lost_id),
            fetch_one=True,
        )
        if not report:
            return None

        class_name, color = report["class_name"], report["dominant_color"]
        rows = await self.db.execute_query(
            """
            SELECT f.found_id, f.event_id, f.stream_id, COALESCE(vs.name, e.camera_name) AS camera_name,
                   f.class_name, f.dominant_color, f.found_at, f.status, f.crop_path, e.bbox,
                   f.phash, f.color_hist
            FROM lost_found_found_items f
            LEFT JOIN video_stream vs     ON vs.stream_id = f.stream_id
            LEFT JOIN lost_found_events e ON e.event_id  = f.event_id
            WHERE f.workspace_id = $5
              AND f.status = 'unclaimed'
              AND ($3::timestamptz IS NULL OR f.found_at >= $3)
              AND (($1::text IS NULL AND $2::text IS NULL)
                   OR f.class_name = $1 OR f.dominant_color = $2
                   OR ($2::text IN ('black', 'gray', 'brown')
                       AND f.dominant_color IN ('black', 'gray', 'brown')))
            -- The pool is capped, so fill it with the likeliest items first:
            -- same type and colour, then the soonest after the loss.
            ORDER BY (f.class_name = $1) IS TRUE DESC,
                     (f.dominant_color = $2) IS TRUE DESC,
                     f.found_at
            LIMIT $4
            """,
            (class_name, color, report["lost_after"], _MAX_CANDIDATES, workspace_id),
            fetch_all=True,
        ) or []
        rows = [_decode(r, "bbox") for r in rows]

        limit = top_k or int(config.lost_found_match_top_k)
        threshold = float(config.lost_found_match_min_score) if min_score is None else min_score
        photo_hash = report["phash"]
        photo_hist = visual.histogram_from_bytes(report["color_hist"])
        has_photo = photo_hash is not None or photo_hist is not None
        if has_photo:
            await self._backfill_features(rows)

        matches = []
        for row in rows:
            similarity = None
            if has_photo:
                similarity = visual.similarity(
                    photo_hash, photo_hist,
                    row["phash"], visual.histogram_from_bytes(row["color_hist"]),
                    float(config.lost_found_match_hash_share),
                )
            matches.append(score_candidate(row, class_name, color, report["lost_after"], similarity, has_photo))
        matches.sort(key=lambda m: (m["score"], m["found_at"]), reverse=True)
        matches = collapse_repeat_sightings(matches, float(config.lost_found_match_dedupe_iou))
        matches = [m for m in matches if m["score"] >= threshold][:limit]

        if persist and matches:
            await self._persist_suggestions(workspace_id, lost_id, matches)
        return matches

    async def _backfill_features(self, rows: List[Dict[str, Any]]) -> None:
        """Compute visual features for found items stored without them (e.g. imported).
        The stored crop carries context around the box, so only its centre is used."""
        from app.services.s3_service import s3_service
        keep = 1.0 / (1.0 + 2.0 * float(config.lost_found_crop_context_ratio))
        for row in rows:
            if row["phash"] is not None or row["color_hist"] is not None or not row.get("crop_path"):
                continue
            data = await s3_service.get_image_data(row["crop_path"])
            features = visual.extract(decode_jpeg(data) if data else None, keep=keep)
            if features is None:
                continue
            await self.db.execute_query(
                "UPDATE lost_found_found_items SET phash = $2, color_hist = $3 WHERE found_id = $1",
                (row["found_id"], features.phash, features.histogram_bytes),
                return_rowcount=True,
            )
            row["phash"], row["color_hist"] = features.phash, features.histogram_bytes

    async def _persist_suggestions(self, workspace_id: UUID, lost_id: UUID, matches: List[Dict[str, Any]]) -> None:
        """Keep the suggested shortlist so operators can review it later."""
        try:
            for m in matches:
                await self.db.execute_query(
                    """
                    INSERT INTO lost_found_matches (lost_id, found_id, workspace_id, score, method)
                    VALUES ($1, $2, $3, $4, 'rule_based')
                    ON CONFLICT (lost_id, found_id)
                    DO UPDATE SET score = EXCLUDED.score, created_at = NOW()
                    WHERE NOT lost_found_matches.confirmed
                    """,
                    (lost_id, m["found_id"], workspace_id, m["score"]),
                    return_rowcount=True,
                )
        except Exception as e:
            logger.warning("Could not persist match suggestions for %s: %s", lost_id, e)

    async def confirm_match(
        self, workspace_id: UUID, lost_id: UUID, found_id: UUID, score: float = 1.0,
        user_id: Optional[UUID] = None,
    ) -> Optional[Dict[str, Any]]:
        """Operator confirms a pairing: the report is resolved, the item claimed and its
        event resolved. None when either side is not in the workspace."""
        match = await self.db.execute_query(
            """
            INSERT INTO lost_found_matches (lost_id, found_id, workspace_id, score, method, confirmed, confirmed_by)
            SELECT l.lost_id, f.found_id, l.workspace_id, $4::real, 'operator_confirmed', TRUE, $5::uuid
            FROM lost_found_lost_items l, lost_found_found_items f
            WHERE l.lost_id = $2 AND f.found_id = $3 AND l.workspace_id = $1 AND f.workspace_id = $1
            ON CONFLICT (lost_id, found_id)
            DO UPDATE SET confirmed = TRUE, method = 'operator_confirmed',
                          confirmed_by = EXCLUDED.confirmed_by, created_at = NOW()
            RETURNING match_id, lost_id, found_id, score, confirmed, created_at
            """,
            (workspace_id, lost_id, found_id, score, user_id),
            fetch_one=True,
        )
        if not match:
            return None
        await self.db.execute_query(
            "UPDATE lost_found_lost_items SET status = 'resolved' WHERE lost_id = $1", (lost_id,),
            return_rowcount=True,
        )
        await self.db.execute_query(
            "UPDATE lost_found_found_items SET status = 'claimed' WHERE found_id = $1", (found_id,),
            return_rowcount=True,
        )
        await self.db.execute_query(
            """
            UPDATE lost_found_events
            SET status = 'resolved', resolved_at = COALESCE(resolved_at, NOW()),
                acknowledged_at = COALESCE(acknowledged_at, NOW()), resolved_by = $2,
                action_taken = COALESCE(action_taken, 'Returned to owner')
            WHERE event_id = (SELECT event_id FROM lost_found_found_items WHERE found_id = $1)
            """,
            (found_id, user_id),
            return_rowcount=True,
        )
        logger.info("Lost & found match confirmed: lost %s <-> found %s", lost_id, found_id)
        return match

    async def reject_match(self, workspace_id: UUID, lost_id: UUID, found_id: UUID) -> bool:
        return bool(await self.db.execute_query(
            "DELETE FROM lost_found_matches WHERE workspace_id = $1 AND lost_id = $2 AND found_id = $3",
            (workspace_id, lost_id, found_id),
            return_rowcount=True,
        ))

    # =========================================================
    # Charts shared with the other features (Visualizations tab)
    # =========================================================

    _CHART_FROM = "lost_found_events e LEFT JOIN video_stream vs ON vs.stream_id = e.stream_id"

    async def incidents_per_camera(self, workspace_id: UUID, **filters) -> List[Dict[str, Any]]:
        conditions, params = self._conditions(workspace_id, alias="e", time_column="event_timestamp", **filters)
        return await evidence_analytics.incidents_per_camera(
            self.db, from_sql=self._CHART_FROM, where=" AND ".join(conditions), params=params,
            alias="e", incident_expr="COUNT(*)",
        )

    async def confidence_audit(self, workspace_id: UUID, **filters) -> Dict[str, Any]:
        conditions, params = self._conditions(workspace_id, alias="e", time_column="event_timestamp", **filters)
        return await evidence_analytics.confidence_audit(
            self.db, from_sql=self._CHART_FROM, where=" AND ".join(conditions), params=params,
            alias="e", threshold=float(config.lost_found_confidence),
        )

    # =========================================================
    # Analytics page (ported from lost-found analytics_service)
    # =========================================================

    async def _window(self, days: int, tz: str) -> Dict[str, Any]:
        """The bucket grid, computed by Postgres so its tz database is the one used. One
        day is the last 24 hourly buckets; longer is ``days`` daily buckets ending today."""
        from datetime import timedelta
        import asyncpg

        bucket = "hour" if days == 1 else "day"
        step = timedelta(hours=1) if bucket == "hour" else timedelta(days=1)
        count = 24 if bucket == "hour" else days
        try:
            row = await self.db.execute_query(
                """
                WITH t AS (SELECT date_trunc($1, NOW() AT TIME ZONE $2) AS end_local)
                SELECT end_local,
                       end_local - $3::interval                    AS start_local,
                       (end_local - $3::interval) AT TIME ZONE $2  AS start_at,
                       NOW()                                       AS now
                FROM t
                """,
                (bucket, tz, step * (count - 1)),
                fetch_one=True,
            )
        except asyncpg.exceptions.InvalidParameterValueError as e:
            raise ValueError(f"Unknown timezone: {tz}") from e
        except Exception as e:
            # db_manager wraps Postgres errors; an unknown zone surfaces as a 500 there.
            if "time zone" in str(getattr(e, "detail", e)):
                raise ValueError(f"Unknown timezone: {tz}") from e
            raise
        return {"bucket": bucket, "step": step, **row}

    async def summary(
        self, workspace_id: UUID, days: int = 7, camera_id: Optional[UUID] = None, tz: str = _TZ_NAME,
    ) -> Dict[str, Any]:
        window = await self._window(days, tz)
        start_at = window["start_at"]

        # $1 workspace, $2 window start, $3 the camera when one is given.
        base: Tuple[Any, ...] = (workspace_id, start_at) + ((camera_id,) if camera_id else ())
        n = len(base)
        event_where = "e.workspace_id = $1 AND e.event_timestamp >= $2" + (" AND e.stream_id = $3" if camera_id else "")
        found_camera = " AND f.stream_id = $3" if camera_id else ""

        totals = await self.db.execute_query(
            f"""
            SELECT COUNT(*)                                          AS events,
                   COUNT(*) FILTER (WHERE status = 'open')           AS open,
                   COUNT(*) FILTER (WHERE status = 'false_positive') AS false_positives,
                   AVG(EXTRACT(EPOCH FROM (event_timestamp - static_since)))
                       FILTER (WHERE static_since IS NOT NULL)       AS avg_unattended_s
            FROM lost_found_events e
            WHERE {event_where}
            """,
            base,
            fetch_one=True,
        )
        found = await self.db.execute_query(
            f"SELECT COUNT(*) AS total FROM lost_found_found_items f "
            f"WHERE f.workspace_id = $1 AND f.found_at >= $2{found_camera}",
            base,
            fetch_one=True,
        )
        # A lost report describes an object, not a place: no camera filter.
        lost = await self.db.execute_query(
            "SELECT COUNT(*) AS total FROM lost_found_lost_items WHERE workspace_id = $1 AND reported_at >= $2",
            (workspace_id, start_at),
            fetch_one=True,
        )
        reunions = await self.db.execute_query(
            f"""
            SELECT COUNT(*) AS reunions,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (m.created_at - l.reported_at))
                   )                                                       AS median_s,
                   MIN(EXTRACT(EPOCH FROM (m.created_at - l.reported_at))) AS fastest_s
            FROM lost_found_matches m
            JOIN lost_found_lost_items l  ON l.lost_id  = m.lost_id
            JOIN lost_found_found_items f ON f.found_id = m.found_id
            WHERE m.workspace_id = $1 AND m.confirmed AND m.created_at >= $2{found_camera}
            """,
            base,
            fetch_one=True,
        )
        # generate_series supplies the empty buckets a GROUP BY would drop.
        series_rows = await self.db.execute_query(
            f"""
            WITH buckets AS (
                SELECT generate_series(${n + 1}::timestamp, ${n + 2}::timestamp, ${n + 3}::interval)
                       AS bucket_start
            ),
            ev AS (
                SELECT date_trunc(${n + 4}, e.event_timestamp AT TIME ZONE ${n + 5}) AS bucket_start,
                       e.status
                FROM lost_found_events e
                WHERE {event_where}
            )
            SELECT b.bucket_start,
                   COUNT(ev.status)                                     AS events,
                   COUNT(*) FILTER (WHERE ev.status = 'false_positive') AS false_positives,
                   COUNT(*) FILTER (WHERE ev.status = 'resolved')       AS resolved
            FROM buckets b
            LEFT JOIN ev ON ev.bucket_start = b.bucket_start
            GROUP BY b.bucket_start
            ORDER BY b.bucket_start
            """,
            base + (window["start_local"], window["end_local"], window["step"], window["bucket"], tz),
            fetch_all=True,
        ) or []
        heatmap_rows = await self.db.execute_query(
            f"""
            SELECT EXTRACT(DOW  FROM e.event_timestamp AT TIME ZONE ${n + 1})::int AS weekday,
                   EXTRACT(HOUR FROM e.event_timestamp AT TIME ZONE ${n + 1})::int AS hour,
                   COUNT(*)                                                      AS events
            FROM lost_found_events e
            WHERE {event_where}
            GROUP BY weekday, hour
            ORDER BY weekday, hour
            """,
            base + (tz,),
            fetch_all=True,
        ) or []
        camera_rows = await self.db.execute_query(
            f"""
            SELECT e.stream_id                                         AS camera_id,
                   COALESCE(vs.name, MAX(e.camera_name))               AS camera_name,
                   vs.location,
                   COUNT(*)                                            AS events,
                   COUNT(*) FILTER (WHERE e.status = 'open')           AS open,
                   COUNT(*) FILTER (WHERE e.status = 'false_positive') AS false_positives,
                   AVG(EXTRACT(EPOCH FROM (e.event_timestamp - e.static_since)))
                       FILTER (WHERE e.static_since IS NOT NULL)       AS avg_unattended_s
            FROM lost_found_events e
            LEFT JOIN video_stream vs ON vs.stream_id = e.stream_id
            WHERE {event_where}
            GROUP BY e.stream_id, vs.name, vs.location
            ORDER BY events DESC, camera_name
            """,
            base,
            fetch_all=True,
        ) or []
        class_rows = await self.db.execute_query(
            f"""
            SELECT e.class_name,
                   COUNT(*)                                            AS events,
                   AVG(e.confidence)                                   AS avg_confidence,
                   COUNT(*) FILTER (WHERE e.status = 'false_positive') AS false_positives
            FROM lost_found_events e
            WHERE {event_where}
            GROUP BY e.class_name
            ORDER BY events DESC, e.class_name
            """,
            base,
            fetch_all=True,
        ) or []

        events = totals["events"]
        kpis = {
            "events": events,
            "open": totals["open"],
            "false_positives": totals["false_positives"],
            "false_positive_pct": _pct(totals["false_positives"], events),
            "found_items": found["total"],
            "lost_reports": lost["total"],
            "reunions": reunions["reunions"],
            "median_reunite_s": _num(reunions["median_s"]),
            "fastest_reunite_s": _num(reunions["fastest_s"]),
            "avg_unattended_s": _num(totals["avg_unattended_s"]),
        }
        return {
            "window": {"start": start_at, "end": window["now"], "bucket": window["bucket"], "tz": tz},
            "kpis": kpis,
            # Naive local timestamps: they are wall-clock times in ``tz``.
            "series": [
                {
                    "bucket_start": row["bucket_start"].isoformat(),
                    "events": row["events"],
                    "false_positives": row["false_positives"],
                    "resolved": row["resolved"],
                }
                for row in series_rows
            ],
            "heatmap": heatmap_rows,
            "by_camera": [
                {
                    **row,
                    "false_positive_pct": _pct(row["false_positives"], row["events"]),
                    "avg_unattended_s": _num(row["avg_unattended_s"]),
                }
                for row in camera_rows
            ],
            "by_class": [{**row, "avg_confidence": _num(row["avg_confidence"])} for row in class_rows],
            "funnel": {
                "events_detected": events,
                "items_harvested": kpis["found_items"],
                "reports_filed": kpis["lost_reports"],
                "reunions": kpis["reunions"],
            },
        }


def _num(value: Any) -> Optional[float]:
    return float(value) if value is not None else None


def _pct(part: int, whole: int) -> Optional[float]:
    return round(100.0 * part / whole, 1) if whole else None


lost_found_service = LostFoundService()
