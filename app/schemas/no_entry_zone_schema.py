from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Literal, Optional
from datetime import datetime, date
from uuid import UUID

import cv2
import numpy as np

EventStatus = Literal["detected", "acknowledged", "resolved"]
ResolveStatus = Literal["acknowledged", "resolved"]
AnchorMode = Literal["bottom_center", "center"]


class NoEntryEventBase(BaseModel):
    event_timestamp: datetime
    incident_id: Optional[UUID] = None
    zone_id: Optional[UUID] = None
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    zone_name: Optional[str] = None
    target_class: Optional[str] = None
    track_id: Optional[int] = None
    confidence: Optional[float] = None
    dwell_seconds: Optional[float] = None
    entered_at: Optional[datetime] = None
    exited_at: Optional[datetime] = None
    status: str
    description: Optional[str] = None
    snapshot_path: Optional[str] = None
    clip_path: Optional[str] = None
    evidence_paths: Optional[List[str]] = None
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    camera_zone: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None


class NoEntryEventResponse(NoEntryEventBase):
    event_id: int

    class Config:
        from_attributes = True


class NoEntryEventListResponse(BaseModel):
    items: List[NoEntryEventResponse]
    total: int
    limit: int
    offset: int
    page: int = 1


class ResolveNoEntryRequest(BaseModel):
    status: ResolveStatus = Field(..., description="'acknowledged' or 'resolved'")
    description: Optional[str] = Field(None, max_length=2000)


class NoEntryIncidentResponse(BaseModel):
    incident_id: UUID
    event_id: int = Field(..., description="Representative event (evidence source)")
    zone_id: Optional[UUID] = None
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    zone_name: Optional[str] = None
    started_at: datetime
    last_seen_at: datetime
    is_ongoing: bool
    event_count: int
    max_confidence: Optional[float] = None
    max_dwell_seconds: Optional[float] = None
    target_classes: List[str] = []
    status: EventStatus
    snapshot_path: Optional[str] = None
    clip_path: Optional[str] = None


class NoEntryIncidentListResponse(BaseModel):
    items: List[NoEntryIncidentResponse]
    total: int
    limit: int
    offset: int
    page: int = 1


class ResolveIncidentResponse(BaseModel):
    message: str
    updated_events: int


class EvidenceUrls(BaseModel):
    snapshot_url: Optional[str] = None
    clip_url: Optional[str] = None
    expires_in: int


class NoEntryDailySummary(BaseModel):
    event_date: date
    workspace_name: str
    total_events: int
    total_incidents: int
    resolved_count: int
    avg_dwell_seconds: Optional[float] = None


class ActiveNoEntryEventSummary(BaseModel):
    event_id: int
    incident_id: Optional[UUID] = None
    event_timestamp: datetime
    camera_name: Optional[str] = None
    zone_name: Optional[str] = None
    target_class: Optional[str] = None
    dwell_seconds: Optional[float] = None
    status: str
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    camera_zone: Optional[str] = None
    workspace_name: Optional[str] = None
    workspace_id: UUID


class OverviewResponse(BaseModel):
    events_24h: int = 0
    incidents_24h: int = 0
    open_incidents: int = 0
    unacknowledged_events: int = 0
    ongoing_events: int = 0
    nez_cameras: int = 0
    nez_cameras_streaming: int = 0
    active_zones: int = 0
    total_zones: int = 0
    latest_incidents: List[NoEntryIncidentResponse] = []


class AnalyticsBucket(BaseModel):
    bucket: datetime
    events: int
    incidents: int


class AnalyticsBreakdown(BaseModel):
    label: str
    events: int
    incidents: int


class AnalyticsHour(BaseModel):
    hour: int
    events: int
    incidents: int


class AnalyticsSummary(BaseModel):
    bucket: Literal["hour", "day"]
    timezone: str
    since: datetime
    until: datetime
    total_events: int = 0
    total_incidents: int = 0
    resolved_events: int = 0
    avg_dwell_seconds: Optional[float] = None
    series: List[AnalyticsBucket]
    by_zone: List[AnalyticsBreakdown]
    by_camera: List[AnalyticsBreakdown]
    by_class: List[AnalyticsBreakdown]
    by_hour: List[AnalyticsHour]


class CameraHealth(BaseModel):
    """Live state of a camera's stream, read from this server's memory. ``live_status`` is
    ``stopped`` when the stream is not processed here (see ``running_elsewhere``)."""
    live_status: str = "stopped"          # starting | active_pending | active | stopping | stopped | ...
    running_elsewhere: bool = False       # locked by another server; its memory is not visible here
    healthy: bool = False                 # a frame within the last 30 s
    last_frame_at: Optional[datetime] = None
    capture_state: Optional[str] = None   # idle | connecting | running | reconnecting | failed | ...
    total_frames: Optional[int] = None
    avg_fps: Optional[float] = None
    uptime_s: Optional[float] = None
    connection_attempts: Optional[int] = None
    consecutive_errors: Optional[int] = None
    last_error: Optional[str] = None      # credentials redacted
    resolution: Optional[str] = None
    zones_loaded: Optional[int] = None    # zones the detection engine is enforcing right now


class NoEntryCamera(BaseModel):
    stream_id: UUID
    name: str
    location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    status: Optional[str] = None
    is_streaming: bool = False
    is_no_entry_zone_camera: bool = False
    zone_count: int = 0
    active_zone_count: int = 0
    type: Optional[str] = None
    source_display: Optional[str] = None  # the source URL with credentials redacted
    last_activity: Optional[datetime] = None
    stop_reason: Optional[str] = None
    retry_count: Optional[int] = None
    auto_retry_enabled: Optional[bool] = None
    health: CameraHealth = Field(default_factory=CameraHealth)


class NoEntryCameraDetail(NoEntryCamera):
    zones: List["ZoneResponse"] = []


class ProbeResult(BaseModel):
    stream_id: Optional[UUID] = None
    name: Optional[str] = None
    reachable: bool
    latency_ms: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    error: Optional[str] = None


# ─────────────────────────────────────────────
# Zones
# ─────────────────────────────────────────────

def _segments_cross(a1, a2, b1, b2) -> bool:
    def orient(p, q, r):
        v = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])
        return 0 if v == 0 else (1 if v > 0 else 2)

    def on_seg(p, q, r):
        return min(p[0], r[0]) <= q[0] <= max(p[0], r[0]) and min(p[1], r[1]) <= q[1] <= max(p[1], r[1])

    o1, o2, o3, o4 = orient(a1, a2, b1), orient(a1, a2, b2), orient(b1, b2, a1), orient(b1, b2, a2)
    if o1 != o2 and o3 != o4:
        return True
    return (o1 == 0 and on_seg(a1, b1, a2)) or (o2 == 0 and on_seg(a1, b2, a2)) \
        or (o3 == 0 and on_seg(b1, a1, b2)) or (o4 == 0 and on_seg(b1, a2, b2))


def _validate_polygon(points: List[List[float]]) -> List[List[float]]:
    if len(points) < 3:
        raise ValueError("polygon needs at least 3 points")
    if len(points) > 64:
        raise ValueError("polygon has too many points (max 64)")
    norm = []
    for pt in points:
        if len(pt) != 2:
            raise ValueError("each polygon point must be [x, y]")
        norm.append((float(pt[0]), float(pt[1])))
    if len(set(norm)) != len(norm):
        raise ValueError("polygon contains duplicate points")
    if cv2.contourArea(np.array(norm, dtype=np.float32)) <= 0:
        raise ValueError("polygon has zero area")
    n = len(norm)
    for i in range(n):
        for j in range(i + 1, n):
            if j == i + 1 or (j + 1) % n == i:
                continue  # adjacent edges share a vertex
            if _segments_cross(norm[i], norm[(i + 1) % n], norm[j], norm[(j + 1) % n]):
                raise ValueError("polygon edges must not cross each other")
    return points


def _check_points_in_frame(points, width, height) -> None:
    for x, y in points:
        if x < 0 or y < 0 or x > width or y > height:
            raise ValueError("polygon points must lie inside the reference frame")


class ZoneSchedule(BaseModel):
    days: List[int] = Field(default_factory=lambda: list(range(7)), description="0=Monday .. 6=Sunday")
    start: str = Field("00:00", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    end: str = Field("23:59", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")

    @field_validator("days")
    @classmethod
    def _days(cls, v: List[int]) -> List[int]:
        if any(d < 0 or d > 6 for d in v):
            raise ValueError("days must be 0..6")
        return sorted(set(v))


class _ZoneRules(BaseModel):
    target_classes: Optional[List[str]] = Field(default=None, description="COCO names, e.g. ['person','car']")
    min_dwell_seconds: Optional[float] = Field(None, ge=0, le=3600)
    consecutive_frames: Optional[int] = Field(None, ge=1, le=300)
    cooldown_seconds: Optional[float] = Field(None, ge=0, le=86400)
    anchor: Optional[AnchorMode] = None
    schedule: Optional[ZoneSchedule] = None

    @field_validator("target_classes")
    @classmethod
    def _classes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        from app.config.settings import config
        names = set(getattr(config, "coco_class_names", []) or [])
        cleaned = []
        for c in v:
            c = str(c).strip()
            if not c:
                continue
            if not c.isdigit() and names and c not in names:
                raise ValueError(f"unknown object class '{c}'")
            cleaned.append(c)
        if not cleaned:
            raise ValueError("target_classes must not be empty")
        return sorted(set(cleaned))


class ZoneCreateRequest(_ZoneRules):
    stream_id: UUID
    name: str = Field(..., min_length=1, max_length=100)
    polygon: List[List[float]] = Field(..., description="Polygon points [[x,y], ...] in reference-frame pixels")
    ref_width: int = Field(..., gt=0, le=10000)
    ref_height: int = Field(..., gt=0, le=10000)
    is_active: Optional[bool] = True

    @field_validator("polygon")
    @classmethod
    def _poly(cls, v):
        return _validate_polygon(v)

    @model_validator(mode="after")
    def _in_frame(self):
        _check_points_in_frame(self.polygon, self.ref_width, self.ref_height)
        return self


class ZoneUpdateRequest(_ZoneRules):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    polygon: Optional[List[List[float]]] = None
    ref_width: Optional[int] = Field(None, gt=0, le=10000)
    ref_height: Optional[int] = Field(None, gt=0, le=10000)
    is_active: Optional[bool] = None

    @field_validator("polygon")
    @classmethod
    def _poly(cls, v):
        return v if v is None else _validate_polygon(v)

    @model_validator(mode="after")
    def _geometry_together(self):
        # A polygon only means something against the frame it was drawn on.
        if self.polygon is not None:
            if self.ref_width is None or self.ref_height is None:
                raise ValueError("polygon updates must include ref_width and ref_height")
            _check_points_in_frame(self.polygon, self.ref_width, self.ref_height)
        return self


class ZoneResponse(BaseModel):
    zone_id: UUID
    stream_id: UUID
    camera_name: Optional[str] = None
    name: str
    polygon: List[List[float]]
    ref_width: int
    ref_height: int
    target_classes: List[str]
    min_dwell_seconds: float
    consecutive_frames: Optional[int] = None
    cooldown_seconds: Optional[float] = None
    anchor: Optional[str] = None
    schedule: Optional[dict] = None
    is_active: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# NoEntryCameraDetail refers to ZoneResponse, defined further down.
NoEntryCameraDetail.model_rebuild()
