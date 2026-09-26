from pydantic import BaseModel, Field, field_validator
from typing import List, Literal, Optional
from datetime import datetime, date
from uuid import UUID


class BlockedExitEventBase(BaseModel):
    event_timestamp: datetime
    camera_name: Optional[str] = None
    state: str
    accessibility_pct: Optional[float] = None
    blocking_objects: Optional[List[str]] = None
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    recommended_action: Optional[str] = None
    status: str
    description: Optional[str] = None
    evidence_paths: Optional[List[str]] = None
    zone: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    camera_location: Optional[str] = None
    resolved_at: Optional[datetime] = None
    # Episode fields (NULL for rows written before episodes existed)
    stream_id: Optional[UUID] = None
    ended_at: Optional[datetime] = None
    duration_s: Optional[float] = None
    effective_duration_s: Optional[float] = None
    peak_state: Optional[str] = None
    min_accessibility_pct: Optional[float] = None
    peak_risk_score: Optional[float] = None
    acknowledged_at: Optional[datetime] = None


class BlockedExitEventResponse(BlockedExitEventBase):
    event_id: int

    class Config:
        from_attributes = True


class BlockedExitEventListResponse(BaseModel):
    items: List[BlockedExitEventResponse]
    total: int
    limit: int
    offset: int


class BlockedExitEventDetail(BlockedExitEventResponse):
    snapshot_urls: List[str] = []


class ResolveBlockedExitRequest(BaseModel):
    status: Literal["acknowledged", "resolved"]
    description: Optional[str] = Field(None, max_length=2000)


class BlockedExitDailySummary(BaseModel):
    event_date: date
    workspace_name: str
    total_events: int
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
    resolved_count: int
    avg_accessibility_pct: Optional[float] = None
    open_count: Optional[int] = None
    total_blocked_s: Optional[float] = None
    avg_duration_s: Optional[float] = None
    worst_accessibility_pct: Optional[float] = None


class ActiveBlockedExitEventSummary(BlockedExitEventBase):
    event_id: int
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    workspace_name: Optional[str] = None
    workspace_id: UUID


class ExitZoneConfigRequest(BaseModel):
    door_polygon: List[List[float]] = Field(..., min_length=3, description="Polygon points [[x,y], ...]")
    calibration_frame_w: Optional[int] = Field(None, gt=0)
    calibration_frame_h: Optional[int] = Field(None, gt=0)
    # None keeps the stored value, so re-drawing the polygon never resets settings.
    detector_strategy: Optional[Literal["zone_based", "door_centric"]] = None
    min_accessibility_pct: Optional[float] = Field(None, ge=0, le=100)
    debounce_seconds: Optional[float] = Field(None, ge=0, le=3600)
    is_active: Optional[bool] = None

    @field_validator("door_polygon")
    @classmethod
    def _points_are_pairs(cls, v: List[List[float]]) -> List[List[float]]:
        if any(len(pt) != 2 for pt in v):
            raise ValueError("each polygon point must be [x, y]")
        return v


class ExitZoneConfigUpdate(BaseModel):
    detector_strategy: Optional[Literal["zone_based", "door_centric"]] = None
    min_accessibility_pct: Optional[float] = Field(None, ge=0, le=100)
    debounce_seconds: Optional[float] = Field(None, ge=0, le=3600)
    is_active: Optional[bool] = None


class ExitZoneConfigResponse(BaseModel):
    config_id: UUID
    stream_id: UUID
    door_polygon: Optional[List[List[float]]] = None
    reference_bbox: Optional[List[float]] = None
    calibration_frame_w: Optional[int] = None
    calibration_frame_h: Optional[int] = None
    detector_strategy: str
    min_accessibility_pct: float
    debounce_seconds: float
    is_active: bool
    camera_name: Optional[str] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BlockedExitCameraStatus(BaseModel):
    stream_id: UUID
    camera_name: Optional[str] = None
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    stream_status: Optional[str] = None
    is_streaming: bool = False
    config_id: Optional[UUID] = None
    is_calibrated: bool = False
    config_active: bool = False
    threshold_pct: Optional[float] = None
    debounce_seconds: Optional[float] = None
    open_event_id: Optional[int] = None
    current_state: str = "clear"
    accessibility_pct: Optional[float] = None
    min_accessibility_pct: Optional[float] = None
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    blocking_objects: Optional[List[str]] = None
    recommended_action: Optional[str] = None
    event_status: Optional[str] = None
    blocked_since: Optional[datetime] = None
    blocked_for_s: Optional[float] = None
    last_event_at: Optional[datetime] = None


class BlockedExitAnalyticsSummary(BaseModel):
    days: int
    total_events: int = 0
    fully_blocked_events: int = 0
    unresolved_events: int = 0
    high_risk_events: int = 0
    avg_duration_s: float = 0
    max_duration_s: float = 0
    total_blocked_s: float = 0
    avg_min_accessibility_pct: Optional[float] = None
    cameras_total: int = 0
    cameras_monitored: int = 0
    currently_blocked: int = 0


class BlockedExitCameraStat(BaseModel):
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    camera_location: Optional[str] = None
    event_count: int
    fully_blocked_count: int
    total_blocked_s: float
    worst_accessibility_pct: Optional[float] = None
    peak_risk_score: Optional[float] = None
    last_event_at: Optional[datetime] = None


class BlockedExitDailyReportRow(BaseModel):
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    event_count: int
    fully_blocked_count: int
    total_blocked_s: float
    longest_s: Optional[float] = None
    worst_accessibility_pct: Optional[float] = None
    resolved_count: int


class BlockedExitDailyReport(BaseModel):
    report_date: date
    total_events: int
    total_blocked_s: float
    rows: List[BlockedExitDailyReportRow]


class BlockedExitHeatmapCell(BaseModel):
    weekday: int
    hour: int
    event_count: int


class BlockedExitTimelineEntry(BaseModel):
    event_id: int
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_s: Optional[float] = None
    peak_state: Optional[str] = None
    min_accessibility_pct: Optional[float] = None
    peak_risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    status: str


class CameraHealth(BaseModel):
    live_status: str
    last_frame_at: Optional[datetime] = None
    frame_age_s: Optional[float] = None
    healthy: bool = False
    state: Optional[str] = None
    total_frames: Optional[int] = None
    consecutive_errors: Optional[int] = None
    last_error: Optional[str] = None
    uptime_s: Optional[float] = None
    avg_fps: Optional[float] = None


class BlockedExitCamera(BaseModel):
    """A workspace camera as the Cameras page sees it. Deliberately has no
    source/path field: source_host is the credential-masked form."""
    stream_id: UUID
    camera_name: Optional[str] = None
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    type: Optional[str] = None
    source_host: Optional[str] = None
    stream_status: Optional[str] = None
    is_streaming: bool = False
    is_blocked_exit_camera: bool = False
    last_activity: Optional[datetime] = None
    stop_reason: Optional[str] = None
    retry_count: Optional[int] = None
    auto_retry_enabled: Optional[bool] = None
    running_elsewhere: bool = False
    config_id: Optional[UUID] = None
    is_calibrated: bool = False
    config_active: bool = False
    threshold_pct: Optional[float] = None
    debounce_seconds: Optional[float] = None
    door_polygon: Optional[List[List[float]]] = None
    calibration_frame_w: Optional[int] = None
    calibration_frame_h: Optional[int] = None
    open_event_id: Optional[int] = None
    current_state: str = "clear"
    accessibility_pct: Optional[float] = None
    risk_level: Optional[str] = None
    risk_score: Optional[float] = None
    blocking_objects: Optional[List[str]] = None
    event_status: Optional[str] = None
    blocked_since: Optional[datetime] = None
    blocked_for_s: Optional[float] = None
    last_event_at: Optional[datetime] = None
    health: CameraHealth


class ProbeResult(BaseModel):
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    reachable: bool
    latency_ms: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    error: Optional[str] = None
