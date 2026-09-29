"""Shapes shared by the no-entry-zone and blocked-exit "3 tab" pages: the single
polygon per camera, the evidence video grid and the two visualisations."""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from app.schemas.no_entry_zone_schema import _check_points_in_frame, _validate_polygon


# ─────────────────────────────────────────────
# Polygon editor
# ─────────────────────────────────────────────

class CameraPolygonRequest(BaseModel):
    polygon: List[List[float]] = Field(
        ..., description="Polygon points [[x,y], ...] in pixels of the frame it was drawn on (3-64 points)",
    )
    ref_width: int = Field(..., gt=0, le=10000, description="Width of the frame the polygon was drawn on")
    ref_height: int = Field(..., gt=0, le=10000, description="Height of the frame the polygon was drawn on")
    is_active: Optional[bool] = None

    @field_validator("polygon")
    @classmethod
    def _poly(cls, v):
        return _validate_polygon(v)

    @model_validator(mode="after")
    def _in_frame(self):
        _check_points_in_frame(self.polygon, self.ref_width, self.ref_height)
        return self


class CameraPolygonResponse(BaseModel):
    """``polygon`` is null until one is saved for the camera."""
    stream_id: UUID
    camera_name: Optional[str] = None
    id: Optional[UUID] = Field(None, description="zone_id (no-entry-zone) or config_id (blocked-exit)")
    polygon: Optional[List[List[float]]] = None
    ref_width: Optional[int] = None
    ref_height: Optional[int] = None
    point_count: int = 0
    is_active: Optional[bool] = None
    updated_at: Optional[datetime] = None


# ─────────────────────────────────────────────
# Evidence videos
# ─────────────────────────────────────────────

class EvidenceVideo(BaseModel):
    event_id: int
    event_timestamp: datetime
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    camera_location: Optional[str] = None
    camera_area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    camera_zone: Optional[str] = None
    status: str
    confidence: Optional[float] = None
    clip_url: Optional[str] = None
    snapshot_url: Optional[str] = None
    video_url: Optional[str] = None      # blocked-exit: full-rate episode video
    download_url: Optional[str] = None   # blocked-exit: stable download link (redirects to a fresh URL)
    # feature-specific
    zone_name: Optional[str] = None
    target_class: Optional[str] = None
    peak_state: Optional[str] = None
    risk_level: Optional[str] = None


class EvidenceVideoListResponse(BaseModel):
    items: List[EvidenceVideo]
    total: int
    limit: int
    offset: int
    page: int = 1
    expires_in: int


# ─────────────────────────────────────────────
# Visualisations
# ─────────────────────────────────────────────

class CameraIncidentCount(BaseModel):
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    events: int = 0
    incidents: int = 0
    resolved: int = 0


class ConfidenceSummary(BaseModel):
    events: int = 0
    scored_events: int = 0
    avg_confidence: Optional[float] = None
    min_confidence: Optional[float] = None
    max_confidence: Optional[float] = None
    low_confidence: int = 0
    low_confidence_share: Optional[float] = None
    acknowledged: int = 0
    resolved: int = 0
    threshold: float
    low_confidence_cutoff: float


class CameraConfidence(BaseModel):
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    events: int = 0
    avg_confidence: Optional[float] = None
    min_confidence: Optional[float] = None
    max_confidence: Optional[float] = None
    low_confidence: int = 0


class ConfidenceBucket(BaseModel):
    bucket: str
    min: float
    max: float
    count: int = 0


class EventConfidence(BaseModel):
    event_id: int
    event_timestamp: datetime
    stream_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    confidence: Optional[float] = None
    status: str


class ConfidenceAuditResponse(BaseModel):
    summary: ConfidenceSummary
    per_camera: List[CameraConfidence]
    histogram: List[ConfidenceBucket]
    per_event: List[EventConfidence]
