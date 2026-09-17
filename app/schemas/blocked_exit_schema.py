from pydantic import BaseModel, Field
from typing import List, Optional
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


class BlockedExitEventResponse(BlockedExitEventBase):
    event_id: int

    class Config:
        from_attributes = True


class BlockedExitEventListResponse(BaseModel):
    items: List[BlockedExitEventResponse]
    total: int
    limit: int
    offset: int


class ResolveBlockedExitRequest(BaseModel):
    status: str = Field(
        ...,
        description="Must be 'acknowledged' or 'resolved'",
    )
    description: Optional[str] = None


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


class ActiveBlockedExitEventSummary(BlockedExitEventBase):
    event_id: int
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    workspace_name: Optional[str] = None
    workspace_id: UUID


class ExitZoneConfigRequest(BaseModel):
    door_polygon: List[List[float]] = Field(..., description="Polygon points [[x,y], ...]")
    calibration_frame_w: Optional[int] = None
    calibration_frame_h: Optional[int] = None
    detector_strategy: str = Field("zone_based", pattern="^(zone_based|door_centric)$")
    min_accessibility_pct: Optional[float] = Field(None, ge=0, le=100)
    debounce_seconds: Optional[float] = Field(None, ge=0)


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

    class Config:
        from_attributes = True
