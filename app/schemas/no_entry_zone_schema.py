from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, date
from uuid import UUID


class NoEntryEventBase(BaseModel):
    event_timestamp: datetime
    incident_id: Optional[UUID] = None
    camera_name: Optional[str] = None
    zone_name: Optional[str] = None
    target_class: Optional[str] = None
    dwell_seconds: Optional[float] = None
    status: str
    description: Optional[str] = None
    evidence_paths: Optional[List[str]] = None
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
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


class ResolveNoEntryRequest(BaseModel):
    status: str = Field(
        ...,
        description="Must be 'acknowledged' or 'resolved'",
    )
    description: Optional[str] = None


class NoEntryDailySummary(BaseModel):
    event_date: date
    workspace_name: str
    total_events: int
    total_incidents: int
    resolved_count: int
    avg_dwell_seconds: Optional[float] = None


class ActiveNoEntryEventSummary(NoEntryEventBase):
    event_id: int
    workspace_name: Optional[str] = None
    workspace_id: UUID


class ZoneCreateRequest(BaseModel):
    stream_id: UUID
    name: str
    polygon: List[List[float]] = Field(..., description="Polygon points [[x,y], ...]")
    ref_width: int
    ref_height: int
    target_classes: Optional[List[str]] = Field(default=None)
    min_dwell_seconds: Optional[float] = Field(None, ge=0)
    schedule: Optional[dict] = None


class ZoneUpdateRequest(BaseModel):
    name: Optional[str] = None
    polygon: Optional[List[List[float]]] = None
    ref_width: Optional[int] = None
    ref_height: Optional[int] = None
    target_classes: Optional[List[str]] = None
    min_dwell_seconds: Optional[float] = Field(None, ge=0)
    schedule: Optional[dict] = None
    is_active: Optional[bool] = None


class ZoneResponse(BaseModel):
    zone_id: UUID
    stream_id: UUID
    name: str
    polygon: List[List[float]]
    ref_width: int
    ref_height: int
    target_classes: List[str]
    min_dwell_seconds: float
    schedule: Optional[dict] = None
    is_active: bool

    class Config:
        from_attributes = True
