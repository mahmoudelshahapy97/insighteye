from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, date
from uuid import UUID

class ShopliftingEventBase(BaseModel):
    event_timestamp: datetime
    camera_name: Optional[str] = None
    severity: Optional[str] = None
    event_type: Optional[str] = None
    status: str
    items_stolen: Optional[List[str]] = None
    estimated_value: Optional[float] = None
    action_taken: Optional[str] = None

class ShopliftingEventResponse(ShopliftingEventBase):
    event_id: int

class ShopliftingEventListResponse(BaseModel):
    items: List[ShopliftingEventResponse]
    total: int
    limit: int
    offset: int

class ResolveEventRequest(BaseModel):
    status: str = Field(..., description="Must be 'confirmed', 'dismissed', or 'resolved'")
    action_taken: Optional[str] = None
    description: Optional[str] = None

class ShopliftingDailySummary(BaseModel):
    event_date: date
    workspace_name: str
    total_events: int
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
    confirmed_count: int
    dismissed_count: int
    total_estimated_value: Optional[float] = None

class ActiveShopliftingEventSummary(ShopliftingEventBase):
    event_id: int
    camera_location: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    location_name: Optional[str] = None
    workspace_name: Optional[str] = None
    workspace_id: UUID
