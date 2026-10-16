"""Request models for the lost & found API. Responses are plain dicts (UUID event ids,
presigned media URLs), shaped for the Insight-eye Lost & Found tab."""

from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

EventStatus = Literal["open", "acknowledged", "resolved", "false_positive"]
FoundStatus = Literal["unclaimed", "claimed", "discarded"]
LostStatus = Literal["open", "matched", "resolved", "closed"]


class ResolveLostFoundEventRequest(BaseModel):
    status: EventStatus
    action_taken: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=2000)


class FoundStatusRequest(BaseModel):
    status: FoundStatus


class LostStatusRequest(BaseModel):
    status: LostStatus


class ConfirmMatchRequest(BaseModel):
    lost_id: UUID
    found_id: UUID
    score: float = Field(1.0, ge=0.0, le=1.0)
