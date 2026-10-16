"""Request models for the lost & found API. Responses are plain dicts (UUID event ids,
presigned media URLs), shaped for the Insight-eye Lost & Found tab."""

from dataclasses import fields
from typing import Dict, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.services.lost_found.abandonment import AbandonmentParams

EventStatus = Literal["open", "acknowledged", "resolved", "false_positive"]
FoundStatus = Literal["unclaimed", "claimed", "discarded"]
LostStatus = Literal["open", "matched", "resolved", "closed"]

# Per-camera overridable abandonment rules (AbandonmentParams field names).
SETTING_KEYS = tuple(f.name for f in fields(AbandonmentParams))


class ResolveLostFoundEventRequest(BaseModel):
    status: EventStatus
    action_taken: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=2000)


class LostFoundSettingsRequest(BaseModel):
    """Abandonment rule overrides for one camera; omitted keys use the server defaults."""

    settings: Dict[str, Union[bool, float]] = Field(default_factory=dict)
    is_active: Optional[bool] = None

    @field_validator("settings")
    @classmethod
    def _known_keys(cls, value: Dict[str, Union[bool, float]]) -> Dict[str, Union[bool, float]]:
        unknown = sorted(set(value) - set(SETTING_KEYS))
        if unknown:
            raise ValueError(f"Unknown settings: {', '.join(unknown)}")
        for key, v in value.items():
            if not isinstance(v, bool) and v < 0:
                raise ValueError(f"{key} must not be negative")
        return value


class FoundStatusRequest(BaseModel):
    status: FoundStatus


class LostStatusRequest(BaseModel):
    status: LostStatus


class ConfirmMatchRequest(BaseModel):
    lost_id: UUID
    found_id: UUID
    score: float = Field(1.0, ge=0.0, le=1.0)
