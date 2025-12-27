# app/schemas/camera_schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from decimal import Decimal
from datetime import datetime
from uuid import UUID

class StreamQueryParams(BaseModel):
    frame_delay: Optional[float] = 0
    frame_skip: Optional[int] = 300
    conf: Optional[float] = 0.4

class CameraStreamQueryParams(BaseModel):
    frame_delay: Optional[float] = 0
    frame_skip: Optional[int] = 300
    conf: Optional[float] = 0.4

class StreamCreate(BaseModel):
    name: str = Field(..., max_length=50)
    path: str = Field(..., max_length=255)
    type: str = Field(default='local', pattern='^(rtsp|http|local|other|video file)$')
    status: str = Field(default='inactive', pattern='^(active|inactive|error|processing)$')
    is_streaming: bool = Field(default=False)
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[Decimal] = Field(None, ge=-90, le=90, decimal_places=8)
    longitude: Optional[Decimal] = Field(None, ge=-180, le=180, decimal_places=8)
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: bool = Field(default=False)

class StreamUpdate(BaseModel):
    id: str = Field(...)
    name: str = Field(..., max_length=50)
    path: str = Field(..., max_length=255)
    type: str = Field(default='local', pattern='^(rtsp|http|local|other|video file)$')
    status: str = Field(default='inactive', pattern='^(active|inactive|error|processing)$')
    is_streaming: bool = Field(default=False)
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[Decimal] = Field(None, ge=-90, le=90, decimal_places=8)
    longitude: Optional[Decimal] = Field(None, ge=-180, le=180, decimal_places=8)
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: bool = Field(default=False)

class StreamUpdateList(BaseModel):
    streams: List[StreamUpdate]

class StreamDelete(BaseModel):
    ids: List[str]

class CameraState(BaseModel):
    id: str
    name: str
    status: str = Field("inactive", description="Status: active, inactive")
    is_streaming: bool = False

class CamerasStateResponse(BaseModel):
    cameras: List[CameraState]
    total_active: Optional[int]
    total_inactive: Optional[int]
    total_error: Optional[int]
    total_processing: Optional[int]
    total_cameras: Optional[int]

class StreamCreateWithLocation(BaseModel):
    name: str
    path: str
    type: str = Field("local", pattern="^(rtsp|http|local|other|video file)$")
    status: str = Field("inactive", description="Status: active, inactive")
    is_streaming: bool = False
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)

class StreamUpdateWithLocation(BaseModel):
    id: str
    name: Optional[str] = None
    path: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = Field(None, description="Status: active, inactive")
    is_streaming: Optional[bool] = None
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)

class CameraCSVRecord(BaseModel):
    name: str
    path: str
    type: str = "local"
    status: str = "inactive"
    is_streaming: bool = False
    
    @validator('type')
    def validate_type(cls, v):
        allowed_types = ['rtsp', 'http', 'local', 'other', 'video file']
        if v not in allowed_types:
            raise ValueError(f'Type must be one of: {allowed_types}')
        return v
    
    @validator('status')
    def validate_status(cls, v):
        allowed_statuses = ['active', 'inactive', 'error', 'processing']
        if v not in allowed_statuses:
            raise ValueError(f'Status must be one of: {allowed_statuses}')
        return v

class CameraCSVRecordWithLocation(BaseModel):
    name: str
    path: str
    type: str = "local"
    status: str = "inactive"
    is_streaming: bool = False
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    
    @validator('type')
    def validate_type(cls, v):
        allowed_types = ['rtsp', 'http', 'local', 'other', 'video file']
        if v not in allowed_types:
            raise ValueError(f'Type must be one of: {allowed_types}')
        return v


class CameraDetailedResponse(BaseModel):
    """Detailed camera response with alert settings."""
    stream_id: str = Field(..., description="Camera stream ID")
    name: str = Field(..., description="Camera name")
    path: str = Field(..., description="Camera path/URL")
    type: str = Field(..., description="Camera type")
    status: str = Field(..., description="Camera status")
    is_streaming: bool = Field(..., description="Whether camera is streaming")
    location: Optional[str] = Field(None, description="Camera location")
    area: Optional[str] = Field(None, description="Camera area")
    building: Optional[str] = Field(None, description="Camera building")
    floor_level: Optional[str] = Field(None, description="Camera floor level")
    zone: Optional[str] = Field(None, description="Camera zone")
    latitude: Optional[float] = Field(None, description="Camera latitude")
    longitude: Optional[float] = Field(None, description="Camera longitude")
    count_threshold_greater: Optional[int] = Field(None, description="Alert threshold for greater count")
    count_threshold_less: Optional[int] = Field(None, description="Alert threshold for less count")
    alert_enabled: bool = Field(default=False, description="Whether alerts are enabled")
    created_at: datetime = Field(..., description="Camera creation timestamp")
    updated_at: datetime = Field(..., description="Camera last update timestamp")
    last_activity: datetime = Field(..., description="Camera last activity timestamp")
    owner_username: str = Field(..., description="Camera owner username")

class CameraBulkUploadResult(BaseModel):
    total_processed: int
    successful_uploads: int
    failed_uploads: int
    successful_cameras: List[Dict[str, Any]]
    failed_cameras: List[Dict[str, Any]]
    errors: List[str]

class UserCameraCountUpdate(BaseModel):
    """Schema for updating user camera count limit"""
    user_id: str = Field(..., description="User ID to update")
    count_of_camera: int = Field(..., ge=0, le=1000, description="New camera count limit (0-1000)")
    
    @validator('count_of_camera')
    def validate_camera_count(cls, v):
        if v < 0:
            raise ValueError('Camera count cannot be negative')
        if v > 1000:
            raise ValueError('Camera count cannot exceed 1000')
        return v


class UserCameraCountResponse(BaseModel):
    """Schema for camera count update response"""
    user_id: str
    username: str
    count_of_camera: int
    previous_count: int
    message: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "username": "john_doe",
                "count_of_camera": 10,
                "previous_count": 5,
                "message": "Camera limit updated successfully from 5 to 10"
            }
        }
        