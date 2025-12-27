# app/schemas/alert_schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any

class ThresholdSettings(BaseModel):
    count_threshold_greater: Optional[int] = None
    count_threshold_less: Optional[int] = None
    alert_enabled: bool = False

class CameraAlertSettings(BaseModel):
    """Model for camera alert threshold settings."""
    count_threshold_greater: Optional[int] = Field(None, ge=0, description="Alert when count is greater than this value")
    count_threshold_less: Optional[int] = Field(None, ge=0, description="Alert when count is less than this value")
    alert_enabled: bool = Field(False, description="Whether alerts are enabled for this camera")
    
    @validator('count_threshold_greater', 'count_threshold_less')
    def validate_thresholds(cls, v):
        if v is not None and v < 0:
            raise ValueError('Threshold values must be non-negative')
        return v
    
    @validator('count_threshold_less')
    def validate_threshold_logic(cls, v, values):
        if v is not None and 'count_threshold_greater' in values and values['count_threshold_greater'] is not None:
            if v >= values['count_threshold_greater']:
                raise ValueError('count_threshold_less must be less than count_threshold_greater')
        return v

class CameraCSVRecordWithLocationAndAlerts(BaseModel):
    """Model for CSV record with location and alert data."""
    name: str = Field(..., min_length=1, max_length=50)
    path: str = Field(..., min_length=1, max_length=255)
    type: str = Field(default="local")
    status: str = Field(default="inactive")
    is_streaming: bool = Field(default=False)
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: bool = Field(default=False)
    
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
    
    @validator('count_threshold_less')
    def validate_threshold_logic(cls, v, values):
        if v is not None and 'count_threshold_greater' in values and values['count_threshold_greater'] is not None:
            if v >= values['count_threshold_greater']:
                raise ValueError('count_threshold_less must be less than count_threshold_greater')
        return v

class AlertSummaryResponse(BaseModel):
    """Response model for alert summary."""
    total_cameras: int = Field(..., ge=0, description="Total number of cameras")
    alert_enabled_cameras: int = Field(..., ge=0, description="Number of cameras with alerts enabled")
    cameras: List[Dict[str, Any]] = Field(default_factory=list, description="List of cameras with alert information")

class CameraAlertUpdateResponse(BaseModel):
    """Response model for camera alert update."""
    message: str = Field(..., description="Success message")
    camera_id: str = Field(..., description="Camera ID")
    camera_name: str = Field(..., description="Camera name")
    alert_settings: CameraAlertSettings = Field(..., description="Updated alert settings")