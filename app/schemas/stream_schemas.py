# app/schemas/stream_schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Union, Dict, Any
from datetime import datetime

class CameraData(BaseModel):
    camera_id: str
    source: str
    frame_data: Optional[str] = None
    
    @validator('source')
    def source_validator(cls, value):
        value = value.replace('\\', '/')
        if not value.startswith(('http://', 'https://', 'rtsp://')) and not value.lower() == "local":
            if not value.lower() == "local":
                if not value.startswith("/"):
                    if not (len(value) > 2 and value[1:3] == ":/"):
                        raise ValueError("Source must start with 'http://' or 'https://' or 'rtsp://' or 'local' or local file path with / or with C:/")
        return value

class SearchQuery(BaseModel):
    camera_id: Optional[Union[str, List[str]]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class StreamInputItem(BaseModel):
    source: str
    camera_id: Optional[str] = None

class StreamInput(BaseModel):
    inputs: List[StreamInputItem]

class TimestampRangeResponse(BaseModel):
    first_timestamp: Optional[float] = None
    last_timestamp: Optional[float] = None
    first_datetime: Optional[str] = None
    last_datetime: Optional[str] = None
    first_date: Optional[str] = None
    last_date: Optional[str] = None
    first_time: Optional[str] = None
    last_time: Optional[str] = None
    camera_id: Optional[str] = None

class CameraIdsResponse(BaseModel):
    camera_ids: List[Union[str, int]]
    count: int

class DeleteDataRequest(BaseModel):
    camera_id: Optional[Union[str, int]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class LocationSearchQuery(BaseModel):
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    camera_id: Optional[Union[str, List[str]]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class FrameRequest(BaseModel):
    """Request model for retrieving frames"""
    stream_ids: List[str] = Field(..., description="List of stream IDs to retrieve frames for")
    limit: int = Field(default=1, ge=1, le=100, description="Number of frames per stream")
    include_metadata: bool = Field(default=True, description="Include PostgreSQL metadata")
    include_base64: bool = Field(default=True, description="Include base64 encoded frame")
    start_time: Optional[datetime] = Field(None, description="Filter frames from this time")
    end_time: Optional[datetime] = Field(None, description="Filter frames until this time")

class FrameMetadata(BaseModel):
    """Frame metadata from PostgreSQL"""
    stream_id: str
    camera_name: str
    location: Optional[str]
    area: Optional[str]
    building: Optional[str]
    floor_level: Optional[str]
    zone: Optional[str]
    timestamp: datetime
    detection_count: Optional[int] = None
    detections: Optional[List[Dict[str, Any]]] = None
    is_streaming: bool
    status: str

class FrameData(BaseModel):
    """Complete frame data"""
    frame_id: str
    stream_id: str
    timestamp: datetime
    frame_base64: Optional[str] = None
    metadata: Optional[FrameMetadata] = None
    qdrant_score: Optional[float] = None
    frame_number: Optional[int] = None

class BatchFrameResponse(BaseModel):
    """Response for batch frame retrieval"""
    total_streams: int
    total_frames: int
    frames: List[FrameData]
    errors: List[Dict[str, str]] = []
