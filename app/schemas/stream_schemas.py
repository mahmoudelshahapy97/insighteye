# app/schemas/stream_schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Union

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
    