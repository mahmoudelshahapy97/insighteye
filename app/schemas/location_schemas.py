# app/schemas/location_schemas.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union

class LocationBase(BaseModel):
    location_name: str = Field(..., min_length=1, max_length=100)
    area_name: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    description: Optional[str] = None

class LocationCreate(LocationBase):
    pass

class LocationUpdate(BaseModel):
    location_name: Optional[str] = Field(None, min_length=1, max_length=100)
    area_name: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    description: Optional[str] = None
    is_active: Optional[bool] = None

class LocationResponse(LocationBase):
    location_id: str
    workspace_id: str
    is_active: bool
    created_at: str
    updated_at: str
    camera_count: Optional[int] = 0

class BulkLocationAssignment(BaseModel):
    camera_ids: List[str]
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class BulkLocationAssignmentResult(BaseModel):
    total_processed: int
    successful_assignments: int
    failed_assignments: int
    updated_cameras: List[str]
    failed_cameras: List[Dict[str, Any]]
    errors: List[str]

class BulkLocationAssignmentWithAlerts(BaseModel):
    """Model for bulk location assignment with alert settings."""
    camera_ids: List[str] = Field(..., min_items=1, description="List of camera IDs to update")
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: Optional[bool] = Field(None)

class LocationHierarchy(BaseModel):
    floor_level: Optional[str] = None
    building: Optional[str] = None
    zone: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int = 0

class LocationHierarchyResponse(BaseModel):
    workspace_id: str
    hierarchy: List[LocationHierarchy]
    total_locations: int
    floor_level: List[str] = []
    buildings: List[str] = []
    zones: List[str] = []
    areas: List[str] = []
    locations: List[str] = []

class LocationStatsResponse(BaseModel):
    workspace_id: str
    workspace_name: str
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    total_cameras: int = 0
    active_cameras: int = 0
    inactive_cameras: int = 0
    error_cameras: int = 0
    streaming_cameras: int = 0
    camera_ids: List[str] = []
    camera_names: List[str] = []

class LocationStatsResponseWithAlerts(BaseModel):
    """Enhanced location statistics response with alert information."""
    workspace_id: str = Field(..., description="Workspace ID")
    workspace_name: str = Field(..., description="Workspace name")
    location: Optional[str] = Field(None, description="Location name")
    area: Optional[str] = Field(None, description="Area name")
    building: Optional[str] = Field(None, description="Building name")
    floor_level: Optional[str] = Field(None, description="floor_level name")
    zone: Optional[str] = Field(None, description="Zone name")
    total_cameras: int = Field(..., ge=0, description="Total number of cameras")
    active_cameras: int = Field(..., ge=0, description="Number of active cameras")
    inactive_cameras: int = Field(..., ge=0, description="Number of inactive cameras")
    error_cameras: int = Field(..., ge=0, description="Number of cameras with errors")
    streaming_cameras: int = Field(..., ge=0, description="Number of streaming cameras")
    alert_enabled_cameras: int = Field(default=0, ge=0, description="Number of cameras with alerts enabled")
    camera_ids: List[str] = Field(default_factory=list, description="List of camera IDs")
    camera_names: List[str] = Field(default_factory=list, description="List of camera names")
    alert_configurations: List[Dict[str, Any]] = Field(default_factory=list, description="Alert configurations for cameras")

class CameraLocationGroup(BaseModel):
    group_type: str
    group_name: str
    camera_count: int
    cameras: List[Dict[str, Any]]
    active_count: int = 0
    inactive_count: int = 0
    streaming_count: int = 0

class CameraGroupResponse(BaseModel):
    groups: List[CameraLocationGroup]
    total_groups: int
    total_cameras: int
    group_type: str

class CameraLocationGroupWithAlerts(BaseModel):
    """Enhanced camera location group with alert information."""
    group_type: str = Field(..., description="Type of grouping (location, area, building, zone)")
    group_name: str = Field(..., description="Name of the group")
    camera_count: int = Field(..., ge=0, description="Total number of cameras in this group")
    cameras: List[Dict[str, Any]] = Field(default_factory=list, description="List of cameras in this group")
    active_count: int = Field(default=0, ge=0, description="Number of active cameras")
    inactive_count: int = Field(default=0, ge=0, description="Number of inactive cameras")
    streaming_count: int = Field(default=0, ge=0, description="Number of streaming cameras")
    alert_enabled_count: int = Field(default=0, ge=0, description="Number of cameras with alerts enabled")

class CameraGroupResponseWithAlerts(BaseModel):
    """Enhanced camera group response with alert information."""
    groups: List[CameraLocationGroupWithAlerts] = Field(default_factory=list)
    total_groups: int = Field(..., ge=0)
    total_cameras: int = Field(..., ge=0)
    group_type: str = Field(..., description="Type of grouping used")
    total_alert_enabled: int = Field(default=0, ge=0, description="Total cameras with alerts enabled")

class LocationItem(BaseModel):
    location: str
    camera_count: int

class LocationListResponse(BaseModel):
    locations: List[LocationItem]
    total_count: int

class AreaItem(BaseModel):
    area: str
    location: Optional[str] = None
    camera_count: int

class AreaListResponse(BaseModel):
    areas: List[AreaItem]
    total_count: int
    filtered_by_locations: Optional[Union[str, List[str]]] = None

class BuildingItem(BaseModel):
    building: str
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int

class BuildingListResponse(BaseModel):
    buildings: List[BuildingItem]
    total_count: int
    filtered_by_areas: Optional[Union[str, List[str]]] = None

class FloorLevelItem(BaseModel):
    floor_level: str
    building: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int

class FloorLevelListResponse(BaseModel):
    floor_levels: List[FloorLevelItem]
    total_count: int
    filtered_by_buildings: Optional[Union[str, List[str]]] = None

class ZoneItem(BaseModel):
    zone: str
    floor_level: Optional[str] = None
    building: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int

class ZoneListResponse(BaseModel):
    zones: List[ZoneItem]
    total_count: int
    filtered_by_floor_levels: Optional[Union[str, List[str]]] = None