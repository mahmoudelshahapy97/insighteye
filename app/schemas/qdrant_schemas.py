# app/schemas/qdrant_schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

# ============================================================================
# COLLECTION MANAGEMENT SCHEMAS
# ============================================================================

class VectorParams(BaseModel):
    """Parameters for vector configuration."""
    size: int = Field(..., ge=1, le=65536, description="Vector dimension size")
    distance: str = Field(..., description="Distance metric: 'Cosine', 'Euclid', or 'Dot'")
    on_disk: bool = Field(default=False, description="Store vectors on disk")
    
    @validator('distance')
    def validate_distance(cls, v):
        allowed_distances = ['Cosine', 'Euclid', 'Dot', 'Manhattan']
        if v not in allowed_distances:
            raise ValueError(f'Distance must be one of: {allowed_distances}')
        return v


class VectorConfig(BaseModel):
    """Detailed vector configuration."""
    size: int = Field(..., ge=1, le=65536)
    distance: str = Field(..., description="Distance metric")
    on_disk: bool = Field(default=False)
    hnsw_config: Optional[Dict[str, Any]] = Field(None, description="HNSW index configuration")
    quantization_config: Optional[Dict[str, Any]] = Field(None, description="Quantization settings")
    
    @validator('distance')
    def validate_distance(cls, v):
        allowed_distances = ['Cosine', 'Euclid', 'Dot', 'Manhattan']
        if v not in allowed_distances:
            raise ValueError(f'Distance must be one of: {allowed_distances}')
        return v


class CreateCollectionRequest(BaseModel):
    """Basic collection creation request."""
    collection_name: str = Field(..., min_length=1, max_length=255)
    vector_size: int = Field(default=1, ge=1, le=65536)
    distance: str = Field(default="DOT", description="Distance metric")
    
    @validator('collection_name')
    def validate_collection_name(cls, v):
        # Collection name should not contain special characters
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Collection name can only contain letters, numbers, underscores, and hyphens')
        return v
    
    @validator('distance')
    def validate_distance(cls, v):
        allowed = ['DOT', 'COSINE', 'EUCLID', 'MANHATTAN']
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f'Distance must be one of: {allowed}')
        return v_upper


class CreateCollectionRequest1(BaseModel):
    """Extended collection creation request with additional options."""
    collection_name: str = Field(..., min_length=1, max_length=255)
    vector_size: int = Field(default=1, ge=1, le=65536)
    distance: str = Field(default="Dot")
    on_disk: bool = Field(default=False)
    custom_params: Optional[Dict[str, Any]] = Field(None, description="Additional custom parameters")
    
    @validator('collection_name')
    def validate_collection_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Collection name can only contain letters, numbers, underscores, and hyphens')
        return v


class AdvancedCreateCollectionRequest(BaseModel):
    """Advanced collection creation with full configuration options."""
    collection_name: str = Field(..., min_length=1, max_length=255)
    vectors_config: Optional[Dict[str, VectorConfig]] = Field(None, description="Named vector configurations")
    single_vector_config: Optional[VectorConfig] = Field(None, description="Single vector configuration")
    shard_number: Optional[int] = Field(None, ge=1, le=1000, description="Number of shards")
    replication_factor: Optional[int] = Field(None, ge=1, le=10, description="Replication factor")
    write_consistency_factor: Optional[int] = Field(None, ge=1, description="Write consistency")
    on_disk_payload: Optional[bool] = Field(None, description="Store payload on disk")
    hnsw_config: Optional[Dict[str, Any]] = Field(None, description="HNSW algorithm configuration")
    optimizers_config: Optional[Dict[str, Any]] = Field(None, description="Indexing optimizer settings")
    wal_config: Optional[Dict[str, Any]] = Field(None, description="Write-ahead log settings")
    quantization_config: Optional[Dict[str, Any]] = Field(None, description="Vector quantization settings")
    
    @validator('collection_name')
    def validate_collection_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Collection name can only contain letters, numbers, underscores, and hyphens')
        return v
    
    @validator('vectors_config', 'single_vector_config')
    def validate_vector_config(cls, v, values):
        # Ensure at least one vector config is provided
        if v is None and 'vectors_config' not in values and 'single_vector_config' not in values:
            raise ValueError('Either vectors_config or single_vector_config must be provided')
        return v


class DeleteCollectionRequest(BaseModel):
    """Request to delete a collection."""
    collection_name: str = Field(..., min_length=1)
    force: bool = Field(default=False, description="Force delete even if collection has data")


class CollectionInfo(BaseModel):
    """Information about a collection."""
    name: str
    vector_size: int
    distance: str
    points_count: int = 0
    indexed_vectors_count: Optional[int] = None
    segments_count: Optional[int] = None
    status: str = "green"
    optimizer_status: Optional[str] = None


class CollectionListResponse(BaseModel):
    """Response containing list of collections."""
    collections: List[CollectionInfo]
    total_count: int


# ============================================================================
# POINT (DATA) MANAGEMENT SCHEMAS
# ============================================================================

class PointInsertRequest(BaseModel):
    """Request to insert points into collection."""
    collection_name: str
    points: List[Dict[str, Any]] = Field(..., min_items=1)
    wait: bool = Field(default=True, description="Wait for operation to complete")


class PointUpdateRequest(BaseModel):
    """Request to update points in collection."""
    collection_name: str
    points: List[Dict[str, Any]]
    wait: bool = Field(default=True)


class PointDeleteRequest(BaseModel):
    """Request to delete points from collection."""
    collection_name: str
    points: List[Union[str, int]] = Field(..., min_items=1, description="Point IDs to delete")
    wait: bool = Field(default=True)


class PointSearchRequest(BaseModel):
    """Request to search for similar points."""
    collection_name: str
    vector: List[float] = Field(..., description="Query vector")
    limit: int = Field(default=10, ge=1, le=1000)
    score_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    filter: Optional[Dict[str, Any]] = None
    with_payload: bool = Field(default=True)
    with_vector: bool = Field(default=False)


# ============================================================================
# LOCATION-BASED SCHEMAS
# ============================================================================

class QdrantLocationItem(BaseModel):
    """Location item from Qdrant with camera information."""
    location: str = Field(..., description="Location name")
    camera_count: int = Field(..., ge=0, description="Number of cameras")
    camera_ids: List[str] = Field(default_factory=list, description="List of camera IDs")


class QdrantLocationListResponse(BaseModel):
    """Response containing list of locations."""
    locations: List[QdrantLocationItem]
    total_count: int = Field(..., ge=0)
    workspace_id: str


class QdrantAreaItem(BaseModel):
    """Area item from Qdrant with camera information."""
    area: str = Field(..., description="Area name")
    location: Optional[str] = Field(None, description="Parent location")
    camera_count: int = Field(..., ge=0)
    camera_ids: List[str] = Field(default_factory=list)


class QdrantAreaListResponse(BaseModel):
    """Response containing list of areas."""
    areas: List[QdrantAreaItem]
    total_count: int = Field(..., ge=0)
    filtered_by_locations: Optional[Union[str, List[str]]] = Field(None, description="Filter applied")
    workspace_id: str


class QdrantBuildingItem(BaseModel):
    """Building item from Qdrant with camera information."""
    building: str = Field(..., description="Building name")
    area: Optional[str] = Field(None, description="Parent area")
    location: Optional[str] = Field(None, description="Parent location")
    camera_count: int = Field(..., ge=0)
    camera_ids: List[str] = Field(default_factory=list)


class QdrantBuildingListResponse(BaseModel):
    """Response containing list of buildings."""
    buildings: List[QdrantBuildingItem]
    total_count: int = Field(..., ge=0)
    filtered_by_areas: Optional[Union[str, List[str]]] = None
    workspace_id: str


class QdrantFloorLevelItem(BaseModel):
    """Floor level item from Qdrant with camera information."""
    floor_level: str = Field(..., description="Floor level name")
    building: Optional[str] = Field(None, description="Parent building")
    area: Optional[str] = Field(None, description="Parent area")
    location: Optional[str] = Field(None, description="Parent location")
    camera_count: int = Field(..., ge=0)
    camera_ids: List[str] = Field(default_factory=list)


class QdrantFloorLevelListResponse(BaseModel):
    """Response containing list of floor levels."""
    floor_levels: List[QdrantFloorLevelItem]
    total_count: int = Field(..., ge=0)
    filtered_by_buildings: Optional[Union[str, List[str]]] = None
    workspace_id: str


class QdrantZoneItem(BaseModel):
    """Zone item from Qdrant with camera information."""
    zone: str = Field(..., description="Zone name")
    floor_level: Optional[str] = Field(None, description="Parent floor level")
    building: Optional[str] = Field(None, description="Parent building")
    area: Optional[str] = Field(None, description="Parent area")
    location: Optional[str] = Field(None, description="Parent location")
    camera_count: int = Field(..., ge=0)
    camera_ids: List[str] = Field(default_factory=list)


class QdrantZoneListResponse(BaseModel):
    """Response containing list of zones."""
    zones: List[QdrantZoneItem]
    total_count: int = Field(..., ge=0)
    filtered_by_floor_levels: Optional[Union[str, List[str]]] = None
    workspace_id: str


# ============================================================================
# ANALYTICS SCHEMAS
# ============================================================================

class TimeRange(BaseModel):
    """Time range for analytics data."""
    earliest: Optional[str] = Field(None, description="Earliest timestamp (ISO format)")
    latest: Optional[str] = Field(None, description="Latest timestamp (ISO format)")
    
    @validator('earliest', 'latest')
    def validate_datetime_format(cls, v):
        if v is not None:
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                raise ValueError('Timestamp must be in ISO format')
        return v


class QdrantLocationAnalyticsItem(BaseModel):
    """Analytics item for a specific location/area/building/floor/zone."""
    location: Optional[str] = Field(None, description="Location name")
    area: Optional[str] = Field(None, description="Area name")
    building: Optional[str] = Field(None, description="Building name")
    floor_level: Optional[str] = Field(None, description="Floor level name")
    zone: Optional[str] = Field(None, description="Zone name")
    data_points: int = Field(..., ge=0, description="Number of data points")
    total_person_count: int = Field(..., ge=0, description="Total person count across all data points")
    average_person_count: float = Field(..., ge=0.0, description="Average person count")
    max_person_count: Optional[int] = Field(None, ge=0, description="Maximum person count")
    min_person_count: Optional[int] = Field(None, ge=0, description="Minimum person count")
    unique_cameras: int = Field(..., ge=0, description="Number of unique cameras")
    camera_ids: List[str] = Field(default_factory=list, description="List of camera IDs")
    time_range: TimeRange = Field(..., description="Time range of data")


class QdrantLocationAnalyticsResponse(BaseModel):
    """Response containing location-based analytics."""
    analytics: List[QdrantLocationAnalyticsItem]
    total_groups: int = Field(..., ge=0)
    group_by: str = Field(..., description="Grouping dimension (location, area, building, floor_level, zone)")
    filters_applied: Dict[str, Any] = Field(default_factory=dict, description="Applied filters")
    workspace_id: str
    summary: Optional[Dict[str, Any]] = Field(None, description="Summary statistics")


class QdrantCameraAnalyticsItem(BaseModel):
    """Analytics item for a specific camera."""
    camera_id: str
    camera_name: Optional[str] = None
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    data_points: int = Field(..., ge=0)
    total_person_count: int = Field(..., ge=0)
    average_person_count: float = Field(..., ge=0.0)
    max_person_count: Optional[int] = Field(None, ge=0)
    min_person_count: Optional[int] = Field(None, ge=0)
    time_range: TimeRange


class QdrantCameraAnalyticsResponse(BaseModel):
    """Response containing camera-specific analytics."""
    analytics: List[QdrantCameraAnalyticsItem]
    total_cameras: int = Field(..., ge=0)
    filters_applied: Dict[str, Any] = Field(default_factory=dict)
    workspace_id: str


# ============================================================================
# QUERY/FILTER SCHEMAS
# ============================================================================

class QdrantFilterRequest(BaseModel):
    """Request for filtering Qdrant data."""
    collection_name: str
    filter: Dict[str, Any] = Field(..., description="Qdrant filter conditions")
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)
    with_payload: bool = Field(default=True)
    with_vector: bool = Field(default=False)


class QdrantScrollRequest(BaseModel):
    """Request for scrolling through points."""
    collection_name: str
    limit: int = Field(default=100, ge=1, le=10000)
    offset: Optional[str] = Field(None, description="Offset point ID")
    filter: Optional[Dict[str, Any]] = None
    with_payload: bool = Field(default=True)
    with_vector: bool = Field(default=False)


class QdrantCountRequest(BaseModel):
    """Request to count points matching filter."""
    collection_name: str
    filter: Optional[Dict[str, Any]] = None
    exact: bool = Field(default=True, description="Return exact count (slower) or approximate")


class QdrantCountResponse(BaseModel):
    """Response containing point count."""
    count: int = Field(..., ge=0)
    exact: bool


# ============================================================================
# AGGREGATION SCHEMAS
# ============================================================================

class AggregationRequest(BaseModel):
    """Request for data aggregation."""
    collection_name: str
    group_by: str = Field(..., description="Field to group by (location, area, building, etc.)")
    filters: Optional[Dict[str, Any]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    aggregation_type: str = Field(default="sum", description="sum, avg, min, max, count")
    
    @validator('aggregation_type')
    def validate_aggregation_type(cls, v):
        allowed = ['sum', 'avg', 'average', 'min', 'max', 'count']
        if v.lower() not in allowed:
            raise ValueError(f'Aggregation type must be one of: {allowed}')
        return v.lower()


class TimeSeriesRequest(BaseModel):
    """Request for time-series data."""
    collection_name: str
    camera_ids: Optional[List[str]] = None
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    start_datetime: Optional[str] = None
    end_datetime: Optional[str] = None
    interval: str = Field(default="1h", description="Time interval (1m, 5m, 1h, 1d, etc.)")
    
    @validator('start_datetime', 'end_datetime')
    def validate_datetime(cls, v):
        if v is not None:
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                raise ValueError('Datetime must be in ISO format')
        return v


class TimeSeriesDataPoint(BaseModel):
    """Single data point in time series."""
    timestamp: str
    value: float
    camera_id: Optional[str] = None
    location_info: Optional[Dict[str, Any]] = None


class TimeSeriesResponse(BaseModel):
    """Response containing time-series data."""
    data_points: List[TimeSeriesDataPoint]
    total_points: int
    interval: str
    start_datetime: Optional[str] = None
    end_datetime: Optional[str] = None
    filters_applied: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# BACKUP/RESTORE SCHEMAS
# ============================================================================

class BackupRequest(BaseModel):
    """Request to create collection backup."""
    collection_name: str
    backup_name: Optional[str] = Field(None, description="Name for the backup")


class RestoreRequest(BaseModel):
    """Request to restore collection from backup."""
    collection_name: str
    backup_name: str
    overwrite: bool = Field(default=False, description="Overwrite existing collection")


class SnapshotInfo(BaseModel):
    """Information about a collection snapshot."""
    name: str
    size_bytes: int
    created_at: str
    collection_name: str


# ============================================================================
# UTILITY RESPONSE SCHEMAS
# ============================================================================

class OperationResponse(BaseModel):
    """Generic operation response."""
    success: bool
    message: str
    operation_id: Optional[str] = None
    affected_count: Optional[int] = None


class HealthCheckResponse(BaseModel):
    """Qdrant health check response."""
    status: str = Field(..., description="ok, error")
    version: Optional[str] = None
    collections_count: Optional[int] = None


class ClusterInfoResponse(BaseModel):
    """Qdrant cluster information."""
    peer_id: int
    shard_count: int
    pending_operations_count: int
    raft_info: Optional[Dict[str, Any]] = None