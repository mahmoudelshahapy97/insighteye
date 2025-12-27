# app/schemas/stream_schemas.py
"""
Pydantic schemas for stream service endpoints.
"""
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from uuid import UUID


# ==================== Request Schemas ====================

class StreamStartRequest(BaseModel):
    """Request to start a stream."""
    stream_id: str = Field(..., description="UUID of the stream to start")
    
    @validator('stream_id')
    def validate_stream_id(cls, v):
        """Validate stream_id is a valid UUID."""
        try:
            UUID(v)
            return v
        except ValueError:
            raise ValueError("Invalid stream_id format")


class StreamStopRequest(BaseModel):
    """Request to stop a stream."""
    stream_id: str = Field(..., description="UUID of the stream to stop")
    
    @validator('stream_id')
    def validate_stream_id(cls, v):
        """Validate stream_id is a valid UUID."""
        try:
            UUID(v)
            return v
        except ValueError:
            raise ValueError("Invalid stream_id format")


# ==================== Response Schemas ====================

class StreamTimingInfo(BaseModel):
    """Stream timing information."""
    start_time: Optional[str] = None
    last_frame_time: Optional[str] = None
    uptime_seconds: float = 0
    uptime_formatted: str = "0h 0m"


class StreamPerformanceMetrics(BaseModel):
    """Stream performance metrics."""
    frames_processed: int = 0
    detection_count: int = 0
    avg_processing_time: float = 0.0
    fps: float = 0.0
    errors: int = 0


class StreamClientInfo(BaseModel):
    """Stream client connection information."""
    count: int = 0
    connected: bool = False


class StreamErrorInfo(BaseModel):
    """Stream error information."""
    timestamp: str
    operation: str
    error: str


class StreamStatusResponse(BaseModel):
    """Detailed stream status response."""
    stream_id: str
    status: str
    state: str
    camera_name: Optional[str] = None
    workspace_id: Optional[str] = None
    owner: Optional[str] = None
    source: Optional[str] = None
    timing: StreamTimingInfo
    performance: StreamPerformanceMetrics
    clients: StreamClientInfo
    recent_errors: List[StreamErrorInfo] = []
    task_status: str


class StreamListItem(BaseModel):
    """Individual stream in list."""
    stream_id: str
    name: str
    type: str
    status: str
    is_streaming: bool
    is_active_in_memory: bool
    workspace_id: str
    workspace_name: str
    owner_id: str
    location: Optional[str] = None
    created_at: Optional[str] = None


class WorkspaceSummary(BaseModel):
    """Summary of workspace streams."""
    workspace_id: str
    workspace_name: str
    total_streams: int
    active_streams: int
    in_memory_streams: int


class StreamListResponse(BaseModel):
    """Response for list streams endpoint."""
    streams: List[StreamListItem]
    total: int
    workspaces: List[WorkspaceSummary]


# ==================== Workspace Analytics ====================

class WorkspaceInfo(BaseModel):
    """Workspace information."""
    id: str
    name: str
    is_active: bool


class WorkspaceLimits(BaseModel):
    """Workspace stream limits."""
    total_camera_limit: int
    active_members: int
    subscribed_members: int
    current_active: int
    available_slots: int


class WorkspaceStreamStats(BaseModel):
    """Workspace stream statistics."""
    total: int
    streaming_now: int
    alerts_enabled: int
    types: Dict[str, int]
    locations: Dict[str, int]


class WorkspacePerformance(BaseModel):
    """Workspace performance metrics."""
    total_frames_processed: int
    total_detections: int
    total_errors: int
    error_rate: float
    avg_detections_per_stream: float
    recent_errors: int


class WorkspaceStreamAnalytics(BaseModel):
    """Comprehensive workspace stream analytics."""
    workspace: WorkspaceInfo
    limits: WorkspaceLimits
    streams: WorkspaceStreamStats
    performance: WorkspacePerformance
    timestamp: str


# ==================== System Diagnostics ====================

class SystemMetrics(BaseModel):
    """System-wide metrics."""
    total_workspaces: int
    total_streams: int
    total_streaming: int
    total_shared_sources: int
    active_shared_sources: int
    total_errors: int
    state_distribution: Dict[str, int]


class SystemPerformance(BaseModel):
    """System performance metrics."""
    total_started: int
    total_stopped: int
    total_errors: int
    avg_duration: float
    uptime: float


class MemoryState(BaseModel):
    """System memory state."""
    active_streams_count: int
    workspace_registrations: int
    notification_subscribers: int
    shared_registry_size: int
    cache_size: int


class HealthStatus(BaseModel):
    """System health status."""
    last_healthcheck: str
    healthcheck_interval: int


class SystemDiagnostics(BaseModel):
    """Comprehensive system diagnostics."""
    timestamp: str
    system: SystemMetrics
    performance: SystemPerformance
    workspaces: List[WorkspaceStreamAnalytics]
    sharing_stats: Dict[str, Any]
    memory_state: MemoryState
    health: HealthStatus


# ==================== Active Stream Info ====================

class ActiveStreamInfo(BaseModel):
    """Information about an active stream."""
    stream_id: str
    camera_name: str
    status: str
    state: str
    owner_username: str
    start_time: Optional[str] = None
    last_frame_time: Optional[str] = None
    client_count: int
    stats: Dict[str, Any] = {}


class ActiveStreamsResponse(BaseModel):
    """Response for active streams endpoint."""
    active_streams: List[ActiveStreamInfo]
    count: int


# ==================== Workspace Limits Response ====================

class WorkspaceLimitsResponse(BaseModel):
    """Response for workspace limits endpoint."""
    total_camera_limit: int
    active_members: int
    subscribed_members: int
    current_active: int
    available_slots: int


# ==================== WebSocket Message Schemas ====================

class WSMessageBase(BaseModel):
    """Base WebSocket message."""
    type: str
    timestamp: Optional[str] = None


class WSConnectedMessage(WSMessageBase):
    """WebSocket connected message."""
    type: str = "connected"
    stream_id: Optional[str] = None
    message: Optional[str] = None


class WSErrorMessage(WSMessageBase):
    """WebSocket error message."""
    type: str = "error"
    message: str
    code: Optional[int] = None


class WSStreamEndedMessage(WSMessageBase):
    """WebSocket stream ended message."""
    type: str = "stream_ended"
    message: str


class WSNotificationMessage(WSMessageBase):
    """WebSocket notification message."""
    type: str = "notification"
    notification_id: str
    status: str
    message: str
    camera_name: Optional[str] = None
    stream_id: Optional[str] = None


class WSPingMessage(WSMessageBase):
    """WebSocket ping message."""
    type: str = "ping"


class WSPongMessage(WSMessageBase):
    """WebSocket pong message."""
    type: str = "pong"


class WSSubscriptionConfirmed(WSMessageBase):
    """WebSocket subscription confirmed message."""
    type: str = "subscription_confirmed"
    for_user_id: str


# ==================== Bulk Operations ====================

class BulkStopResult(BaseModel):
    """Result of bulk stop operation."""
    stream_id: str
    camera_name: Optional[str] = None
    error: Optional[str] = None


class BulkStopResponse(BaseModel):
    """Response for bulk stop operation."""
    stopped_count: int
    total_count: int
    failed_streams: List[BulkStopResult]


# ==================== Error Recovery ====================

class RecoveryResponse(BaseModel):
    """Response for stream recovery operation."""
    success: bool
    message: str
    action: str
    data: Optional[Dict[str, Any]] = None


# ==================== Health Check ====================

class HealthCheckResponse(BaseModel):
    """Response for health check operation."""
    success: bool
    message: str
    timestamp: str
    streams_checked: Optional[int] = None
    issues_found: Optional[int] = None
    streams_restarted: Optional[int] = None


# ==================== Metrics Response ====================

class PerformanceMetrics(BaseModel):
    """System performance metrics."""
    total_streams_started: int
    total_streams_stopped: int
    total_errors: int
    avg_stream_duration: float
    last_reset: str


class SystemMetricsResponse(BaseModel):
    """Response for system metrics endpoint."""
    performance: PerformanceMetrics
    active_streams: int
    workspace_count: int
    notification_subscribers: int
    shared_sources: int
    timestamp: str


# ==================== Stream Operation Responses ====================

class StreamOperationResponse(BaseModel):
    """Generic stream operation response."""
    success: bool
    message: str
    data: Dict[str, Any]


class StreamStartResponse(StreamOperationResponse):
    """Response for stream start operation."""
    pass


class StreamStopResponse(StreamOperationResponse):
    """Response for stream stop operation."""
    pass


class StreamRestartResponse(StreamOperationResponse):
    """Response for stream restart operation."""
    pass


# ==================== Validation Helpers ====================

def validate_uuid_string(v: str, field_name: str = "id") -> str:
    """Validate UUID string format."""
    try:
        UUID(v)
        return v
    except ValueError:
        raise ValueError(f"Invalid {field_name} format: must be a valid UUID")


def validate_workspace_id(cls, v):
    """Validator for workspace_id fields."""
    return validate_uuid_string(v, "workspace_id")


def validate_stream_id(cls, v):
    """Validator for stream_id fields."""
    return validate_uuid_string(v, "stream_id")


def validate_user_id(cls, v):
    """Validator for user_id fields."""
    return validate_uuid_string(v, "user_id")


# ==================== Additional Stream Info ====================

class StreamLocationInfo(BaseModel):
    """Stream location information."""
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class DetailedStreamInfo(BaseModel):
    """Detailed stream information."""
    stream_id: str
    name: str
    type: str
    status: str
    is_streaming: bool
    workspace_id: str
    workspace_name: str
    owner_id: str
    owner_username: str
    location_info: StreamLocationInfo
    alert_enabled: bool
    count_threshold_greater: Optional[int] = None
    count_threshold_less: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_activity: Optional[str] = None


class DetailedStreamListResponse(BaseModel):
    """Response with detailed stream information."""
    streams: List[DetailedStreamInfo]
    total: int
    workspace_id: Optional[str] = None


# ==================== Stream State ====================

class StreamStateInfo(BaseModel):
    """Stream state information."""
    stream_id: str
    state: str
    status: str
    is_streaming: bool
    in_memory: bool
    last_transition: Optional[str] = None


class StreamStatesResponse(BaseModel):
    """Response for stream states."""
    states: List[StreamStateInfo]
    total: int


# ==================== Error Information ====================

class DetailedErrorInfo(BaseModel):
    """Detailed error information."""
    stream_id: str
    camera_name: str
    error_count: int
    last_error: Optional[str] = None
    last_error_time: Optional[str] = None
    recent_errors: List[StreamErrorInfo]


class StreamErrorsResponse(BaseModel):
    """Response for stream errors."""
    errors: List[DetailedErrorInfo]
    total_streams_with_errors: int
    total_error_count: int


# ==================== Configuration ====================

class StreamConfiguration(BaseModel):
    """Stream configuration."""
    frame_delay: float
    frame_skip: int
    conf_threshold: float
    alert_enabled: bool
    count_threshold_greater: Optional[int] = None
    count_threshold_less: Optional[int] = None


class StreamConfigurationResponse(BaseModel):
    """Response for stream configuration."""
    stream_id: str
    configuration: StreamConfiguration
    can_modify: bool


# ==================== Shared Stream Info ====================

class SharedStreamSubscriber(BaseModel):
    """Shared stream subscriber info."""
    stream_id: str
    camera_name: str
    frames_received: int
    last_frame_time: Optional[str] = None


class SharedStreamInfo(BaseModel):
    """Information about a shared video stream."""
    source: str
    subscriber_count: int
    is_running: bool
    frame_count: int
    last_frame_time: float
    reconnect_attempts: int
    last_error: Optional[str] = None
    error_count: int
    subscribers: List[SharedStreamSubscriber]


class SharedStreamsResponse(BaseModel):
    """Response for shared streams information."""
    shared_streams: List[SharedStreamInfo]
    total_sources: int
    total_subscribers: int


# ==================== Export Config ====================

class StreamDataExportConfig(BaseModel):
    """Configuration for stream data export."""
    stream_ids: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    format: str = "json"  # json, csv, excel
    include_frames: bool = False
    include_metadata: bool = True


# ==================== Batch Operations ====================

class BatchStreamOperation(BaseModel):
    """Batch stream operation request."""
    stream_ids: List[str]
    operation: Optional[str]= ""  # start, stop, restart, recover
    
    @validator('stream_ids')
    def validate_stream_ids(cls, v):
        """Validate all stream IDs."""
        for stream_id in v:
            validate_uuid_string(stream_id, "stream_id")
        return v


class BatchOperationResult(BaseModel):
    """Result of a single operation in batch."""
    stream_id: str
    success: bool
    message: str
    error: Optional[str] = None


class BatchOperationResponse(BaseModel):
    """Response for batch operations."""
    total_requested: int
    successful: int
    failed: int
    results: List[BatchOperationResult]