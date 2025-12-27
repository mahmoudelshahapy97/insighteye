# __init__.py
"""
Schemas package - organized Pydantic models for the application.

Structure:
- config.py: Application settings and configuration
- auth_schemas.py: Authentication and token management
- user_schemas.py: User management
- session_schemas.py: Session management
- workspace_schemas.py: Workspace and member management
- otp_schemas.py: OTP generation and verification
- log_schemas.py: Logging and audit trails
- chat_schemas.py: Chat and conversation management
- camera_schemas.py: Camera and stream management
- alert_schemas.py: Alert and threshold management
- location_schemas.py: Location hierarchy and management
- stream_schemas.py: Stream data and search
- stream_schemas1.py: Stream data and search
- qdrant_schemas.py: Vector database operations
- system_schemas.py: System information and metadata
- utils.py: Validation and helper functions
"""

# Config
from app.schemas.config import Settings

# Authentication
from app.schemas.auth_schemas import (
    PKCERequest,
    TokenPair,
    TokenData,
    RefreshTokenRequest,
    CreateTokenRequest,
    VerifyTokenRequest,
    RevokeTokenRequest,
    InvalidateTokenRequest,
    BlacklistTokenRequest,
    TokenVerifyResponse,
    TokenIdResponse,
    TokenMessageResponse,
    BlacklistCheckResponse,
    MaintenanceResponse,
    TokenStats,
    TokenInfo,
    TokenListResponse,
)

# User Management
from app.schemas.user_schemas import (
    CreateUserRequest,
    LoginRequest,
    UpdatePasswordRequest,
    EmailRequest,
    UserIdRequest,
    UserRequest,
    VerifyPasswordRequest,
    ResetPasswordRequest,
    UserResponse,
    GetUserIdResponse,
    GetUserNameResponse,
    GetUserEmailResponse,
    UsersListResponse,
    UsersListResponse1,
    User,
)

# Session Management
from app.schemas.session_schemas import (
    CreateSessionRequest,
    SessionIDRequest,
    SessionUsernameRequest,
    ValidateSessionRequest,
    DeleteSessionRequest,
    DeleteSessionByUsernameRequest,
    GetUsernameFromSessionRequest,
    CurrentUserRequest,
    CurrentUserHeaderRequest,
    TokenSessionIdRequest,
    TokenRequest,
    LogoutSessionIdRequest,
    SessionResponse,
    GetSessionIdResponse,
    ValidateSessionResponse,
    DeleteSessionResponse,
    DeleteUserSessionsResponse,
    DeleteAllSessionsResponse,
    GetAllSessionsResponse,
    GetUserSessionsResponse,
    GetSessionIdFromUsernameResponse,
    SessionsListResponse,
    CleanExpiredSessionsResponse,
    CurrentUserResponse,
)

# Workspace Management
from app.schemas.workspace_schemas import (
    WorkspaceBase,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
    WorkspaceWithMemberInfo,
    WorkspaceResponse,
    WorkspaceMemberBase,
    WorkspaceMemberCreate,
    WorkspaceMemberUpdate,
    WorkspaceMemberInDB,
    WorkspaceMemberResponse,
)

# OTP Management
from app.schemas.otp_schemas import (
    OTPRequest,
    OTPVerification,
    OTPDeletion,
    OTPSendEmail,
    OTPResponse,
)

# Log Management
from app.schemas.log_schemas import (
    LogEntry,
    LogListResponse,
    LogFilterRequest,
)

# Chat Management
from app.schemas.chat_schemas import (
    BaseChatRequest,
    ChatRequest,
    ChatResponse,
)

# Camera Management
from app.schemas.camera_schemas import (
    StreamQueryParams,
    CameraStreamQueryParams,
    StreamCreate,
    StreamUpdate,
    StreamUpdateList,
    StreamDelete,
    CameraState,
    CamerasStateResponse,
    StreamCreateWithLocation,
    StreamUpdateWithLocation,
    CameraCSVRecord,
    CameraCSVRecordWithLocation,
    UserCameraCountUpdate,
    UserCameraCountResponse,
)

# Alert Management
from app.schemas.alert_schemas import (
    ThresholdSettings,
    CameraAlertSettings,
    CameraCSVRecordWithLocationAndAlerts,
    AlertSummaryResponse,
    CameraAlertUpdateResponse,
)

# Location Management
from app.schemas.location_schemas import (
    LocationBase,
    LocationCreate,
    LocationUpdate,
    LocationResponse,
    BulkLocationAssignment,
    BulkLocationAssignmentResult,
    BulkLocationAssignmentWithAlerts,
    LocationHierarchy,
    LocationHierarchyResponse,
    LocationStatsResponse,
    LocationStatsResponseWithAlerts,
    CameraLocationGroup,
    CameraGroupResponse,
    CameraLocationGroupWithAlerts,
    CameraGroupResponseWithAlerts,
    LocationItem,
    LocationListResponse,
    AreaItem,
    AreaListResponse,
    BuildingItem,
    BuildingListResponse,
    FloorLevelItem,
    FloorLevelListResponse,
    ZoneItem,
    ZoneListResponse,
)

# Stream Management
from app.schemas.stream_schemas import (
    CameraData,
    SearchQuery,
    StreamInputItem,
    StreamInput,
    TimestampRangeResponse,
    CameraIdsResponse,
    DeleteDataRequest,
    LocationSearchQuery,
)

# Stream1 Management
from app.schemas.stream_schemas1 import (
    StreamStartRequest,
    StreamStopRequest,
    StreamTimingInfo,
    StreamPerformanceMetrics,
    StreamClientInfo,
    StreamErrorInfo,
    StreamStatusResponse,
    StreamListItem,
    WorkspaceSummary,
    StreamListResponse,
    WorkspaceInfo,
    WorkspaceLimits,
    WorkspaceStreamStats,
    WorkspacePerformance,
    WorkspaceStreamAnalytics,
    SystemMetrics,
    SystemPerformance,
    MemoryState,
    HealthStatus,
    SystemDiagnostics,
    ActiveStreamInfo,
    ActiveStreamsResponse,
    WorkspaceLimitsResponse,
    WSConnectedMessage,
    WSErrorMessage,
    WorkspaceLimitsResponse,
    WSStreamEndedMessage,
    WSNotificationMessage,
    WSPingMessage,
    WSPongMessage,
    WSSubscriptionConfirmed,
    BulkStopResult,
    BulkStopResponse,
    RecoveryResponse,
    HealthCheckResponse,
    PerformanceMetrics,
    SystemMetricsResponse,
    StreamOperationResponse,
    StreamStartResponse,
    StreamStopResponse,
    StreamRestartResponse,
    StreamLocationInfo,
    DetailedStreamInfo,
    DetailedStreamListResponse,
    StreamStateInfo,
    StreamStatesResponse,
    DetailedErrorInfo,
    StreamErrorsResponse,
    StreamConfiguration,
    StreamConfigurationResponse,
    SharedStreamSubscriber,
    SharedStreamInfo,
    SharedStreamsResponse,
    StreamDataExportConfig,
    BatchStreamOperation,
    BatchOperationResult,
    BatchOperationResponse,
)

# Qdrant Management
from app.schemas.qdrant_schemas import (
    VectorParams,
    CreateCollectionRequest,
    CreateCollectionRequest1,
    VectorConfig,
    AdvancedCreateCollectionRequest,
    DeleteCollectionRequest,
    QdrantLocationItem,
    QdrantLocationListResponse,
    QdrantAreaItem,
    QdrantAreaListResponse,
    QdrantBuildingItem,
    QdrantBuildingListResponse,
    QdrantFloorLevelItem,
    QdrantFloorLevelListResponse,
    QdrantZoneItem,
    QdrantZoneListResponse,
    TimeRange,
    QdrantLocationAnalyticsItem,
    QdrantLocationAnalyticsResponse,
)

# System Management
from app.schemas.system_schemas import (
    ContactCreate,
    ContactResponse,
    MessageRequest,
    MessageResponse,
    SystemInfo,
    ProjectMetadata,
    SQLQueryRequest,
    SQLQueryResponse,
)

# Utility Functions
from app.schemas.utils import (
    validate_camera_alert_thresholds,
    sanitize_camera_data_with_alerts,
    validate_location_hierarchy,
    sanitize_string,
)

__all__ = [
    # Config
    'Settings',
    
    # Auth
    'PKCERequest', 'TokenPair', 'TokenData', 'RefreshTokenRequest',
    'CreateTokenRequest', 'VerifyTokenRequest', 'RevokeTokenRequest',
    'InvalidateTokenRequest', 'BlacklistTokenRequest', 'TokenVerifyResponse',
    'TokenIdResponse', 'TokenMessageResponse', 'BlacklistCheckResponse',
    'MaintenanceResponse', 'TokenStats', 'TokenInfo', 'TokenListResponse',
    
    # User
    'CreateUserRequest', 'LoginRequest', 'UpdatePasswordRequest',
    'EmailRequest', 'UserIdRequest', 'UserRequest', 'VerifyPasswordRequest',
    'ResetPasswordRequest', 'UserResponse', 'GetUserIdResponse',
    'GetUserNameResponse', 'GetUserEmailResponse', 'UsersListResponse',
    'UsersListResponse1', 'User',
    
    # Session
    'CreateSessionRequest', 'SessionIDRequest', 'SessionUsernameRequest',
    'ValidateSessionRequest', 'DeleteSessionRequest',
    'DeleteSessionByUsernameRequest', 'GetUsernameFromSessionRequest',
    'CurrentUserRequest', 'CurrentUserHeaderRequest', 'TokenSessionIdRequest',
    'TokenRequest', 'LogoutSessionIdRequest', 'SessionResponse',
    'GetSessionIdResponse', 'ValidateSessionResponse', 'DeleteSessionResponse',
    'DeleteUserSessionsResponse', 'DeleteAllSessionsResponse',
    'GetAllSessionsResponse', 'GetUserSessionsResponse',
    'GetSessionIdFromUsernameResponse', 'SessionsListResponse',
    'CleanExpiredSessionsResponse', 'CurrentUserResponse',
    
    # Workspace
    'WorkspaceBase', 'WorkspaceCreate', 'WorkspaceUpdate', 'WorkspaceInDB',
    'WorkspaceWithMemberInfo', 'WorkspaceResponse', 'WorkspaceMemberBase',
    'WorkspaceMemberCreate', 'WorkspaceMemberUpdate', 'WorkspaceMemberInDB',
    'WorkspaceMemberResponse',
    
    # OTP
    'OTPRequest', 'OTPVerification', 'OTPDeletion', 'OTPSendEmail',
    'OTPResponse',
    
    # Log
    'LogEntry', 'LogListResponse', 'LogFilterRequest',
    
    # Chat
    'BaseChatRequest', 'ChatRequest', 'ChatResponse',
    
    # Camera
    'StreamQueryParams', 'CameraStreamQueryParams', 'StreamCreate',
    'StreamUpdate', 'StreamUpdateList', 'StreamDelete', 'CameraState',
    'CamerasStateResponse', 'StreamCreateWithLocation',
    'StreamUpdateWithLocation', 'CameraCSVRecord',
    'CameraCSVRecordWithLocation', 'UserCameraCountUpdate', 
    'UserCameraCountResponse',
    
    # Alert
    'ThresholdSettings', 'CameraAlertSettings',
    'CameraCSVRecordWithLocationAndAlerts', 'AlertSummaryResponse',
    'CameraAlertUpdateResponse',
    
    # Location
    'LocationBase', 'LocationCreate', 'LocationUpdate', 'LocationResponse',
    'BulkLocationAssignment', 'BulkLocationAssignmentResult',
    'BulkLocationAssignmentWithAlerts', 'LocationHierarchy',
    'LocationHierarchyResponse', 'LocationStatsResponse',
    'LocationStatsResponseWithAlerts', 'CameraLocationGroup',
    'CameraGroupResponse', 'CameraLocationGroupWithAlerts',
    'CameraGroupResponseWithAlerts', 'LocationItem', 'LocationListResponse',
    'AreaItem', 'AreaListResponse', 'BuildingItem', 'BuildingListResponse',
    'FloorLevelItem', 'FloorLevelListResponse', 'ZoneItem',
    'ZoneListResponse',
    
    # Stream
    'CameraData', 'SearchQuery', 'StreamInputItem', 'StreamInput',
    'TimestampRangeResponse', 'CameraIdsResponse', 'DeleteDataRequest',
    'LocationSearchQuery',
    
    # Qdrant
    'VectorParams', 'CreateCollectionRequest', 'CreateCollectionRequest1',
    'VectorConfig', 'AdvancedCreateCollectionRequest',
    'DeleteCollectionRequest', 'QdrantLocationItem',
    'QdrantLocationListResponse', 'QdrantAreaItem', 'QdrantAreaListResponse',
    'QdrantBuildingItem', 'QdrantBuildingListResponse',
    'QdrantFloorLevelItem', 'QdrantFloorLevelListResponse', 'QdrantZoneItem',
    'QdrantZoneListResponse', 'TimeRange', 'QdrantLocationAnalyticsItem',
    'QdrantLocationAnalyticsResponse',
    
    # System
    'ContactCreate', 'ContactResponse', 'MessageRequest', 'MessageResponse',
    'SystemInfo', 'ProjectMetadata', 'SQLQueryRequest', 'SQLQueryResponse',
    
    # Utils
    'validate_camera_alert_thresholds', 'sanitize_camera_data_with_alerts',
    'validate_location_hierarchy', 'sanitize_string',
]