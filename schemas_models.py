# schemas_models.py
from pydantic_settings import BaseSettings
from pydantic import BaseModel, RootModel, EmailStr, Field, validator, constr
from typing import Generator, Dict, List, Optional, Tuple, Any, Set, Union
from uuid import UUID
from datetime import datetime
from fastapi import Header, Cookie
from decimal import Decimal

class Settings(BaseSettings):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: str
    smtp_password: str
    otp_sender_email: str
    
    class Config:
        env_file = ".env"

class PKCERequest(BaseModel):
    code_challenge: str
    code_challenge_method: str = "S256"  # Only support SHA-256
    
###### Log ######
class LogEntry(BaseModel):
    """Schema for a log entry."""
    log_id: Optional[str] = None
    workspace_id: Optional[str] = None
    created_at: Optional[str] = None
    content: Optional[str] = None
    username: Optional[str] = None
    action_type: Optional[str] = None
    ip_address: Optional[str] = None
    status: Optional[str] = None
    
class LogListResponse(BaseModel):
    """Schema for a list of log entries."""
    logs: List[LogEntry]
    
class LogFilterRequest(BaseModel):
    """Schema for advanced log filtering."""
    username: Optional[str] = None
    action_type: Optional[str] = None
    ip_address: Optional[str] = None
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    sort_by: str = "created_at"
    sort_direction: str = "desc"
    workspace_id: Optional[UUID] = None

###### Chat ######
class BaseChatRequest(BaseModel):
    history: Optional[List[Dict[str, str]]] =  Field(default=[])
    system_prompt: Optional[str] = Field(default="")
    max_tokens: Optional[int] = Field(default=512, ge=1, le=2048)
    temperature: Optional[float] = Field(default=0.7, ge=0.0, le=2.0)
    format_: Optional[str] = Field(default='')
    stream: Optional[bool] = Field(default=False)

class ChatRequest(BaseChatRequest):
    """Basic chat request"""
    id: Optional[str] = Field(default="0")
    context: Optional[str] = Field(default="")
    prompt: str = Field(..., min_length=1)

###### TOKEN ######
class TokenPair(BaseModel):
    access_token: str = Field(..., description="Short-lived token for API access")
    refresh_token: str = Field(..., description="Long-lived token to get new access tokens")
    token_type: str = Field(default="bearer", description="Type of token")
    # expires_at: datetime = Field(..., description="Access token expiration timestamp")
    expires_at: str = Field(..., description="Access token expiration timestamp in ISO format")

# Update TokenData model to include workspace_id
class TokenData(BaseModel):
    user_id: str
    workspace_id: Optional[str] = None
    exp: int
    token_type: str
    needs_refresh: Optional[bool] = False

# Workspace Models
class WorkspaceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None

class WorkspaceCreate(WorkspaceBase):
    pass

class WorkspaceUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None

class WorkspaceInDB(WorkspaceBase):
    workspace_id: UUID
    created_at: datetime
    updated_at: datetime
    is_active: bool

class WorkspaceWithMemberInfo(WorkspaceInDB):
    member_role: str
    member_count: Optional[int] = None

# Workspace Member Models
class WorkspaceMemberBase(BaseModel):
    workspace_id: UUID
    user_id: UUID
    role: str = Field(..., pattern="^(member|admin|viewer)$")

class WorkspaceMemberCreate(BaseModel):
    user_id: UUID
    role: str = Field("member", pattern="^(member|admin|viewer)$")

class WorkspaceMemberUpdate(BaseModel):
    role: str = Field(..., pattern="^(member|admin|viewer)$")

class WorkspaceMemberInDB(WorkspaceMemberBase):
    membership_id: UUID
    username: str
    created_at: datetime
    updated_at: datetime

class WorkspaceResponse(BaseModel):
    workspace_id: UUID
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_active: bool
    
class WorkspaceMemberResponse(BaseModel):
    membership_id: UUID
    workspace_id: UUID
    user_id: UUID
    username: str
    role: str
    created_at: datetime
    updated_at: datetime
    
class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., description="Refresh token to get new access token")

class CreateTokenRequest(BaseModel):
    username: str = Field(..., description="Username for token creation")
    password: str = Field(..., description="Password to verify")
    
    class Config:
        json_schema_extra = {
            "example": {
                "username": "johndoe",
                "password": "johndoe1212"
            }
        }

class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., description="Valid refresh token")
    
    class Config:
        json_schema_extra = {
            "example": {
                "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            }
        }

class VerifyTokenRequest(BaseModel):
    token: str = Field(..., description="Token to verify")
    token_type: Optional[str] = Field(None, description="Optional token type (access or refresh)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "access"
            }
        }

class RevokeTokenRequest(BaseModel):
    token: str = Field(..., description="Token to revoke")
    
    class Config:
        json_schema_extra = {
            "example": {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            }
        }

class InvalidateTokenRequest(BaseModel):
    access_token: str = Field(..., description="Access token to invalidate")
    
    class Config:
        json_schema_extra = {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            }
        }

class BlacklistTokenRequest(BaseModel):
    token: str = Field(..., description="Token to blacklist")
    username: str = Field(..., description="Username associated with the token")
    # expires_at: datetime = Field(..., description="Token expiration timestamp")
    expires_at: str = Field(..., description="Access token expiration timestamp in ISO format")
    
    class Config:
        json_schema_extra = {
            "example": {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "username": "johndoe",
                "expires_at": "2023-12-31T23:59:59Z"
            }
        }

class TokenVerifyResponse(BaseModel):
    username: str
    expires_at: str
    token_type: str
    valid: bool

class TokenIdResponse(BaseModel):
    token_id: str

class TokenMessageResponse(BaseModel):
    detail: str

class BlacklistCheckResponse(BaseModel):
    is_blacklisted: bool

class MaintenanceResponse(BaseModel):
    removed_tokens: int

class TokenStats(BaseModel):
    total_active_tokens: int
    tokens_by_user: Dict[str, int]

class TokenInfo(BaseModel):
    token_id: str
    access_token: str
    refresh_token: str
    access_expires_at: str
    refresh_expires_at: str
    created_at: str
    username: Optional[str] = None

class TokenListResponse(BaseModel):
    tokens: List[TokenInfo]

###### USER ######
# Request Models
class CreateUserRequest(BaseModel):
    username: str = Field(..., description="Username for the new user")
    password: str = Field(..., description="Password for the new user")
    email: EmailStr = Field(..., description="Email address for the new user")
    role: str = Field("user", pattern="^(user|admin)$")#|moderator|workspace_admin

class LoginRequest(BaseModel):
    username: Optional[str] = Field(None, description="Username of the user")
    email: Optional[EmailStr] = Field(None, description="Email address for the new user")
    password: str = Field(..., description="Password to verify")

class TokenSessionIdRequest(BaseModel):
    token: Optional[str] = Header(None, alias="Authorization header (Bearer token)")
    session_id: Optional[str] = Cookie(None)

class TokenRequest(BaseModel):
    token: Optional[str] = Header(None, alias="Authorization header (Bearer token)")

class TokenRequest(BaseModel):
    token: Optional[str] = Header(None, alias="Authorization header (Bearer token)")

class LogoutSessionIdRequest(BaseModel):
    session_id: Optional[str] = Cookie(None)

class UpdatePasswordRequest(BaseModel):
    username: Optional[str] = Field("", description="Username of the user")
    # email: Optional[EmailStr] = None
    current_password: str = Field(..., description="Current password of the user")
    new_password: str = Field(..., description="New password to set")
    confirm_password: str = Field(..., description="Confirmation of the new password")

class EmailRequest(BaseModel):
    email: EmailStr = Field(..., description="Email of the user")

class UserIdRequest(BaseModel):
    user_id: str = Field(..., description="UserId of the user")

class UserRequest(BaseModel):
    username: str = Field(..., description="Username of the user")

class VerifyPasswordRequest(BaseModel):
    username: str = Field(..., description="Username of the user")
    password: str = Field(..., description="Password to verify")

class ResetPasswordRequest(BaseModel):
    email: EmailStr = Field(..., description="Username of the user")
    # username: str = Field(..., description="Username of the user")
    new_password: str = Field(..., description="New password for the user")

# Response Models
class UserResponse(BaseModel):
    user_id: str
    username: str
    email: EmailStr

class GetUserIdResponse(BaseModel):
    user_id: str

class GetUserNameResponse(BaseModel):
    username: Optional[str] = Field(None, description="Username associated with the session, or None if invalid")

class GetUserEmailResponse(BaseModel):
    email: EmailStr

class UsersListResponse(BaseModel):
    user_list: List[UserResponse]
    
class UsersListResponse1(RootModel[List[UserResponse]]):
    pass

###### OTP ######
class OTPRequest(BaseModel):
    email: EmailStr
    length: Optional[int] = Field(default=6, ge=4, le=10)
    purpose: str = 'login'
    expiration: Optional[int] = Field(default=None, ge=60, le=3600)

    @validator('length')
    def validate_length(cls, v):
        if v < 4 or v > 10:
            raise ValueError('OTP length must be between 4 and 10 digits')
        return v

    @validator('purpose')
    def validate_purpose(cls, v):
        allowed = {'login', 'password_reset', 'email_verification'}
        if v not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v

class OTPVerification(BaseModel):
    email: EmailStr
    otp: str
    expiration: Optional[int] = Field(default=600, ge=60, le=3600)
    purpose: str = 'login'

    @validator('otp')
    def validate_otp(cls, v):
        if not v.isdigit():
            raise ValueError('OTP must contain only digits')
        return v

    @validator('purpose')
    def validate_purpose(cls, v):
        allowed = {'login', 'password_reset', 'email_verification'}
        if v not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v
    
class OTPDeletion(BaseModel):
    email: EmailStr
    purpose: str = 'login'

    @validator('purpose')
    def validate_purpose(cls, v):
        allowed = {'login', 'password_reset', 'email_verification'}
        if v not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v

class OTPSendEmail(BaseModel):
    email: EmailStr
    otp: str
    purpose: str = 'login'
    expiration: Optional[int] = Field(default=None, ge=60, le=3600)

    @validator('otp')
    def validate_otp(cls, v):
        if not v.isdigit():
            raise ValueError('OTP must contain only digits')
        return v

    @validator('purpose')
    def validate_purpose(cls, v):
        allowed = {'login', 'password_reset', 'email_verification'}
        if v not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v

###### SESSION REQUEST MODELS ######

class CreateSessionRequest(BaseModel):
    username: str = Field(..., description="The Username for whom to creating a session")

class SessionIDRequest(BaseModel):
    session_id: Optional[str] = Field("", description="The session ID to validate or delete")

class SessionUsernameRequest(BaseModel):
    username: str = Field(..., description="The username for session operations")

class ValidateSessionRequest(BaseModel):
    session_id: Optional[str] = Field("", description="Session ID to validate")

class DeleteSessionRequest(BaseModel):
    session_id: Optional[str] = Field("", description="Session ID to delete")

class DeleteSessionByUsernameRequest(BaseModel):
    username: str = Field(..., description="Username whose session needs to be deleted")

class GetUsernameFromSessionRequest(BaseModel):
    session_id: Optional[str] = Field("", description="Session ID to retrieve username")

class CurrentUserRequest(BaseModel):
    session_id: Optional[str] = Field("", description="Session ID to validate")

class CurrentUserHeaderRequest(BaseModel):
    token: Optional[str] = Field(None, description="token header (Bearer token)")

###### SESSION RESPONSE MODELS ######

class SessionResponse(BaseModel):
    session_id: str = Field(..., description="The ID of the session")
    username: str = Field(..., description="The username associated with the session")
    created_at: str#datetime, time

class GetSessionIdResponse(BaseModel):
    session_id: str

class ValidateSessionResponse(BaseModel):
    is_valid: bool = Field(..., description="Indicates if the session is valid")

class DeleteSessionResponse(BaseModel):
    success: bool = Field(..., description="Indicates if the session was successfully deleted")
    username: Optional[str] = Field(None, description="The username associated with the deleted session, if available")

class DeleteUserSessionsResponse(BaseModel):
    deleted_count: int = Field(..., description="The number of sessions deleted")

class DeleteAllSessionsResponse(BaseModel):
    deleted_count: int = Field(..., description="The number of sessions deleted")

class GetAllSessionsResponse(BaseModel):
    sessions: List[SessionResponse]

class GetUserSessionsResponse(BaseModel):
    sessions: List[SessionResponse]

class GetSessionIdFromUsernameResponse(BaseModel):
      session_id : Optional[str] = Field(None, description="session_id from username")

class SessionsListResponse(RootModel[List[SessionResponse]]):
    pass

class CleanExpiredSessionsResponse(BaseModel):
     deleted_count: int = Field(..., description="Number of expired sessions cleaned")

class CurrentUserResponse(BaseModel):
    username : str = Field(..., description="Username of the current user from the session.")

###### CAMERA ######
class StreamQueryParams(BaseModel):
    frame_delay: Optional[float] = 0
    frame_skip: Optional[int] = 300
    conf: Optional[float] = 0.4

class CameraStreamQueryParams(BaseModel):
    frame_delay: Optional[float] = 0
    frame_skip: Optional[int] = 300
    conf: Optional[float] = 0.4

class StreamCreate(BaseModel):
    # Required fields
    name: str = Field(..., max_length=50)
    path: str = Field(..., max_length=255)
    type: str = Field(default='local', pattern='^(rtsp|http|local|other|video file)$')
    status: str = Field(default='inactive', pattern='^(active|inactive|error|processing)$')
    is_streaming: bool = Field(default=False)
    
    # Optional location and metadata fields
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[Decimal] = Field(None, ge=-90, le=90, decimal_places=8)
    longitude: Optional[Decimal] = Field(None, ge=-180, le=180, decimal_places=8)
    
    # Optional alert fields
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: bool = Field(default=False)
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Main Entrance Camera",
                "path": "/dev/video0",
                "type": "local",
                "status": "active",
                "is_streaming": True,
                "location": "Building A Entrance",
                "area": "Reception",
                "building": "Building A",
                "floor_level": "Ground Floor",
                "zone": "Security Zone 1",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "count_threshold_greater": 10,
                "count_threshold_less": 2,
                "alert_enabled": True
            }
        }

class StreamUpdate(BaseModel):
    # Required fields
    id: str = Field(...)
    name: str = Field(..., max_length=50)
    path: str = Field(..., max_length=255)
    type: str = Field(default='local', pattern='^(rtsp|http|local|other|video file)$')
    status: str = Field(default='inactive', pattern='^(active|inactive|error|processing)$')
    is_streaming: bool = Field(default=False)
    
    # Optional location and metadata fields
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[Decimal] = Field(None, ge=-90, le=90, decimal_places=8)
    longitude: Optional[Decimal] = Field(None, ge=-180, le=180, decimal_places=8)
    
    # Optional alert fields
    count_threshold_greater: Optional[int] = Field(None, ge=0)
    count_threshold_less: Optional[int] = Field(None, ge=0)
    alert_enabled: bool = Field(default=False)
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "091312-312312-12312-231232-1321",
                "name": "Main Entrance Camera",
                "path": "/dev/video0",
                "type": "local",
                "status": "active",
                "is_streaming": True,
                "location": "Building A Entrance",
                "area": "Reception",
                "building": "Building A",
                "floor_level": "Ground Floor",
                "zone": "Security Zone 1",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "count_threshold_greater": 10,
                "count_threshold_less": 2,
                "alert_enabled": True
            }
        }

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

###### STREAM ######
class CameraData(BaseModel):
    camera_id: str
    source: str
    frame_data: Optional[str] = None
    
    @validator('source')
    def source_validator(cls, value):
        value = value.replace('\\', '/')
        if not value.startswith(('http://', 'https://','rtsp://')) and not value.lower() == "local":
           if not value.lower() == "local":
             if not value.startswith("/"):
               if not (len(value)>2 and value[1:3] == ":/"):
                raise ValueError("Source must start with 'http://' or 'https://' or 'rtsp://' or 'local' or local file path with / or with C:/")
        return value

class SearchQuery(BaseModel):
    camera_id: Optional[Union[str, List[str]]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class StreamInputItem(BaseModel):
    source: str # List of source URLs
    camera_id: Optional[str]

class StreamInput(BaseModel):
    inputs: List[StreamInputItem] # List of source URLs

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

class DeleteCollectionRequest(BaseModel):
    collection_name: str
    force: bool = False

class DeleteDataRequest(BaseModel):
    camera_id: Optional[Union[str, int]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

###### COLLECTION ######

class VectorParams(BaseModel):
    size: int
    distance: str = Field(..., description="Distance metric: 'Cosine', 'Euclid', or 'Dot'")
    on_disk: bool = False

class CreateCollectionRequest1(BaseModel):
    collection_name: str
    vector_size: int = 1
    distance: str = "Dot"
    on_disk: bool = False
    custom_params: Optional[Dict[str, Any]] = None

class CreateCollectionRequest(BaseModel):
    collection_name: str
    vector_size: int = 1
    distance: str = "DOT"

class VectorConfig(BaseModel):
    size: int
    distance: str
    on_disk: bool = False

class AdvancedCreateCollectionRequest(BaseModel):
    collection_name: str
    vectors_config: Dict[str, VectorConfig] = None
    single_vector_config: Optional[VectorConfig] = None
    shard_number: Optional[int] = None
    replication_factor: Optional[int] = None
    write_consistency_factor: Optional[int] = None
    on_disk_payload: Optional[bool] = None
    hnsw_config: Optional[Dict[str, Any]] = None
    optimizers_config: Optional[Dict[str, Any]] = None
    wal_config: Optional[Dict[str, Any]] = None
    quantization_config: Optional[Dict[str, Any]] = None


###### SYSTEM ######
# Models for contact information
class ContactCreate(BaseModel):
    name: str = Field(..., description="Contact name")
    email: EmailStr = Field(..., description="Contact email")
    phone: Optional[str] = Field(None, description="Contact phone number")
    message: str = Field(..., description="Contact message")

class ContactResponse(BaseModel):
    id: str
    name: str
    email: EmailStr
    phone: Optional[str]
    message: str
    created_at: datetime

class MessageRequest(BaseModel):
    message: str = Field(..., description="Message to be displayed")
    title: Optional[str] = Field(None, description="Optional title for the message")
    level: str = Field("info", description="Message level (info, warning, error)")
    
class MessageResponse(BaseModel):
    id: str
    message: str
    title: Optional[str]
    level: str
    timestamp: datetime

# Models for system information
class SystemInfo(BaseModel):
    os: str = Field(..., description="Operating system information")
    cpu_usage: float = Field(..., description="CPU usage percentage")
    memory_usage: float = Field(..., description="Memory usage percentage")
    disk_usage: float = Field(..., description="Disk usage percentage")
    uptime: float = Field(..., description="System uptime in seconds")
    hostname: str = Field(..., description="System hostname")
    ip_address: Optional[str] = None
    time_now: datetime = Field(default_factory=datetime.now)

# Models for project metadata
class ProjectMetadata(BaseModel):
    name: str = Field(..., description="Project name")
    version: str = Field(..., description="Project version")
    description: Optional[str] = Field(None, description="Project description")
    last_updated: datetime = Field(default_factory=datetime.now)
    features: Optional[Dict[str, bool]] = Field(default_factory=dict)

# SQLQuery
class SQLQueryRequest(BaseModel):
    query: str = Field(..., description="The SQL query to execute.")
    params: Optional[List[Any]] = Field(None, description="Optional list of parameters for parameterized queries.")

class SQLQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = Field(None, description="Query result data, if any (e.g., list of records for SELECT, row count for DML).")

class User(BaseModel):
    user_id: UUID
    username: str
    role: str 
    is_active: bool

class CameraBulkUploadResult(BaseModel):
    total_processed: int
    successful_uploads: int
    failed_uploads: int
    successful_cameras: List[Dict[str, Any]]
    failed_cameras: List[Dict[str, Any]]
    errors: List[str]

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


# Location Management Models
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

# Enhanced Camera Models with Location
class StreamCreateWithLocation(BaseModel):
    name: str
    path: str
    type: str = Field("local", pattern="^(rtsp|http|local|other|video file)$")
    status: str = Field("inactive", description="Status: active, inactive")
    is_streaming: bool = False
    # Location fields
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
    # Location fields
    location: Optional[str] = Field(None, max_length=100)
    area: Optional[str] = Field(None, max_length=100)
    building: Optional[str] = Field(None, max_length=100)
    floor_level: Optional[str] = Field(None, max_length=20)
    zone: Optional[str] = Field(None, max_length=50)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)

# Enhanced CSV Record with Location
class CameraCSVRecordWithLocation(BaseModel):
    name: str
    path: str
    type: str = "local"
    status: str = "inactive"
    is_streaming: bool = False
    # Location fields (optional in CSV)
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
    
    @validator('status')
    def validate_status(cls, v):
        allowed_statuses = ['active', 'inactive', 'error', 'processing']
        if v not in allowed_statuses:
            raise ValueError(f'Status must be one of: {allowed_statuses}')
        return v
    
    @validator('latitude')
    def validate_latitude(cls, v):
        if v is not None:
            try:
                lat = float(v)
                if not -90 <= lat <= 90:
                    raise ValueError('Latitude must be between -90 and 90')
                return lat
            except (ValueError, TypeError):
                raise ValueError('Invalid latitude format')
        return v
    
    @validator('longitude')
    def validate_longitude(cls, v):
        if v is not None:
            try:
                lng = float(v)
                if not -180 <= lng <= 180:
                    raise ValueError('Longitude must be between -180 and 180')
                return lng
            except (ValueError, TypeError):
                raise ValueError('Invalid longitude format')
        return v

# Location-based Search Models
class LocationSearchQuery(BaseModel):
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    # Existing search fields
    camera_id: Optional[Union[str, List[str]]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class LocationHierarchy(BaseModel):
    floor_level: Optional[str] = None
    building: Optional[str] = None
    zone: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int = 0

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

class CameraLocationGroup(BaseModel):
    group_type: str  # 'location', 'area', 'building', 'zone'
    group_name: str
    camera_count: int
    cameras: List[Dict[str, Any]]
    active_count: int = 0
    inactive_count: int = 0
    streaming_count: int = 0

class LocationHierarchyResponse(BaseModel):
    workspace_id: str
    hierarchy: List[LocationHierarchy]
    total_locations: int
    floor_level: List[str] = []
    buildings: List[str] = []
    zones: List[str] = []
    areas: List[str] = []
    locations: List[str] = []

class CameraGroupResponse(BaseModel):
    groups: List[CameraLocationGroup]
    total_groups: int
    total_cameras: int
    group_type: str

# Bulk location assignment
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

class ThresholdSettings(BaseModel):
    count_threshold_greater: Optional[int] = None
    count_threshold_less: Optional[int] = None
    alert_enabled: bool = False

################################################################################
################################################################################
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

    # @validator('count_threshold_less')
    # def validate_threshold_logic(cls, v, values):
    #     # Only validate if both thresholds are provided and not None
    #     if (v is not None and v != "" and 
    #         'count_threshold_greater' in values and 
    #         values['count_threshold_greater'] is not None and 
    #         values['count_threshold_greater'] != ""):
    #         if v >= values['count_threshold_greater']:
    #             raise ValueError('count_threshold_less must be less than count_threshold_greater')
    #     return v

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
    
    @validator('count_threshold_less')
    def validate_threshold_logic(cls, v, values):
        if v is not None and 'count_threshold_greater' in values and values['count_threshold_greater'] is not None:
            if v >= values['count_threshold_greater']:
                raise ValueError('count_threshold_less must be less than count_threshold_greater')
        return v

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


# === ADDITIONAL RESPONSE MODELS FOR THE NEW ENDPOINTS ===
# Add these to your schemas_models.py file:

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


# === RESPONSE MODELS FOR QDRANT ENDPOINTS ===
# Add these to your schemas_models.py file if not already present:

class QdrantLocationItem(BaseModel):
    location: str
    camera_count: int
    camera_ids: List[str]

class QdrantLocationListResponse(BaseModel):
    locations: List[QdrantLocationItem]
    total_count: int
    workspace_id: str

class QdrantAreaItem(BaseModel):
    area: str
    location: Optional[str] = None
    camera_count: int
    camera_ids: List[str]

class QdrantAreaListResponse(BaseModel):
    areas: List[QdrantAreaItem]
    total_count: int
    filtered_by_locations: Optional[Union[str, List[str]]] = None
    workspace_id: str

class QdrantBuildingItem(BaseModel):
    building: str
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int
    camera_ids: List[str]

class QdrantBuildingListResponse(BaseModel):
    buildings: List[QdrantBuildingItem]
    total_count: int
    filtered_by_areas: Optional[Union[str, List[str]]] = None
    workspace_id: str

class QdrantFloorLevelItem(BaseModel):
    floor_level: str
    building: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int
    camera_ids: List[str]

class QdrantFloorLevelListResponse(BaseModel):
    floor_levels: List[QdrantFloorLevelItem]
    total_count: int
    filtered_by_buildings: Optional[Union[str, List[str]]] = None
    workspace_id: str

class QdrantZoneItem(BaseModel):
    zone: str
    floor_level: Optional[str] = None
    building: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    camera_count: int
    camera_ids: List[str]

class QdrantZoneListResponse(BaseModel):
    zones: List[QdrantZoneItem]
    total_count: int
    filtered_by_floor_levels: Optional[Union[str, List[str]]] = None
    workspace_id: str

class TimeRange(BaseModel):
    earliest: Optional[str] = None
    latest: Optional[str] = None

class QdrantLocationAnalyticsItem(BaseModel):
    location: Optional[str] = None
    area: Optional[str] = None
    building: Optional[str] = None
    floor_level: Optional[str] = None
    zone: Optional[str] = None
    data_points: int
    total_person_count: int
    average_person_count: float
    unique_cameras: int
    camera_ids: List[str]
    time_range: TimeRange

class QdrantLocationAnalyticsResponse(BaseModel):
    analytics: List[QdrantLocationAnalyticsItem]
    total_groups: int
    group_by: str
    filters_applied: Dict[str, Any]
    workspace_id: str


# === VALIDATION FUNCTIONS ===
def validate_camera_alert_thresholds(greater_threshold: Optional[int], less_threshold: Optional[int]) -> bool:
    """Validate that alert thresholds are logically consistent."""
    if greater_threshold is not None and less_threshold is not None:
        if less_threshold >= greater_threshold:
            return False
    return True

def sanitize_camera_data_with_alerts(data: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize camera data including alert fields."""
    sanitized = {}
    
    # Basic fields
    sanitized['name'] = str(data.get('name', '')).strip()
    sanitized['path'] = str(data.get('path', '')).strip()
    sanitized['type'] = str(data.get('type', 'local')).strip().lower()
    sanitized['status'] = str(data.get('status', 'inactive')).strip().lower()
    
    # Boolean fields
    sanitized['is_streaming'] = str(data.get('is_streaming', 'false')).lower() in ['true', '1', 'yes', 'on']
    sanitized['alert_enabled'] = str(data.get('alert_enabled', 'false')).lower() in ['true', '1', 'yes', 'on']
    
    # Location fields
    location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
    for field in location_fields:
        value = data.get(field)
        sanitized[field] = str(value).strip() if value and str(value).strip() else None
    
    # Coordinate fields
    for coord_field in ['latitude', 'longitude']:
        value = data.get(coord_field)
        if value and str(value).strip():
            try:
                sanitized[coord_field] = float(value)
            except (ValueError, TypeError):
                sanitized[coord_field] = None
        else:
            sanitized[coord_field] = None
    
    # Alert threshold fields
    for threshold_field in ['count_threshold_greater', 'count_threshold_less']:
        value = data.get(threshold_field)
        if value and str(value).strip():
            try:
                sanitized[threshold_field] = int(value)
            except (ValueError, TypeError):
                sanitized[threshold_field] = None
        else:
            sanitized[threshold_field] = None
    
    return sanitized

class UserCameraCountUpdate(BaseModel):
    user_id: str = Field(..., description="User ID to update camera count for")
    count_of_camera: int = Field(..., ge=0, le=1000, description="New camera count limit (0-1000)")

class UserCameraCountResponse(BaseModel):
    user_id: str
    username: str
    count_of_camera: int
    previous_count: int
    message: str
    