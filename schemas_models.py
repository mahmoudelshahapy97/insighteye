# schemas_models.py
from pydantic_settings import BaseSettings
from pydantic import BaseModel, RootModel, EmailStr, Field, validator, constr
from typing import Generator, Dict, List, Optional, Tuple, Any, Set, Union
from uuid import UUID
from datetime import datetime
from fastapi import Header, Cookie

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
    stream: Optional[bool] = Field(default=True)#True

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
    frame_skip: Optional[int] = 5
    conf: Optional[float] = 0.4

class CameraStreamQueryParams(BaseModel):
    frame_delay: Optional[float] = 0
    frame_skip: Optional[int] = 5
    conf: Optional[float] = 0.4

class StreamCreate(BaseModel):
    name: str
    path: str
    type: str = Field("local", pattern="^(rtsp|http|local|other|video file)$")
    status: str = Field("inactive", description="Status: active, inactive")
    is_streaming: bool = False

class StreamUpdate(BaseModel):
    id: str
    name: Optional[str]
    path: Optional[str]
    type: Optional[str]
    status: Optional[str] = Field("inactive", description="Status: active, inactive")
    is_streaming: Optional[bool] = False
    

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

