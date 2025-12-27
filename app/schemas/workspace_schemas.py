# app/schemas/workspace_schemas.py
from pydantic import BaseModel, Field, validator, EmailStr
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID

# ============================================================================
# WORKSPACE BASE SCHEMAS
# ============================================================================

class WorkspaceBase(BaseModel):
    """Base workspace model with common fields."""
    name: str = Field(
        ..., 
        min_length=1, 
        max_length=100,
        description="Workspace name"
    )
    description: Optional[str] = Field(
        None,
        max_length=500,
        description="Workspace description"
    )
    
    @validator('name')
    def validate_name(cls, v):
        """Validate workspace name format."""
        v = v.strip()
        if not v:
            raise ValueError('Workspace name cannot be empty')
        
        # Check for invalid characters
        invalid_chars = ['<', '>', '/', '\\', '|', ':', '*', '?', '"']
        if any(char in v for char in invalid_chars):
            raise ValueError(f'Workspace name cannot contain: {", ".join(invalid_chars)}')
        
        return v
    
    @validator('description')
    def validate_description(cls, v):
        """Validate and clean description."""
        if v:
            v = v.strip()
            return v if v else None
        return v


class WorkspaceCreate(WorkspaceBase):
    """Schema for creating a new workspace."""
    settings: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Workspace-specific settings"
    )
    tags: Optional[List[str]] = Field(
        default_factory=list,
        description="Tags for workspace categorization"
    )
    max_members: Optional[int] = Field(
        None,
        ge=1,
        le=1000,
        description="Maximum number of members allowed"
    )
    
    @validator('tags')
    def validate_tags(cls, v):
        """Validate tags format."""
        if v:
            # Remove duplicates and empty strings
            v = [tag.strip() for tag in v if tag.strip()]
            v = list(set(v))  # Remove duplicates
            
            # Limit number of tags
            if len(v) > 20:
                raise ValueError('Maximum 20 tags allowed')
            
            # Validate each tag
            for tag in v:
                if len(tag) > 50:
                    raise ValueError('Tag length cannot exceed 50 characters')
        
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Engineering Team",
                "description": "Main engineering workspace for project development",
                "settings": {
                    "timezone": "UTC",
                    "notification_enabled": True
                },
                "tags": ["engineering", "development"],
                "max_members": 50
            }
        }


class WorkspaceUpdate(BaseModel):
    """Schema for updating an existing workspace."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    is_active: Optional[bool] = Field(None, description="Workspace active status")
    settings: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    max_members: Optional[int] = Field(None, ge=1, le=1000)
    
    @validator('name')
    def validate_name(cls, v):
        """Validate workspace name if provided."""
        if v is not None:
            v = v.strip()
            if not v:
                raise ValueError('Workspace name cannot be empty')
            
            invalid_chars = ['<', '>', '/', '\\', '|', ':', '*', '?', '"']
            if any(char in v for char in invalid_chars):
                raise ValueError(f'Workspace name cannot contain: {", ".join(invalid_chars)}')
        
        return v
    
    @validator('tags')
    def validate_tags(cls, v):
        """Validate tags if provided."""
        if v is not None:
            v = [tag.strip() for tag in v if tag.strip()]
            v = list(set(v))
            
            if len(v) > 20:
                raise ValueError('Maximum 20 tags allowed')
            
            for tag in v:
                if len(tag) > 50:
                    raise ValueError('Tag length cannot exceed 50 characters')
        
        return v


class WorkspaceInDB(WorkspaceBase):
    """Workspace model as stored in database."""
    workspace_id: UUID = Field(..., description="Unique workspace identifier")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    is_active: bool = Field(default=True, description="Workspace active status")
    owner_id: Optional[UUID] = Field(None, description="Workspace owner user ID")
    settings: Optional[Dict[str, Any]] = Field(default_factory=dict)
    tags: Optional[List[str]] = Field(default_factory=list)
    member_count: Optional[int] = Field(None, ge=0, description="Number of members")
    
    class Config:
        from_attributes = True


class WorkspaceWithMemberInfo(WorkspaceInDB):
    """Workspace model with current user's membership information."""
    member_role: str = Field(
        ..., 
        description="Current user's role in this workspace"
    )
    member_count: Optional[int] = Field(None, ge=0)
    is_owner: Optional[bool] = Field(default=False, description="Whether current user is owner")
    joined_at: Optional[datetime] = Field(None, description="When current user joined")
    permissions: Optional[List[str]] = Field(
        default_factory=list,
        description="User's permissions in this workspace"
    )


class WorkspaceResponse(BaseModel):
    """Standard response model for workspace operations."""
    workspace_id: UUID
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_active: bool
    owner_id: Optional[UUID] = None
    settings: Optional[Dict[str, Any]] = Field(default_factory=dict)
    tags: Optional[List[str]] = Field(default_factory=list)
    member_count: Optional[int] = None
    
    class Config:
        from_attributes = True


class WorkspaceListResponse(BaseModel):
    """Response model for listing workspaces."""
    workspaces: List[WorkspaceWithMemberInfo] = Field(default_factory=list)
    total_count: int = Field(..., ge=0)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    total_pages: int = Field(..., ge=0)


class WorkspaceDetailResponse(WorkspaceResponse):
    """Detailed workspace response with additional metadata."""
    recent_activity: Optional[List[Dict[str, Any]]] = Field(
        default_factory=list,
        description="Recent workspace activities"
    )
    statistics: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Workspace statistics"
    )


# ============================================================================
# WORKSPACE MEMBER SCHEMAS
# ============================================================================

class WorkspaceMemberBase(BaseModel):
    """Base workspace member model."""
    workspace_id: UUID = Field(..., description="Workspace identifier")
    user_id: UUID = Field(..., description="User identifier")
    role: str = Field(
        ..., 
        description="Member role: admin, member, viewer"
    )
    
    @validator('role')
    def validate_role(cls, v):
        """Validate role is one of allowed values."""
        allowed_roles = ['admin', 'member', 'viewer', 'owner']
        if v.lower() not in allowed_roles:
            raise ValueError(f'Role must be one of: {allowed_roles}')
        return v.lower()


class WorkspaceMemberCreate(BaseModel):
    """Schema for adding a member to workspace."""
    user_id: UUID = Field(..., description="User ID to add")
    role: str = Field(
        default="member",
        description="Member role"
    )
    permissions: Optional[List[str]] = Field(
        default_factory=list,
        description="Custom permissions for this member"
    )
    send_notification: Optional[bool] = Field(
        default=True,
        description="Send notification to user"
    )
    
    @validator('role')
    def validate_role(cls, v):
        """Validate role."""
        allowed_roles = ['admin', 'member', 'viewer']
        if v.lower() not in allowed_roles:
            raise ValueError(f'Role must be one of: {allowed_roles}')
        return v.lower()
    
    @validator('permissions')
    def validate_permissions(cls, v):
        """Validate permissions list."""
        if v:
            allowed_permissions = {
                'read', 'write', 'delete', 'manage_members',
                'manage_cameras', 'manage_locations', 'manage_alerts',
                'view_analytics', 'export_data', 'manage_settings'
            }
            
            for perm in v:
                if perm.lower() not in allowed_permissions:
                    raise ValueError(f'Invalid permission: {perm}. Allowed: {allowed_permissions}')
            
            # Remove duplicates
            v = list(set([p.lower() for p in v]))
        
        return v


class WorkspaceMemberUpdate(BaseModel):
    """Schema for updating workspace member."""
    role: Optional[str] = Field(None, description="Updated role")
    permissions: Optional[List[str]] = Field(None, description="Updated permissions")
    is_active: Optional[bool] = Field(None, description="Member active status")
    
    @validator('role')
    def validate_role(cls, v):
        """Validate role if provided."""
        if v is not None:
            allowed_roles = ['admin', 'member', 'viewer']
            if v.lower() not in allowed_roles:
                raise ValueError(f'Role must be one of: {allowed_roles}')
            return v.lower()
        return v
    
    @validator('permissions')
    def validate_permissions(cls, v):
        """Validate permissions if provided."""
        if v is not None:
            allowed_permissions = {
                'read', 'write', 'delete', 'manage_members',
                'manage_cameras', 'manage_locations', 'manage_alerts',
                'view_analytics', 'export_data', 'manage_settings'
            }
            
            for perm in v:
                if perm.lower() not in allowed_permissions:
                    raise ValueError(f'Invalid permission: {perm}')
            
            v = list(set([p.lower() for p in v]))
        
        return v


class WorkspaceMemberInDB(WorkspaceMemberBase):
    """Workspace member as stored in database."""
    membership_id: UUID = Field(..., description="Unique membership identifier")
    username: str = Field(..., description="Member username")
    email: Optional[EmailStr] = Field(None, description="Member email")
    created_at: datetime = Field(..., description="When member joined")
    updated_at: datetime = Field(..., description="Last update timestamp")
    is_active: Optional[bool] = Field(default=True, description="Membership active status")
    permissions: Optional[List[str]] = Field(default_factory=list)
    last_activity: Optional[datetime] = Field(None, description="Last activity timestamp")
    
    class Config:
        from_attributes = True


class WorkspaceMemberResponse(BaseModel):
    """Response model for workspace member operations."""
    membership_id: UUID
    workspace_id: UUID
    user_id: UUID
    username: str
    email: Optional[EmailStr] = None
    role: str
    permissions: Optional[List[str]] = Field(default_factory=list)
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_activity: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class WorkspaceMemberListResponse(BaseModel):
    """Response model for listing workspace members."""
    members: List[WorkspaceMemberResponse] = Field(default_factory=list)
    total_count: int = Field(..., ge=0)
    workspace_id: UUID
    workspace_name: str


class WorkspaceMemberBulkAdd(BaseModel):
    """Schema for adding multiple members at once."""
    user_ids: List[UUID] = Field(..., min_items=1, max_items=100)
    role: str = Field(default="member")
    permissions: Optional[List[str]] = Field(default_factory=list)
    send_notifications: bool = Field(default=True)
    
    @validator('user_ids')
    def validate_user_ids(cls, v):
        """Ensure no duplicate user IDs."""
        if len(v) != len(set(v)):
            raise ValueError('Duplicate user IDs are not allowed')
        return v
    
    @validator('role')
    def validate_role(cls, v):
        allowed_roles = ['admin', 'member', 'viewer']
        if v.lower() not in allowed_roles:
            raise ValueError(f'Role must be one of: {allowed_roles}')
        return v.lower()


class WorkspaceMemberBulkRemove(BaseModel):
    """Schema for removing multiple members at once."""
    user_ids: List[UUID] = Field(..., min_items=1, max_items=100)
    send_notifications: bool = Field(default=True)


# ============================================================================
# WORKSPACE INVITATION SCHEMAS
# ============================================================================

class WorkspaceInvitationCreate(BaseModel):
    """Schema for creating workspace invitation."""
    workspace_id: UUID
    email: EmailStr = Field(..., description="Email to invite")
    role: str = Field(default="member")
    message: Optional[str] = Field(None, max_length=500, description="Custom invitation message")
    expires_in_days: int = Field(default=7, ge=1, le=30, description="Invitation validity in days")
    
    @validator('role')
    def validate_role(cls, v):
        allowed_roles = ['admin', 'member', 'viewer']
        if v.lower() not in allowed_roles:
            raise ValueError(f'Role must be one of: {allowed_roles}')
        return v.lower()


class WorkspaceInvitationResponse(BaseModel):
    """Response model for workspace invitation."""
    invitation_id: UUID
    workspace_id: UUID
    workspace_name: str
    email: EmailStr
    role: str
    status: str = Field(..., description="pending, accepted, rejected, expired")
    invited_by: UUID
    invited_by_username: str
    created_at: datetime
    expires_at: datetime
    accepted_at: Optional[datetime] = None


class WorkspaceInvitationAccept(BaseModel):
    """Schema for accepting workspace invitation."""
    invitation_id: UUID
    user_id: UUID


class WorkspaceInvitationReject(BaseModel):
    """Schema for rejecting workspace invitation."""
    invitation_id: UUID
    reason: Optional[str] = Field(None, max_length=500)


# ============================================================================
# WORKSPACE SETTINGS SCHEMAS
# ============================================================================

class WorkspaceSettings(BaseModel):
    """Workspace configuration settings."""
    timezone: str = Field(default="UTC")
    date_format: str = Field(default="YYYY-MM-DD")
    time_format: str = Field(default="24h")
    language: str = Field(default="en")
    notification_enabled: bool = Field(default=True)
    email_notifications: bool = Field(default=True)
    alert_notifications: bool = Field(default=True)
    default_camera_retention_days: int = Field(default=30, ge=1, le=365)
    max_upload_size_mb: int = Field(default=100, ge=1, le=1000)
    analytics_enabled: bool = Field(default=True)
    
    @validator('timezone')
    def validate_timezone(cls, v):
        """Validate timezone format."""
        # Basic validation - in production, use pytz or zoneinfo
        if not v:
            return "UTC"
        return v
    
    @validator('language')
    def validate_language(cls, v):
        allowed = {'en', 'ar', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'zh', 'ja'}
        if v.lower() not in allowed:
            raise ValueError(f'Language must be one of: {allowed}')
        return v.lower()


class WorkspaceStatistics(BaseModel):
    """Workspace usage statistics."""
    total_cameras: int = Field(default=0, ge=0)
    active_cameras: int = Field(default=0, ge=0)
    total_locations: int = Field(default=0, ge=0)
    total_members: int = Field(default=0, ge=0)
    active_members: int = Field(default=0, ge=0)
    storage_used_mb: float = Field(default=0.0, ge=0.0)
    data_points_count: int = Field(default=0, ge=0)
    alerts_count: int = Field(default=0, ge=0)
    last_activity: Optional[datetime] = None


# ============================================================================
# WORKSPACE TRANSFER/OWNERSHIP SCHEMAS
# ============================================================================

class WorkspaceTransferOwnership(BaseModel):
    """Schema for transferring workspace ownership."""
    workspace_id: UUID
    new_owner_id: UUID
    confirm: bool = Field(..., description="Confirmation required")
    
    @validator('confirm')
    def validate_confirm(cls, v):
        if not v:
            raise ValueError('Ownership transfer must be confirmed')
        return v


class WorkspaceArchive(BaseModel):
    """Schema for archiving/unarchiving workspace."""
    workspace_id: UUID
    archive: bool = Field(..., description="True to archive, False to unarchive")
    reason: Optional[str] = Field(None, max_length=500)


class WorkspaceDelete(BaseModel):
    """Schema for deleting workspace."""
    workspace_id: UUID
    confirm_name: str = Field(..., description="Must match workspace name")
    permanent: bool = Field(default=False, description="Permanent deletion (cannot be recovered)")
    
    @validator('confirm_name')
    def validate_confirm_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Workspace name confirmation is required')
        return v.strip()


# ============================================================================
# WORKSPACE SEARCH/FILTER SCHEMAS
# ============================================================================

class WorkspaceSearchRequest(BaseModel):
    """Schema for searching workspaces."""
    query: Optional[str] = Field(None, min_length=1, max_length=100)
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None
    role: Optional[str] = Field(None, description="Filter by user's role")
    sort_by: str = Field(default="created_at", description="Field to sort by")
    sort_direction: str = Field(default="desc", description="asc or desc")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    
    @validator('sort_by')
    def validate_sort_by(cls, v):
        allowed = ['name', 'created_at', 'updated_at', 'member_count']
        if v not in allowed:
            raise ValueError(f'sort_by must be one of: {allowed}')
        return v
    
    @validator('sort_direction')
    def validate_sort_direction(cls, v):
        if v.lower() not in ['asc', 'desc']:
            raise ValueError('sort_direction must be asc or desc')
        return v.lower()