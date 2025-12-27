# app/schemas/log_schemas.py
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import UUID

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