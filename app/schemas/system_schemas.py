# app/schemas/system_schemas.py
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

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

class SystemInfo(BaseModel):
    os: str = Field(..., description="Operating system information")
    cpu_usage: float = Field(..., description="CPU usage percentage")
    memory_usage: float = Field(..., description="Memory usage percentage")
    disk_usage: float = Field(..., description="Disk usage percentage")
    uptime: float = Field(..., description="System uptime in seconds")
    hostname: str = Field(..., description="System hostname")
    ip_address: Optional[str] = None
    time_now: datetime = Field(default_factory=datetime.now)

class ProjectMetadata(BaseModel):
    name: str = Field(..., description="Project name")
    version: str = Field(..., description="Project version")
    description: Optional[str] = Field(None, description="Project description")
    last_updated: datetime = Field(default_factory=datetime.now)
    features: Optional[Dict[str, bool]] = Field(default_factory=dict)

class SQLQueryRequest(BaseModel):
    query: str = Field(..., description="The SQL query to execute.")
    params: Optional[List[Any]] = Field(None, description="Optional list of parameters for parameterized queries.")

class SQLQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = Field(None, description="Query result data, if any (e.g., list of records for SELECT, row count for DML).")
