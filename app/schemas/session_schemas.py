# app/schemas/session_schemas.py
from pydantic import BaseModel, Field, RootModel
from fastapi import Cookie, Header
from typing import Optional, List

# Request Models
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

class TokenSessionIdRequest(BaseModel):
    token: Optional[str] = Header(None, alias="Authorization header (Bearer token)")
    session_id: Optional[str] = Cookie(None)

class TokenRequest(BaseModel):
    token: Optional[str] = Header(None, alias="Authorization header (Bearer token)")

class LogoutSessionIdRequest(BaseModel):
    session_id: Optional[str] = Cookie(None)

# Response Models
class SessionResponse(BaseModel):
    session_id: str = Field(..., description="The ID of the session")
    username: str = Field(..., description="The username associated with the session")
    created_at: str

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
    session_id: Optional[str] = Field(None, description="session_id from username")

class SessionsListResponse(RootModel[List[SessionResponse]]):
    pass

class CleanExpiredSessionsResponse(BaseModel):
    deleted_count: int = Field(..., description="Number of expired sessions cleaned")

class CurrentUserResponse(BaseModel):
    username: str = Field(..., description="Username of the current user from the session.")
