# app/schemas/auth_schemas.py
from pydantic import BaseModel, Field
from typing import Optional, Dict

class PKCERequest(BaseModel):
    code_challenge: str
    code_challenge_method: str = "S256"

# Token Models
class TokenPair(BaseModel):
    access_token: str = Field(..., description="Short-lived token for API access")
    refresh_token: str = Field(..., description="Long-lived token to get new access tokens")
    token_type: str = Field(default="bearer", description="Type of token")
    expires_at: str = Field(..., description="Access token expiration timestamp in ISO format")

class TokenData(BaseModel):
    user_id: str
    workspace_id: Optional[str] = None
    exp: int
    token_type: str
    needs_refresh: Optional[bool] = False

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
    tokens: list[TokenInfo]