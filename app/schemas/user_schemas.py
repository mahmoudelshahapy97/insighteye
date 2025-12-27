# app/schemas/user_schemas.py
from pydantic import BaseModel, EmailStr, Field, RootModel
from typing import Optional, List
from uuid import UUID

class CreateUserRequest(BaseModel):
    username: str = Field(..., description="Username for the new user")
    password: str = Field(..., description="Password for the new user")
    email: EmailStr = Field(..., description="Email address for the new user")
    role: str = Field("user", pattern="^(user|admin)$")
    count_of_camera: int = Field(5)

class LoginRequest(BaseModel):
    username: Optional[str] = Field(None, description="Username of the user")
    email: Optional[EmailStr] = Field(None, description="Email address for the new user")
    password: str = Field(..., description="Password to verify")

class UpdatePasswordRequest(BaseModel):
    username: Optional[str] = Field("", description="Username of the user")
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
    new_password: str = Field(..., description="New password for the user")

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

class User(BaseModel):
    user_id: UUID
    username: str
    role: str 
    is_active: bool