# app/schemas/otp_schemas.py
from pydantic import BaseModel, EmailStr, Field, validator, root_validator
from typing import Optional, List, Dict, Any
from datetime import datetime

# ============================================================================
# OTP GENERATION SCHEMAS
# ============================================================================

class OTPRequest(BaseModel):
    """
    Request schema for OTP generation.
    
    Supports multiple purposes and configurable parameters.
    """
    email: EmailStr = Field(..., description="Email address to send OTP")
    length: Optional[int] = Field(
        default=6, 
        ge=4, 
        le=10, 
        description="Length of OTP (4-10 digits)"
    )
    purpose: str = Field(
        default='login',
        description="Purpose of OTP: login, password_reset, email_verification, 2fa"
    )
    expiration: Optional[int] = Field(
        default=None, 
        ge=60, 
        le=3600,
        description="OTP expiration time in seconds (60-3600)"
    )
    numeric_only: bool = Field(
        default=True,
        description="Generate numeric-only OTP (True) or alphanumeric (False)"
    )
    
    @validator('length')
    def validate_length(cls, v):
        """Validate OTP length is within acceptable range."""
        if v < 4 or v > 10:
            raise ValueError('OTP length must be between 4 and 10 digits')
        return v
    
    @validator('purpose')
    def validate_purpose(cls, v):
        """Validate OTP purpose is recognized."""
        allowed = {
            'login', 
            'password_reset', 
            'email_verification', 
            '2fa', 
            'two_factor',
            'account_verification',
            'transaction_verification'
        }
        if v.lower() not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v.lower()
    
    @validator('expiration')
    def validate_expiration(cls, v):
        """Validate expiration time is reasonable."""
        if v is not None:
            if v < 60:
                raise ValueError('Expiration time must be at least 60 seconds')
            if v > 3600:
                raise ValueError('Expiration time cannot exceed 3600 seconds (1 hour)')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "length": 6,
                "purpose": "login",
                "expiration": 300,
                "numeric_only": True
            }
        }


class OTPBatchRequest(BaseModel):
    """Request schema for generating multiple OTPs."""
    emails: List[EmailStr] = Field(..., min_items=1, max_items=100)
    length: Optional[int] = Field(default=6, ge=4, le=10)
    purpose: str = Field(default='login')
    expiration: Optional[int] = Field(default=None, ge=60, le=3600)
    
    @validator('emails')
    def validate_emails(cls, v):
        """Ensure no duplicate emails."""
        if len(v) != len(set(v)):
            raise ValueError('Duplicate emails are not allowed')
        return v


# ============================================================================
# OTP VERIFICATION SCHEMAS
# ============================================================================

class OTPVerification(BaseModel):
    """
    Request schema for OTP verification.
    
    Validates the OTP code against stored value.
    """
    email: EmailStr = Field(..., description="Email address associated with OTP")
    otp: str = Field(..., min_length=4, max_length=10, description="OTP code to verify")
    expiration: Optional[int] = Field(default=600, ge=60, le=3600)
    purpose: str = Field(
        default='login',
        description="Purpose must match the OTP generation purpose"
    )
    auto_delete: bool = Field(
        default=True,
        description="Automatically delete OTP after successful verification"
    )
    
    @validator('otp')
    def validate_otp(cls, v):
        """Validate OTP format."""
        v = v.strip()
        
        # Check if OTP is not empty
        if not v:
            raise ValueError('OTP cannot be empty')
        
        # Check length
        if len(v) < 4 or len(v) > 10:
            raise ValueError('OTP must be between 4 and 10 characters')
        
        # For numeric OTPs, ensure only digits
        if v.isdigit():
            return v
        
        # For alphanumeric, ensure valid characters
        if not v.isalnum():
            raise ValueError('OTP must contain only letters and numbers')
        
        return v.upper()  # Normalize to uppercase
    
    @validator('purpose')
    def validate_purpose(cls, v):
        """Validate purpose matches allowed values."""
        allowed = {
            'login', 
            'password_reset', 
            'email_verification', 
            '2fa',
            'two_factor',
            'account_verification',
            'transaction_verification'
        }
        if v.lower() not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v.lower()
    
    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "otp": "123456",
                "purpose": "login",
                "auto_delete": True
            }
        }


class OTPBatchVerification(BaseModel):
    """Request schema for verifying multiple OTPs."""
    verifications: List[Dict[str, str]] = Field(..., min_items=1, max_items=50)
    
    @validator('verifications')
    def validate_verifications(cls, v):
        """Ensure each verification has required fields."""
        for item in v:
            if 'email' not in item or 'otp' not in item:
                raise ValueError('Each verification must have email and otp fields')
        return v


# ============================================================================
# OTP DELETION SCHEMAS
# ============================================================================

class OTPDeletion(BaseModel):
    """
    Request schema for OTP deletion.
    
    Used to manually invalidate OTPs.
    """
    email: EmailStr = Field(..., description="Email address associated with OTP")
    purpose: str = Field(
        default='login',
        description="Purpose of OTP to delete"
    )
    delete_all: bool = Field(
        default=False,
        description="Delete all OTPs for this email regardless of purpose"
    )
    
    @validator('purpose')
    def validate_purpose(cls, v):
        """Validate purpose is recognized."""
        allowed = {
            'login', 
            'password_reset', 
            'email_verification', 
            '2fa',
            'two_factor',
            'account_verification',
            'transaction_verification'
        }
        if v.lower() not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v.lower()


class OTPBulkDeletion(BaseModel):
    """Request schema for deleting multiple OTPs."""
    emails: Optional[List[EmailStr]] = Field(None, description="Specific emails to delete OTPs for")
    purpose: Optional[str] = Field(None, description="Delete OTPs with specific purpose")
    expired_only: bool = Field(default=False, description="Only delete expired OTPs")
    older_than_minutes: Optional[int] = Field(None, ge=1, description="Delete OTPs older than X minutes")


# ============================================================================
# EMAIL SENDING SCHEMAS
# ============================================================================

class OTPSendEmail(BaseModel):
    """
    Request schema for sending OTP via email.
    
    Contains OTP and email delivery configuration.
    """
    email: EmailStr = Field(..., description="Recipient email address")
    otp: str = Field(..., min_length=4, max_length=10, description="OTP code to send")
    purpose: str = Field(
        default='login',
        description="Purpose for email template selection"
    )
    expiration: Optional[int] = Field(
        default=None, 
        ge=60, 
        le=3600,
        description="OTP validity duration in seconds"
    )
    language: str = Field(
        default='en',
        description="Email language (en, ar, es, fr, etc.)"
    )
    custom_message: Optional[str] = Field(
        None,
        max_length=500,
        description="Custom message to include in email"
    )
    
    @validator('otp')
    def validate_otp(cls, v):
        """Validate OTP format for email sending."""
        v = v.strip()
        if not v:
            raise ValueError('OTP cannot be empty')
        if not v.isdigit() and not v.isalnum():
            raise ValueError('OTP must contain only letters and numbers')
        return v
    
    @validator('purpose')
    def validate_purpose(cls, v):
        """Validate purpose for template selection."""
        allowed = {
            'login', 
            'password_reset', 
            'email_verification', 
            '2fa',
            'two_factor',
            'account_verification',
            'transaction_verification'
        }
        if v.lower() not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v.lower()
    
    @validator('language')
    def validate_language(cls, v):
        """Validate language code."""
        allowed_languages = {'en', 'ar', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'zh', 'ja'}
        if v.lower() not in allowed_languages:
            raise ValueError(f"Language must be one of {allowed_languages}")
        return v.lower()


class OTPEmailTemplate(BaseModel):
    """Email template configuration for OTP."""
    purpose: str
    subject: str = Field(..., min_length=1, max_length=200)
    body_template: str = Field(..., description="HTML or text template with {otp} placeholder")
    language: str = Field(default='en')


# ============================================================================
# RESPONSE SCHEMAS
# ============================================================================

class OTPResponse(BaseModel):
    """
    Standard response after OTP generation.
    
    Contains confirmation and metadata.
    """
    message: str = Field(..., description="Success/error message")
    email: EmailStr = Field(..., description="Email address OTP was sent to")
    expires_in: Optional[int] = Field(None, description="Seconds until OTP expires")
    expires_at: Optional[str] = Field(None, description="ISO timestamp of expiration")
    purpose: str = Field(..., description="Purpose of the OTP")
    otp_length: int = Field(..., ge=4, le=10, description="Length of generated OTP")
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "OTP sent successfully",
                "email": "user@example.com",
                "expires_in": 300,
                "expires_at": "2024-01-01T12:05:00Z",
                "purpose": "login",
                "otp_length": 6
            }
        }


class OTPVerificationResponse(BaseModel):
    """Response after OTP verification attempt."""
    valid: bool = Field(..., description="Whether OTP is valid")
    message: str = Field(..., description="Verification result message")
    email: EmailStr = Field(..., description="Email address verified")
    purpose: str = Field(..., description="Purpose of the OTP")
    attempts_remaining: Optional[int] = Field(None, description="Remaining verification attempts")
    locked_until: Optional[str] = Field(None, description="ISO timestamp if account is temporarily locked")
    
    class Config:
        json_schema_extra = {
            "example": {
                "valid": True,
                "message": "OTP verified successfully",
                "email": "user@example.com",
                "purpose": "login",
                "attempts_remaining": None,
                "locked_until": None
            }
        }


class OTPStatusResponse(BaseModel):
    """Response for OTP status check."""
    exists: bool = Field(..., description="Whether OTP exists")
    email: EmailStr
    purpose: str
    is_expired: bool = Field(..., description="Whether OTP has expired")
    expires_in: Optional[int] = Field(None, description="Seconds until expiration")
    created_at: Optional[str] = Field(None, description="ISO timestamp of creation")
    attempts_used: int = Field(default=0, ge=0, description="Number of verification attempts")
    max_attempts: int = Field(default=5, ge=1, description="Maximum allowed attempts")


class OTPDeletionResponse(BaseModel):
    """Response after OTP deletion."""
    success: bool
    message: str
    email: EmailStr
    purpose: Optional[str] = None
    deleted_count: int = Field(default=1, ge=0, description="Number of OTPs deleted")


class OTPBatchResponse(BaseModel):
    """Response for batch OTP operations."""
    total_processed: int = Field(..., ge=0)
    successful: int = Field(..., ge=0)
    failed: int = Field(..., ge=0)
    results: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class OTPStatistics(BaseModel):
    """Statistics about OTP usage."""
    total_active_otps: int = Field(..., ge=0)
    expired_otps: int = Field(..., ge=0)
    otps_by_purpose: Dict[str, int] = Field(default_factory=dict)
    total_sent_today: int = Field(default=0, ge=0)
    total_verified_today: int = Field(default=0, ge=0)
    average_verification_time: Optional[float] = Field(None, description="Average time to verify in seconds")


# ============================================================================
# RATE LIMITING SCHEMAS
# ============================================================================

class OTPRateLimitCheck(BaseModel):
    """Check rate limit status for OTP operations."""
    email: EmailStr
    action: str = Field(..., description="Action to check: generate, verify, resend")
    
    @validator('action')
    def validate_action(cls, v):
        allowed = {'generate', 'verify', 'resend', 'send'}
        if v.lower() not in allowed:
            raise ValueError(f"Action must be one of {allowed}")
        return v.lower()


class OTPRateLimitResponse(BaseModel):
    """Response for rate limit check."""
    allowed: bool = Field(..., description="Whether action is allowed")
    email: EmailStr
    action: str
    requests_made: int = Field(..., ge=0)
    max_requests: int = Field(..., ge=1)
    reset_at: Optional[str] = Field(None, description="ISO timestamp when limit resets")
    retry_after: Optional[int] = Field(None, description="Seconds to wait before retry")


# ============================================================================
# CONFIGURATION SCHEMAS
# ============================================================================

class OTPConfig(BaseModel):
    """OTP system configuration."""
    default_length: int = Field(default=6, ge=4, le=10)
    default_expiration: int = Field(default=300, ge=60, le=3600)
    max_attempts: int = Field(default=5, ge=1, le=10)
    lockout_duration: int = Field(default=900, ge=60, description="Seconds to lock after max attempts")
    rate_limit_per_hour: int = Field(default=10, ge=1, le=100)
    numeric_only: bool = Field(default=True)
    case_sensitive: bool = Field(default=False)
    resend_cooldown: int = Field(default=60, ge=30, description="Seconds before allowing resend")
    cleanup_expired_after: int = Field(default=86400, ge=3600, description="Delete expired OTPs after X seconds")


class OTPResendRequest(BaseModel):
    """Request to resend OTP."""
    email: EmailStr = Field(..., description="Email address to resend OTP to")
    purpose: str = Field(default='login')
    force: bool = Field(default=False, description="Force resend even if cooldown not expired")
    
    @validator('purpose')
    def validate_purpose(cls, v):
        allowed = {
            'login', 
            'password_reset', 
            'email_verification', 
            '2fa',
            'two_factor',
            'account_verification',
            'transaction_verification'
        }
        if v.lower() not in allowed:
            raise ValueError(f"Purpose must be one of {allowed}")
        return v.lower()


class OTPResendResponse(BaseModel):
    """Response after OTP resend."""
    success: bool
    message: str
    email: EmailStr
    purpose: str
    new_otp_sent: bool = Field(..., description="Whether a new OTP was generated")
    expires_in: Optional[int] = None
    cooldown_remaining: Optional[int] = Field(None, description="Seconds remaining in cooldown")
    