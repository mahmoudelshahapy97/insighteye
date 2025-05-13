# login_user.py
from fastapi import APIRouter, HTTPException, status, Depends, Query, Body, Form, Response, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional
import logging
from datetime import datetime
from user_manager import UserManager
from session_manager import SessionManager
from schemas_models import CreateUserRequest, UpdatePasswordRequest, LoginRequest
from utils import send_email, send_email_from_client
from schemas_models import PKCERequest, MessageRequest, MessageResponse, ContactCreate, ContactResponse, TokenPair, RefreshTokenRequest

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__) # Use logger

# Initialize the router
router = APIRouter(tags=["authentication"])
security = HTTPBearer()

# Initialize SessionManager
session_manager = SessionManager()
user_manager = UserManager()

@router.post("/signup", response_model=TokenPair, status_code=status.HTTP_201_CREATED)
async def signup_route(user: CreateUserRequest):
    """
    Register a new user and create authentication tokens.
    
    Args:
        user: The user data containing username, password, and email.
        response: FastAPI Response object to set headers.
    
    Returns:
        A token pair containing access and refresh tokens.
        Dictionary with signup success message.
    
    Raises:
        HTTPException: If the username/email already exists or if a database error occurs.
    """
    try:
        success = await user_manager.create_user(user.username, user.email, user.password)
        if not success:
            raise HTTPException(status_code=409, detail="Username or email already exists")

        user_data = await user_manager.get_user_by_username(user.username)
        user_id = user_data["user_id"]

        # Create token pair for the new user
        token_pair = session_manager.create_token_pair(user_id)
        
        # Store tokens in the database
        session_manager.store_token_pair(
            user_id,
            token_pair.access_token,
            token_pair.refresh_token
        )

        return token_pair

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Signup error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"This username is already taken. Please choose a different one. This email address is already registered. Please use a different email or try logging in. Password does not meet security requirements. It must be at least 8 characters with uppercase, lowercase, and numbers.: {e}")

@router.post("/login", response_model=None)#TokenPair)
async def login_route(login_data: LoginRequest):
    """
    Authenticate a user and create access and refresh tokens.
    
    Args:
        login_data: The login credentials.
        response: FastAPI Response object to set headers.
    
    Returns:
        A token pair containing access and refresh tokens.
        Dictionary with login success message.
    """
    try:


        # Verify the password
        # is_valid = await user_manager.verify_user_password(login_data.username, login_data.password)
        is_valid, username = await user_manager.verify_credentials(login_data.username, login_data.email, login_data.password)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials. Please check your username/email and password."
            )
        
        # # Check if user has 2FA enabled
        # user = await user_manager.get_user_by_username(login_data.username)
        # if user.get("two_factor_enabled", False):
        #     # If 2FA code not provided, return 2FA required response
        #     if not login_data.totp_code:
        #         return {
        #             "message": "Two-factor authentication required",
        #             "requires_2fa": True
        #         }
            
        #     # Verify 2FA code
        #     is_valid_totp = user_manager.verify_totp_code(user["totp_secret"], login_data.totp_code)
        #     if not is_valid_totp:
        #         raise HTTPException(
        #             status_code=status.HTTP_401_UNAUTHORIZED,
        #             detail="Invalid verification 2FA code. Please try again."
        #         )

        user_data = await user_manager.get_user_by_username(username)
        user_id = user_data["user_id"]

        # Create tokens for the user
        token_pair = session_manager.create_token_pair(user_id)
        
        # Store tokens in database
        session_manager.store_token_pair(
            user_id, 
            token_pair.access_token, 
            token_pair.refresh_token
        )

        session_manager.log_action(
            content=f"User {username} Login",
            user_id=user_id,
            action_type="Login",
        )

        # return token_pair
        return {
            "access_token": token_pair.access_token,
            "refresh_token": token_pair.refresh_token,
            "is_active": user_data.get("is_active", False),
            "username": username,
            "token_type": token_pair.token_type,
            "expires_at": token_pair.expires_at,
        }

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Invalid credentials. Please check your username/email and password.: {e}"
        )

@router.post("/refresh-token", response_model=TokenPair)
async def refresh_token_route(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Get a new access token using a refresh token.
    
    Args:
        refresh_request: The refresh token.
        response: FastAPI Response object to set headers.
    
    Returns:
        A new token pair.
        Dictionary with refresh success message.
    """
    try:
        refresh_token = credentials.credentials

        # Verify the refresh token is not blacklisted
        if session_manager.is_token_blacklisted(refresh_token):
            # Log suspicious activity
            session_manager.log_action(
                content=f"Attempt to use revoked refresh token",
                user_id=None,  # Will be filled if we can extract user_id
                action_type="SecurityAlert"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token has been revoked"
            )

        # Now actually verify the refresh token signature and expiration
        validated_token_data = session_manager.verify_token(refresh_token)
        if not validated_token_data:
            # If the token was in DB but invalid, it might be an attack
            # Revoke all tokens for this user as a security measure
            session_manager.invalidate_all_user_tokens(user_id)
            session_manager.log_action(
                content=f"Token tampering detected, all sessions revoked",
                user_id=user_id,
                action_type="SecurityAlert"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token validation failed - all sessions have been terminated"
            )
        
        user_id = validated_token_data.user_id

        # # Optional: Check if refresh token device info matches current request
        # # This adds another layer of security against token theft
        # if refresh_request.device_fingerprint and user_agent:
        #     is_same_device = session_manager.verify_device_fingerprint(
        #         user_id,
        #         refresh_token,
        #         refresh_request.device_fingerprint,
        #         user_agent
        #     )
        #     if not is_same_device:
        #         # Potential token theft - revoke this token
        #         session_manager.revoke_token(refresh_token)
        #         session_manager.log_action(
        #             content=f"Potential refresh token theft detected",
        #             user_id=user_id,
        #             action_type="SecurityAlert"
        #         )
        #         raise HTTPException(
        #             status_code=status.HTTP_401_UNAUTHORIZED,
        #             detail="Device verification failed - token revoked"
        #         )
                
        # # Check for suspicious activity (optional)
        # # For example: multiple refresh attempts in short time, unusual locations
        # if client_ip and session_manager.detect_suspicious_activity(user_id, client_ip):
        #     # Don't revoke tokens but require additional verification
        #     session_manager.log_action(
        #         content=f"Suspicious activity detected",
        #         user_id=user_id,
        #         action_type="SecurityAlert"
        #     )
        #     return {
        #         "message": "Additional verification required",
        #         "requires_verification": True
        #     }


        # Get username from refresh token
        token_pair = session_manager.refresh_access_token(refresh_token)
        if not token_pair:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Log successful token refresh
        session_manager.log_action(
            content=f"Token refreshed successfully",
            user_id=user_id,
            action_type="TokenRefresh"
        )

        return token_pair

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Token refresh error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"our session has expired. Please log in again.: {e}"
        )

@router.post("/logout")
async def logout_route(token: str = Depends(session_manager.get_token_from_header)):
    """
    Logout a user by invalidating their refresh token.
    
    Args:
        token: The access token from the header.
    
    Returns:
        A message indicating successful logout.
    
    Raises:
        HTTPException: If token validation fails or a database error occurs.
    """
    try:
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )

        # user_id = session_manager.get_user_id_by_token(token)
        # Verify the token to get the username
        token_data = session_manager.verify_token(token)
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
        
        # Blacklist the token
        success = session_manager.invalidate_token(token)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to revoke token"
            )

        # # Invalidate all active refresh tokens for this user
        session_manager.invalidate_all_user_tokens(token_data.user_id)

        session_manager.log_action(
            content=f"User Logout",
            user_id=token_data.user_id,
            action_type="Logout"
        )

        return {"message": "Logged out successfully", "token_revoked": True}
        
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Logout error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unable to complete logout. Your session may have already expired.: {e}"
        )

@router.post("/revoke-token")
async def revoke_token_route(token: str = Depends(session_manager.get_token_from_header)):
    """
    Explicitly revoke a token by adding it to the blacklist.
    
    Args:
        token: The token to revoke.
    
    Returns:
        A message indicating successful revocation.
    """
    try:
        success = session_manager.revoke_token(token)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid token or token already revoked"
            )
            
        return {"message": "Token successfully revoked"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Token revocation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get("/protected-route")
async def protected_route(username: str = Depends(session_manager.get_current_user)):
    """
    A protected route that requires authentication.
    
    Args:
        username: The authenticated username from the dependency.
    
    Returns:
        The user data for the authenticated user.
    """
    try:
        user_data = await user_manager.get_user_by_username(username)
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")
            
        return {"message": "Access granted", "user": user_data}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Protected route error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving user data"
        )

@router.put("/update-password")
async def update_password_route(password_data: UpdatePasswordRequest, username: str = Depends(session_manager.get_current_user)):
    """
    Update the password for the authenticated user.
    
    Args:
        password_data: The current and new password.
        # username: The authenticated user's username (from session).
    
    Returns:
        A message indicating successful password update.
    
    Raises:
        HTTPException: If the current password is incorrect or a database error occurs.
    """
    try:
        # Ensure the authenticated user is updating their own password
        # If username is provided in the request, ensure it matches the authenticated user
        if password_data.username and password_data.username != username:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only update your own password"
            )
        # # If email is provided, use it to get the username
        # if password_data.email:
        #     user_data = await user_manager.get_user_by_email(password_data.email)
        #     if not user_data:
        #         raise HTTPException(status_code=404, detail="Email not found")
            
        #     # Override the username if found by email
        #     username = user_data.username
           
        # Validate passwords match
        if password_data.new_password != password_data.confirm_password:
            raise HTTPException(status_code=400, detail="Passwords do not match")
        
        # Verify the current password
        is_valid = await user_manager.verify_user_password(username, password_data.current_password)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Current password is incorrect"
            )

        # Update the password
        success = await user_manager.reset_password(username, password_data.new_password)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Failed to update password"
            )
            
        return {"message": "Password updated successfully"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Password update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating the password: {e}"
        )

# @router.post("/contact", response_model=ContactResponse, status_code=status.HTTP_201_CREATED)
@router.post("/contact", status_code=status.HTTP_201_CREATED)
async def create_contact(
    contact: ContactCreate,
):
    """
    Create a new contact message.
    
    Args:
        contact: The contact information with name, email, and message.
        
    Returns:
        The created contact with ID and timestamp.
    """
    try:
        subject = f"New Contact Inquiry from {contact.name}"
        body = f"""
        Hello,

        You have received a new contact request.

        Name: {contact.name}
        Email: {contact.email}
        Phone: {contact.phone if contact.phone else 'Not provided'}
        Message:
        {contact.message}

        Please follow up as needed.

        Best regards,
        {contact.name}
        """

        success = await send_email_from_client(contact.email, subject, body)

        if success:

            subject = f"New Contact Inquiry from {contact.name}"
            body = f"""
            Dear {contact.name},

            Thank you for reaching out to us. We have received your inquiry and appreciate you taking the time to contact us. Our team will review your message and get back to you as soon as possible.

            If you need immediate assistance, please feel free to reach out to us at Insighteye@gateworx.net.

            Best regards,
            InsightEye
            GateWorx
            """

            success = await send_email(contact.email, subject, body)
            if success:
                return {"message": "Thanks for contact us"}

        return {"message": "An error occurred while creating contact"}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while creating contact: {str(e)}"
        )

@router.post("/message", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
async def create_message(
    message_request: MessageRequest
):
    """
    Create and show a message.
    
    Args:
        message_request: The message data with content, optional title, and level.

    Returns:
        The created message with ID and timestamp.
    """
    try:
        # Log the message based on its level
        if message_request.level == "error":
            logging.error(f"{message_request.message}")
        elif message_request.level == "warning":
            logging.warning(f"{message_request.message}")
        else:
            logging.info(f"{message_request.message}")
        
        # Generate a unique ID for this message
        message_id = f"msg_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # In a real implementation, you might store this message in a database
        # or broadcast it to connected clients
        
        return MessageResponse(
            id=message_id,
            message=message_request.message,
            title=message_request.title,
            level=message_request.level,
            timestamp=datetime.now()
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while processing message: {str(e)}"
        )

@router.get("/message", response_model=MessageResponse, status_code=status.HTTP_200_OK)
async def get_message():
    """
    Return a default message when the endpoint is accessed.

    Returns:
        A generated message with ID and timestamp.
    """
    try:
        # Default message content
        message = "This is a default system message."
        title = "Default Title"
        level = "info"

        # Log the message
        logging.info(message)

        # Generate a unique ID for this message
        message_id = f"msg_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        return MessageResponse(
            id=message_id,
            message=message,
            title=title,
            level=level,
            timestamp=datetime.now()
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while processing message: {str(e)}"
        )
