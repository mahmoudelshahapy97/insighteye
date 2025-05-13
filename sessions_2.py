from fastapi import APIRouter, Depends, HTTPException, Query, Path, status, Header, Response
from typing import List, Optional
from datetime import datetime
import logging
from schemas_models import LogListResponse, LogEntry, LogFilterRequest, TokenPair, TokenData, CreateTokenRequest, RefreshTokenRequest, VerifyTokenRequest, RevokeTokenRequest, InvalidateTokenRequest, BlacklistTokenRequest, TokenVerifyResponse, TokenIdResponse, TokenMessageResponse, BlacklistCheckResponse, MaintenanceResponse, TokenStats, TokenInfo, TokenListResponse 
from session_manager import SessionManager
from user_manager import UserManager

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Initialize the router
router = APIRouter(prefix="/auth", tags=["auth"])

# Initialize SessionManager
session_manager = SessionManager()
user_manager = UserManager()

# Endpoint for getting logs for the current user
@router.get("/logs/me", response_model=LogListResponse)
async def get_my_logs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    username: str = Depends(session_manager.get_current_user)
):
    """Get logs for the current authenticated user."""
    filters = {"username": username}
    
    # Add optional filters
    if action_type:
        filters["action_type"] = action_type
    if status:
        filters["status"] = status
    
    # Handle date filtering
    date_filter = {}
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            date_filter["start_date"] = start_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_filter["end_date"] = end_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    logs = session_manager.get_logs(
        filters=filters,
        date_filter=date_filter,
        limit=limit,
        offset=offset
    )
    
    user_data = await user_manager.get_user_by_username(username)
    user_id = user_data["user_id"]

    # Log this action
    session_manager.log_action(
        content=f"Retrieved {len(logs)} logs",
        user_id=user_id,
        action_type="logs_retrieval"
    )
    
    return LogListResponse(logs=logs)

# Endpoint for getting logs for a specific user (admin only)
@router.get("/logs/user/{username}", response_model=LogListResponse)
async def get_user_logs(
    username: str = Path(...),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user: str = Depends(session_manager.get_current_user)
):
    """Get logs for a specific user (admin only)."""
    # This should be protected with admin role check in a real application
    # For now, only allow users to access their own logs (or admins)
    if username != current_user:
        # In a real application, check if current_user is an admin
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this user's logs",
        )
    
    filters = {"username": username}
    
    # Add optional filters
    if action_type:
        filters["action_type"] = action_type
    if status:
        filters["status"] = status
    
    # Handle date filtering
    date_filter = {}
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            date_filter["start_date"] = start_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_filter["end_date"] = end_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    logs = session_manager.get_logs(
        filters=filters,
        date_filter=date_filter,
        limit=limit,
        offset=offset
    )

    user_data = await user_manager.get_user_by_username(username)
    user_id = user_data["user_id"]
    
    # Log this action
    session_manager.log_action(
        content=f"Retrieved {len(logs)} logs for user {username}",
        user_id=user_id,
        action_type="admin_logs_retrieval"
    )
    
    return LogListResponse(logs=logs)

# Endpoint for getting all logs (admin only)
@router.get("/logs/all", response_model=LogListResponse)
async def get_all_logs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    username: Optional[str] = Query(None),
    action_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user: str = Depends(session_manager.get_current_user)
):
    """Get all logs in the system (admin only)."""
    # This endpoint should be protected by an admin role in a real application
    
    filters = {}
    
    # Add optional filters
    if username:
        filters["username"] = username
    if action_type:
        filters["action_type"] = action_type
    if status:
        filters["status"] = status
    
    # Handle date filtering
    date_filter = {}
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            date_filter["start_date"] = start_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_filter["end_date"] = end_datetime
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
            )
    
    logs = session_manager.get_logs(
        filters=filters,
        date_filter=date_filter,
        limit=limit,
        offset=offset
    )

    user_data = await user_manager.get_user_by_username(current_user)
    user_id = user_data["user_id"]
    
    # Log this action
    session_manager.log_action(
        content=f"Retrieved {len(logs)} system logs",
        user_id=user_id,
        action_type="admin_all_logs_retrieval"
    )
    
    return LogListResponse(logs=logs)

# Endpoint for advanced log filtering
@router.post("/logs/filter", response_model=LogListResponse)
async def filter_logs(
    request: LogFilterRequest,
    current_user: str = Depends(session_manager.get_current_user)
):
    """Advanced log filtering (admin only)."""
    # This endpoint should be protected by an admin role in a real application
    
    # Build filters from request
    filters = {}
    if request.username:
        filters["username"] = request.username
    if request.action_type:
        filters["action_type"] = request.action_type
    if request.status:
        filters["status"] = request.status
    if request.ip_address:
        filters["ip_address"] = request.ip_address
    
    # Handle date filtering
    date_filter = {}
    if request.start_date:
        date_filter["start_date"] = request.start_date
    if request.end_date:
        date_filter["end_date"] = request.end_date
    
    logs = session_manager.get_logs(
        filters=filters,
        date_filter=date_filter,
        limit=request.limit,
        offset=request.offset,
        sort_by=request.sort_by,
        sort_direction=request.sort_direction
    )
    
    user_data = await user_manager.get_user_by_username(current_user)
    user_id = user_data["user_id"]

    # Log this action
    session_manager.log_action(
        content=f"Performed advanced log filtering, found {len(logs)} logs",
        user_id=user_id,
        action_type="admin_logs_filtering"
    )
    
    return LogListResponse(logs=logs)

@router.get("/logs/user", response_model=LogListResponse)
async def get_user_logs(
    user_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user: str = Depends(session_manager.get_current_user)
):
    """Get logs for a specific user ID or the current user."""
    try:
        # Get current user data
        current_user_data = await user_manager.get_user_by_username(current_user)
        current_user_id = current_user_data["user_id"]
        current_user_role = current_user_data.get("role", "user")
        
        # Determine which user_id to use
        target_user_id = user_id if user_id else current_user_id
        
        # Only admins can view other users' logs
        if user_id and user_id != current_user_id and current_user_role != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access this user's logs"
            )
        
        filters = {"user_id": target_user_id}
        
        # Add optional filters
        if action_type:
            filters["action_type"] = action_type
        if status:
            filters["status"] = status
        
        # Handle date filtering
        date_filter = {}
        if start_date:
            try:
                start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                date_filter["start_date"] = start_datetime
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
                )
        
        if end_date:
            try:
                end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                date_filter["end_date"] = end_datetime
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
                )
        
        logs = session_manager.get_logs(
            filters=filters,
            date_filter=date_filter,
            limit=limit,
            offset=offset
        )
        
        # Log this action
        log_message = f"Retrieved {len(logs)} logs"
        if user_id and user_id != current_user_id:
            log_message += f" for user {user_id}"
        
        session_manager.log_action(
            content=log_message,
            user_id=current_user_id,
            action_type="logs_retrieval"
        )
        
        return LogListResponse(logs=logs)
    
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Error retrieving user logs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while retrieving logs: {str(e)}"
        )

@router.get("/logs", response_model=LogListResponse)
async def get_logs(
    user_id: Optional[str] = Query(None),
    username: Optional[str] = Query(None),
    all_logs: bool = Query(False),  # Flag to get all logs (admin only)
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    action_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user: str = Depends(session_manager.get_current_user)
):
    """
    Get logs with flexible filtering options:
    - No parameters: returns current user's logs
    - user_id or username: returns specified user's logs (admin only)
    - all_logs=True: returns all system logs (admin only)
    """
    try:
        # Get current user data
        current_user_data = await user_manager.get_user_by_username(current_user)
        current_user_id = current_user_data["user_id"]
        current_user_role = current_user_data.get("role", "user")
        
        # Initialize filters
        filters = {}
        
        # Determine which logs to retrieve
        if all_logs:
            # Admin only - retrieve all logs
            if current_user_role != "admin":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Not authorized to access all system logs"
                )
            log_action_type = "admin_all_logs_retrieval"
            log_message = "Retrieved all system logs"
            
        elif user_id or username:
            # Get logs for a specific user (admin only)
            if current_user_role != "admin":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Not authorized to access other users' logs"
                )
                
            # Determine target user ID
            target_user_id = user_id
            if username and not user_id:
                user_data = await user_manager.get_user_by_username(username)
                if not user_data:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User not found"
                    )
                target_user_id = user_data["user_id"]
                
            filters["user_id"] = target_user_id
            log_action_type = "admin_user_logs_retrieval"
            log_message = f"Retrieved logs for user {username or target_user_id}"
            
        else:
            # Get logs for the current user
            filters["user_id"] = current_user_id
            log_action_type = "logs_retrieval"
            log_message = "Retrieved personal logs"
        
        # Add optional filters
        if action_type:
            filters["action_type"] = action_type
        if status:
            filters["status"] = status
        
        # Handle date filtering
        date_filter = {}
        if start_date:
            try:
                start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                date_filter["start_date"] = start_datetime
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
                )
        
        if end_date:
            try:
                end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                date_filter["end_date"] = end_datetime
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)."
                )
        
        # Get logs from session manager
        logs = session_manager.get_logs(
            filters=filters,
            date_filter=date_filter,
            limit=limit,
            offset=offset
        )
        
        # Log this action
        session_manager.log_action(
            content=f"{log_message} ({len(logs)} results)",
            user_id=current_user_id,
            action_type=log_action_type
        )
        
        return LogListResponse(logs=logs)
    
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Error retrieving logs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while retrieving logs: {str(e)}"
        )
        
# Endpoint for creating a new token pair
@router.post("/token", response_model=None)#TokenPair
async def create_token_pair(request: CreateTokenRequest, response: Response):
    """Create a new token pair (access and refresh) for a user."""
    # Verify the password
    is_valid = await user_manager.verify_user_password(request.username, request.password)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )

    token_pair = session_manager.create_token_pair(request.username)
    
    # Store the tokens in the database
    session_manager.store_token_pair(
        request.username, token_pair.access_token, token_pair.refresh_token
    )
    
    # return token_pair
    # Set the access token in the Authorization header
    response.headers["Authorization"] = f"Bearer {token_pair.access_token}"

    # Return just the success message (optional)
    return {
        "message": "Token Created successful", 
        "username": token_pair.username
    }

# Endpoint for refreshing an access token
@router.post("/token/refresh", response_model=None)#TokenPair
async def refresh_token(request: RefreshTokenRequest, response: Response):
    """Create a new token pair using a valid refresh token."""
    # Verify the refresh token is not blacklisted
    if session_manager.is_token_blacklisted(request.refresh_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token has been revoked"
        )
    
    # Get username from refresh token
    token_pair = session_manager.refresh_access_token(request.refresh_token)
    
    if not token_pair:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # return token_pair
    # Set the access token in the Authorization header
    response.headers["Authorization"] = f"Bearer {token_pair.access_token}"

    # Return just the success message (optional)
    return {
        "message": "Token refreshed successful", 
        "username": token_pair.username
    }

# Endpoint for verifying a token
@router.post("/token/verify", response_model=TokenVerifyResponse)
async def verify_token(request: VerifyTokenRequest):
    """Verify a token and return its data if valid."""
    token_data = session_manager.verify_token(request.token, request.token_type)
    
    if not token_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return TokenVerifyResponse(
        username=token_data.username,
        expires_at=datetime.fromtimestamp(token_data.exp).isoformat(),
        token_type=token_data.token_type,
        valid=True
    )

# Endpoint for revoking a token
@router.post("/token/revoke", response_model=TokenMessageResponse)
async def revoke_token(request: RevokeTokenRequest):
    """Revoke a token by adding it to the blacklist."""
    success = session_manager.revoke_token(request.token)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to revoke token",
        )
    
    return TokenMessageResponse(detail="Token successfully revoked")

# Endpoint for invalidating a token
@router.post("/token/invalidate", response_model=TokenMessageResponse)
async def invalidate_token(access_token: str = Depends(session_manager.get_token_from_header)):
    """Invalidate a token by removing it from the tokens table and adding to blacklist."""
    success = session_manager.invalidate_token(access_token)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to invalidate token",
        )
    
    return TokenMessageResponse(detail="Token successfully invalidated")

# Endpoint for checking if a token is blacklisted
@router.post("/token/blacklist/check", response_model=BlacklistCheckResponse)
async def check_blacklisted(request: RevokeTokenRequest):
    """Check if a token is blacklisted."""
    is_blacklisted = session_manager.is_token_blacklisted(request.token)
    return BlacklistCheckResponse(is_blacklisted=is_blacklisted)

# Endpoint for blacklisting a token
@router.post("/token/blacklist", response_model=TokenIdResponse)
async def blacklist_token(request: BlacklistTokenRequest):
    """Add a token to the blacklist."""
    token_id = session_manager.blacklist_token(request.token, request.username, request.expires_at)
    return TokenIdResponse(token_id=token_id)

# Endpoint for getting all tokens for a user
@router.get("/tokens/user/{username}", response_model=TokenListResponse)
async def get_user_tokens(
    username: str,
    current_user: str = Depends(session_manager.get_current_user)
):
    """Get all active tokens for a specific user."""
    # Only allow users to access their own tokens (or admins)
    if username != current_user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this user's tokens",
        )
    
    tokens = session_manager.get_tokens_by_username(username)
    return TokenListResponse(tokens=tokens)

# Endpoint for getting all tokens (admin only)
@router.get("/tokens/all", response_model=TokenListResponse)
async def get_all_tokens(current_user: str = Depends(session_manager.get_current_user)):
    """Get all active tokens in the database (admin only)."""
    # This endpoint should be protected by an admin role in a real application
    tokens = session_manager.get_all_tokens()
    return TokenListResponse(tokens=tokens)

# Endpoint for token statistics
@router.get("/tokens/stats", response_model=TokenStats)
async def get_token_stats(current_user: str = Depends(session_manager.get_current_user)):
    """Get statistics about active tokens."""
    # This endpoint should be protected by an admin role in a real application
    all_tokens = session_manager.get_all_tokens()
    
    # Analyze token data
    total_tokens = len(all_tokens)
    tokens_by_user = {}
    for token in all_tokens:
        username = token["username"]
        if username in tokens_by_user:
            tokens_by_user[username] += 1
        else:
            tokens_by_user[username] = 1
    
    return TokenStats(
        total_active_tokens=total_tokens,
        tokens_by_user=tokens_by_user,
    )

# Endpoint for cleaning expired tokens
@router.post("/maintenance/clean-expired-tokens", response_model=MaintenanceResponse)
async def clean_expired_tokens(current_user: str = Depends(session_manager.get_current_user)):
    """Clean expired tokens from the tokens table."""
    # This endpoint should be protected by an admin role in a real application
    removed_count = session_manager.clean_expired_tokens()
    return MaintenanceResponse(removed_tokens=removed_count)

# Endpoint for cleaning expired blacklisted tokens
@router.post("/maintenance/clean-expired-blacklist", response_model=MaintenanceResponse)
async def clean_expired_blacklist(current_user: str = Depends(session_manager.get_current_user)):
    """Clean expired tokens from the blacklist."""
    # This endpoint should be protected by an admin role in a real application
    removed_count = session_manager.clean_expired_blacklist()
    return MaintenanceResponse(removed_tokens=removed_count)

# Endpoint for user logout
@router.post("/token-logout", response_model=TokenMessageResponse)
async def logout(token: str = Depends(session_manager.get_token_from_header)):
    """Logout by invalidating the current token."""
    try:
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )
            
        # Verify the token to get the username
        token_data = session_manager.verify_token(token)
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
            
        # Blacklist the token
        # success = session_manager.revoke_token(token)
        success = session_manager.invalidate_token(token)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to revoke token"
            )
        
        return TokenMessageResponse(detail="Successfully logged out")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not logout: {str(e)}",
        )
