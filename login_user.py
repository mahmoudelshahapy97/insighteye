############################
# login_user.py
from fastapi import APIRouter, HTTPException, status, Depends, Response, Request as FastAPIRequest
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from uuid import UUID 
from user_manager import UserManager 
from session_manager import SessionManager 
from schemas_models import CreateUserRequest, UpdatePasswordRequest, LoginRequest, TokenPair, MessageRequest, MessageResponse, ContactCreate 
from utils import send_email_from_client_to_admin, send_email
from typing import Dict, Any, Optional 
from config import config
import asyncpg

logger = logging.getLogger(__name__) 

router = APIRouter(tags=["authentication"])
security = HTTPBearer()

# Initialize SessionManager and UserManager (constructors should be non-blocking)
session_manager = SessionManager()
user_manager = UserManager()

@router.post("/signup", response_model=TokenPair, status_code=status.HTTP_201_CREATED)
async def signup_route(user_create_request: CreateUserRequest, request_obj: FastAPIRequest): 
    try:
        existing_users_count_query = "SELECT COUNT(*) as count FROM users"
        count_result_record = await user_manager.db_manager.execute_query(existing_users_count_query, fetch_one=True)
        is_first_user = (count_result_record["count"] == 0) if count_result_record and "count" in count_result_record else True

        role_to_assign = "admin" if is_first_user else "user"

        success, new_user_id_obj, workspace_id_obj = await user_manager.create_user_with_workspace(
            user_create_request.username,
            user_create_request.email,
            user_create_request.password,
            role=role_to_assign
        ) # new_user_id_obj and workspace_id_obj are UUIDs

        if not success or not new_user_id_obj or not workspace_id_obj:
            if not success:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username or email already exists.")
            else: 
                detail_msg = "User or workspace ID missing after user creation process."
                if not new_user_id_obj and not workspace_id_obj:
                    detail_msg = "User and Workspace ID missing after user creation process."
                elif not new_user_id_obj:
                    detail_msg = "User ID missing after user creation process."
                elif not workspace_id_obj:
                    detail_msg = "Workspace ID missing after user creation process."
                logger.error(f"Signup for {user_create_request.username}: {detail_msg} (Success flag was True)")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail_msg)

        token_pair = session_manager.create_token_pair(new_user_id_obj, workspace_id_obj) # Expects Union[str, UUID]

        await session_manager.store_token_pair( 
            new_user_id_obj, # Expects Union[str, UUID]
            token_pair.access_token,
            token_pair.refresh_token,
            workspace_id_obj # Expects Union[str, UUID]
        )

        await session_manager.log_action(
            content=f"User '{user_create_request.username}' (Role: {role_to_assign}) signed up successfully. Assigned to workspace ID: {str(workspace_id_obj)}",
            user_id=new_user_id_obj, 
            workspace_id=workspace_id_obj, 
            action_type="Signup",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )
        return token_pair

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error during signup for {user_create_request.username}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content=f"Signup attempt failed for username '{user_create_request.username}'. Reason: Database error.",
            user_id=None, 
            action_type="Signup_Failed_DB_Error",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred during signup.")
    except ValueError as ve: 
        logger.error(f"Invalid data during signup for {user_create_request.username}: {ve}", exc_info=True)
        await session_manager.log_action(
            content=f"Signup attempt failed for username '{user_create_request.username}'. Reason: Invalid data provided.",
            user_id=None, 
            action_type="Signup_Failed_Invalid_Data",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid data provided: {ve}")
    except HTTPException as http_exc:
        await session_manager.log_action(
            content=f"Signup attempt failed for username '{user_create_request.username}'. Reason: {http_exc.detail}",
            user_id=None, 
            action_type="Signup_Failed_HTTPError",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during signup for {user_create_request.username}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Signup attempt failed for username '{user_create_request.username}'. Reason: Internal server error.",
            user_id=None,
            action_type="Signup_Failed_ServerError",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during signup.")

@router.post("/login", response_model=Dict[str, Any])
async def login_route(login_data: LoginRequest, request_obj: FastAPIRequest, response: Response):
    attempted_identifier = login_data.username or login_data.email or "unknown_identifier"
    try:
        is_valid, username = await user_manager.verify_credentials(login_data.username, login_data.email, login_data.password)
        if not is_valid or not username: 
            await session_manager.log_action(
                content=f"Failed login attempt for '{attempted_identifier}'",
                user_id=None, 
                action_type="Login_Failed",
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"),
                status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials. Please check your username/email and password."
            )

        user_data = await user_manager.get_user_by_username(username) 
        if not user_data: 
            logger.error(f"User '{username}' passed credential check but was not found by get_user_by_username.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found after successful credential verification.")

        user_id_obj = user_data.get("user_id") # This is UUID from UserManager
        if not isinstance(user_id_obj, UUID): 
            logger.error(f"User ID for {username} is not a UUID: {type(user_id_obj)}. Value: {user_id_obj}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error: Invalid user ID format.")

        active_workspace_info = await user_manager.get_active_workspace(user_id_obj) # Expects UUID, returns Dict with UUID for workspace_id
        active_workspace_id_obj: Optional[UUID] = None
        
        if active_workspace_info and active_workspace_info.get("workspace_id"):
            ws_id_raw = active_workspace_info["workspace_id"] # This should now be UUID
            if isinstance(ws_id_raw, UUID):
                active_workspace_id_obj = ws_id_raw
            else: 
                logger.warning(f"Workspace ID for user {username} from get_active_workspace was not UUID: {type(ws_id_raw)}. Value: {ws_id_raw}")
                # Attempt conversion if it's a string UUID
                if isinstance(ws_id_raw, str):
                    try:
                        active_workspace_id_obj = UUID(ws_id_raw)
                    except ValueError:
                         logger.error(f"Failed to convert workspace_id string '{ws_id_raw}' to UUID.")
        
        if not active_workspace_id_obj:
            logger.warning(f"User {username} (ID: {str(user_id_obj)}) has no active or default workspace. Login proceeding without workspace context in token initially.")

        token_pair = session_manager.create_token_pair(user_id_obj, active_workspace_id_obj)

        await session_manager.store_token_pair(
            user_id_obj,
            token_pair.access_token,
            token_pair.refresh_token,
            active_workspace_id_obj
        )

        await session_manager.log_action(
            content=f"User '{username}' logged in successfully. Active Workspace ID: {str(active_workspace_id_obj) if active_workspace_id_obj else 'None'}",
            user_id=user_id_obj,
            workspace_id=active_workspace_id_obj,
            action_type="Login_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )

        response.headers["Authorization"] = f"Bearer {token_pair.access_token}"

        return {
            "access_token": token_pair.access_token,
            "refresh_token": token_pair.refresh_token,
            "token_type": token_pair.token_type,
            "expires_at": token_pair.expires_at,
            "is_active": user_data.get("is_active", False),
            "username": username,
            "user_id": str(user_id_obj), 
            "workspace_id": str(active_workspace_id_obj) if active_workspace_id_obj else None,
            "user_role": user_data.get("role")
        }

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error during login for {attempted_identifier}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content=f"Login error for '{attempted_identifier}'. Reason: Database error.",
            user_id=None,
            action_type="Login_Error_DB",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred during login.")
    except ValueError as ve: 
        logger.error(f"Invalid data during login for {attempted_identifier}: {ve}", exc_info=True)
        await session_manager.log_action(
            content=f"Login error for '{attempted_identifier}'. Reason: Invalid data: {ve}",
            user_id=None,
            action_type="Login_Error_Invalid_Data",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid data: {ve}")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during login for {attempted_identifier}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Login error for '{attempted_identifier}'. Reason: Internal server error.",
            user_id=None,
            action_type="Login_Error_ServerError",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during login.")

@router.post("/refresh-token", response_model=TokenPair)
async def refresh_token_route(request_obj: FastAPIRequest, credentials: HTTPAuthorizationCredentials = Depends(security)): 
    refresh_token_str = credentials.credentials
    user_id_from_unverified_token: Optional[UUID] = None
    workspace_id_from_unverified_token: Optional[UUID] = None
    
    try:
        payload_unverified = session_manager.decode_token_without_verification(refresh_token_str)
        if payload_unverified:
            if payload_unverified.get("user_id"):
                try: user_id_from_unverified_token = UUID(payload_unverified.get("user_id"))
                except ValueError: pass 
            if payload_unverified.get("workspace_id"):
                try: workspace_id_from_unverified_token = UUID(payload_unverified.get("workspace_id"))
                except ValueError: pass 

        validated_token_data = await session_manager.verify_token(refresh_token_str, expected_token_type="refresh")
        
        if not validated_token_data:
            await session_manager.log_action(
                content="Invalid, expired, or revoked refresh token provided for refresh operation.",
                user_id=user_id_from_unverified_token,
                workspace_id=workspace_id_from_unverified_token,
                action_type="Refresh_Token_Verification_Failed",
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"),
                status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token is invalid, expired, or has been revoked."
            )

        new_token_pair = await session_manager.refresh_access_token(refresh_token_str)
        
        if not new_token_pair: 
            logger.error(f"refresh_access_token returned None unexpectedly for user {validated_token_data.user_id}") # Use ID from validated token data
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Token refresh process failed internally.")

        await session_manager.log_action(
            content="Token refreshed successfully",
            user_id=UUID(validated_token_data.user_id), # Log with UUID
            workspace_id=UUID(validated_token_data.workspace_id) if validated_token_data.workspace_id else None, # Log with UUID
            action_type="Refresh_Token_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )
        return new_token_pair

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error during token refresh for user {user_id_from_unverified_token or 'unknown'}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content="Token refresh error: Database error.",
            user_id=user_id_from_unverified_token, 
            workspace_id=workspace_id_from_unverified_token,
            action_type="Refresh_Token_Error_DB",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred during token refresh.")
    except ValueError as ve: 
        logger.error(f"Invalid data during token refresh for user {user_id_from_unverified_token or 'unknown'}: {ve}", exc_info=True)
        await session_manager.log_action(
            content=f"Token refresh error: Invalid data encountered: {ve}",
            user_id=user_id_from_unverified_token, 
            workspace_id=workspace_id_from_unverified_token,
            action_type="Refresh_Token_Error_Invalid_Data",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid data in token: {ve}")
    except HTTPException as http_exc:
        uid_log = user_id_from_unverified_token
        wsid_log = workspace_id_from_unverified_token
        if 'validated_token_data' in locals() and validated_token_data: 
            try: uid_log = UUID(validated_token_data.user_id) 
            except: pass
            try: wsid_log = UUID(validated_token_data.workspace_id) if validated_token_data.workspace_id else None
            except: pass

        await session_manager.log_action(
            content=f"Token refresh failed: {http_exc.detail}",
            user_id=uid_log, workspace_id=wsid_log,
            action_type="Refresh_Token_Denied_HTTPError",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during token refresh for user {user_id_from_unverified_token or 'unknown'}: {e}", exc_info=True)
        await session_manager.log_action(
            content="Token refresh error: Internal server error.",
            user_id=user_id_from_unverified_token, 
            workspace_id=workspace_id_from_unverified_token,
            action_type="Refresh_Token_Error_ServerError",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during token refresh.")

@router.post("/logout")
async def logout_route(request_obj: FastAPIRequest, token: str = Depends(session_manager.get_token_from_header)): 
    user_id_for_log: Optional[UUID] = None
    workspace_id_for_log: Optional[UUID] = None
    try:
        # session_manager.verify_token should be async and check type "access"
        token_data = await session_manager.verify_token(token, expected_token_type="access")
        if not token_data:
            payload_unverified = session_manager.decode_token_without_verification(token)
            unverified_user_id_str = payload_unverified.get("user_id") if payload_unverified else None
            
            await session_manager.log_action(
                content="Logout attempt with invalid or expired access token.",
                user_id=UUID(unverified_user_id_str) if unverified_user_id_str and isinstance(unverified_user_id_str, str) else None, # Ensure it's a string before UUID()
                action_type="Logout_Invalid_Token",
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"),
                status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token for logout."
            )

        user_id_for_log = UUID(token_data.user_id)
        workspace_id_for_log = UUID(token_data.workspace_id) if token_data.workspace_id else None

        # session_manager.invalidate_token should be async, handle blacklisting and DB update
        success = await session_manager.invalidate_token(token) 
        
        if not success: # Token might have been invalidated by another request, or not found
            await session_manager.log_action(
                content="Failed to invalidate token pair during logout (token might be already invalid or not found in DB).",
                user_id=user_id_for_log,
                workspace_id=workspace_id_for_log,
                action_type="Logout_Invalidation_Warning", # Changed from Failed to Warning as logout is still "successful" from user POV
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"),
                status="warning" 
            )
            # Proceed with logout message, as user intent is to logout.

        await session_manager.log_action(
            content="User logged out successfully.",
            user_id=user_id_for_log,
            workspace_id=workspace_id_for_log,
            action_type="Logout_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )
        return {"message": "Logged out successfully", "token_revoked": True}

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error during logout for user {user_id_for_log or 'unknown'}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content="Logout error: Database error.", user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Logout_Error_DB", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred during logout.")
    except ValueError as ve: # e.g. Invalid UUID string from token
        logger.error(f"Invalid data during logout for user {user_id_for_log or 'unknown'}: {ve}", exc_info=True)
        await session_manager.log_action(
            content=f"Logout error: Invalid data encountered: {ve}", user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Logout_Error_Invalid_Data", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid data in token: {ve}")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during logout for user {user_id_for_log or 'unknown'}: {e}", exc_info=True)
        await session_manager.log_action(
            content="Logout error: Internal server error.", user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Logout_Error_ServerError", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during logout.")

@router.post("/revoke-token") 
async def revoke_token_route(request_obj: FastAPIRequest, token: str = Depends(session_manager.get_token_from_header)): 
    user_id_for_log: Optional[UUID] = None
    workspace_id_for_log: Optional[UUID] = None
    token_type_for_log: Optional[str] = None
    try:
        # Verify token first to get details for logging, even if revoke_token does its own verification.
        # This helps in logging even if revoke_token fails early.
        token_data = await session_manager.verify_token(token) # Not expecting specific type, just valid
        if token_data:
            user_id_for_log = UUID(token_data.user_id)
            workspace_id_for_log = UUID(token_data.workspace_id) if token_data.workspace_id else None
            token_type_for_log = token_data.token_type
        else: # Token is invalid (expired, bad signature, blacklisted)
            payload_unverified = session_manager.decode_token_without_verification(token)
            unverified_user_id_str = payload_unverified.get("user_id") if payload_unverified else None
            user_id_for_log = UUID(unverified_user_id_str) if unverified_user_id_str and isinstance(unverified_user_id_str, str) else None

            await session_manager.log_action(
                content="Attempt to revoke an already invalid or expired token.",
                user_id=user_id_for_log, # May be None if token is completely unparseable
                action_type="Revoke_Invalid_Token_Attempt",
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"), status="warning"
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token is invalid or already expired, cannot revoke."
            )
        
        # session_manager.revoke_token should be async and handle blacklisting
        success = await session_manager.revoke_token(token, reason="manual_revocation_endpoint")

        if not success: # Should be caught by revoke_token raising an error if it fails internally
            await session_manager.log_action(
                content="Failed to revoke token (it might have been already blacklisted or a DB error occurred).",
                user_id=user_id_for_log, workspace_id=workspace_id_for_log,
                action_type="Revoke_Token_Failed",
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"), status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to revoke token."
            )

        await session_manager.log_action(
            content=f"Token (type: {token_type_for_log or 'unknown'}) revoked successfully via endpoint.",
            user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Revoke_Token_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="success"
        )
        return {"message": "Token successfully revoked"}

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error during token revocation for user {user_id_for_log or 'unknown'}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content="Token revocation error: Database error.",user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Revoke_Token_Error_DB", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred during token revocation.")
    except ValueError as ve:
        logger.error(f"Invalid data during token revocation for user {user_id_for_log or 'unknown'}: {ve}", exc_info=True)
        await session_manager.log_action(
            content=f"Token revocation error: Invalid data: {ve}",user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Revoke_Token_Error_Invalid_Data", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid data in token: {ve}")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during token revocation for user {user_id_for_log or 'unknown'}: {e}", exc_info=True)
        await session_manager.log_action(
            content="Token revocation error: Internal server error.",user_id=user_id_for_log, workspace_id=workspace_id_for_log,
            action_type="Revoke_Token_Error_ServerError", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during token revocation.")

@router.get("/protected-route") 
async def protected_route(request_obj: FastAPIRequest, current_user_full_data: dict = Depends(session_manager.get_current_user_full_data_dependency)): 
    # Dependency ensures user is authenticated and provides data
    # current_user_full_data should contain user_id, workspace_id as UUID objects
    username = current_user_full_data.get("username")
    user_id_obj = current_user_full_data.get("user_id") 
    user_role = current_user_full_data.get("role")
    workspace_id_obj = current_user_full_data.get("workspace_id")

    try:
        await session_manager.log_action(
            content=f"User '{username}' (Role: {user_role}) accessed protected route.",
            user_id=user_id_obj,
            workspace_id=workspace_id_obj,
            action_type="Protected_Route_Access",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )
        
        # Prepare response, converting UUIDs to strings for JSON compatibility
        response_data = current_user_full_data.copy()
        if isinstance(response_data.get("user_id"), UUID):
            response_data["user_id"] = str(response_data["user_id"])
        if isinstance(response_data.get("workspace_id"), UUID):
            response_data["workspace_id"] = str(response_data["workspace_id"])
            
        return {"message": "Access granted", "user": response_data}

    # Most auth errors should be caught by the dependency. This handles other potential errors.
    except asyncpg.PostgresError as db_err: # Should ideally be caught by dependency if it makes DB calls
        logger.error(f"Database error accessing protected route for user {username}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content=f"Error accessing protected route for user '{username}'. Reason: Database error.", user_id=user_id_obj, workspace_id=workspace_id_obj,
            action_type="Protected_Route_Error_DB", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred.")
    except HTTPException as http_exc: 
        raise http_exc # Re-raise if dependency raised it
    except Exception as e:
        logger.error(f"Unexpected error accessing protected route for user {username}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Error accessing protected route for user '{username}'. Error: Internal server error.", user_id=user_id_obj, workspace_id=workspace_id_obj,
            action_type="Protected_Route_Error_ServerError", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while accessing protected data.")

@router.put("/update-password")
async def update_password_route(
    password_data: UpdatePasswordRequest,
    request_obj: FastAPIRequest, 
    current_user_full_data: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    username = current_user_full_data.get("username")
    user_id_obj = current_user_full_data.get("user_id") 
    workspace_id_obj = current_user_full_data.get("workspace_id")

    try:
        if password_data.username and password_data.username != username:
            await session_manager.log_action(
                content=f"User '{username}' attempt to update password for '{password_data.username}' denied.",
                user_id=user_id_obj, workspace_id=workspace_id_obj, action_type="Update_Password_Forbidden",
                ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only update your own password."
            )

        if password_data.new_password != password_data.confirm_password:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="New passwords do not match.")

        # UserManager.change_password should be async and handle:
        # 1. Verification of current_password
        # 2. Validation of new_password strength
        # 3. Hashing and updating the password in DB
        # It should raise HTTPException for specific failures (e.g., incorrect current pass, weak new pass).
        success = await user_manager.change_password(username, password_data.current_password, password_data.new_password)
        
        if not success: # Should be caught by HTTPException in change_password
            # This path indicates an unexpected failure if change_password returned False without raising.
            await session_manager.log_action(
                content=f"Password update for user '{username}' failed unexpectedly (change_password returned False).",
                user_id=user_id_obj, workspace_id=workspace_id_obj, action_type="Update_Password_Unexpected_Fail",
                ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to update password due to an unexpected internal error."
            )

        await session_manager.log_action(
            content=f"Password updated successfully for user '{username}'.",
            user_id=user_id_obj, workspace_id=workspace_id_obj, action_type="Update_Password_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="success"
        )
        return {"message": "Password updated successfully"}

    except asyncpg.PostgresError as db_err:
        logger.error(f"Database error updating password for user {username}: {db_err}", exc_info=True)
        await session_manager.log_action(
            content=f"Password update error for user '{username}'. Reason: Database error.", user_id=user_id_obj, workspace_id=workspace_id_obj,
            action_type="Update_Password_Error_DB", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred while updating password.")
    except HTTPException as http_exc: # Includes errors from user_manager.change_password
        # Log if not already logged by change_password, or if adding context
        log_action_type = "Update_Password_Denied_HTTPError"
        if http_exc.status_code == status.HTTP_401_UNAUTHORIZED: log_action_type = "Update_Password_Incorrect_Current"
        elif http_exc.status_code == status.HTTP_400_BAD_REQUEST: log_action_type = "Update_Password_Strength_Or_Input_Fail"
        
        await session_manager.log_action(
            content=f"Password update for user '{username}' failed: {http_exc.detail}",
            user_id=user_id_obj, workspace_id=workspace_id_obj, action_type=log_action_type,
            ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating password for user {username}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Password update error for user '{username}'. Error: Internal server error.", user_id=user_id_obj, workspace_id=workspace_id_obj,
            action_type="Update_Password_Error_ServerError", ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while updating password.")

@router.post("/contact", status_code=status.HTTP_201_CREATED)
async def create_contact_route(contact: ContactCreate, request_obj: FastAPIRequest): # Renamed for consistency
    log_action_type_prefix = "Contact_Form"
    try:
        admin_email_recipient = config.get("sender_email", "insighteye@gateworx.net") 
        if not admin_email_recipient:
            logger.error("Admin contact email not configured. Cannot process contact form.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System configuration error for contact form.")

        subject_to_admin = f"New Contact Inquiry from {contact.name} ({contact.email})"
        body_to_admin = f"""
        You have received a new contact request:

        Name: {contact.name}
        Email: {contact.email}
        Phone: {contact.phone if contact.phone else 'Not provided'}
        Message:
        {contact.message}

        IP Address: {request_obj.client.host if request_obj.client else 'N/A'}
        User Agent: {request_obj.headers.get("user-agent", 'N/A')}
        """

        # utils.send_email_from_client_to_admin should be async
        success_admin_email = await send_email_from_client_to_admin(
            admin_recipient_email=admin_email_recipient,
            subject=subject_to_admin,
            body=body_to_admin,
            reply_to_email=contact.email
        )

        if not success_admin_email:
            logger.error(f"Failed to send contact email to admin for: {contact.email}")
            await session_manager.log_action(
                content=f"Failed to send contact form (from {contact.name} <{contact.email}>) to admin.",
                action_type=f"{log_action_type_prefix}_Admin_Email_Fail",
                ip_address=request_obj.client.host if request_obj.client else "N/A", 
                user_agent=request_obj.headers.get("user-agent"), status="failure"
            )
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="We encountered an error sending your message to support. Please try again later.")

        # Aligning email content with the synchronous version
        subject_to_user = "Thank You for Contacting Us - InsightEye"
        text_body_to_user = f"""
        Dear {contact.name},

        Thank you for reaching out to us. We have received your inquiry:

        Your Message:
        "{contact.message}" 
        (If this is not what you intended to send, please disregard this email or contact us again.)

        Our team will review your message and get back to you as soon as possible (typically within 1-2 business days).

        If you need immediate assistance, please feel free to contact us directly at {admin_email_recipient}.

        Best regards,
        The InsightEye Team
        GateWorx
        """
        html_body_to_user = f"""
        <html><body>
        <p>Dear {contact.name},</p>
        <p>Thank you for reaching out to us. We have received your inquiry:</p>
        <blockquote><p><i>{contact.message}</i></p></blockquote>
        <p>(If this is not what you intended to send, please disregard this email or contact us again.)</p>
        <p>Our team will review your message and get back to you as soon as possible (typically within 1-2 business days).</p>
        <p>If you need immediate assistance, please feel free to contact us directly at <a href="mailto:{admin_email_recipient}">{admin_email_recipient}</a>.</p>
        <p>Best regards,<br/>The InsightEye Team<br/>GateWorx</p>
        </body></html>
        """
        
        # utils.send_email should be async
        success_user_email = await send_email(
            recipient_email=contact.email,
            subject=subject_to_user,
            body=text_body_to_user,
            html_body=html_body_to_user
        )

        if success_user_email:
            await session_manager.log_action(
                content=f"Contact form submitted by {contact.name} <{contact.email}>. Admin notified. User confirmation sent.",
                action_type=f"{log_action_type_prefix}_Success",
                ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="success"
            )
            return {"message": "Thank you for contacting us. We have received your message and will get back to you shortly."}
        else:
            logger.warning(f"Contact form submitted by {contact.name} <{contact.email}>. Admin was notified, but FAILED to send user confirmation email.")
            await session_manager.log_action(
                content=f"Contact form submitted by {contact.name} <{contact.email}>. Admin notified. FAILED to send user confirmation.",
                action_type=f"{log_action_type_prefix}_User_Email_Fail",
                ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="warning"
            )
            return {"message": "Your message has been sent to support. However, we had trouble sending a confirmation email to you. Please check your spam folder."}

    except asyncpg.PostgresError as db_err: # Should not occur here unless log_action fails
        logger.error(f"Database error processing contact form from {contact.email}: {db_err}", exc_info=True)
        # This log might be redundant if the primary error was email sending and this is from log_action
        await session_manager.log_action(
            content=f"Contact form processing DB error for '{contact.name} <{contact.email}>'.",
            action_type=f"{log_action_type_prefix}_DB_Error",
            ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred while processing your request.")
    except HTTPException as http_exc:
        # Log if not already logged by a deeper layer that raised this
        await session_manager.log_action(
            content=f"Contact form processing failed for '{contact.name} <{contact.email}>'. Reason: {http_exc.detail}",
            action_type=f"{log_action_type_prefix}_HTTPError_Fail",
            ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise http_exc
    except Exception as e: # Catch-all for other errors, e.g., email library failure
        logger.error(f"Unexpected error processing contact form from {contact.email}: {e}", exc_info=True)
        await session_manager.log_action(
            content=f"Contact form processing error for '{contact.name} <{contact.email}>': Internal server error.",
            action_type=f"{log_action_type_prefix}_ServerError_Fail",
            ip_address=request_obj.client.host if request_obj.client else "N/A", user_agent=request_obj.headers.get("user-agent"), status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while processing your contact request.")

@router.post("/message", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
async def create_message_route(message_request: MessageRequest, request_obj: FastAPIRequest): 
    try:
        log_message_str = f"Client Message: {(message_request.title + ': ') if message_request.title else ''}{message_request.message}"
        if message_request.level == "error": logger.error(log_message_str)
        elif message_request.level == "warning": logger.warning(log_message_str)
        else: logger.info(log_message_str) # Default to info for "info" or other levels

        message_id = f"msg_{datetime.now(ZoneInfo('Africa/Cairo')).strftime('%Y%m%d%H%M%S%f')}"

        # Log to system logs via SessionManager if it's an error reported by client
        if message_request.level == "error":
            await session_manager.log_action(
                content=f"Client Reported Error: {(message_request.title + ': ') if message_request.title else ''}{message_request.message}",
                action_type="Client_Reported_Error_Message", # More specific
                ip_address=request_obj.client.host if request_obj.client else "N/A",
                user_agent=request_obj.headers.get("user-agent"),
                status="failure" # Using status to map severity for logs
            )
        
        return MessageResponse(
            id=message_id, 
            message=message_request.message, 
            title=message_request.title,
            level=message_request.level, 
            timestamp=datetime.now(ZoneInfo("Africa/Cairo"))
        )
    except asyncpg.PostgresError as db_err: # If log_action fails
        logger.error(f"Database error processing POST /message: {db_err}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A database error occurred.")
    except Exception as e:
        logger.error(f"Unexpected error processing POST /message: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while processing the message.")

@router.get("/message", response_model=MessageResponse, status_code=status.HTTP_200_OK)
async def get_message_route(request_obj: FastAPIRequest): 
    try:
        message_content = "This is a default system message from GET /message."
        title = "System Information"
        level = "info"
        
        logger.info(f"Default GET /message accessed by {request_obj.client.host if request_obj.client else 'N/A'}")
        message_id = f"msg_{datetime.now(ZoneInfo('Africa/Cairo')).strftime('%Y%m%d%H%M%S%f')}"
        
        return MessageResponse(
            id=message_id, 
            message=message_content,
            title=title, 
            level=level, 
            timestamp=datetime.now(ZoneInfo("Africa/Cairo"))
        )
    except Exception as e: # Should be very unlikely for this simple GET
        logger.error(f"Unexpected error processing GET /message: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while retrieving the message.")
        