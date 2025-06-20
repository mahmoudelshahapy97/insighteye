# async_sessions_2.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path, status, Response
from typing import Optional, Dict, Any, List # Added List for type hinting if needed
from datetime import datetime, timezone
import logging
from uuid import UUID # For type hints and potential direct use if managers expect it
from schemas_models import (LogListResponse, # LogEntry not used directly here
                            LogFilterRequest, TokenPair, # TokenData not used directly here
                            CreateTokenRequest, RefreshTokenRequest, VerifyTokenRequest,
                            RevokeTokenRequest, # InvalidateTokenRequest not used directly
                            BlacklistTokenRequest, TokenVerifyResponse,
                            TokenIdResponse, TokenMessageResponse,
                            BlacklistCheckResponse, MaintenanceResponse, TokenStats,
                            TokenInfo, TokenListResponse)
from async_session_manager import SessionManager # Assuming SessionManager has async methods or is wrapped
from async_user_manager import UserManager     # Assuming UserManager has async methods or is wrapped

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

# Use distinct names or ensure global instances are async-compatible
session_manager_s2 = SessionManager()
user_manager_s2 = UserManager()

# --- Log Endpoints ---
@router.get("/logs", response_model=LogListResponse)
async def get_logs_endpoint( # Renamed from get_logs from original, matching user's async
    user_id_filter: Optional[str] = Query(None, alias="user_id", description="Filter by specific user ID."),
    workspace_id_filter: Optional[str] = Query(None, alias="workspace_id", description="Filter by specific workspace ID."),
    action_type: Optional[str] = Query(None),
    start_date_str: Optional[str] = Query(None, alias="startDate", description="Format: YYYY-MM-DDTHH:MM:SSZ"), # Original only specified ISO
    end_date_str: Optional[str] = Query(None, alias="endDate", description="Format: YYYY-MM-DDTHH:MM:SSZ"),     # Original only specified ISO
    log_status: Optional[str] = Query(None, alias="status"), # Matched original 'log_status', aliased
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)
):
    # Original converts UUID from current_user_data to str for use.
    # current_user_data["user_id"] is likely UUID if TokenData.user_id is UUID
    requesting_user_id = str(current_user_data["user_id"])
    requesting_user_role = current_user_data["role"]

    filters: Dict[str, Any] = {} # Matched original variable name
    log_action_description = ""

    if requesting_user_role == "admin":
        if user_id_filter:
            filters["user_id"] = user_id_filter # Pass as string, like original
            log_action_description = f"Retrieved logs for user {user_id_filter}"
        else:
            log_action_description = "Retrieved system-wide logs"
    else:
        if user_id_filter and user_id_filter != requesting_user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to view other users' logs.")
        filters["user_id"] = requesting_user_id # Pass as string
        log_action_description = "Retrieved personal logs"

    if workspace_id_filter:
        filters["workspace_id"] = workspace_id_filter # Pass as string
        log_action_description += f" for workspace {workspace_id_filter}"
    if action_type:
        filters["action_type"] = action_type
    if log_status: # Matched original param name
        filters["status"] = log_status

    date_filter: Dict[str, Any] = {} # Matched original variable name
    try:
        if start_date_str:
            # Original converts to naive UTC datetime.
            date_filter["start_date"] = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if end_date_str:
            date_filter["end_date"] = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).replace(tzinfo=None)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SSZ).")

    logs_data = await session_manager_s2.get_logs( # Assuming get_logs is async
        filters=filters, date_filter=date_filter,
        limit=limit, offset=offset
    )

    await session_manager_s2.log_action( # Assuming log_action is async
        content=f"{log_action_description} ({len(logs_data)} results). Filters: {filters}, Dates: {date_filter}",
        user_id=requesting_user_id, # Pass string user_id
        action_type="Logs_Retrieval"
    )
    return LogListResponse(logs=logs_data)

@router.post("/logs/filter", response_model=LogListResponse)
async def filter_logs_advanced_endpoint( # Renamed from filter_logs_advanced
    request: LogFilterRequest, # Matched original 'request'
    current_user_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)
):
    requesting_user_id = str(current_user_data["user_id"]) # String, as in original
    requesting_user_role = current_user_data["role"]

    # Logic for admin vs non-admin for username filter from original
    if requesting_user_role != "admin":
        if request.username and request.username != current_user_data["username"]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to filter logs for other users.")
        # No 'else if not request.username:' here, handled by user_id filter below

    filters: Dict[str, Any] = {} # Matched original name
    if request.username:
        user_to_filter = await user_manager_s2.get_user_by_username(request.username)
        if not user_to_filter:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{request.username}' not found for filtering.")
        # Original passes str(user_to_filter["user_id"])
        filters["user_id"] = str(user_to_filter["user_id"]) # user_to_filter["user_id"] is likely UUID
    elif requesting_user_role != "admin": # If no username specified and not admin, filter by self
        filters["user_id"] = requesting_user_id # String

    # Original checks request.workspace_id (from LogFilterRequest)
    # Assuming LogFilterRequest.workspace_id is Optional[UUID]
    if request.workspace_id: filters["workspace_id"] = str(request.workspace_id) # Pass as string
    if request.action_type: filters["action_type"] = request.action_type
    if request.status: filters["status"] = request.status

    date_filter: Dict[str, Any] = {} # Matched original name
    # LogFilterRequest has start_date: Optional[datetime], end_date: Optional[datetime]
    # Pydantic converts string ISO dates from request body to datetime objects.
    # The original implicitly uses these as naive if they are naive, or aware if they are aware.
    # For consistency with GET /logs and "DB stores naive UTC", ensure they are naive if not None.
    if request.start_date:
        date_filter["start_date"] = request.start_date.replace(tzinfo=None) if request.start_date.tzinfo else request.start_date
    if request.end_date:
        date_filter["end_date"] = request.end_date.replace(tzinfo=None) if request.end_date.tzinfo else request.end_date

    logs_data = await session_manager_s2.get_logs(
        filters=filters, date_filter=date_filter,
        limit=request.limit, offset=request.offset,
        sort_by=request.sort_by, sort_direction=request.sort_direction
    )

    await session_manager_s2.log_action(
        content=f"Performed advanced log filtering, found {len(logs_data)} logs. Filters: {request.model_dump(exclude_none=True)}",
        user_id=requesting_user_id, # String
        action_type="Admin_Logs_Filtering" if requesting_user_role == "admin" else "User_Logs_Filtering"
    )
    return LogListResponse(logs=logs_data)

# --- Token Management Endpoints ---
@router.post("/token", response_model=TokenPair)
async def create_token_pair_endpoint(request_data: CreateTokenRequest, response: Response): # Param name like original
    is_valid, username = await user_manager_s2.verify_credentials(request_data.username, None, request_data.password)
    if not is_valid or not username: # Original also checks username implicitly
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    user_account = await user_manager_s2.get_user_by_username(username)
    if not user_account or not user_account.get("user_id"):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User account data incomplete after verification.")

    # user_account["user_id"] is likely UUID. Original casts to UUID explicitly.
    # If user_account["user_id"] is already UUID, no cast needed.
    user_id_uuid = UUID(str(user_account["user_id"])) if not isinstance(user_account["user_id"], UUID) else user_account["user_id"]

    active_workspace_info = await user_manager_s2.get_active_workspace(user_id_uuid)
    active_workspace_id_uuid = UUID(str(active_workspace_info["workspace_id"])) if active_workspace_info and active_workspace_info.get("workspace_id") else None
    if active_workspace_info and active_workspace_info.get("workspace_id") and not isinstance(active_workspace_info["workspace_id"], UUID):
         active_workspace_id_uuid = UUID(str(active_workspace_info["workspace_id"]))
    elif active_workspace_info and isinstance(active_workspace_info["workspace_id"], UUID):
        active_workspace_id_uuid = active_workspace_info["workspace_id"]
    else:
        active_workspace_id_uuid = None


    # session_manager.create_token_pair is sync in the original. Assuming it remains sync.
    token_pair_obj = session_manager_s2.create_token_pair(user_id_uuid, active_workspace_id_uuid)

    await session_manager_s2.store_token_pair( # store_token_pair is async
        user_id_uuid, token_pair_obj.access_token, token_pair_obj.refresh_token, active_workspace_id_uuid
    )

    response.headers["Authorization"] = f"Bearer {token_pair_obj.access_token}"
    return token_pair_obj

@router.post("/token/refresh", response_model=TokenPair)
async def refresh_token_endpoint(request_data: RefreshTokenRequest, response: Response): # Param name like original
    # session_manager.refresh_access_token is async (was sync in original based on usage, but better if async)
    token_pair_obj = await session_manager_s2.refresh_access_token(request_data.refresh_token)

    if not token_pair_obj:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token", headers={"WWW-Authenticate": "Bearer"},
        )

    response.headers["Authorization"] = f"Bearer {token_pair_obj.access_token}"
    return token_pair_obj

@router.post("/token/verify", response_model=TokenVerifyResponse)
async def verify_token_endpoint(request_data: VerifyTokenRequest): # Param name like original
    # session_manager.verify_token is async (was sync in original)
    token_data_obj = await session_manager_s2.verify_token(request_data.token, request_data.token_type)

    if not token_data_obj:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token", headers={"WWW-Authenticate": "Bearer"},
        )

    # Original: await user_manager.get_user_by_id(str(token_data.user_id))
    # token_data_obj.user_id is UUID from TokenData model
    user_details = await user_manager_s2.get_user_by_id(str(token_data_obj.user_id))
    username_for_response = user_details["username"] if user_details else "unknown_user"

    return TokenVerifyResponse(
        user_id=token_data_obj.user_id, # UUID
        username=username_for_response,
        workspace_id=token_data_obj.workspace_id, # UUID or None
        expires_at=datetime.fromtimestamp(token_data_obj.exp, tz=timezone.utc).isoformat(), # Original ensures tz
        token_type=token_data_obj.token_type,
        needs_refresh=token_data_obj.needs_refresh,
        valid=True # If we reach here, it's valid
        # jti removed to match original response which didn't include it
    )

@router.post("/token/revoke", response_model=TokenMessageResponse)
async def revoke_token_endpoint(request_data: RevokeTokenRequest): # Param name like original
    # session_manager.revoke_token is async (was sync)
    success = await session_manager_s2.revoke_token(request_data.token, reason="endpoint_revoke")
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to revoke token (it might be already invalid or blacklisted).")
    return TokenMessageResponse(detail="Token successfully revoked")

@router.post("/token/invalidate", response_model=TokenMessageResponse)
async def invalidate_token_endpoint(access_token: str = Depends(session_manager_s2.get_token_from_header)):
    # session_manager.invalidate_token is async (was sync)
    success = await session_manager_s2.invalidate_token(access_token)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to invalidate token (it might be already invalid or not found).")
    return TokenMessageResponse(detail="Token successfully invalidated and session ended.")

@router.post("/token/blacklist/check", response_model=BlacklistCheckResponse)
async def check_blacklisted_endpoint(request_data: RevokeTokenRequest): # Param name like original
    # session_manager.is_token_blacklisted is async (was sync)
    is_blacklisted = await session_manager_s2.is_token_blacklisted(request_data.token)
    return BlacklistCheckResponse(is_blacklisted=is_blacklisted)

@router.post("/token/blacklist", response_model=TokenIdResponse)
async def blacklist_token_endpoint(
    request_data: BlacklistTokenRequest,
    current_admin_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)
):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

    user_to_blacklist_for = await user_manager_s2.get_user_by_username(request_data.username)
    if not user_to_blacklist_for:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{request_data.username}' not found.")

    # Original: str(user_to_blacklist_for["user_id"])
    user_id_for_blacklist = str(user_to_blacklist_for["user_id"]) # user_to_blacklist_for["user_id"] is likely UUID

    # BlacklistTokenRequest.expires_at is Optional[datetime] (Pydantic converts from ISO string)
    # Original passes request_data.expires_at (datetime object or None) directly.
    expires_at_value = request_data.expires_at # This is already datetime or None

    # session_manager.blacklist_token is async (was sync)
    blacklist_id = await session_manager_s2.blacklist_token(request_data.token, user_id_for_blacklist, expires_at_value, reason="admin_blacklist_endpoint")
    return TokenIdResponse(token_id=blacklist_id) # Assuming blacklist_token returns str ID

@router.get("/tokens/user/{username_path}", response_model=TokenListResponse)
async def get_user_tokens_endpoint(
    username_path: str, # Matched original path param name
    current_user_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)
):
    # requesting_user_id_str = str(current_user_data["user_id"]) # Not used in original for this check
    requesting_user_username = current_user_data["username"]
    requesting_user_role = current_user_data["role"]

    target_username = username_path # Matched original

    if requesting_user_role != "admin" and target_username != requesting_user_username:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to access this user's tokens.")

    # session_manager.get_tokens_by_username is async
    tokens_data = await session_manager_s2.get_tokens_by_username(target_username)
    return TokenListResponse(tokens=[TokenInfo(**t) for t in tokens_data])

@router.get("/tokens/all", response_model=TokenListResponse)
async def get_all_tokens_endpoint(current_admin_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

    # session_manager.get_all_tokens is async (was sync)
    tokens_data = await session_manager_s2.get_all_tokens()
    enriched_tokens = []
    for token_dict in tokens_data:
        # Original gets user_id (string) from token_dict and passes to get_user_by_id
        if "username" not in token_dict and "user_id" in token_dict:
            user_id_str = str(token_dict["user_id"]) # Ensure it's string for get_user_by_id if it expects string
            user = await user_manager_s2.get_user_by_id(user_id_str)
            token_dict["username"] = user["username"] if user else f"user_id_{user_id_str}"
        enriched_tokens.append(TokenInfo(**token_dict))
    return TokenListResponse(tokens=enriched_tokens)

@router.get("/tokens/stats", response_model=TokenStats)
async def get_token_stats_endpoint(current_admin_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")

    # session_manager.get_all_tokens is async (was sync)
    all_tokens = await session_manager_s2.get_all_tokens()
    total_tokens = len(all_tokens)
    tokens_by_user: Dict[str, int] = {} # Key is username (string)

    # Original used user_id_str as key for cache, which is string
    user_id_to_username_cache: Dict[str, str] = {}

    for token_info_dict in all_tokens:
        user_id_str = token_info_dict.get("user_id")
        if not user_id_str: continue
        user_id_str = str(user_id_str) # Ensure it's string for dict key and get_user_by_id

        username_for_stat = user_id_to_username_cache.get(user_id_str)
        if not username_for_stat:
            user = await user_manager_s2.get_user_by_id(user_id_str) # Pass string ID
            username_for_stat = user["username"] if user else f"user_id_{user_id_str}"
            user_id_to_username_cache[user_id_str] = username_for_stat

        tokens_by_user[username_for_stat] = tokens_by_user.get(username_for_stat, 0) + 1

    return TokenStats(total_active_tokens=total_tokens, tokens_by_user=tokens_by_user)

# --- Maintenance Endpoints ---
@router.post("/maintenance/clean-expired-tokens", response_model=MaintenanceResponse)
async def clean_expired_tokens_endpoint(current_admin_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")
    # session_manager.clean_expired_tokens is async (was sync)
    removed_count = await session_manager_s2.clean_expired_tokens()
    # Original response: MaintenanceResponse(removed_tokens=removed_count)
    return MaintenanceResponse(removed_tokens=removed_count)

@router.post("/maintenance/clean-expired-blacklist", response_model=MaintenanceResponse)
async def clean_expired_blacklist_endpoint(current_admin_data: Dict = Depends(session_manager_s2.get_current_user_full_data_dependency)):
    if current_admin_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required.")
    # session_manager.clean_expired_blacklist is async (was sync)
    removed_count = await session_manager_s2.clean_expired_blacklist()
    # Original response: MaintenanceResponse(removed_tokens=removed_count)
    return MaintenanceResponse(removed_tokens=removed_count)
