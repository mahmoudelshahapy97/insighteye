# app/routes/session_router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path, status, Response
from typing import Optional, Dict, Any, List # Added List for type hinting if needed
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import logging
from uuid import UUID # For type hints and potential direct use if managers expect it
from app.schemas import (LogListResponse, # LogEntry not used directly here
                            LogFilterRequest, TokenPair, # TokenData not used directly here
                            CreateTokenRequest, RefreshTokenRequest, VerifyTokenRequest,
                            RevokeTokenRequest, # InvalidateTokenRequest not used directly
                            BlacklistTokenRequest, TokenVerifyResponse,
                            TokenIdResponse, TokenMessageResponse,
                            BlacklistCheckResponse, MaintenanceResponse, TokenStats,
                            TokenInfo, TokenListResponse)
from app.services.workspace_service import workspace_service 
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.utils.permission_utils import is_system_admin_role

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

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
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    # Original converts UUID from current_user_data to str for use.
    # current_user_data["user_id"] is likely UUID if TokenData.user_id is UUID
    requesting_user_id = str(current_user_data["user_id"])
    requesting_user_role = current_user_data["role"]

    filters: Dict[str, Any] = {} # Matched original variable name
    log_action_description = ""

    if is_system_admin_role(requesting_user_role):
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

    logs_data = await session_manager.get_logs( # Assuming get_logs is async
        filters=filters, date_filter=date_filter,
        limit=limit, offset=offset
    )

    await session_manager.log_action( # Assuming log_action is async
        content=f"{log_action_description} ({len(logs_data)} results). Filters: {filters}, Dates: {date_filter}",
        user_id=requesting_user_id, # Pass string user_id
        action_type="Logs_Retrieval"
    )
    return LogListResponse(logs=logs_data)

@router.post("/logs/filter", response_model=LogListResponse)
async def filter_logs_advanced_endpoint( # Renamed from filter_logs_advanced
    request: LogFilterRequest, # Matched original 'request'
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    requesting_user_id = str(current_user_data["user_id"]) # String, as in original
    requesting_user_role = current_user_data["role"]

    # Logic for admin vs non-admin for username filter from original
    if not is_system_admin_role(requesting_user_role):
        if request.username and request.username != current_user_data["username"]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to filter logs for other users.")
        # No 'else if not request.username:' here, handled by user_id filter below

    filters: Dict[str, Any] = {} # Matched original name
    if request.username:
        user_to_filter = await user_manager.get_user_by_username(request.username)
        if not user_to_filter:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{request.username}' not found for filtering.")
        # Original passes str(user_to_filter["user_id"])
        filters["user_id"] = str(user_to_filter["user_id"]) # user_to_filter["user_id"] is likely UUID
    elif not is_system_admin_role(requesting_user_role): # If no username specified and not admin, filter by self
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

    logs_data = await session_manager.get_logs(
        filters=filters, date_filter=date_filter,
        limit=request.limit, offset=request.offset,
        sort_by=request.sort_by, sort_direction=request.sort_direction
    )

    await session_manager.log_action(
        content=f"Performed advanced log filtering, found {len(logs_data)} logs. Filters: {request.model_dump(exclude_none=True)}",
        user_id=requesting_user_id, # String
        action_type="Admin_Logs_Filtering" if is_system_admin_role(requesting_user_role) else "User_Logs_Filtering"
    )
    return LogListResponse(logs=logs_data)
