# admin_router.py

import logging
from fastapi import APIRouter, Depends, HTTPException, status, Request as FastAPIRequest
from uuid import UUID
import asyncpg # For specific DB error type handling

# Managers
from async_database import DatabaseManager
from async_session_manager import SessionManager # For auth and logging
from async_user_manager import UserManager     # Potentially used by SessionManager or for user data enrichment

# Schemas
from schemas_models import SQLQueryRequest, SQLQueryResponse
from typing import Dict, Any # For current_admin_user_data type hint

logger = logging.getLogger(__name__)

# Router setup
router = APIRouter(prefix="/admin", tags=["Admin Utilities"])

# Initialize Managers (similar to async_login_user.py)
db_manager = DatabaseManager()
session_manager = SessionManager()
user_manager = UserManager() # Initialized for consistency, though SessionManager might handle user data

# --- Admin Authentication Dependency ---
async def get_current_admin_user_dependency(
    request_obj: FastAPIRequest, # For logging context
    # This dependency will fetch the full user data, including roles, user_id (UUID), workspace_id (UUID or None)
    current_user_full_data: Dict[str, Any] = Depends(session_manager.get_current_user_full_data_dependency)
) -> Dict[str, Any]:
    """
    Ensures the current user is authenticated and has an 'admin' role.
    Reuses the base authentication from SessionManager and adds role check.
    """
    user_role = current_user_full_data.get("role")
    username = current_user_full_data.get("username", "UnknownUser")
    user_id_obj = current_user_full_data.get("user_id") # Expected to be UUID by now
    workspace_id_obj = current_user_full_data.get("workspace_id") # Expected to be UUID or None

    if user_role != "admin":
        logger.warning(
            f"Non-admin user '{username}' (ID: {str(user_id_obj)}) attempted to access admin route: {request_obj.url.path}"
        )
        # Log this unauthorized access attempt
        await session_manager.log_action(
            content=f"Forbidden access attempt to admin route {request_obj.url.path} by non-admin user '{username}'.",
            user_id=user_id_obj, # Should be UUID
            workspace_id=workspace_id_obj, # Should be UUID or None
            action_type="Admin_Access_Forbidden",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrator privileges required to access this resource."
        )

    logger.info(f"Admin user '{username}' (ID: {str(user_id_obj)}) granted access to admin route: {request_obj.url.path}")
    return current_user_full_data
# --- End Admin Authentication Dependency ---

@router.post(
    "/execute-sql",
    response_model=SQLQueryResponse,
    summary="Execute Arbitrary SQL Query (Admin Only)",
    description=(
        "**HIGHLY DANGEROUS**: Allows an authenticated administrator to execute an arbitrary SQL query. "
        "This endpoint must be strictly protected. \n\n"
        "- For `SELECT` queries, data is returned in the `data` field.\n"
        "- For `INSERT`, `UPDATE`, `DELETE` queries, the number of affected rows is returned in `data.rows_affected`.\n"
        "- For DDL statements (`CREATE`, `ALTER`, `DROP`, etc.), a success message is returned if no error occurs.\n\n"
        "Use the `params` field for parameterized queries to mitigate SQL injection risks within the parameters themselves. "
        "The main query string is executed as provided."
    )
)
async def execute_sql_query_route(
    sql_request: SQLQueryRequest, # Renamed from 'request' to avoid FastAPIRequest conflict
    request_obj: FastAPIRequest,  # For client IP and user agent
    current_admin_user_data: Dict[str, Any] = Depends(get_current_admin_user_dependency) # Enforces admin auth
):
    admin_username = current_admin_user_data.get("username", "UnknownAdmin")
    admin_user_id = current_admin_user_data.get("user_id") # This is UUID from dependency
    admin_workspace_id = current_admin_user_data.get("workspace_id") # UUID or None from dependency

    query_norm = sql_request.query.strip()
    query_lower = query_norm.lower()
    params_tuple = tuple(sql_request.params) if sql_request.params is not None else None

    # For logging, be cautious about logging full queries if they contain sensitive data.
    # Here, we log the first part and the fact that an admin executed it.
    log_content_prefix = f"Admin '{admin_username}' (ID: {str(admin_user_id)}) executed SQL (first 50 chars): '{query_norm[:50]}...'"

    try:
        result_data: Any = None
        message: str = "Query executed."

        if query_lower.startswith("select"):
            result_data = await db_manager.execute_query(query_norm, params=params_tuple, fetch_all=True)
            message = "SELECT query executed successfully."

        elif any(query_lower.startswith(cmd) for cmd in ["insert", "update", "delete"]):
            rowcount = await db_manager.execute_query(query_norm, params=params_tuple, return_rowcount=True)
            result_data = {"rows_affected": rowcount}
            message = f"{query_lower.split()[0].upper()} query executed. Rows affected: {rowcount}."

        elif any(query_lower.startswith(cmd) for cmd in ["create", "alter", "drop", "truncate", "grant", "revoke", "comment"]):
            await db_manager.execute_query(query_norm, params=params_tuple)
            message = f"{query_lower.split()[0].upper()} DDL/DCL query executed successfully."
        else:
            # For other/unknown query types
            logger.info(f"Admin '{admin_username}' executing query of undetermined type (e.g., SET, SHOW): {query_norm[:50]}")
            try:
                # Try to fetch_all, as some utility commands might return rows
                result_data = await db_manager.execute_query(query_norm, params=params_tuple, fetch_all=True)
                message = "Query executed (attempted fetch_all)."
            except HTTPException as http_exc_fetchall:
                # Check if the error is because it's not a query that returns rows
                detail_str = str(http_exc_fetchall.detail).lower()
                if "cannot fetch rows" in detail_str or "no results to fetch" in detail_str:
                    logger.info("Query did not return rows, executing as a non-returning command for admin.")
                    await db_manager.execute_query(query_norm, params=params_tuple)
                    message = "Non-row-returning query executed successfully."
                    result_data = None # Ensure no data from failed fetch_all
                else:
                    raise # Re-raise other fetch_all errors
            except Exception as e_fetchall_generic: # Catch other generic errors from fetch_all not being HTTPException
                logger.info(f"Query execution (fetch_all) failed for admin: {e_fetchall_generic}, trying as non-returning command.")
                await db_manager.execute_query(query_norm, params=params_tuple)
                message = "Non-row-returning query executed successfully after fetch_all attempt failed."
                result_data = None # Ensure no data from failed fetch_all

        # Log successful execution
        await session_manager.log_action(
            content=f"{log_content_prefix}. Result: {message}",
            user_id=admin_user_id,
            workspace_id=admin_workspace_id,
            action_type="Admin_Execute_SQL_Success",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="success"
        )
        return SQLQueryResponse(success=True, message=message, data=result_data)

    except asyncpg.PostgresError as db_err:
        # This block might be hit if a DB error occurs outside db_manager.execute_query,
        # or if db_manager.execute_query is modified not to raise HTTPException for all PostgresErrors.
        # Given current db_manager, most DB errors are converted to HTTPException(500).
        logger.error(f"Database error during admin SQL execution by {admin_username}: {db_err}. Query: {query_norm[:200]}", exc_info=True)
        await session_manager.log_action(
            content=f"{log_content_prefix}. Error: Database error - {str(db_err)}.",
            user_id=admin_user_id,
            workspace_id=admin_workspace_id,
            action_type="Admin_Execute_SQL_DB_Error",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error executing query: {str(db_err)}")

    except HTTPException as http_exc:
        # This will catch HTTPExceptions raised by:
        # 1. get_current_admin_user_dependency (e.g., 401, 403)
        # 2. db_manager.execute_query (e.g., 500 for DB errors, 409 for unique violations)
        # 3. Explicit raises within this route for specific conditions.
        logger.error(f"HTTPException for admin '{admin_username}' SQL execution: {http_exc.detail}. Query: {query_norm[:50]}...", exc_info=False) # exc_info=False if db_manager already logged it
        await session_manager.log_action(
            content=f"{log_content_prefix}. Error: {http_exc.status_code} - {http_exc.detail}.",
            user_id=admin_user_id,
            workspace_id=admin_workspace_id,
            action_type="Admin_Execute_SQL_HTTP_Error",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise http_exc # Re-raise the original HTTPException

    except Exception as e:
        logger.error(f"Unexpected error during admin SQL execution by {admin_username}: {e}. Query: {query_norm[:200]}", exc_info=True)
        await session_manager.log_action(
            content=f"{log_content_prefix}. Error: Unexpected server error - {str(e)}.",
            user_id=admin_user_id,
            workspace_id=admin_workspace_id,
            action_type="Admin_Execute_SQL_Server_Error",
            ip_address=request_obj.client.host if request_obj.client else "N/A",
            user_agent=request_obj.headers.get("user-agent"),
            status="failure"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")
