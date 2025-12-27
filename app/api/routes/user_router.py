# app/routes/user_router.py
from fastapi import APIRouter, HTTPException, status, Depends, Request as FastAPIRequest
from typing import Dict, Any
from uuid import UUID
from app.services.database import db_manager
from app.services.session_service import session_manager 
from app.services.user_service import user_manager
from app.schemas import SQLQueryRequest, SQLQueryResponse, CreateUserRequest, VerifyPasswordRequest, ResetPasswordRequest, EmailRequest, UserRequest 
import logging
import asyncpg

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/users", tags=["users"])

@router.post("", status_code=status.HTTP_201_CREATED)  # Use 201 Created for successful creation
async def create_user_route(request: CreateUserRequest):
    """
    Creates a new user.

    Args:
        username: The username for the new user.
        password: The password for the new user.
        email: The email address for the new user.

    Returns:
        A message indicating success or failure.

    Raises:
        HTTPException: If the username already exists or if a database error occurs.
    """
    try:
        success = await user_manager.create_user(request.username, request.email, request.password, request.role, request.count_of_camera)
        if success:
            return {"message": "User created successfully"}
        else:
            raise HTTPException(status_code=409, detail="Username already exists")  # Conflict
    except HTTPException as http_exc:
        print(f"create_user_route: HTTPException caught - Status Code: {http_exc.status_code}") # Add print here
        raise http_exc
    except Exception as e:
        print(f"create_user_route: Unexpected Exception - Raising 500: {e}") # Add print here
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.post("/verify_password")
async def verify_password_route(
    request: VerifyPasswordRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Verifies a user's password.

    Args:
        username: The username of the user.
        password: The password to verify.

    Returns:
        A message indicating whether the password is valid.

    Raises:
        HTTPException: If the user is not found or if a database error occurs.
    """
    try:
        is_valid = await user_manager.verify_user_password(request.username, request.password)
        if is_valid:
            return {"message": "Password is valid"}
        else:
            return {"message": "Invalid username or password"}  # Don't reveal which is wrong
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.post("/by_email")
async def get_user_by_email_route(
    request: EmailRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Gets a user's username by their email address.

    Args:
        email: The email address of the user.

    Returns:
        The username of the user

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        data = await user_manager.get_user_by_email(request.email)
        if data:  # FIX: Changed from 'username' to 'data'
            return {"username": data["username"]}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.put("/reset_password")
async def reset_password_route(
    request: ResetPasswordRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Resets a user's password.

    Args:
        email: The email of the user.
        new_password: The new password for the user.

    Returns:
        A message indicating success or failure.

    Raises:
       HTTPException: If the user is not found or if a database error occurs.
    """

    try:
        user_data = await user_manager.get_user_by_email(request.email)
        success = await user_manager.reset_password(user_data["username"], request.new_password)
        if success:
            return {"message": "Password reset successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.get("")
async def get_all_users_route(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Gets all users.

    Returns:
        A list of all users.

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        users = await user_manager.get_all_users()
        return users
    except HTTPException as http_exc:
         raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("/user")
async def delete_user_route(
    request: UserRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Deletes a user.

    Args:
        request: UserRequest containing username.

    Returns:
        A message indicating success.

    Raises:
        HTTPException: If the user is not found or if a database error occurs.
    """
    try:
        success = await user_manager.delete_user_db_only(request.username)
        if success:
            return {"message": f"User '{request.username}' deleted successfully."}
        else:
            raise HTTPException(status_code=404, detail=f"User '{request.username}' not found.")
    except HTTPException as e:  # Catch HTTPExceptions raised by delete_user
        raise e
    except Exception as e: #catch the other exceptions
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("")
async def delete_all_users_route(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Deletes all users.

    Returns:
        A message indicating how many users were deleted.

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        count = await user_manager.delete_all_users()
        return {"message": f"Deleted {count} users."}
    except HTTPException as e:
        raise e
    except Exception as e: #catch the other exceptions
         raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("/{username}/complete", response_model=Dict)
async def delete_user_completely(
    username: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Completely delete a user from all systems.
    
    This will:
    - Remove all camera data from Qdrant
    - Remove user from all workspaces
    - Delete all database records (cascading)
    - Clean up all related resources
    
    Requires:
    - Admin role OR user deleting their own account
    - User must not be the only admin in any workspace
    """
    current_username = current_user.get("username")
    current_user_id = current_user.get("user_id")
    is_admin = current_user.get("role") == "admin"
    
    # Check if user can perform deletion
    if not is_admin and current_username != username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only delete your own account unless you are an admin"
        )
    
    result = await user_manager.delete_user_completely(
        username=username,
        performing_user_id=UUID(str(current_user_id)) if current_user_id else None,
        is_admin=is_admin
    )
    
    return {
        "message": f"User '{username}' has been completely deleted from all systems",
        "details": result
    }

@router.get("/{username}/deletion-preview", response_model=Dict)
async def preview_user_deletion(
    username: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Preview what will be deleted when removing a user.
    
    Returns:
    - Count of cameras, workspaces, sessions, etc.
    - Workspace memberships and roles
    - Warnings about deletion constraints
    
    Use this before actual deletion to understand the impact.
    """
    current_username = current_user.get("username")
    is_admin = current_user.get("role") == "admin"
    
    # Check if user can view deletion preview
    if not is_admin and current_username != username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only preview your own account deletion unless you are an admin"
        )
    
    preview = await user_manager.get_user_deletion_preview(username)
    
    return {
        "message": f"Deletion preview for user '{username}'",
        "preview": preview,
        "can_delete": len(preview.get("warnings", [])) == 0
    }

@router.delete("/me", response_model=Dict)
async def delete_own_account(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Delete your own account completely.
    
    This is a convenience endpoint for users to self-delete.
    Will fail if you are the only admin in any workspace.
    """
    username = current_user.get("username")
    user_id = current_user.get("user_id")
    
    if not username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not determine current user"
        )
    
    result = await user_manager.delete_user_completely(
        username=username,
        performing_user_id=UUID(str(user_id)) if user_id else None,
        is_admin=False
    )
    
    return {
        "message": "Your account has been completely deleted",
        "details": result
    }

@router.post("/admin/bulk-delete", response_model=Dict)
async def bulk_delete_users(
    usernames: list[str],
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint to delete multiple users at once.
    
    Will skip users who are the only admin in workspaces.
    Returns detailed results for each user.
    """
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    results = {
        "successful_deletions": [],
        "failed_deletions": [],
        "skipped": []
    }
    
    for username in usernames:
        try:
            # Check constraints first
            preview = await user_manager.get_user_deletion_preview(username)
            if preview.get("warnings"):
                results["skipped"].append({
                    "username": username,
                    "reason": preview["warnings"]
                })
                continue
            
            # Perform deletion
            result = await user_manager.delete_user_completely(
                username=username,
                performing_user_id=UUID(str(current_user.get("user_id"))),
                is_admin=True
            )
            
            results["successful_deletions"].append({
                "username": username,
                "details": result
            })
            
        except Exception as e:
            results["failed_deletions"].append({
                "username": username,
                "error": str(e)
            })
    
    return {
        "message": f"Bulk deletion completed",
        "summary": {
            "total_requested": len(usernames),
            "successful": len(results["successful_deletions"]),
            "failed": len(results["failed_deletions"]),
            "skipped": len(results["skipped"])
        },
        "results": results
    }

async def get_current_admin_user_dependency(
    request_obj: FastAPIRequest, # For logging context
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
