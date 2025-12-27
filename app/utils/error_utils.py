# app/utils/error_utils.py
from functools import wraps
from typing import Optional, Callable
import asyncpg
from fastapi import HTTPException, status
import logging

logger = logging.getLogger(__name__)

def handle_db_errors(
    not_found_msg: Optional[str] = "Resource not found",
    error_msg: Optional[str] = "Database error occurred"
):
    """
    Decorator for handling common database errors.
    
    Usage:
        @handle_db_errors(not_found_msg="User not found")
        async def get_user(self, user_id):
            result = await self.db_manager.execute_query(...)
            if not result:
                return None  # Will raise 404
            return result
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                # If function returns None, treat as not found
                if result is None and not_found_msg:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=not_found_msg
                    )
                return result
                
            except HTTPException:
                # Re-raise HTTP exceptions as-is
                raise
                
            except asyncpg.UniqueViolationError as e:
                logger.error(f"Unique constraint violation: {e}")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Resource already exists: {e.detail or ''}"
                )
                
            except asyncpg.ForeignKeyViolationError as e:
                logger.error(f"Foreign key violation: {e}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid reference to related resource"
                )
                
            except asyncpg.PostgresError as e:
                logger.error(f"Database error in {func.__name__}: {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=error_msg
                )
                
            except Exception as e:
                logger.error(
                    f"Unexpected error in {func.__name__}: {e}",
                    exc_info=True
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="An unexpected error occurred"
                )
        
        return wrapper
    return decorator