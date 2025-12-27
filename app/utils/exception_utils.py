# app/utils/exception_utils.py
"""Exception handling utilities for async operations."""

import logging
from functools import wraps

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


def handle_exceptions(func):
    """Decorator for handling exceptions in async functions."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An unexpected error occurred."
            )
    return wrapper
