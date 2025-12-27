# app/config/rate_limit_middleware.py
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from typing import Callable, Optional
from app.utils.rate_limiter import RateLimiter
import logging

logger = logging.getLogger(__name__)

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware to apply rate limiting to FastAPI routes.
    """
    def __init__(
        self, 
        app: ASGIApp, 
        rate_limiter: RateLimiter,
        user_extractor: Optional[Callable] = None,
        excluded_paths: list = None
    ):
        """
        Initialize the rate limit middleware.
        
        Args:
            app: The FastAPI application
            rate_limiter: The RateLimiter instance to use
            user_extractor: Optional function to extract username from request
            excluded_paths: List of path prefixes to exclude from rate limiting
        """
        super().__init__(app)
        self.rate_limiter = rate_limiter
        self.user_extractor = user_extractor
        self.excluded_paths = excluded_paths or []
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request through rate limiting.
        
        Args:
            request: The incoming request
            call_next: Function to call the next middleware or endpoint
            
        Returns:
            The response from the next middleware or endpoint
        """
        # Skip rate limiting for excluded paths
        for path in self.excluded_paths:
            if request.url.path.startswith(path):
                return await call_next(request)
        
        # Extract username if available
        user = None
        if self.user_extractor:
            try:
                user = await self.user_extractor(request)
            except Exception as e:
                logger.debug(f"Error extracting user: {e}")
        
        # Check rate limit
        is_allowed, headers = await self.rate_limiter.check_rate_limit(request, user)
        
        # Store headers in request state for later use
        request.state.rate_limit_headers = headers
        
        # If rate limited, return 429 response
        if not is_allowed:
            return Response(
                content="Rate limit exceeded",
                status_code=429,
                headers=headers
            )
        
        # Process the request normally
        response = await call_next(request)
        
        # Add rate limit headers to response
        for name, value in headers.items():
            response.headers[name] = value
            
        return response

rate_limit_middleware = RateLimitMiddleware()