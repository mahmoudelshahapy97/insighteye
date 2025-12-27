# app/config/rate_limiter.py
import time
import logging
from typing import Dict, Optional, List, Tuple, Callable
from fastapi import Request, HTTPException, status
import functools
from datetime import datetime, timedelta
from app.services.database import db_manager


logger = logging.getLogger(__name__)
        
class RateLimiter:
    """
    Implements rate limiting using a sliding window algorithm with PostgreSQL backend.
    
    Features:
    - Configurable time windows and limits
    - Different rate limits for different endpoints
    - IP-based and user-based rate limiting
    - In-memory fallback if database connection fails
    """
    def __init__(self, 
                 redis_url: Optional[str] = None,  # Kept for compatibility
                 default_limit: int = 60,
                 default_window: int = 60,
                 enable_in_memory_fallback: bool = True):
        """
        Initialize the rate limiter.
        
        Args:
            redis_url: Not used (kept for compatibility)
            default_limit: Default request limit per window
            default_window: Default time window in seconds
            enable_in_memory_fallback: Whether to use in-memory fallback if database fails
        """
        self.default_limit = default_limit
        self.default_window = default_window
        self.enable_in_memory_fallback = enable_in_memory_fallback
        self.limits_config = {}
        
        # Initialize database tables
        self._init_database()
        
        # In-memory storage fallback
        self.in_memory_storage = {}
        self.last_cleanup = time.time()
        
    def _init_database(self):
        """Initialize the rate limiting tables in the database."""
        cur = None
        try:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            
            # Create rate limit requests table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_requests (
                id SERIAL PRIMARY KEY,
                key TEXT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Create index on key and timestamp for faster lookups
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_rate_limit_key_timestamp 
            ON rate_limit_requests (key, timestamp)
            """)
            
            conn.commit()
            logger.info("Rate limiting tables initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize rate limiting tables: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def configure_limit(self, 
                        path: str, 
                        limit: int, 
                        window: int = None, 
                        by_user: bool = True, 
                        by_ip: bool = True):
        """
        Configure rate limits for specific paths.
        
        Args:
            path: API path to apply rate limit to (can use wildcards like "/auth/*")
            limit: Maximum number of requests per window
            window: Time window in seconds (defaults to default_window)
            by_user: Apply limit per user
            by_ip: Apply limit per IP address
        """
        self.limits_config[path] = {
            "limit": limit,
            "window": window or self.default_window,
            "by_user": by_user,
            "by_ip": by_ip
        }
        
    def _get_config_for_path(self, path: str) -> Dict:
        """Get rate limit configuration for a path."""
        # First check for exact match
        if path in self.limits_config:
            return self.limits_config[path]
            
        # Check for wildcard match
        for pattern, config in self.limits_config.items():
            if pattern.endswith("*") and path.startswith(pattern[:-1]):
                return config
                
        # Return default configuration
        return {
            "limit": self.default_limit,
            "window": self.default_window,
            "by_user": True,
            "by_ip": True
        }
    
    def _get_keys(self, request: Request, user: str = None) -> List[str]:
        """Generate rate limit keys based on configuration."""
        path = request.url.path
        config = self._get_config_for_path(path)
        keys = []
        
        # Get client IP
        client_ip = request.client.host if request.client else "unknown"
        
        # Add IP-based key if configured
        if config["by_ip"]:
            keys.append(f"ratelimit:ip:{client_ip}:{path}")
            
        # Add user-based key if configured and a user is authenticated
        if config["by_user"] and user:
            keys.append(f"ratelimit:user:{user}:{path}")
            
        # If no keys were generated, use IP as fallback
        if not keys:
            keys.append(f"ratelimit:ip:{client_ip}:{path}")
            
        return keys
    
    def _check_db_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        """Check rate limit using PostgreSQL database."""
        try:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            
            now = datetime.utcnow()
            window_start = now - timedelta(seconds=window)
            
            # Insert current request
            cur.execute(
                "INSERT INTO rate_limit_requests (key, timestamp) VALUES (%s, %s)",
                (key, now)
            )
            
            # Count requests in the current window
            cur.execute(
                "SELECT COUNT(*) FROM rate_limit_requests WHERE key = %s AND timestamp > %s",
                (key, window_start)
            )
            count = cur.fetchone()[0]
            
            # Clean up old requests periodically (1% chance to avoid doing it too often)
            if time.time() % 100 < 1:
                cleanup_time = now - timedelta(days=1)  # Remove entries older than 1 day
                cur.execute(
                    "DELETE FROM rate_limit_requests WHERE timestamp < %s",
                    (cleanup_time,)
                )
            
            conn.commit()
            
            # Check if under limit
            is_allowed = count <= limit
            
            # Calculate remaining requests and reset time
            remaining = max(0, limit - count)
            reset_time = int((now + timedelta(seconds=window)).timestamp())
            
            return is_allowed, remaining, reset_time
            
        except Exception as e:
            logger.error(f"Database rate limit error: {e}")
            conn.rollback()
            # Fall back to in-memory if database fails
            if self.enable_in_memory_fallback:
                return self._check_memory_limit(key, limit, window)
            return True, limit, int(time.time() + window)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def _check_memory_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        """Check rate limit using in-memory storage."""
        now = time.time()
        
        # Clean up expired entries periodically (every 60 seconds)
        if now - self.last_cleanup > 60:
            self._cleanup_memory_storage()
            self.last_cleanup = now
        
        # Initialize if key doesn't exist
        if key not in self.in_memory_storage:
            self.in_memory_storage[key] = []
            
        # Add current timestamp
        self.in_memory_storage[key].append(now)
        
        # Remove timestamps outside window
        self.in_memory_storage[key] = [t for t in self.in_memory_storage[key] if t > now - window]
        
        # Count requests in window
        count = len(self.in_memory_storage[key])
        
        # Check if under limit
        is_allowed = count <= limit
        
        # Calculate remaining requests and reset time
        remaining = max(0, limit - count)
        reset_time = now + window
        
        return is_allowed, remaining, int(reset_time)
    
    def _cleanup_memory_storage(self):
        """Remove expired entries from memory storage."""
        now = time.time()
        for key in list(self.in_memory_storage.keys()):
            # Find the configuration for this key
            path = key.split(":", 3)[-1] if ":" in key else ""
            config = self._get_config_for_path(path)
            window = config["window"]
            
            # Remove timestamps outside the window
            self.in_memory_storage[key] = [t for t in self.in_memory_storage[key] if t > now - window]
            
            # Remove empty lists
            if not self.in_memory_storage[key]:
                del self.in_memory_storage[key]
    
    async def check_rate_limit(self, request: Request, user: str = None) -> Tuple[bool, Dict]:
        """
        Check if a request is within rate limits.
        
        Args:
            request: The FastAPI request object
            user: Username if authenticated
            
        Returns:
            Tuple of (is_allowed, headers_dict)
        """
        path = request.url.path
        config = self._get_config_for_path(path)
        limit = config["limit"]
        window = config["window"]
        
        # Get all applicable keys
        keys = self._get_keys(request, user)
        
        # Check each key (we're limited by the most restrictive rule)
        min_remaining = limit
        min_reset = int(time.time()) + window
        is_allowed = True
        
        for key in keys:
            # Use database check with in-memory fallback
            key_allowed, key_remaining, key_reset = self._check_db_limit(key, limit, window)
                
            # Update minimum values
            is_allowed = is_allowed and key_allowed
            min_remaining = min(min_remaining, key_remaining)
            min_reset = min(min_reset, key_reset)
        
        # Prepare headers
        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(min_remaining),
            "X-RateLimit-Reset": str(min_reset)
        }
        
        return is_allowed, headers
    
    async def clean_expired_entries(self):
        """
        Clean up expired rate limit entries.
        This method is intended to be called periodically by a background task.
        """
        try:
            conn = db_manager.get_connection()
            cur = conn.cursor()
            
            # Delete entries older than 1 day
            cleanup_time = datetime.utcnow() - timedelta(days=1)
            cur.execute(
                "DELETE FROM rate_limit_requests WHERE timestamp < %s",
                (cleanup_time,)
            )
            
            count = cur.rowcount
            conn.commit()
            logger.info(f"Cleaned up {count} expired rate limit entries")
            
        except Exception as e:
            logger.error(f"Error cleaning up rate limit entries: {e}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    def rate_limit_dependency(self, user_extractor: Callable = None):
        """
        Create a FastAPI dependency for rate limiting.
        
        Args:
            user_extractor: Optional function to extract username from request
            
        Returns:
            A dependency function for FastAPI
        """
        async def dependency(request: Request):
            # Extract username if available
            user = None
            if user_extractor:
                try:
                    user = await user_extractor(request)
                except:
                    # If we can't extract a user, continue with IP-based limiting
                    pass
                    
            # Check rate limit
            is_allowed, headers = await self.check_rate_limit(request, user)
            
            # Add headers to response
            request.state.rate_limit_headers = headers
            
            # Raise exception if rate limited
            if not is_allowed:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                    headers=headers
                )
                
            return request
            
        return dependency
    
    def limit(self, limit: int = None, window: int = None):
        """
        Decorator for rate limiting specific routes.
        
        Args:
            limit: Request limit (overrides default)
            window: Time window in seconds (overrides default)
            
        Returns:
            A decorator function
        """
        def decorator(func):
            # Get endpoint path from function
            path = func.__name__
            
            # Configure specific limit for this path
            self.configure_limit(
                path=path,
                limit=limit or self.default_limit,
                window=window or self.default_window
            )
            
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                # Get request object
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                
                if not request:
                    for _, value in kwargs.items():
                        if isinstance(value, Request):
                            request = value
                            break
                
                if not request:
                    # We can't rate limit without a request
                    return await func(*args, **kwargs)
                
                # Check rate limit
                is_allowed, headers = await self.check_rate_limit(request)
                
                # Store headers in request state for middleware to pick up
                request.state.rate_limit_headers = headers
                
                # Raise exception if rate limited
                if not is_allowed:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Rate limit exceeded",
                        headers=headers
                    )
                
                # Call the original function
                return await func(*args, **kwargs)
                
            return wrapper
        return decorator

rate_limiter = RateLimiter()