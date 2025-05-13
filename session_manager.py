# session_manager.py
from fastapi import Request, Form, Depends, WebSocket, WebSocketDisconnect, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import time
from typing import Dict, Optional, List
from config import config
from database import get_db_connection
from dotenv import load_dotenv
import os
import uuid
import secrets
from datetime import datetime, timedelta, timezone 
import pytz
from schemas_models import TokenPair, TokenData
import jwt
from jwt import DecodeError, PyJWTError
import logging
import hashlib
import json
from token_expiration import TokenExpirationStrategy

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)
# Create a security instance
security = HTTPBearer()

class SessionManager:
    # Add these class variables
    ACCESS_TOKEN_EXPIRE_MINUTES = config.get("ACCESS_TOKEN_EXPIRE_MINUTES", 30)  # Short-lived (15 min)
    REFRESH_TOKEN_EXPIRE_DAYS = config.get("REFRESH_TOKEN_EXPIRE_DAYS", 7)     # Long-lived (7 days)
    SECRET_KEY = config.get("SECRET_KEY") ##secrets.token_urlsafe(32)  
    ALGORITHM = config.get("ALGORITHM", "HS256")

    def __init__(self):
        # Initialize token expiration strategy
        self.token_expiration = TokenExpirationStrategy(
            access_token_expire_minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES,
            refresh_token_expire_days=self.REFRESH_TOKEN_EXPIRE_DAYS,
            sliding_window=True,  # Enable sliding window expiration
            minimum_remaining_time_percent=0.2  # Auto-refresh at 20% remaining lifetime
        )

    def _execute_db_query(self, query, params=None, fetch_one=False, fetch_all=False, return_rowcount=False):
        """Execute a database query with error handling and connection management."""
        conn = get_db_connection()
        cur = conn.cursor()
        result = None
        
        try:
            cur.execute(query, params or ())
            
            if fetch_one:
                result = cur.fetchone()
            elif fetch_all:
                result = cur.fetchall()
            elif return_rowcount:
                result = cur.rowcount
                
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            cur.close()
            conn.close()

    # Token Management Methods
    def is_token_blacklisted(self, token: str) -> bool:
        """Check if a token is blacklisted."""
        try:
            result = self._execute_db_query(
                "SELECT blacklist_id FROM token_blacklist WHERE token = %s",
                (token,),
                fetch_one=True
            )
            return result is not None
        except Exception as e:
            logger.error(f"Error checking blacklisted token: {str(e)}")
            # Return False instead of raising an exception to keep the flow going
            return False

    def blacklist_token(self, token: str, user_id: str, expires_at: str, reason: str = None) -> str:
        """Add a token to the blacklist."""
        blacklist_id = str(uuid.uuid4())
        blacklisted_at = datetime.utcnow()
        
        # Parse the ISO string back to datetime for DB storage
        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))

        self._execute_db_query(
            """
            INSERT INTO token_blacklist (blacklist_id, user_id, token, expires_at, blacklisted_at, reason) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (blacklist_id, user_id, token, expires_at, blacklisted_at, reason)
        )
        return blacklist_id
    
    def create_token(self, data: dict, token_type: str) -> str:
        """Create a JWT token with specified expiration."""
        to_encode = data.copy()
        
        # Add unique JWT ID to prevent replay attacks
        jti = str(uuid.uuid4())

        if token_type == "access":
            expires_delta = timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        else:  # refresh token
            expires_delta = timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
            
        expire = datetime.utcnow() + expires_delta
        to_encode.update({
            "exp": expire.timestamp(), 
            "token_type": token_type, 
            "created_at": datetime.utcnow().timestamp(),
            "jti": jti  # Add JTI
            })
        
        encoded_jwt = jwt.encode(to_encode, self.SECRET_KEY, algorithm=self.ALGORITHM)
        # PyJWT might return bytes in some versions, convert to string if needed
        if isinstance(encoded_jwt, bytes):
            encoded_jwt = encoded_jwt.decode('utf-8')
        return encoded_jwt

    def create_token_with_context(self, user_id: str, client_ip: str) -> TokenPair:
        """Create tokens bound to specific IP address."""
        # Create fingerprint from username and partial IP
        ip_fingerprint = hashlib.sha256(f"{user_id}:{client_ip}".encode()).hexdigest()[:16]
        
        access_token = self.create_token({
            "user_id": user_id,
            "context": ip_fingerprint  # Bind to client context
        }, "access")
        
        refresh_token = self.create_token({
            "user_id": user_id,
            "context": ip_fingerprint
        }, "refresh")
        

        # Calculate expiration
        expires_at = datetime.utcnow() + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_at=expires_at.isoformat()
        )
    
    def create_token_pair(self, user_id: str) -> TokenPair:
        """Create both access and refresh tokens for a user."""
        # Create tokens
        access_token = self.create_token({"user_id": user_id}, "access")
        refresh_token = self.create_token({"user_id": user_id}, "refresh")
        
        # Calculate expiration
        expires_at = datetime.utcnow() + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_at=expires_at.isoformat()
        )
    
    def verify_token(self, token: str, token_type: str = None) -> Optional[TokenData]:
        """Verify JWT token and return token data if valid."""
        try:
            # First, perform basic validation to avoid unnecessary database lookups
            if not token or not isinstance(token, str) or '.' not in token:
                logger.warning("Invalid token format received")
                return None
            
            # Check if token is blacklisted - this can throw exceptions
            try:
                if self.is_token_blacklisted(token):
                    logger.info("Token is blacklisted")
                    return None
            except Exception as e:
                logger.error(f"Error checking blacklist: {str(e)}")
                # Continue with validation even if blacklist check fails
                

            # Decode the token
            # Enable all security options in token verification
            try:
                # First just get the payload without full verification
                payload = jwt.decode(
                    token, 
                    self.SECRET_KEY, 
                    algorithms=[self.ALGORITHM],
                    options={
                        "verify_signature": True,
                        "verify_exp": False, # changed from True to False Don't check expiration yet
                    #     "verify_nbf": True,
                    #     "verify_iat": True,
                    #     "require": ["exp", "iat", "username", "token_type", "jti"]
                    }
                )
            except PyJWTError as e:
                logger.error(f"Token decode error (no payload): {str(e)}")
                return None

            

            user_id = payload.get("user_id")
            decoded_token_type = payload.get("token_type")
            exp = payload.get("exp")
            
            # Check if required fields exist
            if not user_id or not exp or not decoded_token_type:
                logger.warning(f"Missing required fields in token: {payload.keys()}")
                return None
                
            # Check token type if specified
            if token_type and decoded_token_type != token_type:
                logger.warning(f"Token type mismatch: expected {token_type}, got {decoded_token_type}")
                return None
            
            # Check if token is expired (explicitly check timestamps)
            if exp is None or datetime.fromtimestamp(exp) < datetime.utcnow():
                logger.info(f"Token expired: {datetime.fromtimestamp(exp).isoformat() if exp else 'no exp'}")
                return None

            # Check if we should auto-refresh the token (sliding window)
            if self.token_expiration.should_refresh(payload) and self.token_expiration.sliding_window:
                # We'll handle this in the caller by checking token_data.needs_refresh
                return TokenData(
                    user_id=user_id, 
                    exp=int(exp), 
                    token_type=decoded_token_type,
                    needs_refresh=True
                )
                
            return TokenData(
                user_id=user_id, 
                exp=int(exp), 
                token_type=decoded_token_type,
                needs_refresh=False
            )
            
        except PyJWTError as e:
            logger.error(f"JWT decode error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            return None

    def force_token_rotation(self, user_id: str) -> bool:
        """Force rotation of all user tokens on security events."""
        try:
            # Blacklist all existing tokens
            tokens = self.get_tokens_by_user_id(user_id)
            for token in tokens:
                self.revoke_token(token["access_token"], "security_rotation")
                self.revoke_token(token["refresh_token"], "security_rotation")
                
            # Delete from tokens table
            self._execute_db_query(
                "DELETE FROM user_tokens WHERE user_id = %s",
                (user_id,)
            )
            
            # Log security event
            self._log_security_event(
                user_id=user_id,
                event_type="force_token_rotation",
                severity="high",
                event_data={"reason": "security_event"}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to perform token rotation: {str(e)}")
            return False

    def refresh_access_token(self, refresh_token: str) -> Optional[TokenPair]:
        """Create new token pair using a valid refresh token."""
        # Verify the refresh token
        token_data = self.verify_token(refresh_token, "refresh")
        
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token data",
                headers={"WWW-Authenticate": "Bearer"},
            )
            # return None
        
        # Get the original username from the refresh token
        user_id = token_data.user_id

        # Create a new access token
        new_access_token = self.create_token({"user_id": user_id}, "access")
        
        # Calculate expiration for the new access token
        access_expire = datetime.utcnow() + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        
        # Update the tokens in the database
        self._update_access_token(user_id, refresh_token, new_access_token)
        
        # Create a new TokenPair with the same refresh token
        return TokenPair(
            access_token=new_access_token,
            refresh_token=refresh_token,  # Keep the same refresh token
            token_type="bearer",
            expires_at=access_expire.isoformat()
        )
    
    def _update_access_token(self, user_id: str, refresh_token: str, new_access_token: str):
        """
        Update the access token in the database for a specific user and refresh token.
        
        Args:
            username: The username
            refresh_token: The current refresh token
            new_access_token: The new access token to replace the old one
        """

        # Calculate new access token expiration
        access_expires = datetime.utcnow() + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
            
        # Update the access token while keeping the same refresh token
        self._execute_db_query(
            """
            UPDATE user_tokens 
            SET access_token = %s, access_expires_at = %s, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND refresh_token = %s
            """,
            (new_access_token, access_expires, user_id, refresh_token)
        )

    def revoke_token(self, token: str, reason: str = None) -> bool:
        """Revoke a token by adding it to the blacklist."""
        try:
            # Verify the token first
            token_data = self.verify_token(token)
            if not token_data:
                return False
            
            # Add the token to the blacklist
            expires_at = datetime.fromtimestamp(token_data.exp).isoformat()
            self.blacklist_token(token, token_data.user_id, expires_at, reason)
            
            return True
        except Exception as e:
            logger.error(f"Error revoking token: {str(e)}")
            return False

    def store_token_pair(self, user_id: str, access_token: str, refresh_token: str) -> str:
        """
        Store a token pair in the database.
        
        Args:
            username: The username associated with the tokens
            access_token: The JWT access token
            refresh_token: The JWT refresh token
            
        Returns:
            The token_id of the stored tokens
        """
        token_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        # Calculate expiration times using strategy
        access_expires, refresh_expires = self.token_expiration.get_expiration_times()
        
        self._execute_db_query(
            """
            INSERT INTO user_tokens 
            (token_id, user_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (token_id, user_id, access_token, refresh_token, access_expires, refresh_expires, now, now, True)
        )
        return token_id

    def get_tokens_by_user_id(self, user_id: str):
        """
        Get all active tokens for a user.
        
        Args:
            user_id: The user UUID to get tokens for
            
        Returns:
            A list of token pairs with their expiration info
        """
        now = datetime.utcnow()
        
        results = self._execute_db_query(
            """
            SELECT token_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at
            FROM user_tokens
            WHERE user_id = %s AND refresh_expires_at > %s AND is_active = TRUE
            ORDER BY created_at DESC
            """,
            (user_id, now),
            fetch_all=True
        )
        
        tokens = []
        for row in results:
            tokens.append({
                "token_id": row[0],
                "access_token": row[1],
                "refresh_token": row[2],
                "access_expires_at": row[3].isoformat(),
                "refresh_expires_at": row[4].isoformat(),
                "created_at": row[5].isoformat()
            })
            
        return tokens

    def get_tokens_by_username(self, username: str):
        """
        Get all active tokens for a user.
        
        Args:
            username: The username to get tokens for
            
        Returns:
            A list of token pairs with their expiration info
        """
        now = datetime.utcnow()
        
        results = self._execute_db_query(
            """
            SELECT token_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at
            FROM user_tokens
            WHERE username = %s AND refresh_expires_at > %s
            ORDER BY created_at DESC
            """,
            (username, now),
            fetch_all=True
        )
        
        tokens = []
        for row in results:
            tokens.append({
                "token_id": row[0],
                "access_token": row[1],
                "refresh_token": row[2],
                "access_expires_at": row[3].isoformat(),
                "refresh_expires_at": row[4].isoformat(),
                "created_at": row[5].isoformat()
            })
            
        return tokens

    def invalidate_token(self, access_token: str) -> bool:
        """Invalidate a token by removing it from the tokens table and adding to blacklist."""
        # Verify the token first
        token_data = self.verify_token(access_token)
        if not token_data:
            return False
        
        # Find the token in the database
        result = self._execute_db_query(
            "SELECT refresh_token FROM user_tokens WHERE access_token = %s",
            (access_token,),
            fetch_one=True
        )
        
        if not result:
            return False
            
        refresh_token = result[0]
        
        # # Delete from tokens table
        # self._execute_db_query(
        #     "DELETE FROM user_tokens WHERE access_token = %s",
        #     (access_token,)
        # )
        # Set tokens as inactive
        self._execute_db_query(
            "UPDATE user_tokens SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE access_token = %s",#RETURNING user_id
            (access_token,)
        )
        # Add both tokens to blacklist
        expires_at = datetime.fromtimestamp(token_data.exp).isoformat()
        self.blacklist_token(access_token, token_data.user_id, expires_at, "manual_invalidation")
        
        # Also blacklist the refresh token
        refresh_token_data = self.verify_token(refresh_token, "refresh")
        if refresh_token_data:
            refresh_expires = datetime.fromtimestamp(refresh_token_data.exp).isoformat()
            self.blacklist_token(refresh_token, token_data.user_id, refresh_expires, "manual_invalidation")
        
        return True
    
    def get_all_tokens(self):
        """
        Get all active tokens in the database.
        
        Returns:
            A list of all token pairs with their expiration info and associated usernames
        """
        now = datetime.utcnow()
        
        results = self._execute_db_query(
            """
            SELECT token_id, user_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, updated_at
            FROM user_tokens
            WHERE refresh_expires_at > %s AND is_active = TRUE
            ORDER BY created_at DESC
            """,
            (now,),
            fetch_all=True
        )
        
        tokens = []
        for row in results:
            tokens.append({
                "token_id": row[0],
                "user_id": row[1],
                "access_token": row[2],
                "refresh_token": row[3],
                "access_expires_at": row[4].isoformat(),
                "refresh_expires_at": row[5].isoformat(),
                "created_at": row[6].isoformat(),
                "updated_at": row[7].isoformat()
            })
            
        return tokens

    def get_all_tokens_for_user(self, user_id):
        """
        Get all active tokens for a specific user.
        
        Args:
            user_id (UUID): The user ID to look up
            
        Returns:
            list: List of dictionaries containing token information
        """
        query = """
            SELECT token_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at 
            FROM user_tokens 
            WHERE user_id = %s AND is_active = TRUE
        """
        results = self._execute_db_query(query, (user_id,), fetch_all=True)
        
        if not results:
            return []
        
        tokens = []
        for row in results:
            tokens.append({
                "token_id": row[0],
                "access_token": row[1],
                "refresh_token": row[2],
                "access_expires_at": row[3],
                "refresh_expires_at": row[4],
                "created_at": row[5]
            })
            
        return tokens

    def invalidate_all_user_tokens(self, user_id: str):
        """
        Completely invalidate all tokens for a user.
        
        Args:
            username: The user whose tokens should be invalidated
        """
        try:
            # Get all active tokens for the user
            active_tokens = self.get_tokens_by_user_id(user_id)
            
            # Revoke each token
            for token_info in active_tokens:
                # Blacklist both access and refresh tokens
                self.revoke_token(token_info['access_token'], "user_security_action")
                self.revoke_token(token_info['refresh_token'], "user_security_action")
            
            # # Remove tokens from the database
            # self._execute_db_query(
            #     "DELETE FROM user_tokens WHERE user_id = %s",
            #     (user_id,)
            # )
            
            # Set all tokens as inactive
            self._execute_db_query(
                "UPDATE user_tokens SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE user_id = %s",
                (user_id,)
            )
            
            # Log the security event
            self._log_security_event(
                user_id=user_id,
                event_type="all_tokens_invalidated",
                severity="medium",
                event_data={"token_count": len(active_tokens)}
            )
        
        except Exception as e:
            logger.error(f"Failed to invalidate tokens for {user_id}: {str(e)}")

    def get_user_id_by_token(self, access_token):
        """
        Get user_id associated with a given access token.
        
        Args:
            access_token (str): The access token to look up
            
        Returns:
            UUID: The user_id if found, None otherwise
        """
        query = """
            SELECT user_id 
            FROM user_tokens 
            WHERE access_token = %s AND is_active = TRUE AND access_expires_at > CURRENT_TIMESTAMP
        """
        result = self._execute_db_query(query, (access_token,), fetch_one=True)
        return result[0] if result else None

    def get_user_id_by_refresh_token(self, refresh_token):
        """
        Get user_id associated with a given refresh token.
        
        Args:
            refresh_token (str): The refresh token to look up
            
        Returns:
            UUID: The user_id if found, None otherwise
        """
        query = """
            SELECT user_id 
            FROM user_tokens 
            WHERE refresh_token = %s AND is_active = TRUE AND refresh_expires_at > CURRENT_TIMESTAMP
        """
        result = self._execute_db_query(query, (refresh_token,), fetch_one=True)
        return result[0] if result else None

    async def get_token_from_header(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
        """
        Extract token from the Authorization header.
        
        Args:
            credentials: The security credentials containing the token
            
        Returns:
            The extracted token
            
        Raises:
            HTTPException: If the Authorization header is missing or malformed
        """
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing authorization credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = credentials.credentials
        
        # Check if token is blacklisted
        try:
            if self.is_token_blacklisted(token):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token has been revoked",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error checking token blacklist: {str(e)}",
            )
        
        return token
    
    # For websocket endpoints, use a different approach
    async def get_token_from_websocket(self, websocket: WebSocket) -> Optional[str]:
        """Get token from WebSocket query parameters or headers"""
        # Try to get from query params
        token = websocket.query_params.get("token")
        
        # If not in query params, try to get from headers
        if not token:
            auth_header = websocket.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.replace("Bearer ", "")
        
        return token

    # Add this to your SessionManager class
    async def get_token_from_websocket_header(self, websocket: WebSocket) -> Optional[str]:
        """
        Extract token from the Authorization header in WebSocket connection.
        
        Args:
            websocket: The WebSocket connection
            
        Returns:
            The extracted token or None if not found
        """
        logging.info(f"WebSocket connection attempt - Auth header present: {'Authorization' in websocket.headers}")
        # Get authorization header
        auth_header = websocket.headers.get("authorization")
        
        # Extract token if header exists and has correct format
        if auth_header and auth_header.startswith("Bearer "):
            return auth_header.replace("Bearer ", "")
        
        return None

    async def get_current_user(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
        """
        Get the current authenticated user from the token.
        
        Args:
            credentials: The security credentials containing the token
                
        Returns:
            The username from the token
                
        Raises:
            HTTPException: If the token is invalid or expired
        """
        token = credentials.credentials
        
        # Check if token is blacklisted
        try:
            if self.is_token_blacklisted(token):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token has been revoked",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error checking token blacklist: {str(e)}",
            )
        
        token_data = self.verify_token(token, "access")
        
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        query = """
            SELECT username
            FROM users 
            WHERE user_id = %s
        """
        username = self._execute_db_query(query, (token_data.user_id,), fetch_one=True)
        
        return username[0]

    def clean_expired_tokens(self) -> int:
        """
        Remove expired tokens from the tokens table.
        
        Returns:
            The number of expired tokens that were deleted.
        """
        now = datetime.utcnow()
        
        return self._execute_db_query(
            "DELETE FROM user_tokens WHERE refresh_expires_at < %s",
            (now,),
            return_rowcount=True
        )
    
    def clean_expired_blacklist(self) -> int:
        """Remove expired tokens from the blacklist."""
        now = datetime.utcnow()
        return self._execute_db_query(
            "DELETE FROM token_blacklist WHERE expires_at < %s",
            (now,),
            return_rowcount=True
        )

    def log_action(self, content, user_id=None, action_type=None, ip_address=None, user_agent=None, status="success"):
        """
        Log an action in the system.
        
        Args:
            content: Description of the action
            username: The username associated with the action (optional)
            action_type: Type of action (e.g., 'login', 'token_refresh', 'api_call')
            ip_address: IP address of the user (optional)
            user_agent: User agent of the request (optional)
            status: Outcome of the action ('success', 'failure', 'error')
        
        Returns:
            The log_id of the created log entry
        """
        log_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        self._execute_db_query(
            """
            INSERT INTO logs 
            (log_id, created_at, user_id, action_type, status, ip_address, user_agent, content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (log_id, now, user_id, action_type, status, ip_address, user_agent, content)
        )
        
        # Also log to the application log file
        log_message = f"ACTION: {action_type or 'unknown'} | USER: {user_id or 'anonymous'} | STATUS: {status} | {content}"
        if status == "success":
            logger.info(log_message)
        elif status == "failure":
            logger.warning(log_message)
        else:
            logger.error(log_message)
            
        return log_id
    
    def get_logs(self, filters=None, date_filter=None, limit=100, offset=0, sort_by="created_at", sort_direction="desc"):
        """
        Retrieve logs with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria (e.g., {"username": "john", "action_type": "login"})
            date_filter: Dictionary with start_date and/or end_date for time-based filtering
            limit: Maximum number of logs to return
            offset: Pagination offset
            sort_by: Field to sort by
            sort_direction: Sort direction ("asc" or "desc")
            
        Returns:
            List of log entries
        """
        # Build the base query - use the log_id, created_at primary key for partitioned table
        query = """
            SELECT log_id, created_at, user_id, action_type, status, ip_address, user_agent, content
            FROM logs
        """
        
        params = []
        where_clauses = []
        
        # Build WHERE clause if filters provided
        if filters:
            for key, value in filters.items():
                if key in ["user_id", "action_type", "status"]:
                    where_clauses.append(f"{key} = %s")
                    params.append(value)
        
        # Add date filtering
        if date_filter:
            if "start_date" in date_filter:
                where_clauses.append("created_at >= %s")
                params.append(date_filter["start_date"])
            if "end_date" in date_filter:
                where_clauses.append("created_at <= %s")
                params.append(date_filter["end_date"])
        
        # Add WHERE clause to query if we have any conditions
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Validate and add ORDER BY
        valid_sort_fields = ["created_at", "user_id", "action_type", "status"]
        valid_directions = ["asc", "desc"]
        
        if sort_by not in valid_sort_fields:
            sort_by = "created_at"  # Default to created_at if invalid
        
        if sort_direction.lower() not in valid_directions:
            sort_direction = "desc"  # Default to descending if invalid
        
        query += f" ORDER BY {sort_by} {sort_direction.upper()}"
        
        # Add limit and offset
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        results = self._execute_db_query(query, params, fetch_all=True)
        
        logs = []
        for row in results:
            logs.append({
                "log_id": row[0],
                "created_at": row[1].isoformat(),
                "user_id": row[2],
                "action_type": row[3],
                "status": row[4],
                "ip_address": row[5],
                "user_agent": row[6],
                "content": row[7]
            })
            
        return logs

    def _log_security_event(self, user_id, event_type, severity, event_data):
        """Log security events to the database"""
        event_id = str(uuid.uuid4())
        created_at = datetime.utcnow()#datetime.now(timezone.utc)#
        
        try:
            self._execute_db_query(
                """
                INSERT INTO security_events 
                (event_id, created_at, user_id, event_type, severity, ip_address, event_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    created_at,
                    user_id,
                    event_type,
                    severity,
                    None,  # IP address would be passed from request in actual implementation
                    json.dumps(event_data)
                )
            )
            logger.info(f"Security event logged: {event_type}")
        except Exception as e:
            logger.error(f"Failed to log security event: {e}")

    def run_cleanup_maintenance(self):
        """Run database maintenance tasks to clean up expired data"""
        try:
            self._execute_db_query("SELECT cleanup_expired_data()")
            logger.info("Database maintenance cleanup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Database maintenance cleanup failed: {str(e)}")
            return False

    ##################################################
    def get_user_active_refresh_tokens(self, user_id):
        """Get all active refresh tokens for a user."""
        try:
            # Query database for all active refresh tokens for this user
            # This is a placeholder - implement according to your database structure
            query = "SELECT refresh_token FROM user_sessions WHERE user_id = ? AND is_active = TRUE"
            result = self.db.execute_query(query, (user_id,))
            return [row['refresh_token'] for row in result]
        except Exception as e:
            logging.error(f"Error getting user refresh tokens: {e}")
            return []
    
    def revoke_all_tokens_for_user(self, user_id):
        """Revoke all tokens for a specific user."""
        try:
            # Update all tokens for this user to inactive
            # This is a placeholder - implement according to your database structure
            query = "UPDATE user_sessions SET is_active = FALSE WHERE user_id = ?"
            self.db.execute_query(query, (user_id,))
            return True
        except Exception as e:
            logging.error(f"Error revoking all user tokens: {e}")
            return False
    
    def decode_token_without_verification(self, token):
        """Decode token payload without verifying signature."""
        try:
            # This is potentially unsafe and should only be used
            # when you need to extract the user_id to look up the token
            # in your database before performing full verification
            payload = jwt.decode(token, options={"verify_signature": False})
            return payload
        except Exception as e:
            logging.error(f"Error decoding token: {e}")
            return None
    
    def verify_refresh_token_in_database(self, user_id, refresh_token):
        """Verify a refresh token exists in the database for this user."""
        try:
            # Check if this refresh token exists and is active for this user
            # This is a placeholder - implement according to your database structure
            query = """
                SELECT id FROM user_sessions 
                WHERE user_id = ? AND refresh_token = ? AND is_active = TRUE
            """
            result = self.db.execute_query(query, (user_id, refresh_token))
            return len(result) > 0
        except Exception as e:
            logging.error(f"Error verifying refresh token in DB: {e}")
            return False
    
    def verify_device_fingerprint(self, user_id, refresh_token, device_fingerprint, user_agent):
        """Verify the device fingerprint matches what was stored with the token."""
        try:
            # Get the stored device info for this token
            # This is a placeholder - implement according to your database structure
            query = """
                SELECT device_fingerprint, user_agent FROM user_sessions 
                WHERE user_id = ? AND refresh_token = ? AND is_active = TRUE
            """
            result = self.db.execute_query(query, (user_id, refresh_token))
            
            if not result:
                return False
                
            stored_fingerprint = result[0]['device_fingerprint']
            stored_user_agent = result[0]['user_agent']
            
            # Check if current device info matches stored info
            # You can implement fuzzy matching for user agents if needed
            fingerprint_match = secrets.compare_digest(stored_fingerprint, device_fingerprint)
            user_agent_match = user_agent == stored_user_agent
            
            return fingerprint_match and user_agent_match
        except Exception as e:
            logging.error(f"Error verifying device fingerprint: {e}")
            return False
    
    def detect_suspicious_activity(self, user_id, client_ip):
        """Detect suspicious activity based on IP and recent access patterns."""
        try:
            # Implement suspicious activity detection logic
            # Examples:
            # 1. Check if IP is from unusual location compared to previous logins
            # 2. Check if there have been many token refresh attempts in short time
            # 3. Check if this IP has been flagged for suspicious activity
            
            # This is a placeholder implementation
            recent_refresh_attempts = self.get_recent_refresh_attempts(user_id)
            if recent_refresh_attempts > 10:  # More than 10 attempts in short time
                return True
                
            is_unusual_location = self.check_unusual_location(user_id, client_ip)
            if is_unusual_location:
                return True
                
            return False
        except Exception as e:
            logging.error(f"Error detecting suspicious activity: {e}")
            # Default to not suspicious on error
            return False
            
    def get_recent_refresh_attempts(self, user_id):
        """Get count of recent refresh attempts for this user."""
        # Placeholder implementation
        try:
            query = """
                SELECT COUNT(*) as count FROM auth_logs
                WHERE user_id = ? AND action_type = 'TokenRefresh'
                AND timestamp > datetime('now', '-15 minutes')
            """
            result = self.db.execute_query(query, (user_id,))
            return result[0]['count'] if result else 0
        except Exception as e:
            logging.error(f"Error getting refresh attempts: {e}")
            return 0
            
    def check_unusual_location(self, user_id, client_ip):
        """Check if this IP is from an unusual location for this user."""
        # Placeholder implementation
        # In a real implementation, you would:
        # 1. Store IP geolocations with login attempts
        # 2. Compare current IP geolocation with previously seen locations
        # 3. Flag large distances/different countries as suspicious
        return False
    
    ##################################################
