# session_manager.py
from fastapi import Request, Form, Depends, WebSocket, WebSocketDisconnect, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import time 
from typing import Dict, Optional, List, Union
from config import config
from database import DatabaseManager 
import uuid 
from uuid import UUID, uuid4
import secrets 
from datetime import datetime, timedelta, timezone 
from zoneinfo import ZoneInfo
from schemas_models import TokenPair, TokenData
import jwt
from jwt import DecodeError, PyJWTError
import logging
import hashlib
# import json 
from token_expiration import TokenExpirationStrategy
from user_manager import UserManager 

logger = logging.getLogger(__name__)
security = HTTPBearer()

class SessionManager:
    ACCESS_TOKEN_EXPIRE_MINUTES = config.get("ACCESS_TOKEN_EXPIRE_MINUTES", 30)
    REFRESH_TOKEN_EXPIRE_DAYS = config.get("REFRESH_TOKEN_EXPIRE_DAYS", 7)
    SECRET_KEY = config.get("SECRET_KEY")
    ALGORITHM = config.get("ALGORITHM", "HS256")

    def __init__(self):
        self.db_manager = DatabaseManager() 
        self.user_manager = UserManager()
        self.token_expiration = TokenExpirationStrategy(
            access_token_expire_minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES,
            refresh_token_expire_days=self.REFRESH_TOKEN_EXPIRE_DAYS,
            sliding_window=True,
            minimum_remaining_time_percent=0.2
        )
        if not self.SECRET_KEY:
            logger.critical("SECRET_KEY is not set in config. Application will not function correctly.")
            raise ValueError("SECRET_KEY is not configured.")

    async def is_token_blacklisted(self, token: str) -> bool:
        """Check if a token is blacklisted."""
        try:
            result = await self.db_manager.execute_query(
                "SELECT blacklist_id FROM token_blacklist WHERE token = $1 AND expires_at > CURRENT_TIMESTAMP",
                (token,),
                fetch_one=True
            )
            return result is not None
        except Exception as e:
            logger.error(f"Error checking blacklisted token: {e}", exc_info=True)
            return True # Fail-safe: assume blacklisted if error occurs

    async def blacklist_token(self, token: str, user_id: Union[str, UUID], expires_at_iso: Union[str, datetime], reason: Optional[str] = None) -> str:
        """Add a token to the blacklist."""
        blacklist_id = uuid4()
        blacklisted_at = datetime.now(ZoneInfo("Africa/Cairo"))
        
        try:
            if isinstance(expires_at_iso, str):
                expires_at_dt = datetime.fromisoformat(expires_at_iso.replace('Z', '+00:00'))
            elif isinstance(expires_at_iso, datetime):
                expires_at_dt = expires_at_iso
            else:
                raise ValueError("expires_at must be an ISO string or datetime object")

            if expires_at_dt.tzinfo is None: # Ensure timezone-aware (UTC)
                expires_at_dt = expires_at_dt.replace(tzinfo=timezone.utc)

            await self.db_manager.execute_query(
                """
                INSERT INTO token_blacklist (blacklist_id, user_id, token, expires_at, blacklisted_at, reason) 
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                (blacklist_id, UUID(str(user_id)), token, expires_at_dt, blacklisted_at, reason)
            )
            return str(blacklist_id)
        except ValueError as ve:
            logger.error(f"Invalid expires_at format for blacklist_token: {ve}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid token expiration format.")
        except Exception as e:
            logger.error(f"Error blacklisting token for user {user_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not blacklist token.")
    
    def create_token(self, data: dict, token_type: str) -> str: # This remains sync, ensures str output
        """Create a JWT token with specified expiration."""
        to_encode = data.copy()
        for key, value in to_encode.items():
            if isinstance(value, UUID):
                to_encode[key] = str(value)

        jti = str(uuid4())
        if token_type == "access":
            expires_delta = timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        elif token_type == "refresh":
            expires_delta = timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
        else:
            raise ValueError("Invalid token_type specified")

        expire = datetime.now(ZoneInfo("Africa/Cairo")) + expires_delta
        to_encode.update({
            "exp": expire, 
            "token_type": token_type, 
            "iat": datetime.now(ZoneInfo("Africa/Cairo")),
            "jti": jti
        })
        
        encoded_jwt = jwt.encode(to_encode, self.SECRET_KEY, algorithm=self.ALGORITHM)
        if isinstance(encoded_jwt, bytes): # Ensure string output like original sync version
            encoded_jwt = encoded_jwt.decode('utf-8')
        return encoded_jwt

    # Added from original sync version, remains sync
    def create_token_with_context(self, user_id: str, client_ip: str) -> TokenPair:
        """Create tokens bound to specific IP address."""
        ip_fingerprint = hashlib.sha256(f"{user_id}:{client_ip}".encode()).hexdigest()[:16]
        
        access_token = self.create_token({
            "user_id": user_id,
            "context": ip_fingerprint
        }, "access") # create_token now returns str
        
        refresh_token = self.create_token({
            "user_id": user_id,
            "context": ip_fingerprint
        }, "refresh") # create_token now returns str
        
        access_token_payload = jwt.decode(access_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})
        expires_at_timestamp = access_token_payload["exp"]
        expires_at_dt = datetime.fromtimestamp(expires_at_timestamp, tz=timezone.utc)

        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_at=expires_at_dt.isoformat()
        )

    def create_token_pair(self, user_id: Union[str, UUID], workspace_id: Optional[Union[str, UUID]] = None) -> TokenPair: # Sync
        """Create both access and refresh tokens for a user."""
        user_id_str = str(user_id)
        workspace_id_str = str(workspace_id) if workspace_id else None

        token_data = {"user_id": user_id_str}
        if workspace_id_str:
            token_data["workspace_id"] = workspace_id_str
            
        access_token = self.create_token(token_data, "access") # Receives str
        refresh_token = self.create_token(token_data, "refresh") # Receives str
        
        try:
            # jwt.decode can handle str token
            access_token_payload = jwt.decode(access_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})
            expires_at_timestamp = access_token_payload["exp"]
            expires_at_dt = datetime.fromtimestamp(expires_at_timestamp, tz=timezone.utc)
        except PyJWTError as e: 
            logger.error(f"Error decoding newly created access token to get exp: {e}")
            expires_at_dt = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)

        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_at=expires_at_dt.isoformat()
        )
    
    async def verify_token(self, token: str, expected_token_type: Optional[str] = None) -> Optional[TokenData]:
        """Verify JWT token and return token data if valid."""
        try:
            if not token or not isinstance(token, str) or '.' not in token:
                logger.warning("Invalid token format received")
                return None
            
            if await self.is_token_blacklisted(token):
                unverified_jti = "N/A"
                try:
                    unverified_payload = self.decode_token_without_verification(token) # Sync call
                    unverified_jti = unverified_payload.get('jti', 'N/A') if unverified_payload else 'N/A'
                except PyJWTError: # Should be caught by decode_token_without_verification
                    pass 
                logger.info(f"Token verification failed: Token is blacklisted. JTI: {unverified_jti}")
                return None
            
            payload = jwt.decode(token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": True})
            user_id = payload.get("user_id")
            workspace_id = payload.get("workspace_id")
            actual_token_type  = payload.get("token_type")
            exp_timestamp  = payload.get("exp")
            
            if not all([user_id, actual_token_type, exp_timestamp is not None]):
                logger.warning(f"Token verification failed: Missing required fields. Payload keys: {list(payload.keys())}")
                return None
                
            if expected_token_type and actual_token_type  != expected_token_type:
                logger.warning(f"Token type mismatch: expected {expected_token_type}, got {actual_token_type }")
                return None
            
            needs_refresh = False
            if self.token_expiration.sliding_window and self.token_expiration.should_refresh(payload):
                needs_refresh = True

            return TokenData(
                user_id=str(user_id), # Ensure string representation
                workspace_id=str(workspace_id) if workspace_id else None, 
                exp=int(exp_timestamp), 
                token_type=actual_token_type,
                needs_refresh=needs_refresh
            )
        except jwt.ExpiredSignatureError:
            logger.info("Token verification failed: Expired signature.")
            return None
        except DecodeError as e:
            logger.error(f"Token verification failed: JWT decode error - {str(e)}")
            return None
        except PyJWTError as e:
            logger.error(f"Token verification failed: PyJWTError - {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during token verification: {str(e)}", exc_info=True)
            return None

    async def force_token_rotation(self, user_id: Union[str, UUID]) -> bool:
        """Force rotation of all user tokens on security events."""
        user_id_str = str(user_id)
        user_id_uuid = UUID(user_id_str)
        try:
            active_db_tokens = await self.db_manager.execute_query(
                "SELECT access_token, refresh_token FROM user_tokens WHERE user_id = $1 AND is_active = TRUE",
                (user_id_uuid,),
                fetch_all=True
            )
            
            for db_token_pair in active_db_tokens or []: # Handle case where active_db_tokens might be None
                access_token_data = await self.verify_token(db_token_pair['access_token'])
                if access_token_data:
                    await self.blacklist_token(db_token_pair['access_token'], user_id_uuid, datetime.fromtimestamp(access_token_data.exp, tz=timezone.utc), "force_token_rotation")
                
                refresh_token_data = await self.verify_token(db_token_pair['refresh_token'])
                if refresh_token_data:
                     await self.blacklist_token(db_token_pair['refresh_token'], user_id_uuid, datetime.fromtimestamp(refresh_token_data.exp, tz=timezone.utc), "force_token_rotation")

            await self.db_manager.execute_query(
                "UPDATE user_tokens SET is_active = FALSE, updated_at = $1 WHERE user_id = $2",
                (datetime.now(ZoneInfo("Africa/Cairo")), user_id_uuid)
            )
            
            # Ensure user_manager is initialized if this can be called early
            if hasattr(self, 'user_manager') and self.user_manager:
                await self.user_manager._log_security_event(
                    user_id=user_id_uuid,
                    event_type="force_token_rotation",
                    severity="high",
                    event_data={"reason": "security_event"}
                )
            return True
        except Exception as e:
            logger.error(f"Failed to perform token rotation for user {user_id_str}: {e}", exc_info=True)
            return False

    async def refresh_access_token(self, refresh_token: str) -> Optional[TokenPair]:
        """Create new access token using a valid refresh token. Keeps the same refresh token."""
        token_data = await self.verify_token(refresh_token, "refresh")
        
        if not token_data:
            unverified_payload = self.decode_token_without_verification(refresh_token) # Sync call
            attempted_user_id = unverified_payload.get("user_id") if unverified_payload else "unknown"
            logger.warning(f"Refresh token failed verification for attempted user: {attempted_user_id}. Token: {refresh_token[:20]}...")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        user_id_str = token_data.user_id
        workspace_id_str = token_data.workspace_id

        new_access_token = self.create_token( # create_token now returns str
            {"user_id": user_id_str, "workspace_id": workspace_id_str},
            "access"
        )
        
        # jwt.decode can handle str token
        access_token_payload = jwt.decode(new_access_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})
        access_expires_at_timestamp = access_token_payload["exp"]
        access_expires_dt = datetime.fromtimestamp(access_expires_at_timestamp, tz=timezone.utc)
        
        await self.db_manager.execute_query(
            """
            UPDATE user_tokens 
            SET access_token = $1, access_expires_at = $2, updated_at = $3
            WHERE user_id = $4 AND refresh_token = $5 AND is_active = TRUE 
            """,
            (new_access_token, access_expires_dt, datetime.now(ZoneInfo("Africa/Cairo")), UUID(user_id_str), refresh_token)
        )
        
        return TokenPair(
            access_token=new_access_token,
            refresh_token=refresh_token, 
            token_type="bearer",
            expires_at=access_expires_dt.isoformat()
        )
    
    # Added from original sync version, made async
    async def _update_access_token(self, user_id: str, refresh_token: str, new_access_token: str):
        """Helper to update access token in DB. Typically used by refresh_access_token."""
        access_token_payload = jwt.decode(new_access_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})
        access_expires_at_timestamp = access_token_payload["exp"]
        access_expires_dt = datetime.fromtimestamp(access_expires_at_timestamp, tz=timezone.utc)
            
        await self.db_manager.execute_query(
            """
            UPDATE user_tokens 
            SET access_token = $1, access_expires_at = $2, updated_at = $3
            WHERE user_id = $4 AND refresh_token = $5 AND is_active = TRUE
            """,
            (new_access_token, access_expires_dt, datetime.now(ZoneInfo("Africa/Cairo")), UUID(str(user_id)), refresh_token)
        )

    async def revoke_token(self, token: str, reason: Optional[str] = None) -> bool:
        """Revoke a token by adding it to the blacklist."""
        try:
            token_data = await self.verify_token(token)
            if not token_data: # Token is invalid, expired, or already blacklisted (verify_token checks blacklist)
                logger.info(f"Revoke_token: Token already invalid or blacklisted. Token: {token[:20]}...")
                # Attempt to blacklist it anyway if we can get payload details, similar to original logic but async
                try:
                    payload = self.decode_token_without_verification(token) # Sync call
                    if payload and payload.get("exp") and payload.get("user_id"):
                        # Ensure exp is a number (timestamp)
                        exp_val = payload["exp"]
                        if not isinstance(exp_val, (int, float)):
                            logger.warning(f"Revoke_token: Decoded 'exp' is not a timestamp: {exp_val}. Cannot blacklist.")
                            return False # Cannot proceed if 'exp' is bad
                        exp_dt = datetime.fromtimestamp(exp_val, tz=timezone.utc)
                        user_id_for_blacklist = payload["user_id"]
                        await self.blacklist_token(token, user_id_for_blacklist, exp_dt, reason or "revoked_after_initial_fail")
                        return True # Successfully blacklisted after initial fail
                    else:
                        logger.warning(f"Revoke_token: Could not decode token for user_id/exp after initial fail. Token: {token[:20]}...")
                except Exception as e_decode:
                    logger.error(f"Revoke_token: Error during decode/blacklist attempt after initial fail: {e_decode}", exc_info=True)
                return False # Original verify_token failed, and secondary attempt also failed or was not possible

            # If token_data is valid from verify_token:
            expires_at_dt_for_blacklist = datetime.fromtimestamp(token_data.exp, tz=timezone.utc)
            await self.blacklist_token(token, token_data.user_id, expires_at_dt_for_blacklist, reason)
            return True
        except Exception as e:
            logger.error(f"Error revoking token: {e}", exc_info=True)
            return False

    async def store_token_pair(self, user_id: Union[str, UUID], access_token: str, refresh_token: str, workspace_id: Optional[Union[str, UUID]] = None) -> str:
        """Store a token pair in the database."""
        token_id = uuid4()
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        
        access_token_payload = jwt.decode(access_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})
        refresh_token_payload = jwt.decode(refresh_token, self.SECRET_KEY, algorithms=[self.ALGORITHM], options={"verify_exp": False})

        access_expires = datetime.fromtimestamp(access_token_payload["exp"], tz=timezone.utc)
        refresh_expires = datetime.fromtimestamp(refresh_token_payload["exp"], tz=timezone.utc)
        
        db_user_id = UUID(str(user_id))
        db_workspace_id = UUID(str(workspace_id)) if workspace_id else None

        query = """
            INSERT INTO user_tokens 
            (token_id, user_id, workspace_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, updated_at, is_active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """
        await self.db_manager.execute_query(query, (
            token_id, db_user_id, db_workspace_id, access_token, refresh_token, 
            access_expires, refresh_expires, now, now, True
        ))
        return str(token_id)
  
    async def get_tokens_by_user_id_fixed(self, user_id_str: str) -> List[Dict]: # Name kept from async template
        """
        Get all active tokens for a user by user_id (string).
        Corresponds to original's get_tokens_by_user_id_fixed.
        """
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        user_id_uuid = UUID(user_id_str)
        
        results = await self.db_manager.execute_query(
            """
            SELECT token_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, workspace_id
            FROM user_tokens
            WHERE user_id = $1 AND refresh_expires_at > $2 AND is_active = TRUE
            ORDER BY created_at DESC
            """,
            (user_id_uuid, now_utc),
            fetch_all=True
        )
        
        tokens = []
        for row in results or []: # Handle case where results might be None
            tokens.append({
                "token_id": str(row["token_id"]),
                "access_token": row["access_token"],
                "refresh_token": row["refresh_token"],
                "access_expires_at": row["access_expires_at"].isoformat(),
                "refresh_expires_at": row["refresh_expires_at"].isoformat(),
                "created_at": row["created_at"].isoformat(),
                "workspace_id": str(row["workspace_id"]) if row["workspace_id"] else None
            })
        return tokens

    async def get_tokens_by_username(self, username: str) -> List[Dict]:
        """
        Get all active tokens for a user by username.
        """
        user = await self.user_manager.get_user_by_username(username)
        if not user or not user.get("user_id"):
            logger.warning(f"User not found for username: {username} in get_tokens_by_username")
            return []
        
        user_id_str = str(user["user_id"])
        return await self.get_tokens_by_user_id_fixed(user_id_str)

    async def invalidate_token(self, access_token: str) -> bool:
        """Invalidate a token pair by setting is_active=FALSE and blacklisting both."""
        token_data = await self.verify_token(access_token, "access")
        if not token_data:
            logger.warning(f"Invalidate_token: Access token is invalid or expired. Cannot proceed. Token: {access_token[:20]}...")
            return False # If token is already invalid, no further action can be based on its content.
        
        user_id_str = token_data.user_id
        user_id_uuid = UUID(user_id_str)
        access_exp_dt = datetime.fromtimestamp(token_data.exp, tz=timezone.utc)

        # Find the corresponding refresh token from the database
        token_pair_info = await self.db_manager.execute_query(
            "SELECT refresh_token FROM user_tokens WHERE access_token = $1 AND user_id = $2 AND is_active = TRUE",
            (access_token, user_id_uuid),
            fetch_one=True
        )
        
        # Mark the token pair as inactive in the database
        updated_rows = await self.db_manager.execute_query(
            "UPDATE user_tokens SET is_active = FALSE, updated_at = $1 WHERE access_token = $2 AND user_id = $3",
            (datetime.now(ZoneInfo("Africa/Cairo")), access_token, user_id_uuid),
            return_rowcount=True
        )

        # Blacklist the access token
        # This happens regardless of DB update success, as long as original token was verifiable
        await self.blacklist_token(access_token, user_id_str, access_exp_dt, "manual_invalidation")

        if updated_rows is None or updated_rows == 0:
            logger.warning(f"Invalidate_token: No active token pair found in DB for access_token {access_token[:20]}... (user: {user_id_str}). Access token blacklisted. Paired refresh token cannot be processed.")
            return False # Indicate that DB update for pair failed or was not needed.

        # If refresh token was found, verify and blacklist it too
        if token_pair_info and token_pair_info.get("refresh_token"):
            refresh_token = token_pair_info["refresh_token"]
            refresh_token_data = await self.verify_token(refresh_token, "refresh")
            if refresh_token_data:
                refresh_exp_dt = datetime.fromtimestamp(refresh_token_data.exp, tz=timezone.utc)
                await self.blacklist_token(refresh_token, user_id_str, refresh_exp_dt, "manual_invalidation_paired_refresh")
            else:
                logger.warning(f"Invalidate_token: Paired refresh token for access token {access_token[:20]}... was invalid or expired. Attempting to blacklist based on decoded data.")
                try:
                    rt_payload = self.decode_token_without_verification(refresh_token) # Sync call
                    if rt_payload and rt_payload.get("exp") and rt_payload.get("user_id"):
                        exp_val_rt = rt_payload["exp"]
                        if not isinstance(exp_val_rt, (int, float)):
                             logger.warning(f"Invalidate_token: Decoded 'exp' for RT is not a timestamp: {exp_val_rt}.")
                        else:
                            rt_exp_dt = datetime.fromtimestamp(exp_val_rt, tz=timezone.utc)
                            await self.blacklist_token(refresh_token, rt_payload["user_id"], rt_exp_dt, "manual_invalidation_paired_refresh_after_fail")
                except Exception as e_rt_decode:
                     logger.error(f"Invalidate_token: Error blacklisting paired RT after fail: {e_rt_decode}", exc_info=True)
        else:
            logger.warning(f"Invalidate_token: No paired refresh token found in DB for access token {access_token[:20]}... while invalidating.")

        return True # Successfully invalidated the token row and blacklisted access token
    
    async def get_all_tokens(self) -> List[Dict]:
        """
        Get all active tokens in the database.
        """
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        results = await self.db_manager.execute_query(
            """
            SELECT token_id, user_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, updated_at, workspace_id
            FROM user_tokens
            WHERE refresh_expires_at > $1 AND is_active = TRUE
            ORDER BY created_at DESC
            """,
            (now_utc,),
            fetch_all=True
        )
        
        tokens = []
        for row in results or []: # Handle case where results might be None
            tokens.append({
                "token_id": str(row["token_id"]),
                "user_id": str(row["user_id"]),
                "access_token": row["access_token"],
                "refresh_token": row["refresh_token"],
                "access_expires_at": row["access_expires_at"].isoformat(),
                "refresh_expires_at": row["refresh_expires_at"].isoformat(),
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "workspace_id": str(row["workspace_id"]) if row["workspace_id"] else None
            })
        return tokens
    
    async def get_all_tokens_for_user_async(self, user_id_str: str) -> List[Dict]: # Corresponds to original's get_all_tokens_for_user_async
        """ Get all active tokens for a specific user. """
        user_id_uuid = UUID(user_id_str)
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        query = """
            SELECT token_id, access_token, refresh_token, access_expires_at, refresh_expires_at, created_at, workspace_id
            FROM user_tokens 
            WHERE user_id = $1 AND is_active = TRUE AND refresh_expires_at > $2 
        """
        results = await self.db_manager.execute_query(query, (user_id_uuid, now_utc), fetch_all=True)
        
        tokens = []
        for row in results or []: # Handle case where results might be None
            tokens.append({
                "token_id": str(row["token_id"]),
                "access_token": row["access_token"],
                "refresh_token": row["refresh_token"],
                "access_expires_at": row["access_expires_at"].isoformat(),
                "refresh_expires_at": row["refresh_expires_at"].isoformat(),
                "created_at": row["created_at"].isoformat(),
                "workspace_id": str(row["workspace_id"]) if row["workspace_id"] else None
            })
        return tokens
    
    async def invalidate_all_user_tokens(self, user_id_str: str): # Parameter name matches original's intent
        """Completely invalidate all tokens for a user."""
        user_id_uuid = UUID(user_id_str)
        try:
            active_tokens = await self.get_all_tokens_for_user_async(user_id_str)
            
            for token_info in active_tokens:
                # Blacklist access token
                access_token_data = await self.verify_token(token_info['access_token'])
                if access_token_data:
                    await self.blacklist_token(token_info['access_token'], user_id_uuid, datetime.fromtimestamp(access_token_data.exp, tz=timezone.utc), "user_security_action_all")
                
                # Blacklist refresh token
                refresh_token_data = await self.verify_token(token_info['refresh_token'])
                if refresh_token_data:
                    await self.blacklist_token(token_info['refresh_token'], user_id_uuid, datetime.fromtimestamp(refresh_token_data.exp, tz=timezone.utc), "user_security_action_all_paired")

            await self.db_manager.execute_query(
                "UPDATE user_tokens SET is_active = FALSE, updated_at = $1 WHERE user_id = $2",
                (datetime.now(ZoneInfo("Africa/Cairo")), user_id_uuid)
            )
            
            if hasattr(self, 'user_manager') and self.user_manager:
                await self.user_manager._log_security_event(
                    user_id=user_id_uuid,
                    event_type="all_tokens_invalidated",
                    severity="medium",
                    event_data={"token_count": len(active_tokens)}
                )
        except Exception as e:
            logger.error(f"Failed to invalidate tokens for user {user_id_str}: {e}", exc_info=True)

    async def get_user_id_by_token(self, access_token: str) -> Optional[str]:
        """Get user_id associated with a given ACTIVE access token from DB."""
        query = """
            SELECT user_id FROM user_tokens 
            WHERE access_token = $1 AND is_active = TRUE AND access_expires_at > $2
        """
        result = await self.db_manager.execute_query(query, (access_token, datetime.now(ZoneInfo("Africa/Cairo"))), fetch_one=True)
        return str(result["user_id"]) if result and result.get("user_id") else None

    async def get_user_id_by_refresh_token(self, refresh_token: str) -> Optional[str]:
        """Get user_id associated with a given ACTIVE refresh token from DB."""
        query = """
            SELECT user_id FROM user_tokens 
            WHERE refresh_token = $1 AND is_active = TRUE AND refresh_expires_at > $2
        """
        result = await self.db_manager.execute_query(query, (refresh_token, datetime.now(ZoneInfo("Africa/Cairo"))), fetch_one=True)
        return str(result["user_id"]) if result and result.get("user_id") else None

    async def get_token_from_websocket(self, websocket: WebSocket) -> Optional[str]: # No DB access, logic is sync
        token = websocket.query_params.get("token")
        if not token:
            auth_header = websocket.headers.get("authorization")
            if auth_header and auth_header.lower().startswith("bearer "):
                token = auth_header.split(" ", 1)[1] # Safer split
        return token

    async def get_token_from_websocket_header(self, websocket: WebSocket) -> Optional[str]: # From async template
        auth_header = websocket.headers.get("authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            return auth_header.split(" ", 1)[1] # Safer split
        return None

    async def get_current_user(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
        """Gets username of current user. For full data, use get_current_user_full_data_dependency."""
        token = credentials.credentials
        token_data = await self.verify_token(token, "access")

        if not token_data:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired access token", headers={"WWW-Authenticate": "Bearer"})
        
        user_details = await self.user_manager.get_user_by_id(UUID(token_data.user_id)) # Ensure UUID

        if not user_details or not user_details.get("is_active"):
            logger.warning(f"Authenticated user_id {token_data.user_id} not found or inactive in DB.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User associated with token not found or is inactive.", headers={"WWW-Authenticate": "Bearer"})
        return user_details["username"]

    async def get_token_from_header(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
        """Extracts token and checks if it's blacklisted."""
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authorization credentials", headers={"WWW-Authenticate": "Bearer"})
        token = credentials.credentials
        try:
            if await self.is_token_blacklisted(token):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked", headers={"WWW-Authenticate": "Bearer"})
        except Exception as e:
            logger.error(f"Error checking token blacklist: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error verifying token status.")
        return token
    
    async def get_current_user_full_data_dependency(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict: 
        """Dependency to get the current authenticated user's full data from token and DB."""
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authorization credentials", headers={"WWW-Authenticate": "Bearer"})
        token = credentials.credentials
        try:
            if await self.is_token_blacklisted(token):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked", headers={"WWW-Authenticate": "Bearer"})
        except Exception as e:
            logger.error(f"Error checking token blacklist during full data retrieval: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error verifying token status.")
        
        token_data = await self.verify_token(token, expected_token_type="access")
        if not token_data:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired access token for full data", headers={"WWW-Authenticate": "Bearer"})

        db_user_data = await self.user_manager.get_user_by_id(UUID(token_data.user_id)) # Ensure UUID

        if not db_user_data or not db_user_data.get("is_active"):
            logger.warning(f"User {token_data.user_id} from token not found in DB or is inactive.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User (from token) not found or inactive.", headers={"WWW-Authenticate": "Bearer"})

        user_id_uuid = UUID(token_data.user_id) # Already UUID if token_data.user_id is from validated source
        workspace_id_uuid = UUID(token_data.workspace_id) if token_data.workspace_id else None

        return {
            "user_id": user_id_uuid, 
            "username": db_user_data.get("username"),
            "role": db_user_data.get("role"), 
            "workspace_member_role": db_user_data.get("workspace_member_role"), # Added from original sync
            "workspace_id": workspace_id_uuid,
            "email": db_user_data.get("email"), 
            "is_active": db_user_data.get("is_active")
        }
    
    async def clean_expired_tokens(self) -> int:
        """Clean expired tokens from user_tokens table."""
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        deleted_user_tokens = await self.db_manager.execute_query(
            "DELETE FROM user_tokens WHERE refresh_expires_at < $1", (now,), return_rowcount=True
        )
        deleted_user_tokens = deleted_user_tokens or 0 # Handle None from DB manager
        
        if deleted_user_tokens > 0:
            logger.info(f"Cleaned {deleted_user_tokens} user tokens.")
        return deleted_user_tokens
    
    async def clean_expired_blacklist(self) -> int:
        """Remove expired tokens from the blacklist."""
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        deleted_count = await self.db_manager.execute_query(
            "DELETE FROM token_blacklist WHERE expires_at < $1", (now,), return_rowcount=True
        )
        deleted_count = deleted_count or 0 # Handle None from DB manager
        if deleted_count > 0:
            logger.info(f"Cleaned {deleted_count} expired blacklisted tokens.")
        return deleted_count

    async def log_action(self, content: str, user_id: Optional[Union[str, UUID]] = None, workspace_id: Optional[Union[str, UUID]] = None, 
                   action_type: Optional[str] = None, ip_address: Optional[str] = None, 
                   user_agent: Optional[str] = None, status: str ="success"):
        """Log an action in the system."""
        log_id = uuid4()
        now = datetime.now(ZoneInfo("Africa/Cairo"))

        log_user_id_uuid = UUID(str(user_id)) if user_id else None
        log_workspace_id_uuid = UUID(str(workspace_id)) if workspace_id else None
        
        try:
            await self.db_manager.execute_query(
                """
                INSERT INTO logs 
                (log_id, created_at, user_id, workspace_id, action_type, status, ip_address, user_agent, content)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                (log_id, now, log_user_id_uuid, log_workspace_id_uuid, action_type, status, ip_address, user_agent, content)
            )
            log_message = f"ACTION: {action_type or 'unknown'} | USER: {str(log_user_id_uuid) if log_user_id_uuid else 'anonymous'} | WS: {str(log_workspace_id_uuid) if log_workspace_id_uuid else 'N/A'} | STATUS: {status} | {content}"
            
            # Align logging levels with schema 'status' values (from async template)
            if status == "success" or status == "info": logger.info(log_message)
            elif status == "failure" or status == "warning": logger.warning(log_message) # Original sync mapped failure to warning too
            elif status == "error": logger.error(log_message)
            # Consider adding critical if needed, though original sync didn't have it
            else: logger.info(log_message) 

        except Exception as e:
            logger.error(f"Failed to log action ({action_type}) for user {str(log_user_id_uuid) if log_user_id_uuid else 'anonymous'}: {e}", exc_info=True)
    
    async def get_logs(self, filters:Optional[Dict]=None, date_filter:Optional[Dict]=None, limit:int=100, offset:int=0, sort_by:str="created_at", sort_direction:str="desc") -> List[Dict]:
        """Retrieve logs with optional filtering."""
        query = "SELECT log_id, created_at, user_id, workspace_id, action_type, status, ip_address, user_agent, content FROM logs"
        params_list: list = [] 
        where_clauses = []
        
        param_idx_counter = 1 # For $1, $2, ... placeholders

        if filters:
            for key, value in filters.items():
                if key in ["user_id", "workspace_id", "action_type", "status"]:
                    # Special handling for user_id and workspace_id if they might be None in DB
                    if value is None:
                         where_clauses.append(f"{key} IS NULL")
                    else:
                        where_clauses.append(f"{key} = ${param_idx_counter}")
                        # Ensure UUIDs are passed correctly if they are UUIDs in DB
                        # For string based filters, value is passed as is.
                        params_list.append(UUID(str(value)) if key in ["user_id", "workspace_id"] and value is not None else value)
                        param_idx_counter += 1
                # Original sync had "username" filter pass-through, which doesn't match logs table.
                # This version correctly filters only by available columns.

        if date_filter:
            if "start_date" in date_filter and date_filter["start_date"]:
                where_clauses.append(f"created_at >= ${param_idx_counter}")
                params_list.append(date_filter["start_date"])
                param_idx_counter += 1
            if "end_date" in date_filter and date_filter["end_date"]:
                where_clauses.append(f"created_at <= ${param_idx_counter}")
                params_list.append(date_filter["end_date"])
                param_idx_counter += 1
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        valid_sort_fields = ["created_at", "user_id", "workspace_id", "action_type", "status"]
        sort_by = sort_by if sort_by in valid_sort_fields else "created_at"
        sort_direction = sort_direction.upper() if sort_direction.lower() in ["asc", "desc"] else "DESC"
        
        query += f" ORDER BY {sort_by} {sort_direction}"
        query += f" LIMIT ${param_idx_counter} OFFSET ${param_idx_counter + 1}"
        params_list.extend([limit, offset])
        
        results = await self.db_manager.execute_query(query, tuple(params_list), fetch_all=True)
        
        logs_list = [] 
        for row in results or []: 
            logs_list.append({
                "log_id": str(row["log_id"]),
                "created_at": row["created_at"].isoformat(),
                "user_id": str(row["user_id"]) if row["user_id"] else None,
                "workspace_id": str(row["workspace_id"]) if row["workspace_id"] else None,
                "action_type": row["action_type"],
                "status": row["status"],
                # MODIFICATION: Convert ip_address object to string
                "ip_address": str(row["ip_address"]) if row["ip_address"] is not None else None,
                "user_agent": row["user_agent"],
                "content": row["content"]
            })
        return logs_list

    async def run_cleanup_maintenance(self) -> bool:
        """Run database maintenance tasks to clean up expired data."""
        try:
            # Assumes cleanup_expired_data() is a SQL function/procedure in your DB
            await self.db_manager.execute_query("SELECT cleanup_expired_data()") # Adjust if it's a procedure call
            logger.info("Database maintenance cleanup function (cleanup_expired_data) executed successfully.")
            return True
        except Exception as e:
            logger.error(f"Database maintenance cleanup failed: {e}", exc_info=True)
            return False
    
    def decode_token_without_verification(self, token: str) -> Optional[Dict]: # Stays sync
        """Decode token payload without verifying signature or expiration."""
        try:
            payload = jwt.decode(token, algorithms=[self.ALGORITHM], 
                                 options={"verify_signature": False, "verify_exp": False, 
                                          "verify_nbf": False, "verify_iat": False, # Be explicit
                                          "require_exp":False, "require_iat":False, "require_nbf":False}) # Be explicit
            return payload
        except PyJWTError as e: # Catch specific PyJWTError
            logging.error(f"Error decoding token without verification: {e}") # Use logging for consistency
            return None
        except Exception as e: # Catch any other error
            logging.error(f"Generic error decoding token without verification: {e}", exc_info=True) # Add exc_info
            return None
            