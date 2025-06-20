# async_user_manager.py
from typing import Dict, List, Optional, Tuple, Union
from passlib.context import CryptContext
from async_database import DatabaseManager
import uuid
from uuid import UUID 
import logging
from fastapi import HTTPException, status
import re
import pyotp 
from datetime import datetime, timezone, timedelta
import json
import hashlib
import requests

logger = logging.getLogger(__name__)

class UserManager:
    def __init__(self):
        self.pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
        self.db_manager = DatabaseManager() 

    def get_password_hash(self, password: str) -> str: # Sync
        return self.pwd_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool: # Sync
        if not hashed_password:
            logger.warning("Attempted to verify password against a null/empty hash.")
            return False
        return self.pwd_context.verify(plain_password, hashed_password)

    def validate_password_strength(self, password: str) -> Tuple[bool, str]: # Sync
        if len(password) < 8: 
            return False, "Password must be at least 8 characters long"
        if not re.search(r'[A-Z]', password):
            return False, "Password must contain at least one uppercase letter"
        if not re.search(r'[a-z]', password):
            return False, "Password must contain at least one lowercase letter"
        if not re.search(r'[0-9]', password):
            return False, "Password must contain at least one digit"
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character"
        return True, "Password meets requirements."

    def _is_common_password(self, password: str) -> bool: # Sync, HIBP is blocking I/O
        common_passwords = {
            "password123", "12345678", "qwerty123", "admin1234", 
            "welcome1", "123456789", "password1", "iloveyou", 
            "1234567890", "letmein123", "abc123456", "trustno1",
            "password!", "admin123", "football", "monkey123",
            "password", "123456", "qwerty", "admin"
        }
        if password.lower() in common_passwords: return True
        try:
            sha1_password = hashlib.sha1(password.encode()).hexdigest().upper()
            prefix, suffix = sha1_password[:5], sha1_password[5:]
            response = requests.get(f"https://api.pwnedpasswords.com/range/{prefix}", timeout=5)
            if response.status_code == 200:
                hashes = (line.split(':') for line in response.text.splitlines())
                for hash_suffix, count in hashes:
                    if hash_suffix == suffix:
                        logger.warning(f"Password found in HIBP breach data (count: {count}).")
                        return True
        except requests.RequestException as e:
            logger.warning(f"Could not check HIBP for password: {e}")
        except Exception as e_gen:
            logger.error(f"Generic error during HIBP check: {e_gen}")
        return False

    async def get_user_id_by_username_str(self, username: str) -> Optional[str]: 
        user_data = await self.get_user_by_username(username)
        if user_data and user_data.get("user_id"):
            return str(user_data["user_id"]) # user_id from get_user_by_username is UUID
        return None

    async def get_user_id_by_username_uuid(self, username: str) -> Optional[UUID]: 
        user_data = await self.get_user_by_username(username)
        if user_data and user_data.get("user_id"):
            # user_data["user_id"] should already be UUID from DB
            return user_data["user_id"]
        return None

    async def _log_security_event(self, user_id: Optional[Union[str, UUID]], event_type: str, severity: str, event_data: dict, ip_address: Optional[str] = None, workspace_id: Optional[Union[str, UUID]] = None):
        query = """
            INSERT INTO security_events 
            (event_id, user_id, workspace_id, event_type, severity, ip_address, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        event_id = uuid.uuid4()
        created_at = datetime.now(timezone.utc)
        
        # Ensure UUID objects for DB
        db_user_id = UUID(str(user_id)) if user_id and not isinstance(user_id, UUID) else (user_id if isinstance(user_id, UUID) else None)
        db_workspace_id = UUID(str(workspace_id)) if workspace_id and not isinstance(workspace_id, UUID) else (workspace_id if isinstance(workspace_id, UUID) else None)

        try:
            await self.db_manager.execute_query(query, (
                event_id, db_user_id, db_workspace_id, event_type, severity,
                ip_address, json.dumps(event_data), created_at
            ))
        except Exception as e:
            logger.error(f"Failed to log security event ({event_type}) for user {str(db_user_id)}: {e}", exc_info=True)

    async def create_user(self, username, email, password, role='user') -> bool:
        query_check = "SELECT user_id FROM users WHERE username = $1"
        existing_user = await self.db_manager.execute_query(query_check, (username,), fetch_one=True)

        if existing_user:
            logger.warning(f"Attempt to create user with existing username: {username}")
            return False 

        is_valid, msg = self.validate_password_strength(password)
        if not is_valid:
            logger.warning(f"Password validation failed for new user {username}: {msg}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg) 

        hashed_password = self.get_password_hash(password)
        user_id_obj = uuid.uuid4() # This is a UUID object

        created_at = datetime.now(timezone.utc)
        subscription_date = created_at + timedelta(days=90) 
        is_active = True 

        query_insert_user = """
            INSERT INTO users 
            (user_id, username, email, created_at, is_active, role, subscription_date, is_subscribed, is_search, is_prediction) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """
        await self.db_manager.execute_query(query_insert_user, (
            user_id_obj, username, email, created_at, is_active, role, # Pass user_id_obj (UUID)
            subscription_date, True, True, True 
        ))

        password_id_obj = uuid.uuid4() # This is a UUID object
        query_insert_password = """
            INSERT INTO user_accounts (password_id, user_id, password_hash, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
        """
        await self.db_manager.execute_query(query_insert_password, (
            password_id_obj, user_id_obj, hashed_password, created_at, created_at # Pass password_id_obj and user_id_obj (UUIDs)
        ))

        await self._log_security_event(user_id=user_id_obj, event_type="user_created", severity="low", event_data={"username": username, "email": email})
        return True 

    async def verify_user_password(self, username, password) -> bool:
        query = """
            SELECT u.user_id, ua.password_hash, u.is_active 
            FROM users u JOIN user_accounts ua ON u.user_id = ua.user_id
            WHERE u.username = $1
        """
        user_data = await self.db_manager.execute_query(query, (username,), fetch_one=True)
        
        if not user_data: return False
        
        user_id_obj = user_data["user_id"] # This is already UUID from asyncpg
        hashed_password = user_data["password_hash"]
        is_active = user_data["is_active"]
        
        if not is_active: return False
        
        is_valid = self.verify_password(password, hashed_password)
        
        if is_valid:
            await self.db_manager.execute_query("UPDATE users SET last_login = $1 WHERE user_id = $2", (datetime.now(timezone.utc), user_id_obj))
            await self._log_security_event(user_id=user_id_obj,event_type="successful_login",severity="low",event_data={"username": username})
        else:
            await self._log_security_event(user_id=user_id_obj,event_type="failed_login",severity="medium",event_data={"username": username})
        return is_valid
    
    async def get_user_by_email(self, email: str) -> Optional[Dict]:
        query = "SELECT * FROM users WHERE email = $1"
        user = await self.db_manager.execute_query(query, (email,), fetch_one=True)
        return dict(user) if user else None # user_id will be UUID
    
    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE username = $1
        """
        user = await self.db_manager.execute_query(query, (username,), fetch_one=True)
        return dict(user) if user else None # user_id will be UUID
        
    async def get_user_by_id(self, user_id: Union[str, UUID]) -> Optional[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE user_id = $1
        """
        db_param_user_id = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        user = await self.db_manager.execute_query(query, (db_param_user_id,), fetch_one=True)
        return dict(user) if user else None # user_id will be UUID

    async def reset_password(self, username: str, new_password: str) -> bool:
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # This is UUID
        
        is_valid, msg = self.validate_password_strength(new_password)
        if not is_valid:
            logger.warning(f"Password reset for {username} failed strength check: {msg}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

        hashed_password = self.get_password_hash(new_password)
        query = "UPDATE user_accounts SET password_hash = $1, updated_at = $2 WHERE user_id = $3"
        await self.db_manager.execute_query(query, (hashed_password, datetime.now(timezone.utc), user_id)) # user_id is UUID
        await self._log_security_event(user_id=user_id, event_type="password_reset", severity="medium", event_data={"username": username})
        return True
        
    async def verify_credentials(self, username: Optional[str] = None, email: Optional[str] = None, password: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        if not password or (not username and not email): return False, None
        
        field_name = "username" if username else "email"
        field_value = username if username else email

        query = f"""
            SELECT u.user_id, u.username, ua.password_hash, u.is_active
            FROM users u JOIN user_accounts ua ON u.user_id = ua.user_id
            WHERE u.{field_name} = $1
        """ 
        user_data = await self.db_manager.execute_query(query, (field_value,), fetch_one=True)

        if not user_data: return False, None
        
        user_id_obj = user_data["user_id"] # UUID from DB
        db_username = user_data["username"]
        hashed_password = user_data["password_hash"]
        is_active = user_data["is_active"]
        
        if not is_active: return False, db_username 
        
        is_valid = self.verify_password(password, hashed_password)
        if is_valid:
            await self.db_manager.execute_query("UPDATE users SET last_login = $1 WHERE user_id = $2", (datetime.now(timezone.utc), user_id_obj))
            await self._log_security_event(user_id=user_id_obj,event_type="successful_login",severity="low",event_data={"username": db_username})
            return True, db_username
        else:
            await self._log_security_event(user_id=user_id_obj,event_type="failed_login",severity="medium",event_data={"username": db_username})
        return False, db_username
    
    async def delete_user(self, username: str) -> bool:
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # UUID
        
        query = "DELETE FROM users WHERE user_id = $1" 
        rows_affected = await self.db_manager.execute_query(query, (user_id,), return_rowcount=True)

        if rows_affected is not None and rows_affected > 0:
            await self._log_security_event(user_id=None, event_type="user_deleted", severity="high", event_data={"username": username, "deleted_user_id": str(user_id)})
            return True
        return False
    
    async def delete_all_users(self) -> int:
        query = "DELETE FROM users" 
        rows_deleted = await self.db_manager.execute_query(query, return_rowcount=True)
        rows_deleted = rows_deleted if rows_deleted is not None else 0

        await self._log_security_event(user_id=None, event_type="all_users_deleted", severity="critical", event_data={"count": rows_deleted})
        return rows_deleted
    
    async def update_user_status(self, username: str, is_active: bool) -> bool:
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # UUID

        query = "UPDATE users SET is_active = $1 WHERE user_id = $2"
        rows_affected = await self.db_manager.execute_query(query, (is_active, user_id), return_rowcount=True)

        if rows_affected is not None and rows_affected > 0:
            status_str = "activated" if is_active else "deactivated"
            await self._log_security_event(user_id=user_id, event_type=f"user_{status_str}", severity="medium", event_data={"username": username})
        return bool(rows_affected is not None and rows_affected > 0)
    
    async def update_user_subscription(self, username: str, is_subscribed: bool, months: int = 3) -> bool:
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # UUID
        
        rows_affected = 0
        subscription_date_val: Optional[datetime] = None 
        if is_subscribed:
            subscription_date_val = datetime.now(timezone.utc) + timedelta(days=30*months)
            query = "UPDATE users SET is_subscribed = $1, subscription_date = $2 WHERE user_id = $3"
            rows_affected = await self.db_manager.execute_query(query, (is_subscribed, subscription_date_val, user_id), return_rowcount=True)
        else: 
            query = "UPDATE users SET is_subscribed = $1 WHERE user_id = $2"
            rows_affected = await self.db_manager.execute_query(query, (is_subscribed, user_id), return_rowcount=True)
        
        if rows_affected is not None and rows_affected > 0:
            status_str = "subscribed" if is_subscribed else "unsubscribed"
            await self._log_security_event(user_id=user_id, event_type=f"user_{status_str}", severity="low", event_data={"username": username, "subscription_date": subscription_date_val.isoformat() if subscription_date_val else None})
        return bool(rows_affected is not None and rows_affected > 0)
    
    async def update_user_role(self, username: str, role: str) -> bool:
        valid_roles = ['user', 'admin'] 
        if role not in valid_roles:
            logger.warning(f"Invalid role attempted: {role}")
            return False
            
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # UUID
        
        query = "UPDATE users SET role = $1 WHERE user_id = $2"
        rows_affected = await self.db_manager.execute_query(query, (role, user_id), return_rowcount=True)
        
        if rows_affected is not None and rows_affected > 0:
            await self._log_security_event(user_id=user_id, event_type="user_role_changed", severity="high", event_data={"username": username, "new_role": role})
        return bool(rows_affected is not None and rows_affected > 0)
    
    async def update_camera_count(self, username: str, count: int) -> bool:
        if count < 0:
            logger.warning(f"Invalid camera count attempted: {count}")
            return False
            
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] # UUID
        
        query = "UPDATE users SET count_of_camera = $1 WHERE user_id = $2"
        rows_affected = await self.db_manager.execute_query(query, (count, user_id), return_rowcount=True)
        
        if rows_affected is not None and rows_affected > 0:
            await self._log_security_event(user_id=user_id, event_type="camera_count_updated", severity="low", event_data={"username": username, "new_count": count})
        return bool(rows_affected is not None and rows_affected > 0)
    
    async def get_all_users(self) -> List[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera
            FROM users
        """
        users_data = await self.db_manager.execute_query(query, fetch_all=True)
        return [dict(user) for user in users_data] if users_data else [] # user_id will be UUID
        
    async def get_subscribed_users(self) -> List[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, 
                role, subscription_date, count_of_camera  
            FROM users
            WHERE is_subscribed = TRUE
        """
        users_data = await self.db_manager.execute_query(query, fetch_all=True)
        return [dict(user) for user in users_data] if users_data else [] # user_id will be UUID

    async def change_password(self, username: str, current_password: str, new_password: str) -> bool:
        if not await self.verify_user_password(username, current_password):
            return False
            
        user_info = await self.get_user_by_username(username)
        if not user_info: return False 
        user_id = user_info["user_id"] # UUID
        
        is_valid, msg = self.validate_password_strength(new_password)
        if not is_valid:
            logger.warning(f"Password change for {username} failed strength check: {msg}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

        hashed_password = self.get_password_hash(new_password)
        query = "UPDATE user_accounts SET password_hash = $1, updated_at = $2 WHERE user_id = $3"
        rows_affected = await self.db_manager.execute_query(query, (hashed_password, datetime.now(timezone.utc), user_id), return_rowcount=True)
        
        if rows_affected is not None and rows_affected > 0:
            await self._log_security_event(user_id=user_id, event_type="password_changed", severity="medium", event_data={"username": username})
            return True
        return False

    async def get_search_status(self, user_id: Union[str, UUID]) -> Optional[bool]:
        query = "SELECT is_search FROM users WHERE user_id = $1"
        db_user_id = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        result = await self.db_manager.execute_query(query, (db_user_id,), fetch_one=True)
        return result["is_search"] if result else None

    async def get_prediction_status(self, user_id: Union[str, UUID]) -> Optional[bool]:
        query = "SELECT is_prediction FROM users WHERE user_id = $1"
        db_user_id = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        result = await self.db_manager.execute_query(query, (db_user_id,), fetch_one=True)
        return result["is_prediction"] if result else None
    
    async def update_search_status(self, user_id: Union[str, UUID], is_search: bool) -> bool:
        query = "UPDATE users SET is_search = $1 WHERE user_id = $2"
        db_user_id = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        rows_updated = await self.db_manager.execute_query(query, (is_search, db_user_id), return_rowcount=True)
        return bool(rows_updated is not None and rows_updated > 0)
    
    async def update_prediction_status(self, user_id: Union[str, UUID], is_prediction: bool) -> bool:
        query = "UPDATE users SET is_prediction = $1 WHERE user_id = $2"
        db_user_id = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        rows_updated = await self.db_manager.execute_query(query, (is_prediction, db_user_id), return_rowcount=True)
        return bool(rows_updated is not None and rows_updated > 0)

    def generate_totp_secret(self) -> str: # Sync
        return pyotp.random_base32()

    def verify_totp_code(self, secret: str, code: str) -> bool: # Sync
        totp = pyotp.TOTP(secret)
        return totp.verify(code) 
    
    async def create_user_with_workspace(self, username: str, email: str, password: str, role: str = 'user') -> Tuple[bool, Optional[UUID], Optional[UUID]]:
        async with self.db_manager.transaction() as conn: 
            query_check = "SELECT user_id FROM users WHERE username = $1"
            existing_user = await self.db_manager.execute_query(query_check, (username,), fetch_one=True, connection=conn)
            if existing_user:
                return False, None, None
            
            is_valid, msg = self.validate_password_strength(password)
            if not is_valid:
                logger.warning(f"Password validation failed for user {username}: {msg}")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Password does not meet security requirements. {msg}")

            hashed_password = self.get_password_hash(password)
            user_id_obj = uuid.uuid4() # UUID object
            created_at = datetime.now(timezone.utc)
            subscription_date = created_at + timedelta(days=90)
            
            user_query = """
                INSERT INTO users (user_id, username, email, created_at, is_active, role, subscription_date, is_subscribed, is_search, is_prediction) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """
            await self.db_manager.execute_query(user_query, (user_id_obj, username, email, created_at, True, role, subscription_date, True, True, True), connection=conn)

            password_id_obj = uuid.uuid4() # UUID object
            password_query = "INSERT INTO user_accounts (password_id, user_id, password_hash, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)"
            await self.db_manager.execute_query(password_query, (password_id_obj, user_id_obj, hashed_password, created_at, created_at), connection=conn)
            
            workspace_q = "SELECT workspace_id FROM workspaces WHERE name = $1 LIMIT 1"
            default_ws_row = await self.db_manager.execute_query(workspace_q, ('Default Workspace',), fetch_one=True, connection=conn)
            
            workspace_id_to_use: UUID
            if default_ws_row and default_ws_row.get("workspace_id"):
                workspace_id_to_use = default_ws_row["workspace_id"] # Already UUID from DB
            else:
                workspace_id_to_use = uuid.uuid4() # UUID object
                create_ws_q = "INSERT INTO workspaces (workspace_id, name, description, created_at, updated_at, is_active) VALUES ($1, $2, $3, $4, $5, $6)"
                await self.db_manager.execute_query(create_ws_q, (workspace_id_to_use, "Default Workspace", "Default workspace for new users", created_at, created_at, True), connection=conn)
            
            membership_id_obj = uuid.uuid4() # UUID object
            member_role = "admin" if role == "admin" else "member" 
            membership_query = "INSERT INTO workspace_members (membership_id, workspace_id, user_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"
            await self.db_manager.execute_query(membership_query, (membership_id_obj, workspace_id_to_use, user_id_obj, member_role, created_at, created_at), connection=conn)
            
        await self._log_security_event(user_id=user_id_obj, event_type="user_created_with_workspace", severity="low", event_data={"username": username, "workspace_id": str(workspace_id_to_use)})
        return True, user_id_obj, workspace_id_to_use

    async def get_active_workspace(self, user_id: Union[str, UUID]) -> Optional[Dict]:
        user_id_obj = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        query_session_ws = """
            SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, w.is_active, wm.role as member_role
            FROM workspaces w
            JOIN user_tokens ut ON w.workspace_id = ut.workspace_id 
            JOIN workspace_members wm ON w.workspace_id = wm.workspace_id AND ut.user_id = wm.user_id
            WHERE ut.user_id = $1 AND ut.is_active = TRUE AND w.is_active = TRUE
            ORDER BY ut.updated_at DESC LIMIT 1 
        """
        workspace_data = await self.db_manager.execute_query(query_session_ws, (user_id_obj,), fetch_one=True)
        
        if not workspace_data:
            query_default_ws = """
                SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, w.is_active, wm.role as member_role
                FROM workspaces w JOIN workspace_members wm ON w.workspace_id = wm.workspace_id
                WHERE wm.user_id = $1 AND w.is_active = TRUE
                ORDER BY wm.created_at ASC LIMIT 1 
            """
            workspace_data = await self.db_manager.execute_query(query_default_ws, (user_id_obj,), fetch_one=True)

        if workspace_data:
            # workspace_data["workspace_id"] is UUID from DB
            # workspace_data["created_at"] / "updated_at" are datetime from DB
            return {
                "workspace_id": workspace_data["workspace_id"], # Return as UUID object
                "name": workspace_data["name"],
                "description": workspace_data["description"],
                "created_at": workspace_data["created_at"], # Return as datetime object
                "updated_at": workspace_data["updated_at"], # Return as datetime object
                "is_active": workspace_data["is_active"],
                "member_role": workspace_data["member_role"]
            }
        return None

    async def set_active_workspace(self, user_id: Union[str, UUID], workspace_id: Union[str, UUID]) -> Tuple[bool, str]:
        user_id_obj = UUID(str(user_id)) if not isinstance(user_id, UUID) else user_id
        workspace_id_obj = UUID(str(workspace_id)) if not isinstance(workspace_id, UUID) else workspace_id

        async with self.db_manager.transaction() as conn:
            member_query = "SELECT membership_id FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
            membership = await self.db_manager.execute_query(member_query, (user_id_obj, workspace_id_obj), fetch_one=True, connection=conn)
            if not membership:
                return False, "You are not a member of this workspace or workspace does not exist."
                
            ws_query = "SELECT is_active FROM workspaces WHERE workspace_id = $1"
            workspace = await self.db_manager.execute_query(ws_query, (workspace_id_obj,), fetch_one=True, connection=conn)
            if not workspace or not workspace.get("is_active"):
                return False, "Workspace is not active or not found."
                
            now_utc = datetime.now(timezone.utc)
            token_update_q = "UPDATE user_tokens SET workspace_id = $1, updated_at = $2 WHERE user_id = $3 AND is_active = TRUE"
            await self.db_manager.execute_query(token_update_q, (workspace_id_obj, now_utc, user_id_obj), connection=conn)
        
        await self._log_security_event(user_id=user_id_obj, workspace_id=workspace_id_obj, event_type="active_workspace_set", severity="info", event_data={})
        return True, "Active workspace updated for all current sessions/tokens."
