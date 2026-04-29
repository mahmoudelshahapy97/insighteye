# app/services/user_service.py
from typing import Dict, List, Optional, Tuple, Union, Any
from passlib.context import CryptContext
from app.services.postgres_service import postgres_service
from app.services.database import db_manager
import uuid
from uuid import UUID 
import logging
from fastapi import HTTPException, status
import re
import pyotp 
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import hashlib
import requests
import asyncpg
from collections import defaultdict

logger = logging.getLogger(__name__)

class UserManager:
    def __init__(self):
        self.pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
        self.db_manager = db_manager

    def get_password_hash(self, password: str) -> str:
        return self.pwd_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        if not hashed_password:
            logger.warning("Attempted to verify password against a null/empty hash.")
            return False
        return self.pwd_context.verify(plain_password, hashed_password)

    def validate_password_strength(self, password: str) -> Tuple[bool, str]:
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

    def _is_common_password(self, password: str) -> bool:
        """Check if password is common or breached. Returns True if unsafe."""
        common_passwords = {
            "password123", "12345678", "qwerty123", "admin1234", 
            "welcome1", "123456789", "password1", "iloveyou", 
            "1234567890", "letmein123", "abc123456", "trustno1",
            "password!", "admin123", "football", "monkey123",
            "password", "123456", "qwerty", "admin"
        }

        if password.lower() in common_passwords:
            logger.warning(f"Password found in local common password list.")
            return True
        
        try:
            sha1_password = hashlib.sha1(password.encode()).hexdigest().upper()
            prefix, suffix = sha1_password[:5], sha1_password[5:]
            
            response = requests.get(
                f"https://api.pwnedpasswords.com/range/{prefix}", 
                timeout=3  
            )
            
            if response.status_code == 200:
                hashes = (line.split(':') for line in response.text.splitlines())
                for hash_suffix, count in hashes:
                    if hash_suffix == suffix:
                        logger.warning(f"Password found in HIBP breach data (count: {count}).")
                        return True
                return False
            elif response.status_code == 429:
                logger.warning("HIBP API rate limit reached. Skipping breach check.")
                return False
            else:
                logger.warning(f"HIBP API returned status {response.status_code}. Skipping breach check.")
                return False
                
        except requests.Timeout:
            logger.warning("HIBP API timeout. Skipping breach check.")
            return False
        except requests.RequestException as e:
            logger.warning(f"Could not check HIBP for password: {e}")
            return False
        except Exception as e_gen:
            logger.error(f"Unexpected error during HIBP check: {e_gen}", exc_info=True)
            return False

    async def _log_security_event(self, user_id: Optional[Union[str, UUID]], event_type: str, severity: str, event_data: dict, ip_address: Optional[str] = None, workspace_id: Optional[Union[str, UUID]] = None):
        query = """
            INSERT INTO security_events 
            (event_id, user_id, workspace_id, event_type, severity, ip_address, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        event_id = uuid.uuid4()
        created_at = datetime.now(ZoneInfo("Africa/Cairo"))
        
        db_user_id = UUID(str(user_id)) if user_id and not isinstance(user_id, UUID) else (user_id if isinstance(user_id, UUID) else None)
        db_workspace_id = UUID(str(workspace_id)) if workspace_id and not isinstance(workspace_id, UUID) else (workspace_id if isinstance(workspace_id, UUID) else None)

        try:
            await self.db_manager.execute_query(query, (
                event_id, db_user_id, db_workspace_id, event_type, severity,
                ip_address, json.dumps(event_data), created_at
            ))
        except Exception as e:
            logger.error(f"Failed to log security event ({event_type}) for user {str(db_user_id)}: {e}", exc_info=True)

    async def create_user(self, username, email, password, role='user', count_of_camera: int = 5) -> bool:
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

        created_at = datetime.now(ZoneInfo("Africa/Cairo"))
        subscription_date = created_at + timedelta(days=90) 
        is_active = True 

        query_insert_user = """
            INSERT INTO users 
            (user_id, username, email, created_at, is_active, role, count_of_camera, subscription_date, is_subscribed, is_search, is_prediction) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """
        await self.db_manager.execute_query(query_insert_user, (
            user_id_obj, username, email, created_at, is_active, role, count_of_camera, 
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
        
        user_id_obj = user_data["user_id"] 
        hashed_password = user_data["password_hash"]
        is_active = user_data["is_active"]
        
        if not is_active: return False
        
        is_valid = self.verify_password(password, hashed_password)
        
        if is_valid:
            await self.db_manager.execute_query("UPDATE users SET last_login = $1 WHERE user_id = $2", (datetime.now(ZoneInfo("Africa/Cairo")), user_id_obj))
            await self._log_security_event(user_id=user_id_obj,event_type="successful_login",severity="low",event_data={"username": username})
        else:
            await self._log_security_event(user_id=user_id_obj,event_type="failed_login",severity="medium",event_data={"username": username})
        return is_valid
    
    async def get_user_by_email(self, email: str) -> Optional[Dict]:
        query = "SELECT * FROM users WHERE email = $1 AND is_active = TRUE"
        user = await self.db_manager.execute_query(query, (email,), fetch_one=True)
        return dict(user) if user else None
    
    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE username = $1
        """
        user = await self.db_manager.execute_query(query, (username,), fetch_one=True)
        return dict(user) if user else None 
        
    async def get_user_by_id(self, user_id: Union[str, UUID]) -> Optional[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE user_id = $1
        """
        db_param_user_id = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        user = await self.db_manager.execute_query(query, (db_param_user_id,), fetch_one=True)
        return dict(user) if user else None 

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
        await self.db_manager.execute_query(query, (hashed_password, datetime.now(ZoneInfo("Africa/Cairo")), user_id)) # user_id is UUID
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
        
        user_id_obj = user_data["user_id"]
        db_username = user_data["username"]
        hashed_password = user_data["password_hash"]
        is_active = user_data["is_active"]
        
        if not is_active: return False, db_username 
        
        is_valid = self.verify_password(password, hashed_password)
        if is_valid:
            await self.db_manager.execute_query("UPDATE users SET last_login = $1 WHERE user_id = $2", (datetime.now(ZoneInfo("Africa/Cairo")), user_id_obj))
            await self._log_security_event(user_id=user_id_obj,event_type="successful_login",severity="low",event_data={"username": db_username})
            return True, db_username
        else:
            await self._log_security_event(user_id=user_id_obj,event_type="failed_login",severity="medium",event_data={"username": db_username})
        return False, db_username

    async def _check_workspace_admin_constraints(
        self, user_id: UUID
    ) -> None:
        """
        Check if user is the only admin in any workspace.
        Raises HTTPException if user cannot be deleted.
        """
        query = """
            SELECT wm.workspace_id, w.name as workspace_name,
                COUNT(*) FILTER (WHERE wm.role = 'admin') as admin_count
            FROM workspace_members wm
            JOIN workspaces w ON wm.workspace_id = w.workspace_id
            WHERE wm.workspace_id IN (
                SELECT workspace_id FROM workspace_members WHERE user_id = $1 AND role = 'admin'
            )
            GROUP BY wm.workspace_id, w.name
        """
        
        workspaces = await self.db_manager.execute_query(query, (user_id,), fetch_all=True)
        
        blocking_workspaces = []
        for ws in workspaces or []:
            if ws["admin_count"] == 1:
                blocking_workspaces.append(ws["workspace_name"])
        
        if blocking_workspaces:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Cannot delete user. User is the only admin in workspace(s): {', '.join(blocking_workspaces)}. "
                    f"Please assign another admin first."
            )

    async def _delete_user_from_database(self, user_id: UUID, username: str) -> Dict:
        """
        Delete user from database with all cascading relationships.
        
        The database schema has CASCADE deletes set up, so deleting from users
        will automatically delete from:
        - user_accounts (CASCADE)
        - workspace_members (CASCADE)
        - video_stream (CASCADE)
        - param_stream (CASCADE)
        - sessions (CASCADE)
        - user_tokens (CASCADE)
        - token_blacklist (CASCADE)
        - notifications (CASCADE)
        - fire_detection_state (CASCADE via video_stream)
        - people_count_alert_state (CASCADE via video_stream)
        
        Related data with SET NULL:
        - logs (user_id SET NULL)
        - security_events (user_id SET NULL)
        
        Returns:
            Dictionary with deletion results
        """
        result = {
            "success": False,
            "tables_affected": [],
            "rows_deleted": {}
        }
        
        try:
            async with self.db_manager.transaction() as conn:
                # Track what will be deleted before the cascade
                tables_to_check = [
                    ("user_accounts", "user_id"),
                    ("workspace_members", "user_id"),
                    ("video_stream", "user_id"),
                    ("param_stream", "user_id"),
                    ("sessions", "user_id"),
                    ("user_tokens", "user_id"),
                    ("token_blacklist", "user_id"),
                    ("notifications", "user_id"),
                ]
                
                # Get counts before deletion
                for table_name, column_name in tables_to_check:
                    count_query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {column_name} = $1"
                    count_result = await self.db_manager.execute_query(
                        count_query, (user_id,), fetch_one=True, connection=conn
                    )
                    if count_result and count_result["count"] > 0:
                        result["tables_affected"].append(table_name)
                        result["rows_deleted"][table_name] = count_result["count"]
                
                # Delete the user (cascading will handle related records)
                delete_query = "DELETE FROM users WHERE user_id = $1"
                rows_affected = await self.db_manager.execute_query(
                    delete_query, (user_id,), return_rowcount=True, connection=conn
                )
                
                if rows_affected == 0:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"User with ID {user_id} not found during deletion"
                    )
                
                result["tables_affected"].append("users")
                result["rows_deleted"]["users"] = rows_affected
                result["success"] = True
                
                logger.info(
                    f"Successfully deleted user {username} (ID: {user_id}) "
                    f"from {len(result['tables_affected'])} tables"
                )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Database deletion failed for user {username}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database deletion failed: {str(e)}"
            )
        
        return result

    async def delete_user_completely(
        self,
        username: str,
        performing_user_id: Optional[UUID] = None,
        is_admin: bool = False
    ) -> Dict:
        """
        Completely delete a user from all systems including:
        - Database tables (cascading deletes)
        - Workspace memberships
        - All associated resources
        
        Args:
            username: Username to delete
            performing_user_id: ID of user performing the deletion
            is_admin: Whether performing user is system admin
            
        Returns:
            Dictionary with deletion results
        """
        deletion_results = {
            "username": username,
            "user_id": None,

            "workspace_removal": {"success": False, "workspaces_removed": []},
            "database_deletion": {"success": False, "tables_affected": []},
            "errors": [],
            "deleted_at": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
        
        try:
            # Step 1: Get user details
            user_details = await self.get_user_by_username(username)
            if not user_details:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"User '{username}' not found"
                )
            
            user_id = user_details["user_id"]
            deletion_results["user_id"] = str(user_id)
            
            # Step 2: Permission check (unless system admin)
            if not is_admin and performing_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You can only delete your own account unless you are an admin"
                )
            
            # Step 3: Check if user is the only admin in any workspace
            await self._check_workspace_admin_constraints(user_id)
            

            # Step 6: Delete from database (with cascading)
            db_result = await self._delete_user_from_database(user_id, username)
            deletion_results["database_deletion"] = db_result
            
            # Step 7: Log security event
            await self._log_security_event(
                user_id=performing_user_id,
                event_type="user_deleted_complete",
                severity="high",
                event_data={
                    "deleted_username": username,
                    "deleted_user_id": str(user_id),
                    "tables_affected": len(deletion_results["database_deletion"]["tables_affected"])
                }
            )
            
            return deletion_results
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error during complete user deletion for '{username}': {e}", exc_info=True)
            deletion_results["errors"].append(str(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to completely delete user: {str(e)}"
            )

    async def get_user_deletion_preview(self, username: str) -> Dict:
        """
        Preview what will be deleted when removing a user.
        Useful for confirming before actual deletion.
        
        Returns:
            Dictionary containing:
            - Counts of related records
            - Workspace memberships
            - Warnings about deletion constraints
        """
        user_details = await self.get_user_by_username(username)
        if not user_details:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User '{username}' not found"
            )
        
        user_id = user_details["user_id"]
        
        preview = {
            "username": username,
            "user_id": str(user_id),
            "will_delete": {},
            "workspace_memberships": [],
            "warnings": []
        }
        
        # Count related records
        tables_to_preview = [
            ("video_stream", "user_id", "cameras"),
            ("workspace_members", "user_id", "workspace_memberships"),
            ("sessions", "user_id", "active_sessions"),
            ("user_tokens", "user_id", "tokens"),
            ("notifications", "user_id", "notifications"),
        ]
        
        for table_name, column_name, display_name in tables_to_preview:
            query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {column_name} = $1"
            result = await self.db_manager.execute_query(query, (user_id,), fetch_one=True)
            if result:
                preview["will_delete"][display_name] = result["count"]
        
        # Get workspace details
        ws_query = """
            SELECT w.name, w.workspace_id, wm.role
            FROM workspace_members wm
            JOIN workspaces w ON wm.workspace_id = w.workspace_id
            WHERE wm.user_id = $1
        """
        workspaces = await self.db_manager.execute_query(ws_query, (user_id,), fetch_all=True)
        
        for ws in workspaces or []:
            preview["workspace_memberships"].append({
                "workspace_name": ws["name"],
                "workspace_id": str(ws["workspace_id"]),
                "role": ws["role"]
            })
            
            # Check if user is only admin
            if ws["role"] == "admin":
                admin_count_query = """
                    SELECT COUNT(*) as count FROM workspace_members
                    WHERE workspace_id = $1 AND role = 'admin'
                """
                admin_result = await self.db_manager.execute_query(
                    admin_count_query, (ws["workspace_id"],), fetch_one=True
                )
                if admin_result and admin_result["count"] == 1:
                    preview["warnings"].append(
                        f"User is the only admin in workspace '{ws['name']}'. "
                        "Deletion will be blocked."
                    )
        
        return preview

    async def delete_user(self, username: str, is_admin: bool = False, 
                            performing_user_id: Optional[UUID] = None) -> bool:
        """
        Delete user completely from all systems.
        
        This now uses the comprehensive deletion logic.
        """
        try:
            result = await self.delete_user_completely(
                username=username,
                performing_user_id=performing_user_id,
                is_admin=is_admin
            )
            return result["database_deletion"]["success"]
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in delete_user for {username}: {e}", exc_info=True)
            return False
        
    async def delete_user_db_only(self, username: str) -> bool:
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] 
        
        query = "DELETE FROM users WHERE user_id = $1" 
        rows_affected = await self.db_manager.execute_query(query, (user_id,), return_rowcount=True)

        if rows_affected > 0:
            await self._log_security_event(user_id=None, event_type="user_deleted", severity="high", event_data={"username": username, "deleted_user_id": str(user_id)})
            return True
        return False
    
    async def delete_all_users(self) -> int:
        query = "DELETE FROM users" 
        rows_deleted = await self.db_manager.execute_query(query, return_rowcount=True)
        rows_deleted = rows_deleted if rows_deleted is not None else 0

        await self._log_security_event(user_id=None, event_type="all_users_deleted", severity="critical", event_data={"count": rows_deleted})
        return rows_deleted

    async def update_user_role(self, username: str, role: str) -> bool:
        valid_roles = ['user', 'admin'] 
        if role not in valid_roles:
            logger.warning(f"Invalid role attempted: {role}")
            return False
            
        user_info = await self.get_user_by_username(username)
        if not user_info: return False
        user_id = user_info["user_id"] 
        
        query = "UPDATE users SET role = $1 WHERE user_id = $2"
        rows_affected = await self.db_manager.execute_query(query, (role, user_id), return_rowcount=True)
        
        if rows_affected > 0:
            await self._log_security_event(user_id=user_id, event_type="user_role_changed", severity="high", event_data={"username": username, "new_role": role})
        return bool(rows_affected is not None and rows_affected > 0)

    async def get_all_users(self) -> List[Dict]:
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                is_subscribed, subscription_date, count_of_camera
            FROM users
        """
        users_data = await self.db_manager.execute_query(query, fetch_all=True)
        return [dict(user) for user in users_data] if users_data else [] 

    async def change_password(self, username: str, current_password: str, new_password: str) -> bool:
        if not await self.verify_user_password(username, current_password):
            return False
            
        user_info = await self.get_user_by_username(username)
        if not user_info: return False 
        user_id = user_info["user_id"] 
        
        is_valid, msg = self.validate_password_strength(new_password)
        if not is_valid:
            logger.warning(f"Password change for {username} failed strength check: {msg}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

        hashed_password = self.get_password_hash(new_password)
        query = "UPDATE user_accounts SET password_hash = $1, updated_at = $2 WHERE user_id = $3"
        rows_affected = await self.db_manager.execute_query(query, (hashed_password, datetime.now(ZoneInfo("Africa/Cairo")), user_id), return_rowcount=True)
        
        if rows_affected > 0:
            await self._log_security_event(user_id=user_id, event_type="password_changed", severity="medium", event_data={"username": username})
            return True
        return False

    async def create_user_with_workspace(
        self, username: str, email: str, password: str, role: str = 'user', count_of_camera: int = 5
    ) -> Tuple[bool, Optional[UUID], Optional[UUID]]:
        """Create user and assign to workspace in a transaction."""
        try:
            async with self.db_manager.transaction() as conn: 
                # Check existing user
                query_check = "SELECT user_id FROM users WHERE username = $1"
                existing_user = await self.db_manager.execute_query(
                    query_check, (username,), fetch_one=True, connection=conn
                )
                if existing_user:
                    logger.warning(f"Attempt to create duplicate user: {username}")
                    return False, None, None
                
                # Validate password
                is_valid, msg = self.validate_password_strength(password)
                if not is_valid:
                    logger.warning(f"Password validation failed for user {username}: {msg}")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST, 
                        detail=f"Password does not meet security requirements. {msg}"
                    )

                # Hash password
                hashed_password = self.get_password_hash(password)
                user_id_obj = uuid.uuid4()
                created_at = datetime.now(ZoneInfo("Africa/Cairo"))
                subscription_date = created_at + timedelta(days=90)
                
                # Insert user with count_of_camera
                user_query = """
                    INSERT INTO users 
                    (user_id, username, email, created_at, is_active, role, count_of_camera,
                    subscription_date, is_subscribed, is_search, is_prediction) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """
                await self.db_manager.execute_query(
                    user_query, 
                    (user_id_obj, username, email, created_at, True, role, count_of_camera,
                    subscription_date, True, True, True), 
                    connection=conn
                )

                # Insert password
                password_id_obj = uuid.uuid4()
                password_query = """
                    INSERT INTO user_accounts 
                    (password_id, user_id, password_hash, created_at, updated_at) 
                    VALUES ($1, $2, $3, $4, $5)
                """
                await self.db_manager.execute_query(
                    password_query, 
                    (password_id_obj, user_id_obj, hashed_password, created_at, created_at), 
                    connection=conn
                )
                
                # Get or create default workspace
                workspace_q = "SELECT workspace_id FROM workspaces WHERE name = $1 LIMIT 1"
                default_ws_row = await self.db_manager.execute_query(
                    workspace_q, ('Default Workspace',), fetch_one=True, connection=conn
                )
                
                workspace_id_to_use: UUID
                if default_ws_row and default_ws_row.get("workspace_id"):
                    workspace_id_to_use = default_ws_row["workspace_id"]
                else:
                    workspace_id_to_use = uuid.uuid4()
                    create_ws_q = """
                        INSERT INTO workspaces 
                        (workspace_id, name, description, created_at, updated_at, is_active) 
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """
                    await self.db_manager.execute_query(
                        create_ws_q, 
                        (workspace_id_to_use, "Default Workspace", 
                        "Default workspace for new users", created_at, created_at, True), 
                        connection=conn
                    )
                
                # Add workspace membership
                membership_id_obj = uuid.uuid4()
                member_role = "admin" if role == "admin" else "member"
                membership_query = """
                    INSERT INTO workspace_members 
                    (membership_id, workspace_id, user_id, role, created_at, updated_at) 
                    VALUES ($1, $2, $3, $4, $5, $6)
                """
                await self.db_manager.execute_query(
                    membership_query, 
                    (membership_id_obj, workspace_id_to_use, user_id_obj, 
                    member_role, created_at, created_at), 
                    connection=conn
                )
            
            # Log success (outside transaction)
            await self._log_security_event(
                user_id=user_id_obj, 
                event_type="user_created_with_workspace", 
                severity="low", 
                event_data={
                    "username": username, 
                    "workspace_id": str(workspace_id_to_use),
                    "count_of_camera": count_of_camera
                }
            )
            return True, user_id_obj, workspace_id_to_use
            
        except HTTPException:
            raise
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error creating user with workspace: {db_err}", exc_info=True)
            raise HTTPException(
                status_code=500, 
                detail="Database error occurred while creating user."
            )
        except Exception as e:
            logger.error(f"Unexpected error creating user with workspace: {e}", exc_info=True)
            raise HTTPException(
                status_code=500, 
                detail="Failed to create user and workspace."
            )

user_manager = UserManager()