# user_manager.py
from typing import Dict, List
from passlib.context import CryptContext
from database import  get_db_connection, execute_db_query
import bcrypt
import uuid
import logging
from fastapi import HTTPException
import re
import pyotp
from datetime import datetime, timezone, timedelta
import json
import hashlib
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

class UserManager:
    def __init__(self):
        self.pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

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
            logger.error(f"Database error: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            cur.close()
            conn.close()

    def get_password_hash(self, password: str) -> str:
        """Generate password hash using Argon2."""
        return self.pwd_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify password against hashed password."""
        return self.pwd_context.verify(plain_password, hashed_password)

    def validate_password_strength(self, password: str) -> bool:
        """
        Validate password strength requirements.
        Returns (is_valid, message)
        """
        # Check minimum length
        if len(password) < 8:
            return False, "Password must be at least 12 characters long"
        
        # Check for uppercase, lowercase, digit and special characters
        if not re.search(r'[A-Z]', password):
            return False, "Password must contain at least one uppercase letter"
        if not re.search(r'[a-z]', password):
            return False, "Password must contain at least one lowercase letter"
        if not re.search(r'[0-9]', password):
            return False, "Password must contain at least one digit"
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character"
        
        # Check against common password list (you'd need to implement this)
        if self._is_common_password(password):
            return False, "Password is too common or has been breached"
        
        return True, "Password does not meet security requirements. It must be at least 8 characters with uppercase, lowercase, and numbers."

    def _is_common_password(self, password: str) -> bool:
        """
        Check if a password is common or has been previously breached.
        Returns True if the password is common/breached, False otherwise.
        """
        # Option 1: Check against a hardcoded list of common passwords
        common_passwords = {
            "password123", "12345678", "qwerty123", "admin1234", 
            "welcome1", "123456789", "password1", "iloveyou", 
            "1234567890", "letmein123", "abc123456", "trustno1",
            "password!", "admin123", "football", "monkey123"
        }
        
        if password.lower() in common_passwords:
            return True
        
        # Option 2: Check against a password dictionary file
        try:
            with open("common_passwords.txt", "r") as file:
                for line in file:
                    if password == line.strip():
                        return True
        except FileNotFoundError:
            # If file doesn't exist, skip this check
            pass
        
        # Option 3: Use HIBP API (Have I Been Pwned) to check for breached passwords
        # This uses k-anonymity so the full password is never sent to the API
        
        
        sha1_password = hashlib.sha1(password.encode()).hexdigest().upper()
        prefix, suffix = sha1_password[:5], sha1_password[5:]
        
        try:
            response = requests.get(f"https://api.pwnedpasswords.com/range/{prefix}")
            if response.status_code == 200:
                hashes = (line.split(':') for line in response.text.splitlines())
                for hash_suffix, count in hashes:
                    if hash_suffix == suffix:
                        return True
        except Exception:
            # If API request fails, skip this check
            pass
        
        return False
                
    async def create_user(self, username, email, password, role='user') -> bool:
        """Creates a new user with the given credentials.
        
        Returns:
            bool: True if user was created successfully, False if username already exists
        """
        # Check if the username already exists
        query = "SELECT * FROM users WHERE username = %s"
        existing_user = self._execute_db_query(query, (username,), fetch_one=True)
        msg = "This username is already taken. Please choose a different one."
        if existing_user:
            return False 
        
        # Validate password strength
        is_valid, msg = self.validate_password_strength(password)
        if not is_valid:
            logger.warning(f"Password validation failed: {msg}")
            raise HTTPException(status_code=409, detail="Password does not meet security requirements. It must be at least 8 characters with uppercase, lowercase, and numbers.")
            return False

        # Hash the password using Argon2
        hashed_password = self.get_password_hash(password)
        
        user_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc)
        subscription_date = created_at + timedelta(days=90)  # 3 months from now
        is_subscribed = True  # Default value as per schema
        is_active=True

        # Insert the new user into the users table with additional fields
        query = """
            INSERT INTO users 
            (user_id, username, email, created_at, is_active, role, subscription_date, is_subscribed, is_search, is_prediction) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # , %s
        self._execute_db_query(query, (
            user_id, 
            username, 
            email, 
            created_at,  
            is_active,  
            role,
            subscription_date,
            is_subscribed,
            True,
            True
        ))

        # Insert password into user_accounts table
        password_query = """
            INSERT INTO user_accounts
            (password_id, user_id, password_hash, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        password_id = str(uuid.uuid4())
        self._execute_db_query(password_query, (
            password_id,
            user_id,
            hashed_password,
            created_at,
            created_at
        ))

        # Log the successful user creation
        self._log_security_event(
            user_id=user_id,
            event_type="user_created", 
            severity="low",
            event_data={"username": username, "email": email}
        )
        
        return True 

    async def verify_user_password(self, username, password) -> bool:
        """Verify user's password and update last_login"""
        # First get the user details
        query = """
            SELECT u.user_id, ua.password_hash, u.is_active 
            FROM users u
            JOIN user_accounts ua ON u.user_id = ua.user_id
            WHERE u.username = %s
        """
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False  # User not found
        
        user_id, hashed_password, is_active = user

        # Check if user is active
        if not is_active:  # is_active column
            return False
        
        # Verify password using Argon2
        is_valid = self.verify_password(password, hashed_password)
        
        if is_valid:
            # Update last_login timestamp
            update_query = "UPDATE users SET last_login = %s WHERE username = %s"
            self._execute_db_query(update_query, (datetime.now(timezone.utc), username))
            
            # Log successful login
            self._log_security_event(
                user_id=user_id,
                event_type="successful_login",
                severity="low",
                event_data={"username": username}
            )
        else:
            # Log failed login attempts
            self._log_security_event(
                user_id=user_id,
                event_type="failed_login",
                severity="medium",
                event_data={"username": username}
            )
            
        return is_valid
    
    async def get_user_by_email(self, email):
        """Gets user details by email address."""
        query = """SELECT user_id, username, email, is_active, is_subscribed, subscription_date, role FROM users WHERE email = %s"""
        user = self._execute_db_query(query, (email,), fetch_one=True)
        
        if user:
            return {
                "user_id": user[0],
                "username": user[1],
                "email": user[2],
                "is_active": user[3],
                "is_subscribed": user[4],
                "subscription_date": user[5],
                "role": user[6]
            }
        
        return None
    
    async def get_user_by_username(self, username):
        """Gets user details by username."""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE username = %s
        """
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if user:
            return {
                "user_id": user[0],
                "username": user[1],
                "email": user[2],
                "created_at": user[3],
                "is_active": user[4],
                "last_login": user[5],
                "role": user[6],
                "is_subscribed": user[7],
                "subscription_date": user[8],
                "count_of_camera": user[9],
                "is_search": user[10],
                "is_prediction": user[11]
            }
        return None
    
    async def get_user_by_id(self, user_id):
        """Gets user details by username."""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE user_id = %s
        """
        user = self._execute_db_query(query, (user_id,), fetch_one=True)
        
        if user:
            return {
                "user_id": user[0],
                "username": user[1],
                "email": user[2],
                "created_at": user[3],
                "is_active": user[4],
                "last_login": user[5],
                "role": user[6],
                "is_subscribed": user[7],
                "subscription_date": user[8],
                "count_of_camera": user[9],
                "is_search": user[10],
                "is_prediction": user[11]
            }
        return None

    async def get_search_status(self, user_id):
        """Gets the search status for a user."""
        query = "SELECT is_search FROM users WHERE user_id = %s"
        result = self._execute_db_query(query, (user_id,), fetch_one=True)
        
        if result:
            return result[0]  # Return the boolean value
        
        return None  # User not found
    
    async def get_prediction_status(self, user_id):
        """Gets the prediction status for a user."""
        query = "SELECT is_prediction FROM users WHERE user_id = %s"
        result = self._execute_db_query(query, (user_id,), fetch_one=True)
        
        if result:
            return result[0]  # Return the boolean value
        
        return None  # User not found
    
    async def update_search_status(self, user_id, is_search):
        """Updates the search status for a user."""
        query = "UPDATE users SET is_search = %s WHERE user_id = %s"
        rows_updated = self._execute_db_query(query, (is_search, user_id), return_rowcount=True)
        
        return rows_updated > 0  # Return True if at least one row was updated
    
    async def update_prediction_status(self, user_id, is_prediction):
        """Updates the prediction status for a user."""
        query = "UPDATE users SET is_prediction = %s WHERE user_id = %s"
        rows_updated = self._execute_db_query(query, (is_prediction, user_id), return_rowcount=True)
        
        return rows_updated > 0  # Return True if at least one row was updated

    async def reset_password(self, username, new_password) -> bool:
        """Reset password for the user
        
        Returns:
            bool: True if password was reset successfully, False if user not found
        """
        query = "SELECT user_id  FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        if not user:
            return False  
        
        user_id = user[0]
        
        # Validate password strength
        is_valid, msg = self.validate_password_strength(new_password)
        if not is_valid:
            logger.warning(f"Password does not meet security requirements. It must be at least 8 characters with uppercase, lowercase, and numbers.: {msg}")
            return False

        # Hash the new password with Argon2
        hashed_password = self.get_password_hash(new_password)

        # Update the password in the user_accounts table
        update_time = datetime.now(timezone.utc)
        query = """
            UPDATE user_accounts 
            SET password_hash = %s, updated_at = %s 
            WHERE user_id = %s
        """
        self._execute_db_query(query, (hashed_password, update_time, user_id))
        
        # Log the password reset
        self._log_security_event(
            user_id=user_id,
            event_type="password_reset",
            severity="medium",
            event_data={"username": username}
        )

        return True
        
    async def verify_credentials(self, username=None, email=None, password=None) -> tuple:
        """
        Verify user's credentials using either username or email
        
        Args:
            username: Optional username
            email: Optional email
            password: Password to verify
        
        Returns:
            tuple: (is_valid, username) - Boolean indicating if credentials are valid and the username
        """
        if not password:
            return False, None
        
        if not username and not email:
            return False, None
        
        # Determine which credential to use
        if username:
            query = """
                SELECT u.user_id, u.username, ua.password_hash, u.is_active
                FROM users u
                JOIN user_accounts ua ON u.user_id = ua.user_id
                WHERE u.username = %s
            """
            user = self._execute_db_query(query, (username,), fetch_one=True)
        else:
            query = """
                SELECT u.user_id, u.username, ua.password_hash, u.is_active
                FROM users u
                JOIN user_accounts ua ON u.user_id = ua.user_id
                WHERE u.email = %s
            """
            user = self._execute_db_query(query, (email,), fetch_one=True)
        
        if not user:
            return False, None  # User not found
        
        user_id = user[0]
        username_from_db = user[1]
        hashed_password = user[2]
        is_active = user[3]

        # Check if user is active
        if not is_active:
            return False, username_from_db
        
        # Verify password using Argon2
        is_valid = self.verify_password(password, hashed_password)
        
        if is_valid:
            # Update last_login timestamp
            update_query = "UPDATE users SET last_login = %s WHERE username = %s"
            self._execute_db_query(update_query, (datetime.now(timezone.utc), username_from_db))
            
            # Log successful login
            self._log_security_event(
                user_id=user_id,
                event_type="successful_login",
                severity="low",
                event_data={"username": username_from_db}
            )
            return True, username_from_db
        else:
            # Log failed login
            self._log_security_event(
                user_id=user_id,
                event_type="failed_login",
                severity="medium",
                event_data={"username": username_from_db}
            )

        return False, username_from_db

    async def delete_user(self, username) -> bool:
        """Deletes a user by username."""
        # First get the user_id for logging
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]
        
        query = "DELETE FROM users WHERE username = %s"
        rows_affected = self._execute_db_query(query, (username,), return_rowcount=True)

        if rows_affected > 0:
            self._log_security_event(
                user_id=None,  # User is deleted, so no user_id
                event_type="user_deleted",
                severity="high",
                event_data={"username": username, "deleted_user_id": user_id}
            )
            return True

        return False

    async def delete_all_users(self) -> int:
        """Deletes all users from the database.  Returns the number of users deleted."""
        query = "DELETE FROM users"
        rows_deleted = self._execute_db_query(query, return_rowcount=True)

        self._log_security_event(
            user_id=None,
            event_type="all_users_deleted",
            severity="critical",
            event_data={"count": rows_deleted}
        )
        
        return rows_deleted

    async def update_user_status(self, username, is_active: bool) -> bool:
        """Update user's active status"""
        # Get user_id first for logging
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]

        query = "UPDATE users SET is_active = %s WHERE username = %s"
        rows_affected = self._execute_db_query(query, (is_active, username), return_rowcount=True)

        if rows_affected > 0:
            status = "activated" if is_active else "deactivated"
            self._log_security_event(
                user_id=user_id,
                event_type=f"user_{status}",
                severity="medium",
                event_data={"username": username}
            )

        return rows_affected > 0

    async def update_user_subscription(self, username, is_subscribed: bool, months: int = 3) -> bool:
        """Update user's subscription status"""
        # Get user_id first for logging
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]
        
        # Calculate new subscription date if subscribed
        subscription_date = None
        if is_subscribed:
            subscription_date = datetime.now(timezone.utc) + timedelta(days=30*months)
            query = """
                UPDATE users 
                SET is_subscribed = %s, subscription_date = %s 
                WHERE username = %s
            """
            rows_affected = self._execute_db_query(
                query, 
                (is_subscribed, subscription_date, username), 
                return_rowcount=True
            )
        else:
            query = "UPDATE users SET is_subscribed = %s WHERE username = %s"
            rows_affected = self._execute_db_query(query, (is_subscribed, username), return_rowcount=True)
        
        if rows_affected > 0:
            status = "subscribed" if is_subscribed else "unsubscribed"
            self._log_security_event(
                user_id=user_id,
                event_type=f"user_{status}",
                severity="low",
                event_data={
                    "username": username,
                    "subscription_date": subscription_date.isoformat() if subscription_date else None
                }
            )

        return rows_affected > 0

    async def update_user_role(self, username, role: str) -> bool:
        """Update user's role"""
        # Validate role
        valid_roles = ['user', 'admin', 'moderator']
        if role not in valid_roles:
            logger.warning(f"Invalid role attempted: {role}")
            return False
            
        # Get user_id first for logging
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]
        
        query = "UPDATE users SET role = %s WHERE username = %s"
        rows_affected = self._execute_db_query(query, (role, username), return_rowcount=True)
        
        if rows_affected > 0:
            self._log_security_event(
                user_id=user_id,
                event_type="user_role_changed",
                severity="high",
                event_data={"username": username, "new_role": role}
            )

        return rows_affected > 0
    
    async def update_camera_count(self, username, count: int) -> bool:
        """Update user's camera count"""
        if count < 0:
            logger.warning(f"Invalid camera count attempted: {count}")
            return False
            
        # Get user_id first for logging
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]
        
        query = "UPDATE users SET count_of_camera = %s WHERE username = %s"
        rows_affected = self._execute_db_query(query, (count, username), return_rowcount=True)
        
        if rows_affected > 0:
            self._log_security_event(
                user_id=user_id,
                event_type="camera_count_updated",
                severity="low",
                event_data={"username": username, "new_count": count}
            )
            
        return rows_affected > 0

    async def get_all_users(self) -> List:
        """Retrieves all users from the database and returns them as a list of dictionaries."""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, is_subscribed, subscription_date, count_of_camera
            FROM users
        """
        # is_subscription, 
        users = self._execute_db_query(query, fetch_all=True)
        
        # Convert the results to a list of dictionaries
        user_list = [
            {
                "user_id": user[0],
                "username": user[1],
                "email": user[2],
                "created_at": user[3],
                "is_active": user[4],
                "last_login": user[5],
                "role": user[6],
                "is_subscribed": user[7],
                "subscription_date": user[8],
                "count_of_camera": user[9]
            }
            for user in users
        ]
        
        return user_list

    async def get_subscribed_users(self) -> List:
        """Retrieves all users with active subscriptions."""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, subscription_date, count_of_camera  
            FROM users
            WHERE is_subscribed = TRUE
        """
        users = self._execute_db_query(query, fetch_all=True)
        
        # Convert the results to a list of dictionaries
        user_list = [
            {
                "user_id": user[0],
                "username": user[1],
                "email": user[2],
                "created_at": user[3],
                "is_active": user[4],
                "last_login": user[5],
                "role": user[6],
                "subscription_date": user[7],
                "count_of_camera": user[8]
            }
            for user in users
        ]
        
        return user_list

    async def change_password(self, username: str, current_password: str, new_password: str) -> bool:
        """Change a user's password with verification of current password"""
        # First verify the current password
        if not await self.verify_user_password(username, current_password):
            return False  # Current password verification failed
            
        # Get user_id
        query = "SELECT user_id FROM users WHERE username = %s"
        user = self._execute_db_query(query, (username,), fetch_one=True)
        
        if not user:
            return False
            
        user_id = user[0]
        
        # Validate new password strength
        is_valid, msg = self.validate_password_strength(new_password)
        if not is_valid:
            logger.warning(f"Password change failed: {msg}")
            return False
            
        # Hash the new password
        hashed_password = self.get_password_hash(new_password)
        
        # Update the password in user_accounts
        update_time = datetime.now(timezone.utc)
        query = """
            UPDATE user_accounts 
            SET password_hash = %s, updated_at = %s 
            WHERE user_id = %s
        """
        rows_affected = self._execute_db_query(query, (hashed_password, update_time, user_id), return_rowcount=True)
        
        if rows_affected > 0:
            self._log_security_event(
                user_id=user_id,
                event_type="password_changed",
                severity="medium",
                event_data={"username": username}
            )
            return True
            
        return False

    def _log_security_event(self, user_id, event_type, severity, event_data):
        """Log security events to the database"""
        query = """
            INSERT INTO security_events 
            (event_id, user_id, event_type, severity, ip_address, event_data, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Get client IP - this would need to be passed from the API layer
        ip_address = None  # This should be replaced with actual IP from request
        
        event_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc)
        
        try:
            self._execute_db_query(query, (
                event_id,
                user_id,
                event_type,
                severity,
                ip_address,
                json.dumps(event_data),
                created_at
            ))
            logger.info(f"Security event logged: {event_type}")
        except Exception as e:
            logger.error(f"Failed to log security event: {e}")

    def generate_totp_secret(self):
        """Generate a new TOTP secret for a user."""
        return pyotp.random_base32()

    def verify_totp_code(self, secret: str, code: str) -> bool:
        """Verify a TOTP code against a user's secret."""
        totp = pyotp.TOTP(secret)
        return totp.verify(code)
