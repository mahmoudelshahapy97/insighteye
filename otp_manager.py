# otp_manager.py
import secrets
import logging
from typing import Dict, Tuple, Optional
from config import config
from utils import send_email  
from database import DatabaseManager
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class OTPManager:
    def __init__(self):
        self.rate_limit_seconds = config.get("otp_rate_limit_seconds", 60)
        self.otp_expiration_seconds = config.get("otp_expiration_seconds", 300)  # 5 minutes default
        self.db_manager = DatabaseManager()

    def _hash_otp(self, otp: str) -> str:
        return hashlib.sha256(otp.encode()).hexdigest()

    async def _get_otp_data_from_db(self, email: str, purpose: str = 'login') -> Optional[Dict]:
        query = """
            SELECT email, purpose, otp_hash, created_at, request_count, last_request_at, expires_at
            FROM otps
            WHERE email = $1 AND purpose = $2
        """
        return await self.db_manager.execute_query(query, (email, purpose), fetch_one=True)

    async def is_rate_limited(self, email: str, purpose: str = 'login') -> Tuple[bool, str]:
        otp_data = await self._get_otp_data_from_db(email, purpose)
        current_time_dt = datetime.now(ZoneInfo("Africa/Cairo"))

        if otp_data and otp_data["last_request_at"]:
            last_request_at_dt = otp_data["last_request_at"]
            time_since_last_request = (current_time_dt - last_request_at_dt).total_seconds()

            if time_since_last_request < self.rate_limit_seconds:
                remaining = self.rate_limit_seconds - time_since_last_request
                wait_time = max(1, int(remaining))
                msg = f"OTP request too frequent for {purpose}. Please wait {wait_time} seconds."
                logger.warning(f"Rate limit exceeded for {email}, purpose: {purpose}. {msg}")
                return True, msg
        return False, ""

    async def generate_otp(self, email: str, purpose: str = 'login', length: int = 6, expiration: Optional[int] = None) -> Tuple[bool, str]:
        try:
            if purpose not in ('login', 'password_reset', 'email_verification'):
                return False, f"Invalid purpose: {purpose}. Must be one of 'login', 'password_reset', 'email_verification'."
            if length < 4 or length > 10:
                return False, f"OTP length must be between 4 and 10 digits, got {length}."

            limited, msg = await self.is_rate_limited(email, purpose)
            if limited:
                return False, msg

            otp_value = ''.join(secrets.choice('0123456789') for _ in range(length))
            otp_hash = self._hash_otp(otp_value)

            now_dt = datetime.now(ZoneInfo("Africa/Cairo"))
            expiration_seconds = expiration if expiration is not None else self.otp_expiration_seconds
            expires_at_dt = now_dt + timedelta(seconds=expiration_seconds)

            query = """
                INSERT INTO otps (email, purpose, otp_hash, created_at, request_count, last_request_at, expires_at)
                VALUES ($1, $2, $3, $4, 1, $5, $6)
                ON CONFLICT (email, purpose) DO UPDATE SET
                    otp_hash = EXCLUDED.otp_hash,
                    created_at = EXCLUDED.created_at,
                    request_count = 1,
                    last_request_at = EXCLUDED.last_request_at,
                    expires_at = EXCLUDED.expires_at;
            """
            await self.db_manager.execute_query(query, (email, purpose, otp_hash, now_dt, now_dt, expires_at_dt))

            logger.info(f"Generated and stored OTP hash for email {email}, purpose: {purpose}, expires in {expiration_seconds}s")
            return True, otp_value

        except Exception as e:
            logger.error(f"Error generating OTP for {email}, purpose: {purpose}: {e}", exc_info=True)
            return False, "Failed to generate OTP due to an internal server error."

    async def verify_otp(self, email: str, otp_attempt: str, purpose: str = 'login') -> bool:
        try:
            otp_data = await self._get_otp_data_from_db(email, purpose)

            if not otp_data:
                logger.warning(f"Verification failed: No OTP data for {email}, purpose: {purpose}")
                return False

            stored_otp_hash = otp_data["otp_hash"]
            expires_at_dt = otp_data["expires_at"]

            current_time_dt = datetime.now(ZoneInfo("Africa/Cairo"))
            if current_time_dt > expires_at_dt:
                logger.warning(f"Verification failed: OTP expired for {email}, purpose: {purpose} at {expires_at_dt.isoformat()}")
                await self.delete_otp(email, purpose)
                return False

            attempt_hash = self._hash_otp(otp_attempt)
            is_valid = secrets.compare_digest(stored_otp_hash, attempt_hash)

            if is_valid:
                logger.info(f"OTP verified successfully for email {email}, purpose: {purpose}")
                await self.delete_otp(email, purpose)
                return True
            else:
                logger.warning(f"Verification failed: Invalid OTP for {email}, purpose: {purpose}")
                return False
        except Exception as e:
            logger.error(f"Error verifying OTP for {email}, purpose: {purpose}: {e}", exc_info=True)
            return False

    async def delete_otp(self, email: str, purpose: str = 'login') -> bool:
        try:
            query = "DELETE FROM otps WHERE email = $1 AND purpose = $2"
            rows_affected = await self.db_manager.execute_query(query, (email, purpose), return_rowcount=True)
            if rows_affected and rows_affected > 0:
                logger.info(f"Deleted OTP for {email}, purpose: {purpose} from DB.")
            else:
                logger.debug(f"No OTP found to delete for {email}, purpose: {purpose} in DB.")
            return True
        except Exception as e:
            logger.error(f"Error deleting OTP for {email}, purpose: {purpose} from DB: {e}", exc_info=True)
            return False

    async def send_otp_email(self, email: str, otp: str, purpose: str = 'login', expiration: Optional[int] = None) -> bool:
        try:
            logger.info(f"Preparing to send OTP to {email} for purpose: {purpose}")
            purpose_text = {
                'login': 'login to your account',
                'password_reset': 'reset your password',
                'email_verification': 'verify your email address'
            }.get(purpose, 'complete your request')
            expiration_seconds = expiration if expiration is not None else self.otp_expiration_seconds

            subject = f"Your One-Time Password (OTP) for {purpose_text}"
            body = (
                f"Your OTP to {purpose_text} is: {otp}\n\n"
                f"This OTP will expire in {expiration_seconds // 60} minutes.\n"
                "If you did not request this OTP, please ignore this email."
            )
            html_body = (
                "<html><body>"
                f"<p>Your One-Time Password (OTP) to {purpose_text} is: <strong>{otp}</strong></p>"
                f"<p>This OTP will expire in {expiration_seconds // 60} minutes.</p>"
                "<p>If you did not request this OTP, please ignore this email.</p>"
                "</body></html>"
            )

            success = await send_email(email, subject, body, html_body=html_body)
            if success:
                logger.info(f"OTP email sent successfully to {email} for purpose: {purpose}")
            else:
                logger.error(f"Failed to send OTP email to {email} for purpose: {purpose} (send_email returned False)")
            return success
        except Exception as e:
            logger.error(f"Error sending OTP email to {email} for purpose: {purpose}: {e}", exc_info=True)
            return False
        