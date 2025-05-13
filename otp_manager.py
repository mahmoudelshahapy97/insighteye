import secrets
import time
import logging
from typing import Dict
from config import config
from utils import send_email

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

class OTPManager:
    def __init__(self):
        self.otps: Dict[str, Dict[str, str]] = {}
        self.request_timestamps: Dict[str, float] = {}
        self.rate_limit_seconds = 60 

    async def is_rate_limited(self, email: str) -> bool:
        """
        Check if a request is rate limited.
        
        Args:
            email: The email address to check
            limit_seconds: Minimum seconds between requests
            
        Returns:
            bool: True if rate limited, False otherwise
        """
        current_time = time.time()
        last_request_time = self.request_timestamps.get(email)
        
        if last_request_time and (current_time - last_request_time < self.rate_limit_seconds):
        # if email in self.request_timestamps and (current_time - self.request_timestamps[email] < self.rate_limit_seconds):
            # Calculate remaining time
            remaining = self.rate_limit_seconds - (current_time - last_request_time)
            logger.warning(f"Rate limit exceeded for {email}. Try again in {remaining:.1f} seconds.")

            return True
            
        return False
    
    async def update_rate_limit(self, email: str) -> None:
        """Update rate limit timestamp for an email"""
        self.request_timestamps[email] = time.time()

    async def generate_otp(self, email, length) -> str:
        """Generates a secure OTP and stores it."""
        try:
            # Rate limiting: Prevent multiple requests within 30 seconds
            if await self.is_rate_limited(email):
                # Calculate remaining time for the error message
                current_time = time.time()
                last_request_time = self.request_timestamps.get(email, current_time) # Should exist if rate limited
                remaining = self.rate_limit_seconds - (current_time - last_request_time)
                wait_time = max(1, int(remaining)) # Ensure at least 1 second is shown

                return False, f"OTP request too frequent. Please wait {wait_time} seconds before requesting again."
            
            # Generate a secure OTP
            otp = ''.join(secrets.choice('0123456789') for _ in range(length))
            
            current_time = time.time()
            # Store OTP with timestamp
            self.otps[email] = {
                'otp': otp,
                'timestamp': current_time
            }
            # Update rate limit
            await self.update_rate_limit(email)

            logging.info(f"Generated OTP for email {email}")
            return otp

        except Exception as e:
            logger.error(f"Error generating OTP for {email}: {str(e)}", exc_info=True)
            return False, "Failed to generate OTP due to an internal error."
    
    async def verify_otp(self, email, otp, expiration: int = 600) -> bool:
        """Verify an OTP for the given email."""
        try:
            stored_otp_data = None

            # Check if OTP exists for email
            if email not in self.otps:
                logging.warning(f"Verification attempt failed: No OTP found for email {email}")
                return False
            
            stored_otp_data = self.otps[email]
            
            # Check OTP expiration (default 10 minutes)
            current_time = time.time()
            if current_time - stored_otp_data['timestamp'] > expiration:
                logging.warning(f"Verification attempt failed: OTP expired for email {email}")
                del self.otps[email]
                return False
            

            # Verify OTP (constant-time comparison to prevent timing attacks)
            stored_otp = stored_otp_data['otp']
            is_valid = secrets.compare_digest(stored_otp, otp)
            
            # Clear OTP after verification
            if is_valid:
                logging.info(f"OTP verified successfully for email {email}")
                await self.delete_otp(email)
                return True
            else:
                logging.warning(f"Verification attempt failed: Invalid OTP provided for email {email}")
                return False

        except Exception as e:
            logger.error(f"Error verifying OTP for {email}: {str(e)}", exc_info=True)
            return False
    
    async def delete_otp(self, email):
        """Delete an OTP for a specific email address."""
        try:
            if email in self.otps:
                del self.otps[email]
                logger.info(f"Deleted OTP for {email}.")
            else:
                 logger.info(f"Attempted to delete OTP for {email}, but none was found.")\

            logger.info(f"Deleted OTP for email {email}")
            return True
        except Exception as e:
            logger.error(f"Error deleting OTP for {email}: {str(e)}", exc_info=True)
            return False

    async def send_otp_email(self, email, otp):
        """Sends an OTP to the specified email address."""
        try:
                
            logging.info(f"Preparing to send OTP {otp} to {email}")
            
            sender_email = config.get("otp_sender_email", "your-email@gmail.com")
            sender_password = config.get("smtp_password", "your-app-password")

            subject = "Your One-Time Password (OTP)"
            body = f"""
            Your OTP is: {otp}
            
            This OTP will expire in 10 minutes.
            If you did not request this OTP, please ignore this email.
            """
            
            success = await send_email(email, subject, body)

            if success:
                logger.info(f"OTP email sent successfully to {email}")
            else:
                logger.error(f"Failed to send OTP email to {email}")
                
            return success
        except Exception as e:
            logger.error(f"Error sending OTP email: {str(e)}")
            return False
