# app/auth/token_expiration.py
from datetime import datetime, timedelta
from typing import Dict, Tuple
import logging
from app.config.settings import config

logger = logging.getLogger(__name__)

class TokenExpirationStrategy:
    def __init__(self, 
                access_token_expire_minutes: int = config.get("ACCESS_TOKEN_EXPIRE_MINUTES", 30), 
                refresh_token_expire_days: int = config.get("REFRESH_TOKEN_EXPIRE_DAYS", 7),
                sliding_window: bool = True,
                minimum_remaining_time_percent: float = 0.2):
        """
        Initialize the token expiration strategy.
        
        Args:
            access_token_expire_minutes: Default lifetime for access tokens in minutes
            refresh_token_expire_days: Default lifetime for refresh tokens in days
            sliding_window: If True, extend token lifetimes on usage
            minimum_remaining_time_percent: Percentage of time remaining to trigger auto-refresh
        """
        self.access_token_expire_minutes = access_token_expire_minutes
        self.refresh_token_expire_days = refresh_token_expire_days
        self.sliding_window = sliding_window
        self.minimum_remaining_time_percent = minimum_remaining_time_percent
        
    def get_expiration_times(self) -> Tuple[datetime, datetime]:
        """
        Calculate expiration times for new tokens.
        
        Returns:
            Tuple containing access token expiration and refresh token expiration
        """
        now = datetime.utcnow()
        access_expires = now + timedelta(minutes=self.access_token_expire_minutes)
        refresh_expires = now + timedelta(days=self.refresh_token_expire_days)
        
        return access_expires, refresh_expires
    
    def should_refresh(self, token_data: Dict) -> bool:
        """
        Determine if a token should be automatically refreshed based on remaining time.
        
        Args:
            token_data: Token data including expiration timestamp
            
        Returns:
            True if token should be refreshed, False otherwise
        """
        if not token_data or "exp" not in token_data:
            return False
            
        expiration = datetime.fromtimestamp(token_data["exp"])
        now = datetime.utcnow()
        
        # Calculate the original token duration
        if token_data.get("token_type") == "access":
            original_duration = timedelta(minutes=self.access_token_expire_minutes)
        else:
            original_duration = timedelta(days=self.refresh_token_expire_days)
        
        # Calculate time remaining as a percentage of original duration
        time_elapsed = now - (expiration - original_duration)
        time_remaining = expiration - now
        
        if time_elapsed.total_seconds() <= 0:
            return False  # Token was just created
            
        percent_remaining = time_remaining.total_seconds() / original_duration.total_seconds()
        
        # Return True if token has less than minimum percentage remaining
        return percent_remaining <= self.minimum_remaining_time_percent
        
    def extend_expiration(self, token_data: Dict, token_type: str) -> Dict:
        """
        Extend the expiration time of a token for sliding window functionality.
        
        Args:
            token_data: Token data to update
            token_type: Type of token ('access' or 'refresh')
            
        Returns:
            Updated token data with extended expiration
        """
        if not self.sliding_window:
            return token_data
            
        # Copy the token data
        new_token_data = token_data.copy()
        
        # Set new expiration based on token type
        if token_type == "access":
            new_exp = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        else:
            new_exp = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
            
        new_token_data["exp"] = new_exp.timestamp()
        
        return new_token_data
        
    def is_expired(self, expiration_timestamp: float) -> bool:
        """
        Check if a token is expired.
        
        Args:
            expiration_timestamp: The token's expiration time as a UNIX timestamp
            
        Returns:
            True if the token is expired, False otherwise
        """
        try:
            now = datetime.utcnow().timestamp()
            return now >= expiration_timestamp
        except (TypeError, ValueError):
            return True

token_expiration_strategy = TokenExpirationStrategy()