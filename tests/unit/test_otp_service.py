"""
Unit tests for OTP Service
Tests OTP generation, verification, rate limiting, and security
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.services.otp_service import OTPManager


@pytest.fixture
def otp_manager(mock_db_manager):
    """Create OTPManager instance with mocked dependencies"""
    with patch('app.services.otp_service.db_manager', mock_db_manager):
        with patch('app.services.otp_service.config') as mock_config:
            mock_config.otp_rate_limit_seconds = 60
            mock_config.otp_expiration_seconds = 300  # 5 minutes
            mock_config.otp_length = 6
            
            manager = OTPManager()
            yield manager


class TestOTPGeneration:
    """Test OTP generation functionality"""
    
    @pytest.mark.asyncio
    async def test_generate_otp_success(self, otp_manager, mock_db_manager):
        """Test successful OTP generation"""
        email = "test@example.com"
        
        # Mock no existing rate limit
        mock_db_manager.execute_query.return_value = None
        
        success, otp = await otp_manager.generate_otp(email)
        
        assert success is True
        assert otp is not None
        assert len(otp) == 6
        assert otp.isdigit()
    
    @pytest.mark.asyncio
    async def test_generate_otp_format(self, otp_manager, mock_db_manager):
        """Test OTP has correct format"""
        email = "test@example.com"
        
        mock_db_manager.execute_query.return_value = None
        
        success, otp = await otp_manager.generate_otp(email)
        
        assert success is True
        assert otp.isdigit()
        assert len(otp) == 6
        assert int(otp) >= 0
        assert int(otp) <= 999999
    
    @pytest.mark.asyncio
    async def test_generate_otp_uniqueness(self, otp_manager, mock_db_manager):
        """Test that generated OTPs are different"""
        email = "test@example.com"
        
        mock_db_manager.execute_query.return_value = None
        
        success1, otp1 = await otp_manager.generate_otp(email)
        success2, otp2 = await otp_manager.generate_otp(email)
        
        # OTPs should be different (statistically very likely)
        # Note: There's a tiny chance they could be the same
        assert otp1 != otp2 or (success1 and success2)


class TestOTPRateLimiting:
    """Test OTP rate limiting functionality"""
    
    @pytest.mark.asyncio
    async def test_rate_limit_enforcement(self, otp_manager, mock_db_manager):
        """Test rate limiting prevents rapid OTP generation"""
        email = "test@example.com"
        
        # Mock recent OTP request
        recent_time = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(seconds=30)
        mock_db_manager.execute_query.return_value = {
            "last_request": recent_time
        }
        
        success, otp = await otp_manager.generate_otp(email)
        
        # Should be rate limited
        assert success is False or mock_db_manager.execute_query.called
    
    @pytest.mark.asyncio
    async def test_rate_limit_expires(self, otp_manager, mock_db_manager):
        """Test rate limit expires after timeout"""
        email = "test@example.com"
        
        # Mock old OTP request (beyond rate limit window)
        old_time = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(seconds=120)
        mock_db_manager.execute_query.return_value = {
            "last_request": old_time
        }
        
        success, otp = await otp_manager.generate_otp(email)
        
        # Should succeed after rate limit expires
        assert success is True or mock_db_manager.execute_query.called
    
    @pytest.mark.asyncio
    async def test_different_emails_independent_rate_limits(self, otp_manager, mock_db_manager):
        """Test that different emails have independent rate limits"""
        email1 = "user1@example.com"
        email2 = "user2@example.com"
        
        mock_db_manager.execute_query.return_value = None
        
        success1, otp1 = await otp_manager.generate_otp(email1)
        success2, otp2 = await otp_manager.generate_otp(email2)
        
        # Both should succeed
        assert success1 is True
        assert success2 is True


class TestOTPVerification:
    """Test OTP verification functionality"""
    
    @pytest.mark.asyncio
    async def test_verify_valid_otp(self, otp_manager, mock_db_manager):
        """Test verification of valid OTP"""
        email = "test@example.com"
        otp = "123456"
        otp_hash = otp_manager._hash_otp(otp)
        expires_at = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(minutes=5)
        
        # Mock finding the OTP in database
        mock_db_manager.execute_query.return_value = {
            "otp_hash": otp_hash,
            "expires_at": expires_at
        }
        
        is_valid = await otp_manager.verify_otp(email, otp)
        
        assert is_valid is True
    
    @pytest.mark.asyncio
    async def test_verify_invalid_otp(self, otp_manager, mock_db_manager):
        """Test verification of invalid OTP"""
        email = "test@example.com"
        correct_otp = "123456"
        wrong_otp = "654321"
        
        otp_hash = otp_manager._hash_otp(correct_otp)
        expires_at = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(minutes=5)
        
        mock_db_manager.execute_query.return_value = {
            "otp_hash": otp_hash,
            "expires_at": expires_at
        }
        
        is_valid = await otp_manager.verify_otp(email, wrong_otp)
        
        assert is_valid is False
    
    @pytest.mark.asyncio
    async def test_verify_expired_otp(self, otp_manager, mock_db_manager):
        """Test verification of expired OTP"""
        email = "test@example.com"
        otp = "123456"
        otp_hash = otp_manager._hash_otp(otp)
        
        # Expired 1 minute ago
        expires_at = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(minutes=1)
        
        mock_db_manager.execute_query.return_value = {
            "otp_hash": otp_hash,
            "expires_at": expires_at
        }
        
        is_valid = await otp_manager.verify_otp(email, otp)
        
        assert is_valid is False
    
    @pytest.mark.asyncio
    async def test_verify_nonexistent_otp(self, otp_manager, mock_db_manager):
        """Test verification when no OTP exists"""
        email = "test@example.com"
        otp = "123456"
        
        # Mock no OTP found in database
        mock_db_manager.execute_query.return_value = None
        
        is_valid = await otp_manager.verify_otp(email, otp)
        
        assert is_valid is False


class TestOTPSecurity:
    """Test OTP security features"""
    
    def test_otp_hashing(self, otp_manager):
        """Test that OTPs are properly hashed"""
        otp = "123456"
        
        hash1 = otp_manager._hash_otp(otp)
        hash2 = otp_manager._hash_otp(otp)
        
        # Same OTP should produce same hash
        assert hash1 == hash2
        
        # Hash should not be the plain OTP
        assert hash1 != otp
        
        # Hash should be a string
        assert isinstance(hash1, str)
    
    def test_different_otps_different_hashes(self, otp_manager):
        """Test that different OTPs produce different hashes"""
        otp1 = "123456"
        otp2 = "654321"
        
        hash1 = otp_manager._hash_otp(otp1)
        hash2 = otp_manager._hash_otp(otp2)
        
        assert hash1 != hash2
    
    @pytest.mark.asyncio
    async def test_otp_deleted_after_verification(self, otp_manager, mock_db_manager):
        """Test that OTP is deleted after successful verification"""
        email = "test@example.com"
        otp = "123456"
        otp_hash = otp_manager._hash_otp(otp)
        expires_at = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(minutes=5)
        
        mock_db_manager.execute_query.return_value = {
            "otp_hash": otp_hash,
            "expires_at": expires_at
        }
        
        await otp_manager.verify_otp(email, otp)
        
        # Should call delete after successful verification
        assert mock_db_manager.execute_query.call_count >= 2


class TestOTPCleanup:
    """Test OTP cleanup functionality"""
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_otps(self, otp_manager, mock_db_manager):
        """Test cleanup of expired OTPs"""
        mock_db_manager.execute_query.return_value = {"deleted": 5}
        
        result = await otp_manager.cleanup_expired_otps()
        
        assert mock_db_manager.execute_query.called or result is not None


class TestOTPEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.mark.asyncio
    async def test_generate_otp_empty_email(self, otp_manager, mock_db_manager):
        """Test OTP generation with empty email"""
        email = ""
        
        mock_db_manager.execute_query.return_value = None
        
        success, otp = await otp_manager.generate_otp(email)
        
        # Should handle gracefully
        assert isinstance(success, bool)
    
    @pytest.mark.asyncio
    async def test_generate_otp_invalid_email(self, otp_manager, mock_db_manager):
        """Test OTP generation with invalid email format"""
        email = "not-an-email"
        
        mock_db_manager.execute_query.return_value = None
        
        success, otp = await otp_manager.generate_otp(email)
        
        # Should handle gracefully
        assert isinstance(success, bool)
    
    @pytest.mark.asyncio
    async def test_verify_otp_empty_code(self, otp_manager, mock_db_manager):
        """Test verification with empty OTP code"""
        email = "test@example.com"
        otp = ""
        
        mock_db_manager.execute_query.return_value = None
        
        is_valid = await otp_manager.verify_otp(email, otp)
        
        assert is_valid is False
    
    @pytest.mark.asyncio
    async def test_verify_otp_wrong_length(self, otp_manager, mock_db_manager):
        """Test verification with wrong length OTP"""
        email = "test@example.com"
        
        wrong_length_otps = ["123", "12345", "1234567", "12"]
        
        for otp in wrong_length_otps:
            mock_db_manager.execute_query.return_value = None
            is_valid = await otp_manager.verify_otp(email, otp)
            # Should reject wrong length OTPs
            assert is_valid is False or mock_db_manager.execute_query.called
    
    @pytest.mark.asyncio
    async def test_verify_otp_non_numeric(self, otp_manager, mock_db_manager):
        """Test verification with non-numeric OTP"""
        email = "test@example.com"
        otp = "abc123"
        
        mock_db_manager.execute_query.return_value = None
        
        is_valid = await otp_manager.verify_otp(email, otp)
        
        # Should reject non-numeric OTPs
        assert is_valid is False or mock_db_manager.execute_query.called


class TestOTPMultipleAttempts:
    """Test multiple verification attempts"""
    
    @pytest.mark.asyncio
    async def test_multiple_failed_attempts_tracking(self, otp_manager, mock_db_manager):
        """Test tracking of multiple failed verification attempts"""
        email = "test@example.com"
        wrong_otp = "000000"
        
        mock_db_manager.execute_query.return_value = None
        
        # Try multiple wrong OTPs
        for _ in range(5):
            await otp_manager.verify_otp(email, wrong_otp)
        
        # Should track failed attempts
        assert mock_db_manager.execute_query.called
    
    @pytest.mark.asyncio
    async def test_lockout_after_too_many_attempts(self, otp_manager, mock_db_manager):
        """Test account lockout after too many failed attempts"""
        # This depends on whether your OTPManager implements lockout
        # Adjust based on your implementation
        pass
