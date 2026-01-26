
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from app.services.session_service import SessionManager
from app.services.otp_service import OTPManager
from app.schemas import TokenPair

@pytest.fixture
def session_manager(mock_db_manager):
    with patch('app.services.session_service.db_manager', mock_db_manager):
        # Patch config values
        with patch('app.services.session_service.config') as mock_config:
            mock_config.access_token_expire_minutes = 15
            mock_config.refresh_token_expire_days = 7
            mock_config.secret_key = "test_secret"
            mock_config.algorithm = "HS256"
            
            manager = SessionManager()
            yield manager

@pytest.fixture
def otp_manager(mock_db_manager):
    with patch('app.services.otp_service.db_manager', mock_db_manager):
        with patch('app.services.otp_service.config') as mock_config:
            mock_config.otp_rate_limit_seconds = 60
            mock_config.otp_expiration_seconds = 300
            
            manager = OTPManager()
            yield manager

@pytest.mark.asyncio
async def test_create_token_pair(session_manager, mock_db_manager):
    user_id = uuid4()
    workspace_id = uuid4()
    
    # Mock execute_query for store_token_pair
    mock_db_manager.execute_query.return_value = None
    
    token_pair = session_manager.create_token_pair(user_id, workspace_id)
    
    assert isinstance(token_pair, TokenPair)
    assert token_pair.access_token is not None
    assert token_pair.refresh_token is not None
    
    # Check if we can verify the token
    token_data = await session_manager.verify_token(token_pair.access_token)
    assert token_data is not None
    assert token_data.user_id == str(user_id)
    assert token_data.workspace_id == str(workspace_id)

@pytest.mark.asyncio
async def test_generate_otp(otp_manager, mock_db_manager):
    email = "test@example.com"
    
    # Mock no existing rate limit
    mock_db_manager.execute_query.return_value = None
    
    success, otp = await otp_manager.generate_otp(email)
    
    assert success is True
    assert len(otp) == 6
    assert otp.isdigit()

@pytest.mark.asyncio
async def test_verify_otp(otp_manager, mock_db_manager):
    email = "test@example.com"
    otp = "123456"
    otp_hash = otp_manager._hash_otp(otp)
    expires_at = datetime.now(ZoneInfo("Africa/Cairo")) + timedelta(minutes=5)
    
    # Mock finding the OTP in DB
    mock_db_manager.execute_query.return_value = {
        "otp_hash": otp_hash,
        "expires_at": expires_at
    }
    
    is_valid = await otp_manager.verify_otp(email, otp)
    
    assert is_valid is True
    # Should call delete after success
    assert mock_db_manager.execute_query.call_count >= 2

@pytest.mark.asyncio
async def test_verify_otp_expired(otp_manager, mock_db_manager):
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
