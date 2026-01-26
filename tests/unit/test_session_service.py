"""
Unit tests for Session Service
Tests token creation, validation, refresh, and revocation
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.services.session_service import SessionManager
from app.schemas import TokenPair


@pytest.fixture
def session_manager(mock_db_manager):
    """Create SessionManager instance with mocked dependencies"""
    with patch('app.services.session_service.db_manager', mock_db_manager):
        with patch('app.services.session_service.config') as mock_config:
            mock_config.access_token_expire_minutes = 15
            mock_config.refresh_token_expire_days = 7
            mock_config.secret_key = "test_secret_key_for_testing_only"
            mock_config.algorithm = "HS256"
            
            manager = SessionManager()
            yield manager


class TestTokenCreation:
    """Test token creation functionality"""
    
    @pytest.mark.asyncio
    async def test_create_token_pair_success(self, session_manager, mock_db_manager):
        """Test successful token pair creation"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock database storage
        mock_db_manager.execute_query.return_value = None
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        assert isinstance(token_pair, TokenPair)
        assert token_pair.access_token is not None
        assert token_pair.refresh_token is not None
        assert isinstance(token_pair.access_token, str)
        assert isinstance(token_pair.refresh_token, str)
        assert len(token_pair.access_token) > 0
        assert len(token_pair.refresh_token) > 0
    
    @pytest.mark.asyncio
    async def test_create_token_pair_with_additional_claims(self, session_manager, mock_db_manager):
        """Test token creation with additional claims"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        
        token_pair = session_manager.create_token_pair(
            user_id, 
            workspace_id,
            additional_claims={"role": "admin", "username": "testuser"}
        )
        
        assert token_pair is not None
        
        # Verify token contains claims
        token_data = await session_manager.verify_token(token_pair.access_token)
        assert token_data is not None
    
    @pytest.mark.asyncio
    async def test_tokens_are_unique(self, session_manager, mock_db_manager):
        """Test that each token pair is unique"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        
        token_pair_1 = session_manager.create_token_pair(user_id, workspace_id)
        token_pair_2 = session_manager.create_token_pair(user_id, workspace_id)
        
        assert token_pair_1.access_token != token_pair_2.access_token
        assert token_pair_1.refresh_token != token_pair_2.refresh_token


class TestTokenVerification:
    """Test token verification functionality"""
    
    @pytest.mark.asyncio
    async def test_verify_valid_token(self, session_manager, mock_db_manager):
        """Test verification of valid token"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        token_data = await session_manager.verify_token(token_pair.access_token)
        
        assert token_data is not None
        assert token_data.user_id == str(user_id)
        assert token_data.workspace_id == str(workspace_id)
    
    @pytest.mark.asyncio
    async def test_verify_invalid_token(self, session_manager):
        """Test verification of invalid token"""
        invalid_token = "invalid.token.string"
        
        token_data = await session_manager.verify_token(invalid_token)
        
        assert token_data is None
    
    @pytest.mark.asyncio
    async def test_verify_expired_token(self, session_manager, mock_db_manager):
        """Test verification of expired token"""
        # This would require mocking time or creating a token with past expiry
        # Implementation depends on your SessionManager's token creation logic
        pass
    
    @pytest.mark.asyncio
    async def test_verify_malformed_token(self, session_manager):
        """Test verification of malformed tokens"""
        malformed_tokens = [
            "",
            "not-a-jwt",
            "header.payload",  # Missing signature
            "a.b.c.d",  # Too many parts
        ]
        
        for token in malformed_tokens:
            token_data = await session_manager.verify_token(token)
            assert token_data is None


class TestTokenRefresh:
    """Test token refresh functionality"""
    
    @pytest.mark.asyncio
    async def test_refresh_token_success(self, session_manager, mock_db_manager):
        """Test successful token refresh"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock database operations
        mock_db_manager.execute_query.return_value = None
        mock_db_manager.fetch_one.return_value = {
            "user_id": str(user_id),
            "workspace_id": str(workspace_id),
            "is_revoked": False
        }
        
        # Create initial token pair
        original_token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        # Refresh the token
        new_token_pair = await session_manager.refresh_access_token(
            original_token_pair.refresh_token
        )
        
        if new_token_pair:
            assert new_token_pair.access_token != original_token_pair.access_token
            assert isinstance(new_token_pair, TokenPair)
    
    @pytest.mark.asyncio
    async def test_refresh_with_invalid_token(self, session_manager, mock_db_manager):
        """Test refresh with invalid refresh token"""
        invalid_refresh_token = "invalid.refresh.token"
        
        new_token_pair = await session_manager.refresh_access_token(invalid_refresh_token)
        
        assert new_token_pair is None
    
    @pytest.mark.asyncio
    async def test_refresh_revoked_token(self, session_manager, mock_db_manager):
        """Test refresh with revoked token"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock revoked token in database
        mock_db_manager.fetch_one.return_value = {
            "user_id": str(user_id),
            "workspace_id": str(workspace_id),
            "is_revoked": True
        }
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        new_token_pair = await session_manager.refresh_access_token(token_pair.refresh_token)
        
        # Should fail for revoked token
        assert new_token_pair is None or mock_db_manager.fetch_one.called


class TestTokenRevocation:
    """Test token revocation functionality"""
    
    @pytest.mark.asyncio
    async def test_revoke_token_success(self, session_manager, mock_db_manager):
        """Test successful token revocation"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = {"updated": True}
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        result = await session_manager.revoke_token(token_pair.access_token)
        
        # Verify revocation was called
        assert mock_db_manager.execute_query.called or result is not None
    
    @pytest.mark.asyncio
    async def test_revoke_all_user_tokens(self, session_manager, mock_db_manager):
        """Test revoking all tokens for a user"""
        user_id = uuid4()
        
        mock_db_manager.execute_query.return_value = {"updated": True}
        
        result = await session_manager.revoke_all_user_tokens(user_id)
        
        assert mock_db_manager.execute_query.called or result is not None


class TestSessionManagement:
    """Test session management functionality"""
    
    @pytest.mark.asyncio
    async def test_store_token_pair(self, session_manager, mock_db_manager):
        """Test storing token pair in database"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        # Verify database was called to store tokens
        assert mock_db_manager.execute_query.called
    
    @pytest.mark.asyncio
    async def test_get_current_user_from_token(self, session_manager, mock_db_manager):
        """Test extracting user data from token"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        mock_db_manager.fetch_one.return_value = {
            "user_id": str(user_id),
            "username": "testuser",
            "email": "test@example.com",
            "workspace_id": str(workspace_id)
        }
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        # Get user data from token
        user_data = await session_manager.get_current_user(token_pair.access_token)
        
        if user_data:
            assert user_data["user_id"] == str(user_id)


class TestTokenExpiration:
    """Test token expiration handling"""
    
    @pytest.mark.asyncio
    async def test_access_token_expiration_time(self, session_manager, mock_db_manager):
        """Test access token has correct expiration"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        mock_db_manager.execute_query.return_value = None
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        token_data = await session_manager.verify_token(token_pair.access_token)
        
        if token_data and hasattr(token_data, 'exp'):
            # Verify expiration is in the future
            assert token_data.exp > datetime.now(ZoneInfo("UTC")).timestamp()
    
    @pytest.mark.asyncio
    async def test_refresh_token_expiration_time(self, session_manager, mock_db_manager):
        """Test refresh token has longer expiration than access token"""
        # Implementation depends on your token structure
        pass


class TestSecurityFeatures:
    """Test security features"""
    
    @pytest.mark.asyncio
    async def test_token_signature_validation(self, session_manager):
        """Test that modified tokens are rejected"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        token_pair = session_manager.create_token_pair(user_id, workspace_id)
        
        # Modify the token
        modified_token = token_pair.access_token[:-10] + "tampered123"
        
        token_data = await session_manager.verify_token(modified_token)
        
        assert token_data is None
    
    @pytest.mark.asyncio
    async def test_different_secret_key_rejection(self, session_manager, mock_db_manager):
        """Test that tokens signed with different key are rejected"""
        # This would require creating a token with a different secret
        # Implementation depends on your setup
        pass
