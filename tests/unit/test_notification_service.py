"""
Unit tests for Notification Service
Tests creation and retrieval of notifications.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4

@pytest.fixture
def notification_service(mock_db_manager):
    """Create NotificationService instance with mocked dependencies"""
    with patch('app.services.notification_service.db_manager', mock_db_manager):
        from app.services.notification_service import NotificationService
        service = NotificationService()
        yield service

class TestNotificationCreation:
    """Test notification creation functionality"""
    
    @pytest.mark.asyncio
    async def test_create_notification(self, notification_service, mock_db_manager):
        """Test creating a new notification"""
        workspace_id = uuid4()
        user_id = uuid4()
        status = "info"
        message = "Test message"
        
        mock_db_manager.execute_query.return_value = {
            "notification_id": uuid4(),
            "workspace_id": workspace_id,
            "user_id": user_id,
            "status": status,
            "message": message
        }
        
        result = await notification_service.create_notification(
            workspace_id=workspace_id,
            user_id=user_id,
            status=status,
            message=message
        )
        
        assert result is not None
        assert mock_db_manager.execute_query.called

class TestNotificationRetrieval:
    """Test notification retrieval functionality"""
    
    @pytest.mark.asyncio
    async def test_get_user_notifications(self, notification_service, mock_db_manager):
        """Test getting user notifications"""
        user_id = uuid4()
        
        mock_db_manager.execute_query.return_value = [{
            "notification_id": uuid4(),
            "message": "Test"
        }]
        
        results = await notification_service.get_user_notifications(user_id)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert mock_db_manager.execute_query.called

    @pytest.mark.asyncio
    async def test_get_unread_count(self, notification_service, mock_db_manager):
        """Test getting unread count"""
        user_id = uuid4()
        mock_db_manager.execute_query.return_value = {"count": 5}
        
        count = await notification_service.get_unread_count(user_id)
        assert count == 5
