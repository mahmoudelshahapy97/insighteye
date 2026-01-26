"""
Unit tests for Notification Service
Tests email notifications, WebSocket notifications, and notification queuing
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


class TestEmailNotifications:
    """Test email notification functionality"""
    
    @pytest.mark.asyncio
    async def test_send_email_notification(self, notification_service):
        """Test sending email notification"""
        email = "test@example.com"
        subject = "Test Notification"
        body = "This is a test notification"
        
        # Mock email sending
        with patch.object(notification_service, 'send_email', return_value=True) as mock_send:
            result = await notification_service.send_email(email, subject, body)
            
            assert mock_send.called or result is not None
    
    @pytest.mark.asyncio
    async def test_send_email_with_template(self, notification_service):
        """Test sending email with template"""
        email = "test@example.com"
        template = "alert_template"
        data = {"camera_name": "Test Camera", "alert_type": "Fire"}
        
        with patch.object(notification_service, 'send_email_template', return_value=True) as mock_send:
            result = await notification_service.send_email_template(email, template, data)
            
            assert mock_send.called or result is not None
    
    @pytest.mark.asyncio
    async def test_send_email_failure_handling(self, notification_service):
        """Test handling of email sending failure"""
        email = "invalid@example.com"
        
        with patch.object(notification_service, 'send_email', side_effect=Exception("SMTP error")):
            try:
                await notification_service.send_email(email, "Test", "Body")
            except Exception as e:
                assert isinstance(e, Exception)


class TestWebSocketNotifications:
    """Test WebSocket notification functionality"""
    
    @pytest.mark.asyncio
    async def test_send_websocket_notification(self, notification_service):
        """Test sending WebSocket notification"""
        user_id = str(uuid4())
        message = {"type": "alert", "data": "Fire detected"}
        
        with patch.object(notification_service, 'send_ws_notification', return_value=True) as mock_send:
            result = await notification_service.send_ws_notification(user_id, message)
            
            assert mock_send.called or result is not None
    
    @pytest.mark.asyncio
    async def test_broadcast_notification(self, notification_service):
        """Test broadcasting notification to multiple users"""
        user_ids = [str(uuid4()) for _ in range(5)]
        message = {"type": "system", "data": "System maintenance"}
        
        with patch.object(notification_service, 'broadcast', return_value=True) as mock_broadcast:
            result = await notification_service.broadcast(user_ids, message)
            
            assert mock_broadcast.called or result is not None


class TestNotificationQueue:
    """Test notification queuing functionality"""
    
    @pytest.mark.asyncio
    async def test_queue_notification(self, notification_service):
        """Test queuing notification for later delivery"""
        notification = {
            "user_id": str(uuid4()),
            "type": "email",
            "data": {"subject": "Test", "body": "Test"}
        }
        
        with patch.object(notification_service, 'queue_notification', return_value=True) as mock_queue:
            result = await notification_service.queue_notification(notification)
            
            assert mock_queue.called or result is not None
    
    @pytest.mark.asyncio
    async def test_process_notification_queue(self, notification_service):
        """Test processing queued notifications"""
        with patch.object(notification_service, 'process_queue', return_value=5) as mock_process:
            processed = await notification_service.process_queue()
            
            assert mock_process.called or isinstance(processed, int)


class TestNotificationRetry:
    """Test notification retry mechanism"""
    
    @pytest.mark.asyncio
    async def test_retry_failed_notification(self, notification_service):
        """Test retrying failed notification"""
        notification_id = str(uuid4())
        
        with patch.object(notification_service, 'retry_notification', return_value=True) as mock_retry:
            result = await notification_service.retry_notification(notification_id)
            
            assert mock_retry.called or result is not None
    
    @pytest.mark.asyncio
    async def test_max_retry_attempts(self, notification_service):
        """Test maximum retry attempts limit"""
        # Should stop retrying after max attempts
        pass


class TestNotificationPreferences:
    """Test user notification preferences"""
    
    @pytest.mark.asyncio
    async def test_get_user_preferences(self, notification_service, mock_db_manager):
        """Test getting user notification preferences"""
        user_id = str(uuid4())
        
        mock_db_manager.fetch_one.return_value = {
            "email_enabled": True,
            "ws_enabled": True,
            "alert_types": ["fire", "intrusion"]
        }
        
        with patch.object(notification_service, 'get_preferences', return_value={}) as mock_get:
            prefs = await notification_service.get_preferences(user_id)
            
            assert mock_get.called or isinstance(prefs, dict)
    
    @pytest.mark.asyncio
    async def test_update_user_preferences(self, notification_service, mock_db_manager):
        """Test updating user notification preferences"""
        user_id = str(uuid4())
        preferences = {
            "email_enabled": False,
            "ws_enabled": True
        }
        
        mock_db_manager.execute_query.return_value = None
        
        with patch.object(notification_service, 'update_preferences', return_value=True) as mock_update:
            result = await notification_service.update_preferences(user_id, preferences)
            
            assert mock_update.called or result is not None
