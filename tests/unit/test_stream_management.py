
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
from datetime import datetime
from zoneinfo import ZoneInfo
from app.services.camera_service import CameraService
from app.services.stream_service import StreamManager
from app.schemas import StreamCreate, StreamUpdate

@pytest.fixture
def camera_service(mock_db_manager):
    with patch('app.services.camera_service.db_manager', mock_db_manager):
        with patch('app.services.camera_service.user_manager') as mock_user_mgr:
            with patch('app.services.camera_service.postgres_service') as mock_pg:
                service = CameraService()
                service.user_manager = mock_user_mgr
                service.postgres_service = mock_pg
                yield service

@pytest.fixture
def stream_manager(mock_db_manager):
    with patch('app.services.stream_service.db_manager', mock_db_manager):
         # Mock all the dependencies
        with patch('app.services.stream_service.video_file_manager'), \
             patch('app.services.stream_service.retry_service'), \
             patch('app.services.stream_service.fire_detection_service'), \
             patch('app.services.stream_service.people_count_service'), \
             patch('app.services.stream_service.notification_service'), \
             patch('app.services.stream_service.video_stream_service'), \
             patch('app.services.stream_service.parameter_service'), \
             patch('app.services.stream_service.workspace_service'), \
             patch('app.services.stream_service.stream_processing_service'):
                
                manager = StreamManager()
                # Override max limits for testing
                manager.max_concurrent_streams = 100
                manager.max_streams_per_workspace = 10
                yield manager

@pytest.mark.asyncio
async def test_create_camera(camera_service, mock_db_manager):
    user_id = uuid4()
    workspace_id = uuid4()
    username = "testuser"
    
    stream_data = StreamCreate(
        name="Test Cam",
        path="rtsp://test",
        type="rtsp",
        location="Main Hall"
    )
    
    # Mock user details for limit check
    # Use AsyncMock for async method calls
    camera_service.user_manager.get_user_by_id = AsyncMock(return_value={
        "count_of_camera": 5, "role": "user"
    })
    
    # Mock limit check count query
    mock_db_manager.execute_query.side_effect = [
        {'stream_count': 0}, # check limit
        None # insert
    ]
    
    stream_id = await camera_service.create_camera(
        stream_data, user_id, workspace_id, username
    )
    
    assert stream_id is not None
    assert mock_db_manager.execute_query.call_count >= 2

@pytest.mark.asyncio
async def test_update_camera(camera_service, mock_db_manager):
    stream_id = uuid4()
    user_id = uuid4()
    workspace_id = uuid4()
    
    update_data = StreamUpdate(id=str(stream_id), name="Updated Name")
    
    # Mock get existing stream
    mock_db_manager.execute_query.side_effect = [
        {'user_id': user_id, 'workspace_id': workspace_id}, # get info
        {'row_count': 1} # update
    ]
    
    success, error = await camera_service.update_camera(
        update_data, user_id, "user"
    )
    
    assert success is True
    assert error is None

@pytest.mark.asyncio
async def test_can_start_stream_quota_check(stream_manager, mock_db_manager):
    workspace_id = uuid4()
    user_id = uuid4()
    
    # Mock limits
    mock_db_manager.execute_query.side_effect = [
        { # get_workspace_stream_limits (first query)
            'total_camera_limit': 10,
            'active_members': 1,
            'subscribed_members': 1
        },
        {'count': 0}, # get_workspace_stream_limits (second query - active count)
        { # user_info
            'is_active': True,
            'is_subscribed': True,
            'role': 'user',
            'count_of_camera': 5
        },
        {'count': 0} # user_active_query
    ]
    
    can_start, msg = await stream_manager.can_start_stream_in_workspace(workspace_id, user_id)
    
    assert can_start is True
    assert msg == "OK"

@pytest.mark.asyncio
async def test_can_start_stream_quota_exceeded(stream_manager, mock_db_manager):
    workspace_id = uuid4()
    user_id = uuid4()
    
    # Mock limits - NO available slots
    mock_db_manager.execute_query.side_effect = [
        { 
            'total_camera_limit': 5,
            'active_members': 1,
            'subscribed_members': 1
        },
        {'count': 5}, # 5 active streams = 0 slots
    ]
    
    can_start, msg = await stream_manager.can_start_stream_in_workspace(workspace_id, user_id)
    
    assert can_start is False
    assert "Workspace camera quota exhausted" in msg
