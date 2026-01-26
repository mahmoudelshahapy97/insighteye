
import pytest
import asyncio
from unittest.mock import MagicMock, patch
from uuid import uuid4
from datetime import datetime, timedelta
from app.services.distributed_stream_manager import DistributedStreamManager

@pytest.fixture
def manager(mock_db_manager):
    # Patch the db_manager inside the class instance or module
    with patch('app.services.distributed_stream_manager.db_manager', mock_db_manager):
        mgr = DistributedStreamManager()
        yield mgr

@pytest.mark.asyncio
async def test_claim_available_cameras(manager, mock_db_manager):
    # Setup mock return for claim_query
    stream_id = uuid4()
    mock_db_manager.execute_query.return_value = [{
        'stream_id': stream_id,
        'name': 'Test Cam',
        'is_streaming': True,
        'status': 'processing',
        'locked_by_server': manager.server_id,
        'is_rtsp': True
    }]
    
    claimed = await manager.claim_available_cameras(slots_available=5)
    
    assert len(claimed) == 1
    assert claimed[0]['stream_id'] == stream_id
    assert mock_db_manager.execute_query.called

@pytest.mark.asyncio
async def test_zombie_cleanup(manager, mock_db_manager):
    # Mock locking 1 camera
    stream_id = str(uuid4())
    mock_db_manager.execute_query.return_value = [{
        'stream_id': stream_id,
        'name': 'Zombie Cam',
        'is_streaming': True,
        'status': 'active',
        'last_activity': datetime.now()
    }]
    
    # It is NOT in active_streams (so it is a zombie)
    # We also need to mock stream_manager.active_streams
    with patch('app.services.stream_service.stream_manager') as mock_sm:
        mock_sm._lock = asyncio.Lock()
        mock_sm.active_streams = {} # Empty
        
        zombies = await manager.detect_and_cleanup_zombie_streams()
        
        assert len(zombies) == 1
        assert zombies[0] == stream_id
        # Should attempt recovery (update query)
        assert mock_db_manager.execute_query.call_count >= 2

@pytest.mark.asyncio
async def test_heartbeat_update(manager, mock_db_manager):
    # Add a fake stream to local memory
    sid = str(uuid4())
    manager.active_streams[sid] = {'status': 'running'}
    
    mock_db_manager.execute_query.return_value = [{'stream_id': sid}]
    
    updated = await manager.update_heartbeat_for_owned_cameras()
    
    assert updated == 1
    # Check if update query was called
    args, _ = mock_db_manager.execute_query.call_args
    assert "UPDATE video_stream" in args[0]
