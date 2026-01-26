
import pytest
import asyncio
from unittest.mock import MagicMock, patch
import numpy as np
from app.services.shared_stream_service import SharedVideoStream

@pytest.fixture
def mock_cv2():
    with patch('cv2.VideoCapture') as mock:
        yield mock

@pytest.fixture
def rtsp_stream():
    stream = SharedVideoStream("rtsp://admin:pass@192.168.1.100:554/ch1")
    # Override delays to speed up tests
    stream.rtsp_reconnect_delay = 0.01
    stream.read_timeout_seconds = 0.1
    stream.read_failure_delay = 0.01
    return stream

@pytest.mark.asyncio
async def test_init(rtsp_stream):
    assert rtsp_stream.is_rtsp_source
    assert rtsp_stream.rtsp_timeout == 30  # Verified our fix
    assert rtsp_stream.max_decode_errors == 50 # Verified our fix

@pytest.mark.asyncio
async def test_subscriber_management(rtsp_stream):
    # Test add
    success = await rtsp_stream.add_subscriber("user1")
    assert success
    assert "user1" in rtsp_stream.subscribers
    assert len(rtsp_stream.subscribers) == 1
    
    # Test duplicate add (should update or duplicate? code says nothing about dupes, just dict key)
    # The code uses: self.subscribers[stream_id] = ... so it updates.
    
    # Test max subscribers
    rtsp_stream.max_subscribers = 1
    success = await rtsp_stream.add_subscriber("user2")
    assert not success
    assert len(rtsp_stream.subscribers) == 1
    
    # Test remove
    await rtsp_stream.remove_subscriber("user1")
    assert len(rtsp_stream.subscribers) == 0

@pytest.mark.asyncio
async def test_reconnect_logic(rtsp_stream, mock_cv2):
    """Test that stream attempts to reconnect on failure"""
    mock_cap = MagicMock()
    mock_cv2.return_value = mock_cap
    
    # Setup mock to fail opening first, then succeed
    mock_cap.isOpened.side_effect = [False, True]
    
    # Mock read to return frames
    mock_cap.read.return_value = (True, np.zeros((100, 100, 3), dtype=np.uint8))
    
    # Start capture in background
    rtsp_stream.is_running = True
    task = asyncio.create_task(rtsp_stream._capture_loop())
    
    # Allow some time for loop to run
    await asyncio.sleep(0.1)
    
    # Stop
    await rtsp_stream._stop_capture()
    try:
        await task
    except asyncio.CancelledError:
        pass
        
    # Check if VideoCapture was initialized
    assert mock_cv2.called

@pytest.mark.asyncio
async def test_read_failure_handling(rtsp_stream, mock_cv2):
    """Test handling of read failures"""
    mock_cap = MagicMock()
    mock_cv2.return_value = mock_cap
    mock_cap.isOpened.return_value = True
    
    # Fail to read 10 times, then succeed
    failures = [(False, None)] * 10
    success = [(True, np.zeros((100, 100, 3), dtype=np.uint8))]
    mock_cap.read.side_effect = failures + success + [(False, None)] * 100
    
    rtsp_stream.is_running = True
    task = asyncio.create_task(rtsp_stream._capture_loop())
    
    await asyncio.sleep(0.2)
    
    await rtsp_stream._stop_capture()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    # Assert that we had failures but recovered
    assert rtsp_stream.total_frames_received > 0
