
import asyncio
import logging
import time
import sys
import os
import numpy as np
from unittest.mock import MagicMock, patch

# Mock cv2 before importing app modules
sys.modules["cv2"] = MagicMock()

# Add app to path
sys.path.append(os.getcwd())

# Mock configuration before importing app modules
with patch.dict(os.environ, {"MAX_LOCAL_STREAMS": "100", "RTSP_TIMEOUT": "5"}):
    from app.services.shared_stream_service import SharedVideoStream, capture_pool

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def simulate_io_heavy_stream(stream_instance, duration_seconds=10):
    """
    Simulates a stream that is I/O bound (like a slow RTSP connection).
    We want to verify that 100 of these don't block the event loop.
    """
    # Mock the cv2.VideoCapture to be slow
    mock_cap = MagicMock()
    mock_cap.isOpened.return_value = True
    
    # Simulate a blocking read that takes 100ms (network latency)
    def blocking_read():
        time.sleep(0.01) # Simulate network mismatch/latency
        # Return a dummy 1080p frame
        return True, np.zeros((1080, 1920, 3), dtype=np.uint8)

    mock_cap.read.side_effect = blocking_read
    stream_instance.cap = mock_cap
    stream_instance.is_running = True
    
    # Start the capture loop (which should use the thread pool)
    task = asyncio.create_task(stream_instance._capture_loop())
    
    return task

async def run_stress_test():
    count = 100
    logger.info(f"🚀 Starting stress test with {count} concurrent slow streams...")
    
    streams = []
    tasks = []
    
    # Create 100 streams
    for i in range(count):
        stream = SharedVideoStream(f"rtsp://mock_stream_{i}")
        await stream.add_subscriber(f"sub_{i}")
        streams.append(stream)
        
    logger.info("✅ Created streams. Starting capture loops...")
    
    start_time = time.time()
    
    # Start all of them
    for stream in streams:
        t = await simulate_io_heavy_stream(stream)
        tasks.append(t)
        
    logger.info("✅ All tasks started. Monitoring for blocking...")
    
    # Monitor for 5 seconds
    # If the thread pool is working, the event loop should remain responsive.
    # If it was blocking, this loop would freeze.
    
    loop_start = time.time()
    checks = 0
    
    try:
        while time.time() - loop_start < 5:
            await asyncio.sleep(0.1)
            checks += 1
            if checks % 10 == 0:
                logger.info(f"💓 Main loop heartbeat {checks/10:.1f}s - System is responsive")
                
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
    finally:
        # Cleanup
        logger.info("🛑 Stopping streams...")
        for stream in streams:
            stream.is_running = False
            stream.stop_capture.set()
        
        # Wait a bit for threads to spin down (mock threads are fast)
        logger.info(f"Test completed. {checks} heartbeats recorded.")
        if checks >= 40:
             logger.info("✅ SUCCESS: Main loop remained responsive under load.")
        else:
             logger.error("❌ FAILURE: Main loop was blocked or sluggish.")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
