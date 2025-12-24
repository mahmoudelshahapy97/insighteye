import asyncio
import cv2
import logging
from concurrent.futures import ThreadPoolExecutor
import os
import re

# Configure logging
logger = logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

# Thread pool for blocking operations
thread_pool = ThreadPoolExecutor(max_workers=4)


class StreamValidator:
    """Handle stream validation with proper timeout and retry logic"""
    
    @staticmethod
    def check_rtsp_connectivity(source: str, timeout: int = 5) -> bool:
        """Quick TCP connectivity check"""
        import socket
        
        match = re.search(r'@([\d\.]+):(\d+)', source)
        if match:
            host, port = match.groups()
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((host, int(port)))
                sock.close()
                return result == 0
            except:
                return False
        return True
    
    @staticmethod
    async def validate_stream_source(source: str, timeout: int = 8) -> bool:
        """Validate stream source with configurable timeout"""
        
        def _check_source():
            test_cap = None
            try:
                # File validation
                if not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and not source.isdigit():
                    if not os.path.exists(source):
                        logger.error(f"Video file does not exist: {source}")
                        return False
                    
                    if not os.access(source, os.R_OK):
                        logger.error(f"Video file is not readable: {source}")
                        return False
                    
                    if os.path.getsize(source) == 0:
                        logger.error(f"Video file is empty: {source}")
                        return False
                
                # Set FFmpeg timeout options
                os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = f'rtsp_transport;udp|timeout;{timeout * 1000000}'
                
                # Open with FFmpeg backend
                test_cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
                
                # Configure OpenCV timeouts
                if source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    test_cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout * 1000)
                    test_cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
                
                if not test_cap.isOpened():
                    logger.error(f"Cannot open video source: {source}")
                    return False
                
                # Try to read one frame
                ret, frame = test_cap.read()
                
                if not ret or frame is None:
                    logger.error(f"Cannot read from video source: {source}")
                    return False
                
                return True
                
            except Exception as e:
                logger.error(f"Error validating source {source}: {e}")
                return False
            finally:
                if test_cap is not None:
                    test_cap.release()
        
        loop = asyncio.get_event_loop()
        
        try:
            future = loop.run_in_executor(thread_pool, _check_source)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"Validation timeout ({timeout}s) for source: {source}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during validation: {e}")
            return False
    
    @staticmethod
    async def validate_with_retry(source: str, max_retries: int = 3, timeout: int = 8) -> bool:
        """Validate with retry logic and exponential backoff"""
        
        # Quick connectivity check for RTSP streams
        if source.startswith('rtsp://'):
            if not StreamValidator.check_rtsp_connectivity(source, timeout=5):
                logger.error(f"Network connectivity check failed for: {source}")
                return False
        
        # Retry validation
        for attempt in range(max_retries):
            logger.info(f"Validation attempt {attempt + 1}/{max_retries} for: {source}")
            
            if await StreamValidator.validate_stream_source(source, timeout=timeout):
                logger.info(f"✓ Stream validated successfully: {source}")
                return True
            
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                logger.warning(f"Validation attempt {attempt + 1} failed, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"All validation attempts failed for: {source}")
        return False


# Integration with your existing system
class StreamManager:
    """Your existing stream manager with validation"""
    
    def __init__(self):
        self.validator = StreamValidator()
    
    async def add_stream(self, stream_id: str, source: str) -> bool:
        """Add stream with validation"""
        logger.info(f"Adding stream {stream_id}: {source}")
        
        # Validate before adding
        is_valid = await self.validator.validate_with_retry(source, max_retries=2, timeout=8)
        
        if not is_valid:
            logger.error(f"Failed to validate stream {stream_id}")
            return False
        
        # Your existing stream setup code here...
        logger.info(f"Stream {stream_id} added successfully")
        return True
    
    async def restart_stream(self, stream_id: str, source: str) -> bool:
        """Restart stream with validation"""
        logger.info(f"Restarting stream {stream_id}")
        
        # Validate before restart
        is_valid = await self.validator.validate_stream_source(source, timeout=8)
        
        if not is_valid:
            logger.warning(f"Stream {stream_id} validation failed during restart")
            # Maybe retry after a delay
            await asyncio.sleep(5)
            is_valid = await self.validator.validate_stream_source(source, timeout=8)
        
        if not is_valid:
            logger.error(f"Cannot restart stream {stream_id} - still invalid")
            return False
        
        # Your existing restart logic here...
        logger.info(f"Stream {stream_id} restarted successfully")
        return True


# Example usage
async def main():
    manager = StreamManager()
    
    # Test with your RTSP stream
    source = "rtsp://admin:12345678@41.178.2.61:554/ch01/0"
    success = await manager.add_stream("stream-001", source)
    
    if success:
        print("✅ Stream added and validated successfully!")
    else:
        print("❌ Failed to add stream")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    finally:
        thread_pool.shutdown(wait=False)
        print("🧹 Cleanup complete")