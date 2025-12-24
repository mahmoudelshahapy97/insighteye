import asyncio
import cv2
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

# Create thread pool for blocking operations
thread_pool = ThreadPoolExecutor(max_workers=4)


async def _validate_stream_source(source: str, timeout: int = 10) -> bool:
    """Validate stream source before processing with configurable timeout"""
    loop = asyncio.get_event_loop()
    
    def _check_source():
        try:
            # Check if it's a file
            if not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and not source.isdigit():
                import os
                if not os.path.exists(source):
                    logging.error(f"Video file does not exist: {source}")
                    return False
                
                if not os.access(source, os.R_OK):
                    logging.error(f"Video file is not readable: {source}")
                    return False
                
                file_size = os.path.getsize(source)
                if file_size == 0:
                    logging.error(f"Video file is empty: {source}")
                    return False
            
            # Quick validation with OpenCV - set timeout options
            test_cap = cv2.VideoCapture(source)
            
            # Configure shorter timeout for network streams
            if source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                test_cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout * 1000)
                test_cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
            
            if not test_cap.isOpened():
                logging.error(f"Cannot open video source: {source}")
                test_cap.release()
                return False
            
            # Try to read one frame
            ret, frame = test_cap.read()
            test_cap.release()
            
            if not ret or frame is None:
                logging.error(f"Cannot read from video source: {source}")
                return False
            
            logging.info(f"Successfully validated source: {source}")
            return True
            
        except Exception as e:
            logging.error(f"Error validating source {source}: {e}")
            return False
    
    try:
        # Add asyncio timeout wrapper
        return await asyncio.wait_for(
            loop.run_in_executor(thread_pool, _check_source),
            timeout=timeout + 2  # Slightly longer than OpenCV timeout
        )
    except asyncio.TimeoutError:
        logging.error(f"Validation timeout for source: {source}")
        return False


async def _validate_with_retry(source: str, max_retries: int = 3) -> bool:
    """Validate with retry logic"""
    for attempt in range(max_retries):
        logging.info(f"Validation attempt {attempt + 1}/{max_retries} for: {source}")
        
        if await _validate_stream_source(source, timeout=10):
            return True
        
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logging.warning(f"Validation attempt {attempt + 1} failed, retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
    
    logging.error(f"All validation attempts failed for: {source}")
    return False


def _check_rtsp_connectivity(source: str, timeout: int = 5) -> bool:
    """Quick network check before full validation"""
    import socket
    import re
    
    match = re.search(r'@([\d\.]+):(\d+)', source)
    if match:
        host, port = match.groups()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            
            if result == 0:
                logging.info(f"Network connectivity OK: {host}:{port}")
                return True
            else:
                logging.error(f"Cannot connect to {host}:{port} (error code: {result})")
                return False
        except Exception as e:
            logging.error(f"Connectivity check failed: {e}")
            return False
    
    logging.warning("Could not parse host/port from source, skipping connectivity check")
    return True


async def main():
    """Main async function"""
    source = "rtsp://admin:12345678@41.178.2.61:554/ch01/0"
    
    print(f"\n{'='*60}")
    print(f"Testing RTSP Stream Validation")
    print(f"{'='*60}\n")
    
    # Step 1: Quick connectivity check
    print("Step 1: Checking network connectivity...")
    if not _check_rtsp_connectivity(source):
        print("❌ Network connectivity check failed. Camera may be offline.")
        return
    print("✓ Network connectivity OK\n")
    
    # Step 2: Validate with retry
    print("Step 2: Validating stream with retry logic...")
    is_valid = await _validate_with_retry(source, max_retries=3)
    
    print(f"\n{'='*60}")
    if is_valid:
        print("✓ Stream validation SUCCESSFUL")
    else:
        print("❌ Stream validation FAILED")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Run the async main function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    finally:
        # Cleanup thread pool
        thread_pool.shutdown(wait=True)
        print("Cleanup complete")