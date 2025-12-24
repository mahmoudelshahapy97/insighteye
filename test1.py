import asyncio
import cv2
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import signal
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

# Create thread pool for blocking operations
thread_pool = ThreadPoolExecutor(max_workers=4)


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


async def _validate_stream_source(source: str, timeout: int = 10) -> bool:
    """Validate stream source before processing with configurable timeout"""
    
    def _check_source():
        test_cap = None
        try:
            # Check if it's a file
            if not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and not source.isdigit():
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
            
            # Use environment variables to set FFmpeg timeout (more reliable)
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = f'rtsp_transport;udp|timeout;{timeout * 1000000}'
            
            # Quick validation with OpenCV
            test_cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
            
            # Configure timeout for network streams
            if source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                test_cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout * 1000)
                test_cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
            
            if not test_cap.isOpened():
                logging.error(f"Cannot open video source: {source}")
                return False
            
            # Try to read one frame
            ret, frame = test_cap.read()
            
            if not ret or frame is None:
                logging.error(f"Cannot read from video source: {source}")
                return False
            
            logging.info(f"Successfully validated source: {source}")
            return True
            
        except Exception as e:
            logging.error(f"Error validating source {source}: {e}")
            return False
        finally:
            if test_cap is not None:
                test_cap.release()
    
    loop = asyncio.get_event_loop()
    
    try:
        # Submit to thread pool and wait with timeout
        future = loop.run_in_executor(thread_pool, _check_source)
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        logging.error(f"Validation timeout ({timeout}s) for source: {source}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during validation: {e}")
        return False


async def _validate_with_retry(source: str, max_retries: int = 3, timeout: int = 8) -> bool:
    """Validate with retry logic"""
    for attempt in range(max_retries):
        logging.info(f"Validation attempt {attempt + 1}/{max_retries} for: {source}")
        
        if await _validate_stream_source(source, timeout=timeout):
            return True
        
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logging.warning(f"Validation attempt {attempt + 1} failed, retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
    
    logging.error(f"All validation attempts failed for: {source}")
    return False


async def test_rtsp_detailed(source: str):
    """Detailed RTSP stream testing"""
    print(f"\n{'='*60}")
    print(f"Testing RTSP Stream: {source}")
    print(f"{'='*60}\n")
    
    # Extract host info
    import re
    match = re.search(r'@([\d\.]+):(\d+)', source)
    if match:
        host, port = match.group(1), match.group(2)
        print(f"📡 Host: {host}")
        print(f"🔌 Port: {port}\n")
    
    # Test 1: Ping/Network connectivity
    print("Test 1: Network Connectivity Check")
    print("-" * 40)
    if _check_rtsp_connectivity(source, timeout=5):
        print("✓ TCP connection successful\n")
    else:
        print("❌ TCP connection failed - camera may be offline\n")
        return False
    
    # Test 2: Quick validation (short timeout)
    print("Test 2: Quick Stream Validation (8s timeout)")
    print("-" * 40)
    result = await _validate_stream_source(source, timeout=8)
    
    if result:
        print("✓ Stream validation successful\n")
        return True
    else:
        print("❌ Quick validation failed\n")
    
    # Test 3: Retry with backoff
    print("Test 3: Retry with Exponential Backoff")
    print("-" * 40)
    result = await _validate_with_retry(source, max_retries=2, timeout=8)
    
    print(f"\n{'='*60}")
    if result:
        print("✅ STREAM IS VALID AND ACCESSIBLE")
    else:
        print("❌ STREAM VALIDATION FAILED")
        print("\nPossible issues:")
        print("  • Camera is offline or unreachable")
        print("  • Wrong credentials (admin:12345678)")
        print("  • RTSP path incorrect (/ch01/0)")
        print("  • Firewall blocking RTSP (port 554)")
        print("  • Camera requires specific RTSP transport (TCP vs UDP)")
    print(f"{'='*60}\n")
    
    return result


async def main():
    """Main async function"""
    source = "rtsp://admin:12345678@41.178.2.61:554/ch12/0"
    
    # Run detailed test
    await test_rtsp_detailed(source)
    
    # Optional: Try alternative RTSP transports
    print("\n" + "="*60)
    print("Alternative RTSP URLs to Try:")
    print("="*60)
    print("• TCP transport: rtsp://admin:12345678@41.178.2.61:554/ch12/0?tcp")
    print("• Main stream:   rtsp://admin:12345678@41.178.2.61:554/Streaming/Channels/101")
    print("• Sub stream:    rtsp://admin:12345678@41.178.2.61:554/Streaming/Channels/102")
    print("• Channel 1:     rtsp://admin:12345678@41.178.2.61:554/cam/realmonitor?channel=1&subtype=0")
    print("="*60 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    finally:
        thread_pool.shutdown(wait=False)
        print("🧹 Cleanup complete")