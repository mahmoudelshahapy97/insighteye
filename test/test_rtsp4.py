import asyncio
import cv2
import logging
from concurrent.futures import ThreadPoolExecutor
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
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
    
    logging.warning("Could not parse host/port from source")
    return True


async def _validate_stream_source(source: str, timeout: int = 10, use_tcp: bool = False) -> bool:
    """Validate stream source with TCP transport option"""
    
    def _check_source():
        test_cap = None
        try:
            # Configure FFmpeg options for RTSP
            if use_tcp:
                # Force TCP transport (more reliable but higher latency)
                os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
                    f'rtsp_transport;tcp|'
                    f'timeout;{timeout * 1000000}|'
                    f'stimeout;{timeout * 1000000}'
                )
                logging.info(f"Using TCP transport for RTSP")
            else:
                # Use UDP (default, lower latency but less reliable)
                os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
                    f'rtsp_transport;udp|'
                    f'timeout;{timeout * 1000000}|'
                    f'stimeout;{timeout * 1000000}'
                )
                logging.info(f"Using UDP transport for RTSP")
            
            test_cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
            
            # Additional timeout settings
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
            
            # Get stream properties
            width = int(test_cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(test_cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = test_cap.get(cv2.CAP_PROP_FPS)
            
            logging.info(f"✓ Stream validated: {width}x{height} @ {fps}fps")
            return True
            
        except Exception as e:
            logging.error(f"Error validating source {source}: {e}")
            return False
        finally:
            if test_cap is not None:
                test_cap.release()
    
    loop = asyncio.get_event_loop()
    
    try:
        future = loop.run_in_executor(thread_pool, _check_source)
        return await asyncio.wait_for(future, timeout=timeout + 2)
    except asyncio.TimeoutError:
        logging.error(f"Validation timeout ({timeout}s) for source: {source}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during validation: {e}")
        return False


async def test_multiple_urls(base_url: str, credentials: str):
    """Test multiple RTSP URL variations"""
    
    # Extract host and port
    import re
    match = re.search(r'@([\d\.]+):(\d+)', base_url)
    if not match:
        print("❌ Could not parse host/port from URL")
        return
    
    host, port = match.group(1), match.group(2)
    
    # Common RTSP path variations for different camera brands
    paths = [
        # Your original path
        "/ch12/0",
        "/ch01/0",
        
        # Hikvision paths
        "/Streaming/Channels/101",  # Main stream
        "/Streaming/Channels/102",  # Sub stream
        "/Streaming/Channels/201",  # Channel 2 main
        
        # Dahua paths
        "/cam/realmonitor?channel=1&subtype=0",  # Main
        "/cam/realmonitor?channel=1&subtype=1",  # Sub
        
        # Generic paths
        "/stream1",
        "/stream2",
        "/live",
        "/h264",
        "/video",
        "/",
    ]
    
    print(f"\n{'='*70}")
    print(f"Testing RTSP Stream: {credentials}@{host}:{port}")
    print(f"{'='*70}\n")
    
    # Test network connectivity first
    test_url = f"rtsp://{credentials}@{host}:{port}/test"
    if not _check_rtsp_connectivity(test_url, timeout=5):
        print("❌ Network connectivity failed. Camera may be offline.\n")
        return
    
    print("✓ Network connectivity OK\n")
    
    # Test each path with both UDP and TCP
    successful_urls = []
    
    for path in paths:
        url = f"rtsp://{credentials}@{host}:{port}{path}"
        
        print(f"\n{'─'*70}")
        print(f"Testing: {path}")
        print(f"{'─'*70}")
        
        # Try UDP first (faster)
        print("  → Trying UDP transport...")
        if await _validate_stream_source(url, timeout=8, use_tcp=False):
            print(f"  ✅ SUCCESS with UDP: {path}")
            successful_urls.append((url, "UDP"))
            continue
        
        # Try TCP if UDP fails
        print("  → Trying TCP transport...")
        if await _validate_stream_source(url, timeout=8, use_tcp=True):
            print(f"  ✅ SUCCESS with TCP: {path}")
            successful_urls.append((url, "TCP"))
            continue
        
        print(f"  ❌ Failed: {path}")
    
    # Summary
    print(f"\n{'='*70}")
    print("RESULTS SUMMARY")
    print(f"{'='*70}\n")
    
    if successful_urls:
        print(f"✅ Found {len(successful_urls)} working URL(s):\n")
        for url, transport in successful_urls:
            print(f"  • {url}")
            print(f"    Transport: {transport}\n")
    else:
        print("❌ No working URLs found.\n")
        print("Troubleshooting steps:")
        print("  1. Verify camera credentials are correct")
        print("  2. Check if RTSP is enabled in camera settings")
        print("  3. Try accessing camera web interface")
        print("  4. Check firewall rules on camera and network")
        print("  5. Try VLC or FFmpeg to test RTSP directly:")
        print(f"     ffmpeg -rtsp_transport tcp -i 'rtsp://{credentials}@{host}:{port}/Streaming/Channels/101' -frames:v 1 test.jpg")
    
    print(f"{'='*70}\n")


async def main():
    """Main async function"""
    
    # Your RTSP camera details
    credentials = "admin:12345678"
    host = "41.178.2.61"
    port = "554"
    
    base_url = f"rtsp://{credentials}@{host}:{port}/"
    
    await test_multiple_urls(base_url, credentials)


if __name__ == "__main__":
    try:
        print("\n🔍 RTSP Stream Scanner")
        print("=" * 70)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    finally:
        thread_pool.shutdown(wait=False)
        print("🧹 Cleanup complete")