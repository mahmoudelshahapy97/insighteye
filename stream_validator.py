# stream_validator.py - Enhanced RTSP validation and connection handling

import asyncio
import socket
import re
import logging
import subprocess
from typing import Optional, Dict, Any
import time

class StreamValidator:
    """Enhanced stream validator with robust RTSP handling"""
    
    @staticmethod
    def parse_rtsp_url(url: str) -> Optional[Dict[str, str]]:
        """Parse RTSP URL to extract host, port, and credentials"""
        try:
            # Pattern: rtsp://[username:password@]host:port/path
            pattern = r'rtsp://(?:([^:]+):([^@]+)@)?([^:/]+):?(\d+)?/(.+)'
            match = re.match(pattern, url)
            
            if match:
                username, password, host, port, path = match.groups()
                return {
                    'username': username,
                    'password': password,
                    'host': host,
                    'port': port or '554',  # Default RTSP port
                    'path': path
                }
        except Exception as e:
            logging.error(f"Error parsing RTSP URL: {e}")
        return None
    
    @staticmethod
    async def check_network_connectivity(host: str, port: int, timeout: float = 5.0) -> bool:
        """Check if we can reach the RTSP server"""
        try:
            loop = asyncio.get_event_loop()
            
            def _connect():
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                try:
                    result = sock.connect_ex((host, port))
                    return result == 0
                finally:
                    sock.close()
            
            result = await loop.run_in_executor(None, _connect)
            
            if result:
                logging.info(f"✓ Network connectivity OK: {host}:{port}")
            else:
                logging.error(f"✗ Cannot reach {host}:{port}")
            
            return result
            
        except Exception as e:
            logging.error(f"Network connectivity check failed for {host}:{port}: {e}")
            return False
    
    @staticmethod
    async def test_rtsp_with_ffprobe(url: str, timeout: float = 15.0) -> bool:
        """Use ffprobe to test RTSP stream validity"""
        try:
            # Sanitize URL for logging (hide credentials)
            sanitized_url = re.sub(r'://([^:]+):([^@]+)@', r'://***:***@', url)
            logging.info(f"Testing RTSP stream with ffprobe: {sanitized_url}")
            
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-rtsp_transport', 'tcp',  # Use TCP instead of UDP
                '-timeout', str(int(timeout * 1000000)),  # microseconds
                '-i', url,
                '-show_entries', 'stream=codec_type',
                '-of', 'default=noprint_wrappers=1'
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout + 5.0
                )
                
                if process.returncode == 0 and b'codec_type' in stdout:
                    logging.info(f"✓ ffprobe validation successful for {sanitized_url}")
                    return True
                else:
                    error_msg = stderr.decode('utf-8', errors='ignore')
                    logging.error(f"✗ ffprobe validation failed: {error_msg[:200]}")
                    return False
                    
            except asyncio.TimeoutError:
                logging.error(f"✗ ffprobe timeout after {timeout}s")
                process.kill()
                return False
                
        except FileNotFoundError:
            logging.warning("ffprobe not found, skipping validation")
            return True  # Continue anyway if ffprobe is not available
        except Exception as e:
            logging.error(f"Error in ffprobe validation: {e}")
            return False
    
    @staticmethod
    async def validate_rtsp_stream(url: str, timeout: float = 30.0) -> bool:
        """Comprehensive RTSP stream validation"""
        
        # Parse URL
        parsed = StreamValidator.parse_rtsp_url(url)
        if not parsed:
            logging.error(f"Failed to parse RTSP URL: {url}")
            return False
        
        host = parsed['host']
        port = int(parsed['port'])
        
        # Step 1: Check network connectivity
        logging.info(f"Step 1: Checking network connectivity to {host}:{port}")
        if not await StreamValidator.check_network_connectivity(host, port, timeout=5.0):
            logging.error(f"❌ Network connectivity failed for {host}:{port}")
            return False
        
        # Step 2: Test with ffprobe (if available)
        logging.info("Step 2: Testing stream with ffprobe")
        if not await StreamValidator.test_rtsp_with_ffprobe(url, timeout=15.0):
            logging.warning("ffprobe test failed, will try OpenCV anyway")
        
        # Step 3: Test with OpenCV (final validation)
        logging.info("Step 3: Testing stream with OpenCV")
        return await StreamValidator._validate_with_opencv(url, timeout)
    
    @staticmethod
    async def _validate_with_opencv(url: str, timeout: float) -> bool:
        """Validate stream using OpenCV with proper timeout handling"""
        import cv2
        import os
        
        def _test_capture():
            # Set FFmpeg options for RTSP
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
                'rtsp_transport;tcp|'  # Use TCP transport
                f'timeout;{int(timeout * 1000000)}|'  # Timeout in microseconds
                'stimeout;5000000|'  # Socket timeout: 5 seconds
                'max_delay;500000'  # Max delay: 500ms
            )
            
            cap = None
            try:
                # Try with FFmpeg backend first
                cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
                
                if not cap or not cap.isOpened():
                    logging.error("Failed to open video capture")
                    return False
                
                # Set additional properties
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                
                # Try to read a frame
                ret, frame = cap.read()
                
                if not ret or frame is None or frame.size == 0:
                    logging.error("Failed to read frame from stream")
                    return False
                
                logging.info(f"✓ Successfully validated stream with OpenCV (frame size: {frame.shape})")
                return True
                
            except Exception as e:
                logging.error(f"OpenCV validation error: {e}")
                return False
            finally:
                if cap:
                    cap.release()
        
        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, _test_capture),
                timeout=timeout
            )
            return result
        except asyncio.TimeoutError:
            logging.error(f"OpenCV validation timeout after {timeout}s")
            return False
        except Exception as e:
            logging.error(f"Unexpected error in OpenCV validation: {e}")
            return False


# Integration into SharedVideoStream class
class RTSPConnectionManager:
    """Manages RTSP connections with automatic retry and recovery"""
    
    def __init__(self, source: str):
        self.source = source
        self.validator = StreamValidator()
        self.connection_attempts = 0
        self.max_connection_attempts = 5
        self.base_retry_delay = 2.0
        self.max_retry_delay = 30.0
    
    async def validate_and_prepare(self) -> bool:
        """Validate RTSP stream before attempting to use it"""
        logging.info(f"Validating RTSP stream: {self.source}")
        
        for attempt in range(1, self.max_connection_attempts + 1):
            logging.info(f"Validation attempt {attempt}/{self.max_connection_attempts}")
            
            if await self.validator.validate_rtsp_stream(self.source, timeout=30.0):
                logging.info(f"✓ RTSP stream validated successfully on attempt {attempt}")
                return True
            
            if attempt < self.max_connection_attempts:
                # Calculate retry delay with exponential backoff
                delay = min(
                    self.max_retry_delay,
                    self.base_retry_delay * (2 ** (attempt - 1))
                )
                logging.warning(
                    f"Validation failed, retrying in {delay:.1f}s "
                    f"(attempt {attempt}/{self.max_connection_attempts})"
                )
                await asyncio.sleep(delay)
        
        logging.error(f"❌ Failed to validate RTSP stream after {self.max_connection_attempts} attempts")
        return False
    
    def get_opencv_options(self) -> str:
        """Get optimized OpenCV options for RTSP"""
        return (
            'rtsp_transport;tcp|'  # Use TCP for reliability
            'timeout;30000000|'     # 30 second timeout
            'stimeout;5000000|'     # 5 second socket timeout
            'max_delay;500000|'     # 500ms max delay
            'reorder_queue_size;10|'  # Reorder queue size
            'buffer_size;1024000'   # 1MB buffer
        )


# Usage in your stream processing
async def enhanced_stream_validation_example(source: str):
    """Example of how to use the enhanced validation"""
    
    # For RTSP streams, use the connection manager
    if source.startswith('rtsp://'):
        manager = RTSPConnectionManager(source)
        
        if not await manager.validate_and_prepare():
            raise RuntimeError(f"Failed to validate RTSP stream: {source}")
        
        # Set OpenCV environment variables
        import os
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = manager.get_opencv_options()
        
        logging.info("✓ RTSP stream ready for processing")
    
    # For file sources, use existing validation
    else:
        validator = StreamValidator()
        # ... existing file validation logic
