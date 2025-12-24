# shared_stream.py
import time
import logging
import os
import queue
import random
import cv2
import numpy as np
import threading 
from typing import Dict, Optional, Any

class SharedVideoStream:
    """Shared video stream to avoid multiple file handles for the same source"""
    
    def __init__(self, source: str, max_subscribers: int = 10):
        self.source = source
        self.is_rtsp = source.startswith('rtsp://')
        self.subscribers: Dict[str, Dict[str, Any]] = {}
        self.cap: Optional[cv2.VideoCapture] = None
        self.latest_frame: Optional[np.ndarray] = None
        self.is_running = False
        self.lock = threading.RLock()
        self.frame_available = threading.Event()
        self.max_subscribers = max_subscribers
        self.capture_thread: Optional[threading.Thread] = None
        self.stop_capture = threading.Event()
        self.frame_queue = queue.Queue(maxsize=3)
        self.last_frame_time = time.time()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Enhanced error tracking
        self.frame_count = 0
        self.last_error = None
        self.error_count = 0
        self.consecutive_failures = 0
        self.last_successful_read = time.time()
        
        # File validation
        self.is_file_source = self._is_file_source(source)
        self.file_exists = self._validate_file_source(source) if self.is_file_source else True
        
        logging.info(f"Created SharedVideoStream for source: {source} (file: {self.is_file_source}, exists: {self.file_exists})")

    def _get_opencv_capture_options(self) -> str:
        """Get OpenCV capture options optimized for source type"""
        if self.is_rtsp:
            return (
                'rtsp_transport;tcp|'      # TCP is more reliable than UDP
                'timeout;30000000|'         # 30 second total timeout
                'stimeout;5000000|'         # 5 second socket timeout
                'max_delay;500000|'         # 500ms max delay
                'reorder_queue_size;10|'    # Small reorder queue
                'buffer_size;1024000'       # 1MB buffer
            )
        return ''

    def add_subscriber(self, stream_id: str, callback_info: Dict[str, Any] = None) -> bool:
        """Add a subscriber to this shared stream"""
        with self.lock:
            if len(self.subscribers) >= self.max_subscribers:
                logging.warning(f"Max subscribers ({self.max_subscribers}) reached for {self.source}")
                return False
                
            self.subscribers[stream_id] = {
                'added_at': time.time(),
                'frames_received': 0,
                'last_frame_time': None,
                'callback_info': callback_info or {}
            }
            
            logging.info(f"Added subscriber {stream_id} to {self.source}. Total subscribers: {len(self.subscribers)}")
            
            # Start capture if this is the first subscriber
            if len(self.subscribers) == 1 and not self.is_running:
                self._start_capture()
            
            return True
    
    def remove_subscriber(self, stream_id: str):
        """Remove a subscriber from this shared stream"""
        with self.lock:
            if stream_id in self.subscribers:
                subscriber_info = self.subscribers.pop(stream_id)
                logging.info(f"Removed subscriber {stream_id} from {self.source}. "
                           f"Frames received: {subscriber_info.get('frames_received', 0)}")
            
            # Stop capture if no more subscribers
            if not self.subscribers and self.is_running:
                self._stop_capture()
    
    def get_latest_frame(self, stream_id: str) -> Optional[np.ndarray]:
        """Get the latest frame for a specific subscriber"""
        with self.lock:
            if stream_id not in self.subscribers:
                return None
                
            if self.latest_frame is not None:
                self.subscribers[stream_id]['frames_received'] += 1
                self.subscribers[stream_id]['last_frame_time'] = time.time()
                return self.latest_frame.copy()
            
            return None
    
    def wait_for_frame(self, timeout: float = 1.0) -> bool:
        """Wait for a new frame to be available"""
        return self.frame_available.wait(timeout)
    
    def _start_capture(self):
        """Start the video capture thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.stop_capture.clear()
        self.capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.capture_thread.start()
        logging.info(f"Started capture thread for {self.source}")
    
    def _stop_capture(self):
        """FIXED: Stop the video capture thread with proper cleanup order"""
        if not self.is_running:
            return
        
        logging.info(f"Stopping capture for {self.source}")
        
        # CRITICAL FIX #1: Set flags BEFORE releasing cap
        self.is_running = False
        self.stop_capture.set()
        
        # CRITICAL FIX #2: Release VideoCapture FIRST to unblock read()
        # This is the key - the thread is blocked on cap.read(), so we need to
        # release the capture to unblock it BEFORE trying to join the thread
        if self.cap:
            try:
                logging.debug(f"Releasing VideoCapture for {self.source}")
                self.cap.release()
                self.cap = None
                # Give the thread a moment to detect the released capture
                time.sleep(0.2)
            except Exception as e:
                logging.error(f"Error releasing VideoCapture for {self.source}: {e}")
        
        # CRITICAL FIX #3: Now join the thread with appropriate timeout
        if self.capture_thread and self.capture_thread.is_alive():
            # For RTSP streams, use longer timeout as network operations may take time
            timeout = 10.0 if not self.is_file_source else 5.0
            
            logging.debug(f"Waiting for capture thread to stop (timeout: {timeout}s)")
            self.capture_thread.join(timeout=timeout)
            
            if self.capture_thread.is_alive():
                # Thread still alive - this is now very rare with our fix
                logging.error(f"Capture thread for {self.source} did not stop within {timeout}s timeout. "
                            f"Thread may be stuck. Continuing anyway.")
                # In Python, we can't force-kill threads, but at least we've released resources
            else:
                logging.info(f"Capture thread for {self.source} stopped cleanly")
        
        self.capture_thread = None
        logging.info(f"Stop capture complete for {self.source}")

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for this shared stream"""
        with self.lock:
            return {
                'source': self.source,
                'subscriber_count': len(self.subscribers),
                'is_running': self.is_running,
                'frame_count': self.frame_count,
                'last_frame_time': self.last_frame_time,
                'reconnect_attempts': self.reconnect_attempts,
                'last_error': self.last_error,
                'error_count': self.error_count,
                'subscribers': {
                    stream_id: {
                        'frames_received': info['frames_received'],
                        'last_frame_time': info['last_frame_time']
                    }
                    for stream_id, info in self.subscribers.items()
                }
            }

    def _is_file_source(self, source: str) -> bool:
        """Check if source is a file path vs stream URL"""
        return (not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and 
                not source.isdigit())  # Not a camera index
    
    def _validate_file_source(self, source: str) -> bool:
        """Validate that file source exists and is readable"""
        try:
            import os
            if not os.path.exists(source):
                logging.error(f"Video file does not exist: {source}")
                return False
            
            if not os.access(source, os.R_OK):
                logging.error(f"Video file is not readable: {source}")
                return False
            
            # Check file size
            file_size = os.path.getsize(source)
            if file_size == 0:
                logging.error(f"Video file is empty: {source}")
                return False
            
            logging.info(f"Video file validated: {source} ({file_size} bytes)")
            return True
            
        except Exception as e:
            logging.error(f"Error validating video file {source}: {e}")
            return False

    def _capture_loop(self):
        """FIXED: Enhanced capture loop with better stop detection"""
        logging.info(f"Capture loop started for {self.source}")
        
        # For file sources, get video info
        video_duration = None
        video_fps = None
        total_frames = None
        
        if self._is_file_source(self.source):
            try:
                test_cap = cv2.VideoCapture(self.source)
                if test_cap.isOpened():
                    video_fps = test_cap.get(cv2.CAP_PROP_FPS)
                    total_frames = int(test_cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    video_duration = total_frames / video_fps if video_fps > 0 else None
                    logging.info(f"Video info for {self.source}: {total_frames} frames, {video_fps:.1f} FPS, {video_duration:.1f}s duration")
                test_cap.release()
            except Exception as e:
                logging.warning(f"Could not get video info for {self.source}: {e}")
        
        try:
            while not self.stop_capture.is_set() and self.is_running:
                try:
                    # CRITICAL FIX: Check stop condition BEFORE blocking operations
                    if self.stop_capture.is_set() or not self.is_running:
                        logging.debug(f"Stop requested for {self.source}, breaking loop")
                        break
                    
                    # Open video source if needed
                    if self.cap is None or not self.cap.isOpened():
                        if not self._open_video_source():
                            if self.stop_capture.is_set():
                                break
                            delay = min(10.0, 2.0 * (2 ** min(self.reconnect_attempts, 3)))
                            logging.warning(f"Failed to open {self.source}, retrying in {delay:.1f}s")
                            
                            # CRITICAL FIX: Use interruptible sleep
                            if self.stop_capture.wait(delay):
                                logging.debug("Stop signal received during retry delay")
                                break
                            
                            self.reconnect_attempts += 1
                            continue
                    
                    # CRITICAL FIX: Double-check before read
                    if not self.cap or not self.cap.isOpened():
                        logging.warning(f"VideoCapture not valid for {self.source}")
                        time.sleep(0.1)
                        continue
                    
                    # Read frame with timeout awareness
                    # Note: cv2.VideoCapture.read() is blocking and can't be interrupted
                    # but our fix is to release() the cap from another thread
                    ret, frame = self.cap.read()
                    
                    # CRITICAL FIX: Immediately check if we were stopped
                    if self.stop_capture.is_set() or not self.is_running:
                        logging.debug(f"Stop detected after read for {self.source}")
                        break
                    
                    if not ret or frame is None:
                        # Check if cap was released (expected during shutdown)
                        if not self.cap or not self.cap.isOpened():
                            logging.debug(f"VideoCapture released for {self.source}, stopping")
                            break
                        
                        self._handle_read_failure()
                        continue
                    
                    # Validate frame quality
                    if frame.size == 0 or len(frame.shape) != 3:
                        logging.warning(f"Invalid frame received from {self.source}")
                        self._handle_read_failure()
                        continue
                    
                    # Successfully read frame
                    self.reconnect_attempts = 0
                    self.error_count = 0
                    self.consecutive_failures = 0
                    self.frame_count += 1
                    self.last_frame_time = time.time()
                    self.last_successful_read = time.time()
                    
                    # Update latest frame
                    with self.lock:
                        self.latest_frame = frame.copy()
                        self.frame_available.set()
                        self.frame_available.clear()
                    
                    # Frame rate control based on video type
                    if self._is_file_source(self.source) and video_fps and video_fps > 0:
                        target_fps = min(video_fps, 30.0)
                        sleep_time = 1.0 / target_fps
                    else:
                        sleep_time = 0.033  # ~30 FPS
                    
                    # CRITICAL FIX: Use interruptible sleep
                    if self.stop_capture.wait(sleep_time):
                        logging.debug("Stop signal received during frame delay")
                        break
                    
                except Exception as e:
                    if self.stop_capture.is_set() or not self.is_running:
                        logging.debug(f"Stop detected during exception handling for {self.source}")
                        break
                    
                    self.last_error = str(e)
                    self.error_count += 1
                    self.consecutive_failures += 1
                    logging.error(f"Error in capture loop for {self.source}: {e}")
                    
                    if self.consecutive_failures > 20:
                        logging.error(f"Too many consecutive failures for {self.source}, stopping")
                        break
                    
                    # CRITICAL FIX: Interruptible error recovery sleep
                    if self.stop_capture.wait(1.0):
                        break
        
        except Exception as e:
            logging.error(f"Fatal error in capture loop for {self.source}: {e}", exc_info=True)
        
        finally:
            # Final cleanup
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            self.is_running = False
            logging.info(f"Capture loop ended for {self.source}")
    
    def _open_video_source(self) -> bool:
        """FIXED: Enhanced video source opening with proper RTSP handling"""
        try:
            if self.cap:
                self.cap.release()
                time.sleep(0.2)
            
            # Validate file sources
            if self._is_file_source(self.source):
                if not self._validate_file_source(self.source):
                    return False
            
            # Set FFmpeg environment options BEFORE opening capture
            if self.is_rtsp:
                os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = self._get_opencv_capture_options()
                logging.info(f"Opening RTSP stream with TCP transport: {self.source}")
            
            # Determine backends to try
            if self.is_rtsp:
                # For RTSP, try FFmpeg first (best RTSP support)
                backends_to_try = [cv2.CAP_FFMPEG]
            elif self._is_file_source(self.source):
                backends_to_try = [cv2.CAP_FFMPEG, cv2.CAP_ANY]
            else:
                backends_to_try = [cv2.CAP_ANY]
            
            for i, backend in enumerate(backends_to_try):
                try:
                    # Check stop condition
                    if self.stop_capture.is_set():
                        return False
                    
                    logging.info(f"Attempting to open {self.source} with backend {backend}")
                    
                    self.cap = cv2.VideoCapture(self.source, backend)
                    
                    if not self.cap or not self.cap.isOpened():
                        if self.cap:
                            self.cap.release()
                        logging.warning(f"Failed to open with backend {backend}")
                        continue
                    
                    # RTSP-specific settings
                    if self.is_rtsp:
                        # Small buffer to reduce latency
                        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                        # Set timeout properties (if supported)
                        try:
                            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 30000)
                            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 30000)
                        except:
                            pass  # These properties may not be supported
                    else:
                        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                    
                    # Test read with longer timeout for RTSP
                    test_timeout = 30.0 if self.is_rtsp else 10.0
                    logging.info(f"Testing frame read (timeout: {test_timeout}s)...")
                    
                    ret, test_frame = self.cap.read()
                    
                    if not ret or test_frame is None or test_frame.size == 0:
                        logging.warning(f"Failed to read test frame from {self.source}")
                        self.cap.release()
                        self.cap = None
                        continue
                    
                    # For file sources, reset to start
                    if self._is_file_source(self.source):
                        self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    
                    # Success!
                    frame_info = f"({test_frame.shape[1]}x{test_frame.shape[0]})"
                    logging.info(f"✓ Successfully opened {self.source} with backend {backend} {frame_info}")
                    return True
                    
                except Exception as e:
                    logging.error(f"Error with backend {backend}: {e}")
                    if self.cap:
                        self.cap.release()
                        self.cap = None
                    
                    # Don't continue if stop requested
                    if self.stop_capture.is_set():
                        return False
                    
                    continue
            
            logging.error(f"Failed to open {self.source} with all backends")
            return False
            
        except Exception as e:
            logging.error(f"Critical error opening {self.source}: {e}")
            if self.cap:
                self.cap.release()
                self.cap = None
            return False
    
    def _handle_read_failure(self):
        """FIXED: Enhanced read failure handling with RTSP-specific logic"""
        self.consecutive_failures += 1
        
        # Special handling for file sources (video looping)
        if self._is_file_source(self.source) and self.cap and self.cap.isOpened():
            try:
                current_pos = int(self.cap.get(cv2.CAP_PROP_POS_FRAMES))
                total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                
                if total_frames > 0 and current_pos >= total_frames - 1:
                    logging.info(f"End of video file, looping: {self.source}")
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    self.consecutive_failures = 0
                    return
            except Exception as e:
                logging.warning(f"Error checking video position: {e}")
        
        # RTSP-specific handling
        if self.is_rtsp:
            # For RTSP, be more aggressive with reconnection
            if self.consecutive_failures >= 3:  # Reconnect after 3 failures
                logging.warning(f"RTSP stream unstable ({self.consecutive_failures} failures), forcing reconnect")
                if self.cap:
                    self.cap.release()
                    self.cap = None
                self.reconnect_attempts += 1
                
                # Exponential backoff for RTSP
                delay = min(30.0, 2.0 * (1.5 ** (self.reconnect_attempts - 1)))
                logging.info(f"Waiting {delay:.1f}s before RTSP reconnect...")
                
                if self.stop_capture.wait(delay):
                    return
                
                self.consecutive_failures = 0
                return
        
        # General failure handling
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logging.error(f"Max reconnect attempts reached for {self.source}")
            self.is_running = False
            return
        
        # Calculate delay
        base_delay = 2.0 if self.is_rtsp else 1.0
        max_delay = 30.0 if self.is_rtsp else 10.0
        delay = min(max_delay, base_delay * (1.5 ** (self.reconnect_attempts - 1)))
        
        logging.warning(f"Read failure for {self.source}, attempt {self.reconnect_attempts}. "
                       f"Retrying in {delay:.1f}s")
        
        if self.stop_capture.wait(delay):
            return
        
        # Force re-open
        if self.cap:
            self.cap.release()
            self.cap = None

class VideoFileManager:
    """Manages shared video streams to prevent file conflicts"""
    
    def __init__(self):
        self.shared_streams: Dict[str, SharedVideoStream] = {}
        self.lock = threading.RLock()
    
    def get_shared_stream(self, source: str, max_subscribers: int = 10) -> SharedVideoStream:
        """Get or create a shared stream for a source"""
        with self.lock:
            if source not in self.shared_streams:
                self.shared_streams[source] = SharedVideoStream(source, max_subscribers)
            return self.shared_streams[source]
    
    def remove_shared_stream(self, source: str):
        """Remove a shared stream"""
        with self.lock:
            if source in self.shared_streams:
                shared_stream = self.shared_streams[source]
                if shared_stream.is_running:
                    shared_stream._stop_capture()
                del self.shared_streams[source]
                logging.info(f"Removed shared stream for {source}")
    
    def cleanup_empty_streams(self):
        """Clean up streams with no subscribers"""
        with self.lock:
            empty_sources = []
            for source, stream in self.shared_streams.items():
                if not stream.subscribers:
                    empty_sources.append(source)
            
            for source in empty_sources:
                self.remove_shared_stream(source)
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all shared streams"""
        with self.lock:
            return {
                source: stream.get_stats()
                for source, stream in self.shared_streams.items()
            }
