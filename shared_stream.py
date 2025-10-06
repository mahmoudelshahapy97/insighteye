# shared_stream.py
import time
import logging
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
        """Stop the video capture thread"""
        if not self.is_running:
            return
            
        logging.info(f"Stopping capture for {self.source}")
        self.is_running = False
        self.stop_capture.set()
        
        if self.capture_thread and self.capture_thread.is_alive():
            self.capture_thread.join(timeout=5.0)
            if self.capture_thread.is_alive():
                logging.warning(f"Capture thread for {self.source} did not stop within timeout")
        
        if self.cap:
            self.cap.release()
            self.cap = None
        
        self.capture_thread = None
        logging.info(f"Stopped capture for {self.source}")

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

    def _handle_read_failure(self):
        """Enhanced read failure handling with proper video looping"""
        self.consecutive_failures += 1
        
        # Special handling for file sources (like your fire1.mp4)
        if self._is_file_source(self.source) and self.cap and self.cap.isOpened():
            try:
                current_pos = int(self.cap.get(cv2.CAP_PROP_POS_FRAMES))
                total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                
                logging.debug(f"Video position for {self.source}: {current_pos}/{total_frames}")
                
                # Check if we've reached the end of the video
                if total_frames > 0 and (current_pos >= total_frames - 1 or current_pos >= total_frames):
                    logging.info(f"End of video file reached for {self.source} ({current_pos}/{total_frames}), looping to start")
                    
                    # Reset to beginning
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    
                    # Test if reset worked
                    ret, test_frame = self.cap.read()
                    if ret and test_frame is not None:
                        logging.info(f"Successfully looped video {self.source} back to start")
                        # Reset the frame position again for next read
                        self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        # Reset failure counters
                        self.consecutive_failures = 0
                        self.reconnect_attempts = 0
                        return
                    else:
                        logging.warning(f"Failed to read after loop reset for {self.source}")
                
                # If position is valid but read failed, might be a temporary issue
                elif current_pos < total_frames - 5:  # Not near the end
                    logging.warning(f"Read failed mid-video for {self.source} at position {current_pos}")
                    # Don't increment reconnect_attempts for mid-video failures
                    if self.consecutive_failures < 5:
                        return  # Give it another chance without reconnecting
                        
            except Exception as e:
                logging.warning(f"Error checking video position for {self.source}: {e}")
        
        # If we get here, either it's not a file or the loop didn't work
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logging.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached for {self.source}")
            self.is_running = False
            return
        
        # For short videos, use shorter delays to restart quickly
        if self._is_file_source(self.source):
            base_delay = 1.0  # Shorter delay for files
            max_delay = 5.0   # Max 5 seconds for files
        else:
            base_delay = 2.0  # Longer delay for streams
            max_delay = 30.0
        
        delay = min(max_delay, base_delay * (1.5 ** (self.reconnect_attempts - 1)))
        jitter = random.uniform(0.1, 0.5)
        total_delay = delay + jitter
        
        logging.warning(f"Read failure for {self.source}, attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}. "
                    f"Retrying in {total_delay:.1f}s")
        
        time.sleep(total_delay)
        
        # Force re-open the video source
        if self.cap:
            self.cap.release()
            self.cap = None

    def _capture_loop(self):
        """Enhanced capture loop for short video files"""
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
                    # Open video source if needed
                    if self.cap is None or not self.cap.isOpened():
                        if not self._open_video_source():
                            delay = min(10.0, 2.0 * (2 ** min(self.reconnect_attempts, 3)))
                            logging.warning(f"Failed to open {self.source}, retrying in {delay:.1f}s")
                            time.sleep(delay)
                            self.reconnect_attempts += 1
                            continue
                    
                    # Read frame
                    ret, frame = self.cap.read()
                    
                    if not ret or frame is None:
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
                        # For files, respect the original FPS but cap at 30
                        target_fps = min(video_fps, 30.0)
                        sleep_time = 1.0 / target_fps
                    else:
                        # For streams or unknown FPS, use default
                        sleep_time = 0.033  # ~30 FPS
                    
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    self.last_error = str(e)
                    self.error_count += 1
                    self.consecutive_failures += 1
                    logging.error(f"Error in capture loop for {self.source}: {e}")
                    
                    if self.consecutive_failures > 20:  # Reduced threshold
                        logging.error(f"Too many consecutive failures for {self.source}, stopping")
                        break
                    
                    time.sleep(1.0)
        
        except Exception as e:
            logging.error(f"Fatal error in capture loop for {self.source}: {e}", exc_info=True)
        
        finally:
            if self.cap:
                self.cap.release()
                self.cap = None
            self.is_running = False
            logging.info(f"Capture loop ended for {self.source}")

    def _open_video_source(self) -> bool:
        """Enhanced video source opening with multiple backend attempts"""
        try:
            if self.cap:
                self.cap.release()
                time.sleep(0.2)  # Brief pause for cleanup
            
            # Validate file if it's a local file
            if self._is_file_source(self.source):
                if not self._validate_file_source(self.source):
                    return False
            
            # Try different backends in order of preference
            backends_to_try = []
            
            if self._is_file_source(self.source):
                # For file sources - your system supports both CAP_FFMPEG and CAP_ANY
                backends_to_try = [
                    cv2.CAP_FFMPEG,  # Best for video files
                    cv2.CAP_ANY      # Fallback that works
                ]
            else:
                # For stream sources (RTSP, HTTP, etc.)
                backends_to_try = [
                    cv2.CAP_FFMPEG,  # Best for most streaming protocols
                    cv2.CAP_ANY      # Fallback
                ]
            
            for i, backend in enumerate(backends_to_try):
                try:
                    logging.info(f"Attempting to open {self.source} with backend {backend} (attempt {i+1}/{len(backends_to_try)})")
                    
                    self.cap = cv2.VideoCapture(self.source, backend)
                    
                    if not self.cap or not self.cap.isOpened():
                        if self.cap:
                            self.cap.release()
                        continue
                    
                    # Configure buffer size
                    self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                    
                    # Test read to ensure it works
                    ret, test_frame = self.cap.read()
                    if not ret or test_frame is None or test_frame.size == 0:
                        logging.warning(f"Backend {backend} opened but cannot read frames")
                        self.cap.release()
                        self.cap = None
                        continue
                    
                    # Reset to beginning for file sources
                    if self._is_file_source(self.source):
                        self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        
                        # Log video properties
                        total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                        fps = self.cap.get(cv2.CAP_PROP_FPS)
                        width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                        height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                        
                        logging.info(f"Video opened successfully: {width}x{height}, {total_frames} frames, {fps:.2f} FPS")
                    
                    logging.info(f"Successfully opened {self.source} with backend {backend}")
                    return True
                    
                except Exception as e:
                    logging.warning(f"Backend {backend} failed for {self.source}: {e}")
                    if self.cap:
                        self.cap.release()
                        self.cap = None
                    continue
            
            logging.error(f"All backends failed to open {self.source}")
            return False
            
        except Exception as e:
            logging.error(f"Critical error opening {self.source}: {e}", exc_info=True)
            if self.cap:
                self.cap.release()
                self.cap = None
            return False

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
                # Stop the stream if it's running
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
