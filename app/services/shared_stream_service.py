# app/services/shared_stream_service.py
import time
import logging
import queue
import random
import asyncio
import cv2
import numpy as np
import threading 
import os
from typing import Dict, Optional, Any

class SharedVideoStream:
    """
    Thread-safe shared video stream with single-threaded reading.
    CRITICAL: Only ONE thread reads from cv2.VideoCapture to avoid FFmpeg async_lock errors.
    """
    
    def __init__(self, source: str, max_subscribers: int = 10):
        self.source = source
        self.subscribers: Dict[str, Dict[str, Any]] = {}
        self.cap: Optional[cv2.VideoCapture] = None
        self.latest_frame: Optional[np.ndarray] = None
        self.is_running = False
        
        # CRITICAL: Use RLock to prevent deadlocks, but ensure single-threaded reading
        self.lock = threading.RLock()
        self.frame_available = threading.Event()
        self.max_subscribers = max_subscribers
        self.capture_thread: Optional[threading.Thread] = None
        self.stop_capture = threading.Event()
        
        # REMOVED: frame_queue - causes threading issues
        # self.frame_queue = queue.Queue(maxsize=3)
        
        self.last_frame_time = time.time()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Error tracking
        self.frame_count = 0
        self.last_error = None
        self.error_count = 0
        self.consecutive_failures = 0
        self.last_successful_read = time.time()
        
        # Source type detection
        self.is_file_source = self._is_file_source(source)
        self.is_rtsp_source = self._is_rtsp_source(source)
        self.file_exists = self._validate_file_source(source) if self.is_file_source else True
        
        # RTSP settings
        self.rtsp_timeout = 10
        self.rtsp_reconnect_delay = 1
        
        # CRITICAL: Capture lock to ensure only one read() at a time
        self._capture_lock = threading.Lock()
        
        logging.info(f"Created SharedVideoStream for source: {source} "
                    f"(file: {self.is_file_source}, rtsp: {self.is_rtsp_source})")
    
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
            
            logging.info(f"Added subscriber {stream_id} to {self.source}. Total: {len(self.subscribers)}")
            
            # Start capture if first subscriber
            if len(self.subscribers) == 1 and not self.is_running:
                self._start_capture()
            
            return True
    
    def remove_subscriber(self, stream_id: str):
        """Remove a subscriber from this shared stream"""
        with self.lock:
            if stream_id in self.subscribers:
                subscriber_info = self.subscribers.pop(stream_id)
                logging.info(f"Removed subscriber {stream_id} from {self.source}. "
                           f"Frames: {subscriber_info.get('frames_received', 0)}")
            
            # Stop capture if no subscribers
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
                # Return a copy to avoid race conditions
                return self.latest_frame.copy()
            
            return None
    
    def wait_for_frame(self, timeout: float = 10.0) -> bool:
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

    def _is_file_source(self, source: str) -> bool:
        """Check if source is a file path"""
        return (not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and 
                not source.isdigit())
    
    def _is_rtsp_source(self, source: str) -> bool:
        """Check if source is an RTSP stream"""
        return source.startswith('rtsp://')
    
    def _validate_file_source(self, source: str) -> bool:
        """Validate that file source exists"""
        try:
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
            
            logging.info(f"Video file validated: {source} ({file_size} bytes)")
            return True
            
        except Exception as e:
            logging.error(f"Error validating video file {source}: {e}")
            return False

    def _configure_rtsp_environment(self):
        """Configure environment for RTSP"""
        if not self.is_rtsp_source:
            return
        
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;tcp|'
            f'timeout;{self.rtsp_timeout * 1000000}|'
            f'stimeout;{self.rtsp_timeout * 1000000}|'
            f'max_delay;500000'
        )
        
        # CRITICAL: Disable threading in FFmpeg to avoid async_lock issues
        os.environ['OPENCV_FFMPEG_THREAD_COUNT'] = '1'
        
        logging.info(f"Configured RTSP environment for {self.source}")

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics"""
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

    def _safe_release_capture(self):
        """
        Safely release VideoCapture with proper error handling.
        CRITICAL: Prevents "NoneType has no attribute 'release'" errors.
        """
        if self.cap is None:
            return
        
        try:
            # Acquire lock to ensure thread safety
            with self._capture_lock:
                if self.cap is not None:  # Double-check after acquiring lock
                    try:
                        if self.cap.isOpened():
                            self.cap.release()
                            logging.debug(f"✓ Released VideoCapture for {self.source}")
                        else:
                            logging.debug(f"VideoCapture already closed for {self.source}")
                    except Exception as e:
                        logging.warning(f"Error during cap.release() for {self.source}: {e}")
                    finally:
                        self.cap = None  # Always set to None
        except Exception as e:
            logging.error(f"Error in _safe_release_capture for {self.source}: {e}")
            self.cap = None  # Ensure it's set to None even on error
    
    def _stop_capture(self):
        """
        Stop the video capture thread with safe cleanup.
        FIXED: Proper None checks and error handling.
        """
        if not self.is_running:
            return
        
        logging.info(f"Stopping capture for {self.source}")
        self.is_running = False
        self.stop_capture.set()
        
        # Release capture to unblock read()
        self._safe_release_capture()
        
        # Wait for thread with timeout
        if self.capture_thread and self.capture_thread.is_alive():
            timeout = 10.0 if self.is_rtsp_source else 5.0
            
            logging.debug(f"Waiting for capture thread (timeout: {timeout}s)")
            self.capture_thread.join(timeout=timeout)
            
            if self.capture_thread.is_alive():
                logging.warning(
                    f"⚠️ Capture thread for {self.source} did not stop within {timeout}s. "
                    f"Thread will be abandoned (daemon=True)."
                )
        
        self.capture_thread = None
        logging.info(f"✓ Stopped capture for {self.source}")
    
    def _capture_loop(self):
        """
        Main capture loop with safe cleanup in finally block.
        FIXED: All release() calls now use safe method.
        """
        logging.info(f"Capture loop started for {self.source}")
        
        # Configure RTSP if needed
        if self.is_rtsp_source:
            self._configure_rtsp_environment()
        
        # Get video info
        video_fps = None
        if self.is_file_source:
            try:
                test_cap = cv2.VideoCapture(self.source)
                if test_cap.isOpened():
                    video_fps = test_cap.get(cv2.CAP_PROP_FPS)
                    total_frames = int(test_cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    logging.info(f"Video: {total_frames} frames, {video_fps:.1f} FPS")
                test_cap.release()  # This is safe - we just created it
            except Exception as e:
                logging.warning(f"Could not get video info: {e}")
        
        last_successful_read = time.time()
        read_timeout = 30.0
        
        try:
            while not self.stop_capture.is_set() and self.is_running:
                try:
                    # Check stop signal
                    if self.stop_capture.is_set() or not self.is_running:
                        logging.info(f"Stop signal received for {self.source}")
                        break
                    
                    # Open video source if needed
                    if self.cap is None or not self.cap.isOpened():
                        if not self._open_video_source():
                            delay = self.rtsp_reconnect_delay if self.is_rtsp_source else 2.0
                            logging.warning(f"Failed to open {self.source}, retry in {delay}s")
                            
                            # Sleep with stop checks
                            for _ in range(int(delay * 10)):
                                if self.stop_capture.is_set():
                                    break
                                time.sleep(0.1)
                            
                            self.reconnect_attempts += 1
                            continue
                    
                    # CRITICAL: Thread-safe read with proper None check
                    frame_read_successful = False
                    ret = False
                    frame = None
                    
                    # Acquire lock for reading
                    with self._capture_lock:
                        # Triple-check everything is valid
                        if (not self.stop_capture.is_set() and 
                            self.is_running and 
                            self.cap is not None and
                            self.cap.isOpened()):
                            
                            try:
                                ret, frame = self.cap.read()
                                frame_read_successful = True
                            except Exception as read_error:
                                logging.error(f"Exception during cap.read(): {read_error}")
                                ret = False
                                frame = None
                    
                    # Check stop again after read
                    if self.stop_capture.is_set() or not self.is_running:
                        break
                    
                    if not frame_read_successful or not ret or frame is None:
                        self._handle_read_failure()
                        
                        # Check timeout for RTSP
                        if self.is_rtsp_source:
                            time_since_success = time.time() - last_successful_read
                            if time_since_success > read_timeout:
                                logging.error(f"No frames for {time_since_success:.1f}s")
                                break
                        
                        continue
                    
                    # Validate frame
                    if frame.size == 0 or len(frame.shape) != 3:
                        logging.warning(f"Invalid frame from {self.source}")
                        self._handle_read_failure()
                        continue
                    
                    # Successfully read frame
                    last_successful_read = time.time()
                    self.reconnect_attempts = 0
                    self.consecutive_failures = 0
                    self.frame_count += 1
                    self.last_frame_time = time.time()
                    self.last_successful_read = time.time()
                    
                    # Update latest frame (thread-safe)
                    with self.lock:
                        self.latest_frame = frame
                        self.frame_available.set()
                        self.frame_available.clear()
                    
                    # Frame rate control
                    if self.is_rtsp_source:
                        sleep_time = 0.01
                    elif self.is_file_source and video_fps and video_fps > 0:
                        target_fps = min(video_fps, 30.0)
                        sleep_time = 1.0 / target_fps
                    else:
                        sleep_time = 0.033
                    
                    # Sleep with stop checks
                    if sleep_time > 0.01:
                        for _ in range(int(sleep_time * 100)):
                            if self.stop_capture.is_set():
                                break
                            time.sleep(0.01)
                    else:
                        time.sleep(sleep_time)
                    
                except Exception as e:
                    self.last_error = str(e)
                    self.error_count += 1
                    self.consecutive_failures += 1
                    logging.error(f"Error in capture loop: {e}")
                    
                    max_failures = 10 if self.is_rtsp_source else 20
                    if self.consecutive_failures > max_failures:
                        logging.error(f"Too many failures, stopping")
                        break
                    
                    # Sleep with stop checks
                    for _ in range(10):
                        if self.stop_capture.is_set():
                            break
                        time.sleep(0.1)
        
        except Exception as e:
            logging.error(f"Fatal error in capture loop: {e}", exc_info=True)
        
        finally:
            # CRITICAL: Safe cleanup in finally block
            logging.info(f"Capture loop cleanup starting for {self.source}")
            self._safe_release_capture()
            self.is_running = False
            logging.info(f"✓ Capture loop ended for {self.source}")
    
    def _handle_read_failure(self):
        """
        Handle read failures with safe cleanup.
        FIXED: Use safe release method.
        """
        self.consecutive_failures += 1
        
        # Video looping for files
        if self.is_file_source and self.cap is not None:
            try:
                with self._capture_lock:
                    if self.cap is not None and self.cap.isOpened():
                        current_pos = int(self.cap.get(cv2.CAP_PROP_POS_FRAMES))
                        total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                        
                        # End of video - loop
                        if total_frames > 0 and current_pos >= total_frames - 1:
                            logging.info(f"End of video, looping: {self.source}")
                            
                            self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                            
                            # Test read
                            ret, test_frame = self.cap.read()
                            
                            if ret and test_frame is not None:
                                logging.info(f"Successfully looped video")
                                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                                self.consecutive_failures = 0
                                self.reconnect_attempts = 0
                                return
                            
            except Exception as e:
                logging.warning(f"Error handling video loop: {e}")
        
        # RTSP reconnection
        elif self.is_rtsp_source:
            self.reconnect_attempts += 1
            
            if self.reconnect_attempts >= self.max_reconnect_attempts:
                logging.error(f"Max RTSP reconnect attempts reached")
                self.is_running = False
                return
            
            delay = self.rtsp_reconnect_delay
            logging.warning(
                f"RTSP failure, retry in {delay}s "
                f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
            )
            
            time.sleep(delay)
            
            # Force re-open using safe method
            self._safe_release_capture()
            return
        
        # Generic handling
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logging.error(f"Max reconnect attempts reached")
            self.is_running = False
            return
        
        base_delay = 1.0 if self.is_file_source else 2.0
        max_delay = 5.0 if self.is_file_source else 30.0
        
        delay = min(max_delay, base_delay * (1.5 ** (self.reconnect_attempts - 1)))
        jitter = random.uniform(0.1, 0.5)
        total_delay = delay + jitter
        
        logging.warning(
            f"Read failure, retry in {total_delay:.1f}s "
            f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
        )
        
        time.sleep(total_delay)
        
        # Force re-open using safe method
        self._safe_release_capture()
    
    def _open_video_source(self) -> bool:
        """
        Open video source with safe cleanup.
        FIXED: Use safe release method, proper None checks.
        """
        try:
            # CRITICAL: Safe cleanup of existing capture
            self._safe_release_capture()
            
            # Small delay before reopening
            time.sleep(0.2)
            
            # Acquire lock for opening
            with self._capture_lock:
                # Validate file source
                if self.is_file_source:
                    if not self._validate_file_source(self.source):
                        return False
                
                # RTSP opening
                if self.is_rtsp_source:
                    logging.info(f"Opening RTSP stream: {self.source}")
                    
                    self._configure_rtsp_environment()
                    
                    try:
                        self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
                    except Exception as e:
                        logging.error(f"Failed to create VideoCapture: {e}")
                        self.cap = None
                        return False
                    
                    if self.cap is None or not self.cap.isOpened():
                        logging.error(f"Failed to open RTSP: {self.source}")
                        if self.cap is not None:
                            try:
                                self.cap.release()
                            except:
                                pass
                        self.cap = None
                        return False
                    
                    # Set properties
                    try:
                        self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.rtsp_timeout * 1000)
                        self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.rtsp_timeout * 1000)
                        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                    except Exception as e:
                        logging.warning(f"Could not set RTSP properties: {e}")
                    
                    # Test read
                    try:
                        ret, test_frame = self.cap.read()
                    except Exception as e:
                        logging.error(f"Test read failed: {e}")
                        ret = False
                        test_frame = None
                    
                    if not ret or test_frame is None:
                        logging.error(f"RTSP opened but cannot read frames")
                        try:
                            self.cap.release()
                        except:
                            pass
                        self.cap = None
                        return False
                    
                    width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                    height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    fps = self.cap.get(cv2.CAP_PROP_FPS)
                    
                    logging.info(f"✓ RTSP connected: {width}x{height} @ {fps}fps")
                    return True
                
                # File opening
                backends = [cv2.CAP_FFMPEG, cv2.CAP_ANY]
                
                for backend in backends:
                    try:
                        logging.info(f"Trying backend {backend} for {self.source}")
                        
                        try:
                            self.cap = cv2.VideoCapture(self.source, backend)
                        except Exception as e:
                            logging.warning(f"Failed to create VideoCapture with backend {backend}: {e}")
                            self.cap = None
                            continue
                        
                        if self.cap is None or not self.cap.isOpened():
                            if self.cap is not None:
                                try:
                                    self.cap.release()
                                except:
                                    pass
                            self.cap = None
                            continue
                        
                        try:
                            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                        except Exception as e:
                            logging.warning(f"Could not set buffer size: {e}")
                        
                        # Test read
                        try:
                            ret, test_frame = self.cap.read()
                        except Exception as e:
                            logging.warning(f"Test read failed with backend {backend}: {e}")
                            ret = False
                            test_frame = None
                        
                        if not ret or test_frame is None:
                            logging.warning(f"Backend {backend} cannot read")
                            try:
                                self.cap.release()
                            except:
                                pass
                            self.cap = None
                            continue
                        
                        # Reset for files
                        if self.is_file_source:
                            try:
                                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                            except Exception as e:
                                logging.warning(f"Could not reset frame position: {e}")
                        
                        logging.info(f"Successfully opened with backend {backend}")
                        return True
                        
                    except Exception as e:
                        logging.warning(f"Backend {backend} failed: {e}")
                        if self.cap is not None:
                            try:
                                self.cap.release()
                            except:
                                pass
                        self.cap = None
                        continue
                
                logging.error(f"All backends failed for {self.source}")
                return False
                
        except Exception as e:
            logging.error(f"Critical error opening {self.source}: {e}", exc_info=True)
            self._safe_release_capture()
            return False
    
    def __del__(self):
        """
        Destructor to ensure cleanup on object deletion.
        FIXED: Safe cleanup when object is garbage collected.
        """
        try:
            if hasattr(self, 'is_running') and self.is_running:
                self._stop_capture()
        except Exception as e:
            logging.debug(f"Error in __del__ for {self.source}: {e}")

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

    async def force_restart_shared_stream(self, source_path: str) -> bool:
        """Force restart a shared stream"""
        try:
            if source_path in self.shared_streams:
                shared_stream = self.shared_streams[source_path]
                
                affected_stream_ids = list(shared_stream.subscribers.keys())
                
                shared_stream._stop_capture()
                
                await asyncio.sleep(5.0)
                
                logging.info(f"Force restarted shared stream for {source_path}. Affected streams: {affected_stream_ids}")
                return True
            
            return False
        except Exception as e:
            logging.error(f"Error force restarting shared stream {source_path}: {e}", exc_info=True)
            return False

video_file_manager = VideoFileManager()