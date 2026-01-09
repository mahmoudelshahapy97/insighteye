# app/services/shared_stream_service.py
# 🔧 FIXED VERSION - Solves RTSP disconnection issues
import time
import logging
import asyncio
import random
import cv2
import numpy as np
import os
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class SharedVideoStream:
    """
    FIXED: Async-based shared video stream with improved RTSP stability.
    
    KEY FIXES:
    1. Increased timeouts and buffer sizes
    2. Better frame buffer management
    3. Smarter reconnection with exponential backoff
    4. Connection recovery instead of full restart
    5. Reduced aggressive error handling
    """
    
    def __init__(self, source: str, max_subscribers: int = 10):
        self.source = source
        self.subscribers: Dict[str, Dict[str, Any]] = {}
        self.cap: Optional[cv2.VideoCapture] = None
        self.latest_frame: Optional[np.ndarray] = None
        self.is_running = False
        
        # Use asyncio primitives
        self.lock = asyncio.Lock()
        self.frame_available = asyncio.Event()
        self.max_subscribers = max_subscribers
        self.capture_task: Optional[asyncio.Task] = None
        self.stop_capture = asyncio.Event()
        
        self.last_frame_time = time.time()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10  # ✅ Increased from 5 to 10
        
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

        # ✅ FIX #1: More lenient RTSP error handling
        self.consecutive_decode_errors = 0
        self.max_decode_errors = 50  # ✅ Increased from 10 to 50
        self.last_good_frame_time = None

        # ✅ FIX #2: Increased timeouts
        self.rtsp_timeout = 60  # ✅ Increased from 15 to 60 seconds
        self.rtsp_reconnect_delay = 5  # ✅ Increased from 2 to 5 seconds
        self.read_timeout_seconds = 180  # ✅ New: 3 minutes before giving up
        
        # CRITICAL: Capture lock to ensure only one read() at a time
        self._capture_lock = asyncio.Lock()
        
        # ✅ FIX #3: Connection recovery tracking
        self.connection_stable_time = None
        self.min_stable_duration = 60  # Consider connection stable after 60 seconds
        self.last_reconnect_time = 0
        self.min_reconnect_interval = 10  # Wait at least 10 seconds between reconnects
        
        logging.info(f"Created SharedVideoStream for source: {source} "
                    f"(file: {self.is_file_source}, rtsp: {self.is_rtsp_source})")
    
    async def add_subscriber(self, stream_id: str, callback_info: Dict[str, Any] = None) -> bool:
        """Add a subscriber to this shared stream"""
        async with self.lock:
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
                await self._start_capture()
            
            return True
    
    async def remove_subscriber(self, stream_id: str):
        """Remove a subscriber from this shared stream"""
        async with self.lock:
            if stream_id in self.subscribers:
                subscriber_info = self.subscribers.pop(stream_id)
                logging.info(f"Removed subscriber {stream_id} from {self.source}. "
                           f"Frames: {subscriber_info.get('frames_received', 0)}")
            
            # Stop capture if no subscribers
            if not self.subscribers and self.is_running:
                await self._stop_capture()
    
    async def get_latest_frame(self, stream_id: str) -> Optional[np.ndarray]:
        """Get the latest frame for a specific subscriber"""
        async with self.lock:
            if stream_id not in self.subscribers:
                return None
                
            if self.latest_frame is not None:
                self.subscribers[stream_id]['frames_received'] += 1
                self.subscribers[stream_id]['last_frame_time'] = time.time()
                return self.latest_frame.copy()
            
            return None
    
    async def wait_for_frame(self, timeout: float = 10.0) -> bool:
        """Wait for a new frame to be available"""
        try:
            await asyncio.wait_for(self.frame_available.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

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
        """
        ✅ FIX #4: Enhanced RTSP configuration for stability.
        
        KEY IMPROVEMENTS:
        1. Larger buffers to prevent overflow
        2. Longer timeouts for network recovery
        3. TCP transport for reliability
        4. Error tolerance flags
        """
        if not self.is_rtsp_source:
            return
        
        # Suppress FFmpeg error spam
        os.environ['OPENCV_FFMPEG_LOGLEVEL'] = '-8'
        
        # ✅ CRITICAL: Optimized RTSP settings for stability
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;tcp|'  # TCP is more reliable
            f'timeout;{self.rtsp_timeout * 1000000}|'  # 60 seconds
            f'stimeout;{self.rtsp_timeout * 1000000}|'
            f'max_delay;5000000|'  # ✅ Increased to 5 seconds
            f'buffer_size;2048000|'  # ✅ 2MB buffer (was implicit)
            f'reorder_queue_size;0|'
            f'fflags;+genpts+igndts+discardcorrupt|'  # ✅ Added discardcorrupt
            f'flags;+low_delay|'
            f'analyzeduration;5000000|'  # ✅ Increased to 5 seconds
            f'probesize;5000000|'  # ✅ 5MB probe
            f'err_detect;ignore_err'  # ✅ NEW: Ignore minor errors
        )
        
        # Single-threaded FFmpeg
        os.environ['OPENCV_FFMPEG_THREAD_COUNT'] = '1'
        
        logger.info(f"✅ Enhanced RTSP config: timeout={self.rtsp_timeout}s, buffer=2MB")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics"""
        async with self.lock:
            return {
                'source': self.source,
                'subscriber_count': len(self.subscribers),
                'is_running': self.is_running,
                'frame_count': self.frame_count,
                'last_frame_time': self.last_frame_time,
                'reconnect_attempts': self.reconnect_attempts,
                'last_error': self.last_error,
                'error_count': self.error_count,
                'connection_stable': self.connection_stable_time is not None,
                'subscribers': {
                    stream_id: {
                        'frames_received': info['frames_received'],
                        'last_frame_time': info['last_frame_time']
                    }
                    for stream_id, info in self.subscribers.items()
                }
            }

    async def _safe_release_capture(self):
        """Safely release VideoCapture with proper error handling."""
        if self.cap is None:
            return
        
        try:
            async with self._capture_lock:
                if self.cap is not None:
                    try:
                        if self.cap.isOpened():
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(None, self.cap.release)
                            logging.debug(f"✓ Released VideoCapture for {self.source}")
                        else:
                            logging.debug(f"VideoCapture already closed for {self.source}")
                    except Exception as e:
                        logging.warning(f"Error during cap.release() for {self.source}: {e}")
                    finally:
                        self.cap = None
        except Exception as e:
            logging.error(f"Error in _safe_release_capture for {self.source}: {e}")
            self.cap = None
    
    async def _stop_capture(self):
        """Stop the video capture task with safe cleanup."""
        if not self.is_running:
            return
        
        logging.info(f"Stopping capture for {self.source}")
        self.is_running = False
        self.stop_capture.set()
        
        # Release capture to unblock read()
        await self._safe_release_capture()
        
        # Wait for task with timeout
        if self.capture_task and not self.capture_task.done():
            timeout = 10.0 if self.is_rtsp_source else 5.0
            
            logging.debug(f"Waiting for capture task (timeout: {timeout}s)")
            self.capture_task.cancel()
            
            try:
                await asyncio.wait_for(self.capture_task, timeout=timeout)
            except asyncio.TimeoutError:
                logging.warning(f"⚠️ Capture task for {self.source} did not stop within {timeout}s.")
            except asyncio.CancelledError:
                pass
        
        self.capture_task = None
        logging.info(f"✓ Stopped capture for {self.source}")
    
    async def _capture_loop(self):
        """
        ✅ FIX #5: Improved capture loop with better error recovery.
        
        FIXES:
        1. Connection recovery before full restart
        2. Longer timeouts before giving up
        3. Exponential backoff for reconnections
        4. Buffer flush on errors
        """
        logging.info(f"Capture loop started for {self.source}")
        
        # Configure RTSP if needed
        if self.is_rtsp_source:
            self._configure_rtsp_environment()
        
        loop = asyncio.get_event_loop()
        
        # Get video info
        video_fps = None
        if self.is_file_source:
            try:
                test_cap = cv2.VideoCapture(self.source)
                if test_cap.isOpened():
                    video_fps = test_cap.get(cv2.CAP_PROP_FPS)
                    total_frames = int(test_cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    logging.info(f"Video: {total_frames} frames, {video_fps:.1f} FPS")
                test_cap.release()
            except Exception as e:
                logging.warning(f"Could not get video info: {e}")
        
        last_successful_read = time.time()
        
        try:
            while not self.stop_capture.is_set() and self.is_running:
                try:
                    # Check stop signal
                    if self.stop_capture.is_set() or not self.is_running:
                        logging.info(f"Stop signal received for {self.source}")
                        break
                    
                    # Open video source if needed
                    if self.cap is None or not self.cap.isOpened():
                        if not await self._open_video_source_async():
                            # ✅ FIX #6: Exponential backoff for reconnection
                            base_delay = self.rtsp_reconnect_delay if self.is_rtsp_source else 2.0
                            backoff_delay = min(30.0, base_delay * (1.5 ** (self.reconnect_attempts - 1)))
                            
                            logging.warning(
                                f"Failed to open {self.source}, retry in {backoff_delay:.1f}s "
                                f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
                            )
                            
                            try:
                                await asyncio.wait_for(
                                    self.stop_capture.wait(), 
                                    timeout=backoff_delay
                                )
                                break
                            except asyncio.TimeoutError:
                                pass
                            
                            self.reconnect_attempts += 1
                            
                            # ✅ FIX #7: Give up after max attempts
                            if self.reconnect_attempts >= self.max_reconnect_attempts:
                                logging.error(f"Max reconnect attempts reached for {self.source}")
                                break
                            
                            continue
                    
                    # ✅ FIX #8: Try connection recovery before full restart
                    if self.is_rtsp_source and self.consecutive_failures > 5:
                        if await self._try_connection_recovery():
                            logging.info(f"✅ Connection recovered for {self.source}")
                            self.consecutive_failures = 0
                            last_successful_read = time.time()
                            continue
                    
                    # Thread-safe read
                    frame_read_successful = False
                    ret = False
                    frame = None
                    
                    async with self._capture_lock:
                        if (not self.stop_capture.is_set() and 
                            self.is_running and 
                            self.cap is not None and
                            self.cap.isOpened()):
                            
                            try:
                                ret, frame = await loop.run_in_executor(None, self.cap.read)
                                frame_read_successful = True
                            except Exception as read_error:
                                logging.error(f"Exception during cap.read(): {read_error}")
                                ret = False
                                frame = None
                    
                    if self.stop_capture.is_set() or not self.is_running:
                        break
                    
                    if not frame_read_successful or not ret or frame is None:
                        self._handle_read_failure()
                        
                        # ✅ FIX #9: Increased timeout from 30s to 180s
                        if self.is_rtsp_source:
                            time_since_success = time.time() - last_successful_read
                            if time_since_success > self.read_timeout_seconds:
                                logging.error(
                                    f"No frames for {time_since_success:.1f}s "
                                    f"(timeout: {self.read_timeout_seconds}s)"
                                )
                                break
                        
                        continue
                    
                    # Validate frame
                    if frame.size == 0 or len(frame.shape) != 3:
                        logging.warning(f"Invalid frame from {self.source}")
                        self._handle_read_failure()
                        continue
                    
                    # ✅ Successfully read frame
                    last_successful_read = time.time()
                    self.reconnect_attempts = 0
                    self.consecutive_failures = 0
                    self.frame_count += 1
                    self.last_frame_time = time.time()
                    self.last_successful_read = time.time()
                    
                    # Track connection stability
                    if self.connection_stable_time is None:
                        self.connection_stable_time = time.time()
                    
                    # Update latest frame
                    async with self.lock:
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
                    
                    if sleep_time > 0:
                        try:
                            await asyncio.wait_for(
                                self.stop_capture.wait(),
                                timeout=sleep_time
                            )
                            break
                        except asyncio.TimeoutError:
                            pass
                    
                except asyncio.CancelledError:
                    logging.info(f"Capture task cancelled for {self.source}")
                    break
                except Exception as e:
                    self.last_error = str(e)
                    self.error_count += 1
                    self.consecutive_failures += 1
                    logging.error(f"Error in capture loop: {e}")
                    
                    max_failures = 20 if self.is_rtsp_source else 20  # Same for all
                    if self.consecutive_failures > max_failures:
                        logging.error(f"Too many failures ({self.consecutive_failures}), stopping")
                        break
                    
                    try:
                        await asyncio.wait_for(self.stop_capture.wait(), timeout=1.0)
                        break
                    except asyncio.TimeoutError:
                        pass
        
        except Exception as e:
            logging.error(f"Fatal error in capture loop: {e}", exc_info=True)
        
        finally:
            logging.info(f"Capture loop cleanup starting for {self.source}")
            await self._safe_release_capture()
            self.is_running = False
            self.connection_stable_time = None
            logging.info(f"✓ Capture loop ended for {self.source}")
    
    async def _try_connection_recovery(self) -> bool:
        """
        ✅ FIX #10: Try to recover connection without full restart.
        
        This is faster than creating a new connection and avoids
        hitting NVR connection limits.
        """
        try:
            if self.cap is None or not self.cap.isOpened():
                return False
            
            logging.info(f"🔧 Attempting connection recovery for {self.source}")
            
            # Get event loop
            loop = asyncio.get_event_loop()
            
            # Step 1: Flush buffer
            async with self._capture_lock:
                if self.cap is not None and self.cap.isOpened():
                    # Clear buffer by setting buffer size to 1
                    try:
                        await loop.run_in_executor(
                            None,
                            self.cap.set,
                            cv2.CAP_PROP_BUFFERSIZE,
                            1
                        )
                    except:
                        pass
                    
                    # Read and discard buffered frames
                    for _ in range(10):
                        try:
                            await loop.run_in_executor(None, self.cap.read)
                        except:
                            break
                    
                    # Test read
                    try:
                        ret, test_frame = await loop.run_in_executor(None, self.cap.read)
                        if ret and test_frame is not None and test_frame.size > 0:
                            logging.info(f"✅ Connection recovery successful for {self.source}")
                            return True
                    except:
                        pass
            
            return False
            
        except Exception as e:
            logging.error(f"Error during connection recovery: {e}")
            return False
    
    def _handle_read_failure(self):
        """Handle read failures with improved logic."""
        self.consecutive_failures += 1
        
        # Video looping for files
        if self.is_file_source and self.cap is not None:
            try:
                if self.cap is not None and self.cap.isOpened():
                    current_pos = int(self.cap.get(cv2.CAP_PROP_POS_FRAMES))
                    total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    
                    if total_frames > 0 and current_pos >= total_frames - 1:
                        logging.info(f"End of video, looping: {self.source}")
                        self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        ret, test_frame = self.cap.read()
                        
                        if ret and test_frame is not None:
                            logging.info(f"Successfully looped video")
                            self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                            self.consecutive_failures = 0
                            self.reconnect_attempts = 0
                            return
            except Exception as e:
                logging.warning(f"Error handling video loop: {e}")
        
        # ✅ FIX #11: Less aggressive for RTSP
        elif self.is_rtsp_source:
            # Don't increment reconnect_attempts here - let the main loop handle it
            # Just log the failure
            if self.consecutive_failures % 10 == 0:  # Log every 10 failures
                logging.warning(
                    f"RTSP read failures: {self.consecutive_failures} consecutive "
                    f"(will keep trying for {self.read_timeout_seconds}s total)"
                )
            return
        
        # Generic handling for other sources
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logging.error(f"Max reconnect attempts reached for {self.source}")
            self.is_running = False
            return
        
        # Exponential backoff
        base_delay = 1.0 if self.is_file_source else 2.0
        max_delay = 5.0 if self.is_file_source else 30.0
        
        delay = min(max_delay, base_delay * (1.5 ** (self.reconnect_attempts - 1)))
        jitter = random.uniform(0.1, 0.5)
        total_delay = delay + jitter
        
        logging.warning(
            f"Read failure, will retry in {total_delay:.1f}s "
            f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
        )
        
        time.sleep(total_delay)
        self.cap = None
    
    def _open_video_source(self) -> bool:
        """
        Open video source with improved error handling.
        SYNCHRONOUS version for use in blocking context.
        """
        try:
            # Clean up existing capture
            if self.cap is not None:
                try:
                    if self.cap.isOpened():
                        self.cap.release()
                except:
                    pass
                self.cap = None
            
            # Small delay
            time.sleep(0.2)
            
            # Validate file source
            if self.is_file_source:
                if not self._validate_file_source(self.source):
                    return False
            
            # ✅ RTSP opening with enhanced settings
            if self.is_rtsp_source:
                # ✅ FIX #12: Rate limit reconnection attempts
                current_time = time.time()
                if current_time - self.last_reconnect_time < self.min_reconnect_interval:
                    wait_time = self.min_reconnect_interval - (current_time - self.last_reconnect_time)
                    logging.info(f"Rate limiting: waiting {wait_time:.1f}s before reconnect")
                    time.sleep(wait_time)
                
                self.last_reconnect_time = time.time()
                
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
                    self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # Minimal buffer
                except Exception as e:
                    logging.warning(f"Could not set RTSP properties: {e}")
                
                # ✅ FIX #13: Multiple test reads to ensure stability
                test_success = False
                for attempt in range(3):
                    try:
                        ret, test_frame = self.cap.read()
                        if ret and test_frame is not None and test_frame.size > 0:
                            test_success = True
                            break
                        time.sleep(0.5)
                    except Exception as e:
                        logging.warning(f"Test read attempt {attempt + 1} failed: {e}")
                        time.sleep(0.5)
                
                if not test_success:
                    logging.error(f"RTSP opened but cannot read frames after 3 attempts")
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
                
                # Reset connection tracking
                self.connection_stable_time = time.time()
                
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
            if self.cap is not None:
                try:
                    self.cap.release()
                except:
                    pass
            self.cap = None
            return False
    
    async def _open_video_source_async(self) -> bool:
        """
        Open video source asynchronously without blocking event loop.
        CRITICAL: Runs in thread pool to prevent blocking other cameras.
        """
        loop = asyncio.get_event_loop()
        
        # Run blocking _open_video_source in executor
        try:
            return await loop.run_in_executor(None, self._open_video_source)
        except Exception as e:
            logging.error(f"Error in async open: {e}")
            return False

    async def _start_capture(self):
        """Start the video capture task"""
        if self.is_running:
            return
            
        self.is_running = True
        self.stop_capture.clear()
        self.capture_task = asyncio.create_task(self._capture_loop())
        self.capture_task.set_name(f"capture_{self.source}")
        logging.info(f"Started capture task for {self.source}")

    def __del__(self):
        """
        Destructor to ensure cleanup on object deletion.
        FIXED: Safe cleanup when object is garbage collected.
        """
        try:
            if hasattr(self, 'is_running') and self.is_running:
                # Can't await in __del__, so we just set flags
                self.is_running = False
                if hasattr(self, 'stop_capture'):
                    self.stop_capture.set()
        except Exception as e:
            logging.debug(f"Error in __del__ for {self.source}: {e}")


class VideoFileManager:
    """Manages shared video streams to prevent file conflicts"""
    
    def __init__(self):
        self.shared_streams: Dict[str, SharedVideoStream] = {}
        self.lock = asyncio.Lock()
    
    async def get_shared_stream(self, source: str, max_subscribers: int = 10) -> SharedVideoStream:
        """Get or create a shared stream for a source"""
        async with self.lock:
            if source not in self.shared_streams:
                self.shared_streams[source] = SharedVideoStream(source, max_subscribers)
            return self.shared_streams[source]
    
    async def remove_shared_stream(self, source: str):
        """Remove a shared stream"""
        async with self.lock:
            if source in self.shared_streams:
                shared_stream = self.shared_streams[source]
                if shared_stream.is_running:
                    await shared_stream._stop_capture()
                del self.shared_streams[source]
                logging.info(f"Removed shared stream for {source}")
    
    async def cleanup_empty_streams(self):
        """Clean up streams with no subscribers"""
        async with self.lock:
            empty_sources = []
            for source, stream in self.shared_streams.items():
                if not stream.subscribers:
                    empty_sources.append(source)
            
            for source in empty_sources:
                await self.remove_shared_stream(source)
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all shared streams"""
        async with self.lock:
            stats = {}
            for source, stream in self.shared_streams.items():
                stats[source] = await stream.get_stats()
            return stats

    async def force_restart_shared_stream(self, source_path: str) -> bool:
        """Force restart a shared stream"""
        try:
            async with self.lock:
                if source_path in self.shared_streams:
                    shared_stream = self.shared_streams[source_path]
                    
                    affected_stream_ids = list(shared_stream.subscribers.keys())
                    
                    await shared_stream._stop_capture()
                    
                    await asyncio.sleep(5.0)
                    
                    logging.info(
                        f"Force restarted shared stream for {source_path}. "
                        f"Affected streams: {affected_stream_ids}"
                    )
                    return True
            
            return False
        except Exception as e:
            logging.error(f"Error force restarting shared stream {source_path}: {e}", exc_info=True)
            return False


# Global instance
video_file_manager = VideoFileManager()