# app/services/shared_stream_service.py
# 🎯 PRODUCTION-HARDENED VERSION - Fixes for 100+ camera scale
import time
import logging
import asyncio
import os
from typing import Dict, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import concurrent.futures
from datetime import datetime, timedelta
from uuid import UUID
import cv2
import numpy as np
from app.config.settings import config

logger = logging.getLogger(__name__)

# ============================================================================
# CRITICAL FIX #1: Per-stream thread pools to prevent saturation
# ============================================================================
class StreamThreadPool:
    """Manages isolated thread pool per stream to prevent global saturation"""
    _pools: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_pool(cls, stream_id: str) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create dedicated pool for stream (1 thread per stream)"""
        async with cls._lock:
            if stream_id not in cls._pools:
                cls._pools[stream_id] = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix=f"Stream_{stream_id[:8]}"
                )
            return cls._pools[stream_id]
    
    @classmethod
    async def cleanup_pool(cls, stream_id: str):
        """Clean up pool when stream stops"""
        async with cls._lock:
            if stream_id in cls._pools:
                pool = cls._pools.pop(stream_id)
                pool.shutdown(wait=False)


class StreamState(Enum):
    """Stream lifecycle states"""
    IDLE = "idle"
    CONNECTING = "connecting"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"
    FAILED = "failed"
    ZOMBIE = "zombie"  # NEW: Detected as unresponsive

@dataclass
class StreamMetrics:
    """Stream health and performance metrics"""
    total_frames: int = 0
    connection_attempts: int = 0
    consecutive_errors: int = 0
    last_success_time: Optional[float] = None
    last_error_time: Optional[float] = None
    last_error: Optional[str] = None
    state: StreamState = StreamState.IDLE
    uptime_start: Optional[float] = None
    
    # NEW: Health tracking
    last_heartbeat: Optional[float] = None
    zombie_detected_at: Optional[float] = None
    recovery_attempts: int = 0


@dataclass
class Subscriber:
    """Subscriber information"""
    stream_id: str
    added_at: float = field(default_factory=time.time)
    frames_received: int = 0
    last_frame_time: Optional[float] = None
    callback_info: Dict[str, Any] = field(default_factory=dict)


class SharedVideoStream:
    """
    Production-hardened shared video stream for 100+ camera deployment.
    
    Key improvements:
    - Per-stream thread isolation
    - Aggressive timeout detection
    - Zombie state detection
    - TCP-first RTSP strategy
    - Enhanced health monitoring
    """
    
    def __init__(
        self,
        source: str,
        stream_id: str,  # Must be string
        max_subscribers: int = 10,
    ):
        self.source = source
        
        # ✅ CRITICAL FIX: Ensure stream_id is always a string
        if isinstance(stream_id, UUID):
            self.stream_id = str(stream_id)
        elif isinstance(stream_id, int):
            # Convert integer IDs to string
            import hashlib
            self.stream_id = hashlib.md5(f"{source}_{stream_id}".encode()).hexdigest()
            logger.warning(
                f"⚠️ Received integer stream_id={stream_id}, "
                f"converted to hash: {self.stream_id[:8]}"
            )
        else:
            self.stream_id = str(stream_id)
    
        self.max_subscribers = max_subscribers
        self.config = config 
        
        # State management
        self.state = StreamState.IDLE
        self.metrics = StreamMetrics()
        
        # Subscribers
        self.subscribers: Dict[str, Subscriber] = {}
        
        # Video capture
        self.cap: Optional[cv2.VideoCapture] = None
        self.latest_frame: Optional[np.ndarray] = None
        
        # Async primitives
        self.lock = asyncio.Lock()
        self.capture_lock = asyncio.Lock()
        self.frame_available = asyncio.Event()
        self.stop_event = asyncio.Event()
        
        # Tasks
        self.capture_task: Optional[asyncio.Task] = None
        self.health_monitor_task: Optional[asyncio.Task] = None
        
        # Source type detection
        self.is_file = self._is_file_source(source)
        self.is_rtsp = self._is_rtsp_source(source)
        
        # Recovery tracking
        self._last_recovery_attempt: float = 0
        self._last_reconnect_time: float = 0
        
        # NEW: Dedicated thread pool
        self._thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
        
        logger.info(
            f"📹 Created SharedVideoStream: id={self.stream_id[:8]}, source={source}, "
            f"type={'file' if self.is_file else 'rtsp' if self.is_rtsp else 'other'}"
        )
    
    # ==================== Subscriber Management ====================
    
    async def add_subscriber(
        self,
        subscriber_id: str,
        callback_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Add a subscriber and start capture if needed"""
        async with self.lock:
            if len(self.subscribers) >= self.max_subscribers:
                logger.warning(f"Max subscribers ({self.max_subscribers}) reached")
                return False
            
            self.subscribers[subscriber_id] = Subscriber(
                stream_id=subscriber_id,
                callback_info=callback_info or {}
            )
            
            logger.info(
                f"➕ Added subscriber {subscriber_id[:8]} to stream {self.stream_id[:8]}. "
                f"Total: {len(self.subscribers)}"
            )
            
            # Start capture if this is the first subscriber
            if len(self.subscribers) == 1:
                await self._start_capture()
            
            return True
    
    async def remove_subscriber(self, subscriber_id: str) -> None:
        """Remove a subscriber and stop capture if needed"""
        async with self.lock:
            if subscriber_id not in self.subscribers:
                return
            
            subscriber = self.subscribers.pop(subscriber_id)
            logger.info(
                f"➖ Removed subscriber {subscriber_id[:8]} from stream {self.stream_id[:8]}. "
                f"Frames received: {subscriber.frames_received}"
            )
            
            # Stop capture if no subscribers remain
            if not self.subscribers:
                await self._stop_capture()
    
    async def get_latest_frame(self, subscriber_id: str) -> Optional[np.ndarray]:
        """Get the latest frame for a subscriber"""
        async with self.lock:
            if subscriber_id not in self.subscribers:
                return None
            
            if self.latest_frame is not None:
                subscriber = self.subscribers[subscriber_id]
                subscriber.frames_received += 1
                subscriber.last_frame_time = time.time()
                return self.latest_frame.copy()
            
            return None
    
    async def wait_for_frame(self, timeout: float = 10.0) -> bool:
        """Wait for a new frame to become available"""
        try:
            await asyncio.wait_for(self.frame_available.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    # ==================== Capture Management ====================
    
    async def _start_capture(self) -> None:
        """Start the capture loop"""
        if self.state in (StreamState.RUNNING, StreamState.CONNECTING):
            return
        
        self.state = StreamState.CONNECTING
        self.stop_event.clear()
        self.metrics.uptime_start = time.time()
        
        # Get dedicated thread pool
        self._thread_pool = await StreamThreadPool.get_pool(self.stream_id)
        
        self.capture_task = asyncio.create_task(self._capture_loop())
        self.capture_task.set_name(f"capture_{self.stream_id[:8]}")
        
        # NEW: Start health monitor
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        self.health_monitor_task.set_name(f"health_{self.stream_id[:8]}")
        
        logger.info(f"▶️ Started capture task for {self.stream_id[:8]}")
    
    async def _stop_capture(self) -> None:
        """Stop the capture loop gracefully"""
        if self.state == StreamState.STOPPED:
            return
        
        logger.info(f"⏹️ Stopping capture for {self.stream_id[:8]}")
        
        self.state = StreamState.STOPPED
        self.stop_event.set()
        
        # Stop health monitor
        if self.health_monitor_task and not self.health_monitor_task.done():
            self.health_monitor_task.cancel()
            try:
                await asyncio.wait_for(self.health_monitor_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        # Release capture to unblock read()
        await self._safe_release_capture()
        
        # Wait for task to complete
        if self.capture_task and not self.capture_task.done():
            self.capture_task.cancel()
            try:
                timeout = 10.0 if self.is_rtsp else 5.0
                await asyncio.wait_for(self.capture_task, timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        self.capture_task = None
        
        # Cleanup thread pool
        await StreamThreadPool.cleanup_pool(self.stream_id)
        self._thread_pool = None
        
        logger.info(f"✅ Capture stopped for {self.stream_id[:8]}")
    
    # ==================== NEW: Health Monitoring ====================

    async def _health_monitor_loop(self) -> None:
        """
        Monitor stream health and detect zombie states.
        
        ✅ FIXED: 
        - 10-minute timeout for RTSP cameras (not 20 seconds)
        - 5-minute startup grace period for RTSP
        - Prevents premature zombie detection during buffering
        """
        logger.info(f"🏥 Health monitor started for {self.stream_id[:8]}")
        
        # ✅ Determine timeout based on stream type
        is_rtsp = self.is_rtsp
        
        # CRITICAL: Different timeouts for startup vs running
        startup_grace_period = 300.0 if is_rtsp else 60.0  # 5 min for RTSP startup
        running_zombie_timeout = 600.0 if is_rtsp else 120.0  # 10 min RTSP, 2 min files
        
        logger.info(
            f"🏥 Health monitor: {'RTSP' if is_rtsp else 'FILE'} stream, "
            f"startup_grace={startup_grace_period}s, "
            f"running_timeout={running_zombie_timeout}s"
        )
        
        startup_time = time.time()
        first_frame_received = False
        
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(30.0)  # Check every 30 seconds
                
                if self.state not in (StreamState.RUNNING, StreamState.CONNECTING):
                    continue
                
                now = time.time()
                
                # Check if we've received ANY frames
                if self.metrics.last_success_time:
                    if not first_frame_received:
                        first_frame_received = True
                        logger.info(
                            f"✅ {self.stream_id[:8]} received first frame "
                            f"after {now - startup_time:.1f}s"
                        )
                    
                    time_since_frame = now - self.metrics.last_success_time
                    
                    # ✅ Use running timeout (since we've received frames before)
                    if time_since_frame > running_zombie_timeout:
                        logger.error(
                            f"☠️ ZOMBIE DETECTED for {self.stream_id[:8]}: "
                            f"No frames for {time_since_frame:.0f}s "
                            f"(timeout: {running_zombie_timeout}s, type: {'RTSP' if is_rtsp else 'FILE'})"
                        )
                        self.state = StreamState.ZOMBIE
                        self.metrics.zombie_detected_at = now
                        
                        # Trigger emergency stop and cleanup
                        await self._emergency_cleanup()
                        break
                        
                    elif time_since_frame > running_zombie_timeout * 0.5:
                        # ✅ PROACTIVE: Trigger reconnection at 50% threshold
                        logger.warning(
                            f"⚠️ Stream {self.stream_id[:8]} stagnant for {time_since_frame:.0f}s "
                            f"({running_zombie_timeout * 0.5:.0f}s threshold) - triggering proactive recovery"
                        )
                        # Try full reconnection to prevent zombie state
                        try:
                            if await self._force_full_reconnection():
                                logger.info(f"✅ Proactive recovery successful for {self.stream_id[:8]}")
                            else:
                                logger.warning(f"⚠️ Proactive recovery failed for {self.stream_id[:8]}")
                        except Exception as e:
                            logger.error(f"💥 Proactive recovery error for {self.stream_id[:8]}: {e}")
                else:
                    # ✅ NO FRAMES YET - Check startup grace period
                    time_since_startup = now - startup_time
                    
                    if time_since_startup > startup_grace_period:
                        logger.error(
                            f"☠️ STARTUP TIMEOUT for {self.stream_id[:8]}: "
                            f"No frames after {time_since_startup:.0f}s "
                            f"(grace period: {startup_grace_period}s)"
                        )
                        self.state = StreamState.ZOMBIE
                        self.metrics.zombie_detected_at = now
                        
                        await self._emergency_cleanup()
                        break
                        
                    elif time_since_startup > startup_grace_period * 0.7:
                        # Warn at 70% of grace period
                        logger.warning(
                            f"⏳ Stream {self.stream_id[:8]} still buffering: "
                            f"{time_since_startup:.0f}s / {startup_grace_period}s grace period"
                        )
                
                # Update heartbeat
                self.metrics.last_heartbeat = now
        
        except asyncio.CancelledError:
            logger.info(f"🏥 Health monitor cancelled for {self.stream_id[:8]}")
        except Exception as e:
            logger.error(f"💥 Health monitor error for {self.stream_id[:8]}: {e}")


    async def _emergency_cleanup(self) -> None:
        """Emergency cleanup when zombie state detected"""
        logger.warning(f"🚨 Emergency cleanup for {self.stream_id[:8]}")
        
        try:
            # Force release capture
            await self._safe_release_capture()
            
            # Cancel capture task
            if self.capture_task and not self.capture_task.done():
                self.capture_task.cancel()
            
            self.state = StreamState.FAILED
            self.metrics.last_error = "Zombie state - no frames received"
        
        except Exception as e:
            logger.error(f"💥 Emergency cleanup error: {e}")
    
    # ==================== Capture Loop ====================
    
    async def _capture_loop(self) -> None:
        """Main capture loop with robust error handling"""
        logger.info(f"🎬 Capture loop started for {self.stream_id[:8]}")
        
        loop = asyncio.get_event_loop()
        frame_interval = 1.0 / self.config.target_fps
        last_frame_time = 0.0
        last_success_time = time.time()
        
        try:
            while not self.stop_event.is_set():
                # FPS throttling
                now = time.time()
                if now - last_frame_time < frame_interval:
                    await asyncio.sleep(0.01)
                    continue
                last_frame_time = now
                
                # Ensure video source is open
                if not await self._ensure_video_source():
                    delay = self._calculate_backoff_delay()
                    logger.warning(
                        f"⏳ Stream {self.stream_id[:8]}: Waiting {delay:.1f}s before retry "
                        f"(attempt {self.metrics.connection_attempts})"
                    )
                    
                    try:
                        await asyncio.wait_for(
                            self.stop_event.wait(),
                            timeout=delay
                        )
                        break
                    except asyncio.TimeoutError:
                        pass
                    
                    continue
                
                # Check timeout for RTSP
                if self.is_rtsp:
                    elapsed = time.time() - last_success_time
                    if elapsed > self.config.max_read_timeout:
                        logger.error(
                            f"❌ Stream {self.stream_id[:8]}: Read timeout "
                            f"({elapsed:.0f}s > {self.config.max_read_timeout:.0f}s)"
                        )
                        
                        # Try recovery before giving up
                        if self.metrics.recovery_attempts < 3:
                            if await self._try_connection_recovery():
                                last_success_time = time.time()
                                continue
                        
                        # FORCE RECONNECTION instead of breaking
                        # This prevents the stream from entering zombie state
                        logger.warning(
                            f"♻️ Stream {self.stream_id[:8]}: Forcing reconnection after read timeout"
                        )
                        await self._safe_release_capture()
                        continue
                
                # Read frame
                ret, frame = await self._read_frame_safe()
                
                if self.stop_event.is_set():
                    break
                
                # Handle read failure
                if not ret or frame is None or frame.size == 0:
                    await self._handle_read_failure()
                    continue
                
                # Validate frame
                if not self._validate_frame(frame):
                    await self._handle_read_failure()
                    continue
                
                # Successfully read frame
                await self._process_successful_frame(frame)
                last_success_time = time.time()
                
                # Adaptive sleep
                sleep_time = self._calculate_sleep_time()
                if sleep_time > 0:
                    try:
                        await asyncio.wait_for(
                            self.stop_event.wait(),
                            timeout=sleep_time
                        )
                        break
                    except asyncio.TimeoutError:
                        pass
        
        except asyncio.CancelledError:
            logger.info(f"🛑 Capture task cancelled for {self.stream_id[:8]}")
        except Exception as e:
            logger.error(
                f"💥 Fatal error in capture loop for {self.stream_id[:8]}: {e}",
                exc_info=True
            )
            self.state = StreamState.FAILED
            self.metrics.last_error = str(e)
        finally:
            await self._safe_release_capture()
            logger.info(
                f"📊 Capture ended for {self.stream_id[:8]}: "
                f"frames={self.metrics.total_frames}, "
                f"attempts={self.metrics.connection_attempts}"
            )
    
    # ==================== Frame Reading ====================
    
    async def _read_frame_safe(self) -> tuple[bool, Optional[np.ndarray]]:
        """Thread-safe frame reading with dedicated pool"""
        async with self.capture_lock:
            if (self.stop_event.is_set() or
                self.cap is None or
                not self.cap.isOpened() or
                self._thread_pool is None):
                return False, None
            
            try:
                loop = asyncio.get_event_loop()
                # Use dedicated thread pool instead of global
                ret, frame = await loop.run_in_executor(
                    self._thread_pool,
                    self.cap.read
                )
                return ret, frame
            except Exception as e:
                logger.error(f"Exception during read for {self.stream_id[:8]}: {e}")
                return False, None
    
    def _validate_frame(self, frame: np.ndarray) -> bool:
        """Validate frame integrity"""
        if frame is None or frame.size == 0:
            return False
        if len(frame.shape) != 3:
            return False
        if frame.shape[0] < 10 or frame.shape[1] < 10:  # Minimum size check
            return False
        return True
    
    async def _process_successful_frame(self, frame: np.ndarray) -> None:
        """Process a successfully read frame"""
        # Reset error counters
        self.metrics.consecutive_errors = 0
        self.metrics.total_frames += 1
        self.metrics.last_success_time = time.time()
        self.state = StreamState.RUNNING
        
        # Resize if needed
        if frame.shape[1] > self.config.max_frame_width:
            h, w = frame.shape[:2]
            new_w = self.config.max_frame_width
            new_h = int(h * (new_w / w))
            frame = cv2.resize(frame, (new_w, new_h))
        
        # Update latest frame
        async with self.lock:
            self.latest_frame = frame
            self.frame_available.set()
            self.frame_available.clear()
        
        # Log progress periodically
        if self.metrics.total_frames % 100 == 0:
            logger.debug(
                f"📊 Stream {self.stream_id[:8]}: {self.metrics.total_frames} frames"
            )
    
    async def _handle_read_failure(self) -> None:
        """Handle frame read failure with 3-tier recovery strategy"""
        self.metrics.consecutive_errors += 1
        self.metrics.last_error_time = time.time()
        
        # ✅ TIER 3: EMERGENCY - Hit max errors, force stop and restart
        if self.metrics.consecutive_errors >= self.config.max_consecutive_errors:
            logger.error(
                f"❌ Stream {self.stream_id[:8]}: Max consecutive errors reached "
                f"({self.metrics.consecutive_errors}). FORCING EMERGENCY RESTART."
            )
            self.state = StreamState.FAILED
            self.metrics.last_error = f"Max consecutive errors ({self.metrics.consecutive_errors})"
            
            # Force full reconnection
            await self._safe_release_capture()
            
            # Reset error counter to allow restart
            self.metrics.consecutive_errors = 0
            self.metrics.recovery_attempts = 0
            
            # Wait before retry
            await asyncio.sleep(5.0)
            return
        
        # File looping
        if self.is_file and self.cap is not None:
            try:
                async with self.capture_lock:
                    if self.cap and self.cap.isOpened():
                        current = int(self.cap.get(cv2.CAP_PROP_POS_FRAMES))
                        total = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
                        
                        if total > 0 and current >= total - 1:
                            logger.info(f"🔁 Looping video {self.stream_id[:8]}")
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                self._thread_pool,
                                self.cap.set,
                                cv2.CAP_PROP_POS_FRAMES,
                                0
                            )
                            self.metrics.consecutive_errors = 0
                            return
            except Exception as e:
                logger.warning(f"Error during loop for {self.stream_id[:8]}: {e}")
        
        # RTSP 3-tier recovery strategy
        if self.is_rtsp:
            # Log every 10 errors
            if self.metrics.consecutive_errors % 10 == 0:
                logger.warning(
                    f"⚠️ Stream {self.stream_id[:8]}: "
                    f"{self.metrics.consecutive_errors} consecutive errors"
                )
            
            # ✅ TIER 1: LIGHT RECOVERY (10-30 errors) - Buffer flush only
            if (self.metrics.consecutive_errors % 10 == 0 and
                self.metrics.consecutive_errors < 30):
                if await self._try_connection_recovery():
                    logger.info(f"✅ Tier 1 recovery successful for {self.stream_id[:8]}")
                    self.metrics.consecutive_errors = 0
                    return
            
            # ✅ TIER 2: MEDIUM RECOVERY (30-70 errors) - Full reconnection
            elif (self.metrics.consecutive_errors >= 30 and 
                  self.metrics.consecutive_errors < 70 and
                  self.metrics.consecutive_errors % 10 == 0):
                logger.warning(
                    f"🔄 Stream {self.stream_id[:8]}: Tier 2 - Forcing full reconnection "
                    f"({self.metrics.consecutive_errors} errors)"
                )
                if await self._force_full_reconnection():
                    logger.info(f"✅ Tier 2 reconnection successful for {self.stream_id[:8]}")
                    self.metrics.consecutive_errors = 0
                    return
            
            # ✅ TIER 3: HEAVY RECOVERY (70+ errors) - Emergency restart with backoff
            elif self.metrics.consecutive_errors >= 70:
                if self.metrics.consecutive_errors % 20 == 0:
                    logger.error(
                        f"🚨 Stream {self.stream_id[:8]}: Tier 3 - Emergency restart "
                        f"({self.metrics.consecutive_errors} errors)"
                    )
                    await self._safe_release_capture()
                    # Longer backoff for severe failures
                    await asyncio.sleep(10.0)
                    return
            
            # Progressive delay
            delay = min(1.0, 0.05 * (1.3 ** (self.metrics.consecutive_errors // 5)))
            await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0.1)
    
    # ==================== Connection Management ====================
    
    def _open_rtsp_source(self) -> bool:
        """Open RTSP source with TCP-first strategy"""
        self.metrics.connection_attempts += 1
        
        # Rate limiting
        current_time = time.time()
        if self._last_reconnect_time > 0:
            elapsed = current_time - self._last_reconnect_time
            if elapsed < self.config.min_reconnect_interval:
                wait = self.config.min_reconnect_interval - elapsed
                logger.info(
                    f"⏰ Rate limiting stream {self.stream_id[:8]}: waiting {wait:.1f}s"
                )
                time.sleep(wait)
        
        self._last_reconnect_time = time.time()
        
        logger.info(
            f"🔌 Opening RTSP for {self.stream_id[:8]} "
            f"(attempt #{self.metrics.connection_attempts})"
        )
        
        # ============================================================================
        # CRITICAL FIX #2: Use transport from config (TCP by default)
        # ============================================================================
        transport = "tcp" if self.config.prefer_tcp else "udp"
        
        try:
            # Build RTSP options string
            rtsp_options = (
                f"rtsp_transport;{transport}|"
                f"timeout;{self.config.rtsp_timeout * 1000000}|"
                f"stimeout;{self.config.rtsp_timeout * 1000000}|"
                f"max_delay;500000|"
                f"buffer_size;1048576|"
                f"reorder_queue_size;0|"
                f"fflags;+nobuffer+discardcorrupt|"
                f"flags;+low_delay|"
                f"analyzeduration;1000000|"
                f"probesize;500000"
            )
            
            # Set environment for THIS capture only
            # (Note: This is still global but we set it right before opening)
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = rtsp_options
            
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
        except Exception as e:
            logger.error(f"Failed to create VideoCapture for {self.stream_id[:8]}: {e}")
            return False
        
        if not self.cap or not self.cap.isOpened():
            logger.error(f"Failed to open RTSP stream {self.stream_id[:8]}")
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            return False
        
        # Set properties
        try:
            timeout_ms = self.config.rtsp_timeout * 1000
            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout_ms)
            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout_ms)
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, self.config.buffer_size)
        except Exception as e:
            logger.warning(f"Could not set properties for {self.stream_id[:8]}: {e}")
        
        # Test reads - REDUCED to 3 attempts for faster failure
        for attempt in range(3):
            try:
                ret, frame = self.cap.read()
                if ret and frame is not None and frame.size > 0:
                    logger.info(
                        f"✅ RTSP connected for {self.stream_id[:8]}: "
                        f"{frame.shape[1]}x{frame.shape[0]} via {transport.upper()}"
                    )
                    return True
                logger.warning(
                    f"Test read {attempt + 1} failed for {self.stream_id[:8]}"
                )
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Test read exception for {self.stream_id[:8]}: {e}")
                time.sleep(0.5)
        
        logger.error(
            f"❌ RTSP opened but cannot read frames for {self.stream_id[:8]}"
        )
        try:
            self.cap.release()
        except:
            pass
        self.cap = None
        return False
    
    async def _try_connection_recovery(self) -> bool:
        """Try to recover connection without full restart"""
        current_time = time.time()
        
        # Rate limit recovery attempts
        if current_time - self._last_recovery_attempt < self.config.recovery_interval:
            return False
        
        self._last_recovery_attempt = current_time
        self.metrics.recovery_attempts += 1
        
        logger.info(f"🔧 Attempting connection recovery for {self.stream_id[:8]}")
        
        loop = asyncio.get_event_loop()
        
        try:
            async with self.capture_lock:
                if not self.cap or not self.cap.isOpened():
                    return False
                
                # Flush buffer
                try:
                    await loop.run_in_executor(
                        self._thread_pool,
                        self.cap.set,
                        cv2.CAP_PROP_BUFFERSIZE,
                        1
                    )
                except:
                    pass
                
                # Discard buffered frames
                for _ in range(5):
                    try:
                        await loop.run_in_executor(self._thread_pool, self.cap.read)
                    except:
                        break
                
                # Test read
                ret, frame = await loop.run_in_executor(
                    self._thread_pool,
                    self.cap.read
                )
                if ret and frame is not None and frame.size > 0:
                    logger.info(f"✅ Recovery successful for {self.stream_id[:8]}")
                    self.metrics.consecutive_errors = 0
                    self.metrics.last_success_time = time.time()
                    return True
            
            return False
        
        except Exception as e:
            logger.error(f"Recovery error for {self.stream_id[:8]}: {e}")
            return False
    
    async def _force_full_reconnection(self) -> bool:
        """
        Force full reconnection by releasing and reopening the RTSP stream.
        This is stronger than buffer flushing and used for Tier 2 recovery.
        """
        logger.info(f"🔄 Forcing full reconnection for {self.stream_id[:8]}")
        
        try:
            # Release current connection
            await self._safe_release_capture()
            
            # Wait a moment for cleanup
            await asyncio.sleep(2.0)
            
            # Try to reopen
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self._thread_pool,
                self._open_video_source_sync
            )
            
            if success:
                logger.info(f"✅ Full reconnection successful for {self.stream_id[:8]}")
                self.metrics.consecutive_errors = 0
                self.metrics.last_success_time = time.time()
                self.metrics.recovery_attempts = 0
                return True
            else:
                logger.warning(f"⚠️ Full reconnection failed for {self.stream_id[:8]}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Full reconnection error for {self.stream_id[:8]}: {e}")
            return False
    
    async def _safe_release_capture(self) -> None:
        """Safely release video capture"""
        if self.cap is None:
            return
        
        try:
            async with self.capture_lock:
                if self.cap is not None:
                    try:
                        if self.cap.isOpened():
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                self._thread_pool,
                                self.cap.release
                            )
                            logger.debug(f"✓ Released capture for {self.stream_id[:8]}")
                    except Exception as e:
                        logger.warning(
                            f"Error releasing capture for {self.stream_id[:8]}: {e}"
                        )
                    finally:
                        self.cap = None
        except Exception as e:
            logger.error(f"Error in safe release for {self.stream_id[:8]}: {e}")
            self.cap = None
    
    # ==================== Utility Methods ====================
    
    def _calculate_backoff_delay(self) -> float:
        """Calculate exponential backoff delay"""
        return min(
            self.config.max_backoff_delay,
            self.config.min_backoff_delay * (
                self.config.backoff_multiplier ** (self.metrics.connection_attempts - 1)
            )
        )
    
    def _calculate_sleep_time(self) -> float:
        """Calculate adaptive sleep time based on source type"""
        if self.is_rtsp:
            return 0.01
        
        if self.is_file and self.cap:
            try:
                fps = self.cap.get(cv2.CAP_PROP_FPS)
                if fps > 0:
                    return 1.0 / min(fps, 30.0)
            except:
                pass
        
        return 0.033  # ~30 FPS default
    
    def _is_file_source(self, source: str) -> bool:
        """Check if source is a file"""
        return (not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and
                not source.isdigit())
    
    def _is_rtsp_source(self, source: str) -> bool:
        """Check if source is RTSP"""
        return source.startswith('rtsp://')
    
    def _validate_file(self) -> bool:
        """Validate file source"""
        if not os.path.exists(self.source):
            logger.error(f"File does not exist: {self.source}")
            return False
        
        if not os.access(self.source, os.R_OK):
            logger.error(f"File not readable: {self.source}")
            return False
        
        if os.path.getsize(self.source) == 0:
            logger.error(f"File is empty: {self.source}")
            return False
        
        return True
    
    def _configure_rtsp_environment(self) -> None:
        """Configure RTSP environment variables"""
        os.environ['OPENCV_FFMPEG_LOGLEVEL'] = '-8'
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;udp|'
            f'timeout;{self.config.rtsp_timeout * 1000000}|'
            f'stimeout;{self.config.rtsp_timeout * 1000000}|'
            f'max_delay;1000000|'
            f'buffer_size;2097152|'
            f'reorder_queue_size;0|'
            f'fflags;+genpts+igndts+discardcorrupt+nobuffer|'
            f'flags;+low_delay|'
            f'analyzeduration;1000000|'
            f'probesize;1000000|'
            f'err_detect;ignore_err'
        )
        os.environ['OPENCV_FFMPEG_THREAD_COUNT'] = '1'
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        async with self.lock:
            return {
                'source': self.source,
                'state': self.state.value,
                'subscriber_count': len(self.subscribers),
                'metrics': {
                    'total_frames': self.metrics.total_frames,
                    'connection_attempts': self.metrics.connection_attempts,
                    'consecutive_errors': self.metrics.consecutive_errors,
                    'last_success': self.metrics.last_success_time,
                    'last_error': self.metrics.last_error,
                    'uptime': time.time() - self.metrics.uptime_start if self.metrics.uptime_start else 0
                },
                'subscribers': {
                    sub.stream_id: {
                        'frames_received': sub.frames_received,
                        'last_frame_time': sub.last_frame_time
                    }
                    for sub in self.subscribers.values()
                }
            }

    async def _ensure_video_source(self) -> bool:
        """
        Ensure video source is open and ready.
        
        ✅ CRITICAL: This method must be defined BEFORE _capture_loop() calls it
        """
        if self.cap is not None and self.cap.isOpened():
            return True
        
        return await self._open_video_source_async()

    async def _open_video_source_async(self) -> bool:
        """Open video source asynchronously"""
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                self._thread_pool,
                self._open_video_source_sync
            )
        except Exception as e:
            logger.error(f"Error opening source for {self.stream_id[:8]}: {e}")
            return False

    def _open_video_source_sync(self) -> bool:
        """Synchronous video source opening (runs in executor)"""
        try:
            # Clean up existing capture
            if self.cap is not None:
                try:
                    if self.cap.isOpened():
                        self.cap.release()
                except:
                    pass
                self.cap = None
            
            time.sleep(0.2)
            
            # File validation
            if self.is_file and not self._validate_file():
                return False
            
            # RTSP opening
            if self.is_rtsp:
                return self._open_rtsp_source()
            
            # File opening
            return self._open_file_source()
        
        except Exception as e:
            logger.error(
                f"Critical error opening source for {self.stream_id[:8]}: {e}",
                exc_info=True
            )
            if self.cap is not None:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            return False

    def _open_file_source(self) -> bool:
        """Open file source with backend fallback"""
        backends = [cv2.CAP_FFMPEG, cv2.CAP_ANY]
        
        for backend in backends:
            try:
                self.cap = cv2.VideoCapture(self.source, backend)
                
                if not self.cap or not self.cap.isOpened():
                    if self.cap:
                        try:
                            self.cap.release()
                        except:
                            pass
                    self.cap = None
                    continue
                
                self.cap.set(cv2.CAP_PROP_BUFFERSIZE, self.config.buffer_size)
                
                # Test read
                ret, frame = self.cap.read()
                if not ret or frame is None:
                    try:
                        self.cap.release()
                    except:
                        pass
                    self.cap = None
                    continue
                
                # Reset to start
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                
                logger.info(
                    f"✅ Opened file {self.stream_id[:8]} with backend {backend}"
                )
                return True
            
            except Exception as e:
                logger.warning(
                    f"Backend {backend} failed for {self.stream_id[:8]}: {e}"
                )
                if self.cap:
                    try:
                        self.cap.release()
                    except:
                        pass
                    self.cap = None
        
        return False
        

class VideoFileManager:
    """Manages shared video streams"""
    
    def __init__(self):
        self.shared_streams: Dict[str, SharedVideoStream] = {}
        self.lock = asyncio.Lock()
    
    async def get_shared_stream(
        self,
        source: str,
        stream_id: Optional[Union[str, UUID, int]] = None,  # ✅ Accept multiple types
        max_subscribers: int = 10,
    ) -> SharedVideoStream:
        """
        Get or create a shared stream.
        
        ✅ CRITICAL FIX: Properly handle stream_id of any type
        """
        async with self.lock:
            if source not in self.shared_streams:
                # Generate or normalize stream_id
                if stream_id is None:
                    import hashlib
                    stream_id_str = hashlib.md5(source.encode()).hexdigest()
                elif isinstance(stream_id, UUID):
                    stream_id_str = str(stream_id)
                elif isinstance(stream_id, int):
                    # ✅ FIX: Convert integer to string hash
                    import hashlib
                    stream_id_str = hashlib.md5(f"{source}_{stream_id}".encode()).hexdigest()
                else:
                    stream_id_str = str(stream_id)
                
                logger.info(
                    f"🔨 Creating shared stream: source={source}, "
                    f"stream_id={stream_id_str[:8]} (from {type(stream_id).__name__})"
                )
                
                self.shared_streams[source] = SharedVideoStream(
                    source=source,
                    stream_id=stream_id_str,  # ✅ Always pass string
                    max_subscribers=max_subscribers
                )
            return self.shared_streams[source]
    
    async def remove_shared_stream(self, source: str) -> None:
        """Remove a shared stream"""
        async with self.lock:
            if source in self.shared_streams:
                stream = self.shared_streams[source]
                if stream.state in (StreamState.RUNNING, StreamState.CONNECTING):
                    await stream._stop_capture()
                del self.shared_streams[source]
                logger.info(f"Removed shared stream: {source}")
    
    async def cleanup_empty_streams(self) -> None:
        """Clean up streams with no subscribers"""
        async with self.lock:
            empty = [
                source for source, stream in self.shared_streams.items()
                if not stream.subscribers
            ]
            
            for source in empty:
                await self.remove_shared_stream(source)
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all streams"""
        async with self.lock:
            return {
                source: await stream.get_stats()
                for source, stream in self.shared_streams.items()
            }
    
    async def force_restart_stream(self, source: str) -> bool:
        """Force restart a stream"""
        try:
            async with self.lock:
                if source not in self.shared_streams:
                    return False
                
                stream = self.shared_streams[source]
                affected_ids = list(stream.subscribers.keys())
                
                await stream._stop_capture()
                await asyncio.sleep(2.0)
                
                # Subscribers will trigger restart when they request frames
                logger.info(
                    f"🔄 Restarted stream: {source}, "
                    f"affected subscribers: {affected_ids}"
                )
                return True
        
        except Exception as e:
            logger.error(f"Error restarting stream: {e}", exc_info=True)
            return False


# Global instance
video_file_manager = VideoFileManager()