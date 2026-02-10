# app/services/shared_stream_service.py
# 🎯 PRODUCTION-HARDENED VERSION - GPU/CPU fallback + RTSP fixes
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
# GPU Processor Class (if you have one)
# ============================================================================
class GPUFrameProcessor:
    """GPU-based frame processor with CPU fallback"""
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.gpu_available = False
        self.processor = None
        
        try:
            # Try to initialize GPU
            import torch
            if torch.cuda.is_available():
                self.gpu_available = True
                logger.info(f"✅ GPU available for {stream_id[:8]}")
            else:
                logger.warning(f"⚠️ GPU not available for {stream_id[:8]}, using CPU")
        except ImportError:
            logger.warning(f"⚠️ PyTorch not installed for {stream_id[:8]}, using CPU")
        except Exception as e:
            logger.error(f"❌ GPU initialization failed for {stream_id[:8]}: {e}")
    
    def process(self, frame: np.ndarray) -> np.ndarray:
        """Process frame (GPU or CPU)"""
        # Your processing logic here
        return frame


# ============================================================================
# Per-stream thread pools to prevent saturation
# ============================================================================
class StreamThreadPool:
    """Manages isolated thread pool per stream"""
    _pools: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_pool(cls, stream_id: str) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create dedicated pool for stream"""
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
    ZOMBIE = "zombie"


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
    Production-hardened shared video stream with GPU/CPU fallback
    """
    
    def __init__(
        self,
        source: str,
        stream_id: str,
        max_subscribers: int = 10,
        use_gpu: bool = False,  # ✅ NEW: GPU option
    ):
        self.source = source
        
        # ✅ Ensure stream_id is always a string
        if isinstance(stream_id, UUID):
            self.stream_id = str(stream_id)
        elif isinstance(stream_id, int):
            import hashlib
            self.stream_id = hashlib.md5(f"{source}_{stream_id}".encode()).hexdigest()
            logger.warning(
                f"⚠️ Integer stream_id={stream_id} converted to {self.stream_id[:8]}"
            )
        else:
            self.stream_id = str(stream_id)
    
        self.max_subscribers = max_subscribers
        self.config = config
        
        # ✅ GPU initialization with fallback
        self.use_gpu = use_gpu
        self.gpu_processor = None
        if use_gpu:
            try:
                self.gpu_processor = GPUFrameProcessor(self.stream_id)
                logger.info(f"🎮 GPU processor initialized for {self.stream_id[:8]}")
            except Exception as e:
                logger.error(f"❌ GPU init failed for {self.stream_id[:8]}: {e}, falling back to CPU")
                self.use_gpu = False
        
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
        
        # Thread pool
        self._thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
        
        logger.info(
            f"📹 Created SharedVideoStream: id={self.stream_id[:8]}, source={source}, "
            f"type={'RTSP' if self.is_rtsp else 'file' if self.is_file else 'other'}, "
            f"GPU={'enabled' if use_gpu else 'disabled'}"
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
                logger.warning(f"❌ Max subscribers ({self.max_subscribers}) reached for {self.stream_id[:8]}")
                return False
            
            self.subscribers[subscriber_id] = Subscriber(
                stream_id=subscriber_id,
                callback_info=callback_info or {}
            )
            
            logger.info(
                f"➕ Added subscriber {subscriber_id[:8]} to {self.stream_id[:8]}. Total: {len(self.subscribers)}"
            )
            
            # Start capture if first subscriber
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
                f"➖ Removed subscriber {subscriber_id[:8]} from {self.stream_id[:8]}. "
                f"Remaining: {len(self.subscribers)}"
            )
            
            # ✅ CRITICAL FIX: Stop capture when no subscribers remain
            if not self.subscribers:
                logger.info(f"🛑 No subscribers left for {self.stream_id[:8]}, stopping capture loop")
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
        """Wait for a new frame"""
        try:
            await asyncio.wait_for(self.frame_available.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    # ==================== Capture Management ====================
    
    async def _start_capture(self) -> None:
        """Start the capture loop"""
        if self.state in (StreamState.RUNNING, StreamState.CONNECTING):
            logger.warning(f"⚠️ Capture already running for {self.stream_id[:8]}")
            return
        
        self.state = StreamState.CONNECTING
        self.stop_event.clear()
        self.metrics.uptime_start = time.time()
        
        # Get dedicated thread pool
        self._thread_pool = await StreamThreadPool.get_pool(self.stream_id)
        
        # Start capture task
        self.capture_task = asyncio.create_task(self._capture_loop())
        self.capture_task.set_name(f"capture_{self.stream_id[:8]}")
        
        # Start health monitor
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        self.health_monitor_task.set_name(f"health_{self.stream_id[:8]}")
        
        logger.info(f"▶️ Started capture for {self.stream_id[:8]}")
    
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
        
        # Release capture
        await self._safe_release_capture()
        
        # Wait for capture task
        if self.capture_task and not self.capture_task.done():
            self.capture_task.cancel()
            try:
                timeout = 10.0 if self.is_rtsp else 5.0
                await asyncio.wait_for(self.capture_task, timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning(f"⚠️ Capture task timeout for {self.stream_id[:8]}")
        
        self.capture_task = None
        
        # Cleanup thread pool
        await StreamThreadPool.cleanup_pool(self.stream_id)
        self._thread_pool = None
        
        logger.info(f"✅ Capture stopped for {self.stream_id[:8]}")
    
    # ==================== Health Monitoring ====================

    async def _health_monitor_loop(self) -> None:
        """Monitor stream health"""
        logger.info(f"🏥 Health monitor started for {self.stream_id[:8]}")
        
        startup_grace_period = 300.0 if self.is_rtsp else 60.0
        running_zombie_timeout = 600.0 if self.is_rtsp else 120.0
        
        startup_time = time.time()
        first_frame_received = False
        
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(30.0)
                
                # ✅ CRITICAL: Check if subscribers left during monitoring
                async with self.lock:
                    if not self.subscribers:
                        logger.info(f"🛑 No subscribers detected by health monitor for {self.stream_id[:8]}")
                        break
                
                if self.state not in (StreamState.RUNNING, StreamState.CONNECTING):
                    continue
                
                now = time.time()
                
                if self.metrics.last_success_time:
                    if not first_frame_received:
                        first_frame_received = True
                        logger.info(f"✅ First frame received for {self.stream_id[:8]} after {now - startup_time:.1f}s")
                    
                    time_since_frame = now - self.metrics.last_success_time
                    
                    if time_since_frame > running_zombie_timeout:
                        logger.error(
                            f"☠️ ZOMBIE: {self.stream_id[:8]} - no frames for {time_since_frame:.0f}s"
                        )
                        self.state = StreamState.ZOMBIE
                        await self._emergency_cleanup()
                        break
                    
                    elif time_since_frame > running_zombie_timeout * 0.5:
                        logger.warning(f"⚠️ {self.stream_id[:8]} stagnant for {time_since_frame:.0f}s, triggering recovery")
                        try:
                            await self._force_full_reconnection()
                        except Exception as e:
                            logger.error(f"💥 Recovery error: {e}")
                
                else:
                    time_since_startup = now - startup_time
                    
                    if time_since_startup > startup_grace_period:
                        logger.error(f"☠️ STARTUP TIMEOUT: {self.stream_id[:8]} - no frames after {time_since_startup:.0f}s")
                        self.state = StreamState.ZOMBIE
                        await self._emergency_cleanup()
                        break
                
                self.metrics.last_heartbeat = now
        
        except asyncio.CancelledError:
            logger.info(f"🏥 Health monitor cancelled for {self.stream_id[:8]}")
        except Exception as e:
            logger.error(f"💥 Health monitor error for {self.stream_id[:8]}: {e}")


    async def _emergency_cleanup(self) -> None:
        """Emergency cleanup when zombie detected"""
        logger.warning(f"🚨 Emergency cleanup for {self.stream_id[:8]}")
        
        try:
            await self._safe_release_capture()
            
            if self.capture_task and not self.capture_task.done():
                self.capture_task.cancel()
            
            self.state = StreamState.FAILED
            self.metrics.last_error = "Zombie state - no frames received"
        
        except Exception as e:
            logger.error(f"💥 Emergency cleanup error: {e}")
    
    # ==================== Capture Loop ====================
    
    async def _capture_loop(self) -> None:
        """Main capture loop"""
        logger.info(f"🎬 Capture loop started for {self.stream_id[:8]}")
        
        frame_interval = 1.0 / self.config.target_fps
        last_frame_time = 0.0
        last_success_time = time.time()
        
        try:
            while not self.stop_event.is_set():
                # ✅ CRITICAL FIX: Check if all subscribers left
                async with self.lock:
                    if not self.subscribers:
                        logger.info(f"🛑 No subscribers left for {self.stream_id[:8]}, exiting capture loop")
                        break
                
                # FPS throttling
                now = time.time()
                if now - last_frame_time < frame_interval:
                    await asyncio.sleep(0.01)
                    continue
                last_frame_time = now
                
                # Ensure video source
                if not await self._ensure_video_source():
                    delay = self._calculate_backoff_delay()
                    logger.warning(f"⏳ Waiting {delay:.1f}s before retry (attempt {self.metrics.connection_attempts})")
                    
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                        break
                    except asyncio.TimeoutError:
                        pass
                    
                    continue
                
                # Timeout check for RTSP
                if self.is_rtsp:
                    elapsed = time.time() - last_success_time
                    if elapsed > self.config.max_read_timeout:
                        logger.error(f"❌ Read timeout for {self.stream_id[:8]} ({elapsed:.0f}s)")
                        
                        if self.metrics.recovery_attempts < 3:
                            if await self._try_connection_recovery():
                                last_success_time = time.time()
                                continue
                        
                        logger.warning(f"♻️ Forcing reconnection for {self.stream_id[:8]}")
                        await self._safe_release_capture()
                        continue
                
                # Read frame
                ret, frame = await self._read_frame_safe()
                
                if self.stop_event.is_set():
                    break
                
                # Handle failures
                if not ret or frame is None or frame.size == 0:
                    await self._handle_read_failure()
                    continue
                
                if not self._validate_frame(frame):
                    await self._handle_read_failure()
                    continue
                
                # Success
                await self._process_successful_frame(frame)
                last_success_time = time.time()
                
                # Adaptive sleep
                sleep_time = self._calculate_sleep_time()
                if sleep_time > 0:
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=sleep_time)
                        break
                    except asyncio.TimeoutError:
                        pass
        
        except asyncio.CancelledError:
            logger.info(f"🛑 Capture task cancelled for {self.stream_id[:8]}")
        except Exception as e:
            logger.error(f"💥 Fatal error in capture loop for {self.stream_id[:8]}: {e}", exc_info=True)
            self.state = StreamState.FAILED
            self.metrics.last_error = str(e)
        finally:
            await self._safe_release_capture()
            logger.info(
                f"📊 Capture ended for {self.stream_id[:8]}: "
                f"frames={self.metrics.total_frames}, attempts={self.metrics.connection_attempts}"
            )
    
    # ==================== Frame Reading ====================
    
    async def _read_frame_safe(self) -> tuple[bool, Optional[np.ndarray]]:
        """Thread-safe frame reading"""
        async with self.capture_lock:
            if (self.stop_event.is_set() or
                self.cap is None or
                not self.cap.isOpened() or
                self._thread_pool is None):
                return False, None
            
            try:
                loop = asyncio.get_event_loop()
                ret, frame = await loop.run_in_executor(self._thread_pool, self.cap.read)
                return ret, frame
            except Exception as e:
                logger.error(f"❌ Read exception for {self.stream_id[:8]}: {e}")
                return False, None
    
    def _validate_frame(self, frame: np.ndarray) -> bool:
        """Validate frame"""
        if frame is None or frame.size == 0:
            return False
        if len(frame.shape) != 3:
            return False
        if frame.shape[0] < 10 or frame.shape[1] < 10:
            return False
        return True
    
    async def _process_successful_frame(self, frame: np.ndarray) -> None:
        """Process successful frame"""
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
        
        # GPU processing if enabled
        if self.use_gpu and self.gpu_processor:
            try:
                frame = self.gpu_processor.process(frame)
            except Exception as e:
                logger.error(f"❌ GPU processing failed for {self.stream_id[:8]}: {e}, using raw frame")
        
        # Update latest frame
        async with self.lock:
            self.latest_frame = frame
            self.frame_available.set()
            self.frame_available.clear()
        
        # Log progress
        if self.metrics.total_frames % 100 == 0:
            logger.debug(f"📊 {self.stream_id[:8]}: {self.metrics.total_frames} frames")
    
    async def _handle_read_failure(self) -> None:
        """Handle frame read failure"""
        self.metrics.consecutive_errors += 1
        self.metrics.last_error_time = time.time()
        
        # Emergency restart at max errors
        if self.metrics.consecutive_errors >= self.config.max_consecutive_errors:
            logger.error(f"❌ Max errors ({self.metrics.consecutive_errors}) for {self.stream_id[:8]}, forcing restart")
            self.state = StreamState.FAILED
            await self._safe_release_capture()
            self.metrics.consecutive_errors = 0
            self.metrics.recovery_attempts = 0
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
                logger.warning(f"⚠️ Loop error for {self.stream_id[:8]}: {e}")
        
        # RTSP recovery
        if self.is_rtsp:
            if self.metrics.consecutive_errors % 10 == 0:
                logger.warning(f"⚠️ {self.stream_id[:8]}: {self.metrics.consecutive_errors} consecutive errors")
            
            # Tier 1: Light recovery (10-30 errors)
            if self.metrics.consecutive_errors % 10 == 0 and self.metrics.consecutive_errors < 30:
                if await self._try_connection_recovery():
                    logger.info(f"✅ Tier 1 recovery successful for {self.stream_id[:8]}")
                    self.metrics.consecutive_errors = 0
                    return
            
            # Tier 2: Full reconnection (30-70 errors)
            elif self.metrics.consecutive_errors >= 30 and self.metrics.consecutive_errors < 70:
                if self.metrics.consecutive_errors % 10 == 0:
                    logger.warning(f"🔄 Tier 2 reconnection for {self.stream_id[:8]}")
                    if await self._force_full_reconnection():
                        logger.info(f"✅ Tier 2 successful for {self.stream_id[:8]}")
                        self.metrics.consecutive_errors = 0
                        return
            
            # Tier 3: Emergency (70+ errors)
            elif self.metrics.consecutive_errors >= 70:
                if self.metrics.consecutive_errors % 20 == 0:
                    logger.error(f"🚨 Tier 3 emergency for {self.stream_id[:8]}")
                    await self._safe_release_capture()
                    await asyncio.sleep(10.0)
                    return
            
            delay = min(1.0, 0.05 * (1.3 ** (self.metrics.consecutive_errors // 5)))
            await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0.1)
    
    # ==================== Connection Management ====================

    def _open_rtsp_source(self) -> bool:
        """
        Open RTSP source with MINIMAL options (proven to work with your cameras)
        """
        self.metrics.connection_attempts += 1
        
        # Rate limiting
        current_time = time.time()
        if self._last_reconnect_time > 0:
            elapsed = current_time - self._last_reconnect_time
            if elapsed < self.config.min_reconnect_interval:
                wait = self.config.min_reconnect_interval - elapsed
                logger.info(f"⏰ Rate limiting {self.stream_id[:8]}: waiting {wait:.1f}s")
                time.sleep(wait)
        
        self._last_reconnect_time = time.time()
        
        logger.info(f"🔌 Opening RTSP (attempt #{self.metrics.connection_attempts})")
        
        # ✅ CRITICAL FIX: Use MINIMAL FFMPEG options (proven to work)
        # Your simple test script works because it doesn't set any special options
        # So we'll do the same here - only set what's absolutely necessary
        
        masked_url = self.source
        if '@' in self.source:
            parts = self.source.split('@')
            if '://' in parts[0]:
                scheme_user = parts[0].split('://')
                masked_url = f"{scheme_user[0]}://***:***@{parts[1]}"
        
        logger.info(f"📡 Connecting to: {masked_url}")
        
        try:
            # ✅ STRATEGY 1: Try with NO options first (like your working test)
            # Clear any previous FFMPEG options
            if 'OPENCV_FFMPEG_CAPTURE_OPTIONS' in os.environ:
                del os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS']
            
            logger.info(f"   Strategy 1: Trying with default options...")
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
            
            if self.cap and self.cap.isOpened():
                # Test read
                for attempt in range(3):
                    try:
                        ret, frame = self.cap.read()
                        if ret and frame is not None and frame.size > 0:
                            logger.info(
                                f"✅ RTSP connected (default options): {self.stream_id[:8]} - "
                                f"{frame.shape[1]}x{frame.shape[0]}"
                            )
                            
                            # Set minimal properties AFTER opening
                            try:
                                timeout_ms = self.config.rtsp_timeout * 1000
                                self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout_ms)
                                self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                            except:
                                pass
                            
                            return True
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"   Test read {attempt + 1} failed: {e}")
                        time.sleep(0.3)
            
            # Strategy 1 failed, clean up
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            
            # ✅ STRATEGY 2: Try with MINIMAL options (only timeout)
            logger.info(f"   Strategy 2: Trying with minimal options (timeout only)...")
            
            timeout_us = self.config.rtsp_timeout * 1000000  # microseconds
            rtsp_options = (
                f"rtsp_transport;tcp|"
                f"timeout;{timeout_us}|"
                f"stimeout;{timeout_us}"
            )
            
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = rtsp_options
            
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
            
            if self.cap and self.cap.isOpened():
                # Test read
                for attempt in range(3):
                    try:
                        ret, frame = self.cap.read()
                        if ret and frame is not None and frame.size > 0:
                            logger.info(
                                f"✅ RTSP connected (minimal options): {self.stream_id[:8]} - "
                                f"{frame.shape[1]}x{frame.shape[0]} via TCP"
                            )
                            
                            # Set properties
                            try:
                                timeout_ms = self.config.rtsp_timeout * 1000
                                self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout_ms)
                                self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                            except:
                                pass
                            
                            return True
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"   Test read {attempt + 1} failed: {e}")
                        time.sleep(0.3)
            
            # Strategy 2 failed, clean up
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            
            # ✅ STRATEGY 3: Last resort - try UDP
            logger.info(f"   Strategy 3: Trying UDP transport...")
            
            rtsp_options = (
                f"rtsp_transport;udp|"
                f"timeout;{timeout_us}|"
                f"stimeout;{timeout_us}"
            )
            
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = rtsp_options
            
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
            
            if self.cap and self.cap.isOpened():
                # Test read
                for attempt in range(3):
                    try:
                        ret, frame = self.cap.read()
                        if ret and frame is not None and frame.size > 0:
                            logger.info(
                                f"✅ RTSP connected (UDP): {self.stream_id[:8]} - "
                                f"{frame.shape[1]}x{frame.shape[0]}"
                            )
                            
                            # Set properties
                            try:
                                timeout_ms = self.config.rtsp_timeout * 1000
                                self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout_ms)
                                self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                            except:
                                pass
                            
                            return True
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"   Test read {attempt + 1} failed: {e}")
                        time.sleep(0.3)
            
            # All strategies failed
            logger.error(f"❌ All connection strategies failed for {self.stream_id[:8]}")
            
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            
            return False
            
        except Exception as e:
            logger.error(f"❌ VideoCapture creation failed for {self.stream_id[:8]}: {e}")
            self.metrics.last_error = f"VideoCapture creation error: {str(e)}"
            
            if self.cap:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            
            return False    
    
    async def _try_connection_recovery(self) -> bool:
        """Try connection recovery"""
        current_time = time.time()
        
        if current_time - self._last_recovery_attempt < self.config.recovery_interval:
            return False
        
        self._last_recovery_attempt = current_time
        self.metrics.recovery_attempts += 1
        
        logger.info(f"🔧 Attempting recovery for {self.stream_id[:8]}")
        
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
                ret, frame = await loop.run_in_executor(self._thread_pool, self.cap.read)
                if ret and frame is not None and frame.size > 0:
                    logger.info(f"✅ Recovery successful for {self.stream_id[:8]}")
                    self.metrics.consecutive_errors = 0
                    self.metrics.last_success_time = time.time()
                    return True
            
            return False
        
        except Exception as e:
            logger.error(f"❌ Recovery error for {self.stream_id[:8]}: {e}")
            return False
    
    async def _force_full_reconnection(self) -> bool:
        """Force full reconnection"""
        logger.info(f"🔄 Forcing full reconnection for {self.stream_id[:8]}")
        
        try:
            await self._safe_release_capture()
            await asyncio.sleep(2.0)
            
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
            logger.error(f"💥 Full reconnection error: {e}")
            return False
    
    async def _safe_release_capture(self) -> None:
        """Safely release capture"""
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
                        logger.warning(f"⚠️ Error releasing: {e}")
                    finally:
                        self.cap = None
        except Exception as e:
            logger.error(f"❌ Safe release error: {e}")
            self.cap = None
    
    # ==================== Utility Methods ====================
    
    def _calculate_backoff_delay(self) -> float:
        """Calculate backoff delay"""
        return min(
            self.config.max_backoff_delay,
            self.config.min_backoff_delay * (
                self.config.backoff_multiplier ** (self.metrics.connection_attempts - 1)
            )
        )
    
    def _calculate_sleep_time(self) -> float:
        """Calculate sleep time"""
        if self.is_rtsp:
            return 0.01
        
        if self.is_file and self.cap:
            try:
                fps = self.cap.get(cv2.CAP_PROP_FPS)
                if fps > 0:
                    return 1.0 / min(fps, 30.0)
            except:
                pass
        
        return 0.033
    
    def _is_file_source(self, source: str) -> bool:
        """Check if file source"""
        return (not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and
                not source.isdigit())
    
    def _is_rtsp_source(self, source: str) -> bool:
        """Check if RTSP source"""
        return source.startswith('rtsp://')
    
    def _validate_file(self) -> bool:
        """Validate file source"""
        if not os.path.exists(self.source):
            logger.error(f"❌ File does not exist: {self.source}")
            return False
        
        if not os.access(self.source, os.R_OK):
            logger.error(f"❌ File not readable: {self.source}")
            return False
        
        if os.path.getsize(self.source) == 0:
            logger.error(f"❌ File is empty: {self.source}")
            return False
        
        return True
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get stats"""
        async with self.lock:
            return {
                'source': self.source,
                'state': self.state.value,
                'subscriber_count': len(self.subscribers),
                'gpu_enabled': self.use_gpu,
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
        """Ensure video source is open"""
        if self.cap is not None and self.cap.isOpened():
            return True
        
        return await self._open_video_source_async()

    async def _open_video_source_async(self) -> bool:
        """Open video source async"""
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                self._thread_pool,
                self._open_video_source_sync
            )
        except Exception as e:
            logger.error(f"❌ Error opening source: {e}")
            return False

    def _open_video_source_sync(self) -> bool:
        """Open video source sync"""
        try:
            # Cleanup existing
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
            
            # Open based on type
            if self.is_rtsp:
                return self._open_rtsp_source()
            
            return self._open_file_source()
        
        except Exception as e:
            logger.error(f"💥 Critical error opening source: {e}", exc_info=True)
            if self.cap is not None:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            return False

    def _open_file_source(self) -> bool:
        """Open file source"""
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
                
                logger.info(f"✅ Opened file {self.stream_id[:8]} with backend {backend}")
                return True
            
            except Exception as e:
                logger.warning(f"⚠️ Backend {backend} failed: {e}")
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
        stream_id: Optional[Union[str, UUID, int]] = None,
        max_subscribers: int = 10,
        use_gpu: bool = False,  # ✅ NEW: GPU option
    ) -> SharedVideoStream:
        """Get or create shared stream"""
        async with self.lock:
            if source not in self.shared_streams:
                # Normalize stream_id
                if stream_id is None:
                    import hashlib
                    stream_id_str = hashlib.md5(source.encode()).hexdigest()
                elif isinstance(stream_id, UUID):
                    stream_id_str = str(stream_id)
                elif isinstance(stream_id, int):
                    import hashlib
                    stream_id_str = hashlib.md5(f"{source}_{stream_id}".encode()).hexdigest()
                else:
                    stream_id_str = str(stream_id)
                
                logger.info(
                    f"🔨 Creating stream: {stream_id_str[:8]}, "
                    f"GPU={'on' if use_gpu else 'off'}"
                )
                
                self.shared_streams[source] = SharedVideoStream(
                    source=source,
                    stream_id=stream_id_str,
                    max_subscribers=max_subscribers,
                    use_gpu=use_gpu,  # ✅ Pass GPU option
                )
            return self.shared_streams[source]
    
    async def remove_shared_stream(self, source: str) -> None:
        """Remove shared stream"""
        async with self.lock:
            if source in self.shared_streams:
                stream = self.shared_streams[source]
                if stream.state in (StreamState.RUNNING, StreamState.CONNECTING):
                    await stream._stop_capture()
                del self.shared_streams[source]
                logger.info(f"🗑️ Removed shared stream: {source}")
    
    async def cleanup_empty_streams(self) -> None:
        """Clean up empty streams"""
        async with self.lock:
            empty = [
                source for source, stream in self.shared_streams.items()
                if not stream.subscribers
            ]
            
            for source in empty:
                await self.remove_shared_stream(source)
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get all stats"""
        async with self.lock:
            return {
                source: await stream.get_stats()
                for source, stream in self.shared_streams.items()
            }
    
    async def force_restart_stream(self, source: str) -> bool:
        """Force restart stream"""
        try:
            async with self.lock:
                if source not in self.shared_streams:
                    return False
                
                stream = self.shared_streams[source]
                affected_ids = list(stream.subscribers.keys())
                
                await stream._stop_capture()
                await asyncio.sleep(2.0)
                
                logger.info(f"🔄 Restarted stream: {source}, affected: {affected_ids}")
                return True
        
        except Exception as e:
            logger.error(f"❌ Error restarting stream: {e}", exc_info=True)
            return False


# Global instance
video_file_manager = VideoFileManager()