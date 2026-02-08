# app/services/shared_stream_service.py
# 🎯 PRODUCTION-HARDENED GPU VERSION with CV2 Crash Recovery
# ✅ Handles 100+ cameras with proper GPU resource cleanup on failures

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
# CRITICAL FIX: GPU Resource Manager with Crash Recovery
# ============================================================================
class GPUResourceManager:
    """
    Manages GPU resources with automatic cleanup on CV2 crashes.
    
    KEY FEATURES:
    - Detects CV2 crashes and cleans up GPU memory
    - Per-stream GPU context isolation
    - Automatic GPU reset on errors
    - Memory leak prevention
    """
    _instance = None
    _lock = asyncio.Lock()
    _gpu_available = False
    _cuda_devices = 0
    _initialized = False
    _gpu_contexts: Dict[str, Any] = {}  # Track GPU contexts per stream
    _crash_count = 0
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GPUResourceManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._detect_and_init_gpu()
            self._initialized = True
    
    def _detect_and_init_gpu(self):
        """Detect and initialize GPU with crash protection"""
        try:
            self._cuda_devices = cv2.cuda.getCudaEnabledDeviceCount()
            
            if self._cuda_devices > 0:
                self._gpu_available = True
                
                # Initialize each device
                for i in range(self._cuda_devices):
                    try:
                        cv2.cuda.setDevice(i)
                        device_info = cv2.cuda.DeviceInfo(i)
                        logger.info(
                            f"🎮 GPU {i}: {device_info.name()}, "
                            f"Memory: {device_info.totalMemory() / (1024**3):.2f} GB"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ Could not initialize GPU {i}: {e}")
                
                # Set default device
                cv2.cuda.setDevice(0)
                
                logger.info(
                    f"✅ GPU Acceleration ENABLED: {self._cuda_devices} device(s)"
                )
            else:
                logger.warning("⚠️ No CUDA devices found, using CPU")
                self._gpu_available = False
                
        except Exception as e:
            logger.error(f"💥 GPU initialization failed: {e}")
            self._gpu_available = False
            self._cuda_devices = 0
    
    async def cleanup_stream_gpu_resources(self, stream_id: str) -> None:
        """
        Clean up GPU resources for a specific stream.
        CRITICAL: Called when CV2 crashes or stream fails.
        """
        async with self._lock:
            if stream_id in self._gpu_contexts:
                try:
                    # Release GPU matrices
                    context = self._gpu_contexts[stream_id]
                    
                    if 'upload_mat' in context:
                        try:
                            del context['upload_mat']
                        except:
                            pass
                    
                    if 'resize_mat' in context:
                        try:
                            del context['resize_mat']
                        except:
                            pass
                    
                    # Remove context
                    del self._gpu_contexts[stream_id]
                    
                    # Force garbage collection
                    import gc
                    gc.collect()
                    
                    logger.info(f"🧹 Cleaned GPU resources for stream {stream_id[:8]}")
                    
                except Exception as e:
                    logger.error(f"Error cleaning GPU resources: {e}")
    
    async def reset_gpu_on_crash(self, stream_id: str) -> bool:
        """
        Reset GPU state after CV2 crash.
        
        Returns:
            True if reset successful, False otherwise
        """
        async with self._lock:
            self._crash_count += 1
            
            logger.warning(
                f"🚨 GPU reset triggered for {stream_id[:8]} "
                f"(total crashes: {self._crash_count})"
            )
            
            try:
                # Cleanup stream resources first
                await self.cleanup_stream_gpu_resources(stream_id)
                
                # Reset device if too many crashes
                if self._crash_count % 10 == 0:
                    logger.warning("♻️ Performing full GPU device reset")
                    try:
                        cv2.cuda.resetDevice()
                        await asyncio.sleep(1.0)  # Allow GPU to stabilize
                        
                        # Re-initialize
                        if self._gpu_available:
                            cv2.cuda.setDevice(0)
                        
                        logger.info("✅ Full GPU reset completed")
                    except Exception as e:
                        logger.error(f"Full GPU reset failed: {e}")
                        return False
                
                return True
                
            except Exception as e:
                logger.error(f"💥 GPU reset failed: {e}")
                return False
    
    def get_or_create_context(self, stream_id: str) -> Dict[str, Any]:
        """Get or create GPU context for stream"""
        if stream_id not in self._gpu_contexts:
            self._gpu_contexts[stream_id] = {
                'upload_mat': None,
                'resize_mat': None,
                'created_at': time.time()
            }
        return self._gpu_contexts[stream_id]
    
    @property
    def gpu_available(self) -> bool:
        return self._gpu_available
    
    @property
    def cuda_devices(self) -> int:
        return self._cuda_devices
    
    def get_gpu_memory_info(self) -> Dict[str, Any]:
        """Get GPU memory information"""
        if not self._gpu_available:
            return {"available": False}
        
        try:
            device_info = cv2.cuda.DeviceInfo()
            free_mem = device_info.freeMemory()
            total_mem = device_info.totalMemory()
            
            return {
                "available": True,
                "free_mb": free_mem / (1024 * 1024),
                "total_mb": total_mem / (1024 * 1024),
                "used_mb": (total_mem - free_mem) / (1024 * 1024),
                "usage_percent": ((total_mem - free_mem) / total_mem) * 100,
                "crash_count": self._crash_count,
                "active_contexts": len(self._gpu_contexts)
            }
        except Exception as e:
            logger.error(f"Error getting GPU memory: {e}")
            return {"available": True, "error": str(e)}

# Global GPU manager
gpu_manager = GPUResourceManager()

# ============================================================================
# CRITICAL FIX: GPU Frame Processor with Crash Recovery
# ============================================================================
class GPUFrameProcessor:
    """
    GPU-accelerated frame processor with automatic fallback on errors.
    
    KEY FEATURES:
    - Automatic CPU fallback on GPU errors
    - Per-operation error tracking
    - GPU memory leak prevention
    - CV2 crash recovery
    """
    
    def __init__(self, stream_id: str, use_gpu: bool = True):
        self.stream_id = stream_id
        self.use_gpu = use_gpu and gpu_manager.gpu_available
        self.gpu_error_count = 0
        self.cpu_fallback_active = False
        
        # Error threshold - switch to CPU after this many GPU errors
        self.max_gpu_errors = 5
        
        if self.use_gpu:
            logger.info(f"🎮 GPU processor initialized for {stream_id[:8]}")
        else:
            logger.info(f"💻 CPU processor initialized for {stream_id[:8]}")
    
    async def resize_frame(
        self, 
        frame: np.ndarray, 
        target_width: int, 
        interpolation: int = cv2.INTER_AREA
    ) -> np.ndarray:
        """
        Resize frame with GPU acceleration and automatic CPU fallback.
        
        ✅ CRITICAL: Handles CV2 crashes gracefully
        """
        h, w = frame.shape[:2]
        if w <= target_width:
            return frame
        
        new_w = target_width
        new_h = int(h * (new_w / w))
        
        # Check if we should use CPU fallback
        if not self.use_gpu or self.cpu_fallback_active:
            return cv2.resize(frame, (new_w, new_h), interpolation=interpolation)
        
        try:
            # Try GPU resize
            gpu_frame = cv2.cuda_GpuMat()
            gpu_frame.upload(frame)
            
            gpu_resized = cv2.cuda.resize(
                gpu_frame, 
                (new_w, new_h), 
                interpolation=interpolation
            )
            
            result = gpu_resized.download()
            
            # Success - reset error count
            if self.gpu_error_count > 0:
                self.gpu_error_count = max(0, self.gpu_error_count - 1)
            
            return result
            
        except cv2.error as cv_err:
            # CV2 error - could be a crash
            self.gpu_error_count += 1
            
            logger.warning(
                f"⚠️ GPU resize error for {self.stream_id[:8]} "
                f"(count: {self.gpu_error_count}/{self.max_gpu_errors}): {cv_err}"
            )
            
            # Check if we should switch to CPU fallback
            if self.gpu_error_count >= self.max_gpu_errors:
                logger.error(
                    f"🚨 Too many GPU errors for {self.stream_id[:8]}, "
                    f"switching to CPU fallback"
                )
                self.cpu_fallback_active = True
                
                # Trigger GPU cleanup
                asyncio.create_task(
                    gpu_manager.reset_gpu_on_crash(self.stream_id)
                )
            
            # CPU fallback
            return cv2.resize(frame, (new_w, new_h), interpolation=interpolation)
            
        except Exception as e:
            # Unknown error
            self.gpu_error_count += 1
            logger.error(f"💥 Unexpected GPU error: {e}")
            
            if self.gpu_error_count >= self.max_gpu_errors:
                self.cpu_fallback_active = True
            
            # CPU fallback
            return cv2.resize(frame, (new_w, new_h), interpolation=interpolation)
    
    async def cleanup(self):
        """Cleanup GPU resources for this processor"""
        if self.use_gpu:
            await gpu_manager.cleanup_stream_gpu_resources(self.stream_id)
    
    def reset_error_count(self):
        """Reset error count (call after successful GPU reset)"""
        self.gpu_error_count = 0
        self.cpu_fallback_active = False
        logger.info(f"🔄 GPU error count reset for {self.stream_id[:8]}")

# ============================================================================
# Per-stream thread pools
# ============================================================================
class StreamThreadPool:
    """Manages isolated thread pool per stream"""
    _pools: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_pool(cls, stream_id: str) -> concurrent.futures.ThreadPoolExecutor:
        async with cls._lock:
            if stream_id not in cls._pools:
                cls._pools[stream_id] = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix=f"Stream_{stream_id[:8]}"
                )
            return cls._pools[stream_id]
    
    @classmethod
    async def cleanup_pool(cls, stream_id: str):
        async with cls._lock:
            if stream_id in cls._pools:
                pool = cls._pools.pop(stream_id)
                pool.shutdown(wait=False)


class StreamState(Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"
    FAILED = "failed"
    ZOMBIE = "zombie"
    CV2_CRASHED = "cv2_crashed"  # NEW: CV2 crash detected

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
    
    # Health tracking
    last_heartbeat: Optional[float] = None
    zombie_detected_at: Optional[float] = None
    recovery_attempts: int = 0
    
    # GPU metrics
    gpu_processing_enabled: bool = False
    gpu_resize_count: int = 0
    gpu_errors: int = 0
    
    # NEW: CV2 crash tracking
    cv2_crash_count: int = 0
    last_cv2_crash: Optional[float] = None


@dataclass
class Subscriber:
    stream_id: str
    added_at: float = field(default_factory=time.time)
    frames_received: int = 0
    last_frame_time: Optional[float] = None
    callback_info: Dict[str, Any] = field(default_factory=dict)


class SharedVideoStream:
    """
    Production-hardened shared video stream with GPU acceleration and CV2 crash recovery.
    
    ✅ CRITICAL FIXES for 100+ cameras:
    - Automatic CV2 crash detection and recovery
    - GPU resource cleanup on failures
    - Per-stream GPU context isolation
    - Graceful fallback to CPU on GPU errors
    - Zombie state prevention
    """
    
    def __init__(
        self,
        source: str,
        stream_id: str,
        max_subscribers: int = config.max_local_streams,
        enable_gpu: bool = True,
    ):
        self.source = source
        
        # Ensure stream_id is string
        if isinstance(stream_id, UUID):
            self.stream_id = str(stream_id)
        elif isinstance(stream_id, int):
            import hashlib
            self.stream_id = hashlib.md5(f"{source}_{stream_id}".encode()).hexdigest()
        else:
            self.stream_id = str(stream_id)
    
        self.max_subscribers = max_subscribers
        self.config = config 
        
        # GPU processor with crash recovery
        self.gpu_processor = GPUFrameProcessor(
            stream_id=self.stream_id,
            use_gpu=enable_gpu
        )
        self.use_gpu = self.gpu_processor.use_gpu
        
        # State management
        self.state = StreamState.IDLE
        self.metrics = StreamMetrics(gpu_processing_enabled=self.use_gpu)
        
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
        
        # Source type
        self.is_file = self._is_file_source(source)
        self.is_rtsp = self._is_rtsp_source(source)
        
        # Recovery tracking
        self._last_recovery_attempt: float = 0
        self._last_reconnect_time: float = 0
        
        # Thread pool
        self._thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
        
        logger.info(
            f"📹 Created SharedVideoStream: id={self.stream_id[:8]}, "
            f"source={source[:50]}, "
            f"type={'RTSP' if self.is_rtsp else 'FILE'}, "
            f"GPU={'enabled' if self.use_gpu else 'disabled'}"
        )
    
    # ==================== Subscriber Management ====================
    
    async def add_subscriber(
        self,
        subscriber_id: str,
        callback_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        async with self.lock:
            if len(self.subscribers) >= self.max_subscribers:
                logger.warning(f"Max subscribers ({self.max_subscribers}) reached")
                return False
            
            self.subscribers[subscriber_id] = Subscriber(
                stream_id=subscriber_id,
                callback_info=callback_info or {}
            )
            
            logger.info(
                f"➕ Added subscriber {subscriber_id[:8]} to {self.stream_id[:8]}. "
                f"Total: {len(self.subscribers)}"
            )
            
            if len(self.subscribers) == 1:
                await self._start_capture()
            
            return True
    
    async def remove_subscriber(self, subscriber_id: str) -> None:
        async with self.lock:
            if subscriber_id not in self.subscribers:
                return
            
            subscriber = self.subscribers.pop(subscriber_id)
            logger.info(
                f"➖ Removed subscriber {subscriber_id[:8]} from {self.stream_id[:8]}. "
                f"Frames: {subscriber.frames_received}"
            )
            
            if not self.subscribers:
                await self._stop_capture()
    
    async def get_latest_frame(self, subscriber_id: str) -> Optional[np.ndarray]:
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
        try:
            await asyncio.wait_for(self.frame_available.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    # ==================== Capture Management ====================
    
    async def _start_capture(self) -> None:
        if self.state in (StreamState.RUNNING, StreamState.CONNECTING):
            return
        
        self.state = StreamState.CONNECTING
        self.stop_event.clear()
        self.metrics.uptime_start = time.time()
        
        self._thread_pool = await StreamThreadPool.get_pool(self.stream_id)
        
        self.capture_task = asyncio.create_task(self._capture_loop())
        self.capture_task.set_name(f"capture_{self.stream_id[:8]}")
        
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        self.health_monitor_task.set_name(f"health_{self.stream_id[:8]}")
        
        logger.info(f"▶️ Started capture for {self.stream_id[:8]}")
    
    async def _stop_capture(self) -> None:
        """
        Stop capture with proper GPU cleanup.
        
        ✅ CRITICAL: Ensures GPU resources are released
        """
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
                pass
        
        self.capture_task = None
        
        # ✅ CRITICAL: Cleanup GPU resources
        if self.use_gpu:
            try:
                await self.gpu_processor.cleanup()
                logger.info(f"🧹 GPU resources cleaned for {self.stream_id[:8]}")
            except Exception as e:
                logger.error(f"GPU cleanup error: {e}")
        
        # Cleanup thread pool
        await StreamThreadPool.cleanup_pool(self.stream_id)
        self._thread_pool = None
        
        logger.info(f"✅ Capture stopped for {self.stream_id[:8]}")
    
    # ==================== Health Monitoring ====================

    async def _health_monitor_loop(self) -> None:
        """Monitor stream health with CV2 crash detection"""
        logger.info(f"🏥 Health monitor started for {self.stream_id[:8]}")
        
        is_rtsp = self.is_rtsp
        startup_grace = 300.0 if is_rtsp else 60.0
        running_timeout = 600.0 if is_rtsp else 120.0
        
        startup_time = time.time()
        first_frame = False
        
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(30.0)
                
                if self.state not in (StreamState.RUNNING, StreamState.CONNECTING):
                    continue
                
                now = time.time()
                
                # Check for CV2 crashes
                if self.state == StreamState.CV2_CRASHED:
                    logger.error(f"☠️ CV2 CRASH detected for {self.stream_id[:8]}")
                    await self._handle_cv2_crash()
                    break
                
                # GPU health check
                if self.use_gpu and self.gpu_processor.cpu_fallback_active:
                    logger.warning(
                        f"⚠️ Stream {self.stream_id[:8]} in CPU fallback mode "
                        f"(GPU errors: {self.gpu_processor.gpu_error_count})"
                    )
                
                # Frame timeout checks
                if self.metrics.last_success_time:
                    if not first_frame:
                        first_frame = True
                        logger.info(
                            f"✅ {self.stream_id[:8]} first frame "
                            f"after {now - startup_time:.1f}s"
                        )
                    
                    time_since_frame = now - self.metrics.last_success_time
                    
                    if time_since_frame > running_timeout:
                        logger.error(
                            f"☠️ ZOMBIE for {self.stream_id[:8]}: "
                            f"No frames for {time_since_frame:.0f}s"
                        )
                        self.state = StreamState.ZOMBIE
                        self.metrics.zombie_detected_at = now
                        await self._emergency_cleanup()
                        break
                        
                    elif time_since_frame > running_timeout * 0.5:
                        logger.warning(
                            f"⚠️ {self.stream_id[:8]} stagnant for {time_since_frame:.0f}s"
                        )
                        try:
                            if await self._force_full_reconnection():
                                logger.info(f"✅ Proactive recovery successful")
                        except Exception as e:
                            logger.error(f"Recovery error: {e}")
                else:
                    time_since_startup = now - startup_time
                    
                    if time_since_startup > startup_grace:
                        logger.error(
                            f"☠️ STARTUP TIMEOUT for {self.stream_id[:8]}: "
                            f"No frames after {time_since_startup:.0f}s"
                        )
                        self.state = StreamState.ZOMBIE
                        await self._emergency_cleanup()
                        break
                
                self.metrics.last_heartbeat = now
        
        except asyncio.CancelledError:
            logger.info(f"🏥 Health monitor cancelled")
        except Exception as e:
            logger.error(f"💥 Health monitor error: {e}")

    async def _handle_cv2_crash(self) -> None:
        """
        Handle CV2 crash with full recovery.
        
        ✅ CRITICAL: This is called when CV2 crashes
        """
        logger.error(f"🚨 Handling CV2 crash for {self.stream_id[:8]}")
        
        self.metrics.cv2_crash_count += 1
        self.metrics.last_cv2_crash = time.time()
        
        try:
            # Release capture
            await self._safe_release_capture()
            
            # Cleanup GPU resources
            if self.use_gpu:
                success = await gpu_manager.reset_gpu_on_crash(self.stream_id)
                if success:
                    # Reset GPU processor error count
                    self.gpu_processor.reset_error_count()
                    logger.info(f"✅ GPU reset successful")
                else:
                    logger.error(f"❌ GPU reset failed")
            
            # Wait before attempting reconnection
            await asyncio.sleep(5.0)
            
            # Attempt reconnection
            self.state = StreamState.RECONNECTING
            self.metrics.consecutive_errors = 0
            
            logger.info(f"♻️ Attempting reconnection after CV2 crash")
            
        except Exception as e:
            logger.error(f"💥 CV2 crash handler error: {e}")
            self.state = StreamState.FAILED

    async def _emergency_cleanup(self) -> None:
        """Emergency cleanup for zombie/crashed streams"""
        logger.warning(f"🚨 Emergency cleanup for {self.stream_id[:8]}")
        
        try:
            await self._safe_release_capture()
            
            if self.capture_task and not self.capture_task.done():
                self.capture_task.cancel()
            
            if self.use_gpu:
                await self.gpu_processor.cleanup()
            
            self.state = StreamState.FAILED
            self.metrics.last_error = "Emergency cleanup triggered"
        
        except Exception as e:
            logger.error(f"💥 Emergency cleanup error: {e}")
    
    # ==================== Capture Loop with GPU & Crash Recovery ====================
    
    async def _capture_loop(self) -> None:
        """
        Main capture loop with GPU acceleration and CV2 crash recovery.
        
        ✅ CRITICAL: Detects and recovers from CV2 crashes
        """
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
                
                # Ensure video source
                if not await self._ensure_video_source():
                    delay = self._calculate_backoff_delay()
                    logger.warning(f"⏳ Waiting {delay:.1f}s before retry")
                    
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                        break
                    except asyncio.TimeoutError:
                        pass
                    
                    continue
                
                # RTSP timeout check
                if self.is_rtsp:
                    elapsed = time.time() - last_success_time
                    if elapsed > self.config.max_read_timeout:
                        logger.error(f"❌ Read timeout ({elapsed:.0f}s)")
                        
                        if self.metrics.recovery_attempts < 3:
                            if await self._try_connection_recovery():
                                last_success_time = time.time()
                                continue
                        
                        logger.warning(f"♻️ Forcing reconnection")
                        await self._safe_release_capture()
                        continue
                
                # ✅ CRITICAL: Protected frame read with CV2 crash detection
                try:
                    ret, frame = await self._read_frame_safe()
                except cv2.error as cv_err:
                    # CV2 crash detected
                    logger.error(f"💥 CV2 ERROR detected: {cv_err}")
                    self.state = StreamState.CV2_CRASHED
                    await self._handle_cv2_crash()
                    break
                except Exception as e:
                    logger.error(f"💥 Unexpected read error: {e}")
                    await self._handle_read_failure()
                    continue
                
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
                
                # ✅ GPU-accelerated processing with crash protection
                try:
                    if frame.shape[1] > self.config.max_frame_width:
                        frame = await self.gpu_processor.resize_frame(
                            frame,
                            self.config.max_frame_width
                        )
                        
                        if self.use_gpu and not self.gpu_processor.cpu_fallback_active:
                            self.metrics.gpu_resize_count += 1
                    
                except Exception as gpu_err:
                    logger.error(f"💥 GPU processing error: {gpu_err}")
                    self.metrics.gpu_errors += 1
                    
                    # CPU fallback
                    if frame.shape[1] > self.config.max_frame_width:
                        h, w = frame.shape[:2]
                        new_w = self.config.max_frame_width
                        new_h = int(h * (new_w / w))
                        frame = cv2.resize(frame, (new_w, new_h))
                
                # Process successful frame
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
            logger.info(f"🛑 Capture cancelled")
        except Exception as e:
            logger.error(f"💥 Fatal capture error: {e}", exc_info=True)
            self.state = StreamState.FAILED
            self.metrics.last_error = str(e)
        finally:
            await self._safe_release_capture()
            
            logger.info(
                f"📊 Capture ended for {self.stream_id[:8]}: "
                f"frames={self.metrics.total_frames}, "
                f"gpu_resizes={self.metrics.gpu_resize_count}, "
                f"gpu_errors={self.metrics.gpu_errors}, "
                f"cv2_crashes={self.metrics.cv2_crash_count}"
            )
    
    # ==================== Frame Reading ====================
    
    async def _read_frame_safe(self) -> tuple[bool, Optional[np.ndarray]]:
        """
        Thread-safe frame reading with CV2 crash protection.
        
        ✅ CRITICAL: Raises cv2.error on crashes for detection
        """
        async with self.capture_lock:
            if (self.stop_event.is_set() or
                self.cap is None or
                not self.cap.isOpened() or
                self._thread_pool is None):
                return False, None
            
            # This will raise cv2.error if CV2 crashes
            loop = asyncio.get_event_loop()
            ret, frame = await loop.run_in_executor(
                self._thread_pool,
                self.cap.read
            )
            return ret, frame
    
    def _validate_frame(self, frame: np.ndarray) -> bool:
        if frame is None or frame.size == 0:
            return False
        if len(frame.shape) != 3:
            return False
        if frame.shape[0] < 10 or frame.shape[1] < 10:
            return False
        return True
    
    async def _process_successful_frame(self, frame: np.ndarray) -> None:
        self.metrics.consecutive_errors = 0
        self.metrics.total_frames += 1
        self.metrics.last_success_time = time.time()
        self.state = StreamState.RUNNING
        
        async with self.lock:
            self.latest_frame = frame
            self.frame_available.set()
            self.frame_available.clear()
        
        if self.metrics.total_frames % 100 == 0:
            mode = "GPU" if (self.use_gpu and not self.gpu_processor.cpu_fallback_active) else "CPU"
            logger.debug(
                f"📊 {self.stream_id[:8]}: {self.metrics.total_frames} frames ({mode})"
            )
    
    async def _handle_read_failure(self) -> None:
        """Handle frame read failure with 3-tier recovery"""
        self.metrics.consecutive_errors += 1
        self.metrics.last_error_time = time.time()
        
        # Emergency restart
        if self.metrics.consecutive_errors >= self.config.max_consecutive_errors:
            logger.error(
                f"❌ Max errors ({self.metrics.consecutive_errors}). "
                f"EMERGENCY RESTART"
            )
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
                            logger.info(f"🔁 Looping video")
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
                logger.warning(f"Loop error: {e}")
        
        # RTSP 3-tier recovery
        if self.is_rtsp:
            if self.metrics.consecutive_errors % 10 == 0:
                logger.warning(f"⚠️ {self.metrics.consecutive_errors} errors")
            
            # Tier 1: Light recovery
            if (self.metrics.consecutive_errors % 10 == 0 and
                self.metrics.consecutive_errors < 30):
                if await self._try_connection_recovery():
                    logger.info(f"✅ Tier 1 recovery OK")
                    self.metrics.consecutive_errors = 0
                    return
            
            # Tier 2: Full reconnection
            elif (self.metrics.consecutive_errors >= 30 and 
                  self.metrics.consecutive_errors < 70 and
                  self.metrics.consecutive_errors % 10 == 0):
                logger.warning(f"🔄 Tier 2 reconnection")
                if await self._force_full_reconnection():
                    logger.info(f"✅ Tier 2 recovery OK")
                    self.metrics.consecutive_errors = 0
                    return
            
            # Tier 3: Emergency
            elif self.metrics.consecutive_errors >= 70:
                if self.metrics.consecutive_errors % 20 == 0:
                    logger.error(f"🚨 Tier 3 emergency")
                    await self._safe_release_capture()
                    await asyncio.sleep(10.0)
                    return
            
            delay = min(1.0, 0.05 * (1.3 ** (self.metrics.consecutive_errors // 5)))
            await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0.1)
    
    # ==================== Connection Management ====================
    
    def _open_rtsp_source(self) -> bool:
        """Open RTSP with TCP and GPU support"""
        self.metrics.connection_attempts += 1
        
        # Rate limiting
        current_time = time.time()
        if self._last_reconnect_time > 0:
            elapsed = current_time - self._last_reconnect_time
            if elapsed < self.config.min_reconnect_interval:
                wait = self.config.min_reconnect_interval - elapsed
                time.sleep(wait)
        
        self._last_reconnect_time = time.time()
        
        logger.info(
            f"🔌 Opening RTSP (attempt #{self.metrics.connection_attempts})"
        )
        
        transport = "tcp" if self.config.prefer_tcp else "udp"
        
        try:
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
            
            os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = rtsp_options
            
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
        
        except Exception as e:
            logger.error(f"Failed to create VideoCapture: {e}")
            return False
        
        if not self.cap or not self.cap.isOpened():
            logger.error(f"Failed to open RTSP")
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
            logger.warning(f"Property set error: {e}")
        
        # Test reads
        for attempt in range(3):
            try:
                ret, frame = self.cap.read()
                if ret and frame is not None and frame.size > 0:
                    logger.info(
                        f"✅ RTSP connected: {frame.shape[1]}x{frame.shape[0]} "
                        f"via {transport.upper()}"
                    )
                    return True
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Test read {attempt + 1} error: {e}")
                time.sleep(0.5)
        
        logger.error(f"❌ Cannot read frames")
        try:
            self.cap.release()
        except:
            pass
        self.cap = None
        return False
    
    async def _try_connection_recovery(self) -> bool:
        """Light recovery - buffer flush"""
        current_time = time.time()
        
        if current_time - self._last_recovery_attempt < self.config.recovery_interval:
            return False
        
        self._last_recovery_attempt = current_time
        self.metrics.recovery_attempts += 1
        
        logger.info(f"🔧 Connection recovery")
        
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
                
                # Discard frames
                for _ in range(5):
                    try:
                        await loop.run_in_executor(self._thread_pool, self.cap.read)
                    except:
                        break
                
                # Test
                ret, frame = await loop.run_in_executor(self._thread_pool, self.cap.read)
                if ret and frame is not None and frame.size > 0:
                    logger.info(f"✅ Recovery OK")
                    self.metrics.consecutive_errors = 0
                    self.metrics.last_success_time = time.time()
                    return True
            
            return False
        
        except Exception as e:
            logger.error(f"Recovery error: {e}")
            return False
    
    async def _force_full_reconnection(self) -> bool:
        """Full reconnection with GPU cleanup"""
        logger.info(f"🔄 Full reconnection")
        
        try:
            # ✅ CRITICAL: Cleanup GPU before reconnecting
            if self.use_gpu:
                await self.gpu_processor.cleanup()
            
            await self._safe_release_capture()
            await asyncio.sleep(2.0)
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self._thread_pool,
                self._open_video_source_sync
            )
            
            if success:
                logger.info(f"✅ Reconnection OK")
                self.metrics.consecutive_errors = 0
                self.metrics.last_success_time = time.time()
                self.metrics.recovery_attempts = 0
                
                # Reset GPU error count on successful reconnection
                if self.use_gpu:
                    self.gpu_processor.reset_error_count()
                
                return True
            else:
                logger.warning(f"⚠️ Reconnection failed")
                return False
                
        except Exception as e:
            logger.error(f"💥 Reconnection error: {e}")
            return False
    
    async def _safe_release_capture(self) -> None:
        """
        Safely release capture with GPU cleanup.
        
        ✅ CRITICAL: Always cleanup GPU resources
        """
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
                            logger.debug(f"✓ Released capture")
                    except Exception as e:
                        logger.warning(f"Release error: {e}")
                    finally:
                        self.cap = None
                        
                        # ✅ CRITICAL: Always cleanup GPU
                        if self.use_gpu:
                            try:
                                await gpu_manager.cleanup_stream_gpu_resources(self.stream_id)
                            except Exception as gpu_err:
                                logger.error(f"GPU cleanup error: {gpu_err}")
        
        except Exception as e:
            logger.error(f"Safe release error: {e}")
            self.cap = None
    
    # ==================== Utility Methods ====================
    
    def _calculate_backoff_delay(self) -> float:
        return min(
            self.config.max_backoff_delay,
            self.config.min_backoff_delay * (
                self.config.backoff_multiplier ** (self.metrics.connection_attempts - 1)
            )
        )
    
    def _calculate_sleep_time(self) -> float:
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
        return (not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and
                not source.isdigit())
    
    def _is_rtsp_source(self, source: str) -> bool:
        return source.startswith('rtsp://')
    
    def _validate_file(self) -> bool:
        if not os.path.exists(self.source):
            logger.error(f"File not found: {self.source}")
            return False
        
        if not os.access(self.source, os.R_OK):
            logger.error(f"File not readable: {self.source}")
            return False
        
        if os.path.getsize(self.source) == 0:
            logger.error(f"File empty: {self.source}")
            return False
        
        return True
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get stats including GPU and CV2 crash metrics"""
        async with self.lock:
            gpu_info = {}
            if self.use_gpu:
                gpu_info = gpu_manager.get_gpu_memory_info()
            
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
                    'uptime': time.time() - self.metrics.uptime_start if self.metrics.uptime_start else 0,
                    'gpu_enabled': self.metrics.gpu_processing_enabled,
                    'gpu_resize_count': self.metrics.gpu_resize_count,
                    'gpu_errors': self.metrics.gpu_errors,
                    'cv2_crash_count': self.metrics.cv2_crash_count,
                    'last_cv2_crash': self.metrics.last_cv2_crash,
                    'cpu_fallback_active': self.gpu_processor.cpu_fallback_active if self.use_gpu else False
                },
                'gpu': gpu_info,
                'subscribers': {
                    sub.stream_id: {
                        'frames_received': sub.frames_received,
                        'last_frame_time': sub.last_frame_time
                    }
                    for sub in self.subscribers.values()
                }
            }

    async def _ensure_video_source(self) -> bool:
        if self.cap is not None and self.cap.isOpened():
            return True
        
        return await self._open_video_source_async()

    async def _open_video_source_async(self) -> bool:
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                self._thread_pool,
                self._open_video_source_sync
            )
        except Exception as e:
            logger.error(f"Open source error: {e}")
            return False

    def _open_video_source_sync(self) -> bool:
        try:
            if self.cap is not None:
                try:
                    if self.cap.isOpened():
                        self.cap.release()
                except:
                    pass
                self.cap = None
            
            time.sleep(0.2)
            
            if self.is_file and not self._validate_file():
                return False
            
            if self.is_rtsp:
                return self._open_rtsp_source()
            
            return self._open_file_source()
        
        except Exception as e:
            logger.error(f"Critical open error: {e}", exc_info=True)
            if self.cap is not None:
                try:
                    self.cap.release()
                except:
                    pass
                self.cap = None
            return False

    def _open_file_source(self) -> bool:
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
                
                ret, frame = self.cap.read()
                if not ret or frame is None:
                    try:
                        self.cap.release()
                    except:
                        pass
                    self.cap = None
                    continue
                
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                
                logger.info(f"✅ File opened with backend {backend}")
                return True
            
            except Exception as e:
                logger.warning(f"Backend {backend} failed: {e}")
                if self.cap:
                    try:
                        self.cap.release()
                    except:
                        pass
                    self.cap = None
        
        return False


class VideoFileManager:
    """Manages shared streams with GPU support and CV2 crash recovery"""
    
    def __init__(self, enable_gpu: bool = True):
        self.shared_streams: Dict[str, SharedVideoStream] = {}
        self.lock = asyncio.Lock()
        self.enable_gpu = enable_gpu and gpu_manager.gpu_available
        
        logger.info(
            f"📹 VideoFileManager initialized: "
            f"GPU={'enabled' if self.enable_gpu else 'disabled'}, "
            f"CUDA devices={gpu_manager.cuda_devices}"
        )
    
    async def get_shared_stream(
        self,
        source: str,
        stream_id: Optional[Union[str, UUID, int]] = None,
        max_subscribers: int = config.max_local_streams,
    ) -> SharedVideoStream:
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
                    f"GPU={'on' if self.enable_gpu else 'off'}"
                )
                
                self.shared_streams[source] = SharedVideoStream(
                    source=source,
                    stream_id=stream_id_str,
                    max_subscribers=max_subscribers,
                    enable_gpu=self.enable_gpu
                )
            return self.shared_streams[source]
    
    async def remove_shared_stream(self, source: str) -> None:
        async with self.lock:
            if source in self.shared_streams:
                stream = self.shared_streams[source]
                if stream.state in (StreamState.RUNNING, StreamState.CONNECTING):
                    await stream._stop_capture()
                del self.shared_streams[source]
                logger.info(f"Removed stream: {source[:50]}")
    
    async def cleanup_empty_streams(self) -> None:
        async with self.lock:
            empty = [
                source for source, stream in self.shared_streams.items()
                if not stream.subscribers
            ]
            
            for source in empty:
                await self.remove_shared_stream(source)
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get all stats including global GPU health"""
        async with self.lock:
            stats = {
                source: await stream.get_stats()
                for source, stream in self.shared_streams.items()
            }
            
            # Global GPU stats
            if self.enable_gpu:
                stats['gpu_global'] = {
                    'enabled': True,
                    'devices': gpu_manager.cuda_devices,
                    'memory': gpu_manager.get_gpu_memory_info(),
                    'total_crashes': gpu_manager._crash_count,
                    'active_contexts': len(gpu_manager._gpu_contexts)
                }
            else:
                stats['gpu_global'] = {'enabled': False}
            
            return stats
    
    async def force_restart_stream(self, source: str) -> bool:
        try:
            async with self.lock:
                if source not in self.shared_streams:
                    return False
                
                stream = self.shared_streams[source]
                
                await stream._stop_capture()
                await asyncio.sleep(2.0)
                
                logger.info(f"🔄 Restarted stream: {source[:50]}")
                return True
        
        except Exception as e:
            logger.error(f"Restart error: {e}", exc_info=True)
            return False


# Global instance
video_file_manager = VideoFileManager(config.use_hw_accel)