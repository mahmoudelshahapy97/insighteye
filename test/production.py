import cv2
import logging
import time
import os
import signal
import sys
import numpy as np
from typing import Optional, Tuple, List, Dict
from threading import Thread, Event, Lock
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from collections import deque
from queue import Queue, Empty
import json
from enum import Enum

# Configure logging with rotation
from logging.handlers import RotatingFileHandler

def setup_logging(log_dir: str = "logs", max_bytes: int = 10*1024*1024, backup_count: int = 5):
    """Setup logging with file rotation"""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        filename=Path(log_dir) / 'rtsp_stream.log',
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


class StreamState(Enum):
    """Stream connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass
class StreamMetrics:
    """Track stream performance metrics"""
    frames_read: int = 0
    frames_saved: int = 0
    frames_dropped: int = 0
    reconnection_count: int = 0
    last_frame_time: Optional[float] = None
    connection_time: Optional[float] = None
    total_bytes: int = 0
    errors: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def update_frame_read(self, frame_size: int):
        """Update metrics for frame read"""
        self.frames_read += 1
        self.last_frame_time = time.time()
        self.total_bytes += frame_size
    
    def get_fps(self, window_seconds: int = 10) -> float:
        """Calculate recent FPS"""
        if not self.last_frame_time:
            return 0.0
        elapsed = time.time() - (self.connection_time or time.time())
        return self.frames_read / elapsed if elapsed > 0 else 0.0
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary"""
        return {
            'frames_read': self.frames_read,
            'frames_saved': self.frames_saved,
            'frames_dropped': self.frames_dropped,
            'reconnections': self.reconnection_count,
            'total_mb': round(self.total_bytes / (1024*1024), 2),
            'fps': round(self.get_fps(), 2),
            'error_count': len(self.errors)
        }


class FrameBuffer:
    """Thread-safe frame buffer with size limit"""
    
    def __init__(self, maxsize: int = 30):
        self.queue = Queue(maxsize=maxsize)
        self.dropped = 0
    
    def put(self, item: Tuple[np.ndarray, float], timeout: float = 0.1) -> bool:
        """Add frame to buffer, drop if full"""
        try:
            self.queue.put(item, timeout=timeout)
            return True
        except:
            self.dropped += 1
            return False
    
    def get(self, timeout: float = 1.0) -> Optional[Tuple[np.ndarray, float]]:
        """Get frame from buffer"""
        try:
            return self.queue.get(timeout=timeout)
        except Empty:
            return None
    
    def clear(self):
        """Clear all frames from buffer"""
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except Empty:
                break


class RTSPStreamReader:
    """Production-ready RTSP stream reader with monitoring and error handling"""
    
    def __init__(
        self,
        camera_name: str,
        rtsp_url: str,
        output_folder: str,
        skip_frames: int = 100,
        reconnect_delay: int = 5,
        timeout: int = 10,
        max_reconnect_attempts: int = -1,  # -1 for unlimited
        health_check_interval: int = 30,
        buffer_size: int = 30,
        jpeg_quality: int = 95
    ):
        self.camera_name = camera_name
        self.rtsp_url = rtsp_url
        self.stream_id = camera_name
        self.output_folder = Path(output_folder) / camera_name
        self.skip_frames = skip_frames
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.max_reconnect_attempts = max_reconnect_attempts
        self.health_check_interval = health_check_interval
        self.jpeg_quality = jpeg_quality
        
        # Threading and state
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_running = Event()
        self.state = StreamState.DISCONNECTED
        self.state_lock = Lock()
        
        # Frame management
        self.frame_buffer = FrameBuffer(maxsize=buffer_size)
        self.last_successful_frame: Optional[np.ndarray] = None
        self.frame_count = 0
        
        # Metrics and monitoring
        self.metrics = StreamMetrics()
        self.logger = logging.getLogger(f"RTSP.{camera_name}")
        
        # Create output folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        # Save configuration
        self._save_config()
    
    def _save_config(self):
        """Save stream configuration"""
        config = {
            'camera_name': self.camera_name,
            'rtsp_url': self.rtsp_url.replace('admin:12345678', 'admin:****'),  # Hide password
            'skip_frames': self.skip_frames,
            'output_folder': str(self.output_folder),
            'created_at': datetime.now().isoformat()
        }
        config_file = self.output_folder / 'config.json'
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
    
    def _set_state(self, state: StreamState):
        """Thread-safe state update"""
        with self.state_lock:
            self.state = state
            self.logger.debug(f"State changed to: {state.value}")
    
    def _configure_capture(self):
        """Configure OpenCV capture with optimized settings"""
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;tcp|'
            f'timeout;{self.timeout * 1000000}|'
            f'stimeout;{self.timeout * 1000000}|'
            f'max_delay;500000|'
            f'reorder_queue_size;0|'
            f'buffer_size;1024000'
        )
    
    def connect(self) -> bool:
        """Establish connection to RTSP stream with retry logic"""
        self._set_state(StreamState.CONNECTING)
        
        try:
            if self.cap is not None:
                self.cap.release()
                self.cap = None
            
            self._configure_capture()
            self.logger.info(f"Connecting to stream...")
            
            self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
            
            # Set timeouts
            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.timeout * 1000)
            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.timeout * 1000)
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # Minimize buffering
            
            if not self.cap.isOpened():
                raise ConnectionError("Failed to open stream")
            
            # Verify stream with initial frame
            ret, frame = self.cap.read()
            if not ret or frame is None:
                raise ConnectionError("Failed to read initial frame")
            
            # Get stream properties
            width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = self.cap.get(cv2.CAP_PROP_FPS)
            
            self.logger.info(f"✓ Connected: {width}x{height} @ {fps:.1f}fps")
            
            self.last_successful_frame = frame
            self.frame_count = 0
            self.metrics.connection_time = time.time()
            self.metrics.reconnection_count += 1
            self._set_state(StreamState.CONNECTED)
            
            # Clear frame buffer
            self.frame_buffer.clear()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            self.metrics.errors.append({
                'time': datetime.now().isoformat(),
                'error': str(e),
                'type': 'connection'
            })
            self._set_state(StreamState.FAILED)
            return False
    
    def read_frame(self) -> Tuple[bool, Optional[np.ndarray]]:
        """Read frame with error handling and validation"""
        if self.cap is None or not self.cap.isOpened():
            return False, None
        
        try:
            ret, frame = self.cap.read()
            
            if ret and frame is not None and frame.size > 0:
                self.last_successful_frame = frame
                self.frame_count += 1
                self.metrics.update_frame_read(frame.nbytes)
                return True, frame
            else:
                self.metrics.frames_dropped += 1
                return False, None
                
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            self.metrics.errors.append({
                'time': datetime.now().isoformat(),
                'error': str(e),
                'type': 'read'
            })
            return False, None
    
    def save_frame(self, frame: np.ndarray) -> bool:
        """Save frame with compression and error handling"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = self.output_folder / f"{self.camera_name}_{timestamp}.jpg"
            
            # Use JPEG compression with quality setting
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, self.jpeg_quality]
            success = cv2.imwrite(str(filename), frame, encode_params)
            
            if success:
                self.metrics.frames_saved += 1
                if self.metrics.frames_saved % 10 == 0:  # Log every 10th save
                    self.logger.info(f"Saved #{self.metrics.frames_saved}: {filename.name}")
                return True
            else:
                raise IOError("Failed to write image")
                
        except Exception as e:
            self.logger.error(f"Save error: {e}")
            self.metrics.errors.append({
                'time': datetime.now().isoformat(),
                'error': str(e),
                'type': 'save'
            })
            return False
    
    def _read_thread(self):
        """Background thread for reading frames"""
        consecutive_failures = 0
        max_consecutive_failures = 5
        reconnect_attempts = 0
        
        while self.is_running.is_set():
            # Check if we need to connect
            if self.state != StreamState.CONNECTED:
                if self.max_reconnect_attempts > 0 and reconnect_attempts >= self.max_reconnect_attempts:
                    self.logger.error("Max reconnection attempts reached. Stopping.")
                    break
                
                if self.connect():
                    consecutive_failures = 0
                    reconnect_attempts = 0
                else:
                    reconnect_attempts += 1
                    self.logger.warning(f"Retry {reconnect_attempts} in {self.reconnect_delay}s...")
                    time.sleep(self.reconnect_delay)
                    continue
            
            # Read frame
            ret, frame = self.read_frame()
            
            if ret and frame is not None:
                consecutive_failures = 0
                # Add to buffer with timestamp
                self.frame_buffer.put((frame.copy(), time.time()))
                
                # Health check logging
                if self.frame_count % 500 == 0:
                    self.logger.info(
                        f"Health: {self.metrics.frames_read} read, "
                        f"{self.metrics.frames_saved} saved, "
                        f"{self.metrics.frames_dropped} dropped, "
                        f"{self.frame_buffer.dropped} buffer drops"
                    )
            else:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(f"{consecutive_failures} consecutive failures. Reconnecting...")
                    self._set_state(StreamState.DISCONNECTED)
                    if self.cap:
                        self.cap.release()
                        self.cap = None
                    consecutive_failures = 0
                time.sleep(0.1)
    
    def _save_thread(self):
        """Background thread for saving frames"""
        frames_since_save = 0
        
        while self.is_running.is_set():
            frame_data = self.frame_buffer.get(timeout=1.0)
            
            if frame_data is None:
                continue
            
            frame, timestamp = frame_data
            frames_since_save += 1
            
            # Save based on skip_frames setting
            if frames_since_save >= self.skip_frames:
                self.save_frame(frame)
                frames_since_save = 0
    
    def start(self) -> List[Thread]:
        """Start reading and saving in separate threads"""
        self.is_running.set()
        
        read_thread = Thread(
            target=self._read_thread,
            name=f"{self.stream_id}-read",
            daemon=True
        )
        save_thread = Thread(
            target=self._save_thread,
            name=f"{self.stream_id}-save",
            daemon=True
        )
        
        read_thread.start()
        save_thread.start()
        
        return [read_thread, save_thread]
    
    def stop(self):
        """Stop all threads and cleanup"""
        self.logger.info("Stopping stream reader...")
        self.is_running.clear()
        self._set_state(StreamState.STOPPED)
        
        if self.cap is not None:
            self.cap.release()
            self.cap = None
        
        # Save final metrics
        self._save_metrics()
        self.logger.info(f"Stopped. Final stats: {self.metrics.to_dict()}")
    
    def _save_metrics(self):
        """Save metrics to file"""
        metrics_file = self.output_folder / 'metrics.json'
        with open(metrics_file, 'w') as f:
            json.dump({
                'camera': self.camera_name,
                'metrics': self.metrics.to_dict(),
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
    
    def get_status(self) -> Dict:
        """Get current status"""
        return {
            'camera': self.camera_name,
            'state': self.state.value,
            'metrics': self.metrics.to_dict()
        }


class StreamManager:
    """Manage multiple RTSP streams"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.readers: List[RTSPStreamReader] = []
        self.threads: List[Thread] = []
        self.logger = logging.getLogger("StreamManager")
        self.shutdown_event = Event()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}. Initiating shutdown...")
        self.shutdown_event.set()
    
    def add_camera(self, camera_name: str, rtsp_url: str, **kwargs):
        """Add a camera to the manager"""
        reader = RTSPStreamReader(camera_name, rtsp_url, **kwargs)
        self.readers.append(reader)
    
    def add_cameras_from_urls(self, urls: List[str], **kwargs):
        """Add multiple cameras from URL list"""
        for rtsp_url in urls:
            parts = rtsp_url.split('/')
            port = parts[2].split(':')[2]
            channel = parts[3]
            camera_name = f"port{port}_{channel}"
            
            self.add_camera(camera_name, rtsp_url, **kwargs)
    
    def start_all(self, stagger_delay: float = 0.1):
        """Start all streams"""
        total = len(self.readers)
        self.logger.info("=" * 70)
        self.logger.info(f"Starting {total} camera streams")
        self.logger.info("=" * 70)
        
        for i, reader in enumerate(self.readers, 1):
            threads = reader.start()
            self.threads.extend(threads)
            self.logger.info(f"Started {i}/{total}: {reader.stream_id}")
            time.sleep(stagger_delay)
        
        self.logger.info("=" * 70)
        self.logger.info("All streams started successfully!")
        self.logger.info("=" * 70)
    
    def stop_all(self):
        """Stop all streams"""
        self.logger.info("Stopping all streams...")
        
        for reader in self.readers:
            reader.stop()
        
        # Wait for threads
        for thread in self.threads:
            thread.join(timeout=3)
        
        self._print_summary()
    
    def monitor_loop(self, interval: int = 60):
        """Monitor streams and log status"""
        self.logger.info(f"Starting monitor loop (interval: {interval}s)")
        
        try:
            while not self.shutdown_event.is_set():
                time.sleep(interval)
                self._log_status()
        except KeyboardInterrupt:
            pass
    
    def _log_status(self):
        """Log current status of all streams"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("STREAM STATUS")
        self.logger.info("=" * 70)
        
        for reader in self.readers:
            status = reader.get_status()
            self.logger.info(
                f"{status['camera']}: {status['state']} - "
                f"FPS: {status['metrics']['fps']:.1f}, "
                f"Saved: {status['metrics']['frames_saved']}, "
                f"Errors: {status['metrics']['error_count']}"
            )
        
        self.logger.info("=" * 70)
    
    def _print_summary(self):
        """Print final summary"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("FINAL SUMMARY")
        self.logger.info("=" * 70)
        
        # Group by port
        ports = {}
        for reader in self.readers:
            port = reader.camera_name.split('_')[0]
            if port not in ports:
                ports[port] = []
            ports[port].append(reader)
        
        total_read = 0
        total_saved = 0
        
        for port in sorted(ports.keys()):
            port_readers = ports[port]
            port_read = sum(r.metrics.frames_read for r in port_readers)
            port_saved = sum(r.metrics.frames_saved for r in port_readers)
            
            self.logger.info(f"\n{port.upper()} ({len(port_readers)} cameras):")
            for reader in sorted(port_readers, key=lambda x: x.camera_name):
                metrics = reader.metrics.to_dict()
                self.logger.info(
                    f"  {reader.camera_name}: "
                    f"{metrics['frames_read']} read, "
                    f"{metrics['frames_saved']} saved, "
                    f"{metrics['reconnections']} reconnects"
                )
            
            self.logger.info(f"  Subtotal: {port_read} read, {port_saved} saved")
            total_read += port_read
            total_saved += port_saved
        
        self.logger.info(f"\nGRAND TOTAL:")
        self.logger.info(f"  {total_read} frames processed")
        self.logger.info(f"  {total_saved} images saved")
        self.logger.info("=" * 70)


def main():
    """Main entry point"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Configuration
    CAMERA_URLS = [
        # Port 5511 cameras
        'rtsp://admin:12345678@41.178.2.61:5511/ch01/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch02/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch03/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch04/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch05/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch06/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch07/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch08/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch09/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch10/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch11/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch12/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch13/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch14/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch15/0',
        'rtsp://admin:12345678@41.178.2.61:5511/ch16/0',
        # Port 554 cameras
        'rtsp://admin:12345678@41.178.2.61:554/ch01/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch02/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch03/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch04/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch05/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch06/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch07/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch08/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch09/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch10/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch11/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch12/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch13/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch14/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch15/0',
        'rtsp://admin:12345678@41.178.2.61:554/ch16/0',
        # Port 5513 cameras
        'rtsp://admin:12345678@41.178.2.61:5513/ch01/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch02/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch03/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch04/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch05/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch06/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch07/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch08/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch09/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch10/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch11/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch12/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch13/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch14/0',
        'rtsp://admin:12345678@41.178.2.61:5513/ch15/0',
    ]
    
    # Stream settings
    CONFIG = {
        'output_folder': 'output',
        'skip_frames': 100,
        'reconnect_delay': 5,
        'timeout': 10,
        'buffer_size': 30,
        'jpeg_quality': 95
    }
    
    # Create and configure manager
    manager = StreamManager()
    manager.add_cameras_from_urls(CAMERA_URLS, **CONFIG)
    
    # Start all streams
    manager.start_all(stagger_delay=0.1)
    
    # Monitor streams
    try:
        manager.monitor_loop(interval=60)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        manager.stop_all()


if __name__ == "__main__":
    main()