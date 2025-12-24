import cv2
import logging
import time
import os
import numpy as np
from typing import Optional, Tuple
from threading import Thread, Event
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RTSPStreamReader:
    """RTSP stream reader for a single camera at a specific quality level"""
    
    def __init__(self, camera_name: str, rtsp_url: str, quality: int, output_folder: str, 
                 reconnect_delay: int = 5, timeout: int = 10, save_interval: float = 1.0):
        self.camera_name = camera_name
        self.rtsp_url = rtsp_url
        self.quality = quality
        self.stream_id = f"{camera_name}_q{quality}"
        self.output_folder = Path(output_folder) / camera_name / f"quality_{quality}"
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.save_interval = save_interval
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_running = Event()
        self.last_frame: Optional[Tuple[bool, np.ndarray]] = None
        self.frame_count = 0
        self.last_save_time = 0
        
        # Create output folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        # Configure FFmpeg for TCP transport
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;tcp|'
            f'timeout;{timeout * 1000000}|'
            f'stimeout;{timeout * 1000000}|'
            f'max_delay;500000'
        )
    
    def connect(self) -> bool:
        """Establish connection to RTSP stream"""
        try:
            if self.cap is not None:
                self.cap.release()
            
            logging.info(f"[{self.stream_id}] Connecting to {self.rtsp_url}...")
            self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
            
            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.timeout * 1000)
            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.timeout * 1000)
            
            if not self.cap.isOpened():
                logging.error(f"[{self.stream_id}] Failed to open stream")
                return False
            
            ret, frame = self.cap.read()
            if not ret or frame is None:
                logging.error(f"[{self.stream_id}] Failed to read initial frame")
                return False
            
            width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = self.cap.get(cv2.CAP_PROP_FPS)
            
            logging.info(f"[{self.stream_id}] ✓ Connected: {width}x{height} @ {fps}fps")
            self.last_frame = (ret, frame)
            self.frame_count = 0
            return True
            
        except Exception as e:
            logging.error(f"[{self.stream_id}] Connection error: {e}")
            return False
    
    def read_frame(self) -> Tuple[bool, Optional[np.ndarray]]:
        """Read a single frame with error handling"""
        if self.cap is None or not self.cap.isOpened():
            return False, None
        
        try:
            ret, frame = self.cap.read()
            if ret and frame is not None:
                self.last_frame = (ret, frame)
                self.frame_count += 1
                return ret, frame
            else:
                return False, None
        except Exception as e:
            logging.error(f"[{self.stream_id}] Read error: {e}")
            return False, None
    
    def save_frame(self, frame: np.ndarray) -> bool:
        """Save frame to output folder"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = self.output_folder / f"{self.camera_name}_q{self.quality}_{timestamp}.jpg"
            cv2.imwrite(str(filename), frame)
            logging.info(f"[{self.stream_id}] Saved: {filename.name}")
            return True
        except Exception as e:
            logging.error(f"[{self.stream_id}] Save error: {e}")
            return False
    
    def start_continuous_read(self):
        """Start reading frames in background thread"""
        self.is_running.set()
        
        def read_loop():
            consecutive_failures = 0
            max_failures = 3
            
            while self.is_running.is_set():
                # Connect if not connected
                if self.cap is None or not self.cap.isOpened():
                    if not self.connect():
                        logging.warning(f"[{self.stream_id}] Retrying in {self.reconnect_delay}s...")
                        time.sleep(self.reconnect_delay)
                        continue
                    consecutive_failures = 0
                
                # Read frame
                ret, frame = self.read_frame()
                
                if ret and frame is not None:
                    consecutive_failures = 0
                    
                    # Save frame at specified interval
                    current_time = time.time()
                    if current_time - self.last_save_time >= self.save_interval:
                        self.save_frame(frame)
                        self.last_save_time = current_time
                    
                    # Log progress every 100 frames
                    if self.frame_count % 100 == 0:
                        logging.info(f"[{self.stream_id}] Frames: {self.frame_count}")
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        logging.error(f"[{self.stream_id}] Too many failures, reconnecting...")
                        self.cap.release()
                        self.cap = None
                        consecutive_failures = 0
                    time.sleep(0.1)
        
        thread = Thread(target=read_loop, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop reading and cleanup"""
        self.is_running.clear()
        if self.cap is not None:
            self.cap.release()
            self.cap = None
        logging.info(f"[{self.stream_id}] Stream reader stopped")


def main():
    """Read all cameras at all quality levels in parallel"""
    
    # Camera streams with quality placeholder
    CAMERA_CONFIGS = {
        'camera_ch12': 'rtsp://admin:12345678@41.178.2.61:554/ch16/{quality}',
        'camera_ch01': 'rtsp://admin:12345678@41.178.2.61:554/ch17/{quality}',
    }
    
    OUTPUT_FOLDER = "output"
    SAVE_INTERVAL = 1.0  # Save one image per second from each stream
    QUALITY_LEVELS = range(0, 11)  # 0 through 10
    
    # Create readers for all camera-quality combinations
    readers = []
    threads = []
    
    for camera_name, rtsp_url_template in CAMERA_CONFIGS.items():
        for quality in QUALITY_LEVELS:
            rtsp_url = rtsp_url_template.format(quality=quality)
            reader = RTSPStreamReader(
                camera_name=camera_name,
                rtsp_url=rtsp_url,
                quality=quality,
                output_folder=OUTPUT_FOLDER,
                reconnect_delay=5,
                timeout=10,
                save_interval=SAVE_INTERVAL
            )
            readers.append(reader)
    
    # Start all streams in parallel
    total_streams = len(readers)
    logging.info("=" * 60)
    logging.info(f"Starting {total_streams} parallel streams:")
    logging.info(f"  - {len(CAMERA_CONFIGS)} cameras")
    logging.info(f"  - {len(QUALITY_LEVELS)} quality levels each (0-10)")
    logging.info(f"  - Total: {total_streams} concurrent streams")
    logging.info("=" * 60)
    
    for reader in readers:
        thread = reader.start_continuous_read()
        threads.append(thread)
        time.sleep(0.1)  # Small delay to stagger connection attempts
    
    # Keep main thread alive
    try:
        logging.info("\nPress Ctrl+C to stop all streams...\n")
        while any(reader.is_running.is_set() for reader in readers):
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("\n" + "=" * 60)
        logging.info("Interrupted by user - stopping all streams...")
        logging.info("=" * 60)
    finally:
        for reader in readers:
            reader.stop()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=2)
        
        # Print summary grouped by camera
        logging.info("\n" + "=" * 60)
        logging.info("SUMMARY:")
        
        for camera_name in CAMERA_CONFIGS.keys():
            camera_readers = [r for r in readers if r.camera_name == camera_name]
            total_frames = sum(r.frame_count for r in camera_readers)
            logging.info(f"\n  {camera_name}:")
            for reader in camera_readers:
                logging.info(f"    Quality {reader.quality}: {reader.frame_count} frames")
            logging.info(f"    Total: {total_frames} frames")
        
        logging.info(f"\n  Images saved to: {OUTPUT_FOLDER}/")
        logging.info("=" * 60)


if __name__ == "__main__":
    main()