import cv2
import logging
import time
import os
import numpy as np
from typing import Optional, Tuple, Callable
from threading import Thread, Event
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RTSPStreamReader:
    """Production-ready RTSP stream reader with auto-reconnect"""
    
    def __init__(self, camera_name: str, rtsp_url: str, output_folder: str, 
                 reconnect_delay: int = 5, timeout: int = 10, save_interval: float = 1.0):
        self.camera_name = camera_name
        self.rtsp_url = rtsp_url
        self.output_folder = Path(output_folder) / camera_name
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.save_interval = save_interval  # Save every N seconds
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
            
            logging.info(f"[{self.camera_name}] Connecting to {self.rtsp_url}...")
            self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
            
            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.timeout * 1000)
            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.timeout * 1000)
            
            if not self.cap.isOpened():
                logging.error(f"[{self.camera_name}] Failed to open stream")
                return False
            
            ret, frame = self.cap.read()
            if not ret or frame is None:
                logging.error(f"[{self.camera_name}] Failed to read initial frame")
                return False
            
            width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = self.cap.get(cv2.CAP_PROP_FPS)
            
            logging.info(f"[{self.camera_name}] ✓ Connected: {width}x{height} @ {fps}fps")
            self.last_frame = (ret, frame)
            self.frame_count = 0
            return True
            
        except Exception as e:
            logging.error(f"[{self.camera_name}] Connection error: {e}")
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
            logging.error(f"[{self.camera_name}] Read error: {e}")
            return False, None
    
    def save_frame(self, frame: np.ndarray) -> bool:
        """Save frame to output folder"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = self.output_folder / f"{self.camera_name}_{timestamp}.jpg"
            cv2.imwrite(str(filename), frame)
            logging.info(f"[{self.camera_name}] Saved: {filename.name}")
            return True
        except Exception as e:
            logging.error(f"[{self.camera_name}] Save error: {e}")
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
                        logging.warning(f"[{self.camera_name}] Reconnecting in {self.reconnect_delay}s...")
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
                        logging.info(f"[{self.camera_name}] Frames processed: {self.frame_count}")
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        logging.error(f"[{self.camera_name}] Too many failures, reconnecting...")
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
        logging.info(f"[{self.camera_name}] Stream reader stopped")


def main():
    """Read all 4 cameras in parallel and save to output folder"""
    
    # All 4 camera streams
    CAMERAS = {
        'camera_ch12_high': 'rtsp://admin:12345678@41.178.2.61:554/ch12/0',
        'camera_ch12_low': 'rtsp://admin:12345678@41.178.2.61:554/ch12/1',
        'camera_ch01_high': 'rtsp://admin:12345678@41.178.2.61:554/ch01/0',
        'camera_ch01_low': 'rtsp://admin:12345678@41.178.2.61:554/ch01/1',
    }
    
    OUTPUT_FOLDER = "output"
    SAVE_INTERVAL = 1.0  # Save one image per second from each camera
    
    # Create readers for all cameras
    readers = []
    threads = []
    
    for camera_name, rtsp_url in CAMERAS.items():
        reader = RTSPStreamReader(
            camera_name=camera_name,
            rtsp_url=rtsp_url,
            output_folder=OUTPUT_FOLDER,
            reconnect_delay=5,
            timeout=10,
            save_interval=SAVE_INTERVAL
        )
        readers.append(reader)
    
    # Start all cameras in parallel
    logging.info("=" * 60)
    logging.info("Starting all cameras in parallel...")
    logging.info("=" * 60)
    
    for reader in readers:
        thread = reader.start_continuous_read()
        threads.append(thread)
    
    # Keep main thread alive
    try:
        logging.info("\nPress Ctrl+C to stop all cameras...\n")
        while any(reader.is_running.is_set() for reader in readers):
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("\n" + "=" * 60)
        logging.info("Interrupted by user - stopping all cameras...")
        logging.info("=" * 60)
    finally:
        for reader in readers:
            reader.stop()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=2)
        
        # Print summary
        logging.info("\n" + "=" * 60)
        logging.info("SUMMARY:")
        for reader in readers:
            logging.info(f"  {reader.camera_name}: {reader.frame_count} frames processed")
        logging.info(f"  Images saved to: {OUTPUT_FOLDER}/")
        logging.info("=" * 60)


if __name__ == "__main__":
    main()