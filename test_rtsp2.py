#RTSP_Stream_Reader.py
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
    """RTSP stream reader for a single camera with frame skipping"""
    
    def __init__(self, camera_name: str, rtsp_url: str, output_folder: str, 
                 skip_frames: int = 100, reconnect_delay: int = 5, timeout: int = 10):
        self.camera_name = camera_name
        self.rtsp_url = rtsp_url
        self.stream_id = camera_name
        self.output_folder = Path(output_folder) / camera_name
        self.skip_frames = skip_frames
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_running = Event()
        self.last_frame: Optional[Tuple[bool, np.ndarray]] = None
        self.frame_count = 0
        self.saved_count = 0
        
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
            filename = self.output_folder / f"{self.camera_name}_{timestamp}.jpg"
            cv2.imwrite(str(filename), frame)
            self.saved_count += 1
            logging.info(f"[{self.stream_id}] Saved #{self.saved_count}: {filename.name}")
            return True
        except Exception as e:
            logging.error(f"[{self.stream_id}] Save error: {e}")
            return False
    
    def start_continuous_read(self):
        """Start reading frames in background thread with frame skipping"""
        self.is_running.set()
        
        def read_loop():
            consecutive_failures = 0
            max_failures = 3
            frames_since_save = 0
            
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
                    frames_since_save += 1
                    
                    # Save frame after skipping specified number of frames
                    if frames_since_save >= self.skip_frames:
                        self.save_frame(frame)
                        frames_since_save = 0
                    
                    # Log progress every 500 frames
                    if self.frame_count % 500 == 0:
                        logging.info(f"[{self.stream_id}] Total frames: {self.frame_count}, Saved: {self.saved_count}")
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
    """Read all cameras in parallel with frame skipping"""
    
    # All camera streams
    CAMERA_URLS = [
        # Port 5511 cameras (ch01-ch16)
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

        # Port 554 cameras (ch01-ch16)
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

        # Port 5513 cameras (ch01-ch15)
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
    
    OUTPUT_FOLDER = "output"
    SKIP_FRAMES = 100  # Save every 100th frame
    
    # Create readers for all cameras
    readers = []
    threads = []
    
    for rtsp_url in CAMERA_URLS:
        # Extract camera name from URL (e.g., "5511_ch01", "554_ch16", "5513_ch15")
        parts = rtsp_url.split('/')
        port = parts[2].split(':')[2]
        channel = parts[3]
        camera_name = f"port{port}_{channel}"
        
        reader = RTSPStreamReader(
            camera_name=camera_name,
            rtsp_url=rtsp_url,
            output_folder=OUTPUT_FOLDER,
            skip_frames=SKIP_FRAMES,
            reconnect_delay=5,
            timeout=10
        )
        readers.append(reader)
    
    # Start all streams in parallel
    total_streams = len(readers)
    logging.info("=" * 60)
    logging.info(f"Starting {total_streams} parallel camera streams")
    logging.info(f"  - Frame skip: Save every {SKIP_FRAMES}th frame")
    logging.info(f"  - Output folder: {OUTPUT_FOLDER}/")
    logging.info("=" * 60)
    
    for i, reader in enumerate(readers, 1):
        thread = reader.start_continuous_read()
        threads.append(thread)
        logging.info(f"Started {i}/{total_streams}: {reader.stream_id}")
        time.sleep(0.05)  # Small delay to stagger connection attempts
    
    # Keep main thread alive
    try:
        logging.info("\n" + "=" * 60)
        logging.info("All streams started! Press Ctrl+C to stop...")
        logging.info("=" * 60 + "\n")
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
        
        # Print summary
        logging.info("\n" + "=" * 60)
        logging.info("SUMMARY:")
        logging.info("=" * 60)
        
        total_frames = 0
        total_saved = 0
        
        # Group by port
        ports = {}
        for reader in readers:
            port = reader.camera_name.split('_')[0]
            if port not in ports:
                ports[port] = []
            ports[port].append(reader)
        
        for port in sorted(ports.keys()):
            port_readers = ports[port]
            port_frames = sum(r.frame_count for r in port_readers)
            port_saved = sum(r.saved_count for r in port_readers)
            
            logging.info(f"\n  {port.upper()} ({len(port_readers)} cameras):")
            for reader in sorted(port_readers, key=lambda x: x.camera_name):
                logging.info(f"    {reader.camera_name}: {reader.frame_count} frames, {reader.saved_count} saved")
            logging.info(f"    Subtotal: {port_frames} frames, {port_saved} saved")
            
            total_frames += port_frames
            total_saved += port_saved
        
        logging.info(f"\n  GRAND TOTAL:")
        logging.info(f"    {total_frames} frames processed")
        logging.info(f"    {total_saved} images saved")
        logging.info(f"\n  Images saved to: {OUTPUT_FOLDER}/")
        logging.info("=" * 60)


if __name__ == "__main__":
    main()