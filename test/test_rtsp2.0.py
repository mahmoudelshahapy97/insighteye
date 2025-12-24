import cv2
import logging
import time
import os
import numpy as np
from typing import Optional, Tuple, Callable
from threading import Thread, Event

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RTSPStreamReader:
    """Production-ready RTSP stream reader with auto-reconnect"""
    
    def __init__(self, rtsp_url: str, reconnect_delay: int = 5, timeout: int = 10):
        self.rtsp_url = rtsp_url
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_running = Event()
        self.last_frame: Optional[Tuple[bool, np.ndarray]] = None
        self.frame_count = 0
        
        # Configure FFmpeg for TCP transport
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            f'rtsp_transport;tcp|'
            f'timeout;{timeout * 1000000}|'
            f'stimeout;{timeout * 1000000}|'
            f'max_delay;500000'  # 0.5 second max delay
        )
    
    def connect(self) -> bool:
        """Establish connection to RTSP stream"""
        try:
            if self.cap is not None:
                self.cap.release()
            
            logging.info(f"Connecting to {self.rtsp_url} (TCP transport)...")
            self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
            
            # Set timeouts
            self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.timeout * 1000)
            self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.timeout * 1000)
            
            if not self.cap.isOpened():
                logging.error("Failed to open stream")
                return False
            
            # Test read
            ret, frame = self.cap.read()
            if not ret or frame is None:
                logging.error("Failed to read initial frame")
                return False
            
            width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = self.cap.get(cv2.CAP_PROP_FPS)
            
            logging.info(f"✓ Connected successfully: {width}x{height} @ {fps}fps")
            self.last_frame = (ret, frame)
            self.frame_count = 0
            return True
            
        except Exception as e:
            logging.error(f"Connection error: {e}")
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
                logging.warning("Failed to read frame")
                return False, None
        except Exception as e:
            logging.error(f"Read error: {e}")
            return False, None
    
    def start_continuous_read(self, callback: Optional[Callable[[np.ndarray], None]] = None):
        """Start reading frames in background thread"""
        self.is_running.set()
        
        def read_loop():
            consecutive_failures = 0
            max_failures = 3
            
            while self.is_running.is_set():
                # Connect if not connected
                if self.cap is None or not self.cap.isOpened():
                    if not self.connect():
                        logging.warning(f"Reconnecting in {self.reconnect_delay}s...")
                        time.sleep(self.reconnect_delay)
                        continue
                    consecutive_failures = 0
                
                # Read frame
                ret, frame = self.read_frame()
                
                if ret and frame is not None:
                    consecutive_failures = 0
                    if callback:
                        callback(frame)
                    
                    # Log progress every 100 frames
                    if self.frame_count % 100 == 0:
                        logging.info(f"Frames processed: {self.frame_count}")
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        logging.error(f"Too many failures ({consecutive_failures}), reconnecting...")
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
        logging.info("Stream reader stopped")


def main():
    """Example usage"""
    
    # Your working RTSP URLs
    STREAMS = {
        'camera_ch12': 'rtsp://admin:12345678@41.178.2.61:554/ch12/0',
        'camera_ch12': 'rtsp://admin:12345678@41.178.2.61:554/ch12/1',
        'camera_ch01': 'rtsp://admin:12345678@41.178.2.61:554/ch01/0',
        'camera_ch01': 'rtsp://admin:12345678@41.178.2.61:554/ch01/1',
    }
    
    # Choose which stream to use
    stream_url = STREAMS['camera_ch12']
    
    # Create reader
    reader = RTSPStreamReader(
        rtsp_url=stream_url,
        reconnect_delay=5,
        timeout=10
    )
    
    # Option 1: Single frame read
    if reader.connect():
        ret, frame = reader.read_frame()
        if ret:
            # Process frame (e.g., save, display, analyze)
            cv2.imwrite('captured_frame.jpg', frame)
            logging.info("Frame saved to captured_frame.jpg")
        reader.stop()
    
    # Option 2: Continuous reading with callback
    # def process_frame(frame):
    #     """Process each frame"""
    #     # Example: Display frame
    #     cv2.imshow('RTSP Stream', frame)
    #     if cv2.waitKey(1) & 0xFF == ord('q'):
    #         reader.stop()
    # 
    # reader.start_continuous_read(callback=process_frame)
    # 
    # # Keep main thread alive
    # try:
    #     while reader.is_running.is_set():
    #         time.sleep(0.1)
    # except KeyboardInterrupt:
    #     logging.info("Interrupted by user")
    # finally:
    #     reader.stop()
    #     cv2.destroyAllWindows()


if __name__ == "__main__":
    main()