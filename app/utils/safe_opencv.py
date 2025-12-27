#app/utils/safe_opencv.py
import cv2
import logging
from typing import Optional, Tuple, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class SafeVideoCapture:
    """
    Wrapper around cv2.VideoCapture with automatic error handling.
    Prevents "NoneType has no attribute 'release'" errors.
    """
    
    def __init__(self, source: str, backend: int = cv2.CAP_ANY):
        """
        Create a safe VideoCapture.
        
        Args:
            source: Video source (file path, URL, or camera index)
            backend: OpenCV backend to use
        """
        self._cap: Optional[cv2.VideoCapture] = None
        self._source = source
        self._backend = backend
        self._is_opened = False
        
        try:
            self._cap = cv2.VideoCapture(source, backend)
            self._is_opened = self._cap is not None and self._cap.isOpened()
        except Exception as e:
            logger.error(f"Failed to create VideoCapture for {source}: {e}")
            self._cap = None
            self._is_opened = False
    
    def isOpened(self) -> bool:
        """Check if capture is opened."""
        try:
            return self._cap is not None and self._cap.isOpened()
        except Exception as e:
            logger.debug(f"Error checking isOpened: {e}")
            return False
    
    def read(self) -> Tuple[bool, Optional[Any]]:
        """
        Read a frame safely.
        
        Returns:
            Tuple of (success, frame)
        """
        if self._cap is None:
            return False, None
        
        try:
            ret, frame = self._cap.read()
            return ret, frame
        except Exception as e:
            logger.error(f"Error reading frame from {self._source}: {e}")
            return False, None
    
    def get(self, prop_id: int) -> float:
        """Get property safely."""
        if self._cap is None:
            return 0.0
        
        try:
            return self._cap.get(prop_id)
        except Exception as e:
            logger.debug(f"Error getting property {prop_id}: {e}")
            return 0.0
    
    def set(self, prop_id: int, value: float) -> bool:
        """Set property safely."""
        if self._cap is None:
            return False
        
        try:
            return self._cap.set(prop_id, value)
        except Exception as e:
            logger.debug(f"Error setting property {prop_id} to {value}: {e}")
            return False
    
    def release(self):
        """Release capture safely - NEVER throws exceptions."""
        if self._cap is None:
            return
        
        try:
            if self._cap.isOpened():
                self._cap.release()
        except Exception as e:
            logger.debug(f"Error during release (ignored): {e}")
        finally:
            self._cap = None
            self._is_opened = False
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - always releases."""
        self.release()
        return False
    
    def __del__(self):
        """Destructor - safe cleanup."""
        try:
            self.release()
        except Exception as e:
            logger.debug(f"Error in __del__: {e}")

@contextmanager
def safe_video_capture(source: str, backend: int = cv2.CAP_ANY):
    """
    Context manager for safe video capture.
    
    Usage:
        with safe_video_capture("video.mp4") as cap:
            if cap.isOpened():
                ret, frame = cap.read()
    """
    cap = SafeVideoCapture(source, backend)
    try:
        yield cap
    finally:
        cap.release()

def safe_release(cap: Optional[cv2.VideoCapture]) -> None:
    """
    Safely release a VideoCapture object.
    Never throws exceptions, handles None gracefully.
    
    Args:
        cap: VideoCapture object or None
    """
    if cap is None:
        return
    
    try:
        if hasattr(cap, 'isOpened') and cap.isOpened():
            cap.release()
    except AttributeError:
        # cap is not a VideoCapture object
        logger.debug("Attempted to release non-VideoCapture object")
    except Exception as e:
        # Any other exception during release
        logger.debug(f"Error during safe_release (ignored): {e}")

def safe_read(cap: Optional[cv2.VideoCapture]) -> Tuple[bool, Optional[Any]]:
    """
    Safely read a frame from VideoCapture.
    
    Args:
        cap: VideoCapture object or None
        
    Returns:
        Tuple of (success, frame)
    """
    if cap is None:
        return False, None
    
    try:
        if not cap.isOpened():
            return False, None
        
        ret, frame = cap.read()
        return ret, frame
    except Exception as e:
        logger.error(f"Error in safe_read: {e}")
        return False, None

def safe_get_property(
    cap: Optional[cv2.VideoCapture], 
    prop_id: int, 
    default: float = 0.0
) -> float:
    """
    Safely get a property from VideoCapture.
    
    Args:
        cap: VideoCapture object or None
        prop_id: Property ID (e.g., cv2.CAP_PROP_FPS)
        default: Default value if property cannot be read
        
    Returns:
        Property value or default
    """
    if cap is None:
        return default
    
    try:
        return cap.get(prop_id)
    except Exception as e:
        logger.debug(f"Error getting property {prop_id}: {e}")
        return default

def safe_set_property(
    cap: Optional[cv2.VideoCapture], 
    prop_id: int, 
    value: float
) -> bool:
    """
    Safely set a property on VideoCapture.
    
    Args:
        cap: VideoCapture object or None
        prop_id: Property ID
        value: Value to set
        
    Returns:
        True if successful, False otherwise
    """
    if cap is None:
        return False
    
    try:
        return cap.set(prop_id, value)
    except Exception as e:
        logger.debug(f"Error setting property {prop_id} to {value}: {e}")
        return False

def test_video_source(source: str, timeout: float = 5.0) -> dict:
    """
    Test if a video source is accessible and readable.
    
    Args:
        source: Video source path/URL
        timeout: Timeout in seconds
        
    Returns:
        Dictionary with test results:
        {
            'accessible': bool,
            'readable': bool,
            'width': int,
            'height': int,
            'fps': float,
            'frame_count': int,
            'error': str or None
        }
    """
    result = {
        'accessible': False,
        'readable': False,
        'width': 0,
        'height': 0,
        'fps': 0.0,
        'frame_count': 0,
        'error': None
    }
    
    cap = None
    try:
        cap = cv2.VideoCapture(source)
        
        if cap is None or not cap.isOpened():
            result['error'] = "Failed to open video source"
            return result
        
        result['accessible'] = True
        
        # Try to read properties
        result['width'] = int(safe_get_property(cap, cv2.CAP_PROP_FRAME_WIDTH))
        result['height'] = int(safe_get_property(cap, cv2.CAP_PROP_FRAME_HEIGHT))
        result['fps'] = safe_get_property(cap, cv2.CAP_PROP_FPS)
        result['frame_count'] = int(safe_get_property(cap, cv2.CAP_PROP_FRAME_COUNT))
        
        # Try to read a frame
        ret, frame = safe_read(cap)
        if ret and frame is not None:
            result['readable'] = True
        else:
            result['error'] = "Cannot read frames from source"
        
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error testing video source {source}: {e}")
    finally:
        safe_release(cap)
    
    return result
