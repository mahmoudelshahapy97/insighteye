# app/utils/image_utils.py
"""Image processing utilities for frame conversion."""

import logging
import base64
import os
from typing import Optional

import cv2
import numpy as np

from app.config.settings import config

logger = logging.getLogger(__name__)

# Load default base64 image
encoded_string = ""
try:
    image_path = os.path.join("images", "base64_1.jpg")#os.path.dirname(__file__), 
    if os.path.exists(image_path):
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    else:
        logger.warning(f"Default image '{image_path}' not found. Static base64 image will be empty.")
except Exception as e:
    logger.error(f"Error loading default image: {e}", exc_info=True)


def frame_to_base64(frame: np.ndarray) -> str:
    """Converts a OpenCV frame (numpy array) to a base64 encoded string."""
    if frame is None: 
        return ""
    
    success, buffer = cv2.imencode(
        '.jpg', 
        frame, 
        [int(cv2.IMWRITE_JPEG_QUALITY), config.jpeg_quality]
    )
    if not success:
        logger.error("cv2.imencode failed during frame_to_base64 conversion.")
        return ""
    return base64.b64encode(buffer).decode('utf-8')
