# app/services/lost_found/color.py (ported from lost-found app/utils/color_utils.py)
"""Dominant colour extraction, used as a cheap secondary matching attribute."""

import logging
import re
from typing import Optional, Tuple

import cv2
import numpy as np

logger = logging.getLogger(__name__)

# (name, hue_low, hue_high) in OpenCV HSV space where hue is 0-179.
_HUE_NAMES = (
    ("red", 0, 9),
    ("orange", 10, 22),
    ("yellow", 23, 33),
    ("green", 34, 85),
    ("cyan", 86, 100),
    ("blue", 101, 130),
    ("purple", 131, 155),
    ("pink", 156, 169),
    ("red", 170, 179),
)

_SATURATION_MIN = 45
_VALUE_DARK_MAX = 55
_VALUE_LIGHT_MIN = 205


def dominant_color(image: np.ndarray, k: int = 3) -> Optional[str]:
    """Name the dominant colour of a crop via k-means in HSV space.

    Returns a coarse colour word ("black", "blue", "beige", ...) or None when the
    crop is unusable. Coarse on purpose: it only has to survive a rerank bonus.
    """
    if image is None or image.size == 0:
        return None

    try:
        # Downscale first: k-means on a full crop is needless work.
        small = cv2.resize(image, (48, 48), interpolation=cv2.INTER_AREA)
        hsv = cv2.cvtColor(small, cv2.COLOR_BGR2HSV)
        pixels = hsv.reshape(-1, 3).astype(np.float32)

        criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 10, 1.0)
        clusters = min(k, len(pixels))
        _, labels, centers = cv2.kmeans(
            pixels, clusters, None, criteria, 3, cv2.KMEANS_PP_CENTERS
        )

        counts = np.bincount(labels.flatten(), minlength=clusters)
        h, s, v = centers[int(np.argmax(counts))]
        return _name_hsv(float(h), float(s), float(v))
    except Exception as e:
        logger.debug("dominant_color failed: %s", e)
        return None


def _name_hsv(h: float, s: float, v: float) -> str:
    """Map an HSV triple to a colour word."""
    if v <= _VALUE_DARK_MAX:
        return "black"
    if s < _SATURATION_MIN:
        if v >= _VALUE_LIGHT_MIN:
            return "white"
        return "gray"
    if s < 90 and 10 <= h <= 30 and v > 120:
        return "beige"

    for name, low, high in _HUE_NAMES:
        if low <= h <= high:
            # Dark warm hues read as brown far more often than as red/orange.
            if name in ("red", "orange") and v < 140:
                return "brown"
            return name
    return "unknown"


_COLOR_WORDS = {name for name, _, _ in _HUE_NAMES} | {"black", "white", "gray", "beige", "brown"}
_COLOR_ALIASES = {"grey": "gray", "silver": "gray", "navy": "blue", "tan": "beige", "cream": "beige"}


def color_from_text(text: Optional[str]) -> Optional[str]:
    """First colour word in a description ("dark-blue bag" -> "blue").

    Lets a text-only search earn the same colour bonus a photo would.
    """
    if not text:
        return None
    for word in re.findall(r"[a-z]+", text.lower()):
        word = _COLOR_ALIASES.get(word, word)
        if word in _COLOR_WORDS:
            return word
    return None


def color_agreement(a: Optional[str], b: Optional[str]) -> bool:
    """Whether two colour names should count as the same item colour."""
    if not a or not b:
        return False
    if a == b:
        return True
    neutral: Tuple[str, ...] = ("black", "gray", "brown")
    return a in neutral and b in neutral
