# app/services/lost_found/visual.py (ported from lost-found app/utils/visual_similarity.py)
"""Photo-to-crop similarity without a model: perceptual hash + colour histogram.

LostNet (Zhou et al., "LostNet: A smart way for lost and find") retrieves
registered items by perceptual hash once a classifier has narrowed the category.
Here the category comes from the report and the detector, so only the
comparison is needed. A perceptual hash alone is brittle between a reporter's
phone photo and a small CCTV crop - different angle, light and resolution - so
it is blended with an HSV colour histogram, which survives those changes far
better.
"""

from dataclasses import dataclass
from typing import Optional

import cv2
import numpy as np

# Hue x saturation x value. Coarse on purpose: lighting differs between a phone
# photo and a CCTV frame, and coarse bins absorb the shift.
_HIST_BINS = (12, 4, 4)
_HIST_RANGES = (0, 180, 0, 256, 0, 256)
_HASH_BITS = 64
_MASK = (1 << _HASH_BITS) - 1


@dataclass
class VisualFeatures:
    phash: int  # signed 64-bit, to fit a Postgres BIGINT
    histogram: np.ndarray  # float32, L1-normalised

    @property
    def histogram_bytes(self) -> bytes:
        return self.histogram.astype(np.float32).tobytes()


def central(image: np.ndarray, keep: float) -> np.ndarray:
    """The middle ``keep`` fraction of an image, dropping background at the borders."""
    if keep >= 1.0:
        return image
    height, width = image.shape[:2]
    dy, dx = int(height * (1 - keep) / 2), int(width * (1 - keep) / 2)
    inner = image[dy : height - dy, dx : width - dx]
    return inner if inner.size else image


def perceptual_hash(image: np.ndarray) -> int:
    """64-bit DCT hash (LostNet §IV): greyscale, shrink, DCT, keep the 8x8 low
    frequencies, set each bit where the coefficient is above their mean."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if image.ndim == 3 else image
    small = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA).astype(np.float32)
    low = cv2.dct(small)[:8, :8]
    value = 0
    for bit in (low > low.mean()).flatten():
        value = (value << 1) | int(bit)
    return value - (1 << _HASH_BITS) if value >= 1 << (_HASH_BITS - 1) else value


def color_histogram(image: np.ndarray) -> np.ndarray:
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    hist = cv2.calcHist([hsv], [0, 1, 2], None, list(_HIST_BINS), list(_HIST_RANGES)).flatten()
    total = float(hist.sum())
    return (hist / total if total > 0 else hist).astype(np.float32)


def extract(image: Optional[np.ndarray], keep: float = 1.0) -> Optional[VisualFeatures]:
    """Features for one image, or None when it is unusable."""
    if image is None or image.size == 0 or min(image.shape[:2]) < 8:
        return None
    region = central(image, keep)
    return VisualFeatures(perceptual_hash(region), color_histogram(region))


def histogram_from_bytes(data: Optional[bytes]) -> Optional[np.ndarray]:
    if not data:
        return None
    hist = np.frombuffer(bytes(data), dtype=np.float32)
    return hist if hist.size == int(np.prod(_HIST_BINS)) else None


def hash_similarity(a: int, b: int) -> float:
    """1 for identical hashes, 0 at 32 differing bits - what unrelated images average."""
    distance = bin((a ^ b) & _MASK).count("1")
    return max(0.0, 1.0 - distance / (_HASH_BITS / 2))


def histogram_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """1 - Bhattacharyya distance: 1 for the same colour distribution, 0 for disjoint."""
    distance = cv2.compareHist(a, b, cv2.HISTCMP_BHATTACHARYYA)
    return float(min(1.0, max(0.0, 1.0 - distance)))


def similarity(
    a_hash: Optional[int],
    a_hist: Optional[np.ndarray],
    b_hash: Optional[int],
    b_hist: Optional[np.ndarray],
    hash_share: float,
) -> Optional[float]:
    """Blend of histogram and hash similarity; None when either side lacks features."""
    parts = []
    if a_hist is not None and b_hist is not None:
        parts.append((1.0 - hash_share, histogram_similarity(a_hist, b_hist)))
    if a_hash is not None and b_hash is not None:
        parts.append((hash_share, hash_similarity(a_hash, b_hash)))
    weight = sum(w for w, _ in parts)
    if not parts or weight <= 0:
        return None
    return sum(w * value for w, value in parts) / weight
