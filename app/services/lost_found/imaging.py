# app/services/lost_found/imaging.py (ported from lost-found app/utils/image_utils.py)
"""JPEG encode/decode, crops and the annotations drawn on lost & found evidence."""

import logging
from typing import Optional, Sequence, Tuple

import cv2
import numpy as np

from app.services.lost_found.geometry import Box, clamp_box

logger = logging.getLogger(__name__)

JPEG_QUALITY = 85


def encode_jpeg(frame: np.ndarray, quality: int = JPEG_QUALITY) -> Optional[bytes]:
    """Encode a BGR frame as JPEG bytes."""
    if frame is None or frame.size == 0:
        return None
    success, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    if not success:
        logger.error("cv2.imencode failed during JPEG encoding.")
        return None
    return buffer.tobytes()


def decode_jpeg(data: bytes) -> Optional[np.ndarray]:
    """Decode JPEG/PNG bytes into a BGR frame."""
    if not data:
        return None
    try:
        nparr = np.frombuffer(data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    except Exception as e:
        logger.error("Error decoding image bytes: %s", e, exc_info=True)
        return None


def crop_box(
    frame: np.ndarray, box: Box, padding: int = 8, context_ratio: float = 0.0
) -> Optional[np.ndarray]:
    """Crop a padded region of the frame; returns None when the crop is empty.

    ``context_ratio`` widens the pad to a fraction of the box's longer side:
    a tight 80 px crop gives an embedder little to work with, a little
    surrounding context gives it shape and scale.
    """
    if frame is None or frame.size == 0:
        return None

    height, width = frame.shape[:2]
    x1, y1, x2, y2 = box
    if context_ratio > 0:
        padding = max(padding, int(round(max(x2 - x1, y2 - y1) * context_ratio)))
    padded = (x1 - padding, y1 - padding, x2 + padding, y2 + padding)
    x1, y1, x2, y2 = clamp_box(padded, width, height)

    crop = frame[int(y1) : int(y2), int(x1) : int(x2)]
    return crop if crop.size else None


# BGR colours
_COLOR_OBJECT = (0, 165, 255)      # orange - tracked object
_COLOR_ABANDONED = (0, 0, 255)     # red - abandoned
_COLOR_PERSON = (0, 200, 0)        # green - person
_COLOR_ROI = (255, 200, 0)         # cyan-ish - region of interest


def draw_box(
    frame: np.ndarray,
    box: Box,
    label: str = "",
    color: Tuple[int, int, int] = _COLOR_OBJECT,
    thickness: int = 2,
) -> np.ndarray:
    """Draw a labelled rectangle in place and return the frame."""
    x1, y1, x2, y2 = (int(v) for v in box)
    cv2.rectangle(frame, (x1, y1), (x2, y2), color, thickness)

    if label:
        (tw, th), baseline = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
        top = max(0, y1 - th - baseline - 4)
        cv2.rectangle(frame, (x1, top), (x1 + tw + 6, top + th + baseline + 4), color, -1)
        cv2.putText(
            frame,
            label,
            (x1 + 3, top + th + 2),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (255, 255, 255),
            1,
            cv2.LINE_AA,
        )
    return frame


def draw_roi(frame: np.ndarray, polygon: Sequence[Sequence[float]]) -> np.ndarray:
    """Outline the configured region of interest."""
    if not polygon or len(polygon) < 3:
        return frame
    points = np.array([[int(p[0]), int(p[1])] for p in polygon], dtype=np.int32)
    cv2.polylines(frame, [points], isClosed=True, color=_COLOR_ROI, thickness=2)
    return frame


def annotate_abandoned(
    frame: np.ndarray,
    box: Box,
    class_name: str,
    unattended_seconds: float,
) -> np.ndarray:
    """Produce the alert frame stored alongside an abandoned event."""
    annotated = frame.copy()
    draw_box(
        annotated,
        box,
        f"ABANDONED {class_name} ({unattended_seconds:.0f}s)",
        _COLOR_ABANDONED,
        thickness=3,
    )
    return annotated


_COLOR_UNATTENDED = (0, 215, 255)  # yellow - owner gone, clock running
_COLOR_HEADER = (32, 32, 32)

# Tracked-object status (abandonment_service.ObjectStatus values) -> box colour
_STATUS_COLORS = {
    "MOVING": _COLOR_OBJECT,
    "ATTENDED": _COLOR_PERSON,
    "UNATTENDED": _COLOR_UNATTENDED,
    "ABANDONED": _COLOR_ABANDONED,
}


def _track_label(track: dict) -> str:
    status = track.get("status", "")
    label = f"#{track.get('track_id')} {track.get('class_name', '')} {status}"
    if status in ("UNATTENDED", "ABANDONED") and track.get("unattended_seconds"):
        label += f" {track['unattended_seconds']:.0f}s"
    return label


def annotate_tracks(
    frame: np.ndarray,
    persons: Sequence[Box],
    tracks: Sequence[dict],
    roi: Optional[Sequence[Sequence[float]]] = None,
    header: str = "",
) -> np.ndarray:
    """The live view: every tracked object coloured by its abandonment status.

    ``tracks`` is ``AbandonmentTracker.snapshot()``; ``persons`` are the boxes
    of the people detected in this frame. Draws on a copy.
    """
    annotated = frame.copy()
    if roi:
        draw_roi(annotated, roi)

    for box in persons:
        draw_box(annotated, box, color=_COLOR_PERSON, thickness=1)

    for track in tracks:
        status = track.get("status", "")
        draw_box(
            annotated,
            track["box"],
            _track_label(track),
            _STATUS_COLORS.get(status, _COLOR_OBJECT),
            thickness=3 if status == "ABANDONED" else 2,
        )

    if header:
        (_, th), baseline = cv2.getTextSize(header, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 1)
        bar = th + baseline + 10
        cv2.rectangle(annotated, (0, 0), (annotated.shape[1], bar), _COLOR_HEADER, -1)
        cv2.putText(
            annotated,
            header,
            (8, th + 5),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (255, 255, 255),
            1,
            cv2.LINE_AA,
        )
    return annotated
