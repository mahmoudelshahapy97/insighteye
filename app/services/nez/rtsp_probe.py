"""One-off camera probe: open a source, read a frame, report what happened.

Used by the no-entry-zone Cameras page to test connections and to fetch a still to draw
zones on while detection is not running (the normal frame endpoints only have frames
for streams that are being processed).

Deliberately separate from SharedVideoStream: a probe must never register a subscriber
or start processing, and it must give up quickly. Timeouts are passed per capture
rather than through OPENCV_FFMPEG_CAPTURE_OPTIONS, which is process-wide and which the
shared stream code clears.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import cv2
import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = 8.0
# A "check all cameras" over a large workspace must not open dozens of RTSP sessions at
# once — cameras (and the network) cope badly with that.
_MAX_CONCURRENT = 4
_semaphore: Optional[asyncio.Semaphore] = None


def redact_url(url: str) -> str:
    """``rtsp://user:pass@host/path`` -> ``rtsp://***@host/path``."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable url>"
    if "@" not in parts.netloc:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit((parts.scheme, f"***@{host}", parts.path, parts.query, parts.fragment))


def _redact_text(text: str, source: str) -> str:
    return text.replace(source, redact_url(source)) if source else text


def grab_frame(source: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> Tuple[Optional[np.ndarray], Dict[str, Any]]:
    """Blocking. Returns ``(frame or None, info)`` where info is
    ``{reachable, latency_ms, width, height, fps, error}``."""
    started = time.monotonic()
    info: Dict[str, Any] = {
        "reachable": False, "latency_ms": None, "width": None, "height": None, "fps": None, "error": None,
    }
    timeout_ms = int(timeout_s * 1000)
    cap = None
    try:
        cap = cv2.VideoCapture(
            source,
            cv2.CAP_FFMPEG,
            [cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout_ms, cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout_ms],
        )
        if cap is None or not cap.isOpened():
            info["error"] = f"cannot open {redact_url(source)}"
            return None, info
        frame = None
        for _ in range(3):  # the first packets of an RTSP session are often not decodable yet
            ok, candidate = cap.read()
            if ok and candidate is not None and candidate.size > 0:
                frame = candidate
                break
        if frame is None:
            info["error"] = "connected but no frame could be decoded"
            return None, info
        fps = cap.get(cv2.CAP_PROP_FPS)
        info.update(
            reachable=True,
            width=int(frame.shape[1]),
            height=int(frame.shape[0]),
            fps=round(float(fps), 2) if fps and 0 < fps < 1000 else None,
        )
        return frame, info
    except Exception as e:  # cv2 raises on some malformed sources
        info["error"] = _redact_text(str(e), source)[:300]
        return None, info
    finally:
        info["latency_ms"] = int((time.monotonic() - started) * 1000)
        if cap is not None:
            try:
                cap.release()
            except Exception:
                pass


async def _run(source: str, timeout_s: float):
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    async with _semaphore:
        loop = asyncio.get_running_loop()
        # Hard ceiling in case the backend ignores the capture timeouts.
        return await asyncio.wait_for(
            loop.run_in_executor(None, grab_frame, source, timeout_s), timeout=timeout_s * 2 + 5,
        )


async def probe(source: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> Dict[str, Any]:
    try:
        _, info = await _run(source, timeout_s)
    except asyncio.TimeoutError:
        info = {"reachable": False, "latency_ms": None, "width": None, "height": None, "fps": None,
                "error": "timed out"}
    return info


async def snapshot(source: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> Tuple[Optional[np.ndarray], Dict[str, Any]]:
    try:
        return await _run(source, timeout_s)
    except asyncio.TimeoutError:
        return None, {"reachable": False, "error": "timed out"}
