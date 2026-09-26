"""Read-only view of a camera's runtime state, for feature Cameras pages.

Stream health only lives in memory: ``stream_manager.active_streams`` (per stream,
status + last frame time) and ``video_file_manager.shared_streams`` (per source,
capture state + metrics + the latest raw frame). This module reads both without
ever creating or starting anything, and never returns the raw source URL.

Shared by feature modules (blocked exit today); nothing here is feature-specific.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import cv2

from app.services.nez import rtsp_probe

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Africa/Cairo")
# Same rule the stream manager uses to call a stream healthy (stream_service.py).
HEALTHY_FRAME_AGE_S = 30.0


def source_host(source: Optional[str]) -> Optional[str]:
    """The source with credentials masked — safe to show in the UI."""
    return rtsp_probe.redact_url(source) if source else None


def _managers():
    # Imported lazily: both modules pull in the whole streaming stack.
    from app.services.stream_service import stream_manager
    from app.services.shared_stream_service import video_file_manager
    return stream_manager, video_file_manager


def build_health(
    active: Optional[Dict[str, Any]],
    shared: Any,
    source: Optional[str],
    *,
    locked_by_server: Any = None,
    now: Optional[datetime] = None,
    now_ts: Optional[float] = None,
) -> Dict[str, Any]:
    """Pure: turn an active_streams entry and a SharedVideoStream into the health block."""
    now = now or datetime.now(_TZ)
    now_ts = time.time() if now_ts is None else now_ts

    if active is not None:
        live_status = str(active.get("status") or "starting")
    elif locked_by_server:
        live_status = "running_on_other_server"
    else:
        live_status = "stopped"

    last_frame_at = active.get("last_frame_time") if active else None
    frame_age = (now - last_frame_at).total_seconds() if last_frame_at else None

    health: Dict[str, Any] = {
        "live_status": live_status,
        "last_frame_at": last_frame_at,
        "frame_age_s": round(frame_age, 1) if frame_age is not None else None,
        "healthy": bool(frame_age is not None and frame_age < HEALTHY_FRAME_AGE_S),
        "state": None,
        "total_frames": None,
        "consecutive_errors": None,
        "last_error": None,
        "uptime_s": None,
        "avg_fps": None,
    }

    if shared is not None:
        metrics = getattr(shared, "metrics", None)
        state = getattr(shared, "state", None)
        health["state"] = getattr(state, "value", state)
        if metrics is not None:
            health["total_frames"] = metrics.total_frames
            health["consecutive_errors"] = metrics.consecutive_errors
            if metrics.last_error:
                health["last_error"] = rtsp_probe._redact_text(str(metrics.last_error), source or "")
            # Wall-clock timestamps (time.time()) in StreamMetrics.
            if metrics.uptime_start:
                uptime = max(0.0, now_ts - metrics.uptime_start)
                health["uptime_s"] = round(uptime, 1)
                if uptime > 0 and metrics.total_frames:
                    health["avg_fps"] = round(metrics.total_frames / uptime, 1)
    return health


async def camera_health(stream_id: Any, source: Optional[str], *, locked_by_server: Any = None) -> Dict[str, Any]:
    stream_manager, video_file_manager = _managers()
    async with stream_manager._lock:
        active = stream_manager.active_streams.get(str(stream_id))
        active = dict(active) if active is not None else None
    # .get only — get_shared_stream() would create (and later start) a stream.
    shared = video_file_manager.shared_streams.get(source) if source else None
    return build_health(active, shared, source, locked_by_server=locked_by_server)


def _encode_jpeg(frame) -> Optional[bytes]:
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
    return buf.tobytes() if ok else None


async def live_or_grab_jpeg(source: str) -> Tuple[Optional[bytes], Dict[str, Any]]:
    """A still of the camera: the running stream's latest frame if there is one
    ("live"), otherwise a one-off grab from the source ("grab").

    Returns (jpeg or None, {"frame_source", "width", "height", "error"}).
    """
    _, video_file_manager = _managers()
    shared = video_file_manager.shared_streams.get(source)
    frame = getattr(shared, "latest_frame", None) if shared is not None else None
    frame_source = "live"
    error = None

    if frame is None:
        frame_source = "grab"
        frame, info = await rtsp_probe.snapshot(source)
        error = info.get("error")

    if frame is None:
        return None, {"frame_source": frame_source, "width": None, "height": None,
                      "error": error or "no frame received"}

    frame = frame.copy()
    height, width = frame.shape[:2]
    jpeg = await asyncio.get_running_loop().run_in_executor(None, _encode_jpeg, frame)
    if jpeg is None:
        return None, {"frame_source": frame_source, "width": None, "height": None, "error": "could not encode frame"}
    return jpeg, {"frame_source": frame_source, "width": width, "height": height, "error": None}
