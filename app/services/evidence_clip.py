"""Evidence clips shared by the detection engines (no-entry-zone, blocked-exit).

``ClipRecorder`` keeps a short pre-roll of (already annotated, downscaled) frames per
stream; ``start`` opens a clip that begins with that pre-roll and keeps collecting until
``post_seconds`` after the trigger. Finished clips come back from ``pop_finished`` as raw
frames plus an fps derived from their timestamps (engines run at a reduced, uneven rate),
and ``encode_mp4`` turns them into an H.264 file for upload.
"""

from __future__ import annotations

import logging
import subprocess
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Tuple

import cv2
import numpy as np

logger = logging.getLogger(__name__)


def resize_max_width(frame: np.ndarray, max_width: int) -> np.ndarray:
    h, w = frame.shape[:2]
    if w <= max_width:
        return frame
    scale = max_width / float(w)
    return cv2.resize(frame, (max_width, int(round(h * scale))))


@dataclass
class _ClipJob:
    key: str
    frames: List[Tuple[float, np.ndarray]]
    until_ts: float


class ClipRecorder:
    """Pre-roll buffer plus the clips being recorded for one stream. Not thread-safe:
    each stream's frames are processed one at a time."""

    def __init__(self) -> None:
        self.preroll: Deque[Tuple[float, np.ndarray]] = deque()
        self.jobs: List[_ClipJob] = []

    @property
    def busy(self) -> bool:
        return bool(self.jobs)

    def push(self, ts: float, frame: np.ndarray, pre_seconds: float) -> None:
        self.preroll.append((ts, frame))
        while self.preroll and ts - self.preroll[0][0] > pre_seconds:
            self.preroll.popleft()
        for job in self.jobs:
            job.frames.append((ts, frame))

    def start(self, key: str, ts: float, post_seconds: float) -> None:
        self.jobs.append(_ClipJob(key=key, frames=list(self.preroll), until_ts=ts + post_seconds))

    def pop_finished(self, ts: float, force: bool = False) -> List[Dict[str, Any]]:
        """``[{"key", "frames", "fps"}]`` for clips past their end (all of them with
        ``force``). Clips of fewer than two frames are dropped."""
        done, keep = [], []
        for job in self.jobs:
            (done if force or ts >= job.until_ts else keep).append(job)
        self.jobs = keep
        clips = []
        for job in done:
            if len(job.frames) < 2:
                continue
            span = job.frames[-1][0] - job.frames[0][0]
            fps = (len(job.frames) - 1) / span if span > 0 else 5.0
            clips.append({
                "key": job.key,
                "frames": [f for _, f in job.frames],
                "fps": max(1.0, min(30.0, fps)),
            })
        return clips


def encode_mp4(frames: List[np.ndarray], fps: float, out_path: str) -> bool:
    """Encode BGR frames to an H.264 mp4 with ffmpeg (blocking; run it in a thread)."""
    if not frames:
        return False
    h, w = frames[0].shape[:2]
    w -= w % 2
    h -= h % 2  # libx264 + yuv420p needs even dimensions
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "rawvideo", "-vcodec", "rawvideo", "-s", f"{w}x{h}",
        "-pix_fmt", "bgr24", "-r", f"{fps:.3f}", "-i", "pipe:0",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", out_path,
    ]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        for f in frames:
            if f.shape[:2] != (h, w):
                f = cv2.resize(f, (w, h))
            proc.stdin.write(np.ascontiguousarray(f).tobytes())
        proc.stdin.close()
    except BrokenPipeError:
        pass
    proc.wait()
    if proc.returncode != 0:
        logger.error(f"[evidence-clip] ffmpeg failed: {proc.stderr.read().decode(errors='ignore')[:500]}")
        return False
    return True
