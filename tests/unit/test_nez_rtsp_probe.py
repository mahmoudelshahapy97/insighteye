"""rtsp_probe against real sources: a local video file and an address nobody answers on."""

import asyncio
import time

import cv2
import numpy as np

from app.services.nez import rtsp_probe


def _video(tmp_path, w=320, h=240, frames=15):
    path = str(tmp_path / "clip.mp4")
    writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"mp4v"), 15.0, (w, h))
    for i in range(frames):
        writer.write(np.full((h, w, 3), i * 10 % 255, dtype=np.uint8))
    writer.release()
    return path


def test_reachable_source_reports_its_size(tmp_path):
    frame, info = rtsp_probe.grab_frame(_video(tmp_path))
    assert frame is not None and frame.shape == (240, 320, 3)
    assert info["reachable"] is True and (info["width"], info["height"]) == (320, 240)
    assert info["latency_ms"] is not None and info["error"] is None


def test_unreachable_camera_fails_fast_and_hides_credentials():
    started = time.monotonic()
    frame, info = rtsp_probe.grab_frame("rtsp://admin:hunter2@127.0.0.1:9/none", timeout_s=3)
    assert frame is None and info["reachable"] is False
    assert time.monotonic() - started < 15
    assert "hunter2" not in (info["error"] or "")


def test_async_probe_and_snapshot(tmp_path):
    path = _video(tmp_path)
    info = asyncio.run(rtsp_probe.probe(path))
    assert info["reachable"] is True
    frame, _ = asyncio.run(rtsp_probe.snapshot(path))
    assert frame is not None


def test_redact_url():
    assert rtsp_probe.redact_url("rtsp://u:p@h:554/s") == "rtsp://***@h:554/s"
    assert rtsp_probe.redact_url("rtsp://h/s") == "rtsp://h/s"
