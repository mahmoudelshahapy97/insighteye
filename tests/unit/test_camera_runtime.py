"""Unit tests for camera_runtime: health derivation and live-vs-grab stills."""

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from app.services import camera_runtime

TZ = ZoneInfo("Africa/Cairo")
NOW = datetime(2026, 9, 26, 12, 0, 0, tzinfo=TZ)
SOURCE = "rtsp://admin:secret@10.0.0.9/exit"


def _shared(total_frames=250, uptime=10.0, last_error=None, frame=None):
    metrics = SimpleNamespace(total_frames=total_frames, consecutive_errors=0,
                              last_error=last_error, uptime_start=1000.0)
    return SimpleNamespace(state=SimpleNamespace(value="running"), metrics=metrics, latest_frame=frame), 1000.0 + uptime


def test_running_stream_is_healthy_with_fps():
    shared, now_ts = _shared()
    h = camera_runtime.build_health(
        {"status": "active", "last_frame_time": NOW - timedelta(seconds=2)}, shared, SOURCE, now=NOW, now_ts=now_ts,
    )
    assert h["live_status"] == "active"
    assert h["healthy"] is True
    assert h["avg_fps"] == 25.0
    assert h["state"] == "running"
    assert h["uptime_s"] == 10.0


def test_stale_frame_is_not_healthy():
    h = camera_runtime.build_health(
        {"status": "active", "last_frame_time": NOW - timedelta(seconds=45)}, None, SOURCE, now=NOW,
    )
    assert h["healthy"] is False
    assert h["frame_age_s"] == 45.0


def test_stopped_and_other_server():
    assert camera_runtime.build_health(None, None, SOURCE, now=NOW)["live_status"] == "stopped"
    assert camera_runtime.build_health(None, None, SOURCE, locked_by_server="srv-2", now=NOW)["live_status"] == \
        "running_on_other_server"


def test_last_error_is_redacted():
    shared, now_ts = _shared(last_error=f"failed to open {SOURCE}")
    h = camera_runtime.build_health(None, shared, SOURCE, now=NOW, now_ts=now_ts)
    assert "secret" not in h["last_error"]
    assert "***@10.0.0.9" in h["last_error"]


def test_source_host_masks_credentials():
    assert camera_runtime.source_host(SOURCE) == "rtsp://***@10.0.0.9/exit"
    assert camera_runtime.source_host(None) is None


@pytest.fixture
def managers(mocker):
    vfm = SimpleNamespace(shared_streams={})
    mocker.patch.object(camera_runtime, "_managers", return_value=(None, vfm))
    return vfm


async def test_still_prefers_live_frame(managers, mocker):
    frame = np.zeros((360, 640, 3), dtype=np.uint8)
    managers.shared_streams[SOURCE], _ = _shared(frame=frame)
    snap = mocker.patch.object(camera_runtime.rtsp_probe, "snapshot", mocker.AsyncMock())
    jpeg, info = await camera_runtime.live_or_grab_jpeg(SOURCE)
    assert jpeg[:2] == b"\xff\xd8"
    assert info == {"frame_source": "live", "width": 640, "height": 360, "error": None}
    snap.assert_not_awaited()


async def test_still_falls_back_to_grab_and_reports_error(managers, mocker):
    frame = np.zeros((720, 1280, 3), dtype=np.uint8)
    snap = mocker.patch.object(camera_runtime.rtsp_probe, "snapshot",
                               mocker.AsyncMock(return_value=(frame, {"reachable": True, "error": None})))
    jpeg, info = await camera_runtime.live_or_grab_jpeg(SOURCE)
    assert info["frame_source"] == "grab" and info["width"] == 1280
    snap.return_value = (None, {"reachable": False, "error": "timed out"})
    jpeg, info = await camera_runtime.live_or_grab_jpeg(SOURCE)
    assert jpeg is None and info["error"] == "timed out"
