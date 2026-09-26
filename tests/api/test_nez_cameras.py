"""No-entry-zone Cameras endpoints: health merge, credential redaction, probes, snapshots."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest

from app.api.routes import no_entry_zone_router as mod
from test_nez_router import WORKSPACE, _client  # sibling module (tests/api has no package init)

CAM = uuid4()
SECRET_URL = "rtsp://admin:hunter2@10.0.0.5:554/stream1"


def _row(stream_id=CAM, **kw):
    row = {
        "stream_id": stream_id, "name": "gate", "location": "Dock", "building": None, "floor_level": None,
        "status": "active", "is_streaming": True, "is_no_entry_zone_camera": True, "zone_count": 2,
        "active_zone_count": 1, "type": "rtsp", "path": SECRET_URL, "last_activity": None,
        "stop_reason": None, "retry_count": 0, "auto_retry_enabled": True, "locked_by_server": uuid4(),
    }
    row.update(kw)
    return row


@pytest.fixture
def manager(mocker):
    """A stand-in StreamManager holding one running stream with a shared capture."""
    shared = SimpleNamespace(
        state=SimpleNamespace(value="running"),
        metrics=SimpleNamespace(
            total_frames=600, uptime_start=datetime.now(timezone.utc).timestamp() - 60,
            connection_attempts=1, consecutive_errors=0,
            last_error=f"read timeout on {SECRET_URL}",
        ),
        latest_frame=np.zeros((360, 640, 3), dtype=np.uint8),
    )
    fake = SimpleNamespace(
        _lock=asyncio.Lock(),
        active_streams={str(CAM): {
            "status": "active", "source": SECRET_URL,
            "last_frame_time": datetime.now(timezone.utc) - timedelta(seconds=2),
        }},
        video_file_manager=SimpleNamespace(shared_streams={SECRET_URL: shared}),
    )
    mocker.patch("app.services.stream_service.stream_manager", fake)
    return fake


@pytest.fixture
def admin(mocker):
    return _client(mocker, "admin")


async def test_camera_list_merges_live_health_and_hides_credentials(admin, mocker, manager):
    other = uuid4()
    mocker.patch.object(mod.no_entry_zone_service, "list_cameras", mocker.AsyncMock(return_value=[
        _row(), _row(stream_id=other, name="elsewhere", path="rtsp://cam2/live"),
    ]))
    async with admin as c:
        r = await c.get("/no-entry-zone/cameras")
    assert r.status_code == 200, r.text
    assert "hunter2" not in r.text and '"path"' not in r.text
    live, remote = r.json()
    assert live["source_display"] == "rtsp://***@10.0.0.5:554/stream1"
    h = live["health"]
    assert h["live_status"] == "active" and h["healthy"] is True
    assert h["resolution"] == "640x360" and 9 <= h["avg_fps"] <= 11
    assert h["last_error"] == "read timeout on rtsp://***@10.0.0.5:554/stream1"
    # Streaming and locked, but not in this server's memory: another server runs it.
    assert remote["health"]["live_status"] == "running_on_other_server"
    assert remote["health"]["running_elsewhere"] is True


async def test_camera_endpoints_404_outside_the_workspace(admin, mocker, manager):
    mocker.patch.object(mod.no_entry_zone_service, "list_cameras", mocker.AsyncMock(return_value=[]))
    probe = mocker.patch.object(mod.rtsp_probe, "probe", mocker.AsyncMock())
    async with admin as c:
        assert (await c.get(f"/no-entry-zone/cameras/{CAM}")).status_code == 404
        assert (await c.get(f"/no-entry-zone/cameras/{CAM}/snapshot")).status_code == 404
        assert (await c.post(f"/no-entry-zone/cameras/{CAM}/probe")).status_code == 404
    probe.assert_not_awaited()
    assert mod.no_entry_zone_service.list_cameras.await_args.args[0] == WORKSPACE


async def test_snapshot_prefers_the_live_frame(admin, mocker, manager):
    mocker.patch.object(mod.no_entry_zone_service, "list_cameras", mocker.AsyncMock(return_value=[_row()]))
    grab = mocker.patch.object(mod.rtsp_probe, "snapshot", mocker.AsyncMock())
    async with admin as c:
        r = await c.get(f"/no-entry-zone/cameras/{CAM}/snapshot")
    assert r.status_code == 200 and r.content[:2] == b"\xff\xd8"
    assert r.headers["x-frame-source"] == "live" and r.headers["x-frame-size"] == "640x360"
    grab.assert_not_awaited()


async def test_snapshot_grabs_from_a_stopped_camera_and_reports_failures(admin, mocker, manager):
    manager.active_streams.clear()
    manager.video_file_manager.shared_streams.clear()
    mocker.patch.object(mod.no_entry_zone_service, "list_cameras",
                        mocker.AsyncMock(return_value=[_row(is_streaming=False)]))
    grab = mocker.patch.object(mod.rtsp_probe, "snapshot", mocker.AsyncMock(side_effect=[
        (np.zeros((720, 1280, 3), dtype=np.uint8), {"reachable": True}),
        (None, {"reachable": False, "error": "cannot open rtsp://***@10.0.0.5:554/stream1"}),
    ]))
    async with admin as c:
        ok = await c.get(f"/no-entry-zone/cameras/{CAM}/snapshot")
        bad = await c.get(f"/no-entry-zone/cameras/{CAM}/snapshot")
    assert ok.status_code == 200 and ok.headers["x-frame-source"] == "grab"
    assert ok.headers["x-frame-size"] == "1280x720"
    assert grab.await_args.args[0] == SECRET_URL  # the real URL is used server-side only
    assert bad.status_code == 502 and "unreachable" in bad.json()["detail"]
    assert "hunter2" not in bad.text


async def test_probe_all_checks_every_nez_camera(admin, mocker, manager):
    second = uuid4()
    mocker.patch.object(mod.no_entry_zone_service, "list_cameras", mocker.AsyncMock(return_value=[
        _row(), _row(stream_id=second, name="yard", path="rtsp://10.255.255.1/x"),
    ]))
    results = {
        SECRET_URL: {"reachable": True, "latency_ms": 45, "width": 1280, "height": 720, "fps": 25.0, "error": None},
        "rtsp://10.255.255.1/x": {"reachable": False, "latency_ms": 8000, "error": "cannot open rtsp://10.255.255.1/x"},
    }
    mocker.patch.object(mod.rtsp_probe, "probe", mocker.AsyncMock(side_effect=lambda url: results[url]))
    async with admin as c:
        r = await c.post("/no-entry-zone/cameras/probe-all")
    assert r.status_code == 200, r.text
    body = {item["name"]: item for item in r.json()}
    assert body["gate"]["reachable"] is True and body["gate"]["width"] == 1280
    assert body["yard"]["reachable"] is False and "cannot open" in body["yard"]["error"]
    assert mod.no_entry_zone_service.list_cameras.await_args.kwargs["enabled_only"] is True
