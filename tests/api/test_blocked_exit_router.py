"""API tests for the blocked-exit router's workspace scoping and validation.

Runs the router on a minimal app with auth and the service mocked, so it does
not need a login or the rest of the application.
"""

from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.routes import blocked_exit_router as mod
from app.services.session_service import session_manager
from app.services.feature_service import ALL_FEATURES, feature_service

WORKSPACE = uuid4()
USER = {"user_id": uuid4(), "username": "alice", "role": "user"}


@pytest.fixture
def client(mocker):
    app = FastAPI()
    app.include_router(mod.router)
    app.dependency_overrides[session_manager.get_current_user_full_data_dependency] = lambda: USER
    mocker.patch.object(mod, "get_workspace_id_for_user", mocker.AsyncMock(return_value=WORKSPACE))
    # Feature access is covered in test_feature_service.py; open everything here.
    mocker.patch("app.services.feature_service.current_workspace_id", mocker.AsyncMock(return_value=WORKSPACE))
    mocker.patch.object(feature_service, "effective_features", mocker.AsyncMock(return_value=ALL_FEATURES))
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_resolve_passes_callers_workspace_and_404s_when_not_found(client, mocker):
    resolve = mocker.patch.object(mod.blocked_exit_service, "resolve_event", mocker.AsyncMock(return_value=False))
    async with client as c:
        r = await c.post("/blocked-exit/events/42/resolve", json={"status": "resolved"})
    assert r.status_code == 404
    kwargs = resolve.await_args.kwargs
    assert kwargs["workspace_id"] == WORKSPACE
    assert kwargs["event_id"] == 42
    assert kwargs["user_id"] == USER["user_id"]


async def test_resolve_rejects_unknown_status(client, mocker):
    resolve = mocker.patch.object(mod.blocked_exit_service, "resolve_event", mocker.AsyncMock())
    async with client as c:
        r = await c.post("/blocked-exit/events/42/resolve", json={"status": "deleted"})
    assert r.status_code == 422
    resolve.assert_not_awaited()


async def test_camera_endpoints_404_for_camera_outside_workspace(client, mocker):
    mocker.patch.object(mod.blocked_exit_service, "get_camera", mocker.AsyncMock(return_value=None))
    set_polygon = mocker.patch.object(mod.blocked_exit_service, "set_door_polygon", mocker.AsyncMock())
    stream = uuid4()
    body = {"door_polygon": [[0, 0], [10, 0], [10, 10]], "calibration_frame_w": 640, "calibration_frame_h": 360}
    async with client as c:
        assert (await c.put(f"/blocked-exit/cameras/{stream}/door-polygon", json=body)).status_code == 404
        assert (await c.get(f"/blocked-exit/cameras/{stream}/config")).status_code == 404
        assert (await c.patch(f"/blocked-exit/cameras/{stream}/config", json={"is_active": False})).status_code == 404
        assert (await c.delete(f"/blocked-exit/cameras/{stream}/config")).status_code == 404
        assert (await c.get(f"/blocked-exit/analytics/timeline/{stream}")).status_code == 404
    set_polygon.assert_not_awaited()


async def test_door_polygon_needs_three_xy_points(client, mocker):
    mocker.patch.object(mod.blocked_exit_service, "get_camera", mocker.AsyncMock(return_value={"name": "cam"}))
    stream = uuid4()
    async with client as c:
        r1 = await c.put(f"/blocked-exit/cameras/{stream}/door-polygon", json={"door_polygon": [[0, 0], [1, 1]]})
        r2 = await c.put(f"/blocked-exit/cameras/{stream}/door-polygon", json={"door_polygon": [[0, 0, 1], [1, 1], [2, 2]]})
    assert r1.status_code == 422
    assert r2.status_code == 422


async def test_repolygon_does_not_reset_strategy(client, mocker):
    mocker.patch.object(mod.blocked_exit_service, "get_camera", mocker.AsyncMock(return_value={"name": "cam"}))
    stream = uuid4()
    saved = {
        "config_id": uuid4(), "stream_id": stream, "door_polygon": [[0, 0], [10, 0], [10, 10]],
        "detector_strategy": "door_centric", "min_accessibility_pct": 50, "debounce_seconds": 5, "is_active": True,
    }
    set_polygon = mocker.patch.object(mod.blocked_exit_service, "set_door_polygon", mocker.AsyncMock(return_value=saved))
    mocker.patch.object(mod, "_hot_reload")
    async with client as c:
        r = await c.put(f"/blocked-exit/cameras/{stream}/door-polygon", json={"door_polygon": saved["door_polygon"]})
    assert r.status_code == 200
    assert set_polygon.await_args.kwargs["detector_strategy"] is None


# ─── Cameras page ───────────────────────────────────────────────

SECRET_SOURCE = "rtsp://admin:hunter2@10.0.0.5:554/exit"
HEALTH = {"live_status": "stopped", "healthy": False}


def _camera_row(stream_id):
    return {
        "stream_id": stream_id, "camera_name": "Exit A", "is_streaming": False,
        "is_blocked_exit_camera": True, "locked_by_server": None, "current_state": "clear",
    }


async def test_camera_endpoints_404_outside_workspace(client, mocker):
    mocker.patch.object(mod.blocked_exit_service, "get_camera_source", mocker.AsyncMock(return_value=None))
    probe = mocker.patch.object(mod.rtsp_probe, "probe", mocker.AsyncMock())
    grab = mocker.patch.object(mod.camera_runtime, "live_or_grab_jpeg", mocker.AsyncMock())
    stream = uuid4()
    async with client as c:
        assert (await c.get(f"/blocked-exit/cameras/{stream}")).status_code == 404
        assert (await c.get(f"/blocked-exit/cameras/{stream}/snapshot")).status_code == 404
        assert (await c.post(f"/blocked-exit/cameras/{stream}/probe")).status_code == 404
    probe.assert_not_awaited()
    grab.assert_not_awaited()


async def test_camera_list_never_leaks_source_or_credentials(client, mocker):
    stream = uuid4()
    mocker.patch.object(mod.blocked_exit_service, "list_cameras", mocker.AsyncMock(return_value=[_camera_row(stream)]))
    mocker.patch.object(mod.blocked_exit_service, "get_camera_sources",
                        mocker.AsyncMock(return_value=[{"stream_id": stream, "name": "Exit A", "path": SECRET_SOURCE}]))
    mocker.patch.object(mod.camera_runtime, "camera_health", mocker.AsyncMock(return_value=HEALTH))
    async with client as c:
        r = await c.get("/blocked-exit/cameras")
    assert r.status_code == 200
    body = r.text
    assert "hunter2" not in body and "admin:" not in body and '"path"' not in body
    assert r.json()[0]["source_host"] == "rtsp://***@10.0.0.5:554/exit"


async def test_probe_all_covers_every_camera(client, mocker):
    cams = [{"stream_id": uuid4(), "name": f"Exit {i}", "path": f"rtsp://cam{i}/x"} for i in range(3)]
    mocker.patch.object(mod.blocked_exit_service, "get_camera_sources", mocker.AsyncMock(return_value=cams))

    async def fake_probe(source):
        ok = source != "rtsp://cam1/x"
        return {"reachable": ok, "latency_ms": 12.0 if ok else None, "width": 640 if ok else None,
                "height": 360 if ok else None, "fps": 25.0 if ok else None, "error": None if ok else "timed out"}

    mocker.patch.object(mod.rtsp_probe, "probe", side_effect=fake_probe)
    async with client as c:
        r = await c.post("/blocked-exit/cameras/probe-all")
    assert r.status_code == 200
    by_name = {row["camera_name"]: row for row in r.json()}
    assert set(by_name) == {"Exit 0", "Exit 1", "Exit 2"}
    assert by_name["Exit 0"]["reachable"] and by_name["Exit 0"]["width"] == 640
    assert not by_name["Exit 1"]["reachable"] and by_name["Exit 1"]["error"] == "timed out"


async def test_snapshot_reports_frame_source_and_502_when_unreachable(client, mocker):
    stream = uuid4()
    mocker.patch.object(mod.blocked_exit_service, "get_camera_source",
                        mocker.AsyncMock(return_value={"stream_id": stream, "name": "Exit A", "path": SECRET_SOURCE}))
    grab = mocker.patch.object(
        mod.camera_runtime, "live_or_grab_jpeg",
        mocker.AsyncMock(return_value=(b"\xff\xd8jpeg", {"frame_source": "grab", "width": 640, "height": 360, "error": None})),
    )
    async with client as c:
        ok = await c.get(f"/blocked-exit/cameras/{stream}/snapshot")
        grab.return_value = (None, {"frame_source": "grab", "width": None, "height": None,
                                    "error": "could not open rtsp://***@10.0.0.5:554/exit"})
        bad = await c.get(f"/blocked-exit/cameras/{stream}/snapshot")
    assert ok.status_code == 200
    assert ok.headers["content-type"] == "image/jpeg"
    assert ok.headers["x-frame-source"] == "grab"
    assert ok.headers["x-frame-width"] == "640"
    assert bad.status_code == 502
    assert "hunter2" not in bad.text


async def test_permissions_shape(client, mocker):
    async def access(db, user_id, ws, required_role=None, system_role=None):
        if required_role == "admin":
            raise mod.HTTPException(status_code=403, detail="no")

    mocker.patch.object(mod, "check_workspace_access", side_effect=access)
    async with client as c:
        r = await c.get("/blocked-exit/permissions")
    assert r.json() == {"can_resolve": True, "can_manage": False}
