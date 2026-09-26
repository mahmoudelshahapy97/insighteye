"""API tests for the no-entry-zone router: workspace scoping, roles and validation.

Runs the router on a minimal app with auth, permissions and the service mocked, so it
does not need a login or the rest of the application.
"""

from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from app.api.routes import no_entry_zone_router as mod
from app.services.session_service import session_manager
from app.services.feature_service import ALL_FEATURES, feature_service

WORKSPACE = uuid4()
USER = {"user_id": uuid4(), "username": "alice", "role": "user"}
SQUARE = [[10, 10], [110, 10], [110, 110], [10, 110]]


def _access(role_of_user: str):
    levels = {"owner": 3, "admin": 2, "member": 1}

    async def check(db, user_id, workspace_id, required_role=None, system_role=None):
        if required_role and levels.get(role_of_user, 0) < levels[required_role]:
            raise HTTPException(status_code=403, detail="nope")
        return {"role": role_of_user}

    return check


def _client(mocker, role: str):
    app = FastAPI()
    app.include_router(mod.router)
    app.dependency_overrides[session_manager.get_current_user_full_data_dependency] = lambda: USER
    mocker.patch.object(mod, "get_workspace_id_for_user", mocker.AsyncMock(return_value=WORKSPACE))
    # Feature access is covered in test_feature_service.py; open everything here.
    mocker.patch("app.services.feature_service.current_workspace_id", mocker.AsyncMock(return_value=WORKSPACE))
    mocker.patch.object(feature_service, "effective_features", mocker.AsyncMock(return_value=ALL_FEATURES))
    mocker.patch.object(mod, "check_workspace_access", side_effect=_access(role))
    mocker.patch.object(mod, "_refresh_engine", mocker.AsyncMock())
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.fixture
def admin(mocker):
    return _client(mocker, "admin")


@pytest.fixture
def viewer(mocker):
    return _client(mocker, "viewer")


def _zone_body(**kw):
    body = {"stream_id": str(uuid4()), "name": "vault", "polygon": SQUARE, "ref_width": 640, "ref_height": 480}
    body.update(kw)
    return body


async def test_resolve_is_scoped_to_the_callers_workspace(admin, mocker):
    resolve = mocker.patch.object(mod.no_entry_zone_service, "resolve_event", mocker.AsyncMock(return_value=False))
    async with admin as c:
        r = await c.post("/no-entry-zone/events/42/resolve", json={"status": "resolved"})
    assert r.status_code == 404
    kw = resolve.await_args.kwargs
    assert kw["workspace_id"] == WORKSPACE and kw["event_id"] == 42 and kw["user_id"] == USER["user_id"]


async def test_resolve_rejects_unknown_status(admin, mocker):
    resolve = mocker.patch.object(mod.no_entry_zone_service, "resolve_event", mocker.AsyncMock())
    async with admin as c:
        r = await c.post("/no-entry-zone/events/42/resolve", json={"status": "detected"})
    assert r.status_code == 422
    resolve.assert_not_awaited()


async def test_resolve_incident_404_and_count(admin, mocker):
    svc = mocker.patch.object(mod.no_entry_zone_service, "resolve_incident", mocker.AsyncMock(side_effect=[None, 3]))
    inc = uuid4()
    async with admin as c:
        assert (await c.post(f"/no-entry-zone/incidents/{inc}/resolve", json={"status": "resolved"})).status_code == 404
        r = await c.post(f"/no-entry-zone/incidents/{inc}/resolve", json={"status": "acknowledged"})
    assert r.status_code == 200 and r.json()["updated_events"] == 3
    assert svc.await_args.kwargs["workspace_id"] == WORKSPACE


async def test_viewer_can_read_but_not_write(viewer, mocker):
    mocker.patch.object(mod.no_entry_zone_service, "list_zones", mocker.AsyncMock(return_value=[]))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock())
    resolve = mocker.patch.object(mod.no_entry_zone_service, "resolve_event", mocker.AsyncMock())
    async with viewer as c:
        assert (await c.get("/no-entry-zone/zones")).status_code == 200
        assert (await c.post("/no-entry-zone/zones", json=_zone_body())).status_code == 403
        assert (await c.post("/no-entry-zone/events/1/resolve", json={"status": "resolved"})).status_code == 403
        assert (await c.delete(f"/no-entry-zone/zones/{uuid4()}")).status_code == 403
        perms = (await c.get("/no-entry-zone/permissions")).json()
    assert perms == {"can_resolve": False, "can_manage_zones": False}
    create.assert_not_awaited()
    resolve.assert_not_awaited()


async def test_zone_create_rejects_camera_from_another_workspace(admin, mocker):
    mocker.patch.object(mod.no_entry_zone_service, "stream_in_workspace", mocker.AsyncMock(return_value=False))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock())
    async with admin as c:
        r = await c.post("/no-entry-zone/zones", json=_zone_body())
    assert r.status_code == 404
    create.assert_not_awaited()


@pytest.mark.parametrize("polygon,extra", [
    ([[0, 0], [10, 10]], {}),                                   # too few points
    ([[0, 0], [100, 100], [100, 0], [0, 100]], {}),             # bow-tie, edges cross
    ([[0, 0], [10, 0], [10, 0]], {}),                           # duplicate point
    ([[0, 0], [5000, 0], [5000, 50]], {}),                      # outside the reference frame
    (SQUARE, {"target_classes": ["unicorn"]}),                  # unknown class
    (SQUARE, {"schedule": {"days": [7], "start": "09:00", "end": "17:00"}}),
    (SQUARE, {"anchor": "top_left"}),
])
async def test_zone_validation(admin, mocker, polygon, extra):
    mocker.patch.object(mod.no_entry_zone_service, "stream_in_workspace", mocker.AsyncMock(return_value=True))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock())
    async with admin as c:
        r = await c.post("/no-entry-zone/zones", json=_zone_body(polygon=polygon, **extra))
    assert r.status_code == 422
    create.assert_not_awaited()


async def test_zone_create_refreshes_the_engine(admin, mocker):
    stream = uuid4()
    saved = {
        "zone_id": uuid4(), "stream_id": stream, "name": "vault", "polygon": SQUARE,
        "ref_width": 640, "ref_height": 480, "target_classes": ["car", "person"],
        "min_dwell_seconds": 1.0, "schedule": None, "is_active": True,
    }
    mocker.patch.object(mod.no_entry_zone_service, "stream_in_workspace", mocker.AsyncMock(return_value=True))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock(return_value=saved))
    async with admin as c:
        r = await c.post("/no-entry-zone/zones", json=_zone_body(
            stream_id=str(stream), target_classes=["person", "car"],
            schedule={"days": [0, 1], "start": "18:00", "end": "07:00"},
        ))
    assert r.status_code == 201, r.text
    data = create.await_args.args[1]
    assert data["target_classes"] == ["car", "person"]
    assert data["schedule"] == {"days": [0, 1], "start": "18:00", "end": "07:00"}
    mod._refresh_engine.assert_awaited_once_with(stream)


async def test_polygon_patch_needs_its_reference_size(admin, mocker):
    update = mocker.patch.object(mod.no_entry_zone_service, "update_zone", mocker.AsyncMock())
    async with admin as c:
        r = await c.patch(f"/no-entry-zone/zones/{uuid4()}", json={"polygon": SQUARE})
    assert r.status_code == 422
    update.assert_not_awaited()


async def test_delete_zone_refreshes_the_cameras_engine_from_the_row(admin, mocker):
    stream = uuid4()
    mocker.patch.object(mod.no_entry_zone_service, "delete_zone", mocker.AsyncMock(side_effect=[stream, None]))
    async with admin as c:
        assert (await c.delete(f"/no-entry-zone/zones/{uuid4()}")).status_code == 200
        assert (await c.delete(f"/no-entry-zone/zones/{uuid4()}")).status_code == 404
    mod._refresh_engine.assert_awaited_once_with(stream)


async def test_evidence_urls_are_presigned(admin, mocker):
    mocker.patch.object(mod.no_entry_zone_service, "get_event", mocker.AsyncMock(
        return_value={"snapshot_path": "s3://b/s.jpg", "clip_path": None}))
    from app.services import s3_service as s3mod
    sign = mocker.patch.object(s3mod.s3_service, "get_presigned_url", mocker.AsyncMock(return_value="https://signed"))
    async with admin as c:
        r = await c.get("/no-entry-zone/events/7/evidence")
    assert r.json() == {"snapshot_url": "https://signed", "clip_url": None, "expires_in": mod.EVIDENCE_URL_TTL}
    sign.assert_awaited_once()


async def test_evidence_404_outside_workspace(admin, mocker):
    mocker.patch.object(mod.no_entry_zone_service, "get_event", mocker.AsyncMock(return_value=None))
    async with admin as c:
        assert (await c.get("/no-entry-zone/events/7/evidence")).status_code == 404


async def test_analytics_picks_bucket_from_range(admin, mocker):
    summary = mocker.patch.object(mod.no_entry_zone_service, "analytics_summary", mocker.AsyncMock(return_value={
        "bucket": "day", "timezone": "UTC", "since": "2026-01-01T00:00:00Z", "until": "2026-01-08T00:00:00Z",
        "series": [], "by_zone": [], "by_camera": [], "by_class": [], "by_hour": [],
    }))
    async with admin as c:
        r = await c.get("/no-entry-zone/analytics/summary", params={"range_hours": 168})
    assert r.status_code == 200, r.text
    assert summary.await_args.kwargs["bucket"] == "day"


async def test_duplicate_zone_name_is_a_409_not_a_500(admin, mocker):
    from fastapi import HTTPException as DBConflict

    mocker.patch.object(mod.no_entry_zone_service, "stream_in_workspace", mocker.AsyncMock(return_value=True))
    mocker.patch.object(
        mod.no_entry_zone_service, "create_zone",
        mocker.AsyncMock(side_effect=DBConflict(status_code=409, detail="Duplicate entry")),
    )
    async with admin as c:
        r = await c.post("/no-entry-zone/zones", json=_zone_body())
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"]
