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
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=None))
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
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
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
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
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

    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
    mocker.patch.object(
        mod.no_entry_zone_service, "create_zone",
        mocker.AsyncMock(side_effect=DBConflict(status_code=409, detail="Duplicate entry")),
    )
    async with admin as c:
        r = await c.post("/no-entry-zone/zones", json=_zone_body())
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"]


# ─────────────────────────────────────────────
# Feature gating, single-polygon editor, videos and charts
# ─────────────────────────────────────────────

async def test_zones_cannot_be_drawn_on_a_camera_without_the_feature(admin, mocker):
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=False))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock())
    mocker.patch.object(mod.no_entry_zone_service, "get_zone", mocker.AsyncMock(return_value={"stream_id": uuid4()}))
    update = mocker.patch.object(mod.no_entry_zone_service, "update_zone", mocker.AsyncMock())
    async with admin as c:
        assert (await c.post("/no-entry-zone/zones", json=_zone_body())).status_code == 409
        r = await c.patch(f"/no-entry-zone/zones/{uuid4()}", json={"name": "x"})
        assert r.status_code == 409
        r = await c.put(f"/no-entry-zone/cameras/{uuid4()}/polygon",
                        json={"polygon": SQUARE, "ref_width": 640, "ref_height": 480})
        assert r.status_code == 409
    create.assert_not_awaited()
    update.assert_not_awaited()


async def test_get_polygon_is_null_before_one_is_drawn(admin, mocker):
    stream = uuid4()
    mocker.patch.object(mod, "_camera_or_404", mocker.AsyncMock(return_value={"name": "Gate"}))
    mocker.patch.object(mod.no_entry_zone_service, "primary_zone", mocker.AsyncMock(return_value=None))
    async with admin as c:
        r = await c.get(f"/no-entry-zone/cameras/{stream}/polygon")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["stream_id"] == str(stream) and body["camera_name"] == "Gate"
    assert body["polygon"] is None and body["point_count"] == 0


async def test_put_polygon_creates_then_updates_the_primary_zone(admin, mocker):
    stream, zone_id = uuid4(), uuid4()
    saved = {"zone_id": zone_id, "stream_id": stream, "camera_name": "Gate", "polygon": SQUARE,
             "ref_width": 640, "ref_height": 480, "is_active": True}
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
    mocker.patch.object(mod.no_entry_zone_service, "primary_zone", mocker.AsyncMock(side_effect=[None, saved]))
    create = mocker.patch.object(mod.no_entry_zone_service, "create_zone", mocker.AsyncMock(return_value=saved))
    update = mocker.patch.object(mod.no_entry_zone_service, "update_zone", mocker.AsyncMock(return_value=saved))
    body = {"polygon": SQUARE, "ref_width": 640, "ref_height": 480}
    async with admin as c:
        first = await c.put(f"/no-entry-zone/cameras/{stream}/polygon", json=body)
        second = await c.put(f"/no-entry-zone/cameras/{stream}/polygon", json=body)
    assert first.status_code == second.status_code == 200, first.text
    assert first.json()["point_count"] == 4 and first.json()["id"] == str(zone_id)
    data = create.await_args.args[1]
    assert data["stream_id"] == stream and data["name"] == "Default" and data["polygon"] == SQUARE
    assert update.await_args.args[0] == zone_id
    assert mod._refresh_engine.await_count == 2


@pytest.mark.parametrize("polygon", [
    [[0, 0], [10, 10]],                             # too few points
    [[0, 0], [100, 100], [100, 0], [0, 100]],       # edges cross
    [[0, 0], [5000, 0], [5000, 50]],                # outside the frame
])
async def test_put_polygon_validates(admin, mocker, polygon):
    mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
    async with admin as c:
        r = await c.put(f"/no-entry-zone/cameras/{uuid4()}/polygon",
                        json={"polygon": polygon, "ref_width": 640, "ref_height": 480})
    assert r.status_code == 422


async def test_viewer_cannot_save_a_polygon(viewer, mocker):
    flag = mocker.patch.object(mod.no_entry_zone_service, "camera_flag", mocker.AsyncMock(return_value=True))
    async with viewer as c:
        r = await c.put(f"/no-entry-zone/cameras/{uuid4()}/polygon",
                        json={"polygon": SQUARE, "ref_width": 640, "ref_height": 480})
    assert r.status_code == 403
    flag.assert_not_awaited()


async def test_videos_only_ask_for_clips_and_sign_them(admin, mocker):
    from app.services import s3_service as s3mod
    get = mocker.patch.object(mod.no_entry_zone_service, "get_events", mocker.AsyncMock(return_value=[{
        "event_id": 5, "event_timestamp": "2026-09-01T10:00:00Z", "status": "detected",
        "clip_path": "s3://b/c.mp4", "snapshot_path": None, "camera_name": "Gate",
    }]))
    count = mocker.patch.object(mod.no_entry_zone_service, "count_events", mocker.AsyncMock(return_value=1))
    mocker.patch.object(s3mod.s3_service, "get_presigned_url", mocker.AsyncMock(return_value="https://signed"))
    async with admin as c:
        r = await c.get("/no-entry-zone/videos", params={
            "building": "A,B", "start_date": "2026-09-01", "start_time": "08:00", "page": 2, "limit": 6,
        })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1 and body["offset"] == 6
    assert body["items"][0]["clip_url"] == "https://signed" and body["items"][0]["snapshot_url"] is None
    kw = get.await_args.kwargs
    assert kw["has_clip"] is True and kw["building"] == "A,B" and kw["start_time"] == "08:00"
    assert count.await_args.kwargs["has_clip"] is True


async def test_chart_endpoints_pass_filters(admin, mocker):
    per_cam = mocker.patch.object(mod.no_entry_zone_service, "incidents_per_camera", mocker.AsyncMock(return_value=[
        {"stream_id": uuid4(), "camera_name": "Gate", "events": 4, "incidents": 2, "resolved": 1},
    ]))
    audit = mocker.patch.object(mod.no_entry_zone_service, "confidence_audit", mocker.AsyncMock(return_value={
        "summary": {"events": 0, "threshold": 0.4, "low_confidence_cutoff": 0.5},
        "per_camera": [], "histogram": [], "per_event": [],
    }))
    async with admin as c:
        r1 = await c.get("/no-entry-zone/analytics/incidents-per-camera", params={"area": "North"})
        r2 = await c.get("/no-entry-zone/analytics/confidence-audit", params={"end_date": "2026-09-30"})
    assert r1.status_code == 200 and r1.json()[0]["incidents"] == 2
    assert r2.status_code == 200, r2.text
    assert per_cam.await_args.kwargs["area"] == "North"
    assert audit.await_args.kwargs["end_date"] == "2026-09-30"


async def test_extra_chart_endpoints(admin, mocker):
    trend = mocker.patch.object(mod.no_entry_zone_service, "trend", mocker.AsyncMock(return_value={
        "bucket": "day", "points": [{"bucket_start": "2026-09-30T00:00:00", "incidents": 2, "resolved": 1}],
    }))
    heat = mocker.patch.object(mod.no_entry_zone_service, "heatmap", mocker.AsyncMock(
        return_value=[{"weekday": 3, "hour": 7, "incidents": 2}]))
    resp = mocker.patch.object(mod.no_entry_zone_service, "response_times", mocker.AsyncMock(return_value={
        "summary": {"incidents": 2, "open": 2}, "per_camera": [],
    }))
    brk = mocker.patch.object(mod.no_entry_zone_service, "breakdown", mocker.AsyncMock(return_value={
        "per_zone": [{"zone_name": "Default", "incidents": 2, "events": 411, "avg_dwell_s": 10.5}],
        "by_target": [{"target_class": "person", "events": 411, "incidents": 2}],
        "dwell_histogram": [{"min": 0, "max": 5, "count": 68}, {"min": 120, "max": None, "count": 0}],
    }))
    async with admin as c:
        r1 = await c.get("/no-entry-zone/analytics/trend", params={"bucket": "day", "zone": "Zone 1"})
        r2 = await c.get("/no-entry-zone/analytics/heatmap", params={"location": "Cairo,Giza"})
        r3 = await c.get("/no-entry-zone/analytics/response-times", params={"start_time": "08:00"})
        r4 = await c.get("/no-entry-zone/analytics/breakdown", params={"area": "North"})
        bad = await c.get("/no-entry-zone/analytics/trend", params={"bucket": "week"})
    assert [r.status_code for r in (r1, r2, r3, r4)] == [200] * 4, r4.text
    assert r1.json()["points"][0]["incidents"] == 2
    assert trend.await_args.kwargs["bucket"] == "day" and trend.await_args.kwargs["zone"] == "Zone 1"
    assert heat.await_args.kwargs["location"] == "Cairo,Giza"
    assert resp.await_args.kwargs["start_time"] == "08:00"
    assert r3.json()["summary"]["ack_median_s"] is None
    assert brk.await_args.kwargs["area"] == "North"
    assert r4.json()["dwell_histogram"][-1]["max"] is None
    assert bad.status_code == 422


async def test_incidents_sign_clip_into_video_path(admin, mocker):
    from app.services import s3_service as s3mod
    base = {"started_at": "2026-09-30T04:25:40Z", "last_seen_at": "2026-09-30T04:42:40Z",
            "is_ongoing": False, "event_count": 3, "status": "detected", "snapshot_path": None}
    mocker.patch.object(mod.no_entry_zone_service, "list_incidents", mocker.AsyncMock(return_value=[
        {**base, "incident_id": str(uuid4()), "event_id": 1, "clip_path": "s3://b/clips/1.mp4"},
        {**base, "incident_id": str(uuid4()), "event_id": 2, "clip_path": None},
    ]))
    mocker.patch.object(mod.no_entry_zone_service, "count_incidents", mocker.AsyncMock(return_value=2))
    sign = mocker.patch.object(s3mod.s3_service, "get_presigned_url", mocker.AsyncMock(return_value="https://signed"))
    async with admin as c:
        r = await c.get("/no-entry-zone/incidents")
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert items[0]["video_path"] == "https://signed" and items[0]["clip_path"] == "s3://b/clips/1.mp4"
    assert items[1]["video_path"] is None
    sign.assert_awaited_once()
