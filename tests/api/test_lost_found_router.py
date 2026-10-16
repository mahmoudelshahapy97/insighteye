"""API tests for the lost & found router's workspace scoping and validation.

Runs the router on a minimal app with auth and the service mocked, so it does
not need a login or the rest of the application.
"""

from uuid import uuid4

import cv2
import numpy as np
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.routes import lost_found_router as mod
from app.services.feature_service import ALL_FEATURES, feature_service
from app.services.session_service import session_manager

WORKSPACE = uuid4()
USER = {"user_id": uuid4(), "username": "alice", "role": "user"}


@pytest.fixture
def client(mocker):
    app = FastAPI()
    app.include_router(mod.router)
    app.dependency_overrides[session_manager.get_current_user_full_data_dependency] = lambda: USER
    mocker.patch.object(mod, "get_workspace_id_for_user", mocker.AsyncMock(return_value=WORKSPACE))
    mocker.patch("app.services.feature_service.current_workspace_id", mocker.AsyncMock(return_value=WORKSPACE))
    mocker.patch.object(feature_service, "effective_features", mocker.AsyncMock(return_value=ALL_FEATURES))
    mocker.patch.object(mod, "_sign", mocker.AsyncMock(side_effect=lambda p: f"https://signed/{p}" if p else None))
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_lost_found_is_a_feature_key():
    assert "lost_found" in ALL_FEATURES


async def test_events_follow_the_incident_videos_tab_contract(client, mocker):
    event_id = uuid4()
    row = {"event_id": event_id, "status": "open", "class_name": "bag", "target_class": "bag",
           "snapshot_path": "s3://b/f.jpg", "video_path": None, "crop_path": "s3://b/c.jpg",
           "unattended_seconds": 31.6}
    get = mocker.patch.object(mod.lost_found_service, "get_events", mocker.AsyncMock(return_value=[row]))
    count = mocker.patch.object(mod.lost_found_service, "count_events", mocker.AsyncMock(return_value=11))
    camera = uuid4()
    async with client as c:
        r = await c.get("/lost-found/events", params={
            "page": 2, "limit": 10, "status_filter": "open", "target_class": "bag", "camera_id": str(camera),
            "start_date": "2026-10-01",
        })
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 11 and body["page"] == 2
    item = body["items"][0]
    assert item["event_id"] == str(event_id)
    assert item["snapshot_path"] == "https://signed/s3://b/f.jpg" and item["video_path"] is None
    assert item["dwell_seconds"] == 32
    kw = get.await_args.kwargs
    assert get.await_args.args == (WORKSPACE,)
    assert kw["offset"] == 10 and kw["status"] == "open" and kw["class_name"] == "bag"
    assert kw["camera_id"] == str(camera) and kw["start_date"] == "2026-10-01"
    assert count.await_args.kwargs["status"] == "open"


async def test_events_reject_unknown_status(client, mocker):
    get = mocker.patch.object(mod.lost_found_service, "get_events", mocker.AsyncMock())
    async with client as c:
        r = await c.get("/lost-found/events", params={"status_filter": "detected"})
    assert r.status_code == 422
    get.assert_not_awaited()


async def test_resolve_passes_callers_workspace_and_404s_when_not_found(client, mocker):
    resolve = mocker.patch.object(mod.lost_found_service, "resolve_event", mocker.AsyncMock(return_value=False))
    event_id = uuid4()
    async with client as c:
        r = await c.post(f"/lost-found/events/{event_id}/resolve",
                         json={"status": "false_positive", "action_taken": "false_positive"})
    assert r.status_code == 404
    kw = resolve.await_args.kwargs
    assert kw["workspace_id"] == WORKSPACE and kw["event_id"] == event_id
    assert kw["status"] == "false_positive" and kw["user_id"] == USER["user_id"]


async def test_resolve_rejects_unknown_status(client, mocker):
    resolve = mocker.patch.object(mod.lost_found_service, "resolve_event", mocker.AsyncMock())
    async with client as c:
        r = await c.post(f"/lost-found/events/{uuid4()}/resolve", json={"status": "deleted"})
    assert r.status_code == 422
    resolve.assert_not_awaited()


ROI = {"polygon": [[0, 0], [100, 0], [100, 100]], "ref_width": 640, "ref_height": 360}


@pytest.mark.parametrize("camera, status", [
    (None, 404),                                            # not in the workspace
    ({"name": "cam", "is_lost_found_camera": False}, 409),  # Lost & Found not enabled
])
async def test_roi_needs_a_workspace_camera_with_lost_found_enabled(client, mocker, camera, status):
    mocker.patch.object(mod, "_require_role", mocker.AsyncMock())
    mocker.patch.object(mod.lost_found_service, "get_camera", mocker.AsyncMock(return_value=camera))
    upsert = mocker.patch.object(mod.lost_found_service, "upsert_config", mocker.AsyncMock())
    async with client as c:
        assert (await c.put(f"/lost-found/cameras/{uuid4()}/polygon", json=ROI)).status_code == status
    upsert.assert_not_awaited()


async def test_roi_save_keeps_settings_and_hot_reloads(client, mocker):
    mocker.patch.object(mod, "_require_role", mocker.AsyncMock())
    mocker.patch.object(mod.lost_found_service, "get_camera",
                        mocker.AsyncMock(return_value={"name": "cam", "is_lost_found_camera": True}))
    stream = uuid4()
    saved = {"stream_id": stream, "roi": [[0, 0], [100, 0], [100, 100]], "ref_width": 640, "ref_height": 360,
             "settings": {"abandon_seconds": 60}, "is_active": True}
    upsert = mocker.patch.object(mod.lost_found_service, "upsert_config", mocker.AsyncMock(return_value=saved))
    reload = mocker.patch.object(mod, "_hot_reload")
    async with client as c:
        r = await c.put(f"/lost-found/cameras/{stream}/polygon",
                        json={"polygon": saved["roi"], "ref_width": 640, "ref_height": 360})
    assert r.status_code == 200
    assert r.json()["point_count"] == 3
    kw = upsert.await_args.kwargs
    assert kw["replace_roi"] is True and kw["replace_settings"] is False and kw["workspace_id"] == WORKSPACE
    reload.assert_called_once_with(stream, saved)


async def test_settings_reject_unknown_and_negative_rules(client, mocker):
    mocker.patch.object(mod, "_require_role", mocker.AsyncMock())
    upsert = mocker.patch.object(mod.lost_found_service, "upsert_config", mocker.AsyncMock())
    async with client as c:
        r1 = await c.put(f"/lost-found/cameras/{uuid4()}/settings", json={"settings": {"nope": 1}})
        r2 = await c.put(f"/lost-found/cameras/{uuid4()}/settings", json={"settings": {"abandon_seconds": -1}})
    assert r1.status_code == 422 and r2.status_code == 422
    upsert.assert_not_awaited()


async def test_lost_report_needs_a_photo_or_a_description(client, mocker):
    create = mocker.patch.object(mod.lost_found_service, "create_lost_item", mocker.AsyncMock())
    async with client as c:
        r = await c.post("/lost-found/lost-items", data={"description": "   "})
    assert r.status_code == 422
    create.assert_not_awaited()


async def test_lost_report_with_photo_is_decoded_and_scoped(client, mocker):
    lost_id = uuid4()
    create = mocker.patch.object(mod.lost_found_service, "create_lost_item", mocker.AsyncMock(
        return_value={"lost_id": lost_id, "photo_path": "s3://b/p.jpg", "status": "open"}))
    ok, buf = cv2.imencode(".jpg", np.full((32, 32, 3), 200, dtype=np.uint8))
    async with client as c:
        r = await c.post("/lost-found/lost-items",
                         files={"photo": ("bag.jpg", buf.tobytes(), "image/jpeg")},
                         data={"description": "black bag", "class_name": "bag"})
    assert r.status_code == 201
    assert r.json()["photo_path"] == "https://signed/s3://b/p.jpg"
    args, kw = create.await_args
    assert args == (WORKSPACE,) and kw["image"].shape == (32, 32, 3)
    assert kw["description"] == "black bag" and kw["reported_by"] == USER["user_id"]


async def test_lost_report_rejects_other_file_types(client, mocker):
    create = mocker.patch.object(mod.lost_found_service, "create_lost_item", mocker.AsyncMock())
    async with client as c:
        r = await c.post("/lost-found/lost-items", files={"photo": ("x.pdf", b"%PDF", "application/pdf")})
    assert r.status_code == 415
    create.assert_not_awaited()


async def test_matches_404_for_a_report_outside_the_workspace(client, mocker):
    find = mocker.patch.object(mod.lost_found_service, "find_matches", mocker.AsyncMock(return_value=None))
    lost_id = uuid4()
    async with client as c:
        r = await c.get(f"/lost-found/lost-items/{lost_id}/matches")
    assert r.status_code == 404
    assert find.await_args.args == (WORKSPACE, lost_id)


async def test_confirm_match_is_scoped_and_records_who_confirmed(client, mocker):
    confirm = mocker.patch.object(mod.lost_found_service, "confirm_match", mocker.AsyncMock(return_value=None))
    lost_id, found_id = uuid4(), uuid4()
    async with client as c:
        r = await c.post("/lost-found/matches/confirm", json={"lost_id": str(lost_id), "found_id": str(found_id)})
    assert r.status_code == 404
    args, kw = confirm.await_args
    assert args == (WORKSPACE, lost_id, found_id, 1.0) and kw["user_id"] == USER["user_id"]


async def test_analytics_summary_reports_bad_timezones_as_422(client, mocker):
    mocker.patch.object(mod.lost_found_service, "summary",
                        mocker.AsyncMock(side_effect=ValueError("Unknown timezone: Mars/Base")))
    async with client as c:
        r = await c.get("/lost-found/analytics/summary", params={"tz": "Mars/Base"})
    assert r.status_code == 422
