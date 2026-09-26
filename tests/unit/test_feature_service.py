"""Unit tests for superadmin feature access control (app/services/feature_service.py)."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, Depends, HTTPException
from httpx import ASGITransport, AsyncClient

from app.services import feature_service as fs
from app.services.feature_service import ALL_FEATURES, FeatureService, require_feature
from app.services.session_service import session_manager

WORKSPACE = uuid4()
USER = {"user_id": uuid4(), "username": "alice", "role": "user"}


def _service(execute_query):
    db = MagicMock()
    db.execute_query = AsyncMock(side_effect=execute_query)
    return FeatureService(db), db


async def test_superadmin_gets_every_feature_without_a_query():
    svc, db = _service(lambda *a, **k: None)
    assert await svc.effective_features({"role": "superadmin"}, None) == ALL_FEATURES
    db.execute_query.assert_not_called()


async def test_user_without_workspace_gets_nothing():
    svc, db = _service(lambda *a, **k: None)
    assert await svc.effective_features(USER, None) == frozenset()


async def test_effective_features_is_what_the_query_returns():
    svc, db = _service(lambda *a, **k: {"features": ["gender", "shoplifting"]})
    assert await svc.effective_features(USER, WORKSPACE) == {"gender", "shoplifting"}
    query = db.execute_query.call_args.args[0]
    assert "INTERSECT" in query  # user ∩ workspace


async def test_camera_features_are_cached_and_invalidated():
    svc, db = _service(lambda *a, **k: {"features": ["fire_smoke"]})
    stream = uuid4()
    assert await svc.camera_features(stream) == {"fire_smoke"}
    assert await svc.camera_features(stream) == {"fire_smoke"}
    assert db.execute_query.call_count == 1

    svc.invalidate_cameras()
    await svc.camera_features(stream)
    assert db.execute_query.call_count == 2


async def test_camera_features_fail_open_on_db_error():
    def boom(*a, **k):
        raise RuntimeError("db down")

    svc, _ = _service(boom)
    assert await svc.camera_features(uuid4()) == ALL_FEATURES


async def test_setters_invalidate_the_camera_cache():
    svc, _ = _service(lambda *a, **k: {"username": "bob", "name": "ws"})
    svc._camera_cache["x"] = (0.0, frozenset())
    assert await svc.set_user_features(uuid4(), ["gender", "gender"]) == "bob"
    assert svc._camera_cache == {}


def test_require_feature_rejects_unknown_keys():
    with pytest.raises(ValueError):
        require_feature("teleportation")


def _app(mocker, features):
    app = FastAPI()

    @app.get("/nez", dependencies=[Depends(require_feature("no_entry_zone"))])
    async def nez():
        return {"ok": True}

    app.dependency_overrides[session_manager.get_current_user_full_data_dependency] = lambda: USER
    mocker.patch.object(fs, "current_workspace_id", AsyncMock(return_value=WORKSPACE))
    mocker.patch.object(fs.feature_service, "effective_features", AsyncMock(return_value=frozenset(features)))
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_require_feature_403_when_closed(mocker):
    async with _app(mocker, {"gender"}) as c:
        r = await c.get("/nez")
    assert r.status_code == 403
    assert "no_entry_zone" in r.json()["detail"]


async def test_require_feature_passes_when_open(mocker):
    async with _app(mocker, {"no_entry_zone"}) as c:
        assert (await c.get("/nez")).status_code == 200


async def test_camera_update_reports_features_closed_on_the_workspace(mocker):
    from app.api.routes import camera_router
    from app.schemas import StreamUpdate

    mocker.patch.object(
        camera_router.db_manager, "execute_query",
        AsyncMock(return_value={"enabled_features": ["fire_smoke", "people_counting"]}),
    )
    update = StreamUpdate(
        id=str(uuid4()), detection_models=["fire_smoke", "shoplifting"], is_no_entry_zone_camera=True,
    )
    assert await camera_router._closed_features_requested(update) == ["no_entry_zone", "shoplifting"]


async def test_camera_update_without_new_features_skips_the_lookup(mocker):
    from app.api.routes import camera_router
    from app.schemas import StreamUpdate

    q = mocker.patch.object(camera_router.db_manager, "execute_query", AsyncMock())
    assert await camera_router._closed_features_requested(StreamUpdate(id=str(uuid4()), name="cam")) == []
    q.assert_not_called()
