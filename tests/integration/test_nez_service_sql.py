"""NoEntryZoneService SQL against a real, disposable PostgreSQL.

Skipped unless NEZ_TEST_DSN points at a database built from database/initdb (including
insighteye-no-entry-zone-v2.sql). Never point it at a database you care about: the test
creates and deletes its own workspace, user and camera.

    NEZ_TEST_DSN=postgresql://t:t@localhost:5432/t \
      pytest --noconftest -o addopts="" tests/integration/test_nez_service_sql.py
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

DSN = os.environ.get("NEZ_TEST_DSN")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="NEZ_TEST_DSN not set"),
]


class _PoolAdapter:
    """The slice of db_manager.execute_query the service uses, over a plain pool."""

    def __init__(self, pool):
        self.pool = pool

    async def execute_query(self, query, params=None, fetch_one=False, fetch_all=False,
                            return_rowcount=False, **_):
        async with self.pool.acquire() as conn:
            await conn.set_type_codec("uuid", encoder=str, decoder=uuid.UUID, schema="pg_catalog")
            if fetch_one:
                row = await conn.fetchrow(query, *(params or ()))
                return dict(row) if row else None
            if fetch_all:
                return [dict(r) for r in await conn.fetch(query, *(params or ()))]
            status = await conn.execute(query, *(params or ()))
            if return_rowcount:
                last = status.split()[-1]
                return int(last) if last.isdigit() else 0
            return None


@pytest.fixture
async def svc():
    from app.services.no_entry_zone_service import NoEntryZoneService

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    service = NoEntryZoneService()
    service.db = _PoolAdapter(pool)
    ws = uuid.uuid4()
    user = uuid.uuid4()
    stream = uuid.uuid4()
    async with pool.acquire() as c:
        await c.execute("INSERT INTO workspaces (workspace_id, name) VALUES ($1, $2)", ws, f"nez-test-{ws.hex[:6]}")
        await c.execute(
            "INSERT INTO users (user_id, username, email, subscription_date) VALUES ($1, $2, $3, NOW())",
            user, f"nez{user.hex[:8]}", f"nez{user.hex[:8]}@example.test",
        )
        await c.execute(
            "INSERT INTO video_stream (stream_id, workspace_id, user_id, name, path, is_no_entry_zone_camera) "
            "VALUES ($1, $2, $3, 'cam-1', 'rtsp://x/1', TRUE)",
            stream, ws, user,
        )
    try:
        yield service, ws, user, stream
    finally:
        async with pool.acquire() as c:
            await c.execute("DELETE FROM workspaces WHERE workspace_id = $1", ws)
            await c.execute("DELETE FROM users WHERE user_id = $1", user)
        await pool.close()


async def test_full_lifecycle(svc):
    service, ws, user, stream = svc

    zone = await service.create_zone(ws, {
        "stream_id": stream, "name": "vault", "polygon": [[0, 0], [100, 0], [100, 100]],
        "ref_width": 640, "ref_height": 480, "target_classes": ["person", "car"],
        "min_dwell_seconds": 2, "schedule": {"days": [0, 1], "start": "18:00", "end": "07:00"},
        "consecutive_frames": 4, "anchor": "center",
    })
    assert zone["polygon"] == [[0, 0], [100, 0], [100, 100]]
    assert zone["schedule"]["start"] == "18:00" and zone["consecutive_frames"] == 4

    updated = await service.update_zone(zone["zone_id"], ws, {"consecutive_frames": None, "is_active": False})
    assert updated["consecutive_frames"] is None and updated["is_active"] is False
    assert [z["zone_id"] for z in await service.list_zones(ws, stream_id=stream)] == [zone["zone_id"]]
    assert (await service.get_zone(zone["zone_id"], ws))["camera_name"] == "cam-1"
    assert await service.stream_in_workspace(stream, ws)
    assert not await service.stream_in_workspace(stream, uuid.uuid4())
    assert await service.is_camera_enabled(stream)
    cams = await service.list_cameras(ws)
    assert cams[0]["zone_count"] == 1 and cams[0]["active_zone_count"] == 0

    incident = uuid.uuid4()
    now = datetime.now(timezone.utc)
    ids = []
    for i in range(3):
        ids.append(await service.create_open_event(
            incident_id=incident, zone_id=zone["zone_id"], stream_id=stream, workspace_id=ws,
            camera_name="cam-1", zone_name="vault", target_class="person", track_id=i,
            confidence=0.8 + i / 100, entered_at=now, alerted_at=now + timedelta(seconds=i),
            dwell_seconds=1.5,
        ))
    other = await service.create_open_event(
        incident_id=uuid.uuid4(), zone_id=zone["zone_id"], stream_id=stream, workspace_id=ws,
        camera_name="cam-1", zone_name="vault", target_class="car", track_id=9, confidence=0.5,
        entered_at=now, alerted_at=now - timedelta(hours=2), dwell_seconds=3,
    )
    await service.close_event(ids[0], exited_at=now + timedelta(seconds=10), dwell_seconds=10)
    await service.set_evidence(ids[1], snapshot_path="s3://b/snap.jpg")
    await service.set_evidence(ids[1], clip_path="s3://b/clip.mp4")

    ev = await service.get_event(ids[1], ws)
    assert ev["snapshot_path"] == "s3://b/snap.jpg" and ev["clip_path"] == "s3://b/clip.mp4"
    assert sorted(ev["evidence_paths"]) == ["s3://b/clip.mp4", "s3://b/snap.jpg"]
    assert await service.get_event(ids[1], uuid.uuid4()) is None
    assert float((await service.get_event(ids[0], ws))["dwell_seconds"]) == 10.0

    # incidents: the representative event is the one with evidence
    incs = await service.list_incidents(ws, limit=10, offset=0)
    assert await service.count_incidents(ws) == 2
    grouped = next(i for i in incs if i["incident_id"] == incident)
    assert grouped["event_count"] == 3 and grouped["event_id"] == ids[1]
    assert grouped["status"] == "detected" and grouped["is_ongoing"] is True
    assert await service.count_incidents(ws, target_class="car") == 1
    assert await service.count_events(ws, incident_id=str(incident)) == 3
    assert len(await service.get_events(ws, limit=2, offset=0, camera_id=str(stream))) == 2

    # resolve is workspace-scoped; acknowledging never reopens resolved events
    assert not await service.resolve_event(event_id=ids[0], workspace_id=uuid.uuid4(), status="resolved")
    assert await service.resolve_event(event_id=ids[0], workspace_id=ws, status="resolved", user_id=user)
    assert await service.resolve_incident(incident_id=uuid.uuid4(), workspace_id=ws, status="resolved") is None
    assert await service.resolve_incident(
        incident_id=incident, workspace_id=ws, status="acknowledged", description="on it", user_id=user,
    ) == 2
    assert (await service.get_event(ids[0], ws))["status"] == "resolved"
    assert await service.count_incidents(ws, status="acknowledged") == 1

    overview = await service.overview(ws)
    assert overview["events_24h"] == 4 and overview["open_incidents"] == 2
    assert overview["nez_cameras"] == 1 and overview["total_zones"] == 1

    for bucket, hours in (("hour", 24), ("day", 24 * 7)):
        a = await service.analytics_summary(
            ws, since=now - timedelta(hours=hours), until=now + timedelta(minutes=1), bucket=bucket,
        )
        assert a["total_events"] == 4 and a["total_incidents"] == 2
        assert sum(b["events"] for b in a["series"]) == 4
        assert len(a["by_hour"]) == 24 and sum(h["events"] for h in a["by_hour"]) == 4
        assert {r["label"] for r in a["by_class"]} == {"person", "car"}
    a = await service.analytics_summary(
        ws, since=now - timedelta(hours=24), until=now, bucket="hour", stream_id=uuid.uuid4(),
    )
    assert a["total_events"] == 0 and len(a["series"]) >= 24

    assert (await service.delete_events(ws, camera_id=str(stream), start_date="2000-01-01"))["deleted"] == 4
    assert await service.delete_zone(zone["zone_id"], ws) == stream
    assert await service.delete_zone(zone["zone_id"], ws) is None
    assert other
