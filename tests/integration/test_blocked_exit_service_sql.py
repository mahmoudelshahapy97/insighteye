"""BlockedExitService evidence/chart SQL against a real, disposable PostgreSQL.

Skipped unless BLOCKED_EXIT_TEST_DSN (or NEZ_TEST_DSN) points at a database with the
insighteye schema. Never point it at a database you care about: the test creates and
deletes its own workspace, user and camera, and applies EVIDENCE_COLUMNS_DDL.

    BLOCKED_EXIT_TEST_DSN=postgresql://t:t@localhost:5432/t \
      pytest --noconftest -o addopts="" tests/integration/test_blocked_exit_service_sql.py
"""

from __future__ import annotations

import os
import uuid

import pytest

asyncpg = pytest.importorskip("asyncpg")

DSN = os.environ.get("BLOCKED_EXIT_TEST_DSN") or os.environ.get("NEZ_TEST_DSN")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="BLOCKED_EXIT_TEST_DSN / NEZ_TEST_DSN not set"),
]


class _PoolAdapter:
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
    from app.services.blocked_exit_service import BlockedExitService, EVIDENCE_COLUMNS_DDL

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with pool.acquire() as c:
        for stmt in EVIDENCE_COLUMNS_DDL:
            await c.execute(stmt)
    service = BlockedExitService()
    service.db = _PoolAdapter(pool)
    ws, user, stream = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with pool.acquire() as c:
        await c.execute("INSERT INTO workspaces (workspace_id, name) VALUES ($1, $2)", ws, f"be-test-{ws.hex[:6]}")
        await c.execute(
            "INSERT INTO users (user_id, username, email, subscription_date) VALUES ($1, $2, $3, NOW())",
            user, f"be{user.hex[:8]}", f"be{user.hex[:8]}@example.test",
        )
        await c.execute(
            "INSERT INTO video_stream (stream_id, workspace_id, user_id, name, path, is_blocked_exit_camera, "
            "location, area, building) VALUES ($1, $2, $3, 'exit-1', 'rtsp://x/1', TRUE, 'HQ', 'North', 'B1')",
            stream, ws, user,
        )
    try:
        yield service, ws, stream
    finally:
        async with pool.acquire() as c:
            await c.execute("DELETE FROM blocked_exit_events WHERE workspace_id = $1", ws)
            await c.execute("DELETE FROM exit_zone_config WHERE workspace_id = $1", ws)
            await c.execute("DELETE FROM workspaces WHERE workspace_id = $1", ws)
            await c.execute("DELETE FROM users WHERE user_id = $1", user)
        await pool.close()


def _reading(**kw):
    base = dict(state="blocked", accessibility_pct=10.0, blocking_objects=["chair"], risk_score=70.0,
                risk_level="critical", recommended_action="clear it")
    base.update(kw)
    return base


async def test_episode_evidence_and_charts(svc):
    service, ws, stream = svc

    cfg = await service.set_door_polygon(
        stream_id=stream, workspace_id=ws, door_polygon=[[0, 0], [10, 0], [10, 10]],
        calibration_frame_w=640, calibration_frame_h=360,
    )
    assert cfg["door_polygon"] == [[0, 0], [10, 0], [10, 10]]

    event_id, created = await service.open_episode(
        stream_id=stream, workspace_id=ws, camera_name="exit-1", evidence_paths=["s3://b/s.jpg"],
        snapshot_path="s3://b/s.jpg", confidence=0.7, avg_confidence=0.65, **_reading(),
    )
    assert created
    # peak only rises; NULL readings change nothing
    await service.update_episode(event_id=event_id, confidence=0.9, avg_confidence=0.72, **_reading())
    await service.update_episode(event_id=event_id, confidence=0.5, avg_confidence=None, **_reading())
    await service.update_episode(event_id=event_id, **_reading())
    ev = await service.get_event(ws, event_id)
    assert float(ev["confidence"]) == 0.9 and float(ev["avg_confidence"]) == 0.72
    assert ev["snapshot_path"] == "s3://b/s.jpg" and ev["camera_area"] == "North"

    await service.set_evidence(event_id, clip_path="s3://b/first.mp4")
    await service.set_evidence(event_id, clip_path="s3://b/second.mp4")
    await service.set_evidence(event_id, video_path="s3://b/video.mp4")
    assert (await service.get_event(ws, event_id))["video_path"] == "s3://b/video.mp4"
    assert await service.close_open_episodes(stream, confidence=0.95, avg_confidence=0.75) == 1
    ev = await service.get_event(ws, event_id)
    assert ev["clip_path"] == "s3://b/first.mp4" and ev["evidence_paths"] == ["s3://b/s.jpg"]
    assert ev["video_path"] == "s3://b/video.mp4"
    assert float(ev["confidence"]) == 0.95 and ev["ended_at"] is not None

    # a second episode without a clip or confidence
    other, _ = await service.open_episode(stream_id=stream, workspace_id=ws, camera_name="exit-1", **_reading())
    await service.close_open_episodes(stream)

    assert await service.count_events(workspace_id=ws, has_clip=True) == 1
    assert (await service.get_events(workspace_id=ws, has_clip=True))[0]["event_id"] == event_id
    assert await service.count_events(workspace_id=ws, location="HQ, Elsewhere", area="North") == 2
    assert await service.count_events(workspace_id=ws, building="B2") == 0
    assert await service.count_events(workspace_id=ws, start_time="00:00", end_time="23:59") == 2

    [row] = await service.incidents_per_camera(ws)
    assert row["camera_name"] == "exit-1" and row["events"] == row["incidents"] == 2
    assert row["location"] == "HQ" and row["area"] == "North"

    audit = await service.confidence_audit(ws)
    assert audit["summary"]["events"] == 2 and audit["summary"]["scored_events"] == 1
    assert audit["summary"]["max_confidence"] == pytest.approx(0.95)
    assert sum(b["count"] for b in audit["histogram"]) == 1
    assert audit["histogram"][-1]["count"] == 1          # 0.95 falls in 0.9-1.0
    assert [e["event_id"] for e in audit["per_event"]] == [event_id]
    assert other
