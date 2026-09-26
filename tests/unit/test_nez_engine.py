"""NoEntryZoneEngine end to end with a fake model and the real per-stream ByteTrack."""

from __future__ import annotations

import time

import numpy as np
import pytest

pytest.importorskip("ultralytics")

from app.services import no_entry_zone_inference as nez
from app.services.nez.zone_logic import NezSettings

FRAME_W, FRAME_H = 640, 480
ZONE = {
    "zone_id": "3f1c2a64-0000-4000-8000-000000000001",
    "name": "server-room",
    "polygon": [[200, 200], [400, 200], [400, 400], [200, 400]],
    "ref_width": FRAME_W,
    "ref_height": FRAME_H,
    "target_classes": ["person"],
    "min_dwell_seconds": 0,
    "consecutive_frames": 2,
    "cooldown_seconds": 30,
    "anchor": None,
    "schedule": None,
    "is_active": True,
}


class FakeModel:
    """Stands in for a ModelFactory loader; returns whatever boxes the test sets."""

    def __init__(self):
        self.boxes = []

    def predict(self, frame):
        return None

    def postprocess(self, raw, shape):
        return [dict(b) for b in self.boxes]


@pytest.fixture
def engine(monkeypatch):
    monkeypatch.setattr(nez.config, "no_entry_zone_clip_pre_seconds", 1.0, raising=False)
    monkeypatch.setattr(nez.config, "no_entry_zone_clip_post_seconds", 0.0, raising=False)
    e = nez.NoEntryZoneEngine()
    e.settings = NezSettings(consecutive_frames=2, min_inside_seconds=0, cooldown_seconds=30,
                             track_ttl_seconds=0.5, timezone="UTC")
    e.model = FakeModel()
    e.is_loaded = True
    return e


def frame():
    return np.zeros((FRAME_H, FRAME_W, 3), dtype=np.uint8)


PERSON_INSIDE = {"bbox": [280, 150, 320, 350], "confidence": 0.9, "class_id": 0}
CAR_INSIDE = {"bbox": [280, 150, 320, 350], "confidence": 0.9, "class_id": 2}


def run(engine, sid, n):
    results = []
    for _ in range(n):
        results.append(engine.process_frame(sid, frame()))
        time.sleep(0.01)
    return results


def test_entry_produces_trigger_snapshot_and_clip(engine):
    engine.set_zones("s1", [ZONE])
    engine.model.boxes = [PERSON_INSIDE]
    results = run(engine, "s1", 6)
    entries = [t for r in results for t in r["triggers"] if t["kind"] == "entry"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["zone_name"] == "server-room" and entry["target_class"] == "person"
    snaps = {k: v for r in results for k, v in r["snapshots"].items()}
    assert snaps[entry["event_uuid"]][:2] == b"\xff\xd8"  # JPEG
    clips = [c for r in results for c in r["clips"]]
    assert [c["event_uuid"] for c in clips] == [entry["event_uuid"]]
    assert len(clips[0]["frames"]) >= 2


def test_classes_outside_the_zone_filter_are_never_tracked(engine):
    engine.set_zones("s1", [ZONE])
    engine.model.boxes = [CAR_INSIDE]
    results = run(engine, "s1", 6)
    assert not any(r["triggers"] for r in results)


def test_deleting_a_zone_closes_its_open_event(engine):
    engine.set_zones("s1", [ZONE])
    engine.model.boxes = [PERSON_INSIDE]
    run(engine, "s1", 4)
    engine.set_zones("s1", [])
    closing = engine.process_frame("s1", frame())
    assert [t["kind"] for t in closing["triggers"]] == ["exit"]
    assert not engine.has_zones("s1")


def test_release_stream_closes_open_events(engine):
    engine.set_zones("s1", [ZONE])
    engine.model.boxes = [PERSON_INSIDE]
    run(engine, "s1", 4)
    final = engine.release_stream("s1")
    assert [t["kind"] for t in final["triggers"]] == ["exit"]
    assert final["released"] is True
    assert engine.process_frame("s1", frame())["triggers"] == []


def test_streams_do_not_share_tracks_or_incidents(engine):
    engine.set_zones("a", [ZONE])
    engine.set_zones("b", [dict(ZONE, zone_id="3f1c2a64-0000-4000-8000-000000000002")])
    engine.model.boxes = [PERSON_INSIDE]
    ra = [t for r in run(engine, "a", 4) for t in r["triggers"] if t["kind"] == "entry"]
    rb = [t for r in run(engine, "b", 4) for t in r["triggers"] if t["kind"] == "entry"]
    assert len(ra) == 1 and len(rb) == 1
    assert ra[0]["incident_id"] != rb[0]["incident_id"]
