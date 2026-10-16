"""LostFoundEngine: the shared-model wrapper around per-stream ByteTrack, the abandonment
state machine and the JPEG clip buffer. The model is faked; tracking and the state
machine are the real ones."""

from types import SimpleNamespace

import numpy as np
import pytest

from app.services import lost_found_inference as mod
from app.services.lost_found_inference import LostFoundEngine, agnostic_nms, scale_roi

PERSON, BACKPACK, CHAIR = 0, 24, 56
BAG_BOX = [100, 300, 150, 350]
NEAR_PERSON = [160, 270, 200, 350]   # feet 60 px from the bag, 80 px tall
FAR_PERSON = [560, 270, 600, 350]
FRAME = np.full((480, 640, 3), 90, dtype=np.uint8)
FRAME[300:350, 100:150] = (40, 40, 160)  # a red-ish bag, so the crop has a colour


def det(box, cls, conf=0.9):
    return {"bbox": list(box), "class_id": cls, "confidence": conf}


class FakeModel:
    """ModelFactory loader stand-in: postprocess returns whatever the test queued."""

    def __init__(self):
        self.next = []
        self.calls = 0

    def predict(self, frame):
        self.calls += 1
        return "raw"

    def postprocess(self, raw, shape):
        return list(self.next)


@pytest.fixture
def clock(mocker):
    now = [1000.0]
    mocker.patch.object(mod, "time", SimpleNamespace(time=lambda: now[0]))
    return now


@pytest.fixture
def engine(mocker):
    mocker.patch.object(mod.config, "lost_found_clip_pre_seconds", 5.0)
    mocker.patch.object(mod.config, "lost_found_clip_post_seconds", 1.0)
    e = LostFoundEngine()
    e.model = FakeModel()
    e.is_loaded = True
    return e


FAST_RULES = {
    "static_window_seconds": 1.0,
    "abandon_seconds": 2.0,
    "stand_in_seconds": 3.0,
    "event_cooldown_seconds": 300.0,
}


def run(engine, clock, detections, seconds, dt=0.5, stream="s1"):
    out = []
    for _ in range(int(seconds / dt)):
        engine.model.next = detections
        out.append(engine.process_frame(stream, FRAME))
        clock[0] += dt
    return out


def test_agnostic_nms_keeps_one_box_per_object_across_classes():
    raw = [det([0, 0, 100, 100], BACKPACK, 0.4), det([2, 2, 100, 100], CHAIR, 0.8), det([300, 300, 350, 350], BACKPACK, 0.3)]
    kept = agnostic_nms(raw, 0.7)
    assert [d["class_id"] for d in kept] == [CHAIR, BACKPACK]


def test_scale_roi_maps_the_reference_frame_onto_the_live_one():
    roi = [[0, 0], [320, 0], [320, 240]]
    assert scale_roi(roi, (320, 240), (640, 480)) == [(0, 0), (640, 0), (640, 480)]
    assert scale_roi(roi, None, (640, 480)) == [(0, 0), (320, 0), (320, 240)]
    assert scale_roi([[0, 0], [1, 1]], (320, 240), (640, 480)) is None


def test_a_bag_left_behind_raises_one_event_with_evidence_and_a_clip(engine, clock):
    engine.set_config("s1", {"settings": FAST_RULES})
    assert engine.has_stream("s1")

    # Owner beside the bag, then walks away.
    run(engine, clock, [det(BAG_BOX, BACKPACK), det(NEAR_PERSON, PERSON)], seconds=2)
    results = run(engine, clock, [det(BAG_BOX, BACKPACK), det(FAR_PERSON, PERSON)], seconds=5)

    events = [e for r in results for e in r["events"]]
    assert len(events) == 1
    event = events[0]
    assert event["class_name"] == "bag"  # backpack -> bag (lost_found_class_name_map)
    assert event["snapshot_jpeg"][:2] == b"\xff\xd8" and event["crop_jpeg"][:2] == b"\xff\xd8"
    assert event["dominant_color"] and event["phash"] is not None and len(event["color_hist"]) == 192 * 4
    assert event["first_seen_at"] < event["abandoned_at"]
    assert event["bbox"] == {"x1": 100.0, "y1": 300.0, "x2": 150.0, "y2": 350.0}

    clips = [c for r in results for c in r["clips"]]
    assert [c["event_id"] for c in clips] == [event["event_id"]]
    assert all(j[:2] == b"\xff\xd8" for j in clips[0]["jpegs"])  # buffered as JPEG, not raw frames
    assert clips[0]["fps"] == pytest.approx(2.0, rel=0.2)

    statuses = {o["class_name"]: o["status"] for o in engine.tracked_objects("s1")}
    assert statuses == {"bag": "ABANDONED"}
    assert engine.live_jpeg("s1")[:2] == b"\xff\xd8"


def test_other_classes_never_reach_the_state_machine(engine, clock):
    engine.set_config("s1", None)
    run(engine, clock, [det([300, 100, 400, 300], CHAIR)], seconds=2)
    assert engine.tracked_objects("s1") == []


def test_inactive_config_skips_inference(engine, clock):
    engine.set_config("s1", {"is_active": False})
    assert not engine.has_stream("s1")
    out = engine.process_frame("s1", FRAME)
    assert out == {"events": [], "clips": []}
    assert engine.model.calls == 0


def test_unknown_stream_and_unloaded_model_do_nothing(engine):
    assert engine.process_frame("nope", FRAME) == {"events": [], "clips": []}
    engine.set_config("s1", None)
    engine.is_loaded = False
    assert engine.process_frame("s1", FRAME) == {"events": [], "clips": []}


def test_release_flushes_partial_clips_and_forgets_the_stream(engine, clock):
    engine.set_config("s1", {"settings": FAST_RULES})
    run(engine, clock, [det(BAG_BOX, BACKPACK), det(NEAR_PERSON, PERSON)], seconds=2)
    results = run(engine, clock, [det(BAG_BOX, BACKPACK), det(FAR_PERSON, PERSON)], seconds=2.5)
    assert sum(len(r["events"]) for r in results) == 1
    assert not any(r["clips"] for r in results)  # post-roll not over yet

    out = engine.release_stream("s1")
    assert out["released"] and len(out["clips"]) == 1
    assert not engine.has_stream("s1") and engine.tracked_objects("s1") is None


def test_reload_config_only_touches_streams_this_process_runs(engine):
    engine.reload_config("elsewhere", {"roi": [[0, 0], [1, 0], [1, 1]]})
    assert not engine.has_stream("elsewhere")
    engine.set_config("s1", None)
    engine.reload_config("s1", {"is_active": False})
    assert not engine.has_stream("s1")


def test_settings_change_keeps_tracks(engine, clock):
    engine.set_config("s1", {"settings": FAST_RULES})
    run(engine, clock, [det(BAG_BOX, BACKPACK), det(NEAR_PERSON, PERSON)], seconds=1)
    before = engine.tracked_objects("s1")
    engine.set_config("s1", {"settings": {**FAST_RULES, "abandon_seconds": 60.0}})
    run(engine, clock, [det(BAG_BOX, BACKPACK), det(NEAR_PERSON, PERSON)], seconds=0.5)
    after = engine.tracked_objects("s1")
    assert [o["track_id"] for o in after] == [o["track_id"] for o in before]
    assert engine._streams["s1"].abandonment.params.abandon_seconds == 60.0
