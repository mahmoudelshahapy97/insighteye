"""No-entry-zone violation rules, exercised without a camera or a model.

Ported from features/no-entry-zone/backend/tests/test_zone_logic.py and
test_timewindow.py, adapted to platform zone rows (UUID ids, class names).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.nez.timewindow import is_zone_active
from app.services.nez.zone_logic import (
    Detection,
    NezSettings,
    ZoneConfig,
    ZoneEvaluator,
    resolve_class_ids,
)

COCO = ["person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck"]
FRAME = (640, 480)
ZONE_SQUARE = [[200, 200], [400, 200], [400, 400], [200, 400]]

# Feet at (300, 350) -> inside the square; (300, 450) -> below it.
INSIDE_BOX = (280, 150, 320, 350)
OUTSIDE_BOX = (280, 250, 320, 450)
# Feet at (220, 210): inside, but far outside the 48 px dedup radius from INSIDE_BOX.
FAR_INSIDE_BOX = (200, 150, 240, 210)

# Monday 2026-02-02 20:00:00 UTC — inside a "Mon-Fri 18:00-07:00" night window.
WALL_BASE = 1_770_062_400


def make_settings(**overrides) -> NezSettings:
    values = dict(
        min_inside_seconds=1.0,
        consecutive_frames=5,
        cooldown_seconds=45.0,
        anchor="bottom_center",
        dedup_radius_frac=0.06,
        incident_gap_seconds=15.0,
        track_ttl_seconds=3.0,
        timezone="UTC",
    )
    values.update(overrides)
    return NezSettings(**values)


def make_row(**overrides) -> dict:
    row = {
        "zone_id": "3f1c2a64-0000-4000-8000-000000000001",
        "name": "electrical-room",
        "polygon": ZONE_SQUARE,
        "ref_width": FRAME[0],
        "ref_height": FRAME[1],
        "target_classes": ["person"],
        "min_dwell_seconds": None,
        "consecutive_frames": None,
        "cooldown_seconds": None,
        "anchor": None,
        "schedule": None,
        "is_active": True,
    }
    row.update(overrides)
    return row


def make_evaluator(settings: NezSettings, **row_overrides) -> ZoneEvaluator:
    return ZoneEvaluator(ZoneConfig.from_row(make_row(**row_overrides), settings, COCO), settings)


def detection(box=INSIDE_BOX, track_id: int = 1, class_id: int = 0, conf: float = 0.9):
    return Detection(track_id=track_id, class_id=class_id, class_name=COCO[class_id], confidence=conf, xyxy=box)


def run_frames(evaluator, detections, *, count: int, start: float = 0.0, step: float = 0.2):
    triggers = []
    ts = start
    for _ in range(count):
        wall = datetime.fromtimestamp(WALL_BASE + ts, tz=timezone.utc)
        result = evaluator.evaluate(detections, ts=ts, wall=wall, frame_size=FRAME)
        triggers.extend(result.triggers)
        ts += step
    return triggers, ts


# --- zone rows -------------------------------------------------------------

def test_class_names_and_ids_both_resolve():
    assert resolve_class_ids(["person", "car", "7", "nonsense"], COCO) == {0, 2, 7}


def test_null_overrides_fall_back_to_defaults_and_numeric_strings_are_parsed():
    cfg = ZoneConfig.from_row(make_row(min_dwell_seconds="2.50"), make_settings(), COCO)
    assert cfg.min_inside_seconds == 2.5
    assert cfg.consecutive_frames == 5
    assert cfg.cooldown_seconds == 45.0
    assert cfg.anchor == "bottom_center"
    assert cfg.target_classes == {0}


# --- debounce ----------------------------------------------------------------

def test_single_frame_inside_does_not_alert():
    triggers, _ = run_frames(make_evaluator(make_settings()), [detection()], count=1)
    assert triggers == []


def test_alert_needs_both_frame_count_and_duration():
    evaluator = make_evaluator(make_settings())
    triggers, _ = run_frames(evaluator, [detection()], count=5, step=0.2)
    assert triggers == []  # 5 frames but only 0.8 s
    more, _ = run_frames(evaluator, [detection()], count=2, start=1.0, step=0.2)
    assert [t.kind for t in more] == ["entry"]


def test_confirmed_entry_carries_the_evidence_fields():
    triggers, _ = run_frames(make_evaluator(make_settings()), [detection()], count=10)
    entry = [t for t in triggers if t.kind == "entry"][0]
    assert entry.zone_name == "electrical-room"
    assert entry.track_id == 1
    assert entry.class_name == "person"
    assert entry.confidence == pytest.approx(0.9)
    assert entry.anchor == (300, 350)
    assert entry.xyxy == INSIDE_BOX


def test_only_one_alert_while_the_object_stays_inside():
    triggers, _ = run_frames(make_evaluator(make_settings()), [detection()], count=60)
    assert len([t for t in triggers if t.kind == "entry"]) == 1


def test_leaving_the_zone_closes_the_event_with_a_duration():
    evaluator = make_evaluator(make_settings())
    triggers, ts = run_frames(evaluator, [detection()], count=10)
    exit_triggers, _ = run_frames(evaluator, [detection(box=OUTSIDE_BOX)], count=1, start=ts)
    assert [t.kind for t in exit_triggers] == ["exit"]
    assert exit_triggers[0].event_id == triggers[0].event_id
    assert exit_triggers[0].duration_seconds == pytest.approx(2.0, abs=0.05)


def test_re_entry_within_the_cooldown_is_suppressed():
    evaluator = make_evaluator(make_settings(cooldown_seconds=45.0))
    _, ts = run_frames(evaluator, [detection()], count=10)
    _, ts = run_frames(evaluator, [detection(box=OUTSIDE_BOX)], count=2, start=ts)
    again, _ = run_frames(evaluator, [detection()], count=10, start=ts)
    assert again == []


def test_re_entry_after_the_cooldown_alerts_again():
    evaluator = make_evaluator(make_settings(cooldown_seconds=5.0, track_ttl_seconds=600.0))
    _, ts = run_frames(evaluator, [detection()], count=10)
    _, ts = run_frames(evaluator, [detection(box=OUTSIDE_BOX)], count=2, start=ts)
    later, _ = run_frames(evaluator, [detection()], count=10, start=ts + 10.0)
    assert [t.kind for t in later] == ["entry"]


def test_non_target_classes_are_ignored_and_vehicle_zones_work():
    person_zone = make_evaluator(make_settings())
    triggers, _ = run_frames(person_zone, [detection(class_id=2)], count=30)
    assert triggers == []

    vehicle_zone = make_evaluator(make_settings(), target_classes=["car", "truck"])
    triggers, _ = run_frames(vehicle_zone, [detection(class_id=2)], count=30)
    assert [t.kind for t in triggers] == ["entry"]


def test_zone_level_overrides_beat_the_global_defaults():
    evaluator = make_evaluator(make_settings(), consecutive_frames=2, min_dwell_seconds=0.3)
    triggers, _ = run_frames(evaluator, [detection()], count=3)
    assert [t.kind for t in triggers] == ["entry"]


def test_lost_track_closes_the_event_after_the_ttl():
    evaluator = make_evaluator(make_settings(track_ttl_seconds=1.0))
    _, ts = run_frames(evaluator, [detection()], count=10)
    reaped, _ = run_frames(evaluator, [], count=1, start=ts + 2.0)
    assert [t.kind for t in reaped] == ["exit"]
    assert evaluator.tracked_count == 0


def test_schedule_arms_and_disarms_the_zone():
    night = make_evaluator(make_settings(), schedule={"days": [0, 1, 2, 3, 4], "start": "18:00", "end": "07:00"})
    triggers, _ = run_frames(night, [detection()], count=10)
    assert [t.kind for t in triggers] == ["entry"]

    daytime = make_evaluator(make_settings(), schedule={"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "17:00"})
    triggers, _ = run_frames(daytime, [detection()], count=10)
    assert triggers == []


def test_disabled_zone_produces_nothing():
    triggers, _ = run_frames(make_evaluator(make_settings(), is_active=False), [detection()], count=30)
    assert triggers == []


def test_disabling_a_zone_closes_its_open_event():
    settings = make_settings()
    evaluator = make_evaluator(settings)
    _, ts = run_frames(evaluator, [detection()], count=10)
    evaluator.update_config(ZoneConfig.from_row(make_row(is_active=False), settings, COCO))
    closing, _ = run_frames(evaluator, [detection()], count=1, start=ts)
    assert [t.kind for t in closing] == ["exit"]


def test_polygon_is_rescaled_when_the_stream_resolution_changes():
    evaluator = make_evaluator(make_settings())
    assert evaluator.polygon_for((640, 480)).tolist() == ZONE_SQUARE
    assert evaluator.polygon_for((1280, 960)).tolist() == [[400, 400], [800, 400], [800, 800], [400, 800]]


# --- duplicate suppression and incident grouping ------------------------------

def test_a_reacquired_track_at_the_same_spot_does_not_alert_twice():
    evaluator = make_evaluator(make_settings())
    _, ts = run_frames(evaluator, [detection(track_id=1)], count=10)
    reacquired, _ = run_frames(evaluator, [detection(track_id=2)], count=10, start=ts)
    assert [t for t in reacquired if t.kind == "entry"] == []


def test_a_second_intruder_elsewhere_in_the_zone_still_alerts():
    evaluator = make_evaluator(make_settings())
    _, ts = run_frames(evaluator, [detection(track_id=1)], count=10)
    both = [detection(track_id=1), detection(track_id=2, box=FAR_INSIDE_BOX)]
    more, _ = run_frames(evaluator, both, count=10, start=ts)
    assert [(t.kind, t.track_id) for t in more if t.kind == "entry"] == [("entry", 2)]


def test_one_incident_covers_a_continuous_occupation():
    evaluator = make_evaluator(make_settings())
    first, ts = run_frames(evaluator, [detection(track_id=1)], count=10)
    both = [detection(track_id=1), detection(track_id=2, box=FAR_INSIDE_BOX)]
    second, _ = run_frames(evaluator, both, count=10, start=ts)
    entries = [t for t in first + second if t.kind == "entry"]
    assert len(entries) == 2
    assert entries[0].incident_id == entries[1].incident_id


def test_a_new_incident_starts_once_the_zone_has_been_clear():
    evaluator = make_evaluator(make_settings(incident_gap_seconds=2.0, cooldown_seconds=1.0, track_ttl_seconds=1.0))
    first, ts = run_frames(evaluator, [detection(track_id=1)], count=10)
    _, ts = run_frames(evaluator, [], count=30, start=ts)
    second, _ = run_frames(evaluator, [detection(track_id=2)], count=10, start=ts)
    opened = [t for t in first if t.kind == "entry"][0]
    reopened = [t for t in second if t.kind == "entry"][0]
    assert opened.incident_id != reopened.incident_id


def test_the_exit_half_lands_on_the_incident_it_opened():
    evaluator = make_evaluator(make_settings(track_ttl_seconds=1.0))
    triggers, ts = run_frames(evaluator, [detection(track_id=1)], count=10)
    entry = [t for t in triggers if t.kind == "entry"][0]
    closing, _ = run_frames(evaluator, [], count=20, start=ts)
    assert [t.incident_id for t in closing if t.kind == "exit"] == [entry.incident_id]


# --- schedules ----------------------------------------------------------------

def _at(weekday_offset: int, hh: int, mm: int, ss: int = 0) -> datetime:
    # 2026-02-02 is a Monday.
    return datetime(2026, 2, 2 + weekday_offset, hh, mm, ss)


def test_no_schedule_means_always_armed():
    assert is_zone_active(None, _at(0, 3, 0))


def test_day_window():
    schedule = {"days": [0, 1, 2, 3, 4], "start": "09:00", "end": "17:00"}
    assert is_zone_active(schedule, _at(0, 12, 0))
    assert not is_zone_active(schedule, _at(0, 18, 0))
    assert not is_zone_active(schedule, _at(5, 12, 0))  # Saturday


def test_overnight_window_covers_both_sides_of_midnight():
    schedule = {"days": [0], "start": "18:00", "end": "07:00"}
    assert is_zone_active(schedule, _at(0, 23, 0))
    assert is_zone_active(schedule, _at(1, 6, 30))
    assert not is_zone_active(schedule, _at(1, 8, 0))


def test_end_minute_is_inclusive():
    schedule = {"days": [0], "start": "00:00", "end": "23:59"}
    assert is_zone_active(schedule, _at(0, 23, 59, 45))
