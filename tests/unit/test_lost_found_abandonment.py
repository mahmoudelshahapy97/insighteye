"""Lost & found abandonment state machine (ported from the standalone app's
test_abandonment.py): synthetic detections - no camera, no model, no DB."""

from typing import List, Sequence

import pytest

from app.services.lost_found.abandonment import (
    AbandonmentParams,
    AbandonmentTracker,
    ObjectStatus,
)
from app.services.lost_found.tracking import Detection

PERSON_CLASS = 0
BAG_CLASS = 24  # COCO backpack; the app stores it as "bag"

BAG_BOX = (100.0, 100.0, 150.0, 150.0)
NEAR_PERSON_BOX = (160.0, 100.0, 200.0, 180.0)   # 10 px gap from the bag
FAR_PERSON_BOX = (600.0, 100.0, 640.0, 180.0)    # 450 px gap from the bag


def params(**overrides) -> AbandonmentParams:
    base = dict(
        static_tolerance_px=10.0,
        static_window_seconds=1.0,
        owner_distance_px=100.0,
        abandon_distance_px=150.0,
        abandon_seconds=5.0,
        track_max_misses=5,
        event_cooldown_seconds=300.0,
    )
    base.update(overrides)
    return AbandonmentParams(**base)


def bag(box=BAG_BOX, track_id: int = 7) -> Detection:
    return Detection(
        track_id=track_id,
        class_id=BAG_CLASS,
        class_name="bag",
        confidence=0.9,
        box=box,
    )


def person(box=NEAR_PERSON_BOX, track_id: int = 1) -> Detection:
    return Detection(
        track_id=track_id, class_id=PERSON_CLASS, class_name="person", confidence=0.9, box=box
    )


def run(
    tracker: AbandonmentTracker,
    frames: Sequence[Sequence[Detection]],
    dt: float = 0.5,
    start: float = 0.0,
) -> List:
    """Feed a sequence of frames dt seconds apart and collect every event emitted."""
    events = []
    for index, detections in enumerate(frames):
        events.extend(tracker.update(detections, start + index * dt))
    return events


def left_at_start(t: float) -> List[Detection]:
    """The owner puts the bag down at t=0 and is gone from the next frame on."""
    return [bag(), person()] if t == 0 else [bag()]


def frames_for(seconds: float, dt: float, detections_at) -> List[List[Detection]]:
    """Build ``seconds / dt`` frames, calling ``detections_at(t)`` for each timestamp."""
    count = int(seconds / dt) + 1
    return [detections_at(i * dt) for i in range(count)]


# ---------------------------------------------------------------- core behaviour
def test_owner_leaves_object_becomes_abandoned():
    """Bag goes static with an owner beside it; owner walks off; event fires."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        return [bag()] if t >= 5.0 else [bag(), person()]

    events = run(tracker, frames_for(12.0, 0.5, at))

    assert len(events) == 1
    event = events[0]
    assert event.class_name == "bag"
    assert event.track_id == 7
    assert event.owner_track_id == 1
    assert event.unattended_seconds >= 5.0
    assert tracker.tracks[7].status is ObjectStatus.ABANDONED


@pytest.mark.parametrize("dt", [0.3, 0.37, 0.43])
def test_static_detected_when_frames_do_not_land_on_the_window(dt: float):
    """Real frame times never sit exactly one window apart; stillness must still register."""
    tracker = AbandonmentTracker(params=params())

    events = run(tracker, frames_for(12.0, dt, left_at_start), dt=dt)

    assert len(events) == 1
    assert tracker.tracks[7].status is ObjectStatus.ABANDONED


def test_owner_returns_before_timeout_no_event():
    """The clock resets the moment the owner comes back within range."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        if 5.0 <= t < 8.0:
            return [bag()]  # owner briefly out of frame
        return [bag(), person()]

    events = run(tracker, frames_for(20.0, 0.5, at))

    assert events == []
    assert tracker.tracks[7].status is ObjectStatus.ATTENDED


def test_passer_by_does_not_reset_the_clock():
    """Someone walking past an abandoned bag is not its owner coming back."""
    tracker = AbandonmentTracker(params=params(stand_in_seconds=3.0))

    def at(t: float) -> List[Detection]:
        if 3.0 <= t < 4.5:
            return [bag(), person(NEAR_PERSON_BOX, track_id=2)]
        return left_at_start(t)

    # Alone from t=1 (static window), so 5 s later it fires - within 8 s. Had
    # the passer-by been adopted, the clock would restart at 4.5 and miss it.
    events = run(tracker, frames_for(8.0, 0.5, at))

    assert len(events) == 1


def test_stranger_who_stays_close_becomes_the_owner():
    """A person lingering by the bag is plausibly its owner under a new track id."""
    tracker = AbandonmentTracker(params=params(stand_in_seconds=3.0))

    def at(t: float) -> List[Detection]:
        # Owner 1 drops it and leaves; "person 2" (owner back under a new id)
        # turns up at t=2 and stays.
        return [bag(), person(NEAR_PERSON_BOX, track_id=2)] if t >= 2.0 else left_at_start(t)

    events = run(tracker, frames_for(12.0, 0.5, at))

    assert events == []
    assert tracker.tracks[7].status is ObjectStatus.ATTENDED
    assert tracker.tracks[7].owners == [1, 2]


def test_lingering_by_a_bag_nobody_brought_does_not_claim_it():
    """Someone waiting next to a bag that was already there is not its owner."""
    tracker = AbandonmentTracker(params=params(stand_in_seconds=3.0))

    def at(t: float) -> List[Detection]:
        # Bag alone from the start; a stranger waits beside it from t=2 to t=6.
        return [bag(), person(NEAR_PERSON_BOX, track_id=2)] if 2.0 <= t < 6.0 else [bag()]

    events = run(tracker, frames_for(14.0, 0.5, at))

    assert events == []
    assert tracker.tracks[7].owners == []


def test_new_track_id_on_the_same_spot_keeps_the_timers():
    """The tracker re-ids a blinking bag; the replacement must not restart the clock."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        if 3.0 <= t < 4.0:
            return []  # detector lost it for a moment
        if t == 0:
            return [bag(), person()]
        return [bag(track_id=7 if t < 3.0 else 8)]

    # Alone and still from t=1, abandoned 5 s later. Starting afresh at t=4
    # (new window, new timer) would not fire before t=10.
    events = run(tracker, frames_for(8.0, 0.5, at))

    assert len(events) == 1
    assert events[0].track_id == 8
    assert set(tracker.tracks) == {8}


def test_owner_walking_toward_the_camera_has_left():
    """Their box grows and keeps overlapping the bag in the image, but they have gone."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        if t < 2.0:
            return [bag(), person()]
        # From t=2 the owner walks toward the camera: feet move down the frame
        # and the box grows, staying level with the bag on screen.
        step = (t - 2.0) * 60.0
        return [bag(), person((150.0, 60.0, 200.0 + step / 2, 180.0 + step))]

    events = run(tracker, frames_for(12.0, 0.5, at))

    assert len(events) == 1
    assert events[0].owner_track_id == 1


def test_partial_view_of_a_known_bag_is_not_a_new_item():
    """A passer-by half-hides a bag that was already there; its smaller box is the same bag."""
    tracker = AbandonmentTracker(params=params())
    partial = (110.0, 120.0, 140.0, 150.0)  # inside BAG_BOX, low IoU with it

    def at(t: float) -> List[Detection]:
        if 4.0 <= t < 6.0:
            # Both boxes at once, then only the partial one, with a person in front.
            boxes = [bag(), bag(partial, track_id=-5)] if t < 5.0 else [bag(partial, track_id=-5)]
            return boxes + [person(NEAR_PERSON_BOX, track_id=9)]
        return [bag()]

    events = run(tracker, frames_for(16.0, 0.5, at))

    assert events == []
    assert all(not state.owners for state in tracker.tracks.values())


def test_owner_present_but_too_far_starts_the_clock():
    """Still in frame is not enough - the owner has to stay close."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        return [bag(), person(FAR_PERSON_BOX if t >= 5.0 else NEAR_PERSON_BOX)]

    events = run(tracker, frames_for(12.0, 0.5, at))

    assert len(events) == 1
    assert events[0].owner_track_id == 1


def test_moving_object_never_abandoned():
    """An object being carried keeps drifting, so it never goes static."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        shift = t * 60.0  # well beyond static_tolerance_px per frame
        return [bag((100 + shift, 100.0, 150 + shift, 150.0))]

    events = run(tracker, frames_for(20.0, 0.5, at))

    assert events == []
    assert tracker.tracks[7].status is ObjectStatus.MOVING


def test_object_nobody_was_seen_with_never_alarms():
    """Scenery the detector mistakes for a bag has no owner, so it is not abandoned."""
    tracker = AbandonmentTracker(params=params())

    events = run(tracker, frames_for(12.0, 0.5, lambda t: [bag()]))

    assert events == []
    assert tracker.tracks[7].status is ObjectStatus.UNATTENDED


def test_unowned_object_alarms_when_no_owner_is_required():
    """With require_owner off, a bag already alone when it goes static starts the clock."""
    tracker = AbandonmentTracker(params=params(require_owner=False))

    events = run(tracker, frames_for(12.0, 0.5, lambda t: [bag()]))

    assert len(events) == 1
    assert events[0].owner_track_id is None


def test_group_member_staying_keeps_the_bag_attended():
    """Two people arrive with one bag; one leaves, the other stays - not abandoned."""
    tracker = AbandonmentTracker(params=params())
    beside = (40.0, 100.0, 90.0, 180.0)  # the other side of the bag

    def at(t: float) -> List[Detection]:
        if t == 0:
            return [bag(), person(), person(beside, track_id=2)]
        return [bag(), person(beside, track_id=2)]

    events = run(tracker, frames_for(12.0, 0.5, at))

    assert events == []
    assert tracker.tracks[7].owner_track_id == 1
    assert 2 in tracker.tracks[7].owners
    assert tracker.tracks[7].status is ObjectStatus.ATTENDED


def test_distant_person_is_not_next_to_the_bag():
    """Radii scale with each person's height: a small, far figure is not minding the bag."""
    tracker = AbandonmentTracker(params=params())
    far_small = (210.0, 90.0, 225.0, 130.0)  # 40 px tall, 60 px from the bag

    def at(t: float) -> List[Detection]:
        return left_at_start(t) + ([person(far_small, track_id=2)] if t > 0 else [])

    events = run(tracker, frames_for(8.0, 0.5, at))

    assert len(events) == 1


def test_event_emitted_exactly_once():
    """Long after the alert, the same track must not keep re-firing."""
    tracker = AbandonmentTracker(params=params())

    def at(t: float) -> List[Detection]:
        return [bag()] if t >= 5.0 else [bag(), person()]

    events = run(tracker, frames_for(120.0, 0.5, at))

    assert len(events) == 1


def test_person_alone_is_ignored():
    """People are tracked for ownership only; they are never abandonment candidates."""
    tracker = AbandonmentTracker(params=params())

    events = run(tracker, frames_for(30.0, 0.5, lambda t: [person()]))

    assert events == []
    assert tracker.tracks == {}


# --------------------------------------------------------------------- ROI gating
def test_object_outside_roi_is_ignored():
    roi = [(0.0, 0.0), (80.0, 0.0), (80.0, 80.0), (0.0, 80.0)]  # bag centre is (125,125)
    tracker = AbandonmentTracker(params=params(), roi=roi)

    events = run(tracker, frames_for(20.0, 0.5, lambda t: [bag()]))

    assert events == []


def test_object_inside_roi_still_fires():
    roi = [(0.0, 0.0), (400.0, 0.0), (400.0, 400.0), (0.0, 400.0)]
    tracker = AbandonmentTracker(params=params(), roi=roi)

    events = run(tracker, frames_for(12.0, 0.5, left_at_start))

    assert len(events) == 1


# ------------------------------------------------------------------- housekeeping
def test_track_expires_after_max_misses():
    tracker = AbandonmentTracker(params=params(track_max_misses=3))

    run(tracker, [[bag()]] * 4)
    assert 7 in tracker.tracks

    run(tracker, [[]] * 5, start=100.0)
    assert 7 not in tracker.tracks


def test_reset_clears_state():
    tracker = AbandonmentTracker(params=params())
    run(tracker, frames_for(12.0, 0.5, lambda t: [bag()]))

    tracker.reset()

    assert tracker.tracks == {}
    assert tracker.snapshot() == []


def test_snapshot_reports_live_status():
    tracker = AbandonmentTracker(params=params())
    run(tracker, frames_for(3.0, 0.5, lambda t: [bag(), person()]))

    snapshot = tracker.snapshot()

    assert len(snapshot) == 1
    assert snapshot[0]["track_id"] == 7
    assert snapshot[0]["status"] == ObjectStatus.ATTENDED.value
    assert snapshot[0]["owner_track_id"] == 1


@pytest.mark.parametrize("abandon_seconds,expected", [(2.0, 1), (60.0, 0)])
def test_threshold_is_configurable(abandon_seconds: float, expected: int):
    """Per-camera overrides really do change when the alert fires."""
    tracker = AbandonmentTracker(params=params(abandon_seconds=abandon_seconds))

    def at(t: float) -> List[Detection]:
        return [bag()] if t >= 2.0 else [bag(), person()]

    assert len(run(tracker, frames_for(15.0, 0.5, at))) == expected


def test_params_from_settings_overrides_only_known_keys():
    merged = AbandonmentParams.from_settings({"abandon_seconds": 45, "nonsense": "x"})

    assert merged.abandon_seconds == 45.0
    assert not hasattr(merged, "nonsense")
