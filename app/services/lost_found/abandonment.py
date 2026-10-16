# app/services/lost_found/abandonment.py (ported from lost-found app/services/abandonment_service.py)
"""The abandoned-object state machine.

`abandoned` is never a detection class - it is a *status* derived from tracking,
distance and time. The rules follow Vrsalovic et al., "A System for Real-Time
Detection of Abandoned Luggage" (Sensors 2025), with PETS 2006 for the parts it
leaves open:

    OWNERS      every person within the owner radius when the item is first
                seen, plus whoever handles it while it moves (group ownership).
                Nobody else: being near an item that was already there does
                not make it yours
    MOVING      the smoothed centroid drifts more than the tolerance; a moving
                item is being carried and is never abandoned
    ATTENDED    still, and an owner is within the abandon radius - or a stranger
                has stayed close for `stand_in_seconds` and became an owner
    UNATTENDED  still, owned, and every owner is gone or too far: the clock runs
    ABANDONED   unattended for `abandon_seconds`

Distances run between foot points (where a person stands, where the item
rests), and radii are measured in heights of the person concerned, so they
shrink with perspective: a passer-by far down the hall is not "next to" a bag
in the foreground just because their boxes are close in pixels. An item nobody ever
owned (a bin, a chair the detector mistakes for a bag) never alarms.

Pure Python and dependency-free apart from the geometry helpers, so it can be
unit-tested with synthetic detection sequences and no camera.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Deque, Dict, List, Optional, Sequence, Set, Tuple

from app.config.settings import config
from app.services.lost_found.geometry import Box, Point, box_in_polygon, centroid, euclidean, iou, overlap_of_smaller
from app.services.lost_found.tracking import Detection

logger = logging.getLogger(__name__)


class ObjectStatus(str, Enum):
    MOVING = "MOVING"
    ATTENDED = "ATTENDED"
    UNATTENDED = "UNATTENDED"
    ABANDONED = "ABANDONED"


@dataclass
class AbandonmentParams:
    """Thresholds, overridable per camera via ``lost_found_camera_config.settings``."""

    static_tolerance_px: float = config.lost_found_static_tolerance_px
    static_tolerance_ratio: float = config.lost_found_static_tolerance_ratio
    static_window_seconds: float = config.lost_found_static_window_seconds
    owner_distance_px: float = config.lost_found_owner_distance_px
    abandon_distance_px: float = config.lost_found_abandon_distance_px
    owner_radius_heights: float = config.lost_found_owner_radius_heights
    abandon_radius_heights: float = config.lost_found_abandon_radius_heights
    stand_in_seconds: float = config.lost_found_stand_in_seconds
    abandon_seconds: float = config.lost_found_abandon_seconds
    require_owner: bool = config.lost_found_require_owner
    min_object_size_px: float = config.lost_found_min_object_size_px
    track_max_misses: int = config.lost_found_track_max_misses
    reid_iou: float = config.lost_found_reid_iou
    event_cooldown_seconds: float = config.lost_found_event_cooldown_seconds

    @classmethod
    def from_settings(cls, settings: Optional[Dict] = None) -> "AbandonmentParams":
        params = cls()
        if not settings:
            return params
        for key, value in settings.items():
            if hasattr(params, key) and isinstance(value, (int, float)):
                setattr(params, key, type(getattr(params, key))(value))
        return params


@dataclass
class TrackState:
    track_id: int
    class_id: int
    class_name: str
    box: Box
    confidence: float
    first_seen: float
    last_seen: float
    history: Deque[Tuple[float, Point]] = field(default_factory=deque)
    misses: int = 0
    static_since: Optional[float] = None
    # Everyone the item belongs to (a group can share luggage); the first one
    # is reported as the owner.
    owners: List[int] = field(default_factory=list)
    # Each owner's height when closest to the item (with that distance): the
    # scene's scale at the item. It stays put while the owner's own box grows
    # walking toward the camera, because walking off only adds distance.
    owner_scales: Dict[int, Tuple[float, float]] = field(default_factory=dict)
    # A stranger close to the object, and since when - see _is_attended.
    stand_in_track_id: Optional[int] = None
    stand_in_since: Optional[float] = None
    unattended_since: Optional[float] = None
    status: ObjectStatus = ObjectStatus.MOVING
    event_emitted: bool = False
    # Re-appeared where an owner-less still object was: scenery, not luggage
    # someone just brought, so nobody nearby is taken for its owner on sight.
    scenery: bool = False

    @property
    def owner_track_id(self) -> Optional[int]:
        return self.owners[0] if self.owners else None

    @property
    def unattended_seconds(self) -> float:
        if self.unattended_since is None:
            return 0.0
        return max(0.0, self.last_seen - self.unattended_since)

    def add_owner(self, person: Detection) -> None:
        if person.track_id not in self.owners:
            self.owners.append(person.track_id)
        self.observe_owner(person)

    def observe_owner(self, person: Detection) -> None:
        distance = _ground_distance(self.box, person)
        closest = self.owner_scales.get(person.track_id)
        if closest is None or distance < closest[0]:
            self.owner_scales[person.track_id] = (distance, _height(person.box))

    def owner_scale(self, person: Detection) -> float:
        closest = self.owner_scales.get(person.track_id)
        return closest[1] if closest else _height(person.box)


def _height(box: Box) -> float:
    return box[3] - box[1]


def _ground_distance(box: Box, person: Detection) -> float:
    """Distance between where the person stands and where the item rests.

    Foot points (bottom centre of each box) rather than box edges, after PETS
    2006: someone walking toward the camera keeps overlapping a bag behind
    them in the image, but their feet move steadily away from it.
    """
    item = ((box[0] + box[2]) / 2, box[3])
    feet = ((person.box[0] + person.box[2]) / 2, person.box[3])
    return euclidean(item, feet)


@dataclass
class AbandonmentEvent:
    """Emitted exactly once per abandoned object track."""

    track_id: int
    class_name: str
    confidence: float
    box: Box
    first_seen_at: float
    static_since: Optional[float]
    abandoned_at: float
    owner_track_id: Optional[int]
    unattended_seconds: float
    # Unique per camera across tracker restarts, which reuse track ids.
    track_key: str = ""


class AbandonmentTracker:
    """Per-camera state machine. Feed it detections; it hands back events."""

    def __init__(
        self,
        params: Optional[AbandonmentParams] = None,
        roi: Optional[Sequence[Point]] = None,
    ) -> None:
        self.params = params or AbandonmentParams()
        self.roi = roi
        self.tracks: Dict[int, TrackState] = {}
        self._cooldowns: Dict[int, float] = {}
        # Boxes of still objects that expired without ever having an owner.
        self._scenery: Deque[Box] = deque(maxlen=64)
        self._session = self._new_session()

    # ----------------------------------------------------------------- public
    def update(self, detections: Sequence[Detection], timestamp: float) -> List[AbandonmentEvent]:
        """Advance the machine by one processed frame."""
        persons = [d for d in detections if d.is_person]
        objects = [
            d
            for d in detections
            if not d.is_person
            and d.class_id in config.lost_found_object_class_ids
            and self._in_roi(d.box)
            and self._big_enough(d.box)
        ]

        seen_ids = set()
        events: List[AbandonmentEvent] = []
        objects = self._drop_partial_duplicates(objects)
        frame_ids = {d.track_id for d in objects}

        for detection in objects:
            seen_ids.add(detection.track_id)
            state = self._upsert(detection, persons, timestamp, frame_ids)
            event = self._evaluate(state, persons, timestamp)
            if event is not None:
                events.append(event)

        self._expire(seen_ids)
        return events

    def snapshot(self) -> List[Dict]:
        """Current per-object status, for the camera status endpoint."""
        return [
            {
                "track_id": s.track_id,
                "class_name": s.class_name,
                "status": s.status.value,
                "box": list(s.box),
                "owner_track_id": s.owner_track_id,
                "owner_track_ids": list(s.owners),
                "static_seconds": round(s.last_seen - s.static_since, 1) if s.static_since else 0.0,
                "unattended_seconds": round(s.unattended_seconds, 1),
            }
            for s in self.tracks.values()
        ]

    def reset(self) -> None:
        """Forget everything. Called when the stream reconnects and ids restart."""
        self.tracks.clear()
        self._cooldowns.clear()
        self._scenery.clear()
        self._session = self._new_session()

    # ---------------------------------------------------------------- internal
    @staticmethod
    def _new_session() -> str:
        # Base-36 milliseconds: short, and distinct for every restart.
        value, digits = int(time.time() * 1000), ""
        while value:
            value, rem = divmod(value, 36)
            digits = "0123456789abcdefghijklmnopqrstuvwxyz"[rem] + digits
        return digits

    def _in_roi(self, box: Box) -> bool:
        return box_in_polygon(box, self.roi) if self.roi else True

    def _big_enough(self, box: Box) -> bool:
        """Boxes this small are too far away to judge; a closer camera covers them."""
        return min(box[2] - box[0], box[3] - box[1]) >= self.params.min_object_size_px

    def _upsert(
        self, detection: Detection, persons: Sequence[Detection], timestamp: float, frame_ids: set
    ) -> TrackState:
        state = self.tracks.get(detection.track_id) or self._adopt(detection, frame_ids)
        if state is None:
            state = TrackState(
                track_id=detection.track_id,
                class_id=detection.class_id,
                class_name=detection.class_name,
                box=detection.box,
                confidence=detection.confidence,
                first_seen=timestamp,
                last_seen=timestamp,
            )
            state.scenery = any(
                iou(detection.box, box) >= self.params.reid_iou for box in self._scenery
            )
            if not state.scenery:
                # Whoever is next to the item when it appears brought it - all
                # of them, since a group shares its luggage.
                for person in self._persons_within(detection.box, persons, self._owner_radius):
                    state.add_owner(person)
            self.tracks[detection.track_id] = state
        else:
            state.box = detection.box
            state.confidence = detection.confidence
            state.last_seen = timestamp
            state.misses = 0

        state.history.append((timestamp, centroid(detection.box)))
        self._trim_history(state, timestamp)
        return state

    def _drop_partial_duplicates(self, objects: Sequence[Detection]) -> List[Detection]:
        """One detection per object: drop a box lying mostly inside a known object's box.

        A bag partly hidden by a passer-by can come back as a second, smaller
        box next to its usual one. As a "new" item, whoever stood in front of
        it was taken for its owner on sight - and it alarmed when they left.
        Detections already tracked win; then the most confident.
        """
        ordered = sorted(objects, key=lambda d: (d.track_id not in self.tracks, -d.confidence))
        kept: List[Detection] = []
        for detection in ordered:
            if any(overlap_of_smaller(detection.box, k.box) >= self.params.reid_iou for k in kept):
                continue
            kept.append(detection)
        return kept

    def _adopt(self, detection: Detection, frame_ids: set) -> Optional[TrackState]:
        """Carry a dropped track's state over to the new id the tracker gave it.

        A small object left on the floor scores low and blinks in and out; each
        time the tracker loses it, it comes back under a new id. Starting that
        id from scratch reset the static window and the unattended timer on every
        blink, so the alert never fired. A new id whose box sits on an object
        missing from this frame is that object.
        """
        best, best_iou = None, self.params.reid_iou
        for state in self.tracks.values():
            # Only a still object is "the same object" by position: a bag being
            # carried has moved on, and whatever appears where it was last seen
            # must not inherit its owners.
            if state.track_id in frame_ids or state.static_since is None:
                continue
            # Overlap of the smaller box, so a partial view of the object (a
            # box inside its usual one) is recognised as the same object.
            overlap = overlap_of_smaller(detection.box, state.box)
            if overlap >= best_iou:
                best, best_iou = state, overlap
        if best is None:
            return None

        del self.tracks[best.track_id]
        if best.track_id in self._cooldowns:
            self._cooldowns[detection.track_id] = self._cooldowns.pop(best.track_id)
        best.track_id = detection.track_id
        self.tracks[detection.track_id] = best
        return best

    def _trim_history(self, state: TrackState, timestamp: float) -> None:
        """Keep only the sliding window used by the static test.

        The oldest sample kept is the newest one at least a full window old, so
        the history spans the whole window and `_is_static` can see that it does.
        """
        window = self.params.static_window_seconds
        while len(state.history) > 1 and timestamp - state.history[1][0] >= window:
            state.history.popleft()

    def _moved(self, state: TrackState) -> bool:
        """Some centroid in the window strays from their mean by more than the tolerance.

        Measuring from the mean rather than the latest sample averages out the
        box jitter of a weak detection and camera shake. The tolerance grows with
        the object, so a bag close to the camera is allowed proportionally more
        jitter than one far away.
        """
        points = [point for _, point in state.history]
        mean = (sum(p[0] for p in points) / len(points), sum(p[1] for p in points) / len(points))
        height = state.box[3] - state.box[1]
        tolerance = max(self.params.static_tolerance_px, self.params.static_tolerance_ratio * height)
        return any(euclidean(point, mean) > tolerance for point in points)

    def _is_static(self, state: TrackState, timestamp: float) -> bool:
        """Stationary: observed for a whole window without moving."""
        if len(state.history) < 2:
            return False
        if timestamp - state.history[0][0] < self.params.static_window_seconds:
            return False
        return not self._moved(state)

    def _owner_radius(self, person: Detection) -> float:
        return self._radius(person, self.params.owner_radius_heights, self.params.owner_distance_px)

    def _stand_in_radius(self, state: TrackState, person: Detection) -> float:
        """The owner radius, at most at the item's scale (its owners' heights beside it).

        A stranger walking toward the camera grows in the image just as an owner
        does; their own height alone would let them "reach" the item from afar.
        """
        if self.params.owner_radius_heights <= 0:
            return self.params.owner_distance_px
        scales = [height for _, height in state.owner_scales.values()]
        height = min([_height(person.box)] + ([max(scales)] if scales else []))
        return self.params.owner_radius_heights * height

    def _abandon_radius(self, state: TrackState, person: Detection) -> float:
        if self.params.abandon_radius_heights > 0:
            return self.params.abandon_radius_heights * state.owner_scale(person)
        return self.params.abandon_distance_px

    @staticmethod
    def _radius(person: Detection, heights: float, pixels: float) -> float:
        """A radius in multiples of the person's height, or fixed pixels when that is off.

        A person's height is the local scale of the scene: it shrinks with
        distance from the camera exactly as real distances on the floor do.
        """
        if heights > 0:
            return heights * _height(person.box)
        return pixels

    def _persons_within(self, box: Box, persons: Sequence[Detection], radius) -> List[Detection]:
        return [p for p in persons if _ground_distance(box, p) <= radius(p)]

    def _nearest_within(self, box: Box, persons: Sequence[Detection], radius) -> Optional[Detection]:
        close = self._persons_within(box, persons, radius)
        return min(close, key=lambda p: _ground_distance(box, p)) if close else None

    def _evaluate(
        self, state: TrackState, persons: Sequence[Detection], timestamp: float
    ) -> Optional[AbandonmentEvent]:
        if self._moved(state):
            # It is being carried, and whoever carries it owns it too.
            handler = self._nearest_within(state.box, persons, self._owner_radius)
            if handler is not None:
                state.add_owner(handler)

        if not self._is_static(state, timestamp):
            # Moving, or not watched for a full window yet: every timer resets.
            state.static_since = None
            state.stand_in_track_id = None
            state.stand_in_since = None
            state.unattended_since = None
            state.status = ObjectStatus.MOVING
            state.event_emitted = False
            return None

        if state.static_since is None:
            state.static_since = state.history[0][0]

        if self._is_attended(state, persons, timestamp):
            state.status = ObjectStatus.ATTENDED
            state.unattended_since = None
            state.event_emitted = False
            return None

        state.status = ObjectStatus.UNATTENDED
        if not state.owners and self.params.require_owner:
            # Nobody ever brought it: scenery the detector mistakes for luggage,
            # or an item whose owner was never seen. Not an abandonment.
            state.unattended_since = None
            return None

        if state.unattended_since is None:
            state.unattended_since = timestamp

        if state.unattended_seconds < self.params.abandon_seconds:
            return None

        state.status = ObjectStatus.ABANDONED
        if state.event_emitted or self._in_cooldown(state.track_id, timestamp):
            return None

        state.event_emitted = True
        self._cooldowns[state.track_id] = timestamp
        logger.info(
            "Abandoned %s (track %s) after %.1fs unattended",
            state.class_name,
            state.track_id,
            state.unattended_seconds,
        )
        return AbandonmentEvent(
            track_id=state.track_id,
            class_name=state.class_name,
            confidence=state.confidence,
            box=state.box,
            first_seen_at=state.first_seen,
            static_since=state.static_since,
            abandoned_at=timestamp,
            owner_track_id=state.owner_track_id,
            unattended_seconds=state.unattended_seconds,
            track_key=f"{state.track_id}@{self._session}",
        )

    def _is_attended(
        self, state: TrackState, persons: Sequence[Detection], timestamp: float
    ) -> bool:
        """Some owner is minding the object, or a stranger has stayed long enough to be one."""
        visible_owners = [p for p in persons if p.track_id in state.owners]
        for owner in visible_owners:
            state.observe_owner(owner)
        if any(_ground_distance(state.box, p) <= self._abandon_radius(state, p) for p in visible_owners):
            state.stand_in_track_id = None
            state.stand_in_since = None
            return True

        # No owner close (gone, too far, or re-identified under a new track id).
        # A person who stays close is plausibly them and becomes an owner. One
        # who merely walks past is not: adopting passers-by reset the timer of
        # an abandoned bag in any busy corridor, so the alert never fired. An
        # item nobody brought gets no stand-ins: lingering by a bag that was
        # already there does not make it yours to abandon.
        if not state.owners:
            return False
        strangers = [p for p in persons if p.track_id not in state.owners]
        stand_in = self._nearest_within(state.box, strangers, lambda p: self._stand_in_radius(state, p))
        if stand_in is None:
            state.stand_in_track_id = None
            state.stand_in_since = None
            return False

        if stand_in.track_id != state.stand_in_track_id:
            state.stand_in_track_id = stand_in.track_id
            state.stand_in_since = timestamp
        if timestamp - state.stand_in_since < self.params.stand_in_seconds:
            return False

        state.add_owner(stand_in)
        return True

    def _in_cooldown(self, track_id: int, timestamp: float) -> bool:
        last = self._cooldowns.get(track_id)
        return last is not None and (timestamp - last) < self.params.event_cooldown_seconds

    def _expire(self, seen_ids: set) -> None:
        """Drop tracks the detector has stopped reporting."""
        for track_id in list(self.tracks):
            if track_id in seen_ids:
                continue
            state = self.tracks[track_id]
            state.misses += 1
            if state.misses > self.params.track_max_misses:
                if not state.owners and state.static_since is not None:
                    self._scenery.append(state.box)
                del self.tracks[track_id]
                self._cooldowns.pop(track_id, None)
