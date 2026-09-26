"""No-entry-zone violation state machine (ported from features/no-entry-zone).

Deliberately free of capture, database and FastAPI concerns: it takes tracked
detections plus a clock and returns triggers, which makes every rule unit-testable
without a camera or a model.

Rules, per zone:
  * a track must be inside for ``consecutive_frames`` frames AND ``min_inside_seconds``
    before an event opens (debounce);
  * a track that already alerted stays quiet for ``cooldown_seconds``;
  * a *new* track id alerting within ``dedup_radius_frac`` x frame-diagonal of a recent
    alert is suppressed — that is the tracker re-issuing an id for the same person;
  * every event raised while the zone stays occupied shares one ``incident_id``; the
    zone must be empty for ``incident_gap_seconds`` before a new incident starts;
  * an event closes (exit trigger) when the object leaves, its track is lost for
    ``track_ttl_seconds``, or the zone is disabled / outside its schedule / removed.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

import numpy as np

from app.services.nez.geometry import anchor_point, is_inside, scale_polygon, to_numpy_polygon
from app.services.nez.timewindow import is_zone_active, to_local

TriggerKind = Literal["entry", "exit"]


@dataclass(slots=True)
class NezSettings:
    """Global defaults; each zone may override the first four."""

    min_inside_seconds: float = 1.0
    consecutive_frames: int = 3
    cooldown_seconds: float = 45.0
    anchor: str = "bottom_center"
    dedup_radius_frac: float = 0.06
    incident_gap_seconds: float = 15.0
    track_ttl_seconds: float = 3.0
    timezone: str = "Africa/Cairo"

    @classmethod
    def from_config(cls, config: Any) -> "NezSettings":
        return cls(
            consecutive_frames=int(getattr(config, "no_entry_zone_consecutive_frames", 3)),
            cooldown_seconds=float(getattr(config, "no_entry_zone_cooldown_seconds", 45.0)),
            anchor=str(getattr(config, "no_entry_zone_anchor", "bottom_center")),
            dedup_radius_frac=float(getattr(config, "no_entry_zone_dedup_radius_frac", 0.06)),
            incident_gap_seconds=float(getattr(config, "no_entry_zone_incident_gap_seconds", 15.0)),
            track_ttl_seconds=float(getattr(config, "no_entry_zone_track_ttl_seconds", 3.0)),
            timezone=str(getattr(config, "no_entry_zone_timezone", "Africa/Cairo")),
        )


@dataclass(slots=True)
class Detection:
    track_id: int
    class_id: int
    class_name: str
    confidence: float
    xyxy: tuple[float, float, float, float]


def resolve_class_ids(target_classes: Iterable[Any] | None, class_names: Sequence[str]) -> set[int]:
    """Zone rows store classes as names ('person') or numeric strings ('0'); the
    detector speaks class ids. Unknown names are dropped."""
    ids: set[int] = set()
    lookup = {name: idx for idx, name in enumerate(class_names)}
    for value in target_classes or ():
        text = str(value).strip()
        if text.isdigit():
            ids.add(int(text))
        elif text in lookup:
            ids.add(lookup[text])
    return ids


def _opt_float(value: Any) -> float | None:
    return None if value is None else float(value)


@dataclass(slots=True)
class ZoneConfig:
    """A zone flattened for the hot loop: per-zone overrides already resolved
    against the global defaults."""

    zone_id: str
    zone_name: str
    polygon: list[tuple[int, int]]
    ref_size: tuple[int, int]
    target_classes: set[int]
    min_inside_seconds: float
    consecutive_frames: int
    cooldown_seconds: float
    anchor: str
    schedule: dict | None
    enabled: bool

    @classmethod
    def from_row(
        cls, zone: Mapping[str, Any], settings: NezSettings, class_names: Sequence[str]
    ) -> "ZoneConfig":
        classes = resolve_class_ids(zone.get("target_classes") or ["person"], class_names)
        dwell = _opt_float(zone.get("min_dwell_seconds"))
        frames = zone.get("consecutive_frames")
        cooldown = _opt_float(zone.get("cooldown_seconds"))
        return cls(
            zone_id=str(zone["zone_id"]),
            zone_name=zone.get("name") or "zone",
            polygon=[(int(x), int(y)) for x, y in zone["polygon"]],
            ref_size=(int(zone["ref_width"]), int(zone["ref_height"])),
            target_classes=classes,
            min_inside_seconds=dwell if dwell is not None else settings.min_inside_seconds,
            consecutive_frames=int(frames) if frames is not None else settings.consecutive_frames,
            cooldown_seconds=cooldown if cooldown is not None else settings.cooldown_seconds,
            anchor=zone.get("anchor") or settings.anchor,
            schedule=zone.get("schedule") or None,
            enabled=bool(zone.get("is_active", True)),
        )


@dataclass(slots=True)
class TrackState:
    last_seen_at: float
    first_inside_at: float | None = None
    entered_at_wall: datetime | None = None
    consecutive_inside: int = 0
    alerted_at: float | None = None
    open_event_id: uuid.UUID | None = None
    # Held alongside the event id so the exit half lands on the same incident even if the
    # zone has meanwhile emptied and a new incident has been opened.
    incident_id: uuid.UUID | None = None
    max_confidence: float = 0.0
    class_id: int = -1
    class_name: str = ""


@dataclass(slots=True)
class ViolationTrigger:
    kind: TriggerKind
    event_id: uuid.UUID
    incident_id: uuid.UUID
    zone_id: str
    zone_name: str
    track_id: int
    class_id: int
    class_name: str
    confidence: float
    entered_at: datetime
    alerted_at: datetime | None = None
    exited_at: datetime | None = None
    duration_seconds: float | None = None
    anchor: tuple[int, int] | None = None
    xyxy: tuple[float, float, float, float] | None = None


@dataclass(slots=True)
class EvaluationResult:
    inside_track_ids: set[int] = field(default_factory=set)
    triggers: list[ViolationTrigger] = field(default_factory=list)


class ZoneEvaluator:
    """Tracks one zone's per-object state across frames."""

    def __init__(self, config: ZoneConfig, settings: NezSettings) -> None:
        self.config = config
        self._settings = settings
        self._states: dict[int, TrackState] = {}
        self._polygon_cache: tuple[tuple[int, int], np.ndarray] | None = None
        # (monotonic ts, anchor point) of alerts still inside their cooldown, used to
        # suppress a re-acquired track that alerts again from the same spot.
        self._recent_alerts: list[tuple[float, tuple[int, int]]] = []
        # The incident every event opened right now belongs to, and when the zone last
        # became empty. `None` empty-since means "occupied".
        self._incident_id: uuid.UUID | None = None
        self._empty_since: float | None = None

    # --- configuration ---------------------------------------------------
    def update_config(self, config: ZoneConfig) -> None:
        """Apply an edited zone without losing in-flight track state.

        Geometry changes invalidate the cached polygon; open events are closed by the
        normal exit path on the next frame if the object is no longer inside.
        """
        self.config = config
        self._polygon_cache = None

    def polygon_for(self, frame_size: tuple[int, int]) -> np.ndarray:
        if self._polygon_cache is not None and self._polygon_cache[0] == frame_size:
            return self._polygon_cache[1]
        scaled = scale_polygon(self.config.polygon, self.config.ref_size, frame_size)
        polygon = to_numpy_polygon(scaled)
        self._polygon_cache = (frame_size, polygon)
        return polygon

    def is_armed(self, wall: datetime) -> bool:
        return self.config.enabled and is_zone_active(
            self.config.schedule, to_local(wall, self._settings.timezone)
        )

    # --- evaluation ------------------------------------------------------
    def evaluate(
        self,
        detections: Sequence[Detection],
        *,
        ts: float,
        wall: datetime,
        frame_size: tuple[int, int],
    ) -> EvaluationResult:
        """Advance the state machine by one frame.

        ``ts`` is a monotonic clock (durations), ``wall`` is timezone-aware wall time
        (what gets stored on the event). ``frame_size`` is (width, height).
        """
        result = EvaluationResult()

        if not self.is_armed(wall):
            result.triggers.extend(self.close_all(ts, wall))
            return result

        polygon = self.polygon_for(frame_size)
        dedup_radius = self._dedup_radius(frame_size)

        for detection in detections:
            if self.config.target_classes and detection.class_id not in self.config.target_classes:
                continue

            state = self._states.get(detection.track_id)
            if state is None:
                state = TrackState(last_seen_at=ts)
                self._states[detection.track_id] = state
            state.last_seen_at = ts
            state.class_id = detection.class_id
            state.class_name = detection.class_name

            point = anchor_point(detection.xyxy, self.config.anchor)
            if is_inside(polygon, point):
                result.inside_track_ids.add(detection.track_id)
                state.consecutive_inside += 1
                state.max_confidence = max(state.max_confidence, detection.confidence)
                if state.first_inside_at is None:
                    state.first_inside_at = ts
                    state.entered_at_wall = wall

                trigger = self._maybe_open_event(state, detection, ts, wall, point, dedup_radius)
                if trigger is not None:
                    result.triggers.append(trigger)
            else:
                closing = self._close(detection.track_id, state, ts, wall)
                if closing is not None:
                    result.triggers.append(closing)
                state.first_inside_at = None
                state.entered_at_wall = None
                state.consecutive_inside = 0
                state.max_confidence = 0.0

        result.triggers.extend(self._reap(ts, wall))
        self._track_occupancy(result.inside_track_ids, ts)
        return result

    # --- incident bookkeeping -------------------------------------------
    def _dedup_radius(self, frame_size: tuple[int, int]) -> float:
        """Dedup radius in pixels for this frame size (0 disables the rule)."""
        frac = self._settings.dedup_radius_frac
        if frac <= 0:
            return 0.0
        width, height = frame_size
        return float(np.hypot(width, height)) * frac

    def _track_occupancy(self, inside_track_ids: set[int], ts: float) -> None:
        """Close the current incident once the zone has been clear long enough."""
        if inside_track_ids:
            self._empty_since = None
            return
        if self._empty_since is None:
            self._empty_since = ts
        elif ts - self._empty_since >= self._settings.incident_gap_seconds:
            self._incident_id = None

    def _current_incident(self) -> uuid.UUID:
        if self._incident_id is None:
            self._incident_id = uuid.uuid4()
        return self._incident_id

    def _suppressed_by_neighbour(self, point: tuple[int, int], ts: float, radius: float) -> bool:
        """True when a recent alert fired close enough to be the same intruder."""
        if radius <= 0:
            return False
        cooldown = self.config.cooldown_seconds
        for alert_ts, alert_point in self._recent_alerts:
            if ts - alert_ts >= cooldown:
                continue
            if np.hypot(point[0] - alert_point[0], point[1] - alert_point[1]) <= radius:
                return True
        return False

    def _maybe_open_event(
        self,
        state: TrackState,
        detection: Detection,
        ts: float,
        wall: datetime,
        point: tuple[int, int],
        dedup_radius: float = 0.0,
    ) -> ViolationTrigger | None:
        if state.open_event_id is not None:
            return None  # already alerted for this stay

        # `is None` rather than truthiness: a monotonic clock reading of 0.0 is valid.
        first_inside = state.first_inside_at if state.first_inside_at is not None else ts
        duration = ts - first_inside
        if state.consecutive_inside < self.config.consecutive_frames:
            return None
        if duration < self.config.min_inside_seconds:
            return None
        if state.alerted_at is not None and (ts - state.alerted_at) < self.config.cooldown_seconds:
            return None

        # The per-track cooldown above only sees this track. A track id the tracker has
        # just re-issued for the same person arrives with alerted_at unset, so check the
        # neighbourhood too.
        if self._suppressed_by_neighbour(point, ts, dedup_radius):
            return None

        state.open_event_id = uuid.uuid4()
        state.alerted_at = ts
        state.incident_id = self._current_incident()
        self._recent_alerts.append((ts, point))
        return ViolationTrigger(
            kind="entry",
            event_id=state.open_event_id,
            incident_id=state.incident_id,
            zone_id=self.config.zone_id,
            zone_name=self.config.zone_name,
            track_id=detection.track_id,
            class_id=detection.class_id,
            class_name=detection.class_name,
            confidence=max(state.max_confidence, detection.confidence),
            entered_at=state.entered_at_wall if state.entered_at_wall is not None else wall,
            alerted_at=wall,
            duration_seconds=round(duration, 2),
            anchor=point,
            xyxy=detection.xyxy,
        )

    def _close(
        self, track_id: int, state: TrackState, ts: float, wall: datetime
    ) -> ViolationTrigger | None:
        """Emit the exit half of an open event, if there is one."""
        if state.open_event_id is None:
            return None
        first_inside = state.first_inside_at if state.first_inside_at is not None else ts
        duration = ts - first_inside
        trigger = ViolationTrigger(
            kind="exit",
            event_id=state.open_event_id,
            incident_id=state.incident_id or self._current_incident(),
            zone_id=self.config.zone_id,
            zone_name=self.config.zone_name,
            track_id=track_id,
            class_id=state.class_id,
            class_name=state.class_name,
            confidence=state.max_confidence,
            entered_at=state.entered_at_wall if state.entered_at_wall is not None else wall,
            exited_at=wall,
            duration_seconds=round(duration, 2),
        )
        state.open_event_id = None
        state.incident_id = None
        return trigger

    def _reap(self, ts: float, wall: datetime) -> list[ViolationTrigger]:
        """Drop tracks the tracker has stopped reporting (occlusion, left the frame)."""
        triggers: list[ViolationTrigger] = []
        ttl = self._settings.track_ttl_seconds
        cooldown = self.config.cooldown_seconds
        self._recent_alerts = [entry for entry in self._recent_alerts if ts - entry[0] < cooldown]
        for track_id, state in list(self._states.items()):
            if ts - state.last_seen_at <= ttl:
                continue
            closing = self._close(track_id, state, ts, wall)
            if closing is not None:
                triggers.append(closing)
            del self._states[track_id]
        return triggers

    def close_all(self, ts: float, wall: datetime) -> list[ViolationTrigger]:
        """Close every open event. Used when the zone is disarmed, removed, or the
        stream stops — without it those events would stay open forever."""
        triggers: list[ViolationTrigger] = []
        for track_id, state in list(self._states.items()):
            closing = self._close(track_id, state, ts, wall)
            if closing is not None:
                triggers.append(closing)
        self._states.clear()
        # A zone that has been switched off is not "empty for a while", it is not
        # watching. Whatever happens after it comes back is a new incident.
        self._recent_alerts.clear()
        self._incident_id = None
        self._empty_since = None
        return triggers

    @property
    def tracked_count(self) -> int:
        return len(self._states)
