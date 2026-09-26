"""No-entry-zone (restricted polygon) detection engine.

Ported from features/no-entry-zone: YOLO detections -> per-stream ByteTrack ->
per-zone state machine (debounce, cooldown, neighbour dedup, incident grouping,
schedules) -> entry/exit triggers with snapshot and clip evidence.

The model is shared by every stream (one singleton, like the shoplifting engine);
everything stateful — tracker, zone evaluators, pre-roll buffer, pending clips — is
kept per stream, so track ids and incidents never leak between cameras.

Threading: ``process_frame`` runs in the stream-processing thread pool, while
``set_zones`` is called from API handlers on the event loop. Zone changes are therefore
queued and applied at the start of the stream's next frame.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import cv2
import numpy as np

from app.config.settings import config
from app.services.model_loader import ModelFactory, ModelBackend
from app.services.nez.geometry import to_numpy_polygon, scale_polygon
from app.services.nez.zone_logic import (
    NezSettings,
    ViolationTrigger,
    ZoneConfig,
    ZoneEvaluator,
)

logger = logging.getLogger(__name__)

CLIP_MAX_WIDTH = 960
SNAPSHOT_MAX_WIDTH = 1280
_RED = (0, 0, 255)
_AMBER = (0, 165, 255)
_GREEN = (80, 200, 80)


@dataclass
class _ClipJob:
    event_uuid: str
    frames: List[Tuple[float, np.ndarray]]
    until_ts: float


@dataclass
class _StreamState:
    tracker: Any
    evaluators: Dict[str, ZoneEvaluator] = field(default_factory=dict)
    pending_zones: Optional[List[Dict[str, Any]]] = None
    preroll: Deque[Tuple[float, np.ndarray]] = field(default_factory=deque)
    clip_jobs: List[_ClipJob] = field(default_factory=list)


def _trigger_dict(t: ViolationTrigger) -> Dict[str, Any]:
    return {
        "kind": t.kind,
        "event_uuid": str(t.event_id),
        "incident_id": t.incident_id,
        "zone_id": t.zone_id,
        "zone_name": t.zone_name,
        "track_id": t.track_id,
        "target_class": t.class_name or str(t.class_id),
        "confidence": round(float(t.confidence), 4),
        "entered_at": t.entered_at,
        "alerted_at": t.alerted_at,
        "exited_at": t.exited_at,
        "dwell_seconds": t.duration_seconds,
    }


def _resize_max_width(frame: np.ndarray, max_width: int) -> np.ndarray:
    h, w = frame.shape[:2]
    if w <= max_width:
        return frame
    scale = max_width / float(w)
    return cv2.resize(frame, (max_width, int(round(h * scale))))


class NoEntryZoneEngine:
    """Per-stream zone config + intrusion detection."""

    def __init__(self) -> None:
        self.is_loaded = False
        self.model = None
        self.settings = NezSettings.from_config(config)
        self.class_names: List[str] = list(getattr(config, "coco_class_names", []) or [])
        self._streams: Dict[str, _StreamState] = {}
        self._active_zone_count: Dict[str, int] = {}
        self._lock = threading.Lock()        # guards _streams / pending zones
        self._model_lock = threading.Lock()  # one inference at a time on the shared model

    # ------------------------------------------------------------------ model
    def load_models(self) -> bool:
        if self.is_loaded:
            return True
        try:
            model_path = config.no_entry_zone_model_path
            self.model = ModelFactory.create_loader(
                model_path,
                backend=ModelBackend(config.model_backend),
                confidence_threshold=getattr(config, "no_entry_zone_confidence", 0.4),
            )
            self.model.load_model()
            self.is_loaded = True
            logger.info("NoEntryZoneEngine model loaded from %s", model_path)
            return True
        except Exception as e:
            logger.error(f"Failed to load no-entry-zone model: {e}", exc_info=True)
            self.is_loaded = False
            return False

    # ------------------------------------------------------------------ zones
    def set_zones(self, stream_id: str, zones: List[Dict[str, Any]]) -> None:
        """Queue this camera's zones (all of them, active or not); applied on the
        stream's next frame. Safe to call from any thread."""
        with self._lock:
            state = self._streams.get(stream_id)
            if state is None:
                state = _StreamState(tracker=None)
                self._streams[stream_id] = state
            state.pending_zones = list(zones)
            self._active_zone_count[stream_id] = sum(1 for z in zones if z.get("is_active", True))

    def zones_loaded(self, stream_id: str) -> Optional[int]:
        """Zones this process is enforcing on the stream, or None if it has never seen it."""
        state = self._streams.get(stream_id)
        if state is None:
            return None
        return sum(1 for ev in state.evaluators.values() if ev.config.enabled)

    def has_zones(self, stream_id: str) -> bool:
        """True while the stream has active zones, or still has open events to close."""
        if self._active_zone_count.get(stream_id, 0) > 0:
            return True
        state = self._streams.get(stream_id)
        return bool(state and (
            state.pending_zones is not None
            or state.clip_jobs
            or any(ev.tracked_count for ev in state.evaluators.values())
        ))

    def _apply_pending(self, state: _StreamState, ts: float, wall: datetime) -> List[ViolationTrigger]:
        with self._lock:
            pending = state.pending_zones
            state.pending_zones = None
        if pending is None:
            return []

        closing: List[ViolationTrigger] = []
        seen = set()
        for row in pending:
            try:
                cfg = ZoneConfig.from_row(row, self.settings, self.class_names)
            except Exception as e:
                logger.warning(f"[no-entry-zone] skipping malformed zone {row.get('zone_id')}: {e}")
                continue
            seen.add(cfg.zone_id)
            existing = state.evaluators.get(cfg.zone_id)
            if existing is None:
                state.evaluators[cfg.zone_id] = ZoneEvaluator(cfg, self.settings)
            else:
                existing.update_config(cfg)
        for zone_id in list(state.evaluators):
            if zone_id not in seen:
                # Deleted zone: close its open events instead of silently dropping them.
                closing.extend(state.evaluators.pop(zone_id).close_all(ts, wall))
        return closing

    # ------------------------------------------------------------------ frame
    def process_frame(self, stream_id: str, frame: np.ndarray) -> Dict[str, Any]:
        """Advance one frame. Returns::

            {"triggers": [trigger dict, ...],           # entry and exit, in order
             "snapshots": {event_uuid: jpeg bytes},     # one per entry trigger
             "clips": [{"event_uuid", "frames", "fps"}]}  # completed evidence clips
        """
        out: Dict[str, Any] = {"triggers": [], "snapshots": {}, "clips": []}
        if not self.is_loaded:
            return out

        with self._lock:
            state = self._streams.get(stream_id)
        if state is None:
            return out
        if state.tracker is None:
            from app.services.nez.tracker import StreamTracker
            state.tracker = StreamTracker()

        ts = time.monotonic()
        wall = datetime.now(timezone.utc)
        height, width = frame.shape[:2]
        frame_size = (width, height)

        triggers: List[ViolationTrigger] = self._apply_pending(state, ts, wall)

        armed = [ev for ev in state.evaluators.values() if ev.is_armed(wall)]
        wanted_classes = set()
        for ev in armed:
            wanted_classes |= ev.config.target_classes

        detections = []
        if armed:
            try:
                with self._model_lock:
                    raw = self.model.predict(frame)
                    raw_dets = self.model.postprocess(raw, (height, width))
                if wanted_classes:
                    raw_dets = [d for d in raw_dets if int(d["class_id"]) in wanted_classes]
                detections = state.tracker.update(raw_dets, self.class_names)
            except Exception as e:
                logger.warning(f"no-entry-zone inference failed for {stream_id}: {e}")
                detections = []

        for ev in list(state.evaluators.values()):
            result = ev.evaluate(detections, ts=ts, wall=wall, frame_size=frame_size)
            triggers.extend(result.triggers)

        # ---- evidence -------------------------------------------------------
        needs_frames = bool(armed) or bool(state.clip_jobs)
        small = None
        if needs_frames:
            small = self._annotate(
                _resize_max_width(frame, CLIP_MAX_WIDTH), frame_size, state, detections, None
            )
            pre = float(getattr(config, "no_entry_zone_clip_pre_seconds", 3.0))
            state.preroll.append((ts, small))
            while state.preroll and ts - state.preroll[0][0] > pre:
                state.preroll.popleft()
            for job in state.clip_jobs:
                job.frames.append((ts, small))

        post = float(getattr(config, "no_entry_zone_clip_post_seconds", 4.0))
        for t in triggers:
            out["triggers"].append(_trigger_dict(t))
            if t.kind != "entry":
                continue
            try:
                snap = self._annotate(frame.copy(), frame_size, state, detections, t)
                snap = _resize_max_width(snap, SNAPSHOT_MAX_WIDTH)
                ok, buf = cv2.imencode(".jpg", snap, [cv2.IMWRITE_JPEG_QUALITY, 85])
                if ok:
                    out["snapshots"][str(t.event_id)] = buf.tobytes()
            except Exception as e:
                logger.warning(f"[no-entry-zone] snapshot failed: {e}")
            if small is not None:
                state.clip_jobs.append(
                    _ClipJob(event_uuid=str(t.event_id), frames=list(state.preroll), until_ts=ts + post)
                )

        out["clips"] = self._pop_finished_clips(state, ts, force=False)
        return out

    def release_stream(self, stream_id: str) -> Dict[str, Any]:
        """Stream stopped (or the camera was switched off): close every open event,
        flush partial clips and forget the stream."""
        with self._lock:
            state = self._streams.pop(stream_id, None)
            self._active_zone_count.pop(stream_id, None)
        out: Dict[str, Any] = {"triggers": [], "snapshots": {}, "clips": [], "released": True}
        if state is None:
            return out
        ts = time.monotonic()
        wall = datetime.now(timezone.utc)
        for ev in state.evaluators.values():
            out["triggers"].extend(_trigger_dict(t) for t in ev.close_all(ts, wall))
        out["clips"] = self._pop_finished_clips(state, ts, force=True)
        return out

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _pop_finished_clips(state: _StreamState, ts: float, force: bool) -> List[Dict[str, Any]]:
        done, keep = [], []
        for job in state.clip_jobs:
            (done if force or ts >= job.until_ts else keep).append(job)
        state.clip_jobs = keep
        clips = []
        for job in done:
            if len(job.frames) < 2:
                continue
            span = job.frames[-1][0] - job.frames[0][0]
            fps = (len(job.frames) - 1) / span if span > 0 else 5.0
            clips.append({
                "event_uuid": job.event_uuid,
                "frames": [f for _, f in job.frames],
                "fps": max(1.0, min(30.0, fps)),
            })
        return clips

    def _annotate(
        self,
        img: np.ndarray,
        source_size: Tuple[int, int],
        state: _StreamState,
        detections,
        trigger: Optional[ViolationTrigger],
    ) -> np.ndarray:
        """Draw zones and tracked boxes. ``img`` may be a resized copy of the frame the
        detections were made on (``source_size``); coordinates are rescaled."""
        h, w = img.shape[:2]
        sx, sy = w / float(source_size[0]), h / float(source_size[1])
        for ev in state.evaluators.values():
            if not ev.config.enabled:
                continue
            try:
                pts = to_numpy_polygon(scale_polygon(ev.config.polygon, ev.config.ref_size, (w, h)))
            except ValueError:
                continue
            hit = trigger is not None and ev.config.zone_id == trigger.zone_id
            cv2.polylines(img, [pts], True, _RED if hit else _AMBER, 2)
        for det in detections:
            x1, y1, x2, y2 = det.xyxy
            p1 = (int(x1 * sx), int(y1 * sy))
            p2 = (int(x2 * sx), int(y2 * sy))
            is_hit = trigger is not None and det.track_id == trigger.track_id
            color = _RED if is_hit else _GREEN
            cv2.rectangle(img, p1, p2, color, 2)
            cv2.putText(
                img, f"{det.class_name} #{det.track_id}", (p1[0], max(12, p1[1] - 6)),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1, cv2.LINE_AA,
            )
        if trigger is not None:
            label = f"NO-ENTRY: {trigger.zone_name}"
            cv2.putText(img, label, (10, 28), cv2.FONT_HERSHEY_SIMPLEX, 0.8, _RED, 2, cv2.LINE_AA)
        return img


# Global singleton, mirrors shoplifting_engine
no_entry_zone_engine = NoEntryZoneEngine()
