"""Lost & found (abandoned-object) detection engine.

Ported from the standalone lost-found app: YOLO detections -> per-stream ByteTrack
(plus loose IoU tracking for unconfirmed still objects) -> the abandonment state
machine -> one event per abandoned object, with an annotated alert frame, the object's
crop (colour, perceptual hash and colour histogram for matching) and an evidence clip
starting ``lost_found_clip_pre_seconds`` before the alert.

The model is shared by every stream (one singleton, like the no-entry-zone engine);
tracker, abandonment state and clip buffers are kept per stream.

Threading: ``process_frame`` runs in the stream-processing thread pool, while
``set_config`` is called from the event loop (stream start, config refresh, API
hot-reload). Config changes are queued and applied at the start of the stream's next
frame.

Memory: a 40 s pre-roll of raw frames would cost ~300 MB per camera, so the clip buffer
holds JPEG bytes (as the standalone app did); finished clips come back as JPEGs and are
decoded only when encoded to mp4.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

from app.config.settings import config
from app.services.evidence_clip import ClipRecorder, resize_max_width
from app.services.lost_found import visual
from app.services.lost_found.abandonment import AbandonmentParams, AbandonmentTracker
from app.services.lost_found.color import dominant_color
from app.services.lost_found.imaging import annotate_abandoned, annotate_tracks, crop_box, encode_jpeg
from app.services.lost_found.tracking import PERSON_CLASS_ID, StreamObjectTracker
from app.services.model_loader import ModelBackend, ModelFactory

logger = logging.getLogger(__name__)

CLIP_MAX_WIDTH = 960
CLIP_JPEG_QUALITY = 70
SNAPSHOT_MAX_WIDTH = 1280


def scale_roi(
    roi: Optional[Sequence[Sequence[float]]],
    ref_size: Optional[tuple],
    frame_size: tuple,
) -> Optional[List[tuple]]:
    """ROI drawn on a ``ref_size`` (w, h) frame, in ``frame_size`` pixels. No ref size:
    the points are already frame pixels. Fewer than three points: no ROI."""
    if not roi or len(roi) < 3:
        return None
    if not ref_size or not ref_size[0] or not ref_size[1]:
        return [(float(x), float(y)) for x, y in roi]
    sx = frame_size[0] / float(ref_size[0])
    sy = frame_size[1] / float(ref_size[1])
    return [(float(x) * sx, float(y) * sy) for x, y in roi]


def agnostic_nms(raw: List[Dict[str, Any]], iou_threshold: float) -> List[Dict[str, Any]]:
    """One box per object regardless of class (the standalone app ran YOLO with
    ``agnostic_nms=True``; the shared loaders suppress per class only)."""
    if len(raw) < 2:
        return raw
    boxes = np.asarray([d["bbox"] for d in raw], dtype=np.float32)
    scores = np.asarray([d["confidence"] for d in raw], dtype=np.float32)
    x1, y1, x2, y2 = boxes[:, 0], boxes[:, 1], boxes[:, 2], boxes[:, 3]
    areas = np.maximum(0.0, x2 - x1) * np.maximum(0.0, y2 - y1)
    order = scores.argsort()[::-1]
    keep: List[int] = []
    while order.size:
        i = int(order[0])
        keep.append(i)
        rest = order[1:]
        w = np.maximum(0.0, np.minimum(x2[i], x2[rest]) - np.maximum(x1[i], x1[rest]))
        h = np.maximum(0.0, np.minimum(y2[i], y2[rest]) - np.maximum(y1[i], y1[rest]))
        inter = w * h
        union = areas[i] + areas[rest] - inter
        overlap = np.where(union > 0, inter / np.maximum(union, 1e-9), 0.0)
        order = rest[overlap <= iou_threshold]
    return [raw[i] for i in sorted(keep)]


@dataclass
class _StreamState:
    tracker: Optional[StreamObjectTracker] = None
    abandonment: AbandonmentTracker = field(default_factory=AbandonmentTracker)
    clips: ClipRecorder = field(default_factory=ClipRecorder)
    config: Optional[Dict[str, Any]] = None
    pending_config: Optional[Dict[str, Any]] = None
    has_pending: bool = False
    frame_size: Optional[tuple] = None
    roi: Optional[List[tuple]] = None
    last_annotated: Optional[np.ndarray] = None
    last_frame_at: Optional[float] = None


class LostFoundEngine:
    """Per-stream abandoned-object detection over one shared model."""

    def __init__(self) -> None:
        self.is_loaded = False
        self.model = None
        self.class_names: List[str] = list(getattr(config, "coco_class_names", []) or [])
        self._streams: Dict[str, _StreamState] = {}
        self._lock = threading.Lock()        # guards _streams / pending config
        self._model_lock = threading.Lock()  # one inference at a time on the shared model

    # ------------------------------------------------------------------ model
    def load_models(self) -> bool:
        if self.is_loaded:
            return True
        try:
            model_path = config.lost_found_model_path
            self.model = ModelFactory.create_loader(
                model_path,
                backend=ModelBackend(config.model_backend),
                confidence_threshold=float(getattr(config, "lost_found_confidence", 0.05)),
            )
            self.model.load_model()
            self.is_loaded = True
            logger.info("LostFoundEngine model loaded from %s", model_path)
            return True
        except Exception as e:
            logger.error(f"Failed to load lost & found model: {e}", exc_info=True)
            self.is_loaded = False
            return False

    # ----------------------------------------------------------------- config
    def set_config(self, stream_id: str, cfg: Optional[Dict[str, Any]]) -> None:
        """Enable the stream with this camera's config (ROI + rule overrides; ``None``
        means defaults over the whole frame). Applied on the stream's next frame. A
        config with ``is_active`` false disables detection on the camera."""
        with self._lock:
            state = self._streams.get(stream_id)
            if state is None:
                state = _StreamState()
                self._streams[stream_id] = state
            state.pending_config = dict(cfg) if cfg else {}
            state.has_pending = True

    def reload_config(self, stream_id: str, cfg: Optional[Dict[str, Any]]) -> None:
        """API hot-reload: like ``set_config``, but only for a stream this process runs."""
        if stream_id in self._streams:
            self.set_config(stream_id, cfg)

    def has_stream(self, stream_id: str) -> bool:
        """True while lost & found runs on the stream, or still has clips to finish."""
        state = self._streams.get(stream_id)
        if state is None:
            return False
        if state.has_pending:
            return state.pending_config.get("is_active", True) is not False or state.clips.busy
        active = (state.config or {}).get("is_active", True) is not False
        return active or state.clips.busy

    def _apply_pending(self, state: _StreamState) -> None:
        with self._lock:
            if not state.has_pending:
                return
            cfg = state.pending_config or {}
            state.pending_config = None
            state.has_pending = False
        old_settings = (state.config or {}).get("settings") or {}
        new_settings = cfg.get("settings") or {}
        if state.config is None or old_settings != new_settings:
            # Keep the tracks; only the thresholds change.
            state.abandonment.params = AbandonmentParams.from_settings(new_settings)
        state.config = cfg
        state.roi = None
        state.frame_size = None  # ROI is rescaled on the next frame

    def _update_roi(self, state: _StreamState, frame_size: tuple) -> None:
        if state.frame_size == frame_size:
            return
        state.frame_size = frame_size
        cfg = state.config or {}
        ref = (cfg.get("ref_width"), cfg.get("ref_height"))
        state.roi = scale_roi(cfg.get("roi"), ref if all(ref) else None, frame_size)
        state.abandonment.roi = state.roi

    # ------------------------------------------------------------------ frame
    def process_frame(self, stream_id: str, frame: np.ndarray) -> Dict[str, Any]:
        """Advance one frame. Returns::

            {"events": [event dict, ...],                    # newly abandoned objects
             "clips": [{"event_id", "jpegs", "fps"}, ...]}   # finished evidence clips
        """
        out: Dict[str, Any] = {"events": [], "clips": []}
        if not self.is_loaded or frame is None or frame.size == 0:
            return out
        with self._lock:
            state = self._streams.get(stream_id)
        if state is None:
            return out

        self._apply_pending(state)
        now = time.time()
        if (state.config or {}).get("is_active", True) is False:
            out["clips"] = self._pop_finished_clips(state, now, force=True)
            return out
        if state.tracker is None:
            state.tracker = StreamObjectTracker(
                self.class_names, getattr(config, "lost_found_class_name_map", {}) or {},
            )

        height, width = frame.shape[:2]
        self._update_roi(state, (width, height))

        try:
            with self._model_lock:
                raw = self.model.predict(frame)
                raw_dets = self.model.postprocess(raw, (height, width))
            wanted = {PERSON_CLASS_ID, *config.lost_found_object_class_ids}
            raw_dets = [d for d in raw_dets if int(d["class_id"]) in wanted]
            raw_dets = agnostic_nms(raw_dets, float(config.lost_found_agnostic_nms_iou))
            detections = state.tracker.update(raw_dets)
        except Exception as e:
            logger.warning(f"lost & found inference failed for {stream_id}: {e}")
            detections = []

        events = state.abandonment.update(detections, now)

        # Live view and clip pre-roll: every tracked object coloured by status.
        persons = [d.box for d in detections if d.is_person]
        annotated = annotate_tracks(frame, persons, state.abandonment.snapshot(), state.roi)
        state.last_annotated = annotated
        state.last_frame_at = now
        small = resize_max_width(annotated, CLIP_MAX_WIDTH)
        jpeg = encode_jpeg(small, CLIP_JPEG_QUALITY)
        if jpeg:
            state.clips.push(now, jpeg, float(config.lost_found_clip_pre_seconds))

        post = float(config.lost_found_clip_post_seconds)
        for event in events:
            record = self._event_record(frame, event)
            out["events"].append(record)
            state.clips.start(record["event_id"], now, post)

        out["clips"] = self._pop_finished_clips(state, now, force=False)
        return out

    def _event_record(self, frame: np.ndarray, event) -> Dict[str, Any]:
        """Everything the stream service persists for one abandoned object."""
        alert = annotate_abandoned(frame, event.box, event.class_name, event.unattended_seconds)
        alert = resize_max_width(alert, SNAPSHOT_MAX_WIDTH)
        crop = crop_box(frame, event.box, context_ratio=float(config.lost_found_crop_context_ratio))
        # Features from the box itself: the context around the crop would put the
        # floor into the colour histogram.
        features = visual.extract(crop_box(frame, event.box, padding=0))
        return {
            "event_id": str(uuid.uuid4()),
            "track_key": event.track_key or str(event.track_id),
            "class_name": event.class_name,
            "confidence": round(float(event.confidence), 4),
            "bbox": {"x1": event.box[0], "y1": event.box[1], "x2": event.box[2], "y2": event.box[3]},
            "owner_track_id": event.owner_track_id,
            "first_seen_at": event.first_seen_at,
            "static_since": event.static_since,
            "abandoned_at": event.abandoned_at,
            "unattended_seconds": round(float(event.unattended_seconds), 1),
            "snapshot_jpeg": encode_jpeg(alert),
            "crop_jpeg": encode_jpeg(crop) if crop is not None else None,
            "dominant_color": dominant_color(crop) if crop is not None else None,
            "phash": features.phash if features else None,
            "color_hist": features.histogram_bytes if features else None,
        }

    def release_stream(self, stream_id: str) -> Dict[str, Any]:
        """Stream stopped (or the camera was switched off): flush partial clips and
        forget the stream."""
        with self._lock:
            state = self._streams.pop(stream_id, None)
        out: Dict[str, Any] = {"events": [], "clips": [], "released": True}
        if state is not None:
            out["clips"] = self._pop_finished_clips(state, time.time(), force=True)
        return out

    # ------------------------------------------------------------------ status
    def tracked_objects(self, stream_id: str) -> Optional[List[Dict[str, Any]]]:
        """Per-object abandonment status, or None if this process doesn't run the stream."""
        state = self._streams.get(stream_id)
        if state is None or state.last_frame_at is None:
            return None
        return state.abandonment.snapshot()

    def live_jpeg(self, stream_id: str) -> Optional[bytes]:
        """The latest annotated frame, or None if this process doesn't run the stream."""
        state = self._streams.get(stream_id)
        frame = state.last_annotated if state is not None else None
        return encode_jpeg(frame) if frame is not None else None

    @staticmethod
    def _pop_finished_clips(state: _StreamState, ts: float, force: bool) -> List[Dict[str, Any]]:
        return [
            {"event_id": c["key"], "jpegs": c["frames"], "fps": c["fps"]}
            for c in state.clips.pop_finished(ts, force=force)
        ]


# Global singleton, mirrors no_entry_zone_engine
lost_found_engine = LostFoundEngine()
