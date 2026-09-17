"""No-entry-zone (restricted polygon) detection engine.

Ported from features/no-entry-zone's ZoneEvaluator, simplified to per-zone
continuous-occupancy dwell timers (rather than per-track state) since the
generic ModelFactory loaders used in production return bare detections
without persistent track ids.
"""

import logging
import time
from typing import Dict, Any, List, Optional

import cv2
import numpy as np

from app.config.settings import config
from app.services.model_loader import ModelFactory, ModelBackend

logger = logging.getLogger(__name__)

DEFAULT_TARGET_CLASSES = {"person"}
PERSON_CLASS_ID = 0


class _ZoneState:
    __slots__ = ("occupied_since", "last_event_at")

    def __init__(self) -> None:
        self.occupied_since: Optional[float] = None
        self.last_event_at: float = 0.0


class NoEntryZoneEngine:
    """Per-stream zone config + intrusion detection, following the
    StreamAwareInferenceEngine pattern used by the shoplifting engine."""

    def __init__(self) -> None:
        self.is_loaded = False
        self.model = None
        self._zones: Dict[str, List[Dict[str, Any]]] = {}   # stream_id -> [zone rows]
        self._states: Dict[str, Dict[str, _ZoneState]] = {}  # stream_id -> {zone_id: state}
        self.event_cooldown_seconds = 60.0

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

    def set_zones(self, stream_id: str, zones: List[Dict[str, Any]]) -> None:
        """Cache a camera's active zones; called on load and hot-reload."""
        self._zones[stream_id] = [z for z in zones if z.get("is_active", True)]
        self._states.setdefault(stream_id, {})

    def reload_zones(self, stream_id: str) -> None:
        """Marker hook: callers refetch from the DB and call set_zones again."""
        self._states.pop(stream_id, None)

    def has_zones(self, stream_id: str) -> bool:
        return bool(self._zones.get(stream_id))

    def process_frame(self, stream_id: str, frame: np.ndarray) -> List[Dict[str, Any]]:
        """Returns a list of violation dicts (empty if none fired this frame)."""
        zones = self._zones.get(stream_id)
        if not zones or not self.is_loaded:
            return []

        height, width = frame.shape[:2]

        try:
            raw = self.model.predict(frame)
            detections = self.model.postprocess(raw, (height, width))
        except Exception as e:
            logger.warning(f"no-entry-zone inference failed for {stream_id}: {e}")
            return []

        centers = []
        for det in detections:
            x1, y1, x2, y2 = det["bbox"]
            cx, cy = (x1 + x2) / 2.0, y2  # foot anchor, matches the prototype's default anchor
            centers.append((cx, cy, det["class_id"]))

        stream_states = self._states.setdefault(stream_id, {})
        now = time.monotonic()
        violations: List[Dict[str, Any]] = []

        for zone in zones:
            zone_id = zone["zone_id"]
            ref_w = zone.get("ref_width") or width
            ref_h = zone.get("ref_height") or height
            scaled_polygon = self._scale_polygon(zone["polygon"], ref_w, ref_h, width, height)
            poly_pts = np.array(scaled_polygon, dtype=np.float32)
            target_classes = set(zone.get("target_classes") or DEFAULT_TARGET_CLASSES)

            occupied = False
            hit_class = None
            for cx, cy, class_id in centers:
                label = self._class_label(class_id)
                if target_classes and label not in target_classes and str(class_id) not in target_classes:
                    continue
                if len(poly_pts) >= 3 and cv2.pointPolygonTest(poly_pts, (float(cx), float(cy)), False) >= 0:
                    occupied = True
                    hit_class = label
                    break

            state = stream_states.setdefault(zone_id, _ZoneState())
            min_dwell = float(zone.get("min_dwell_seconds") or 1.0)

            if not occupied:
                state.occupied_since = None
                continue

            if state.occupied_since is None:
                state.occupied_since = now
            dwell = now - state.occupied_since

            if dwell >= min_dwell and (now - state.last_event_at) >= self.event_cooldown_seconds:
                state.last_event_at = now
                violations.append({
                    "zone_id": zone_id,
                    "zone_name": zone.get("name"),
                    "target_class": hit_class,
                    "dwell_seconds": round(dwell, 2),
                })

        return violations

    @staticmethod
    def _scale_polygon(
        polygon: List[List[float]], ref_w: int, ref_h: int, width: int, height: int,
    ) -> List[List[float]]:
        if ref_w <= 0 or ref_h <= 0:
            return polygon
        sx, sy = width / ref_w, height / ref_h
        return [[x * sx, y * sy] for x, y in polygon]

    def _class_label(self, class_id: int) -> str:
        names = getattr(config, "coco_class_names", None)
        if names and 0 <= class_id < len(names):
            return names[class_id]
        return str(class_id)


# Global singleton, mirrors shoplifting_engine
no_entry_zone_engine = NoEntryZoneEngine()
