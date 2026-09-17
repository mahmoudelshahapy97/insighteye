"""Blocked-exit detection engine.

Ported from features/blocked&exit's zone_based strategy: a detector finds
obstacles, whatever overlaps the operator's door polygon reduces the
accessible area, and a debounced state machine turns that into an event.
Box-based occupancy (not segmentation masks) since the generic ModelFactory
loaders used in production only return bounding boxes.
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple

import cv2
import numpy as np

from app.config.settings import config
from app.services.model_loader import ModelFactory, ModelBackend

logger = logging.getLogger(__name__)

# How immovable each obstacle class is, 0..1 (ported from blocked&exit's risk.py).
CLASS_WEIGHTS: Dict[str, float] = {
    "person": 0.35, "backpack": 0.45, "handbag": 0.45, "suitcase": 0.65,
    "chair": 0.70, "bench": 0.80, "potted plant": 0.60, "couch": 0.95,
    "bed": 0.95, "dining table": 0.90, "refrigerator": 1.0, "tv": 0.70,
    "laptop": 0.30, "bicycle": 0.75, "motorcycle": 0.90,
}
DEFAULT_CLASS_WEIGHT = 0.6
PERSON_CLASS_ID = 0

_RISK_ACTIONS = {
    "low": "No action required",
    "medium": "Inspect the exit and remove the obstruction",
    "high": "Dispatch staff to clear the exit",
    "critical": "Clear exit immediately",
}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _polygon_to_mask(polygon: List[List[float]], width: int, height: int) -> np.ndarray:
    mask = np.zeros((height, width), dtype=np.uint8)
    points = np.round(np.asarray(polygon, dtype=np.float32)).astype(np.int32)
    cv2.fillPoly(mask, [points], 1)
    return mask


def _bbox_to_mask(bbox: List[float], width: int, height: int) -> np.ndarray:
    mask = np.zeros((height, width), dtype=np.uint8)
    x1, y1, x2, y2 = (int(round(v)) for v in bbox)
    x1, x2 = max(0, min(x1, width)), max(0, min(x2, width))
    y1, y2 = max(0, min(y1, height)), max(0, min(y2, height))
    if x2 > x1 and y2 > y1:
        mask[y1:y2, x1:x2] = 1
    return mask


def _occupancy(region: np.ndarray, obstacle_masks: List[np.ndarray]) -> float:
    region_area = int(np.count_nonzero(region))
    if region_area == 0 or not obstacle_masks:
        return 0.0
    union = obstacle_masks[0].copy()
    for m in obstacle_masks[1:]:
        union = cv2.bitwise_or(union, m)
    overlap = int(np.count_nonzero(cv2.bitwise_and(region, union)))
    return overlap / region_area


class _StreamState:
    __slots__ = ("blocked_since", "last_state", "last_event_at")

    def __init__(self) -> None:
        self.blocked_since: Optional[float] = None
        self.last_state: str = "clear"
        self.last_event_at: float = 0.0


class BlockedExitEngine:
    """Per-stream door-polygon config + obstacle detection, following the
    StreamAwareInferenceEngine pattern used by the shoplifting engine."""

    def __init__(self) -> None:
        self.is_loaded = False
        self.model = None
        self._configs: Dict[str, Dict[str, Any]] = {}   # stream_id -> config row
        self._states: Dict[str, _StreamState] = {}
        self.min_accessibility_default = 50.0
        self.debounce_seconds_default = 5.0
        self.event_cooldown_seconds = 60.0

    def load_models(self) -> bool:
        if self.is_loaded:
            return True
        try:
            model_path = config.blocked_exit_model_path
            self.model = ModelFactory.create_loader(
                model_path,
                backend=ModelBackend(config.model_backend),
                confidence_threshold=getattr(config, "blocked_exit_confidence", 0.4),
            )
            self.model.load_model()
            self.is_loaded = True
            logger.info("BlockedExitEngine model loaded from %s", model_path)
            return True
        except Exception as e:
            logger.error(f"Failed to load blocked-exit model: {e}", exc_info=True)
            self.is_loaded = False
            return False

    def set_zone_config(self, stream_id: str, cfg: Optional[Dict[str, Any]]) -> None:
        """Cache a camera's door-polygon config; called on load and hot-reload."""
        if cfg is None:
            self._configs.pop(stream_id, None)
        else:
            self._configs[stream_id] = cfg

    def has_config(self, stream_id: str) -> bool:
        return stream_id in self._configs and self._configs[stream_id].get("door_polygon")

    def process_frame(self, stream_id: str, frame: np.ndarray) -> Optional[Dict[str, Any]]:
        """Returns a dict describing the current state, or None if no polygon
        is configured / the model isn't loaded / no event should fire yet."""
        cfg = self._configs.get(stream_id)
        if not cfg or not cfg.get("door_polygon"):
            return None
        if not self.is_loaded:
            return None

        height, width = frame.shape[:2]
        door_mask = _polygon_to_mask(cfg["door_polygon"], width, height)
        door_area = int(np.count_nonzero(door_mask))
        if door_area == 0:
            return None

        try:
            raw = self.model.predict(frame)
            detections = self.model.postprocess(raw, (height, width))
        except Exception as e:
            logger.warning(f"blocked-exit inference failed for {stream_id}: {e}")
            return None

        obstacle_masks: List[np.ndarray] = []
        blocking_objects: List[str] = []
        people_count = 0
        for det in detections:
            mask = _bbox_to_mask(det["bbox"], width, height)
            overlap = int(np.count_nonzero(cv2.bitwise_and(door_mask, mask)))
            if overlap <= 0:
                continue
            class_id = det["class_id"]
            label = self._class_label(class_id)
            if class_id == PERSON_CLASS_ID:
                people_count += 1
            obstacle_masks.append(mask)
            blocking_objects.append(label)

        occupied_pct = _occupancy(door_mask, obstacle_masks) * 100.0
        accessibility_pct = round(_clamp(100.0 - occupied_pct), 2)

        min_accessibility = float(cfg.get("min_accessibility_pct") or self.min_accessibility_default)
        debounce_seconds = float(cfg.get("debounce_seconds") or self.debounce_seconds_default)

        if accessibility_pct >= min_accessibility:
            state = "clear"
        elif accessibility_pct <= min_accessibility / 2:
            state = "blocked"
        else:
            state = "partially_blocked"

        stream_state = self._states.setdefault(stream_id, _StreamState())
        now = time.monotonic()

        if state == "clear":
            stream_state.blocked_since = None
            stream_state.last_state = state
            return {"state": state, "accessibility_pct": accessibility_pct, "alert": False}

        if stream_state.blocked_since is None:
            stream_state.blocked_since = now
        blocked_duration = now - stream_state.blocked_since
        stream_state.last_state = state

        alert = False
        if blocked_duration >= debounce_seconds and (now - stream_state.last_event_at) >= self.event_cooldown_seconds:
            alert = True
            stream_state.last_event_at = now

        risk_score, risk_level = self._assess_risk(state, accessibility_pct, blocked_duration, blocking_objects, people_count)

        return {
            "state": state,
            "accessibility_pct": accessibility_pct,
            "blocking_objects": sorted(set(blocking_objects)),
            "risk_score": risk_score,
            "risk_level": risk_level,
            "recommended_action": _RISK_ACTIONS[risk_level],
            "alert": alert,
        }

    def _class_label(self, class_id: int) -> str:
        # ModelFactory loaders don't expose COCO class names; fall back to id.
        names = getattr(config, "coco_class_names", None)
        if names and 0 <= class_id < len(names):
            return names[class_id]
        return str(class_id)

    def _assess_risk(
        self, state: str, accessibility_pct: float, blocked_duration_s: float,
        blocking_objects: List[str], people_count: int,
    ) -> Tuple[float, str]:
        duration_factor = _clamp(blocked_duration_s / 120.0, 0.0, 1.0)
        people_factor = _clamp(people_count / 5.0, 0.0, 1.0)
        class_factor = max(
            (CLASS_WEIGHTS.get(label.lower(), DEFAULT_CLASS_WEIGHT) for label in blocking_objects),
            default=0.0,
        )
        score = (
            0.45 * _clamp(100.0 - accessibility_pct)
            + 0.25 * duration_factor * 100.0
            + 0.15 * people_factor * 100.0
            + 0.15 * class_factor * 100.0
        )
        score = round(_clamp(score), 1)

        if state == "blocked" and score >= 60:
            level = "critical"
        elif score >= 80:
            level = "critical"
        elif score >= 55:
            level = "high"
        elif score >= 30:
            level = "medium"
        else:
            level = "medium" if state != "clear" else "low"
        return score, level


# Global singleton, mirrors shoplifting_engine
blocked_exit_engine = BlockedExitEngine()
