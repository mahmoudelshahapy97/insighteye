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
    """Temporal state for one camera.

    An episode opens once an obstruction has persisted for debounce_seconds and
    closes once the exit has stayed clear for clear_hold_seconds, so a person
    walking past never opens one and a momentary gap never closes one.
    """

    __slots__ = (
        "blocked_since", "clear_since", "episode_open", "episode_state",
        "episode_started", "last_update_at",
    )

    def __init__(self) -> None:
        self.blocked_since: Optional[float] = None
        self.clear_since: Optional[float] = None
        self.episode_open: bool = False
        self.episode_state: Optional[str] = None
        self.episode_started: Optional[float] = None
        self.last_update_at: float = 0.0

    def reset_timers(self) -> None:
        self.blocked_since = None
        self.clear_since = None


class BlockedExitEngine:
    """Per-stream door-polygon config + obstacle detection, following the
    StreamAwareInferenceEngine pattern used by the shoplifting engine."""

    def __init__(self) -> None:
        self.is_loaded = False
        self.model = None
        self._configs: Dict[str, Dict[str, Any]] = {}   # stream_id -> config row
        self._states: Dict[str, _StreamState] = {}
        self._mask_cache: Dict[str, Tuple[Tuple[int, int], np.ndarray]] = {}
        self.min_accessibility_default = 50.0
        self.debounce_seconds_default = 5.0
        self.clear_hold_seconds = 3.0
        self.update_interval_seconds = 10.0

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
        """Cache a camera's door-polygon config; called on load and hot-reload.

        Inactive or polygon-less configs are dropped so has_config() gates them
        out. Debounce timers restart against the new polygon, but an episode
        already open is kept so it can still be closed normally.
        """
        self._mask_cache.pop(stream_id, None)
        if cfg is None or not cfg.get("door_polygon") or cfg.get("is_active") is False:
            self._configs.pop(stream_id, None)
        else:
            self._configs[stream_id] = cfg
        state = self._states.get(stream_id)
        if state is not None:
            state.reset_timers()

    def clear_stream(self, stream_id: str) -> None:
        """Forget everything about a stream (called when it stops)."""
        self._configs.pop(stream_id, None)
        self._states.pop(stream_id, None)
        self._mask_cache.pop(stream_id, None)

    def has_config(self, stream_id: str) -> bool:
        return stream_id in self._configs and bool(self._configs[stream_id].get("door_polygon"))

    @staticmethod
    def _scale_polygon(
        polygon: List[List[float]], ref_w: Optional[int], ref_h: Optional[int], width: int, height: int,
    ) -> List[List[float]]:
        """Map a polygon drawn on a ref_w x ref_h calibration frame onto the live frame."""
        if not ref_w or not ref_h or ref_w <= 0 or ref_h <= 0:
            return polygon
        sx, sy = width / ref_w, height / ref_h
        return [[x * sx, y * sy] for x, y in polygon]

    def _door_mask(self, stream_id: str, cfg: Dict[str, Any], width: int, height: int) -> np.ndarray:
        cached = self._mask_cache.get(stream_id)
        if cached and cached[0] == (width, height):
            return cached[1]
        polygon = self._scale_polygon(
            cfg["door_polygon"], cfg.get("calibration_frame_w"), cfg.get("calibration_frame_h"), width, height,
        )
        mask = _polygon_to_mask(polygon, width, height)
        self._mask_cache[stream_id] = ((width, height), mask)
        return mask

    def process_frame(
        self, stream_id: str, frame: np.ndarray, now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Returns the current reading plus the episode transition it caused.

        transition is one of None, "open", "update", "escalate", "close".
        "alert" is True on open/escalate (the moments worth notifying about).
        Returns None if no polygon is configured or the model isn't loaded.
        """
        cfg = self._configs.get(stream_id)
        if not cfg or not cfg.get("door_polygon"):
            return None
        if not self.is_loaded:
            return None

        height, width = frame.shape[:2]
        door_mask = self._door_mask(stream_id, cfg, width, height)
        if not np.count_nonzero(door_mask):
            return None

        try:
            raw = self.model.predict(frame)
            detections = self.model.postprocess(raw, (height, width))
        except Exception as e:
            logger.warning(f"blocked-exit inference failed for {stream_id}: {e}")
            return None

        obstacle_masks: List[np.ndarray] = []
        blocking: List[Tuple[str, List[float]]] = []
        people_count = 0
        for det in detections:
            mask = _bbox_to_mask(det["bbox"], width, height)
            if not np.count_nonzero(cv2.bitwise_and(door_mask, mask)):
                continue
            class_id = det["class_id"]
            if class_id == PERSON_CLASS_ID:
                people_count += 1
            obstacle_masks.append(mask)
            blocking.append((self._class_label(class_id), det["bbox"]))

        occupied_pct = _occupancy(door_mask, obstacle_masks) * 100.0
        accessibility_pct = round(_clamp(100.0 - occupied_pct), 2)
        reading = self.evaluate(
            stream_id, accessibility_pct, [label for label, _ in blocking], people_count, now=now,
        )
        if reading["transition"] == "open":
            reading["evidence_jpeg"] = self._annotate(frame, cfg, width, height, blocking, reading)
        return reading

    def evaluate(
        self,
        stream_id: str,
        accessibility_pct: float,
        blocking_objects: List[str],
        people_count: int = 0,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Classify one accessibility reading and advance the stream's episode
        state machine. Split from process_frame so it can be tested without a model."""
        cfg = self._configs.get(stream_id) or {}
        now = time.monotonic() if now is None else now
        min_accessibility = float(cfg.get("min_accessibility_pct") or self.min_accessibility_default)
        debounce_seconds = float(
            cfg["debounce_seconds"] if cfg.get("debounce_seconds") is not None else self.debounce_seconds_default
        )

        if accessibility_pct >= min_accessibility:
            state = "clear"
        elif accessibility_pct <= min_accessibility / 2:
            state = "blocked"
        else:
            state = "partially_blocked"

        st = self._states.setdefault(stream_id, _StreamState())
        transition: Optional[str] = None

        if state == "clear":
            st.blocked_since = None
            if st.episode_open:
                if st.clear_since is None:
                    st.clear_since = now
                if now - st.clear_since >= self.clear_hold_seconds:
                    transition = "close"
                    st.episode_open = False
                    st.episode_state = None
                    st.episode_started = None
                    st.clear_since = None
        else:
            st.clear_since = None
            if st.blocked_since is None:
                st.blocked_since = now
            if not st.episode_open:
                if now - st.blocked_since >= debounce_seconds:
                    transition = "open"
                    st.episode_open = True
                    st.episode_state = state
                    st.episode_started = st.blocked_since
                    st.last_update_at = now
            elif state == "blocked" and st.episode_state == "partially_blocked":
                transition = "escalate"
                st.episode_state = "blocked"
                st.last_update_at = now
            elif now - st.last_update_at >= self.update_interval_seconds:
                transition = "update"
                st.last_update_at = now

        blocked_duration = (now - st.episode_started) if st.episode_started is not None else (
            (now - st.blocked_since) if st.blocked_since is not None else 0.0
        )
        risk_score, risk_level = self._assess_risk(
            state, accessibility_pct, blocked_duration, blocking_objects, people_count,
        )
        return {
            "state": state,
            "episode_state": st.episode_state,
            "accessibility_pct": accessibility_pct,
            "blocking_objects": sorted(set(blocking_objects)),
            "risk_score": risk_score,
            "risk_level": risk_level,
            "recommended_action": _RISK_ACTIONS[risk_level],
            "blocked_duration_s": round(blocked_duration, 1),
            "transition": transition,
            "alert": transition in ("open", "escalate"),
        }

    def _annotate(
        self, frame: np.ndarray, cfg: Dict[str, Any], width: int, height: int,
        blocking: List[Tuple[str, List[float]]], reading: Dict[str, Any],
    ) -> Optional[bytes]:
        """JPEG of the frame with the door polygon and blocking boxes drawn on."""
        try:
            img = frame.copy()
            polygon = self._scale_polygon(
                cfg["door_polygon"], cfg.get("calibration_frame_w"), cfg.get("calibration_frame_h"), width, height,
            )
            pts = np.round(np.asarray(polygon, dtype=np.float32)).astype(np.int32)
            color = (0, 0, 255) if reading["state"] == "blocked" else (0, 165, 255)
            overlay = img.copy()
            cv2.fillPoly(overlay, [pts], color)
            img = cv2.addWeighted(overlay, 0.25, img, 0.75, 0)
            cv2.polylines(img, [pts], True, color, 2)
            for label, bbox in blocking:
                x1, y1, x2, y2 = (int(round(v)) for v in bbox)
                cv2.rectangle(img, (x1, y1), (x2, y2), (255, 255, 255), 2)
                cv2.putText(img, label, (x1, max(12, y1 - 6)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            caption = f"{reading['state'].replace('_', ' ')} - {reading['accessibility_pct']:.0f}% accessible"
            cv2.putText(img, caption, (10, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
            ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 85])
            return buf.tobytes() if ok else None
        except Exception as e:
            logger.warning(f"blocked-exit evidence annotation failed: {e}")
            return None

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
