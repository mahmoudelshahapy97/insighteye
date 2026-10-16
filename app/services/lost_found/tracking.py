# app/services/lost_found/tracking.py (ported from lost-found app/services/tracking_service.py)
"""Per-stream tracking for lost & found, over the backend-agnostic ModelFactory detections.

The engine's model is shared by every stream, so tracking lives here, one tracker per
stream (like ``app/services/nez/tracker.py``). ByteTrack only reports confirmed tracks:
a small bag on the floor scoring 0.1-0.3 flickers in and out, never confirms, and so
never reached the abandonment logic although the detector kept seeing it. Object
detections the tracker leaves unmatched are followed here by box overlap instead -
reliable precisely for the still objects that matter.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import yaml

from app.config.settings import config
from app.services.lost_found.geometry import Box, iou

TRACKER_CFG = Path(__file__).with_name("bytetrack_lostfound.yaml")
PERSON_CLASS_ID = 0


@dataclass
class Detection:
    track_id: int
    class_id: int
    class_name: str
    confidence: float
    box: Box

    @property
    def is_person(self) -> bool:
        return self.class_id == PERSON_CLASS_ID


def _new_tracker() -> Any:
    from ultralytics.trackers.byte_tracker import BYTETracker
    from ultralytics.utils import IterableSimpleNamespace

    with open(TRACKER_CFG, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    return BYTETracker(IterableSimpleNamespace(**cfg))


class StreamObjectTracker:
    """ByteTrack plus loose IoU tracking for one stream. Not thread-safe: one per stream."""

    def __init__(self, class_names: Sequence[str], name_map: Mapping[str, str]) -> None:
        self._class_names = list(class_names)
        self._name_map = dict(name_map)
        self._tracker = _new_tracker()
        # Unconfirmed still objects: negative id -> (box, last frame seen).
        self._loose: Dict[int, Tuple[Box, int]] = {}
        self._next_loose_id = -1
        self._frame = 0

    def update(self, raw: Sequence[Dict[str, Any]]) -> List[Detection]:
        """``raw`` is ModelFactory.postprocess output: [{bbox, confidence, class_id}]."""
        from app.services.nez.tracker import _DetResults

        if raw:
            xyxy = np.asarray([d["bbox"] for d in raw], dtype=np.float32).reshape(-1, 4)
            conf = np.asarray([d["confidence"] for d in raw], dtype=np.float32)
            cls = np.asarray([d["class_id"] for d in raw], dtype=np.float32)
        else:
            xyxy = np.zeros((0, 4), dtype=np.float32)
            conf = np.zeros((0,), dtype=np.float32)
            cls = np.zeros((0,), dtype=np.float32)

        self._frame += 1
        # Called even with no detections: that is what ages tracks into "lost".
        tracks = self._tracker.update(_DetResults(xyxy, conf, cls))

        detections: List[Detection] = []
        matched = set()
        for row in tracks:
            idx = int(row[-1])
            matched.add(idx)
            detections.append(self._detection(int(row[4]), int(cls[idx]), conf[idx], xyxy[idx]))

        loose_iou = config.lost_found_loose_track_iou
        tracked_boxes = [d.box for d in detections]
        for idx in range(len(xyxy)):
            cid = int(cls[idx])
            if idx in matched or cid == PERSON_CLASS_ID:
                continue
            box = tuple(float(v) for v in xyxy[idx])
            # Overlapping a box already reported this frame: the same object twice.
            if any(iou(box, other) >= loose_iou for other in tracked_boxes):
                continue
            tracked_boxes.append(box)
            detections.append(self._detection(self._loose_id(box), cid, conf[idx], xyxy[idx]))

        self._forget_loose()
        return detections

    def _detection(self, track_id: int, cid: int, confidence: float, box: Any) -> Detection:
        name = self._class_names[cid] if 0 <= cid < len(self._class_names) else str(cid)
        return Detection(
            track_id=track_id,
            class_id=cid,
            class_name=self._name_map.get(name, name),
            confidence=float(confidence),
            box=(float(box[0]), float(box[1]), float(box[2]), float(box[3])),
        )

    def _loose_id(self, box: Box) -> int:
        """Same id as the overlapping loose object seen recently, else a new one."""
        best, best_iou = None, config.lost_found_loose_track_iou
        for track_id, (other, _) in self._loose.items():
            overlap = iou(box, other)
            if overlap >= best_iou:
                best, best_iou = track_id, overlap
        if best is None:
            best = self._next_loose_id
            self._next_loose_id -= 1
        self._loose[best] = (box, self._frame)
        return best

    def _forget_loose(self) -> None:
        # Short on purpose: this only bridges flicker. Longer gaps of a still
        # object are bridged by the abandonment tracker, which checks the object
        # was still; reusing an id here for whatever appears at a spot many
        # seconds later handed a carried bag's owners to a different object.
        horizon = config.lost_found_loose_track_max_gap_frames
        for track_id in [k for k, (_, seen) in self._loose.items() if self._frame - seen > horizon]:
            del self._loose[track_id]

    def reset(self) -> None:
        """Drop tracker state, e.g. after a stream reconnect where ids are meaningless."""
        self._tracker.reset()
        self._loose.clear()
        self._next_loose_id = -1
        self._frame = 0
