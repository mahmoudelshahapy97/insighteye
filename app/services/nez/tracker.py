"""Per-stream ByteTrack over the backend-agnostic ModelFactory detections.

The ModelFactory loaders (pytorch/onnx/openvino/tensorrt) return bare detections with no
track ids, and the no-entry-zone engine is a process-wide singleton shared by every
stream. So tracking cannot live inside the model the way Ultralytics' ``model.track``
does it; instead each stream owns one BYTETracker fed with that stream's detections.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence

import numpy as np
import yaml

from app.services.nez.zone_logic import Detection

TRACKER_CFG = Path(__file__).with_name("bytetrack_nez.yaml")


class _DetResults:
    """The slice of ``ultralytics.engine.results.Boxes`` that BYTETracker reads."""

    def __init__(self, xyxy: np.ndarray, conf: np.ndarray, cls: np.ndarray) -> None:
        self.xyxy = xyxy
        self.conf = conf
        self.cls = cls

    @property
    def xywh(self) -> np.ndarray:
        xywh = self.xyxy.copy()
        xywh[:, 0] = (self.xyxy[:, 0] + self.xyxy[:, 2]) / 2
        xywh[:, 1] = (self.xyxy[:, 1] + self.xyxy[:, 3]) / 2
        xywh[:, 2] = self.xyxy[:, 2] - self.xyxy[:, 0]
        xywh[:, 3] = self.xyxy[:, 3] - self.xyxy[:, 1]
        return xywh

    def __len__(self) -> int:
        return len(self.conf)

    def __getitem__(self, idx) -> "_DetResults":
        return _DetResults(self.xyxy[idx], self.conf[idx], self.cls[idx])


def _load_args():
    from ultralytics.utils import IterableSimpleNamespace

    with open(TRACKER_CFG, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    return IterableSimpleNamespace(**cfg)


class StreamTracker:
    """One ByteTrack instance for one stream."""

    def __init__(self, frame_rate: int = 30) -> None:
        from ultralytics.trackers.byte_tracker import BYTETracker

        self._tracker = BYTETracker(_load_args(), frame_rate=frame_rate)

    def update(self, raw: Sequence[Dict[str, Any]], class_names: Sequence[str]) -> List[Detection]:
        """``raw`` is ModelFactory.postprocess output: [{bbox, confidence, class_id}]."""
        if raw:
            xyxy = np.asarray([d["bbox"] for d in raw], dtype=np.float32).reshape(-1, 4)
            conf = np.asarray([d["confidence"] for d in raw], dtype=np.float32)
            cls = np.asarray([d["class_id"] for d in raw], dtype=np.float32)
        else:
            xyxy = np.zeros((0, 4), dtype=np.float32)
            conf = np.zeros((0,), dtype=np.float32)
            cls = np.zeros((0,), dtype=np.float32)

        # Called even with no detections: that is what ages tracks into "lost".
        tracks = self._tracker.update(_DetResults(xyxy, conf, cls))

        detections: List[Detection] = []
        for row in tracks:
            x1, y1, x2, y2, track_id, score, class_id = row[:7]
            cid = int(class_id)
            detections.append(
                Detection(
                    track_id=int(track_id),
                    class_id=cid,
                    class_name=class_names[cid] if 0 <= cid < len(class_names) else str(cid),
                    confidence=float(score),
                    xyxy=(float(x1), float(y1), float(x2), float(y2)),
                )
            )
        return detections
