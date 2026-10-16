# app/services/lost_found/matching.py (ported from lost-found app/services/matching_service.py)
"""Rule-based scoring of found items against a lost report. No models are involved.

Candidates share the report's object type or colour, and each is scored on

    same object type        lost_found_match_class_weight   (0.5)
    compatible colour       lost_found_match_color_weight   (0.3)
    found soon after loss   lost_found_match_time_weight    (0.2, fading over match_window_hours)
    looks like the photo    lost_found_match_visual_weight  (0.3, only when the report has one:
                            perceptual hash + colour histogram against the crop)

and the score is the weighted mean over the parts available. A static object
re-detected by the same camera (overlapping boxes) is folded into a single
suggestion with a sighting count.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from app.config.settings import config
from app.services.lost_found.color import color_agreement
from app.services.lost_found.geometry import iou


def _box(bbox: Any) -> Optional[tuple]:
    """lost_found_events.bbox ({"x1":..,"y1":..,"x2":..,"y2":..}) as a tuple."""
    if isinstance(bbox, str):
        try:
            bbox = json.loads(bbox)
        except ValueError:
            return None
    if not isinstance(bbox, dict):
        return None
    try:
        return (float(bbox["x1"]), float(bbox["y1"]), float(bbox["x2"]), float(bbox["y2"]))
    except (KeyError, TypeError, ValueError):
        return None


def score_candidate(
    candidate: Dict[str, Any],
    class_name: Optional[str],
    color: Optional[str],
    lost_after: Optional[datetime],
    visual: Optional[float] = None,
    has_photo: bool = False,
) -> Dict[str, Any]:
    """Weigh one found item against a report's type, colour, loss time and photo.

    ``visual`` is the photo's likeness to the item's crop (0-1). When the report
    has a photo but the item has no crop to compare (``visual`` None), the
    likeness counts as neutral: leaving it out would rank items that cannot be
    checked above items that were checked and look only fairly alike.
    """
    same_class = bool(class_name) and candidate.get("class_name") == class_name
    same_color = color_agreement(color, candidate.get("dominant_color"))

    found_at = candidate.get("found_at")
    if lost_after is not None and found_at is not None:
        hours = max(0.0, (found_at - lost_after).total_seconds() / 3600.0)
        window = max(config.lost_found_match_window_hours, 1e-6)
        time_fit = max(0.0, 1.0 - hours / window)
    else:
        # No loss time given: nothing to be close to, so half credit for all.
        time_fit = 0.5

    parts = [
        (config.lost_found_match_class_weight, float(same_class)),
        (config.lost_found_match_color_weight, float(same_color)),
        (config.lost_found_match_time_weight, time_fit),
    ]
    if visual is not None or has_photo:
        parts.append((config.lost_found_match_visual_weight, 0.5 if visual is None else visual))
    total = sum(weight for weight, _ in parts)
    score = sum(weight * value for weight, value in parts) / total if total > 0 else 0.0

    match = dict(candidate)
    match.pop("phash", None)
    match.pop("color_hist", None)
    match["same_class"] = same_class
    match["same_color"] = same_color
    match["visual_similarity"] = None if visual is None else round(visual, 4)
    match["score"] = round(min(1.0, score), 4)
    return match


def collapse_repeat_sightings(
    matches: Sequence[Dict[str, Any]], min_iou: float
) -> List[Dict[str, Any]]:
    """Fold re-detections of one static object into its best-scoring match.

    ``matches`` must be sorted best-first. A later match from the same camera
    whose box overlaps a kept one by at least ``min_iou`` is the same object
    seen again (an abandoned item does not move): it is dropped, and the kept
    match counts it in ``sightings`` / ``last_seen_at``. Object type is
    deliberately ignored - the detector flips labels on one object.
    """
    kept: List[Dict[str, Any]] = []
    boxes: List[Optional[tuple]] = []

    for match in matches:
        box = _box(match.get("bbox"))
        owner = None
        if box is not None:
            for index, other in enumerate(kept):
                if (
                    boxes[index] is not None
                    and other.get("stream_id") == match.get("stream_id")
                    and iou(box, boxes[index]) >= min_iou
                ):
                    owner = other
                    break

        if owner is None:
            match = dict(match)
            match["sightings"] = 1
            match["last_seen_at"] = match.get("found_at")
            kept.append(match)
            boxes.append(box)
            continue

        owner["sightings"] += 1
        seen = match.get("found_at")
        if seen is not None and (owner["last_seen_at"] is None or seen > owner["last_seen_at"]):
            owner["last_seen_at"] = seen

    for match in kept:
        match.pop("bbox", None)
    return kept
