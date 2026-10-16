"""Lost & found geometry, colour, cropping, visual-likeness and match-rule tests
(ported from the standalone app's test_utils.py)."""

from datetime import datetime, timedelta, timezone

import cv2
import numpy as np
import pytest

from app.services.lost_found.matching import collapse_repeat_sightings, score_candidate
from app.services.lost_found.color import color_agreement, color_from_text, dominant_color
from app.services.lost_found.geometry import (
    box_gap,
    box_in_polygon,
    centroid,
    centroid_distance,
    clamp_box,
    iou,
    point_in_polygon,
)
from app.services.lost_found import visual as visual_similarity
from app.services.lost_found.imaging import crop_box


# ------------------------------------------------------------------- geometry
def test_centroid():
    assert centroid((0.0, 0.0, 10.0, 20.0)) == (5.0, 10.0)


def test_iou_identical_boxes_is_one():
    box = (0.0, 0.0, 10.0, 10.0)
    assert iou(box, box) == pytest.approx(1.0)


def test_iou_disjoint_boxes_is_zero():
    assert iou((0.0, 0.0, 10.0, 10.0), (50.0, 50.0, 60.0, 60.0)) == 0.0


def test_iou_half_overlap():
    assert iou((0.0, 0.0, 10.0, 10.0), (5.0, 0.0, 15.0, 10.0)) == pytest.approx(1 / 3)


def test_box_gap_is_zero_when_boxes_overlap():
    assert box_gap((0.0, 0.0, 10.0, 10.0), (5.0, 5.0, 15.0, 15.0)) == 0.0


def test_box_gap_measures_edge_distance():
    # A tall person next to a small bag: edges are 10 px apart even though the
    # centroids are much further, which is why owner association uses the gap.
    assert box_gap((0.0, 0.0, 10.0, 10.0), (20.0, 0.0, 30.0, 100.0)) == pytest.approx(10.0)
    assert centroid_distance((0.0, 0.0, 10.0, 10.0), (20.0, 0.0, 30.0, 100.0)) > 45.0


def test_point_in_polygon():
    square = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
    assert point_in_polygon((5.0, 5.0), square)
    assert not point_in_polygon((15.0, 5.0), square)


def test_degenerate_polygon_accepts_everything():
    assert point_in_polygon((999.0, 999.0), [])
    assert point_in_polygon((999.0, 999.0), [(0.0, 0.0), (1.0, 1.0)])


def test_box_in_polygon_uses_the_centroid():
    square = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
    assert box_in_polygon((4.0, 4.0, 6.0, 6.0), square)
    assert not box_in_polygon((40.0, 40.0, 60.0, 60.0), square)


def test_clamp_box_keeps_crops_inside_the_frame():
    clamped = clamp_box((-20.0, -30.0, 900.0, 900.0), 640, 480)
    x1, y1, x2, y2 = clamped
    assert (x1, y1) == (0.0, 0.0)
    assert x2 <= 640 and y2 <= 480


def test_clamp_box_never_returns_an_empty_box():
    x1, y1, x2, y2 = clamp_box((100.0, 100.0, 100.0, 100.0), 640, 480)
    assert x2 > x1 and y2 > y1


# --------------------------------------------------------------------- colour
def test_dominant_color_of_a_solid_blue_image():
    image = np.zeros((64, 64, 3), dtype=np.uint8)
    image[:, :] = (255, 0, 0)  # BGR blue
    assert dominant_color(image) == "blue"


def test_dominant_color_of_a_black_image():
    assert dominant_color(np.zeros((32, 32, 3), dtype=np.uint8)) == "black"


def test_dominant_color_of_an_empty_image_is_none():
    assert dominant_color(np.zeros((0, 0, 3), dtype=np.uint8)) is None


def test_color_agreement():
    assert color_agreement("blue", "blue")
    assert color_agreement("black", "gray")       # neutrals are interchangeable
    assert not color_agreement("blue", "red")
    assert not color_agreement(None, "blue")


def test_color_from_text():
    assert color_from_text("black leather bag") == "black"
    assert color_from_text("Dark-Blue umbrella") == "blue"
    assert color_from_text("grey umbrella") == "gray"
    assert color_from_text("leather bag") is None
    assert color_from_text(None) is None


# -------------------------------------------------------------------- crops
def test_crop_context_ratio_widens_the_crop():
    frame = np.zeros((400, 400, 3), dtype=np.uint8)
    tight = crop_box(frame, (100.0, 100.0, 200.0, 180.0))
    wide = crop_box(frame, (100.0, 100.0, 200.0, 180.0), context_ratio=0.15)
    assert tight.shape[:2] == (96, 116)   # 8 px each side
    assert wide.shape[:2] == (110, 130)   # 15 px (0.15 x 100) each side


# ------------------------------------------------------------- match rules
LOST = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


def _found(class_name="bag", color="black", hours_after=1.0, **extra):
    return {
        "class_name": class_name,
        "dominant_color": color,
        "found_at": LOST + timedelta(hours=hours_after),
        **extra,
    }


def test_type_colour_and_timing_all_count():
    best = score_candidate(_found(), "bag", "black", LOST)
    wrong_colour = score_candidate(_found(color="red"), "bag", "black", LOST)
    wrong_type = score_candidate(_found(class_name="umbrella"), "bag", "black", LOST)
    late = score_candidate(_found(hours_after=60), "bag", "black", LOST)

    assert best["same_class"] and best["same_color"]
    assert best["score"] > late["score"] > wrong_colour["score"] > wrong_type["score"]
    # 0.5 type + 0.3 colour + 0.2 x (1 - 1h / 72h)
    assert best["score"] == pytest.approx(0.5 + 0.3 + 0.2 * (1 - 1 / 72), abs=1e-4)


def test_a_relabelled_object_still_scores_on_colour_and_time():
    # The detector called the wallet a phone: colour and timing still surface it.
    match = score_candidate(_found(class_name="mobile_phone"), "wallet", "black", LOST)
    assert not match["same_class"] and match["same_color"]
    assert match["score"] >= 0.3


def test_no_loss_time_gives_neutral_time_credit():
    match = score_candidate(_found(), "bag", None, None)
    assert match["score"] == pytest.approx(0.5 + 0.2 * 0.5)


def _sighting(found_id, camera, box, hours, score=0.8):
    x1, y1, x2, y2 = box
    return {
        "found_id": found_id,
        "stream_id": camera,
        "bbox": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
        "found_at": LOST + timedelta(hours=hours),
        "score": score,
    }


def test_repeat_detections_in_the_same_spot_fold_into_one():
    kept = collapse_repeat_sightings(
        [
            _sighting("a", "cam1", (100, 100, 200, 200), hours=1),
            _sighting("b", "cam1", (104, 102, 204, 198), hours=5),   # same spot
            _sighting("c", "cam1", (400, 100, 500, 200), hours=2),   # elsewhere
        ],
        min_iou=0.6,
    )
    assert [k["found_id"] for k in kept] == ["a", "c"]
    assert kept[0]["sightings"] == 2
    assert kept[0]["last_seen_at"] == LOST + timedelta(hours=5)
    assert "bbox" not in kept[0]


def test_same_spot_on_different_cameras_stays_separate():
    kept = collapse_repeat_sightings(
        [
            _sighting("a", "cam1", (100, 100, 200, 200), hours=1),
            _sighting("b", "cam2", (100, 100, 200, 200), hours=1),
        ],
        min_iou=0.6,
    )
    assert [k["found_id"] for k in kept] == ["a", "b"]


# --------------------------------------------------------------- photo likeness
def _bag(color, size=(120, 90), stripe=None):
    """A synthetic bag: a filled rounded body on a light floor, optionally striped."""
    image = np.full((size[0], size[1], 3), 210, np.uint8)
    image[20:110, 10:80] = color
    if stripe is not None:
        image[55:65, 10:80] = stripe
    return image


def test_perceptual_hash_ignores_scale_and_brightness():
    bag = _bag((60, 30, 120), stripe=(240, 240, 240))
    smaller = cv2.resize(bag, (45, 60))
    darker = (bag * 0.7).astype(np.uint8)
    h = visual_similarity.perceptual_hash(bag)
    assert visual_similarity.hash_similarity(h, visual_similarity.perceptual_hash(smaller)) > 0.8
    assert visual_similarity.hash_similarity(h, visual_similarity.perceptual_hash(darker)) > 0.8
    assert visual_similarity.hash_similarity(h, h) == 1.0


def test_colour_histogram_tells_a_purple_bag_from_a_black_one():
    purple = visual_similarity.color_histogram(_bag((160, 40, 120)))
    lighter_purple = visual_similarity.color_histogram(_bag((185, 60, 140)))
    black = visual_similarity.color_histogram(_bag((20, 20, 20)))
    assert visual_similarity.histogram_similarity(purple, lighter_purple) > visual_similarity.histogram_similarity(
        purple, black
    )


def test_features_round_trip_through_bytes():
    features = visual_similarity.extract(_bag((160, 40, 120)))
    restored = visual_similarity.histogram_from_bytes(features.histogram_bytes)
    assert np.allclose(restored, features.histogram)
    assert -(2**63) <= features.phash < 2**63  # fits a Postgres BIGINT


def test_photo_likeness_ranks_the_same_bag_first():
    photo = visual_similarity.extract(_bag((160, 40, 120), stripe=(240, 240, 240)))
    same = visual_similarity.extract(cv2.resize(_bag((150, 35, 115), stripe=(230, 230, 230)), (60, 80)))
    other = visual_similarity.extract(_bag((30, 120, 30)))

    def likeness(item):
        return visual_similarity.similarity(photo.phash, photo.histogram, item.phash, item.histogram, 0.3)

    assert likeness(same) > likeness(other)


def test_score_without_photo_is_unchanged_and_with_one_is_a_weighted_mean():
    without = score_candidate(_found(), "bag", "black", LOST)
    with_photo = score_candidate(_found(), "bag", "black", LOST, visual=1.0)
    weak_photo = score_candidate(_found(), "bag", "black", LOST, visual=0.0)

    assert without["visual_similarity"] is None
    assert without["score"] == pytest.approx(0.5 + 0.3 + 0.2 * (1 - 1 / 72), abs=1e-4)
    assert with_photo["score"] == pytest.approx((0.5 + 0.3 + 0.2 * (1 - 1 / 72) + 0.3) / 1.3, abs=1e-4)
    assert weak_photo["score"] < without["score"] < with_photo["score"]


def test_item_without_a_crop_counts_as_neutral_when_the_report_has_a_photo():
    unseen = score_candidate(_found(), "bag", "black", LOST, visual=None, has_photo=True)
    alike = score_candidate(_found(), "bag", "black", LOST, visual=0.8, has_photo=True)
    assert unseen["visual_similarity"] is None
    assert unseen["score"] == pytest.approx((0.5 + 0.3 + 0.2 * (1 - 1 / 72) + 0.3 * 0.5) / 1.3, abs=1e-4)
    assert alike["score"] > unseen["score"]
