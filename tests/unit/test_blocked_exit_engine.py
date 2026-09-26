"""Unit tests for the blocked-exit engine's polygon scaling and episode state machine.

evaluate() is driven with explicit timestamps, so no model or video is needed.
"""

import numpy as np
import pytest

from app.services.blocked_exit_inference import BlockedExitEngine

STREAM = "stream-1"
POLYGON = [[0, 0], [100, 0], [100, 200], [0, 200]]


@pytest.fixture
def engine():
    e = BlockedExitEngine()
    e.set_zone_config(STREAM, {
        "door_polygon": POLYGON,
        "calibration_frame_w": 640,
        "calibration_frame_h": 360,
        "min_accessibility_pct": 50,
        "debounce_seconds": 5,
        "is_active": True,
    })
    return e


def test_scale_polygon_maps_calibration_frame_to_live_frame():
    scaled = BlockedExitEngine._scale_polygon([[320, 180], [640, 360]], 640, 360, 1280, 720)
    assert scaled == [[640, 360], [1280, 720]]


def test_scale_polygon_without_calibration_size_is_identity():
    assert BlockedExitEngine._scale_polygon(POLYGON, None, None, 1280, 720) == POLYGON


def test_door_mask_is_scaled_to_frame(engine):
    mask = engine._door_mask(STREAM, engine._configs[STREAM], 1280, 720)
    # 100x200 box on a 640x360 frame doubles to 200x400 on 1280x720 (+ fillPoly edge pixels).
    assert 200 * 400 <= np.count_nonzero(mask) <= 201 * 401


def test_inactive_or_empty_config_is_not_monitored():
    e = BlockedExitEngine()
    e.set_zone_config(STREAM, {"door_polygon": POLYGON, "is_active": False})
    assert not e.has_config(STREAM)
    e.set_zone_config(STREAM, {"door_polygon": None})
    assert not e.has_config(STREAM)


def test_short_obstruction_never_opens_an_episode(engine):
    assert engine.evaluate(STREAM, 20, ["chair"], now=0)["transition"] is None
    assert engine.evaluate(STREAM, 20, ["chair"], now=4)["transition"] is None
    assert engine.evaluate(STREAM, 90, [], now=4.5)["transition"] is None
    # Timer restarted by the clear reading, so 5s after the first reading is not enough.
    assert engine.evaluate(STREAM, 20, ["chair"], now=6)["transition"] is None


def test_episode_opens_after_debounce_and_closes_after_clear_hold(engine):
    engine.evaluate(STREAM, 40, ["box"], now=0)
    opened = engine.evaluate(STREAM, 40, ["box"], now=5)
    assert opened["transition"] == "open"
    assert opened["alert"] is True
    assert opened["state"] == "partially_blocked"

    # A brief gap does not close it.
    assert engine.evaluate(STREAM, 95, [], now=6)["transition"] is None
    assert engine.evaluate(STREAM, 40, ["box"], now=7)["transition"] is None

    assert engine.evaluate(STREAM, 95, [], now=8)["transition"] is None
    closed = engine.evaluate(STREAM, 95, [], now=11)
    assert closed["transition"] == "close"
    assert closed["alert"] is False


def test_partial_to_full_escalates_once(engine):
    engine.evaluate(STREAM, 40, ["box"], now=0)
    engine.evaluate(STREAM, 40, ["box"], now=5)
    escalated = engine.evaluate(STREAM, 10, ["box", "chair"], now=6)
    assert escalated["transition"] == "escalate"
    assert escalated["alert"] is True
    assert engine.evaluate(STREAM, 10, ["box"], now=7)["transition"] is None


def test_open_episode_emits_throttled_updates(engine):
    engine.evaluate(STREAM, 10, ["box"], now=0)
    engine.evaluate(STREAM, 10, ["box"], now=5)
    assert engine.evaluate(STREAM, 10, ["box"], now=10)["transition"] is None
    assert engine.evaluate(STREAM, 10, ["box"], now=15)["transition"] == "update"


def test_config_change_keeps_open_episode_so_it_can_close(engine):
    engine.evaluate(STREAM, 10, ["box"], now=0)
    assert engine.evaluate(STREAM, 10, ["box"], now=5)["transition"] == "open"
    engine.set_zone_config(STREAM, {**engine._configs[STREAM], "min_accessibility_pct": 60})
    engine.evaluate(STREAM, 95, [], now=6)
    assert engine.evaluate(STREAM, 95, [], now=9)["transition"] == "close"


def test_risk_level_reflects_blockage(engine):
    engine.evaluate(STREAM, 0, ["refrigerator"], now=0)
    reading = engine.evaluate(STREAM, 0, ["refrigerator"], now=120)
    assert reading["state"] == "blocked"
    assert reading["risk_level"] == "critical"
