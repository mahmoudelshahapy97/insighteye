"""ClipRecorder pre-roll / finish logic and the ffmpeg encoder shared by the engines."""

import shutil

import numpy as np
import pytest

from app.services.evidence_clip import ClipRecorder, encode_mp4, resize_max_width


def frame(v=0, w=64, h=48):
    return np.full((h, w, 3), v, dtype=np.uint8)


def test_preroll_keeps_only_the_last_seconds():
    r = ClipRecorder()
    for t in range(6):
        r.push(float(t), frame(t), pre_seconds=2.0)
    assert [ts for ts, _ in r.preroll] == [3.0, 4.0, 5.0]


def test_clip_starts_with_preroll_and_finishes_after_post():
    r = ClipRecorder()
    for t in range(3):
        r.push(float(t), frame(t), pre_seconds=10)
    r.start("ep-1", 2.0, post_seconds=2.0)
    assert r.busy
    r.push(3.0, frame(3), pre_seconds=10)
    assert r.pop_finished(3.0) == []
    r.push(4.0, frame(4), pre_seconds=10)
    [clip] = r.pop_finished(4.0)
    assert clip["key"] == "ep-1" and len(clip["frames"]) == 5
    assert clip["fps"] == pytest.approx(1.0)
    assert not r.busy


def test_force_flushes_and_tiny_clips_are_dropped():
    r = ClipRecorder()
    r.start("empty", 0.0, post_seconds=10)
    r.push(0.0, frame(), pre_seconds=1)
    assert r.pop_finished(0.0, force=True) == []     # a single frame is not a clip
    r = ClipRecorder()
    r.start("short", 0.0, post_seconds=10)
    r.push(0.01, frame(), pre_seconds=1)
    r.push(0.02, frame(), pre_seconds=1)
    [clip] = r.pop_finished(0.02, force=True)
    assert clip["fps"] == 30.0                        # clamped


def test_resize_max_width_keeps_aspect_and_small_frames():
    assert resize_max_width(frame(w=2000, h=1000), 960).shape[:2] == (480, 960)
    small = frame()
    assert resize_max_width(small, 960) is small


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not installed")
def test_encode_mp4_writes_a_file(tmp_path):
    out = tmp_path / "c.mp4"
    assert encode_mp4([frame(v, w=65, h=49) for v in range(0, 250, 25)], 5.0, str(out))
    assert out.stat().st_size > 0
