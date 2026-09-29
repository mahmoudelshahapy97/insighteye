"""Stream-processor wiring for blocked-exit evidence: episode ids, clip uploads and
confidence reach the service, including clips that finish on a frame without a transition."""

import asyncio
from uuid import uuid4

import numpy as np
import pytest

from app.services import stream_processing_service as sps
from app.services.blocked_exit_service import blocked_exit_service

STREAM = uuid4()
WS = uuid4()
FRAMES = [np.zeros((8, 8, 3), dtype=np.uint8)] * 3


@pytest.fixture
def proc(mocker):
    p = object.__new__(sps.StreamProcessingService)  # no models / threads
    p.stream_manager = None
    p.blocked_exit_engine = mocker.Mock()
    p._upload_clip = mocker.AsyncMock()
    mocker.patch.object(sps.notification_service, "create_notification", mocker.AsyncMock(return_value=None))
    return p


def _reading(**kw):
    base = {"state": "blocked", "accessibility_pct": 10.0, "blocking_objects": ["chair"], "risk_score": 70,
            "risk_level": "critical", "recommended_action": "x", "transition": None, "clips": [],
            "confidence": 0.9, "avg_confidence": 0.8, "episode_uuid": "ep1"}
    base.update(kw)
    return base


async def _drain(p):
    await asyncio.gather(*getattr(p, "_nez_tasks", set()))


async def test_open_records_confidence_and_a_later_clip_attaches_to_the_episode(proc, mocker):
    opened = mocker.patch.object(blocked_exit_service, "open_episode", mocker.AsyncMock(return_value=(41, True)))
    set_ev = mocker.patch.object(blocked_exit_service, "set_evidence", mocker.AsyncMock())

    await proc._handle_blocked_exit_alert(STREAM, str(STREAM), _reading(transition="open"), "Exit", WS, uuid4())
    kw = opened.await_args.kwargs
    assert kw["confidence"] == 0.9 and kw["avg_confidence"] == 0.8

    # the clip completes on a frame with no transition
    clip = {"key": "ep1", "frames": FRAMES, "fps": 5.0}
    await proc._handle_blocked_exit_alert(STREAM, str(STREAM), _reading(clips=[clip]), "Exit", WS, uuid4())
    await _drain(proc)
    args, kw = proc._upload_clip.await_args
    assert args == (FRAMES, 5.0) and f"/clips/41-" in kw["key"] and kw["key"].startswith(f"blocked-exit/{WS}/")
    await kw["on_uploaded"]("s3://b/clip.mp4")
    set_ev.assert_awaited_once_with(41, clip_path="s3://b/clip.mp4")
    assert "ep1" not in proc._be_episode_map()


async def test_open_saves_the_buffered_video_like_shoplifting(proc, mocker):
    mocker.patch.object(blocked_exit_service, "open_episode", mocker.AsyncMock(side_effect=[(41, True), (41, False)]))
    set_ev = mocker.patch.object(blocked_exit_service, "set_evidence", mocker.AsyncMock())
    frames = [np.zeros((8, 8, 3), dtype=np.uint8)] * 31
    ts = [i / 15 for i in range(31)]  # 31 frames over 2 s = 15 fps
    reading = _reading(transition="open", recent_frames=frames, recent_frames_ts=ts)

    await proc._handle_blocked_exit_alert(STREAM, str(STREAM), reading, "Exit", WS, uuid4())
    await _drain(proc)
    args, kw = proc._upload_clip.await_args
    assert args[0] is frames and args[1] == pytest.approx(15.0)
    assert kw["key"].startswith(f"blocked-exit/{WS}/{STREAM}/videos/41-")
    await kw["on_uploaded"]("s3://b/video.mp4")
    set_ev.assert_awaited_once_with(41, video_path="s3://b/video.mp4")

    # a continued (not newly created) episode already has its video
    proc._upload_clip.reset_mock()
    await proc._handle_blocked_exit_alert(STREAM, str(STREAM), reading, "Exit", WS, uuid4())
    await _drain(proc)
    proc._upload_clip.assert_not_awaited()


async def test_unmapped_clip_falls_back_to_the_open_episode_or_is_dropped(proc, mocker):
    mocker.patch.object(blocked_exit_service, "get_open_episode_id", mocker.AsyncMock(side_effect=[7, None]))
    clip = {"key": "unknown", "frames": FRAMES, "fps": 5.0}
    await proc._handle_blocked_exit_clips(STREAM, str(STREAM), WS, [clip, dict(clip)])
    await _drain(proc)
    assert proc._upload_clip.await_count == 1
    assert "/clips/7-" in proc._upload_clip.await_args.kwargs["key"]


async def test_close_passes_final_confidence(proc, mocker):
    close = mocker.patch.object(blocked_exit_service, "close_open_episodes", mocker.AsyncMock(return_value=1))
    await proc._handle_blocked_exit_alert(STREAM, str(STREAM), _reading(transition="close"), "Exit", WS, uuid4())
    close.assert_awaited_once_with(STREAM, confidence=0.9, avg_confidence=0.8)


async def test_release_flushes_partial_clips_and_forgets_the_stream(proc, mocker):
    close = mocker.patch.object(blocked_exit_service, "close_open_episodes", mocker.AsyncMock(return_value=1))
    proc._be_episode_map()["ep1"] = {"event_id": 5, "stream": str(STREAM)}
    proc._be_episode_map()["other"] = {"event_id": 6, "stream": "another"}
    proc.blocked_exit_engine.release_stream.return_value = {"clips": [{"key": "ep1", "frames": FRAMES, "fps": 2.0}]}
    await proc._release_blocked_exit_stream(STREAM, str(STREAM), WS)
    await _drain(proc)
    close.assert_awaited_once_with(STREAM)
    assert "/clips/5-" in proc._upload_clip.await_args.kwargs["key"]
    assert list(proc._be_episode_map()) == ["other"]
