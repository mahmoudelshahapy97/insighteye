"""Stream-processor wiring for lost & found: events and found items reach the service,
evidence uploads land on the right rows, and clips (buffered as JPEGs) are decoded and
uploaded against their event."""

import asyncio
from uuid import UUID, uuid4

import cv2
import numpy as np
import pytest

from app.services import lost_found_service as lfs
from app.services import stream_processing_service as sps
from app.services.lost_found_service import lost_found_service

STREAM = uuid4()
WS = uuid4()
OWNER = uuid4()


def _jpeg():
    ok, buf = cv2.imencode(".jpg", np.zeros((8, 8, 3), dtype=np.uint8))
    return buf.tobytes()


def _record(**kw):
    base = {
        "event_id": str(uuid4()), "track_key": "7@abc", "class_name": "bag", "confidence": 0.42,
        "bbox": {"x1": 1, "y1": 2, "x2": 3, "y2": 4}, "owner_track_id": 3,
        "first_seen_at": 1000.0, "static_since": 1001.0, "abandoned_at": 1032.0, "unattended_seconds": 31.0,
        "snapshot_jpeg": b"snap", "crop_jpeg": b"crop", "dominant_color": "red", "phash": 5, "color_hist": b"h",
    }
    base.update(kw)
    return base


@pytest.fixture
def proc(mocker):
    p = object.__new__(sps.StreamProcessingService)  # no models / threads
    p.stream_manager = None
    p._upload_clip = mocker.AsyncMock()
    mocker.patch.object(sps.notification_service, "create_notification", mocker.AsyncMock(return_value=None))
    return p


async def _drain(p):
    await asyncio.gather(*getattr(p, "_nez_tasks", set()))


async def test_new_event_stores_event_found_item_and_uploads_evidence(proc, mocker):
    found_id = uuid4()
    insert = mocker.patch.object(lost_found_service, "insert_event", mocker.AsyncMock(return_value=True))
    found = mocker.patch.object(lost_found_service, "insert_found_item", mocker.AsyncMock(return_value=found_id))
    set_ev = mocker.patch.object(lost_found_service, "set_event_evidence", mocker.AsyncMock())
    set_crop = mocker.patch.object(lost_found_service, "set_found_crop", mocker.AsyncMock())
    upload = mocker.patch.object(lfs, "upload_jpeg", mocker.AsyncMock(side_effect=lambda data, key: f"s3://b/{key}"))
    record = _record()

    await proc._handle_lost_found_result(STREAM, str(STREAM), {"events": [record], "clips": []}, "Lobby", WS, OWNER)
    await _drain(proc)

    assert insert.await_args.kwargs == {"stream_id": STREAM, "workspace_id": WS, "camera_name": "Lobby", "record": record}
    assert found.await_args.kwargs["event_id"] == UUID(record["event_id"])
    keys = [c.args[1] for c in upload.await_args_list]
    assert keys == [
        f"lost-found/{WS}/{STREAM}/frames/{record['event_id']}.jpg",
        f"lost-found/{WS}/{STREAM}/crops/{found_id}.jpg",
    ]
    set_ev.assert_awaited_once_with(UUID(record["event_id"]), snapshot_path=f"s3://b/{keys[0]}")
    set_crop.assert_awaited_once_with(found_id, f"s3://b/{keys[1]}")
    sps.notification_service.create_notification.assert_awaited_once()
    assert "bag" in sps.notification_service.create_notification.await_args.kwargs["message"]


async def test_an_already_recorded_object_is_skipped(proc, mocker):
    mocker.patch.object(lost_found_service, "insert_event", mocker.AsyncMock(return_value=False))
    found = mocker.patch.object(lost_found_service, "insert_found_item", mocker.AsyncMock())
    await proc._handle_lost_found_result(STREAM, str(STREAM), {"events": [_record()], "clips": []}, "Lobby", WS, OWNER)
    await _drain(proc)
    found.assert_not_awaited()
    sps.notification_service.create_notification.assert_not_awaited()


async def test_finished_clip_is_decoded_uploaded_and_attached_to_its_event(proc, mocker):
    set_ev = mocker.patch.object(lost_found_service, "set_event_evidence", mocker.AsyncMock())
    event_id = uuid4()
    clip = {"event_id": str(event_id), "jpegs": [_jpeg(), b"not a jpeg", _jpeg()], "fps": 5.0}

    await proc._handle_lost_found_result(STREAM, str(STREAM), {"events": [], "clips": [clip]}, "Lobby", WS, OWNER)
    await _drain(proc)

    args, kw = proc._upload_clip.await_args
    frames, fps = args
    assert len(frames) == 2 and all(f.shape == (8, 8, 3) for f in frames) and fps == 5.0
    assert kw["key"] == f"lost-found/{WS}/{STREAM}/clips/{event_id}.mp4"
    await kw["on_uploaded"]("s3://b/clip.mp4")
    set_ev.assert_awaited_once_with(event_id, video_path="s3://b/clip.mp4")
