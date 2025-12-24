# websocket_stream.py
import asyncio
import uuid
import cv2
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from shared_stream import VideoFileManager  # adjust path if needed
import logging

app = FastAPI()
manager = VideoFileManager()

# IMPORTANT: ensure uvicorn run with --workers 1 in Docker (see notes below)

async def send_frame_over_ws(websocket: WebSocket, frame: 'np.ndarray'):
    """
    Encode frame to JPEG and send as binary over websocket.
    """
    try:
        ret, buf = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
        if not ret:
            return False
        await websocket.send_bytes(buf.tobytes())
        return True
    except Exception as e:
        logging.exception("Failed to send frame over websocket: %s", e)
        return False

@app.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket, source: str):
    """
    WebSocket endpoint that streams frames from a SharedVideoStream source.
    Example: ws://server/ws/stream?source=rtsp://admin:... 
    """
    await websocket.accept()
    stream_id = str(uuid.uuid4())
    shared = manager.get_shared_stream(source)
    # Register subscriber logically (keeps stats)
    added = shared.add_subscriber(stream_id)
    if not added:
        await websocket.send_text("ERROR: max subscribers reached")
        await websocket.close()
        return

    loop = asyncio.get_event_loop()
    logging.info(f"Websocket subscriber {stream_id} connected for {source}")

    try:
        # Main loop: wait for new frame (non-blocking for event loop)
        while True:
            # Wait up to 5s for a frame using threadpool so we don't block asyncio loop
            has_frame = await loop.run_in_executor(None, shared.wait_for_frame, 5.0)
            if not has_frame:
                # Optionally send keepalive or continue
                # If long inactivity, break
                logging.debug(f"No frame available for {stream_id}, continue")
                # check if still running
                stats = shared.get_stats()
                if not stats['is_running']:
                    logging.info("Shared stream not running, closing websocket")
                    break
                continue

            # Fetch latest frame (quick copy op) in threadpool
            frame = await loop.run_in_executor(None, shared.get_latest_frame, stream_id)
            if frame is None:
                # No frame available (race) — continue
                continue

            # Send frame as binary; use a short timeout to avoid long await if client stuck
            try:
                # If send takes too long, drop frame: use wait_for to timeout
                await asyncio.wait_for(send_frame_over_ws(websocket, frame), timeout=2.0)
            except asyncio.TimeoutError:
                logging.warning(f"Slow websocket client {stream_id}; dropping frame")
                # drop and continue
                continue
            except WebSocketDisconnect:
                logging.info(f"Websocket client {stream_id} disconnected")
                break
            except Exception:
                logging.exception("Error sending frame to websocket")
                break

    finally:
        # Cleanup subscriber and close WebSocket
        shared.remove_subscriber(stream_id)
        try:
            await websocket.close()
        except:
            pass
        logging.info(f"Subscriber {stream_id} removed for {source}")
