import cv2
import asyncio
import websockets
import numpy as np

async def send_frames(rtsp_url: str, websocket):
    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        print("Failed to open stream")
        return

    while True:
        ret, frame = cap.read()
        if not ret:
            await asyncio.sleep(0.1)
            continue

        # Encode frame as JPEG
        ret, buffer = cv2.imencode('.jpg', frame)
        if not ret:
            continue

        # Convert to bytes and send over WebSocket
        await websocket.send(buffer.tobytes())
        await asyncio.sleep(0.03)  # roughly 30 FPS


async def handler(websocket, path):
    rtsp_url = "rtsp://admin:12345678@41.178.2.61:5511/ch01/0"
    await send_frames(rtsp_url, websocket)

start_server = websockets.serve(handler, "0.0.0.0", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()

