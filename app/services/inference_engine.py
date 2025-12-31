# app/services/inference_engine.py
import asyncio
import time
import logging
import os
import numpy as np
import torch
from ultralytics import YOLO

logger = logging.getLogger(__name__)

# ===================== CPU TUNING =====================
torch.set_num_threads(min(4, os.cpu_count() or 4))
torch.set_num_interop_threads(2)

# ===================== CONFIG =====================
BATCH_SIZE = 4               # CPU optimal: 2–4
MAX_QUEUE_SIZE = 64
DETECT_INTERVAL_SEC = 2.0    # per stream
IMG_SIZE = 640               # smaller = faster CPU
CONF = 0.4

class InferenceEngine:
    def __init__(self, model_path: str):
        self.model = YOLO(model_path)
        self.model.fuse()

        self.queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.last_detect_time = {}
        self.latest_frame = {}
        self.results = {}

        self.workers = []
        self.running = False

    # ---------- PUBLIC API ----------
    def update_frame(self, stream_id: str, frame: np.ndarray):
        """Store latest frame ONLY (drop old ones)."""
        self.latest_frame[stream_id] = frame

    def get_result(self, stream_id: str):
        return self.results.get(stream_id)

    async def start(self, workers: int = 1):
        self.running = True
        for i in range(workers):
            task = asyncio.create_task(self._worker(i))
            self.workers.append(task)

        asyncio.create_task(self._scheduler())
        logger.info(f"🧠 InferenceEngine started with {workers} worker(s)")

    async def stop(self):
        self.running = False
        for w in self.workers:
            w.cancel()

    # ---------- SCHEDULER ----------
    async def _scheduler(self):
        """Decides WHEN to run detection per stream."""
        while self.running:
            now = time.monotonic()

            for stream_id, frame in list(self.latest_frame.items()):
                last = self.last_detect_time.get(stream_id, 0)

                if now - last < DETECT_INTERVAL_SEC:
                    continue

                if self.queue.full():
                    continue  # graceful drop

                try:
                    self.queue.put_nowait((stream_id, frame))
                    self.last_detect_time[stream_id] = now
                except asyncio.QueueFull:
                    pass

            await asyncio.sleep(0.05)

    # ---------- WORKER ----------
    async def _worker(self, idx: int):
        logger.info(f"⚙️ YOLO worker {idx} started")

        while True:
            batch_frames = []
            batch_streams = []

            try:
                item = await self.queue.get()
                stream_id, frame = item

                batch_frames.append(frame)
                batch_streams.append(stream_id)

                while len(batch_frames) < BATCH_SIZE:
                    try:
                        sid, frm = self.queue.get_nowait()
                        batch_frames.append(frm)
                        batch_streams.append(sid)
                    except asyncio.QueueEmpty:
                        break

                results = self.model.predict(
                    batch_frames,
                    imgsz=IMG_SIZE,
                    conf=CONF,
                    classes=[0],
                    verbose=False
                )

                for sid, res in zip(batch_streams, results):
                    count = 0
                    if res.boxes is not None:
                        count = len(res.boxes)

                    self.results[sid] = {
                        "count": count,
                        "ts": time.monotonic()
                    }

            except Exception as e:
                logger.error(f"Inference worker error: {e}", exc_info=True)
