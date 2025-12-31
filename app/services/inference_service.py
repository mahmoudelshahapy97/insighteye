# app/services/inference_service.py

import asyncio
import time
from typing import Dict, List, Tuple
from collections import defaultdict
import numpy as np
from ultralytics import YOLO

class GlobalInferenceService:
    """
    Centralized inference service with batching for multi-camera systems.
    Use this for 20+ cameras to maximize CPU efficiency.
    """
    def __init__(self, batch_size=4, max_wait_ms=100):
        self.batch_size = batch_size
        self.max_wait_ms = max_wait_ms / 1000.0
        
        # Global inference queue
        self.inference_queue = asyncio.Queue(maxsize=64)
        
        # Result caches per stream
        self.result_cache: Dict[str, Dict] = {}
        
        # Batch accumulation
        self.pending_batch: List[Tuple[np.ndarray, str]] = []
        self.batch_lock = asyncio.Lock()
        
        # Workers
        self.workers: List[asyncio.Task] = []
        self.running = False
        
        # Models (loaded once, shared by all streams)
        self.people_model = None
        self.gender_model = None
        self.fire_model = None
        
        self._initialize_models()

    def _initialize_models(self):
        """Load models once for all streams"""
        import torch
        torch.set_num_threads(4)
        torch.set_num_interop_threads(2)
        
        self.people_model = YOLO("yolov8n.pt")
        self.people_model.fuse()
        
        logger.info("✅ Global inference models loaded")

    async def start_workers(self, num_workers=2):
        """Start inference worker pool"""
        self.running = True
        for i in range(num_workers):
            worker = asyncio.create_task(self._inference_worker(i))
            self.workers.append(worker)
        
        logger.info(f"✅ Started {num_workers} inference workers")

    async def stop_workers(self):
        """Stop all workers"""
        self.running = False
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

    async def submit_frame(self, frame: np.ndarray, stream_id: str) -> bool:
        """
        Submit frame for inference (non-blocking).
        Returns False if queue is full (graceful degradation).
        """
        try:
            self.inference_queue.put_nowait((frame, stream_id))
            return True
        except asyncio.QueueFull:
            logger.warning(f"Inference queue full, dropping frame from {stream_id}")
            return False

    def get_cached_result(self, stream_id: str) -> Dict:
        """Get last inference result for stream"""
        return self.result_cache.get(stream_id, {
            'person_count': 0,
            'male_count': 0,
            'female_count': 0,
            'fire_status': 'no detection',
            'timestamp': time.time()
        })

    async def _inference_worker(self, worker_id: int):
        """
        Worker that processes frames in batches.
        Waits up to max_wait_ms to fill batch, then runs inference.
        """
        logger.info(f"Inference worker {worker_id} started")
        
        while self.running:
            try:
                batch_frames = []
                batch_stream_ids = []
                batch_start = time.time()
                
                # Collect batch
                while len(batch_frames) < self.batch_size:
                    timeout = self.max_wait_ms - (time.time() - batch_start)
                    if timeout <= 0:
                        break
                    
                    try:
                        frame, stream_id = await asyncio.wait_for(
                            self.inference_queue.get(),
                            timeout=timeout
                        )
                        batch_frames.append(frame)
                        batch_stream_ids.append(stream_id)
                    except asyncio.TimeoutError:
                        break
                
                # Process batch if we have frames
                if batch_frames:
                    await self._process_batch(batch_frames, batch_stream_ids, worker_id)
            
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    async def _process_batch(self, frames: List[np.ndarray], 
                        stream_ids: List[str], worker_id: int):
        """Process a batch of frames with YOLO"""
        try:
            start_time = time.time()
            
            # Run inference on batch (blocking, but shared across streams)
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,  # Default thread pool
                lambda: self.people_model.predict(
                    source=frames,
                    conf=0.5,
                    classes=[0],
                    verbose=False,
                    imgsz=512,
                    device='cpu'
                )
            )
            
            # Parse results
            for idx, (result, stream_id) in enumerate(zip(results, stream_ids)):
                person_count = len(result.boxes) if result.boxes else 0
                
                # Update cache
                self.result_cache[stream_id] = {
                    'person_count': person_count,
                    'male_count': 0,  # TODO: Batch gender inference
                    'female_count': 0,
                    'fire_status': 'no detection',  # TODO: Batch fire inference
                    'timestamp': time.time(),
                    'boxes': result.boxes
                }
            
            elapsed = (time.time() - start_time) * 1000
            logger.debug(f"Worker {worker_id} processed batch of {len(frames)} in {elapsed:.1f}ms")
        
        except Exception as e:
            logger.error(f"Batch processing error: {e}", exc_info=True)


class StreamProcessingService:
    def init(self):
    # ... existing code ...
        # Add global inference service (for 20+ cameras)
        self.global_inference = GlobalInferenceService(
            batch_size=4,
            max_wait_ms=100
        )

    async def start_inference_workers(self):
        """Start inference workers (call during app startup)"""
        await self.global_inference.start_workers(num_workers=2)

    def detect_objects_with_batching(
        self,
        frame: np.ndarray,
        stream_id_str: str
    ) -> Tuple[np.ndarray, int, bool, int, int, str]:
        """
        Detection with global batching (for high camera counts).
        Replaces detect_objects_with_threshold when using batching.
        """
        # Submit frame (non-blocking)
        submitted = asyncio.create_task(
            self.global_inference.submit_frame(frame, stream_id_str)
        )
        
        # Return cached result immediately
        cached = self.global_inference.get_cached_result(stream_id_str)
        
        annotated = frame.copy()
        # Draw cached results
        cv2.putText(annotated, f"People: {cached['person_count']}",
                (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        return (annotated, cached['person_count'], False,
                cached['male_count'], cached['female_count'], cached['fire_status'])
                