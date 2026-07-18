# app/services/stream_processing_service.py
"""
Stream Processing Service - Handles video frame processing, object detection, and alerts.
Stream Processing Service - Handles video frame processing, object detection, and alerts.
"""
import asyncio
import logging
import time
import cv2
import numpy as np
from typing import Dict, Optional, Any, Tuple, List
from uuid import UUID
import uuid
import tempfile
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
# from ultralytics import YOLO  # Removing direct dependency
import concurrent.futures
import os
import sys
import subprocess
from app.config.settings import config
from app.services.model_loader import ModelFactory, ModelBackend
from app.utils import send_people_count_alert_email, send_fire_alert_email, send_shoplifting_alert_email
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.postgres_service import postgres_service
from app.services.detection_data_service import detection_data_service
from app.services.video_stream_service import video_stream_service
from app.services.retry_service import retry_service
from app.services.notification_service import notification_service
from app.services.fire_detection_service import fire_detection_service
from app.services.people_count_service import people_count_service

logger = logging.getLogger(__name__)


def _log_shoplifting_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error("Shoplifting video save task failed", exc_info=task.exception())


# ThreadPoolExecutor for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 1) * 2 + 4)
)

class StreamProcessingService:
    """
    Service for processing video streams - detection, alerts, and frame processing.
    """

    def __init__(self):
        self.db_manager = db_manager
        self.postgres_service  = postgres_service 
        self.detection_data_service  = detection_data_service 
        self.video_stream_service  = video_stream_service 
        self.retry_service = retry_service
        self.people_model = None
        self.gender_model = None
        self.fire_model = None
        self.shoplifting_model = None
        self.stream_manager = None
        self.video_file_manager = None

        self._cached_results = {}
        self._active_incidents = {}
        self.use_gpu = True

        # Initialize models
        self._initialize_models()
        
        logger.info("StreamProcessingService initialized")

    def initialize(self, stream_manager, video_file_manager):
        """Initialize service with dependencies."""
        self.stream_manager = stream_manager
        self.video_file_manager = video_file_manager
        logger.info("StreamProcessingService dependencies initialized")

    def _initialize_models(self):
        """Initialize models using ModelFactory."""
        # Config now resolves paths dynamically based on MODEL_BACKEND
        people_model_path = config.people_model_path
        gender_model_path = config.gender_model_path
        fire_model_path = config.fire_model_path
        shoplifting_model_path = config.shoplifting_model_path
        backend = config.model_backend
        
        logger.info(f"🔍 Checking model paths (Backend: {backend}):")
        logger.info(f"  People: {people_model_path} (exists: {os.path.exists(people_model_path)})")
        logger.info(f"  Gender: {gender_model_path} (exists: {os.path.exists(gender_model_path)})")
        logger.info(f"  Fire: {fire_model_path} (exists: {os.path.exists(fire_model_path)})")
        logger.info(f"  Shoplifting: {shoplifting_model_path} (exists: {os.path.exists(shoplifting_model_path)})")
                
        try:
            # Use ModelFactory to create loaders
            # We pass the backend string from config, converting to Enum if needed, 
            # but create_loader handles string if we map it or just let it auto-detect if path has extension.
            # However, config.model_backend provides explicit intent.
            
            logger.info(f"Initializing models with backend: {backend}")
            
            self.people_model = ModelFactory.create_loader(
                people_model_path, 
                backend=ModelBackend(backend),
                confidence_threshold=config.people_confidence
            )
            
            self.gender_model = ModelFactory.create_loader(
                gender_model_path, 
                backend=ModelBackend(backend),
                confidence_threshold=config.gender_confidence
            )
            
            self.fire_model = ModelFactory.create_loader(
                fire_model_path, 
                backend=ModelBackend(backend),
                confidence_threshold=config.fire_confidence
            )
            
            from app.services.shoplifting_inference import shoplifting_engine
            self.shoplifting_engine = shoplifting_engine

            # Load models immediately to fail fast if there's an issue
            self.people_model.load_model()
            self.gender_model.load_model()
            self.fire_model.load_model()
            # Shoplifting engine loaded lazily in the stream loop after logging is set up
            
            logger.info(f"✅ Models initialized successfully using {backend}")
        except Exception as e:
            logger.error(f"Failed to initialize models: {e}", exc_info=True)
            self.people_model = None
            self.gender_model = None
            self.fire_model = None
            self.shoplifting_engine = None

    def detect_objects_with_threshold(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.5,
        threshold_settings: Dict[str, Any] = None,
        stream_id_str: str = None
    ) -> Tuple[np.ndarray, int, bool, int, int, str, bool, float, List[str]]:
        """
        Detect objects in frame with threshold checking.
        Returns: (annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status, is_shoplifting, shoplifting_conf, shoplifting_objects)
        """
        if frame is None or frame.size == 0:
            return np.zeros((100, 100, 3), dtype=np.uint8), 0, False, 0, 0, "no detection", False, 0.0, []

        # ✅ CRITICAL CHECK: This should NEVER happen now
        if self.people_model is None:
            error_msg = f"🚨 CRITICAL: People model not initialized for stream {stream_id_str}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        # Stream-specific frame counting
        if stream_id_str and self.stream_manager:
            frame_count = self.stream_manager.fire_detection_frame_counts.get(stream_id_str, 0) + 1
            self.stream_manager.fire_detection_frame_counts[stream_id_str] = frame_count
        else:
            frame_count = getattr(self, '_frame_count', 0) + 1
            setattr(self, '_frame_count', frame_count)
        
        # Cache setup
        cache_key = f"cache_{stream_id_str}" if stream_id_str else "cache_global"
        if cache_key not in self._cached_results:
            self._cached_results[cache_key] = {
                'male_count': 0,
                'female_count': 0,
                'fire_status': 'no detection',
                'last_gender_frame': 0,
                'last_fire_frame': 0
            }
        cache = self._cached_results[cache_key]

        # Resize frame if needed with GPU support
        max_dim = config.yolo_input_size
        h, w = frame.shape[:2]
        scale = 1.0
        
        if h > max_dim or w > max_dim:
            scale = max_dim / max(h, w)
            new_w, new_h = int(w * scale), int(h * scale)
            new_w = max(2, new_w - (new_w % 2))
            new_h = max(2, new_h - (new_h % 2))
            
            if self.use_gpu:
                try:
                    gpu_frame = cv2.cuda_GpuMat()
                    gpu_frame.upload(frame)
                    gpu_resized = cv2.cuda.resize(gpu_frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
                    input_frame = gpu_resized.download()
                except Exception as e:
                    logger.warning(f"GPU resize failed for stream {stream_id_str}, using CPU: {e}")
                    input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
            else:
                input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        else:
            input_frame = frame

        try:
            # People detection
            raw_people_results = self.people_model.predict(input_frame)
            people_detections = self.people_model.postprocess(
                raw_people_results, 
                original_shape=(h, w),
                input_size=(640, 640)
            )

            # Filter for person class
            person_detections = [d for d in people_detections if d['class_id'] == 0]
            person_count = len(person_detections)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 3 == 0 and self.gender_model and self.gender_model.is_loaded:
                try:
                    raw_gender_results = self.gender_model.predict(input_frame)
                    gender_detections = self.gender_model.postprocess(
                        raw_gender_results,
                        original_shape=(h, w),
                        input_size=(640, 640)
                    )
                    
                    male_count = sum(1 for d in gender_detections if d['class_id'] == 1)
                    female_count = sum(1 for d in gender_detections if d['class_id'] == 0)
                    
                    cache['male_count'] = male_count
                    cache['female_count'] = female_count
                    cache['last_gender_frame'] = frame_count
                except Exception as e:
                    logger.error(f"Gender detection error for stream {stream_id_str}: {e}")

            # Fire detection (every 10th frame)
            if frame_count % 10 == 0 and self.fire_model and self.fire_model.is_loaded:
                try:
                    raw_fire_results = self.fire_model.predict(input_frame)
                    fire_detections = self.fire_model.postprocess(
                        raw_fire_results,
                        original_shape=(h, w),
                        input_size=(640, 640),
                        iou_threshold=0.5
                    )

                    current_fire_status = "no detection"
                    
                    if fire_detections:
                        classes = [d['class_id'] for d in fire_detections]
                        logger.info(f"🔥 Fire model detected classes: {classes} on stream {stream_id_str}")
                        
                        if 0 in classes:
                            current_fire_status = "fire"
                            logger.warning(f"🔥🔥 FIRE DETECTED on stream {stream_id_str}")
                        elif 1 in classes:
                            current_fire_status = "smoke"
                            logger.warning(f"💨 SMOKE DETECTED on stream {stream_id_str}")

                    previous_fire_status = cache['fire_status']
                    cache['fire_status'] = current_fire_status
                    cache['last_fire_frame'] = frame_count
                    
                    if current_fire_status != previous_fire_status:
                        if current_fire_status in ["fire", "smoke"]:
                            logger.warning(f"🔥 Fire detection change: {stream_id_str} changed to '{current_fire_status}'")
                        else:
                            logger.info(f"🌊 Fire cleared: {stream_id_str} changed to 'no detection'")
                        
                except Exception as e:
                    logger.error(f"Fire detection error for stream {stream_id_str}: {e}")

            # Shoplifting detection is handled in the stream loop at its own frame rate,
            # independent of the main detection gate. Always return neutral values here.
            is_shoplifting = False
            shoplifting_conf = 0.0
            shoplifting_objects = []

            # Use cached results
            male_count = cache['male_count']
            female_count = cache['female_count']
            fire_status = cache['fire_status']

            # Threshold checking for people count
            alert_triggered = False
            if threshold_settings and threshold_settings.get("alert_enabled", False):
                greater_than = threshold_settings.get("greater_than")
                less_than = threshold_settings.get("less_than")
                
                if greater_than is not None and person_count > greater_than:
                    alert_triggered = True
                if less_than is not None and person_count < less_than:
                    alert_triggered = True

            # Annotate frame with GPU support
            if self.use_gpu:
                try:
                    annotated_frame = self._annotate_frame_gpu(
                        input_frame, person_detections, person_count,
                        male_count, female_count, fire_status,
                        alert_triggered, frame_count
                    )
                except Exception as e:
                    logger.warning(f"GPU annotation failed for stream {stream_id_str}, using CPU: {e}")
                    annotated_frame = self._annotate_frame_cpu(
                        input_frame, person_detections, person_count,
                        male_count, female_count, fire_status,
                        alert_triggered, frame_count
                    )
            else:
                annotated_frame = self._annotate_frame_cpu(
                    input_frame, person_detections, person_count,
                    male_count, female_count, fire_status,
                    alert_triggered, frame_count
                )
            
            # Scale back if needed with GPU support
            if scale != 1.0:
                if self.use_gpu:
                    try:
                        gpu_annotated = cv2.cuda_GpuMat()
                        gpu_annotated.upload(annotated_frame)
                        gpu_final = cv2.cuda.resize(gpu_annotated, (w, h), interpolation=cv2.INTER_LINEAR)
                        annotated_frame = gpu_final.download()
                    except Exception as e:
                        logger.warning(f"GPU resize back failed for stream {stream_id_str}, using CPU: {e}")
                        annotated_frame = cv2.resize(annotated_frame, (w, h), interpolation=cv2.INTER_LINEAR)
                else:
                    annotated_frame = cv2.resize(annotated_frame, (w, h), interpolation=cv2.INTER_LINEAR)
                
            return annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status, is_shoplifting, shoplifting_conf, shoplifting_objects
            
        except Exception as e:
            logger.error(f"Object detection error for stream {stream_id_str}: {e}", exc_info=True)
            return frame.copy(), 0, False, 0, 0, "no detection", False, 0.0, []

    def _annotate_frame_cpu(
        self,
        frame: np.ndarray,
        person_detections: List[Dict],
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        alert_triggered: bool,
        frame_count: int
    ) -> np.ndarray:
        """CPU-based frame annotation"""
        annotated_frame = frame.copy()
        
        # Draw people bounding boxes
        for det in person_detections:
            bbox = det['bbox']
            x1, y1, x2, y2 = bbox
            conf = det['confidence']
            
            color = (0, 255, 0)
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), color, 2)
            
            label = f"Person {conf:.2f}"
            t_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)[0]
            cv2.rectangle(annotated_frame, (x1, y1 - t_size[1] - 4), (x1 + t_size[0], y1), color, -1)
            cv2.putText(annotated_frame, label, (x1, y1 - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
        
        # Add text overlays
        count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
        count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
        cv2.putText(annotated_frame, count_text, (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA)
        
        # Fire/smoke overlay
        if fire_status != "no detection":
            fire_color = (0, 0, 255)
            fire_text = f"ALERT: {fire_status.upper()}"
            cv2.putText(annotated_frame, fire_text, (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.7, fire_color, 2, cv2.LINE_AA)
            
            if frame_count % 20 < 10:
                red_overlay = annotated_frame.copy()
                red_overlay[:] = (0, 0, 255)
                annotated_frame = cv2.addWeighted(annotated_frame, 0.9, red_overlay, 0.1, 0)

        # Alert styling
        if alert_triggered:
            red_overlay = annotated_frame.copy()
            red_overlay[:] = (0, 0, 255)
            annotated_frame = cv2.addWeighted(annotated_frame, 0.85, red_overlay, 0.15, 0)
        
        return annotated_frame

    def _annotate_frame_gpu(
        self,
        frame: np.ndarray,
        person_detections: List[Dict],
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        alert_triggered: bool,
        frame_count: int
    ) -> np.ndarray:
        """
        GPU-based frame annotation
        Note: OpenCV CUDA has limited drawing functions, so we still do drawing on CPU
        but use GPU for color space conversions and blending operations
        """
        # Drawing operations must be done on CPU
        annotated_frame = frame.copy()
        
        # Draw people bounding boxes
        for det in person_detections:
            bbox = det['bbox']
            x1, y1, x2, y2 = bbox
            conf = det['confidence']
            
            color = (0, 255, 0)
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), color, 2)
            
            label = f"Person {conf:.2f}"
            t_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)[0]
            cv2.rectangle(annotated_frame, (x1, y1 - t_size[1] - 4), (x1 + t_size[0], y1), color, -1)
            cv2.putText(annotated_frame, label, (x1, y1 - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
        
        # Add text overlays
        count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
        count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
        cv2.putText(annotated_frame, count_text, (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA)
        
        # Fire/smoke overlay with GPU blending
        if fire_status != "no detection":
            fire_color = (0, 0, 255)
            fire_text = f"ALERT: {fire_status.upper()}"
            cv2.putText(annotated_frame, fire_text, (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.7, fire_color, 2, cv2.LINE_AA)
            
            if frame_count % 20 < 10:
                # Use GPU for blending
                gpu_annotated = cv2.cuda_GpuMat()
                gpu_annotated.upload(annotated_frame)
                
                red_overlay = np.full_like(annotated_frame, (0, 0, 255), dtype=np.uint8)
                gpu_overlay = cv2.cuda_GpuMat()
                gpu_overlay.upload(red_overlay)
                
                # GPU addWeighted
                gpu_result = cv2.cuda.addWeighted(gpu_annotated, 0.9, gpu_overlay, 0.1, 0)
                annotated_frame = gpu_result.download()

        # Alert styling with GPU blending
        if alert_triggered:
            gpu_annotated = cv2.cuda_GpuMat()
            gpu_annotated.upload(annotated_frame)
            
            red_overlay = np.full_like(annotated_frame, (0, 0, 255), dtype=np.uint8)
            gpu_overlay = cv2.cuda_GpuMat()
            gpu_overlay.upload(red_overlay)
            
            gpu_result = cv2.cuda.addWeighted(gpu_annotated, 0.85, gpu_overlay, 0.15, 0)
            annotated_frame = gpu_result.download()
        
        return annotated_frame

    async def _get_camera_threshold_settings(self, stream_id: UUID) -> Dict[str, Any]:
        """Get threshold settings for a camera stream."""
        query = """
            SELECT alert_enabled, count_threshold_greater, count_threshold_less
            FROM video_stream
            WHERE stream_id = $1
        """
        result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
        
        if not result:
            return {
                "alert_enabled": False,
                "greater_than": None,
                "less_than": None
            }
        
        return {
            "alert_enabled": result.get("alert_enabled", False),
            "greater_than": result.get("count_threshold_greater"),
            "less_than": result.get("count_threshold_less")
        }

    async def _validate_stream_source(self, source: str) -> bool:
        """Validate that the stream source is accessible."""
        loop = asyncio.get_event_loop()
        
        def check_source():
            try:
                cap = cv2.VideoCapture(source)
                is_valid = cap.isOpened()
                cap.release()
                return is_valid
            except Exception:
                return False
        
        try:
            return await loop.run_in_executor(thread_pool, check_source)
        except Exception as e:
            logger.error(f"Error validating source {source}: {e}")
            return False

    async def _save_detection(
        self,
        stream_id_str: str,
        camera_name: str,
        owner_username: str,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        frame: np.ndarray,
        workspace_id: UUID,
        location_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Save detection data to PostgreSQL database.
        This is called periodically based on frame_skip configuration.
        """
        try:
            if self.detection_data_service:
                stream_id = stream_id_str if isinstance(stream_id_str, UUID) else UUID(str(stream_id_str))

                # Get user_id from stream info
                stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
                
                if stream_info:
                    success = await self.detection_data_service.insert_detection_data(
                        stream_id=stream_id ,
                        workspace_id=workspace_id,
                        user_id=stream_info['user_id'],
                        camera_name=camera_name,
                        username=owner_username,
                        person_count=person_count,
                        male_count=male_count,
                        female_count=female_count,
                        fire_status=fire_status,
                        frame=frame,
                        location_info=location_info,
                    )
                    
                    if success:
                        logger.debug(f"✅ Saved detection data for stream {stream_id_str}")
                    else:
                        logger.warning(f"⚠️ Failed to save detection data for stream {stream_id_str}")
                else:
                    logger.error(f"Could not get stream info for {stream_id_str}, skipping save")
            else:
                logger.warning("service not initialized, skipping save")
            
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving detection data for stream {stream_id_str}: {e}", exc_info=True)
            return False

    async def _handle_people_count_alert(
        self,
        stream_id: UUID,
        stream_id_str: str,
        person_count: int,
        threshold_settings: Dict[str, Any],
        camera_name: str,
        workspace_id: UUID,
        owner_id: UUID
    ):
        """
        Handle people count alert logic with workspace-wide broadcasting.
        
        FIXED ISSUES:
        1. Cooldown check was too strict
        2. Missing debug logs
        3. Threshold validation needed improvement
        """
        try:
            # ==================== DEBUG: Entry Point ====================
            logger.info(
                f"🔍 People count alert check: stream={camera_name}, "
                f"count={person_count}, settings={threshold_settings}"
            )
            
            # Validate threshold settings
            if not threshold_settings:
                logger.warning(f"⚠️ No threshold settings for stream {stream_id_str}")
                return
            
            if not threshold_settings.get("alert_enabled", False):
                logger.debug(f"⏭️ Alerts disabled for stream {stream_id_str}")
                return
            
            # Get alert state from database
            from app.services.people_count_service import people_count_service
            
            alert_state = await people_count_service.get_people_count_alert_state(stream_id)
            
            current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            cooldown_minutes = 10
            
            # ==================== Determine Threshold Type ====================
            threshold_type = None
            threshold_value = None
            greater_than = threshold_settings.get("greater_than")
            less_than = threshold_settings.get("less_than")
            
            # Check greater_than threshold
            if greater_than is not None and person_count > greater_than:
                threshold_type = "greater_than"
                threshold_value = greater_than
                logger.info(
                    f"✅ THRESHOLD VIOLATED: {person_count} > {greater_than} "
                    f"(greater_than threshold)"
                )
            
            # Check less_than threshold
            elif less_than is not None and person_count < less_than:
                threshold_type = "less_than"
                threshold_value = less_than
                logger.info(
                    f"✅ THRESHOLD VIOLATED: {person_count} < {less_than} "
                    f"(less_than threshold)"
                )
            
            else:
                logger.debug(
                    f"✅ No threshold violation: count={person_count}, "
                    f"greater_than={greater_than}, less_than={less_than}"
                )
                return

            # ==================== Record Violation ====================
            # Persisted unconditionally (independent of the notification cooldown below)
            # so the threshold-violations chart reflects every real crossing.
            try:
                await self.db_manager.execute_query(
                    """INSERT INTO threshold_violations
                       (violation_id, stream_id, workspace_id, person_count, threshold_type, threshold_value, "timestamp")
                       VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                    (uuid.uuid4(), stream_id, workspace_id, person_count, threshold_type, threshold_value, current_time)
                )
            except Exception as e:
                logger.error(f"Error recording threshold violation for {stream_id}: {e}")

            # ==================== Cooldown Check ====================
            should_notify = False
            cooldown_reason = None
            
            if not alert_state:
                should_notify = True
                cooldown_reason = "first_alert"
                logger.info(f"✅ First people count alert for stream {stream_id_str}")
            else:
                last_notification = alert_state.get("last_notification_time")
                
                if last_notification:
                    # Calculate time since last notification
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    
                    if time_since_last >= cooldown_minutes:
                        should_notify = True
                        cooldown_reason = f"cooldown_expired_{time_since_last:.1f}min"
                        logger.info(
                            f"✅ Cooldown expired for stream {stream_id_str}: "
                            f"{time_since_last:.1f} min since last alert (>= {cooldown_minutes} min)"
                        )
                    else:
                        should_notify = False
                        cooldown_reason = f"cooldown_active_{time_since_last:.1f}min"
                        logger.info(
                            f"⏰ Cooldown active for stream {stream_id_str}: "
                            f"{time_since_last:.1f}/{cooldown_minutes} min remaining"
                        )
                else:
                    should_notify = True
                    cooldown_reason = "no_previous_notification"
                    logger.info(f"✅ No previous notification time for stream {stream_id_str}")
            
            # ==================== Send Notification ====================
            if should_notify:
                logger.warning(
                    f"🚨 SENDING PEOPLE COUNT ALERT for {camera_name}: "
                    f"count={person_count}, threshold={threshold_type}:{threshold_value}, "
                    f"reason={cooldown_reason}"
                )
                
                # Get location info for more detailed alert
                stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
                location_info = None
                location_text = "Unknown Location"
                
                if stream_info:
                    location_info = {
                        'location': stream_info.get('location'),
                        'area': stream_info.get('area'),
                        'building': stream_info.get('building'),
                        'zone': stream_info.get('zone'),
                        'floor_level': stream_info.get('floor_level'),
                    }
                    
                    # Build readable location string
                    location_parts = []
                    if location_info.get('building'):
                        location_parts.append(location_info['building'])
                    if location_info.get('floor_level'):
                        location_parts.append(f"Floor {location_info['floor_level']}")
                    if location_info.get('zone'):
                        location_parts.append(location_info['zone'])
                    if location_info.get('area'):
                        location_parts.append(location_info['area'])
                    
                    location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
                
                # Build alert message with location
                threshold_desc = f">{threshold_value}" if threshold_type == "greater_than" else f"<{threshold_value}"
                message = (
                    f"⚠️ PEOPLE COUNT ALERT: {person_count} people detected "
                    f"(threshold: {threshold_desc}) at {location_text}"
                )
                
                logger.warning(f"🚨 MESSAGE: {message}")
                
                # ==================== Create Database Notification ====================
                try:
                    from app.services.notification_service import notification_service
                    
                    notification = await notification_service.create_notification(
                        workspace_id=workspace_id,
                        user_id=owner_id,
                        status="urgent",
                        message=message,
                        stream_id=stream_id,
                        camera_name=camera_name
                    )
                    
                    if notification:
                        logger.warning(
                            f"✅ People count notification created in database: "
                            f"{notification.get('notification_id')}"
                        )
                    else:
                        logger.error(f"❌ Failed to create people count notification in database")
                        return  # Exit if notification creation failed
                        
                except Exception as notif_err:
                    logger.error(
                        f"❌ Error creating people count notification: {notif_err}", 
                        exc_info=True
                    )
                    return  # Exit if notification creation failed

                # ==================== Broadcast to WebSocket ====================
                if notification and self.stream_manager:
                    try:
                        # Format notification for WebSocket
                        formatted_notification = {
                            "type": "new_notification",
                            "notification": {
                                "id": str(notification.get("notification_id")),
                                "user_id": str(notification.get("user_id")),
                                "workspace_id": str(notification.get("workspace_id")),
                                "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                                "camera_name": notification.get("camera_name"),
                                "status": notification.get("status"),
                                "message": notification.get("message"),
                                "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
                                "read": notification.get("is_read", False)
                            }
                        }
                        
                        # Broadcast to camera owner
                        await self.stream_manager.broadcast_notification(
                            str(owner_id),
                            formatted_notification
                        )
                        logger.warning(
                            f"⚠️ Broadcasted PEOPLE COUNT notification to owner {owner_id} "
                            f"(will appear in notification list)"
                        )
                        
                        # Broadcast to ALL workspace members
                        try:
                            workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
                                workspace_id=workspace_id,
                                current_user_id=workspace_id,
                                is_admin=True
                            )
                            
                            broadcast_count = 0
                            for member in workspace_members:
                                member_id = str(member['user_id'])
                                if member_id != str(owner_id):  # Skip owner (already sent)
                                    try:
                                        await self.stream_manager.broadcast_notification(
                                            member_id,
                                            formatted_notification
                                        )
                                        broadcast_count += 1
                                    except Exception as member_err:
                                        logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                            
                            logger.warning(
                                f"⚠️ Broadcasted people count alert to {broadcast_count} additional workspace members"
                            )
                            
                        except Exception as broadcast_err:
                            logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
                            
                    except Exception as ws_err:
                        logger.error(
                            f"❌ Error broadcasting people count notification: {ws_err}", 
                            exc_info=True
                        )

                # ==================== Send Email Alert ====================
                try:
                    user_info = await user_manager.get_user_by_id(owner_id)
                    
                    if not user_info:
                        logger.error(f"❌ User {owner_id} not found, cannot send email")
                    elif 'email' not in user_info:
                        logger.error(f"❌ User {owner_id} has no email address")
                    else:
                        user_email = user_info["email"]
                        logger.info(f"📧 Preparing to send people count alert email to {user_email}")

                        # Send email with await
                        email_success = await send_people_count_alert_email(
                            user_email=user_email,
                            camera_name=camera_name,
                            person_count=person_count,
                            threshold_settings={
                                "greater_than": greater_than,
                                "less_than": less_than,
                                "alert_enabled": threshold_settings.get("alert_enabled", True)
                            },
                            location_info=location_info
                        )
                        
                        if email_success:
                            logger.warning(f"✅ People count alert email sent to {user_email}")
                        else:
                            logger.warning(f"⚠️ Failed to send email to {user_email} (may be rate-limited)")

                except Exception as email_error:
                    logger.error(
                        f"❌ Error sending people count alert email: {email_error}", 
                        exc_info=True
                    )

                # ==================== Update Alert State ====================
                try:
                    await people_count_service.create_or_update_people_count_alert_state(
                        stream_id=stream_id,
                        last_count=person_count,
                        last_threshold_type=threshold_type,
                        last_notification_time=current_time
                    )
                    logger.info(f"✅ Updated people count alert state for stream {stream_id_str}")
                    
                except Exception as state_err:
                    logger.error(f"❌ Error updating alert state: {state_err}", exc_info=True)
                
                logger.warning(
                    f"✅ People count alert completed for stream {stream_id_str}: "
                    f"count={person_count}, threshold={threshold_desc}, location={location_text}"
                )
            else:
                logger.debug(
                    f"⏭️ People count alert skipped for {stream_id_str}: {cooldown_reason}"
                )
                    
        except Exception as e:
            logger.error(
                f"❌ Error handling people count alert for stream {stream_id_str}: {e}", 
                exc_info=True
            )

    async def _handle_fire_detection_alert(
        self,
        stream_id: UUID,
        stream_id_str: str,
        fire_status: str,
        camera_name: str,
        workspace_id: UUID,
        owner_id: UUID
    ):
        """Handle fire detection alert logic with popup notification."""
        try:
            if fire_status == "no detection":
                return
            
            fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
            current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            cooldown_minutes = 10
            quick_redetection_minutes = 2
            
            # Check if we should send notification
            should_notify = False
            if not fire_state:
                should_notify = True
            else:
                last_notification = fire_state.get("last_notification_time")
                
                if last_notification:
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    if fire_state.get('fire_status') == 'no detection' and time_since_last < quick_redetection_minutes:
                        should_notify = True
                    elif time_since_last >= cooldown_minutes:
                        should_notify = True
                    else:
                        logger.info(
                            f"🔥 Fire alert for {stream_id_str} suppressed "
                            f"(cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
                        )
                else:
                    should_notify = True
            
            if should_notify:
                # Get location info
                stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
                location_info = None
                location_text = "Unknown Location"
                
                if stream_info:
                    location_info = {
                        'location': stream_info.get('location'),
                        'area': stream_info.get('area'),
                        'building': stream_info.get('building'),
                        'zone': stream_info.get('zone'),
                        'floor_level': stream_info.get('floor_level'),
                    }
                    
                    # Build readable location string
                    location_parts = []
                    if location_info.get('building'):
                        location_parts.append(location_info['building'])
                    if location_info.get('floor_level'):
                        location_parts.append(f"Floor {location_info['floor_level']}")
                    if location_info.get('zone'):
                        location_parts.append(location_info['zone'])
                    if location_info.get('area'):
                        location_parts.append(location_info['area'])
                    
                    location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
                
                # ✅ CRITICAL: Use uppercase keywords that your React code checks for
                alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
                
                # ✅ Format message to trigger your existing NotificationMenu popup
                # Your code checks for .toLowerCase().includes("fire") or "smoke"
                message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
                
                # Create database notification
                notification = await notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=owner_id,
                    status="urgent",
                    message=message,  # ← This will trigger your existing React code!
                    stream_id=stream_id,
                    camera_name=camera_name
                )

                # ✅ Send BOTH regular notification AND special fire alert format
                if notification and self.stream_manager:
                    # Format 1: Regular notification (for the notification list)
                    regular_notification = {
                        "type": "new_notification",
                        "notification": {
                            "id": str(notification.get("notification_id")),
                            "user_id": str(notification.get("user_id")),
                            "workspace_id": str(notification.get("workspace_id")),
                            "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                            "camera_name": camera_name,
                            "status": notification.get("status"),
                            "message": message,  # ← Contains "FIRE" or "SMOKE" keyword
                            "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
                            "read": False
                        }
                    }
                    
                    # Broadcast regular notification first
                    await self.stream_manager.broadcast_notification(
                        str(owner_id),
                        regular_notification
                    )
                    
                    logger.warning(
                        f"🔥 Broadcasted FIRE notification to owner {owner_id} "
                        f"(will trigger popup via keyword detection)"
                    )
                    
                    # Also broadcast to all workspace members
                    try:
                        workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
                            workspace_id=workspace_id,
                            current_user_id=workspace_id,
                            is_admin=True
                        )
                        
                        broadcast_count = 0
                        for member in workspace_members:
                            member_id = str(member['user_id'])
                            if member_id != str(owner_id):
                                try:
                                    await self.stream_manager.broadcast_notification(
                                        member_id,
                                        regular_notification
                                    )
                                    broadcast_count += 1
                                except Exception as member_err:
                                    logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                        
                        logger.warning(f"🔥 Broadcasted fire alert to {broadcast_count} workspace members")
                        
                    except Exception as broadcast_err:
                        logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
                
                # Send email alert
                try:
                    user_info = await user_manager.get_user_by_id(owner_id)
                    if user_info and 'email' in user_info:
                        user_email = user_info["email"]
                        logger.info(f"📧 Sending fire alert email to {user_email}")

                        await send_fire_alert_email(
                            user_email=user_email,
                            camera_name=camera_name,
                            fire_status=fire_status,
                            location_info=location_info
                        )
                        logger.info(f"✅ Fire alert email sent to {user_email}")

                except Exception as email_error:
                    logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
                # Update fire detection state
                await fire_detection_service.create_or_update_fire_detection_state(
                    stream_id=stream_id,
                    fire_status=fire_status,
                    last_detection_time=current_time,
                    last_notification_time=current_time
                )
                
                logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status} at {location_text}")
                
        except Exception as e:
            logger.error(f"Error handling Fire/smoke detection alert: {e}", exc_info=True)

    async def process_stream_with_sharing(
        self,
        stream_id: UUID,
        camera_name: str,
        source: str,
        owner_username: str,
        owner_id: UUID,
        workspace_id: UUID,
        stop_event: asyncio.Event,
        location_info: Optional[Dict[str, Any]] = None
    ):
        """
        Main stream processing loop with CRASH PROTECTION.
        
        CRITICAL FIXES:
        1. Catch ALL exceptions to prevent cascade failures
        2. Graceful degradation instead of crashing
        3. Automatic recovery on transient errors
        """

        frame_count = 0
        frames_since_last_save = 0

        from collections import deque
        video_buffer = deque(maxlen=300)   # ~10 s at 30 fps
        last_shoplifting_save_time = None  # time-based dedup — save at most once per 5 min
        shoplifting_engine_load_attempted = False  # throttle: only retry once per stream session

        last_db_update_activity = datetime.now(ZoneInfo("Africa/Cairo"))
        last_heartbeat = datetime.now(ZoneInfo("Africa/Cairo"))

        stream_id_str = str(stream_id)
        loop = asyncio.get_event_loop()
        shared_stream = None
        first_frame_received = False

        max_consecutive_failures = 1000  
        # ✅ NEW: Adaptive failure thresholds
        is_rtsp = source.startswith('rtsp://')
        if is_rtsp:
            max_consecutive_failures = 1000  # More lenient for RTSP
            failure_reset_threshold = 5     # Reset counter after 5 good frames
        else:
            max_consecutive_failures = 30   # Strict for files
            failure_reset_threshold = 1

        consecutive_failures = 0
        consecutive_successes = 0  # ✅ NEW: Track good frames

        logger.info(f"Starting stream processing for {stream_id_str} ({camera_name})")

        # ✅ CRITICAL: Wrap EVERYTHING in try-except to prevent crashes
        try:
            # -------------------- SETTINGS --------------------
            threshold_settings = await self._get_camera_threshold_settings(stream_id)

            from app.services.parameter_service import parameter_service
            params = await parameter_service.get_workspace_params(workspace_id)

            frame_skip = params.get("frame_skip", 300)
            frame_delay_target = params.get("frame_delay", 0.033)
            conf_threshold = params.get("conf", 0.5)

            from app.services.video_stream_service import video_stream_service as _vs_svc
            _stream_db = await _vs_svc.get_video_stream_by_id(stream_id)
            is_shoplifting_camera = bool(_stream_db.get('is_shoplifting_camera', False)) if _stream_db else False
            logger.info(f"[shoplifting] stream={stream_id_str[:8]} is_shoplifting_camera={is_shoplifting_camera}")

            # -------------------- SOURCE --------------------
            if not source.startswith("rtsp://"):
                if not await self._validate_stream_source(source):
                    raise RuntimeError(f"Invalid video source: {source}")

            shared_stream = await self.video_file_manager.get_shared_stream(source)

            if not await shared_stream.add_subscriber(stream_id_str):
                raise RuntimeError("Failed to subscribe to shared stream")

            # -------------------- MAIN LOOP --------------------
            while not stop_event.is_set():
                try:
                    # ✅ CRITICAL: Check stop event frequently
                    if stop_event.is_set():
                        logger.info(f"Stop event detected for {stream_id_str}")
                        break
                    
                    # ---------- WAIT FOR FRAME ----------
                    if not await shared_stream.wait_for_frame(timeout=10):
                        consecutive_failures += 1
                        if consecutive_failures > max_consecutive_failures:
                            logger.error(
                                f"❌ {stream_id_str} exceeded max failures ({max_consecutive_failures})"
                            )
                            break
                        await asyncio.sleep(0.1)
                        continue

                    frame = await shared_stream.get_latest_frame(stream_id_str)
                    if frame is None or frame.size == 0:
                        consecutive_failures += 1
                        consecutive_successes = 0  # ✅ Reset success counter
                        if consecutive_failures > max_consecutive_failures:
                            logger.error(
                                f"❌ {stream_id_str} exceeded max frame failures"
                            )
                            break
                        await asyncio.sleep(0.1)
                        continue

                    consecutive_failures = 0
                    consecutive_successes += 1

                    # ✅ NEW: Gradually forgive old failures
                    if consecutive_successes >= failure_reset_threshold:
                        if consecutive_failures > 0:
                            consecutive_failures = max(0, consecutive_failures - 1)
                            logger.debug(
                                f"✅ Reducing failure count for {stream_id_str}: {consecutive_failures}"
                            )
                            
                    frame_count += 1
                    frames_since_last_save += 1
                    # (no per-frame shoplifting counter needed — dedup is time-based now)
                    
                    # Buffer resized frame to save memory
                    small_frame = cv2.resize(frame, (640, 480))
                    video_buffer.append(small_frame)

                    current_time = datetime.now(ZoneInfo("Africa/Cairo"))

                    # ---------- FIRST FRAME ----------
                    if not first_frame_received:
                        first_frame_received = True
                        await self.update_stream_to_active(stream_id_str)

                    # ===== HEARTBEAT & STATUS UPDATES (EVERY FRAME) =====
                    # Heartbeat
                    if (current_time - last_heartbeat).total_seconds() >= 10:
                        last_heartbeat = current_time

                    # DB status heartbeat (independent of detection)
                    if (current_time - last_db_update_activity).total_seconds() >= 30:
                        last_db_update_activity = current_time
                        if self.stream_manager:
                            try:
                                await self.stream_manager.status_batcher.queue_update(
                                    stream_id, "active", True
                                )
                            except Exception as batch_err:
                                logger.error(f"Error queuing status update: {batch_err}")

                    # ===== SHOPLIFTING DETECTION (independent rate, never gated by frame_skip) =====
                    # Feeds one frame every (frame_skip // 30) video frames so the 120-frame
                    # temporal buffer fills in ~120 * (frame_skip // 30) video frames.
                    is_shoplifting = False
                    shoplifting_conf = 0.0
                    shoplifting_objects = []
                    shoplifting_frame_rate = max(1, frame_skip // 30)
                    if frame_count % shoplifting_frame_rate == 0 and self.shoplifting_engine and is_shoplifting_camera:
                        engine = self.shoplifting_engine
                        # Lazy-load: if startup load failed, retry once in a background thread
                        if not engine.is_loaded and not shoplifting_engine_load_attempted:
                            shoplifting_engine_load_attempted = True
                            logger.warning(f"[shoplifting] engine not loaded for {stream_id_str[:8]}, retrying load_models() in thread")
                            try:
                                await loop.run_in_executor(thread_pool, engine.load_models)
                                logger.info(f"[shoplifting] lazy-load result: is_loaded={engine.is_loaded}")
                            except Exception as _le:
                                logger.error(f"[shoplifting] lazy load_models failed: {_le}")
                        # Log engine state every 300 shoplifting frames for diagnostics
                        shoplifting_tick = frame_count // shoplifting_frame_rate
                        if shoplifting_tick % 300 == 0:
                            buf_len = len(engine.buffers.get(stream_id_str, []))
                            logger.info(
                                f"[shoplifting] stream={stream_id_str[:8]} "
                                f"is_loaded={engine.is_loaded} "
                                f"buf={buf_len}/{engine.num_frames} "
                                f"tick={shoplifting_tick}"
                            )
                        try:
                            shop_label, shop_conf, shop_alert = await loop.run_in_executor(
                                thread_pool,
                                engine.process_frame,
                                stream_id_str,
                                small_frame,
                            )
                            if shop_label not in ("Buffering...", "Disabled"):
                                if shop_alert:
                                    logger.info(f"🚨 Shoplifting detected on stream {stream_id_str} with {shop_conf:.2f} conf")
                                is_shoplifting = shop_alert
                                shoplifting_conf = shop_conf if shop_alert else 0.0
                        except Exception as e:
                            logger.error(f"Shoplifting detection error for stream {stream_id_str}: {e}")

                    # ---------- Shoplifting Event DB Insertion ----------
                    # Save at most once per 5 minutes per stream (matches DB dedup window)
                    _shoplifting_cooldown_ok = (
                        last_shoplifting_save_time is None
                        or (current_time - last_shoplifting_save_time).total_seconds() >= 300
                    )
                    if is_shoplifting and _shoplifting_cooldown_ok:
                        from app.services.s3_service import s3_service
                        from app.services.shoplifting_service import shoplifting_service

                        try:
                            shoplifting_frames = list(video_buffer)

                            current_incident = self._active_incidents.get(stream_id_str)
                            if current_incident and (current_time - current_incident['last_detected']).total_seconds() < 300:
                                incident_session_id = current_incident['session_id']
                                current_incident['last_detected'] = current_time
                            else:
                                incident_session_id = uuid.uuid4()
                                self._active_incidents[stream_id_str] = {
                                    'session_id': incident_session_id,
                                    'last_detected': current_time
                                }

                            async def save_shoplifting_video(frames, s_id, w_id, u_id, conf, objs, session_uuid, cam_name):
                                if not frames:
                                    return

                                try:
                                    temp_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.mp4")

                                    def write_video():
                                        try:
                                            h, w = frames[0].shape[:2]
                                            cmd = [
                                                'ffmpeg', '-y',
                                                '-f', 'rawvideo',
                                                '-vcodec', 'rawvideo',
                                                '-s', f'{w}x{h}',
                                                '-pix_fmt', 'bgr24',
                                                '-r', '10',
                                                '-i', 'pipe:0',
                                                '-c:v', 'libx264',
                                                '-preset', 'fast',
                                                '-crf', '23',
                                                '-pix_fmt', 'yuv420p',
                                                '-movflags', '+faststart',
                                                temp_path,
                                            ]
                                            proc = subprocess.Popen(
                                                cmd,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.DEVNULL,
                                                stderr=subprocess.PIPE,
                                            )
                                            for f in frames:
                                                proc.stdin.write(f.tobytes())
                                            proc.stdin.close()
                                            proc.wait()
                                            if proc.returncode != 0:
                                                logger.error(f"ffmpeg error: {proc.stderr.read().decode()}")
                                                return False
                                            return True
                                        except Exception as e:
                                            logger.error(f"Error writing video: {e}")
                                            return False

                                    success = await asyncio.to_thread(write_video)
                                    if not success:
                                        try:
                                            if os.path.exists(temp_path):
                                                os.remove(temp_path)
                                        except Exception:
                                            pass
                                        return

                                    s3_path = await s3_service.upload_video_file_to_s3(temp_path)

                                    try:
                                        if os.path.exists(temp_path):
                                            os.remove(temp_path)
                                    except Exception:
                                        pass

                                    if s3_path:
                                        await shoplifting_service.insert_surveillance_frame(
                                            session_id=session_uuid,
                                            stream_id=s_id,
                                            workspace_id=w_id,
                                            user_id=u_id,
                                            is_shoplifting=True,
                                            behavior_state="suspicious",
                                            behavior_category="shoplifting",
                                            confidence=float(conf),
                                            objects_detected=objs,
                                            video_path=s3_path,
                                        )
                                        await shoplifting_service.insert_shoplifting_event(
                                            stream_id=s_id,
                                            workspace_id=w_id,
                                            confidence=float(conf),
                                            video_path=s3_path,
                                        )
                                        logger.info(f"✅ Saved shoplifting video to {s3_path}")
                                        try:
                                            notification = await notification_service.create_notification(
                                                workspace_id=w_id,
                                                user_id=u_id,
                                                status="urgent",
                                                message=f"🚨 SHOPLIFTING ALERT: Suspicious behavior detected on {cam_name}",
                                                stream_id=s_id,
                                                camera_name=cam_name,
                                            )
                                            if notification and self.stream_manager:
                                                await self.stream_manager.broadcast_notification(
                                                    str(u_id),
                                                    {
                                                        "type": "new_notification",
                                                        "notification": {
                                                            "id": str(notification.get("notification_id")),
                                                            "user_id": str(notification.get("user_id")),
                                                            "workspace_id": str(notification.get("workspace_id")),
                                                            "stream_id": str(s_id),
                                                            "camera_name": cam_name,
                                                            "status": "urgent",
                                                            "message": notification.get("message"),
                                                            "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                                                            "read": False,
                                                        },
                                                    },
                                                )
                                        except Exception as notif_err:
                                            logger.error(f"Failed to send shoplifting notification: {notif_err}")

                                        # Send email alert
                                        try:
                                            user_info = await user_manager.get_user_by_id(u_id)
                                            if user_info and user_info.get('email'):
                                                stream_info = await video_stream_service.get_video_stream_by_id(s_id)
                                                location_info = None
                                                if stream_info:
                                                    location_info = {
                                                        'location': stream_info.get('location'),
                                                        'area': stream_info.get('area'),
                                                        'building': stream_info.get('building'),
                                                        'zone': stream_info.get('zone'),
                                                        'floor_level': stream_info.get('floor_level'),
                                                    }
                                                # await send_shoplifting_alert_email(
                                                #     user_email=user_info['email'],
                                                #     camera_name=cam_name,
                                                #     confidence=conf,
                                                #     location_info=location_info,
                                                # )
                                        except Exception as email_err:
                                            logger.error(f"Failed to send shoplifting alert email: {email_err}")
                                except Exception:
                                    logger.error(
                                        f"Unhandled exception in save_shoplifting_video for stream {s_id}",
                                        exc_info=True,
                                    )

                            _save_task = asyncio.create_task(save_shoplifting_video(
                                shoplifting_frames, stream_id, workspace_id, owner_id,
                                shoplifting_conf, shoplifting_objects, incident_session_id,
                                camera_name
                            ))
                            _save_task.add_done_callback(_log_shoplifting_task_exception)

                            last_shoplifting_save_time = current_time

                        except Exception as e:
                            logger.error(f"Error triggering shoplifting video save for stream {stream_id_str}: {e}")

                    # ===== DETECTION GATE =====
                    run_detection = (frame_count % frame_skip == 0)

                    # Skip frame if not running detection
                    if not run_detection:
                        await asyncio.sleep(frame_delay_target)
                        continue

                    # ===== DETECTION (with error isolation) =====
                    try:
                        (
                            annotated_frame,
                            person_count,
                            alert_triggered,
                            male_count,
                            female_count,
                            fire_status,
                            is_shoplifting,
                            shoplifting_conf,
                            shoplifting_objects
                        ) = await loop.run_in_executor(
                            thread_pool,
                            self.detect_objects_with_threshold,
                            frame,
                            conf_threshold,
                            threshold_settings,
                            stream_id_str,
                        )
                    except Exception as detection_err:
                        logger.error(
                            f"❌ Detection error for {stream_id_str}: {detection_err}",
                            exc_info=True
                        )
                        # ✅ CRITICAL: Continue processing, don't crash
                        await asyncio.sleep(frame_delay_target)
                        continue

                    # ---------- UPDATE STREAM MANAGER ----------
                    if self.stream_manager:
                        try:
                            async with self.stream_manager._lock:
                                if stream_id_str in self.stream_manager.active_streams:
                                    self.stream_manager.active_streams[stream_id_str].update({
                                        "latest_frame": annotated_frame,
                                        "last_frame_time": current_time,
                                        "last_heartbeat": current_time,
                                        "person_count": person_count,
                                        "male_count": male_count,
                                        "female_count": female_count,
                                        "fire_status": fire_status,
                                    })
                        except Exception as update_err:
                            logger.error(f"Error updating stream manager: {update_err}")

                    # ---------- SAVE (with error isolation) ----------
                    should_save = (
                        frames_since_last_save >= frame_skip or
                        (person_count > 0 and frame_skip > 100 and frames_since_last_save >= 30)
                    )

                    if should_save:
                        try:
                            saved = await self._save_detection(
                                stream_id_str=stream_id_str,
                                camera_name=camera_name,
                                owner_username=owner_username,
                                person_count=person_count,
                                male_count=male_count,
                                female_count=female_count,
                                fire_status=fire_status,
                                frame=annotated_frame,
                                workspace_id=workspace_id,
                                location_info=location_info,
                            )
                            if saved:
                                frames_since_last_save = 0
                        except Exception as save_err:
                            logger.error(f"Error saving detection: {save_err}")
                            # ✅ Don't crash, just log and continue

                    # ---------- ALERTS (with error isolation) ----------
                    try:
                        if alert_triggered and threshold_settings.get("alert_enabled"):
                            await self._handle_people_count_alert(
                                stream_id,
                                stream_id_str,
                                person_count,
                                threshold_settings,
                                camera_name,
                                workspace_id,
                                owner_id,
                            )

                        if fire_status != "no detection":
                            await self._handle_fire_detection_alert(
                                stream_id,
                                stream_id_str,
                                fire_status,
                                camera_name,
                                workspace_id,
                                owner_id,
                            )
                    except Exception as alert_err:
                        logger.error(f"Error handling alerts: {alert_err}")
                        # ✅ Don't crash, just log and continue

                    await asyncio.sleep(frame_delay_target)

                except asyncio.CancelledError:
                    logger.info(f"Stream {stream_id_str} task cancelled")
                    break
                except Exception as loop_err:
                    logger.error(
                        f"❌ LOOP ERROR for {stream_id_str}: {loop_err}",
                        exc_info=True
                    )
                    consecutive_failures += 1
                    if consecutive_failures > max_consecutive_failures:
                        logger.error(f"❌ Too many failures, stopping {stream_id_str}")
                        break
                    await asyncio.sleep(1)

        except Exception as fatal_err:
            # ✅ CRITICAL: Catch fatal errors to prevent cascade
            logger.error(
                f"❌ FATAL ERROR in process_stream for {stream_id_str}: {fatal_err}",
                exc_info=True
            )
        
        finally:
            # ✅ CRITICAL: ALWAYS cleanup, even on crash
            logger.info(f"Cleaning up stream {stream_id_str}")

            try:
                if shared_stream:
                    await shared_stream.remove_subscriber(stream_id_str)
                    
                    # ✅ FIX: If no more subscribers, stop the shared stream
                    async with shared_stream.lock:
                        if not shared_stream.subscribers:
                            logger.info(
                                f"🛑 Last subscriber removed from {source[:50]}, "
                                f"stopping SharedVideoStream"
                            )
                            await shared_stream._stop_capture()
                            
            except Exception as cleanup_err:
                logger.error(f"Error removing subscriber: {cleanup_err}")

            try:
                if self.stream_manager:
                    self.stream_manager.active_streams.pop(stream_id_str, None)
            except Exception as cleanup_err:
                logger.error(f"Error cleaning stream manager: {cleanup_err}")

            logger.info(f"Stream processing completed for {stream_id_str}")

    async def update_stream_to_active(self, stream_id_str: str):
        """Update stream status to 'active' when processing successfully starts."""
        if not self.stream_manager:
            return
            
        async with self.stream_manager._lock:
            if stream_id_str in self.stream_manager.active_streams:
                current_status = self.stream_manager.active_streams[stream_id_str].get('status')
                if current_status == 'active_pending':
                    self.stream_manager.active_streams[stream_id_str]['status'] = 'active'
                    self.stream_manager.active_streams[stream_id_str]['last_frame_time'] = datetime.now(ZoneInfo("Africa/Cairo"))
                    logger.info(f"Stream {stream_id_str} status updated to 'active'")
                    
                    # Update database status - KEEP is_streaming=True
                    try:
                        
                        # CRITICAL: Always explicitly set is_streaming=True
                        current_time = datetime.now(ZoneInfo("Africa/Cairo"))
                        # success = await video_stream_service.update_stream_status(
                        #     UUID(stream_id_str), 'active', is_streaming=True, last_activity=current_time
                        # )
                        if self.stream_manager:
                            await self.stream_manager.status_batcher.queue_update(
                                UUID(stream_id_str), "active", True
                            )
                        
                        # if not success:
                        #     logger.error(f"Failed to update DB status for {stream_id_str}")
                        #     # DON'T set is_streaming=False on error!
                        #     # Just log and continue processing
                            
                    except Exception as e:
                        logger.error(f"Exception updating DB status for {stream_id_str}: {e}")

    def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up StreamProcessingService")
        self._cached_results.clear()
        
        # Shutdown thread pool
        thread_pool.shutdown(wait=False)


stream_processing_service = StreamProcessingService()
