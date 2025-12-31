# app/services/stream_processing_service.py
"""
Stream Processing Service - Handles video frame processing, object detection, and alerts.
Now includes Qdrant data storage after each detection.
"""
import asyncio
import logging
import time
import cv2
import numpy as np
from typing import Dict, Optional, Any, Tuple
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from ultralytics import YOLO
import concurrent.futures
import os

from app.config.settings import config
from app.utils import send_people_count_alert_email, send_fire_alert_email
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
        self.stream_manager = None
        self.video_file_manager = None
        self.qdrant_service = None
        self._cached_results = {}

        # Initialize models
        self._initialize_models()
        
        logger.info("StreamProcessingService initialized")

    def initialize(self, stream_manager, video_file_manager, qdrant_service):
        """Initialize service with dependencies."""
        self.stream_manager = stream_manager
        self.video_file_manager = video_file_manager
        self.qdrant_service = qdrant_service
        logger.info("StreamProcessingService dependencies initialized")

    def _initialize_models(self):
        """Initialize YOLO models."""
        people_model_path = config.get("people_model_path", "yolov8n.pt")
        gender_model_path = config.get("gender_model_path", "gender.pt")
        fire_model_path = config.get("fire_model_path", "fire.pt")
        
        logger.info(f"🔍 Checking model paths:")
        logger.info(f"  People: {people_model_path} (exists: {os.path.exists(people_model_path)})")
        logger.info(f"  Gender: {gender_model_path} (exists: {os.path.exists(gender_model_path)})")
        logger.info(f"  Fire: {fire_model_path} (exists: {os.path.exists(fire_model_path)})")
                
        try:
            self.people_model = YOLO(people_model_path)
            self.people_model.fuse()
            self.gender_model = YOLO(gender_model_path)
            self.gender_model.fuse()
            self.fire_model = YOLO(fire_model_path)
            self.fire_model.fuse()
            logger.info("YOLO models initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize YOLO models: {e}", exc_info=True)
            self.people_model = None
            self.gender_model = None
            self.fire_model = None

    # def _initialize_models(self):
    #     """
    #     Initialize YOLO models with MANDATORY success requirement.
    #     System CANNOT start if people model fails to load.
    #     """
    #     people_model_path = config.get("people_model_path", "yolov8n.pt")
    #     gender_model_path = config.get("gender_model_path", "gender.pt")
    #     fire_model_path = config.get("fire_model_path", "fire.pt")
        
    #     logger.info(f"🔍 Initializing YOLO models:")
    #     logger.info(f"  People: {people_model_path} (exists: {os.path.exists(people_model_path)})")
    #     logger.info(f"  Gender: {gender_model_path} (exists: {os.path.exists(gender_model_path)})")
    #     logger.info(f"  Fire: {fire_model_path} (exists: {os.path.exists(fire_model_path)})")
        
    #     # ===== CRITICAL: People model MUST load (it's mandatory) =====
    #     max_retries = 3
    #     last_error = None
        
    #     for attempt in range(max_retries):
    #         try:
    #             logger.info(f"Loading people model (attempt {attempt + 1}/{max_retries})...")
    #             self.people_model = YOLO(people_model_path)
    #             self.people_model.fuse()
                
    #             # Verify it actually works
    #             test_frame = np.zeros((640, 640, 3), dtype=np.uint8)
    #             test_result = self.people_model.predict(test_frame, verbose=False)
                
    #             logger.info("✅ People model loaded and verified successfully")
    #             break
                
    #         except Exception as e:
    #             last_error = e
    #             logger.error(f"❌ Attempt {attempt + 1} failed to load people model: {e}")
                
    #             if attempt < max_retries - 1:
    #                 import time
    #                 time.sleep(2)  # Wait before retry
    #             else:
    #                 # CRITICAL: MUST raise exception if all retries fail
    #                 error_msg = (
    #                     f"🚨 CRITICAL: Failed to load people model after {max_retries} attempts. "
    #                     f"Last error: {last_error}. "
    #                     f"System CANNOT function without this model!"
    #                 )
    #                 logger.critical(error_msg)
    #                 raise RuntimeError(error_msg) from last_error
        
    #     # ===== Gender model (optional - graceful degradation) =====
    #     try:
    #         logger.info("Loading gender model...")
    #         self.gender_model = YOLO(gender_model_path)
    #         self.gender_model.fuse()
            
    #         # Quick verification
    #         test_frame = np.zeros((640, 640, 3), dtype=np.uint8)
    #         test_result = self.gender_model.predict(test_frame, verbose=False)
            
    #         logger.info("✅ Gender model loaded successfully")
    #     except Exception as e:
    #         logger.warning(f"⚠️ Gender model failed to load: {e}")
    #         logger.warning("Gender detection will be disabled (non-critical)")
    #         self.gender_model = None
        
    #     # ===== Fire model (optional - graceful degradation) =====
    #     try:
    #         logger.info("Loading fire model...")
    #         self.fire_model = YOLO(fire_model_path)
    #         self.fire_model.fuse()
            
    #         # Quick verification
    #         test_frame = np.zeros((640, 640, 3), dtype=np.uint8)
    #         test_result = self.fire_model.predict(test_frame, verbose=False)
            
    #         logger.info("✅ Fire model loaded successfully")
    #     except Exception as e:
    #         logger.warning(f"⚠️ Fire model failed to load: {e}")
    #         logger.warning("Fire detection will be disabled (non-critical)")
    #         self.fire_model = None
        
    #     # ===== Summary =====
    #     models_loaded = ["people"]  # People is always loaded if we get here
    #     if self.gender_model:
    #         models_loaded.append("gender")
    #     if self.fire_model:
    #         models_loaded.append("fire")
        
    #     logger.info(f"✅ Model initialization complete: {', '.join(models_loaded)}")
        
    def get_model_status(self) -> Dict[str, bool]:
        """Get status of all models for health checks."""
        return {
            "people_model": self.people_model is not None,
            "gender_model": self.gender_model is not None,
            "fire_model": self.fire_model is not None
        }

    def detect_objects_with_threshold(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.5,
        threshold_settings: Dict[str, Any] = None,
        stream_id_str: str = None
    ) -> Tuple[np.ndarray, int, bool, int, int, str]:
        """
        Detect objects in frame with threshold checking.
        Returns: (annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status)
        """
        if frame is None or frame.size == 0:
            return np.zeros((100, 100, 3), dtype=np.uint8), 0, False, 0, 0, "no detection"

        # ✅ CRITICAL CHECK: This should NEVER happen now
        if self.people_model is None:
            error_msg = f"🚨 CRITICAL: People model not initialized for stream {stream_id_str}"
            logger.error(error_msg)
            
            # This is a system-level failure - should trigger service restart
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

        # Resize frame if needed
        max_dim = config.get("yolo_max_input_dim", 640)
        h, w = frame.shape[:2]
        scale = 1.0
        
        if h > max_dim or w > max_dim:
            scale = max_dim / max(h, w)
            new_w, new_h = int(w * scale), int(h * scale)
            new_w = max(2, new_w - (new_w % 2))
            new_h = max(2, new_h - (new_h % 2))
            input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        else:
            input_frame = frame

        try:
            # People detection - NOW SAFE because we checked above
            people_results = self.people_model.predict(
                source=input_frame, conf=conf_threshold, classes=[0], verbose=False
            )

            person_count = 0
            if people_results and len(people_results) > 0 and people_results[0].boxes is not None:
                person_count = len(people_results[0].boxes)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 3 == 0 and self.gender_model:
                try:
                    gender_results = self.gender_model.predict(source=input_frame, conf=0.5, verbose=False)
                    if gender_results and len(gender_results) > 0 and gender_results[0].boxes is not None:
                        male_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 1)
                        female_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 0)
                        cache['male_count'] = male_count
                        cache['female_count'] = female_count
                        cache['last_gender_frame'] = frame_count
                except Exception as e:
                    logger.error(f"Gender detection error for stream {stream_id_str}: {e}")

            # Fire detection (every 10th frame) - ✅ Check model first
            if frame_count % 10 == 0 and self.fire_model:
                try:
                    fire_results = self.fire_model.predict(source=input_frame, conf=0.8, verbose=False)

                    current_fire_status = "no detection"
                    if fire_results and len(fire_results) > 0 and fire_results[0].boxes is not None:
                        # classes = [int(box.cls) for box in fire_results[0].boxes]
                        classes = [int(box.cls[0]) for box in fire_results[0].boxes]
                        if 0 in classes:
                            current_fire_status = "fire"
                        elif 1 in classes:
                            current_fire_status = "smoke"

                    previous_fire_status = cache['fire_status']
                    cache['fire_status'] = current_fire_status
                    cache['last_fire_frame'] = frame_count
                    
                    # Log significant changes
                    if current_fire_status != previous_fire_status:
                        if current_fire_status in ["fire", "smoke"]:
                            logger.warning(f"🔥 Fire detection change: {stream_id_str} changed to '{current_fire_status}'")
                        else:
                            logger.info(f"🌊 Fire cleared: {stream_id_str} changed to 'no detection'")
                    
                except Exception as e:
                    logger.error(f"Fire detection error for stream {stream_id_str}: {e}")

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

            # Annotate frame
            annotated_frame = input_frame.copy()
            if people_results and len(people_results) > 0 and people_results[0].boxes is not None:
                annotated_frame = people_results[0].plot(img=annotated_frame)
            
            # Add text overlays
            count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
            count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
            cv2.putText(
                annotated_frame, count_text, (10, 20),
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA
            )
            
            # Fire/smoke overlay
            if fire_status != "no detection":
                fire_color = (0, 0, 255)
                fire_text = f"ALERT: {fire_status.upper()}"
                cv2.putText(
                    annotated_frame, fire_text, (10, 50),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, fire_color, 2, cv2.LINE_AA
                )
                
                # Blinking effect
                if frame_count % 20 < 10:
                    red_overlay = annotated_frame.copy()
                    red_overlay[:] = (0, 0, 255)
                    annotated_frame = cv2.addWeighted(annotated_frame, 0.9, red_overlay, 0.1, 0)

            # Alert styling for people threshold
            if alert_triggered:
                red_overlay = annotated_frame.copy()
                red_overlay[:] = (0, 0, 255)
                annotated_frame = cv2.addWeighted(annotated_frame, 0.85, red_overlay, 0.15, 0)
                
            # Scale back if needed
            if scale != 1.0:
                annotated_frame = cv2.resize(annotated_frame, (w, h), interpolation=cv2.INTER_LINEAR)
                
            return annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status
            
        except Exception as e:
            logger.error(f"Object detection error for stream {stream_id_str}: {e}", exc_info=True)
            return frame.copy(), 0, False, 0, 0, "no detection"

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
        Save detection data to both Qdrant and PostgreSQL databases.
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

    async def _save_detection_old(
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
        Save detection data to both Qdrant and PostgreSQL databases.
        This is called periodically based on frame_skip configuration.
        """
        qdrant_success = False
        postgres_success = False
        
        try:
            # Save to Qdrant
            if self.qdrant_service:
                qdrant_success = await self.qdrant_service.insert_detection_data(
                    username=owner_username,
                    camera_id_str=stream_id_str,
                    camera_name=camera_name,
                    count=person_count,
                    male_count=male_count,
                    female_count=female_count,
                    fire_status=fire_status,
                    frame=frame,
                    workspace_id=workspace_id,
                    location_info=location_info
                )
                
                if qdrant_success:
                    logger.debug(f"✅ Qdrant: Saved detection data for stream {stream_id_str}")
                else:
                    logger.warning(f"⚠️ Qdrant: Failed to save detection data for stream {stream_id_str}")
            else:
                logger.warning("Qdrant service not initialized, skipping Qdrant save")
            
            # Save to PostgreSQL
            if self.postgres_service:
                # Get user_id from stream info
                stream_info = await video_stream_service.get_video_stream_by_id(UUID(stream_id_str))
                
                if stream_info:
                    postgres_success = await self.postgres_service.insert_detection_data(
                        stream_id=UUID(stream_id_str),
                        workspace_id=workspace_id,
                        user_id=stream_info['user_id'],
                        camera_name=camera_name,
                        username=owner_username,
                        person_count=person_count,
                        male_count=male_count,
                        female_count=female_count,
                        fire_status=fire_status,
                        frame=None,
                        location_info=location_info,
                        save_frame=False
                    )
                    
                    if postgres_success:
                        logger.debug(f"✅ PostgreSQL: Saved detection data for stream {stream_id_str}")
                    else:
                        logger.warning(f"⚠️ PostgreSQL: Failed to save detection data for stream {stream_id_str}")
                else:
                    logger.error(f"Could not get stream info for {stream_id_str}, skipping PostgreSQL save")
            else:
                logger.warning("PostgreSQL service not initialized, skipping PostgreSQL save")
            
            # Return success if at least one database succeeded
            overall_success = qdrant_success or postgres_success
            
            if overall_success:
                logger.debug(
                    f"Detection saved for {stream_id_str}: "
                    f"count={person_count}, male={male_count}, female={female_count}, fire={fire_status} "
                    f"(Qdrant: {'✓' if qdrant_success else '✗'}, PostgreSQL: {'✓' if postgres_success else '✗'})"
                )
            
            return overall_success
            
        except Exception as e:
            logger.error(f"Error saving detection data for stream {stream_id_str}: {e}", exc_info=True)
            return False

    async def verify_qdrant_save(
        self,
        workspace_id: UUID,
        stream_id_str: str,
        max_wait: float = 5.0
    ) -> bool:
        """
        Verify that data was actually saved to Qdrant.
        Returns True if data found, False otherwise.
        """
        try:
            if not self.qdrant_service:
                return False
            
            start_time = time.time()
            
            # Try to query recent data for this stream
            while time.time() - start_time < max_wait:
                try:
                    # Query Qdrant for recent detections
                    results = await self.qdrant_service.query_detections(
                        workspace_id=workspace_id,
                        camera_id=stream_id_str,
                        limit=1
                    )
                    
                    if results and len(results) > 0:
                        logger.info(f"✅ Verified Qdrant save for stream {stream_id_str}")
                        return True
                    
                except Exception as e:
                    logger.debug(f"Query failed during verification: {e}")
                
                await asyncio.sleep(0.5)
            
            logger.warning(f"⚠️ Could not verify Qdrant save for stream {stream_id_str}")
            return False
            
        except Exception as e:
            logger.error(f"Error verifying Qdrant save: {e}")
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
        """Handle people count alert logic with workspace-wide broadcasting."""
        try:
            # Get alert state
            alert_state = await people_count_service.get_people_count_alert_state(stream_id)
            
            current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            cooldown_minutes = 10
            
            # Determine threshold type
            threshold_type = None
            greater_than = threshold_settings.get("greater_than")
            less_than = threshold_settings.get("less_than")
            
            if greater_than is not None and person_count > greater_than:
                threshold_type = "greater_than"
            elif less_than is not None and person_count < less_than:
                threshold_type = "less_than"
            
            if not threshold_type:
                logger.debug(f"No threshold violated for stream {stream_id_str}")
                return
            
            # Check cooldown
            should_notify = False
            if not alert_state:
                should_notify = True
                logger.info(f"First people count alert for stream {stream_id_str}")
            else:
                last_notification = alert_state.get("last_notification_time")
                if last_notification:
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    if time_since_last >= cooldown_minutes:
                        should_notify = True
                        logger.info(
                            f"Cooldown expired for stream {stream_id_str}: "
                            f"{time_since_last:.1f} min since last alert"
                        )
                    else:
                        logger.info(
                            f"Cooldown active for stream {stream_id_str}: "
                            f"{time_since_last:.1f}/{cooldown_minutes} min"
                        )
                else:
                    should_notify = True
                    logger.info(f"No previous notification time for stream {stream_id_str}")
            
            if should_notify:
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
                threshold_desc = f">{greater_than}" if threshold_type == "greater_than" else f"<{less_than}"
                message = (
                    f"⚠️ PEOPLE COUNT ALERT: {person_count} people detected "
                    f"(threshold: {threshold_desc}) at {location_text}"
                )
                
                logger.warning(f"🚨 PEOPLE COUNT ALERT: {message}")
                
                # ✅ Create notification in database
                try:
                    notification = await notification_service.create_notification(
                        workspace_id=workspace_id,
                        user_id=owner_id,
                        status="urgent",
                        message=message,
                        stream_id=stream_id,
                        camera_name=camera_name
                    )
                    
                    if notification:
                        logger.info(f"✅ People count notification created: {notification.get('notification_id')}")
                    else:
                        logger.error(f"❌ Failed to create people count notification in database")
                        return  # Exit if notification creation failed
                        
                except Exception as notif_err:
                    logger.error(f"❌ Error creating people count notification: {notif_err}", exc_info=True)
                    return  # Exit if notification creation failed

                # ✅ Broadcast to WebSocket clients
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
                        
                        # 🔥 NEW: Broadcast to ALL workspace members (like fire alerts!)
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
                        logger.error(f"❌ Error broadcasting people count notification: {ws_err}", exc_info=True)

                # ✅ Send email alert
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
                            logger.info(f"✅ People count alert email sent to {user_email}")
                        else:
                            logger.warning(f"⚠️ Failed to send email to {user_email} (may be rate-limited)")

                except Exception as email_error:
                    logger.error(
                        f"❌ Error sending people count alert email: {email_error}", 
                        exc_info=True
                    )

                # ✅ Update alert state in database
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
        """Main stream processing loop."""
        frame_count = 0
        frames_since_last_save = 0
        last_db_update_activity = datetime.now(ZoneInfo("Africa/Cairo"))
        last_heartbeat = datetime.now(ZoneInfo("Africa/Cairo"))
        stream_id_str = str(stream_id)
        loop = asyncio.get_event_loop()
        shared_stream = None
        first_frame_received = False
        consecutive_failures = 0
        max_consecutive_failures = 30
        
        # Initialize fire detection state
        if self.stream_manager:
            self.stream_manager.fire_detection_states[stream_id_str] = {
                'status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }
            self.stream_manager.fire_detection_frame_counts[stream_id_str] = 0
            
        logger.info(f"Starting stream processing for {stream_id_str} ({camera_name})")
        
        try:
            # Get threshold settings
            threshold_settings = await self._get_camera_threshold_settings(stream_id)
            
            # Validate source
            if not source.startswith('rtsp://'):
                if not await self._validate_stream_source(source):
                    logger.error(f"Invalid or inaccessible video source: {source}")
                    # DON'T update database here - let caller handle it
                    raise RuntimeError(f"Invalid or inaccessible video source: {source}")
            else:
                logger.info(f"Skipping pre-validation for RTSP stream {stream_id_str}")

            # Get stream parameters
            from app.services.parameter_service import parameter_service
            params = await parameter_service.get_workspace_params(workspace_id)
            frame_skip = params.get("frame_skip", 300)
            frame_delay_target = params.get("frame_delay", 0.033)
            conf_threshold = params.get("conf", 0.5)
            
            logger.info(f"Stream {stream_id_str} parameters: frame_skip={frame_skip}, "
                    f"frame_delay={frame_delay_target}, conf={conf_threshold}")
            
            # Get or create shared stream
            shared_stream = await self.video_file_manager.get_shared_stream(source)
            
            # Add this stream as a subscriber
            if not await shared_stream.add_subscriber(stream_id_str):
                logger.error(f"Failed to add subscriber {stream_id_str} to shared stream")
                # DON'T update database here - let caller handle it
                raise RuntimeError(f"Failed to subscribe to shared stream for {source}")
            
            logger.info(f"Stream {stream_id_str} subscribed to shared stream for {source}")
            
            # Main processing loop
            while not stop_event.is_set():
                try:
                    current_time = datetime.now(ZoneInfo("Africa/Cairo"))
                    if (current_time - last_heartbeat).total_seconds() >= 10:
                        if self.stream_manager:
                            async with self.stream_manager._lock:
                                if stream_id_str in self.stream_manager.active_streams:
                                    self.stream_manager.active_streams[stream_id_str]['last_heartbeat'] = current_time
                        last_heartbeat = current_time
                    
                    # Wait for frame
                    if not await shared_stream.wait_for_frame(timeout=10.0):
                        consecutive_failures += 1
                        
                        if consecutive_failures % 3 == 0:
                            logger.debug(f"Stream {stream_id_str} waiting for frames... ({consecutive_failures} attempts)")
                        
                        if consecutive_failures > max_consecutive_failures:
                            logger.error(f"Too many consecutive frame wait timeouts for {stream_id_str}")
                            # DON'T set status='error' with is_streaming=False
                            # Just break and let finally block handle cleanup
                            break
                        await asyncio.sleep(0.1)
                        continue
                    
                    # Get frame
                    frame = await shared_stream.get_latest_frame(stream_id_str)
                    
                    if frame is None or frame.size == 0:
                        consecutive_failures += 1
                        if consecutive_failures > max_consecutive_failures:
                            logger.error(f"Too many consecutive empty frames for {stream_id_str}")
                            # DON'T set status='error' with is_streaming=False
                            break
                        await asyncio.sleep(0.1)
                        continue
                    
                    # Reset failure counter
                    consecutive_failures = 0
                    
                    # Mark as active after first frame
                    if not first_frame_received:
                        first_frame_received = True
                        await self.update_stream_to_active(stream_id_str)
                        logger.info(f"First frame received for stream {stream_id_str}")
                    
                    frame_count += 1
                    frames_since_last_save += 1
                    
                    # Process frame
                    annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status = \
                        await loop.run_in_executor(
                            thread_pool,
                            self.detect_objects_with_threshold,
                            frame,
                            conf_threshold,
                            threshold_settings,
                            stream_id_str
                        )
                    
                    # Update stream manager
                    if self.stream_manager:
                        current_time_utc = datetime.now(ZoneInfo("Africa/Cairo"))
                        async with self.stream_manager._lock:
                            if stream_id_str in self.stream_manager.active_streams:
                                self.stream_manager.active_streams[stream_id_str]['latest_frame'] = annotated_frame
                                self.stream_manager.active_streams[stream_id_str]['last_frame_time'] = current_time_utc
                                self.stream_manager.active_streams[stream_id_str]['last_heartbeat'] = current_time_utc
                                self.stream_manager.active_streams[stream_id_str]['person_count'] = person_count
                                self.stream_manager.active_streams[stream_id_str]['male_count'] = male_count
                                self.stream_manager.active_streams[stream_id_str]['female_count'] = female_count
                                self.stream_manager.active_streams[stream_id_str]['fire_status'] = fire_status

                        if stream_id_str in self.stream_manager.fire_detection_states:
                            fire_state = self.stream_manager.fire_detection_states[stream_id_str]
                            fire_state['status'] = fire_status
                            if fire_status != 'no detection':
                                fire_state['last_detection_time'] = current_time_utc
                    
                    # Update stats
                    if stream_id_str in self.stream_manager.stream_processing_stats:
                        self.stream_manager.stream_processing_stats[stream_id_str]['frames_processed'] += 1
                        self.stream_manager.stream_processing_stats[stream_id_str]['last_updated'] = datetime.now(ZoneInfo("Africa/Cairo"))
                    
                    # Save to Qdrant
                    should_save_to_qdrant = False
                    if frames_since_last_save >= frame_skip:
                        should_save_to_qdrant = True
                    elif person_count > 0 and frame_skip > 100 and frames_since_last_save >= 30:
                        should_save_to_qdrant = True
                    
                    if should_save_to_qdrant:
                        save_success = await self._save_detection(
                            stream_id_str=stream_id_str,
                            camera_name=camera_name,
                            owner_username=owner_username,
                            person_count=person_count,
                            male_count=male_count,
                            female_count=female_count,
                            fire_status=fire_status,
                            frame=frame,
                            workspace_id=workspace_id,
                            location_info=location_info
                        )
                        
                        if save_success:
                            frames_since_last_save = 0
                            if self.stream_manager and stream_id_str in self.stream_manager.stream_processing_stats:
                                self.stream_manager.stream_processing_stats[stream_id_str]['detection_count'] = \
                                    self.stream_manager.stream_processing_stats[stream_id_str].get('detection_count', 0) + 1
                    
                    # Handle alerts
                    if alert_triggered and threshold_settings.get("alert_enabled"):
                        await self._handle_people_count_alert(
                            stream_id, stream_id_str, person_count,
                            threshold_settings, camera_name, workspace_id, owner_id
                        )
                    
                    if fire_status != "no detection":
                        await self._handle_fire_detection_alert(
                            stream_id, stream_id_str, fire_status,
                            camera_name, workspace_id, owner_id
                        )
                    
                    # Periodic database updates - CRITICAL: Keep is_streaming=True
                    current_time = datetime.now(ZoneInfo("Africa/Cairo"))
                    if (current_time - last_db_update_activity).total_seconds() >= 30:
                        
                        # # 🔥 CRITICAL FIX: Check if we're being stopped BEFORE updating database
                        # if stop_event.is_set():
                        #     logger.debug(f"Skipping periodic DB update for {stream_id_str} - stop signal received")
                        #     break  # Exit the loop instead of updating
                                                
                        logger.debug(f"📊 Periodic update: {stream_id_str} -> status=active, is_streaming=TRUE")
                        try:
                            # await video_stream_service.update_stream_status(
                            #     stream_id, "active", is_streaming=True, last_activity=current_time  
                            # )
                            if self.stream_manager:
                                await self.stream_manager.status_batcher.queue_update(
                                    stream_id, "active", True
                                )

                        except Exception as e:
                            logger.error(f"Error in periodic DB update for {stream_id_str}: {e}")
                            # DON'T break - continue processing even if DB update fails
                    
                    # Frame delay
                    if frame_delay_target > 0:
                        await asyncio.sleep(frame_delay_target)
                    else:
                        await asyncio.sleep(0.01)
                    
                except asyncio.CancelledError:
                    logger.info(f"Stream processing cancelled for {stream_id_str}")
                    break
                except Exception as e:
                    logger.error(f"Error in processing loop for {stream_id_str}: {e}", exc_info=True)
                    consecutive_failures += 1
                    if consecutive_failures > max_consecutive_failures:
                        logger.error(f"Too many consecutive errors for {stream_id_str}")
                        break
                    await asyncio.sleep(1)
            
        except ConnectionError as e:
            logger.error(f"Connection error for {stream_id_str}: {e}")
            stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
            current_retry_count = stream_info.get('retry_count', 0) if stream_info else 0
            await self.retry_service.schedule_retry(
                stream_id=stream_id,
                stop_reason='connection_error',
                current_retry_count=current_retry_count,
                error_context=str(e)
            )
            raise
            
        except TimeoutError as e:
            logger.error(f"Timeout error for {stream_id_str}: {e}")
            stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
            current_retry_count = stream_info.get('retry_count', 0) if stream_info else 0
            await self.retry_service.schedule_retry(
                stream_id=stream_id,
                stop_reason='timeout',
                current_retry_count=current_retry_count,
                error_context=str(e)
            )
            raise
            
        except Exception as e:
            logger.error(f"System error for {stream_id_str}: {e}", exc_info=True)
            stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
            current_retry_count = stream_info.get('retry_count', 0) if stream_info else 0
            await self.retry_service.schedule_retry(
                stream_id=stream_id,
                stop_reason='system_error',
                current_retry_count=current_retry_count,
                error_context=str(e)
            )
            raise

        finally:
            # Cleanup
            logger.info(f"Cleaning up stream {stream_id_str}. Total frames: {frame_count}")
            
            if shared_stream:
                await shared_stream.remove_subscriber(stream_id_str)
                logger.info(f"Removed subscriber {stream_id_str} from shared stream")
            
            # Clear cache
            cache_key = f"cache_{stream_id_str}"
            if cache_key in self._cached_results:
                del self._cached_results[cache_key]

            # Clean up orphaned cache entries
            if self.stream_manager:
                for cache_id in list(self._cached_results.keys()):
                    if cache_id.startswith("cache_"):
                        sid = cache_id.replace("cache_", "")
                        if sid not in self.stream_manager.active_streams:
                            del self._cached_results[cache_id]
            
            # Keep fire detection state briefly
            if self.stream_manager and stream_id_str in self.stream_manager.fire_detection_states:
                self.stream_manager.fire_detection_states[stream_id_str]['status'] = 'no detection'
                self.stream_manager.fire_detection_states[stream_id_str]['last_detection_time'] = datetime.now(ZoneInfo("Africa/Cairo"))

            # ✅ CRITICAL: Check database state before doing anything
            try:
                db_check = await video_stream_service.db_manager.execute_query(
                    "SELECT stop_reason, is_streaming, status FROM video_stream WHERE stream_id = $1",
                    (stream_id,),
                    fetch_one=True
                )
                
                if not db_check:
                    logger.error(f"Stream {stream_id_str} not found in database during cleanup")
                    return
                
                # ✅ CRITICAL FIX: If user stopped it, don't touch the database AT ALL!
                if db_check['stop_reason'] == 'user_action':
                    logger.info(
                        f"✅ Stream {stream_id_str} was user-stopped "
                        f"(stop_reason='user_action', is_streaming={db_check['is_streaming']}). "
                        f"Skipping ALL database operations in finally block."
                    )
                    return
                
                # If database says is_streaming=FALSE and it's not user_action,
                # still don't touch it (might be workspace disabled, etc)
                if db_check['is_streaming'] == False:
                    logger.info(
                        f"✅ Stream {stream_id_str} already marked is_streaming=FALSE in database "
                        f"(stop_reason={db_check['stop_reason']}). Skipping database update."
                    )
                    return
                
                # ✅ NEW CHECK: If stop event was set gracefully (like from stop API)
                # Check if database was already updated by the API call
                if stop_event.is_set():
                    # Double-check database - API might have already updated it
                    current_db_state = await video_stream_service.db_manager.execute_query(
                        "SELECT stop_reason, is_streaming FROM video_stream WHERE stream_id = $1",
                        (stream_id,),
                        fetch_one=True
                    )
                    
                    if current_db_state and current_db_state['stop_reason'] == 'user_action':
                        logger.info(
                            f"✅ Stop event set AND database shows user_action. "
                            f"API already handled database update. Skipping."
                        )
                        return
                
                # Only update database if:
                # 1. is_streaming=TRUE (stream should still be running but crashed)
                # 2. stop_reason != 'user_action'
                # 3. NOT gracefully stopped by API
                
                if stop_event.is_set():
                    # Graceful stop (but not user-initiated based on checks above)
                    logger.info(
                        f"Stream {stream_id_str} stopped gracefully (system-initiated). "
                        f"Recording as system_error to allow retry."
                    )
                    
                    await video_stream_service.record_camera_stop(
                        stream_id=stream_id,
                        stop_reason='user_action',
                        stopped_by=None,
                        additional_context="Stream stopped cleanly by system"
                    )
                else:
                    # Unexpected termination
                    logger.warning(
                        f"⚠️ Stream {stream_id_str} ended unexpectedly. "
                        f"Recording as system_error."
                    )
                    
                    await video_stream_service.record_camera_stop(
                        stream_id=stream_id,
                        stop_reason='system_error',
                        stopped_by=None,
                        additional_context="Stream ended unexpectedly"
                    )
                
            except Exception as e:
                logger.error(f"Error in finally block for {stream_id_str}: {e}", exc_info=True)
            
            logger.info(f"Stream processing completed for {stream_id_str}")

    async def process_single_frame(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.5
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Process a single frame and return annotated frame with detection data."""
        loop = asyncio.get_event_loop()
        
        annotated_frame, person_count, _, male_count, female_count, fire_status = \
            await loop.run_in_executor(
                thread_pool,
                self.detect_objects_with_threshold,
                frame,
                conf_threshold,
                None,
                None
            )
        
        detection_data = {
            "person_count": person_count,
            "male_count": male_count,
            "female_count": female_count,
            "fire_status": fire_status
        }
        
        return annotated_frame, detection_data

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
