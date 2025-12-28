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
            self.gender_model = YOLO(gender_model_path)
            self.fire_model = YOLO(fire_model_path)
            logger.info("YOLO models initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize YOLO models: {e}", exc_info=True)
            self.people_model = None
            self.gender_model = None
            self.fire_model = None

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
            # People detection
            people_results = self.people_model.predict(
                source=input_frame, conf=conf_threshold, classes=[0], verbose=False
            )

            person_count = 0
            if people_results and len(people_results) > 0 and people_results[0].boxes is not None:
                person_count = len(people_results[0].boxes)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 10 == 0 and self.gender_model:
                try:
                    gender_results = self.gender_model(source=input_frame, conf=0.5, verbose=False)
                    if gender_results and len(gender_results) > 0 and gender_results[0].boxes is not None:
                        male_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 1)
                        female_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 0)
                        cache['male_count'] = male_count
                        cache['female_count'] = female_count
                        cache['last_gender_frame'] = frame_count
                except Exception as e:
                    logger.error(f"Gender detection error for stream {stream_id_str}: {e}")

            # Fire detection (every 10th frame)
            if frame_count % 10 == 0 and self.fire_model:
                try:
                    fire_results = self.fire_model(source=input_frame, conf=0.8, verbose=False)

                    current_fire_status = "no detection"
                    if fire_results and len(fire_results) > 0 and fire_results[0].boxes is not None:
                        classes = [int(box.cls) for box in fire_results[0].boxes]
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
        """Handle people count alert logic."""
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
                return
            
            # Check cooldown
            should_notify = False
            if not alert_state:
                should_notify = True
            else:
                last_notification = alert_state.get("last_notification_time")
                if last_notification:
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    if time_since_last >= cooldown_minutes:
                        should_notify = True
                else:
                    should_notify = True
            
            if should_notify:
                message = f"People count alert: {person_count} people detected (threshold: {threshold_type})"
                
                # ✅ Create notification
                notification = await notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=owner_id,
                    status="unread",
                    message=message,
                    stream_id=stream_id,
                    camera_name=camera_name
                )

                # ✅ CRITICAL: Broadcast to WebSocket clients
                if notification and self.stream_manager:
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
                            "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                            "read": notification.get("is_read", False)
                        }
                    }
                    
                    await self.stream_manager.broadcast_notification(
                        str(owner_id),
                        formatted_notification
                    )
                    logger.info(f"✅ Broadcasted people count notification to user {owner_id}")

                # ✅ FIX: Fetch user email and send email
                try:
                    user_info  = await user_manager.get_user_by_id(owner_id)
                    if user_info and 'email' in user_info:
                        user_email = user_info["email"]

                        stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
                        location_info = None
                        if stream_info:
                            location_info = {
                                'location': stream_info.get('location'),
                                'area': stream_info.get('area'),
                                'building': stream_info.get('building'),
                                'zone': stream_info.get('zone'),
                                'floor_level': stream_info.get('floor_level'),
                                'latitude': stream_info.get('latitude'),
                                'longitude': stream_info.get('longitude')
                            }

                        # ✅ FIXED: Properly await email sending
                        await send_people_count_alert_email(
                            user_email=user_email,
                            camera_name=camera_name,
                            person_count=person_count,
                            threshold_settings=threshold_settings,
                            location_info=location_info
                        )
                        logger.info(f"✅ People count alert email sent to {user_email}")

                except Exception as email_error:
                    logger.error(f"❌ Error sending people count alert email: {email_error}", exc_info=True)

                # Update alert state
                await people_count_service.create_or_update_people_count_alert_state(
                    stream_id=stream_id,
                    last_count=person_count,
                    last_threshold_type=threshold_type,
                    last_notification_time=current_time
                )
                
                logger.info(f"People count alert sent for stream {stream_id_str}")
                
        except Exception as e:
            logger.error(f"Error handling people count alert: {e}", exc_info=True)

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

    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic - sends notification every 10 minutes only.
    #     No popup, just adds to notification list.
    #     """
    #     try:
    #         if fire_status == "no detection":
    #             return
            
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # ============= COOLDOWN SETTINGS =============
    #         cooldown_minutes = 10  # Send notification every 10 minutes
    #         quick_redetection_minutes = 2  # Quick alert if fire returns after clearing
            
    #         # ============= CHECK IF SHOULD SEND NOTIFICATION =============
    #         should_notify = False
            
    #         if not fire_state:
    #             # First time detecting fire
    #             should_notify = True
    #             logger.warning(f"🔥 NEW fire detection: {stream_id_str} - {fire_status}")
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             if previous_status == 'no detection':
    #                 # Fire returned after being clear
    #                 if last_notification:
    #                     time_since_last = (current_time - last_notification).total_seconds() / 60
    #                     if time_since_last < quick_redetection_minutes:
    #                         # Quick re-detection within 2 minutes
    #                         should_notify = True
    #                         logger.warning(f"🔥 Fire returned quickly: {stream_id_str}")
    #                     else:
    #                         # Fire was clear for a while, now back
    #                         should_notify = True
    #                         logger.warning(f"🔥 Fire returned: {stream_id_str}")
    #                 else:
    #                     should_notify = True
    #             else:
    #                 # Fire was already active, check cooldown
    #                 if last_notification:
    #                     time_since_last = (current_time - last_notification).total_seconds() / 60
                        
    #                     if time_since_last >= cooldown_minutes:
    #                         # 10 minutes passed, send another notification
    #                         should_notify = True
    #                         logger.warning(
    #                             f"🔥 Fire still active ({time_since_last:.1f} min since last notification): {stream_id_str}"
    #                         )
    #                     else:
    #                         # Within cooldown period, suppress notification
    #                         logger.debug(
    #                             f"🔥 Fire detected but suppressed (cooldown: {time_since_last:.1f}/{cooldown_minutes} min): {stream_id_str}"
    #                         )
    #                 else:
    #                     should_notify = True
            
    #         # ============= SEND NOTIFICATION (IF NEEDED) =============
    #         if should_notify:
    #             # Get location info
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = "Unknown Location"
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 # Build location string
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
                
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
                
    #             # ✅ Create notification in database
    #             notification = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )

    #             # ✅ Broadcast to WebSocket (just adds to notification list, NO popup)
    #             if notification and self.stream_manager:
    #                 notification_data = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification.get("notification_id")),
    #                         "user_id": str(notification.get("user_id")),
    #                         "workspace_id": str(notification.get("workspace_id")),
    #                         "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": notification.get("status"),
    #                         "message": message,
    #                         "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 # Broadcast to owner
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data
    #                 )
                    
    #                 logger.warning(
    #                     f"🔥 Fire notification sent to owner {owner_id} "
    #                     f"(added to notification list)"
    #                 )
                    
    #                 # Also broadcast to workspace members
    #                 try:
    #                     workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                         workspace_id=workspace_id,
    #                         current_user_id=workspace_id,
    #                         is_admin=True
    #                     )
                        
    #                     broadcast_count = 0
    #                     for member in workspace_members:
    #                         member_id = str(member['user_id'])
    #                         if member_id != str(owner_id):
    #                             try:
    #                                 await self.stream_manager.broadcast_notification(
    #                                     member_id,
    #                                     notification_data
    #                                 )
    #                                 broadcast_count += 1
    #                             except Exception as member_err:
    #                                 logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                        
    #                     logger.warning(f"🔥 Fire notification broadcasted to {broadcast_count} workspace members")
                        
    #                 except Exception as broadcast_err:
    #                     logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
                
    #             # ✅ Send email alert
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     user_email = user_info["email"]
    #                     logger.info(f"📧 Sending fire alert email to {user_email}")

    #                     await send_fire_alert_email(
    #                         user_email=user_email,
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent to {user_email}")

    #             except Exception as email_error:
    #                 logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
    #             # ✅ Update fire detection state WITH notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ← This prevents spam
    #             )
                
    #             logger.warning(
    #                 f"🔥 Fire notification sent for {stream_id_str}: {fire_status} at {location_text}"
    #             )
            
    #         else:
    #             # ✅ Just update detection time, DON'T update notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← Keep old notification time
    #             )
                
    #             logger.debug(
    #                 f"🔥 Fire still detected but within cooldown: {stream_id_str} - {fire_status}"
    #             )
                    
    #     except Exception as e:
    #         logger.error(f"Error handling fire/smoke detection alert: {e}", exc_info=True)
            

    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert - ONLY sends notification every 10 minutes.
    #     This prevents the popup from appearing every 5 seconds.
    #     """
    #     try:
    #         if fire_status == "no detection":
    #             return
            
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
    #         cooldown_minutes = 10  # Only notify every 10 minutes
    #         quick_redetection_minutes = 2
            
    #         # ============= CHECK COOLDOWN =============
    #         should_notify = False
            
    #         if not fire_state:
    #             # First detection
    #             should_notify = True
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             if previous_status == 'no detection' and fire_status != 'no detection':
    #                 # Fire just started after being clear
    #                 if last_notification:
    #                     time_since_last = (current_time - last_notification).total_seconds() / 60
    #                     if time_since_last < quick_redetection_minutes:
    #                         should_notify = True
    #                     elif time_since_last >= cooldown_minutes:
    #                         should_notify = True
    #                 else:
    #                     should_notify = True
    #             elif previous_status != 'no detection' and last_notification:
    #                 # Fire ongoing, check cooldown
    #                 time_since_last = (current_time - last_notification).total_seconds() / 60
    #                 if time_since_last >= cooldown_minutes:
    #                     should_notify = True
    #                 else:
    #                     # ✅ WITHIN COOLDOWN - DO NOT NOTIFY
    #                     logger.debug(
    #                         f"🔥 Fire detected but SUPPRESSED (cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
    #                     )
    #             elif not last_notification:
    #                 should_notify = True
            
    #         # ============= ONLY SEND IF COOLDOWN PASSED =============
    #         if should_notify:
    #             # Get location
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = "Unknown Location"
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
                
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
                
    #             # Create notification (this triggers your popup because of "FIRE" keyword)
    #             notification = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )

    #             # Broadcast notification
    #             if notification and self.stream_manager:
    #                 notification_data = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification.get("notification_id")),
    #                         "user_id": str(notification.get("user_id")),
    #                         "workspace_id": str(notification.get("workspace_id")),
    #                         "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": notification.get("status"),
    #                         "message": message,  # Contains "FIRE" keyword
    #                         "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data
    #                 )
                    
    #                 logger.warning(f"🔥 Fire notification sent to {owner_id}")
                    
    #                 # Broadcast to workspace members
    #                 try:
    #                     workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                         workspace_id=workspace_id,
    #                         current_user_id=workspace_id,
    #                         is_admin=True
    #                     )
                        
    #                     for member in workspace_members:
    #                         member_id = str(member['user_id'])
    #                         if member_id != str(owner_id):
    #                             try:
    #                                 await self.stream_manager.broadcast_notification(
    #                                     member_id,
    #                                     notification_data
    #                                 )
    #                             except Exception as e:
    #                                 logger.error(f"Error broadcasting to member {member_id}: {e}")
                        
    #                 except Exception as e:
    #                     logger.error(f"Error broadcasting to workspace: {e}")
                
    #             # Send email
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     await send_fire_alert_email(
    #                         user_email=user_info["email"],
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent")
    #             except Exception as e:
    #                 logger.error(f"❌ Error sending email: {e}", exc_info=True)
                
    #             # ✅ UPDATE NOTIFICATION TIME (this is the key!)
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ← Prevents next notification for 10 min
    #             )
                
    #             logger.warning(f"🔥 Fire notification sent for {stream_id_str}")
            
    #         else:
    #             # ✅ DON'T update notification time - just update detection time
    #             # This keeps fire as "detected" but doesn't trigger notification
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← Don't change notification time
    #             )
                    
    #     except Exception as e:
    #         logger.error(f"Error handling fire alert: {e}", exc_info=True)

    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic - sends notification every 10 minutes only.
    #     No popup, just adds to notification list.
    #     """
    #     try:
    #         if fire_status == "no detection":
    #             return
            
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # ============= COOLDOWN SETTINGS =============
    #         cooldown_minutes = 10  # Send notification every 10 minutes
    #         quick_redetection_minutes = 2  # Quick alert if fire returns after clearing
            
    #         # ============= CHECK IF SHOULD SEND NOTIFICATION =============
    #         should_notify = False
            
    #         if not fire_state:
    #             # First time detecting fire
    #             should_notify = True
    #             logger.warning(f"🔥 NEW fire detection: {stream_id_str} - {fire_status}")
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             if previous_status == 'no detection':
    #                 # Fire returned after being clear
    #                 if last_notification:
    #                     time_since_last = (current_time - last_notification).total_seconds() / 60
    #                     if time_since_last < quick_redetection_minutes:
    #                         # Quick re-detection within 2 minutes
    #                         should_notify = True
    #                         logger.warning(f"🔥 Fire returned quickly: {stream_id_str}")
    #                     else:
    #                         # Fire was clear for a while, now back
    #                         should_notify = True
    #                         logger.warning(f"🔥 Fire returned: {stream_id_str}")
    #                 else:
    #                     should_notify = True
    #             else:
    #                 # Fire was already active, check cooldown
    #                 if last_notification:
    #                     time_since_last = (current_time - last_notification).total_seconds() / 60
                        
    #                     if time_since_last >= cooldown_minutes:
    #                         # 10 minutes passed, send another notification
    #                         should_notify = True
    #                         logger.warning(
    #                             f"🔥 Fire still active ({time_since_last:.1f} min since last notification): {stream_id_str}"
    #                         )
    #                     else:
    #                         # Within cooldown period, suppress notification
    #                         logger.debug(
    #                             f"🔥 Fire detected but suppressed (cooldown: {time_since_last:.1f}/{cooldown_minutes} min): {stream_id_str}"
    #                         )
    #                 else:
    #                     should_notify = True
            
    #         # ============= SEND NOTIFICATION (IF NEEDED) =============
    #         if should_notify:
    #             # Get location info
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = "Unknown Location"
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 # Build location string
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
                
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
                
    #             # ✅ Create notification in database
    #             notification = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )

    #             # ✅ Broadcast to WebSocket (just adds to notification list, NO popup)
    #             if notification and self.stream_manager:
    #                 notification_data = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification.get("notification_id")),
    #                         "user_id": str(notification.get("user_id")),
    #                         "workspace_id": str(notification.get("workspace_id")),
    #                         "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": notification.get("status"),
    #                         "message": message,
    #                         "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 # Broadcast to owner
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data
    #                 )
                    
    #                 logger.warning(
    #                     f"🔥 Fire notification sent to owner {owner_id} "
    #                     f"(added to notification list)"
    #                 )
                    
    #                 # Also broadcast to workspace members
    #                 try:
    #                     workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                         workspace_id=workspace_id,
    #                         current_user_id=workspace_id,
    #                         is_admin=True
    #                     )
                        
    #                     broadcast_count = 0
    #                     for member in workspace_members:
    #                         member_id = str(member['user_id'])
    #                         if member_id != str(owner_id):
    #                             try:
    #                                 await self.stream_manager.broadcast_notification(
    #                                     member_id,
    #                                     notification_data
    #                                 )
    #                                 broadcast_count += 1
    #                             except Exception as member_err:
    #                                 logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                        
    #                     logger.warning(f"🔥 Fire notification broadcasted to {broadcast_count} workspace members")
                        
    #                 except Exception as broadcast_err:
    #                     logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
                
    #             # ✅ Send email alert
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     user_email = user_info["email"]
    #                     logger.info(f"📧 Sending fire alert email to {user_email}")

    #                     await send_fire_alert_email(
    #                         user_email=user_email,
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent to {user_email}")

    #             except Exception as email_error:
    #                 logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
    #             # ✅ Update fire detection state WITH notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ← This prevents spam
    #             )
                
    #             logger.warning(
    #                 f"🔥 Fire notification sent for {stream_id_str}: {fire_status} at {location_text}"
    #             )
            
    #         else:
    #             # ✅ Just update detection time, DON'T update notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← Keep old notification time
    #             )
                
    #             logger.debug(
    #                 f"🔥 Fire still detected but within cooldown: {stream_id_str} - {fire_status}"
    #             )
                    
    #     except Exception as e:
    #         logger.error(f"Error handling fire/smoke detection alert: {e}", exc_info=True)

    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic - sends notification every 10 minutes only.
    #     No popup, just adds to notification list.
    #     """
    #     try:
    #         if fire_status == "no detection":
    #             # Fire cleared - update state but don't send notification
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=datetime.now(ZoneInfo("Africa/Cairo")),
    #                 last_notification_time=None  # Don't update notification time
    #             )
    #             logger.info(f"🌊 Fire cleared for {stream_id_str}")
    #             return
            
    #         # Get existing fire state from database
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # ============= COOLDOWN SETTINGS =============
    #         cooldown_seconds = 600  # 10 minutes in seconds
    #         quick_redetection_seconds = 120  # 2 minutes in seconds
            
    #         # ============= DETERMINE IF WE SHOULD SEND NOTIFICATION =============
    #         should_notify = False
    #         notification_reason = ""
            
    #         if not fire_state:
    #             # First time detecting fire - always notify
    #             should_notify = True
    #             notification_reason = "first_detection"
    #             logger.warning(f"🔥 NEW fire detection: {stream_id_str} - {fire_status}")
    #         else:
    #             previous_status = fire_state.get('fire_status', 'no detection')
    #             last_notification_time = fire_state.get('last_notification_time')
                
    #             if previous_status == 'no detection' and fire_status in ['fire', 'smoke']:
    #                 # Fire returned after being clear
    #                 if last_notification_time:
    #                     time_since_last_notification = (current_time - last_notification_time).total_seconds()
                        
    #                     if time_since_last_notification < quick_redetection_seconds:
    #                         # Quick re-detection within 2 minutes - notify immediately
    #                         should_notify = True
    #                         notification_reason = "quick_redetection"
    #                         logger.warning(f"🔥 Fire returned quickly after {time_since_last_notification:.1f}s: {stream_id_str}")
    #                     elif time_since_last_notification >= cooldown_seconds:
    #                         # Fire was clear for a while, now back - notify
    #                         should_notify = True
    #                         notification_reason = "fire_returned_after_cooldown"
    #                         logger.warning(f"🔥 Fire returned after {time_since_last_notification:.1f}s: {stream_id_str}")
    #                     else:
    #                         # Within cooldown, suppress
    #                         should_notify = False
    #                         logger.debug(
    #                             f"🔥 Fire returned but within cooldown "
    #                             f"({time_since_last_notification:.1f}s / {cooldown_seconds}s): {stream_id_str}"
    #                         )
    #                 else:
    #                     # No previous notification, send one
    #                     should_notify = True
    #                     notification_reason = "fire_returned_no_previous_notification"
    #                     logger.warning(f"🔥 Fire returned (no previous notification): {stream_id_str}")
                
    #             elif previous_status in ['fire', 'smoke']:
    #                 # Fire was already active, check cooldown
    #                 if last_notification_time:
    #                     time_since_last_notification = (current_time - last_notification_time).total_seconds()
                        
    #                     if time_since_last_notification >= cooldown_seconds:
    #                         # Cooldown period passed - send another notification
    #                         should_notify = True
    #                         notification_reason = "cooldown_expired"
    #                         logger.warning(
    #                             f"🔥 Fire still active, cooldown expired "
    #                             f"({time_since_last_notification:.1f}s / {cooldown_seconds}s): {stream_id_str}"
    #                         )
    #                     else:
    #                         # Still within cooldown - suppress notification
    #                         should_notify = False
    #                         remaining = cooldown_seconds - time_since_last_notification
    #                         logger.debug(
    #                             f"🔥 Fire detected but SUPPRESSED - cooldown active: {stream_id_str} "
    #                             f"(elapsed: {time_since_last_notification:.1f}s, remaining: {remaining:.1f}s)"
    #                         )
    #                 else:
    #                     # Fire active but no previous notification time - send one
    #                     should_notify = True
    #                     notification_reason = "no_previous_notification"
    #                     logger.warning(f"🔥 Fire active but no notification time set: {stream_id_str}")
            
    #         # ============= ALWAYS UPDATE DETECTION TIME (NOT NOTIFICATION TIME) =============
    #         if not should_notify:
    #             # Just update that we detected fire, but DON'T update notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← CRITICAL: Keep existing notification time
    #             )
    #             logger.debug(
    #                 f"🔥 Fire still detected but within cooldown: {stream_id_str} - {fire_status} "
    #                 f"(no notification sent)"
    #             )
    #             return
            
    #         # ============= SEND NOTIFICATION (COOLDOWN HAS PASSED) =============
    #         logger.warning(
    #             f"🔥 Sending fire notification: {stream_id_str} - {fire_status} "
    #             f"(reason: {notification_reason})"
    #         )
            
    #         # Get location info
    #         stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #         location_info = None
    #         location_text = "Unknown Location"
            
    #         if stream_info:
    #             location_info = {
    #                 'location': stream_info.get('location'),
    #                 'area': stream_info.get('area'),
    #                 'building': stream_info.get('building'),
    #                 'zone': stream_info.get('zone'),
    #                 'floor_level': stream_info.get('floor_level'),
    #             }
                
    #             # Build location string
    #             location_parts = []
    #             if location_info.get('building'):
    #                 location_parts.append(location_info['building'])
    #             if location_info.get('floor_level'):
    #                 location_parts.append(f"Floor {location_info['floor_level']}")
    #             if location_info.get('zone'):
    #                 location_parts.append(location_info['zone'])
    #             if location_info.get('area'):
    #                 location_parts.append(location_info['area'])
                
    #             location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
            
    #         alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #         message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
            
    #         # ✅ Create notification in database
    #         notification = await notification_service.create_notification(
    #             workspace_id=workspace_id,
    #             user_id=owner_id,
    #             status="urgent",
    #             message=message,
    #             stream_id=stream_id,
    #             camera_name=camera_name
    #         )

    #         # ✅ Broadcast to WebSocket
    #         if notification and self.stream_manager:
    #             notification_data = {
    #                 "type": "new_notification",
    #                 "notification": {
    #                     "id": str(notification.get("notification_id")),
    #                     "user_id": str(notification.get("user_id")),
    #                     "workspace_id": str(notification.get("workspace_id")),
    #                     "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                     "camera_name": camera_name,
    #                     "status": notification.get("status"),
    #                     "message": message,
    #                     "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                     "read": False
    #                 }
    #             }
                
    #             # Broadcast to owner
    #             await self.stream_manager.broadcast_notification(
    #                 str(owner_id),
    #                 notification_data
    #             )
                
    #             logger.warning(
    #                 f"🔥 Fire notification sent to owner {owner_id} "
    #                 f"(added to notification list)"
    #             )
                
    #             # Also broadcast to workspace members
    #             try:
    #                 workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                     workspace_id=workspace_id,
    #                     current_user_id=workspace_id,
    #                     is_admin=True
    #                 )
                    
    #                 broadcast_count = 0
    #                 for member in workspace_members:
    #                     member_id = str(member['user_id'])
    #                     if member_id != str(owner_id):
    #                         try:
    #                             await self.stream_manager.broadcast_notification(
    #                                 member_id,
    #                                 notification_data
    #                             )
    #                             broadcast_count += 1
    #                         except Exception as member_err:
    #                             logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                    
    #                 logger.warning(f"🔥 Fire notification broadcasted to {broadcast_count} workspace members")
                    
    #             except Exception as broadcast_err:
    #                 logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
            
    #         # ✅ Send email alert
    #         try:
    #             user_info = await user_manager.get_user_by_id(owner_id)
    #             if user_info and 'email' in user_info:
    #                 user_email = user_info["email"]
    #                 logger.info(f"📧 Sending fire alert email to {user_email}")

    #                 await send_fire_alert_email(
    #                     user_email=user_email,
    #                     camera_name=camera_name,
    #                     fire_status=fire_status,
    #                     location_info=location_info
    #                 )
    #                 logger.info(f"✅ Fire alert email sent to {user_email}")

    #         except Exception as email_error:
    #             logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
            
    #         # ✅ CRITICAL: Update BOTH detection time AND notification time
    #         # This resets the cooldown timer
    #         await fire_detection_service.create_or_update_fire_detection_state(
    #             stream_id=stream_id,
    #             fire_status=fire_status,
    #             last_detection_time=current_time,
    #             last_notification_time=current_time  # ← CRITICAL: This starts the 10-minute cooldown
    #         )
            
    #         logger.warning(
    #             f"🔥 Fire notification completed for {stream_id_str}: {fire_status} at {location_text} "
    #             f"(next notification allowed in {cooldown_seconds}s)"
    #         )
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire/smoke detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic.
        
    #     Behavior:
    #     - First fire detection → Send notification immediately
    #     - If fire continues → Send notification every 10 minutes
    #     - If fire clears then returns → Send notification immediately
    #     - If fire continues → Send notification every 10 minutes
    #     - If fire clears then returns → Send notification immediately
    #     - If fire continues → Send notification every 10 minutes
    #     """
    #     try:
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # ============= CONFIGURATION =============
    #         COOLDOWN_MINUTES = 10
    #         COOLDOWN_SECONDS = COOLDOWN_MINUTES * 60  # 600 seconds
            
    #         # ============= HANDLE FIRE CLEARED =============
    #         if fire_status == "no detection":
    #             # Fire cleared - just update state, no notification
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status="no detection",
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # Don't change notification time
    #             )
    #             logger.info(f"🌊 Fire cleared for {stream_id_str}")
    #             return
            
    #         # ============= GET EXISTING STATE =============
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         # ============= DETERMINE IF SHOULD SEND NOTIFICATION =============
    #         should_send_notification = False
    #         reason = ""
            
    #         if not fire_state:
    #             # No previous state - first detection ever
    #             should_send_notification = True
    #             reason = "first_detection"
    #             logger.warning(f"🔥 FIRST DETECTION: {stream_id_str} - {fire_status}")
                
    #         else:
    #             previous_status = fire_state.get('fire_status', 'no detection')
    #             last_notification_time = fire_state.get('last_notification_time')
                
    #             if previous_status == 'no detection':
    #                 # Fire just started (was clear before)
    #                 should_send_notification = True
    #                 reason = "fire_started"
    #                 logger.warning(f"🔥 FIRE STARTED: {stream_id_str} - {fire_status}")
                    
    #             elif previous_status in ['fire', 'smoke']:
    #                 # Fire was already active - check cooldown
                    
    #                 if not last_notification_time:
    #                     # Fire active but no notification time recorded (shouldn't happen)
    #                     should_send_notification = True
    #                     reason = "missing_notification_time"
    #                     logger.warning(f"🔥 NOTIFICATION TIME MISSING: {stream_id_str}")
    #                 else:
    #                     # Calculate time since last notification
    #                     time_elapsed = (current_time - last_notification_time).total_seconds()
    #                     time_remaining = COOLDOWN_SECONDS - time_elapsed
                        
    #                     if time_elapsed >= COOLDOWN_SECONDS:
    #                         # Cooldown expired - send notification
    #                         should_send_notification = True
    #                         reason = f"cooldown_expired_after_{time_elapsed/60:.1f}min"
    #                         logger.warning(
    #                             f"🔥 COOLDOWN EXPIRED: {stream_id_str} - Fire still active after "
    #                             f"{time_elapsed/60:.1f} minutes. Sending periodic notification."
    #                         )
    #                     else:
    #                         # Still in cooldown - DO NOT send notification
    #                         should_send_notification = False
    #                         logger.debug(
    #                             f"🔥 COOLDOWN ACTIVE: {stream_id_str} - Notification suppressed. "
    #                             f"Time remaining: {time_remaining/60:.1f} minutes "
    #                             f"({time_elapsed:.0f}s / {COOLDOWN_SECONDS}s elapsed)"
    #                         )
            
    #         # ============= UPDATE STATE (ALWAYS) =============
    #         # Always update detection time to track that fire is still present
    #         if should_send_notification:
    #             # Send notification AND update notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ← Reset cooldown timer
    #             )
    #         else:
    #             # Just update detection time, keep existing notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← Don't change notification time
    #             )
    #             # EXIT - Don't send notification
    #             return
            
    #         # ============= SEND NOTIFICATION =============
    #         logger.warning(
    #             f"🔥🔥🔥 SENDING FIRE NOTIFICATION: {stream_id_str} - {fire_status} "
    #             f"(Reason: {reason})"
    #         )
            
    #         # Get location info
    #         stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #         location_info = None
    #         location_text = "Unknown Location"
            
    #         if stream_info:
    #             location_info = {
    #                 'location': stream_info.get('location'),
    #                 'area': stream_info.get('area'),
    #                 'building': stream_info.get('building'),
    #                 'zone': stream_info.get('zone'),
    #                 'floor_level': stream_info.get('floor_level'),
    #             }
                
    #             location_parts = []
    #             if location_info.get('building'):
    #                 location_parts.append(location_info['building'])
    #             if location_info.get('floor_level'):
    #                 location_parts.append(f"Floor {location_info['floor_level']}")
    #             if location_info.get('zone'):
    #                 location_parts.append(location_info['zone'])
    #             if location_info.get('area'):
    #                 location_parts.append(location_info['area'])
                
    #             location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
            
    #         alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #         message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
            
    #         # Create database notification
    #         notification = await notification_service.create_notification(
    #             workspace_id=workspace_id,
    #             user_id=owner_id,
    #             status="urgent",
    #             message=message,
    #             stream_id=stream_id,
    #             camera_name=camera_name
    #         )

    #         # Broadcast to WebSocket
    #         if notification and self.stream_manager:
    #             notification_data = {
    #                 "type": "new_notification",
    #                 "notification": {
    #                     "id": str(notification.get("notification_id")),
    #                     "user_id": str(notification.get("user_id")),
    #                     "workspace_id": str(notification.get("workspace_id")),
    #                     "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                     "camera_name": camera_name,
    #                     "status": notification.get("status"),
    #                     "message": message,
    #                     "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                     "read": False
    #                 }
    #             }
                
    #             # Broadcast to stream owner
    #             await self.stream_manager.broadcast_notification(
    #                 str(owner_id),
    #                 notification_data
    #             )
                
    #             logger.warning(f"✅ Fire notification sent to owner {owner_id}")
                
    #             # Broadcast to all workspace members
    #             try:
    #                 workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                     workspace_id=workspace_id,
    #                     current_user_id=workspace_id,
    #                     is_admin=True
    #                 )
                    
    #                 broadcast_count = 0
    #                 for member in workspace_members:
    #                     member_id = str(member['user_id'])
    #                     if member_id != str(owner_id):
    #                         try:
    #                             await self.stream_manager.broadcast_notification(
    #                                 member_id,
    #                                 notification_data
    #                             )
    #                             broadcast_count += 1
    #                         except Exception as member_err:
    #                             logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                    
    #                 logger.warning(f"✅ Fire notification sent to {broadcast_count} workspace members")
                    
    #             except Exception as broadcast_err:
    #                 logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
            
    #         # Send email alert
    #         try:
    #             user_info = await user_manager.get_user_by_id(owner_id)
    #             if user_info and 'email' in user_info:
    #                 user_email = user_info["email"]
    #                 logger.info(f"📧 Sending fire alert email to {user_email}")

    #                 await send_fire_alert_email(
    #                     user_email=user_email,
    #                     camera_name=camera_name,
    #                     fire_status=fire_status,
    #                     location_info=location_info
    #                 )
    #                 logger.info(f"✅ Fire alert email sent to {user_email}")

    #         except Exception as email_error:
    #             logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
            
    #         logger.warning(
    #             f"🔥 NOTIFICATION COMPLETE: {stream_id_str} - Next notification allowed "
    #             f"in {COOLDOWN_MINUTES} minutes at {(current_time + timedelta(minutes=COOLDOWN_MINUTES)).strftime('%H:%M:%S')}"
    #         )
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire/smoke detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic - REPEATING CYCLE.
        
    #     Behavior (repeats infinitely):
    #     1. First fire detection → Send notification immediately
    #     2. If fire continues → Send notification every 10 minutes
    #     3. If fire clears → No notification, just log
    #     4. If fire returns → Send notification immediately (restart cycle)
    #     5. If fire continues → Send notification every 10 minutes
    #     6. Repeat steps 3-5 forever
        
    #     Example timeline:
    #     - 10:00 → Fire detected → NOTIFY ✅
    #     - 10:05 → Fire still there → No notification (cooldown)
    #     - 10:10 → Fire still there → NOTIFY ✅ (10 min passed)
    #     - 10:15 → Fire still there → No notification (cooldown)
    #     - 10:20 → Fire still there → NOTIFY ✅ (10 min passed)
    #     - 10:22 → Fire cleared → No notification
    #     - 10:25 → Fire returns → NOTIFY ✅ (immediate on return)
    #     - 10:30 → Fire still there → No notification (cooldown)
    #     - 10:35 → Fire still there → NOTIFY ✅ (10 min passed)
    #     - ... continues forever
    #     """
    #     try:
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # ============= CONFIGURATION =============
    #         COOLDOWN_MINUTES = 10
    #         COOLDOWN_SECONDS = COOLDOWN_MINUTES * 60  # 600 seconds
            
    #         # ============= HANDLE FIRE CLEARED =============
    #         if fire_status == "no detection":
    #             # Fire cleared - just update state, no notification
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status="no detection",
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # Don't change notification time
    #             )
    #             logger.info(f"🌊 Fire cleared for {stream_id_str}")
    #             return
            
    #         # ============= GET EXISTING STATE =============
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         # ============= DETERMINE IF SHOULD SEND NOTIFICATION =============
    #         should_send_notification = False
    #         reason = ""
            
    #         if not fire_state:
    #             # No previous state - first detection ever
    #             should_send_notification = True
    #             reason = "first_detection"
    #             logger.warning(f"🔥 FIRST DETECTION: {stream_id_str} - {fire_status}")
                
    #         else:
    #             previous_status = fire_state.get('fire_status', 'no detection')
    #             last_notification_time = fire_state.get('last_notification_time')
                
    #             if previous_status == 'no detection':
    #                 # Fire just started (was clear before)
    #                 should_send_notification = True
    #                 reason = "fire_started"
    #                 logger.warning(f"🔥 FIRE STARTED: {stream_id_str} - {fire_status}")
                    
    #             elif previous_status in ['fire', 'smoke']:
    #                 # Fire was already active - check cooldown
                    
    #                 if not last_notification_time:
    #                     # Fire active but no notification time recorded (shouldn't happen)
    #                     should_send_notification = True
    #                     reason = "missing_notification_time"
    #                     logger.warning(f"🔥 NOTIFICATION TIME MISSING: {stream_id_str}")
    #                 else:
    #                     # Calculate time since last notification
    #                     time_elapsed = (current_time - last_notification_time).total_seconds()
    #                     time_remaining = COOLDOWN_SECONDS - time_elapsed
                        
    #                     if time_elapsed >= COOLDOWN_SECONDS:
    #                         # Cooldown expired - send notification
    #                         should_send_notification = True
    #                         reason = f"cooldown_expired_after_{time_elapsed/60:.1f}min"
    #                         logger.warning(
    #                             f"🔥 COOLDOWN EXPIRED: {stream_id_str} - Fire still active after "
    #                             f"{time_elapsed/60:.1f} minutes. Sending periodic notification."
    #                         )
    #                     else:
    #                         # Still in cooldown - DO NOT send notification
    #                         should_send_notification = False
    #                         logger.debug(
    #                             f"🔥 COOLDOWN ACTIVE: {stream_id_str} - Notification suppressed. "
    #                             f"Time remaining: {time_remaining/60:.1f} minutes "
    #                             f"({time_elapsed:.0f}s / {COOLDOWN_SECONDS}s elapsed)"
    #                         )
            
    #         # ============= UPDATE STATE (ALWAYS) =============
    #         # Always update detection time to track that fire is still present
    #         if should_send_notification:
    #             # Send notification AND update notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ← Reset cooldown timer
    #             )
    #         else:
    #             # Just update detection time, keep existing notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=None  # ← Don't change notification time
    #             )
    #             # EXIT - Don't send notification
    #             return
            
    #         # ============= SEND NOTIFICATION =============
    #         logger.warning(
    #             f"🔥🔥🔥 SENDING FIRE NOTIFICATION: {stream_id_str} - {fire_status} "
    #             f"(Reason: {reason})"
    #         )
            
    #         # Get location info
    #         stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #         location_info = None
    #         location_text = "Unknown Location"
            
    #         if stream_info:
    #             location_info = {
    #                 'location': stream_info.get('location'),
    #                 'area': stream_info.get('area'),
    #                 'building': stream_info.get('building'),
    #                 'zone': stream_info.get('zone'),
    #                 'floor_level': stream_info.get('floor_level'),
    #             }
                
    #             location_parts = []
    #             if location_info.get('building'):
    #                 location_parts.append(location_info['building'])
    #             if location_info.get('floor_level'):
    #                 location_parts.append(f"Floor {location_info['floor_level']}")
    #             if location_info.get('zone'):
    #                 location_parts.append(location_info['zone'])
    #             if location_info.get('area'):
    #                 location_parts.append(location_info['area'])
                
    #             location_text = " - ".join(location_parts) if location_parts else location_info.get('location', 'Unknown Location')
            
    #         alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #         message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
            
    #         # Create database notification
    #         notification = await notification_service.create_notification(
    #             workspace_id=workspace_id,
    #             user_id=owner_id,
    #             status="urgent",
    #             message=message,
    #             stream_id=stream_id,
    #             camera_name=camera_name
    #         )

    #         # Broadcast to WebSocket
    #         if notification and self.stream_manager:
    #             notification_data = {
    #                 "type": "new_notification",
    #                 "notification": {
    #                     "id": str(notification.get("notification_id")),
    #                     "user_id": str(notification.get("user_id")),
    #                     "workspace_id": str(notification.get("workspace_id")),
    #                     "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                     "camera_name": camera_name,
    #                     "status": notification.get("status"),
    #                     "message": message,
    #                     "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                     "read": False
    #                 }
    #             }
                
    #             # Broadcast to stream owner
    #             await self.stream_manager.broadcast_notification(
    #                 str(owner_id),
    #                 notification_data
    #             )
                
    #             logger.warning(f"✅ Fire notification sent to owner {owner_id}")
                
    #             # Broadcast to all workspace members
    #             try:
    #                 workspace_members = await self.stream_manager.workspace_service.get_workspace_members(
    #                     workspace_id=workspace_id,
    #                     current_user_id=workspace_id,
    #                     is_admin=True
    #                 )
                    
    #                 broadcast_count = 0
    #                 for member in workspace_members:
    #                     member_id = str(member['user_id'])
    #                     if member_id != str(owner_id):
    #                         try:
    #                             await self.stream_manager.broadcast_notification(
    #                                 member_id,
    #                                 notification_data
    #                             )
    #                             broadcast_count += 1
    #                         except Exception as member_err:
    #                             logger.error(f"Error broadcasting to member {member_id}: {member_err}")
                    
    #                 logger.warning(f"✅ Fire notification sent to {broadcast_count} workspace members")
                    
    #             except Exception as broadcast_err:
    #                 logger.error(f"Error broadcasting to workspace members: {broadcast_err}")
            
    #         # Send email alert
    #         try:
    #             user_info = await user_manager.get_user_by_id(owner_id)
    #             if user_info and 'email' in user_info:
    #                 user_email = user_info["email"]
    #                 logger.info(f"📧 Sending fire alert email to {user_email}")

    #                 await send_fire_alert_email(
    #                     user_email=user_email,
    #                     camera_name=camera_name,
    #                     fire_status=fire_status,
    #                     location_info=location_info
    #                 )
    #                 logger.info(f"✅ Fire alert email sent to {user_email}")

    #         except Exception as email_error:
    #             logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
            
    #         logger.warning(
    #             f"🔥 NOTIFICATION COMPLETE: {stream_id_str} - Next notification allowed "
    #             f"in {COOLDOWN_MINUTES} minutes at {(current_time + timedelta(minutes=COOLDOWN_MINUTES)).strftime('%H:%M:%S')}"
    #         )
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire/smoke detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """Handle fire detection alert logic."""
    #     try:
    #         if fire_status == "no detection":
    #             return
            
            
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
    #         cooldown_minutes = 10
    #         quick_redetection_minutes = 2
            
    #         # Check if we should send notification
    #         should_notify = False
    #         if not fire_state:
    #             should_notify = True
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
                
    #             # STRICT COOLDOWN: Always check time elapsed
    #             if last_notification:
    #                 time_since_last = (current_time - last_notification).total_seconds() / 60
    #                 if fire_state.get('fire_status') == 'no detection' and time_since_last < quick_redetection_minutes:
    #                     should_notify = True
    #                 elif time_since_last >= cooldown_minutes:
    #                     should_notify = True
    #                 else:
    #                     logger.info(
    #                         f"🔥 Fire alert for {stream_id_str} suppressed "
    #                         f"(cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
    #                     )
    #             else:
    #                 should_notify = True
            
    #         if should_notify:
    #             # Send notification
    #             message = f"Fire detection alert: {fire_status.upper()} detected!"
                
    #             await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             user_email = await user_manager.get_user_by_id(owner_id)

    #             user_email = user_email["email"]
    #             # Send email
    #             await send_fire_alert_email(
    #                 user_email=user_email,
    #                 camera_name=camera_name,
    #                 fire_status=fire_status
    #             )
                
    #             # Update fire detection state
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time
    #             )
                
    #             logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status}")
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic.
        
    #     Sends notifications:
    #     1. When fire/smoke is detected (every 10 minutes while active)
    #     2. When fire/smoke clears (immediate notification with "CLEAR")
    #     """
    #     try:
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # Get existing fire state
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         # ============= HANDLE FIRE/SMOKE CLEARED =============
    #         if fire_status == "no detection":
    #             # Check if there was a previous fire/smoke detection
    #             if fire_state and fire_state.get('fire_status') in ['fire', 'smoke']:
    #                 # Fire/smoke just cleared - send "CLEAR" notification
    #                 previous_status = fire_state.get('fire_status')
                    
    #                 # Create "CLEAR" notification
    #                 clear_message = f"✅ ALL CLEAR: Hazard resolved at {camera_name}"
                    
    #                 notification = await notification_service.create_notification(
    #                     workspace_id=workspace_id,
    #                     user_id=owner_id,
    #                     status="info",  # Use "info" status for clear notifications
    #                     message=clear_message,
    #                     stream_id=stream_id,
    #                     camera_name=camera_name
    #                 )
                    
    #                 # ✅ Broadcast to WebSocket
    #                 if notification and self.stream_manager:
    #                     notification_data = {
    #                         "type": "new_notification",
    #                         "notification": {
    #                             "id": str(notification.get("notification_id")),
    #                             "user_id": str(notification.get("user_id")),
    #                             "workspace_id": str(notification.get("workspace_id")),
    #                             "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                             "camera_name": camera_name,
    #                             "status": "info",
    #                             "message": clear_message,
    #                             "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                             "read": False
    #                         }
    #                     }
                        
    #                     await self.stream_manager.broadcast_notification(
    #                         str(owner_id),
    #                         notification_data
    #                     )
                        
    #                     logger.info(f"✅ CLEAR notification sent for {stream_id_str} (was: {previous_status})")
                    
    #                 # Update state to "no detection"
    #                 await fire_detection_service.create_or_update_fire_detection_state(
    #                     stream_id=stream_id,
    #                     fire_status="no detection",
    #                     last_detection_time=current_time,
    #                     last_notification_time=None  # Don't update notification time
    #                 )
                
    #             # Exit - no alert needed
    #             return
            
    #         # ============= HANDLE FIRE/SMOKE DETECTION =============
    #         cooldown_minutes = 10
    #         quick_redetection_minutes = 2
            
    #         # Check if we should send notification
    #         should_notify = False
            
    #         if not fire_state:
    #             # First detection ever
    #             should_notify = True
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             # STRICT COOLDOWN: Always check time elapsed
    #             if last_notification:
    #                 time_since_last = (current_time - last_notification).total_seconds() / 60
                    
    #                 # If previous status was clear and fire returned quickly, notify immediately
    #                 if previous_status == 'no detection' and time_since_last < quick_redetection_minutes:
    #                     should_notify = True
    #                     logger.warning(f"🔥 Fire returned quickly after clearing: {stream_id_str}")
    #                 # If cooldown expired, send periodic notification
    #                 elif time_since_last >= cooldown_minutes:
    #                     should_notify = True
    #                 else:
    #                     # Within cooldown - suppress
    #                     logger.info(
    #                         f"🔥 Fire alert for {stream_id_str} suppressed "
    #                         f"(cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
    #                     )
    #             else:
    #                 should_notify = True
            
    #         if should_notify:
    #             # Get location info
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = camera_name
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 # Build location string
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 if location_parts:
    #                     location_text = " - ".join(location_parts)
    #                 elif location_info.get('location'):
    #                     location_text = location_info['location']
                
    #             # Create alert message
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected at {location_text}"
                
    #             # Create notification
    #             notification = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             # Broadcast to WebSocket
    #             if notification and self.stream_manager:
    #                 notification_data = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification.get("notification_id")),
    #                         "user_id": str(notification.get("user_id")),
    #                         "workspace_id": str(notification.get("workspace_id")),
    #                         "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": "urgent",
    #                         "message": message,
    #                         "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data
    #                 )
                    
    #                 logger.warning(f"🔥 {alert_type} notification sent to {owner_id}")
                
    #             # Send email
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     user_email = user_info["email"]
                        
    #                     await send_fire_alert_email(
    #                         user_email=user_email,
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent to {user_email}")
    #             except Exception as email_error:
    #                 logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
    #             # Update fire detection state
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time
    #             )
                
    #             logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status}")
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic.
        
    #     Sends notifications:
    #     1. When fire/smoke is detected (every 10 minutes while active)
    #     2. When fire/smoke clears (immediate notification with "CLEAR")
    #     """
    #     try:
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # Get existing fire state
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         # ============= HANDLE FIRE/SMOKE CLEARED =============
    #         if fire_status == "no detection":
    #             # Check if there was a previous fire/smoke detection
    #             if fire_state and fire_state.get('fire_status') in ['fire', 'smoke']:
    #                 # Fire/smoke just cleared - send "CLEAR" notification
    #                 previous_status = fire_state.get('fire_status')
                    
    #                 # Create "CLEAR" notification
    #                 clear_message = f"✅ ALL CLEAR: Hazard resolved at {camera_name}"
                    
    #                 notification = await notification_service.create_notification(
    #                     workspace_id=workspace_id,
    #                     user_id=owner_id,
    #                     status="info",  # Use "info" status for clear notifications
    #                     message=clear_message,
    #                     stream_id=stream_id,
    #                     camera_name=camera_name
    #                 )
                    
    #                 # ✅ Broadcast to WebSocket
    #                 if notification and self.stream_manager:
    #                     notification_data = {
    #                         "type": "new_notification",
    #                         "notification": {
    #                             "id": str(notification.get("notification_id")),
    #                             "user_id": str(notification.get("user_id")),
    #                             "workspace_id": str(notification.get("workspace_id")),
    #                             "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                             "camera_name": camera_name,
    #                             "status": "info",
    #                             "message": clear_message,
    #                             "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                             "read": False
    #                         }
    #                     }
                        
    #                     await self.stream_manager.broadcast_notification(
    #                         str(owner_id),
    #                         notification_data
    #                     )
                        
    #                     logger.info(f"✅ CLEAR notification sent for {stream_id_str} (was: {previous_status})")
                    
    #                 # Update state to "no detection"
    #                 await fire_detection_service.create_or_update_fire_detection_state(
    #                     stream_id=stream_id,
    #                     fire_status="no detection",
    #                     last_detection_time=current_time,
    #                     last_notification_time=None  # Don't update notification time
    #                 )
                
    #             # Exit - no alert needed
    #             return
            
    #         # ============= HANDLE FIRE/SMOKE DETECTION =============
    #         cooldown_minutes = 10
    #         quick_redetection_minutes = 2
            
    #         # Check if we should send notification
    #         should_notify = False
            
    #         if not fire_state:
    #             # First detection ever
    #             should_notify = True
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             # STRICT COOLDOWN: Always check time elapsed
    #             if last_notification:
    #                 time_since_last = (current_time - last_notification).total_seconds() / 60
                    
    #                 # If previous status was clear and fire returned quickly, notify immediately
    #                 if previous_status == 'no detection' and time_since_last < quick_redetection_minutes:
    #                     should_notify = True
    #                     logger.warning(f"🔥 Fire returned quickly after clearing: {stream_id_str}")
    #                 # If cooldown expired, send periodic notification
    #                 elif time_since_last >= cooldown_minutes:
    #                     should_notify = True
    #                 else:
    #                     # Within cooldown - suppress
    #                     logger.info(
    #                         f"🔥 Fire alert for {stream_id_str} suppressed "
    #                         f"(cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
    #                     )
    #             else:
    #                 should_notify = True
            
    #         if should_notify:
    #             # Get location info
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = camera_name
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 # Build location string
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 if location_parts:
    #                     location_text = " - ".join(location_parts)
    #                 elif location_info.get('location'):
    #                     location_text = location_info['location']
                
    #             # ========== MESSAGE 1: WITH "FIRE"/"SMOKE" (TRIGGERS POPUP) ==========
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             popup_message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected at {location_text}"
                
    #             # Create first notification (with fire/smoke keyword)
    #             notification1 = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=popup_message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             # Broadcast first notification (will trigger popup)
    #             if notification1 and self.stream_manager:
    #                 notification_data1 = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification1.get("notification_id")),
    #                         "user_id": str(notification1.get("user_id")),
    #                         "workspace_id": str(notification1.get("workspace_id")),
    #                         "stream_id": str(notification1.get("stream_id")) if notification1.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": "urgent",
    #                         "message": popup_message,
    #                         "timestamp": notification1.get("timestamp").timestamp() if notification1.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data1
    #                 )
                    
    #                 logger.warning(f"🔥 {alert_type} POPUP notification sent to {owner_id}")
                
    #             # ========== WAIT 0.1 SECONDS ==========
    #             await asyncio.sleep(5)
                
    #             # ========== MESSAGE 2: WITHOUT "FIRE"/"SMOKE" (NO POPUP) ==========
    #             # Create safe message without fire/smoke keywords
    #             safe_message = f"⚠️ Hazard detected at {location_text} - Please verify immediately"
                
    #             # Create second notification (without fire/smoke keyword)
    #             notification2 = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=safe_message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             # Broadcast second notification (will NOT trigger popup)
    #             if notification2 and self.stream_manager:
    #                 notification_data2 = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification2.get("notification_id")),
    #                         "user_id": str(notification2.get("user_id")),
    #                         "workspace_id": str(notification2.get("workspace_id")),
    #                         "stream_id": str(notification2.get("stream_id")) if notification2.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": "urgent",
    #                         "message": safe_message,
    #                         "timestamp": notification2.get("timestamp").timestamp() if notification2.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data2
    #                 )
                    
    #                 logger.warning(f"⚠️ Follow-up notification sent to {owner_id} (no popup)")
                
    #             # Send email
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     user_email = user_info["email"]
                        
    #                     await send_fire_alert_email(
    #                         user_email=user_email,
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent to {user_email}")
    #             except Exception as email_error:
    #                 logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
    #             # Update fire detection state
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time
    #             )
                
    #             logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status}")
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire detection alert: {e}", exc_info=True)


    # async def _handle_fire_detection_alert(
    #     self,
    #     stream_id: UUID,
    #     stream_id_str: str,
    #     fire_status: str,
    #     camera_name: str,
    #     workspace_id: UUID,
    #     owner_id: UUID
    # ):
    #     """
    #     Handle fire detection alert logic.
        
    #     Notification Strategy:
    #     1. FIRST detection → Send IMMEDIATELY
    #     2. Ongoing detection → Send every 10 minutes
    #     3. Clear detection → Send IMMEDIATELY when hazard resolves
    #     """
    #     try:
    #         current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         # Get existing fire state
    #         fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
    #         # ============= HANDLE FIRE/SMOKE CLEARED =============
    #         if fire_status == "no detection":
    #             # Check if there was a previous fire/smoke detection
    #             if fire_state and fire_state.get('fire_status') in ['fire', 'smoke']:
    #                 # Fire/smoke just cleared - send "CLEAR" notification IMMEDIATELY
    #                 previous_status = fire_state.get('fire_status')
                    
    #                 # Create "CLEAR" notification
    #                 clear_message = f"✅ ALL CLEAR: Hazard resolved at {camera_name}"
                    
    #                 notification = await notification_service.create_notification(
    #                     workspace_id=workspace_id,
    #                     user_id=owner_id,
    #                     status="info",
    #                     message=clear_message,
    #                     stream_id=stream_id,
    #                     camera_name=camera_name
    #                 )
                    
    #                 # Broadcast to WebSocket
    #                 if notification and self.stream_manager:
    #                     notification_data = {
    #                         "type": "new_notification",
    #                         "notification": {
    #                             "id": str(notification.get("notification_id")),
    #                             "user_id": str(notification.get("user_id")),
    #                             "workspace_id": str(notification.get("workspace_id")),
    #                             "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
    #                             "camera_name": camera_name,
    #                             "status": "info",
    #                             "message": clear_message,
    #                             "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else current_time.timestamp(),
    #                             "read": False
    #                         }
    #                     }
                        
    #                     await self.stream_manager.broadcast_notification(
    #                         str(owner_id),
    #                         notification_data
    #                     )
                        
    #                     logger.info(f"✅ CLEAR notification sent IMMEDIATELY for {stream_id_str} (was: {previous_status})")
                    
    #                 # Update state to "no detection"
    #                 await fire_detection_service.create_or_update_fire_detection_state(
    #                     stream_id=stream_id,
    #                     fire_status="no detection",
    #                     last_detection_time=current_time,
    #                     last_notification_time=None
    #                 )
                
    #             return
            
    #         # ============= HANDLE FIRE/SMOKE DETECTION =============
    #         cooldown_minutes = 10
    #         should_notify = False
            
    #         if not fire_state:
    #             # ✅ FIRST DETECTION EVER → Send IMMEDIATELY
    #             should_notify = True
    #             logger.warning(f"🔥 FIRST fire detection for {stream_id_str} → Sending IMMEDIATELY")
                
    #         else:
    #             last_notification = fire_state.get("last_notification_time")
    #             previous_status = fire_state.get('fire_status', 'no detection')
                
    #             # Check if this is a NEW fire (was clear before)
    #             if previous_status == 'no detection':
    #                 # ✅ NEW FIRE after clear → Send IMMEDIATELY
    #                 should_notify = True
    #                 logger.warning(f"🔥 NEW fire detection for {stream_id_str} (was clear) → Sending IMMEDIATELY")
                    
    #             elif last_notification:
    #                 # Fire was already active - check cooldown
    #                 time_since_last = (current_time - last_notification).total_seconds() / 60
                    
    #                 if time_since_last >= cooldown_minutes:
    #                     # ✅ Cooldown expired → Send periodic notification
    #                     should_notify = True
    #                     logger.warning(f"🔥 Fire still active for {stream_id_str} → Sending periodic alert (last: {time_since_last:.1f} min ago)")
    #                 else:
    #                     # ❌ Within cooldown → Suppress
    #                     should_notify = False
    #                     logger.info(
    #                         f"🔥 Fire alert for {stream_id_str} suppressed "
    #                         f"(cooldown: {time_since_last:.1f}/{cooldown_minutes} min)"
    #                     )
    #             else:
    #                 # No last notification time but fire was active - send now
    #                 should_notify = True
            
    #         if should_notify:
    #             # Get location info
    #             stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
    #             location_info = None
    #             location_text = camera_name
                
    #             if stream_info:
    #                 location_info = {
    #                     'location': stream_info.get('location'),
    #                     'area': stream_info.get('area'),
    #                     'building': stream_info.get('building'),
    #                     'zone': stream_info.get('zone'),
    #                     'floor_level': stream_info.get('floor_level'),
    #                 }
                    
    #                 # Build location string
    #                 location_parts = []
    #                 if location_info.get('building'):
    #                     location_parts.append(location_info['building'])
    #                 if location_info.get('floor_level'):
    #                     location_parts.append(f"Floor {location_info['floor_level']}")
    #                 if location_info.get('zone'):
    #                     location_parts.append(location_info['zone'])
    #                 if location_info.get('area'):
    #                     location_parts.append(location_info['area'])
                    
    #                 if location_parts:
    #                     location_text = " - ".join(location_parts)
    #                 elif location_info.get('location'):
    #                     location_text = location_info['location']
                
    #             # ========== MESSAGE 1: WITH "FIRE"/"SMOKE" (TRIGGERS POPUP) ==========
    #             alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
    #             popup_message = f"🚨 {alert_type} ALERT: {fire_status.upper()} detected at {location_text}"
                
    #             # Create first notification (with fire/smoke keyword)
    #             notification1 = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=popup_message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             # Broadcast first notification (will trigger popup)
    #             if notification1 and self.stream_manager:
    #                 notification_data1 = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification1.get("notification_id")),
    #                         "user_id": str(notification1.get("user_id")),
    #                         "workspace_id": str(notification1.get("workspace_id")),
    #                         "stream_id": str(notification1.get("stream_id")) if notification1.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": "urgent",
    #                         "message": popup_message,
    #                         "timestamp": notification1.get("timestamp").timestamp() if notification1.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data1
    #                 )
                    
    #                 logger.warning(f"🔥 {alert_type} POPUP notification sent to {owner_id} IMMEDIATELY")
                
    #             # ========== WAIT 5 SECONDS ==========
    #             await asyncio.sleep(5)
                
    #             # ========== MESSAGE 2: WITHOUT "FIRE"/"SMOKE" (NO POPUP) ==========
    #             safe_message = f"⚠️ Hazard detected at {location_text} - Please verify immediately"
                
    #             # Create second notification (without fire/smoke keyword)
    #             notification2 = await notification_service.create_notification(
    #                 workspace_id=workspace_id,
    #                 user_id=owner_id,
    #                 status="urgent",
    #                 message=safe_message,
    #                 stream_id=stream_id,
    #                 camera_name=camera_name
    #             )
                
    #             # Broadcast second notification (will NOT trigger popup)
    #             if notification2 and self.stream_manager:
    #                 notification_data2 = {
    #                     "type": "new_notification",
    #                     "notification": {
    #                         "id": str(notification2.get("notification_id")),
    #                         "user_id": str(notification2.get("user_id")),
    #                         "workspace_id": str(notification2.get("workspace_id")),
    #                         "stream_id": str(notification2.get("stream_id")) if notification2.get("stream_id") else None,
    #                         "camera_name": camera_name,
    #                         "status": "urgent",
    #                         "message": safe_message,
    #                         "timestamp": notification2.get("timestamp").timestamp() if notification2.get("timestamp") else current_time.timestamp(),
    #                         "read": False
    #                     }
    #                 }
                    
    #                 await self.stream_manager.broadcast_notification(
    #                     str(owner_id),
    #                     notification_data2
    #                 )
                    
    #                 logger.warning(f"⚠️ Follow-up notification sent to {owner_id} (no popup)")
                
    #             # Send email
    #             try:
    #                 user_info = await user_manager.get_user_by_id(owner_id)
    #                 if user_info and 'email' in user_info:
    #                     user_email = user_info["email"]
                        
    #                     await send_fire_alert_email(
    #                         user_email=user_email,
    #                         camera_name=camera_name,
    #                         fire_status=fire_status,
    #                         location_info=location_info
    #                     )
    #                     logger.info(f"✅ Fire alert email sent to {user_email}")
    #             except Exception as email_error:
    #                 logger.error(f"❌ Error sending fire alert email: {email_error}", exc_info=True)
                
    #             # Update fire detection state with current notification time
    #             await fire_detection_service.create_or_update_fire_detection_state(
    #                 stream_id=stream_id,
    #                 fire_status=fire_status,
    #                 last_detection_time=current_time,
    #                 last_notification_time=current_time  # ✅ Record when we sent notification
    #             )
                
    #             logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status}")
                
    #     except Exception as e:
    #         logger.error(f"Error handling fire detection alert: {e}", exc_info=True)

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
            shared_stream = self.video_file_manager.get_shared_stream(source)
            
            # Add this stream as a subscriber
            if not shared_stream.add_subscriber(stream_id_str):
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
                    if not shared_stream.wait_for_frame(timeout=10.0):
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
                    frame = shared_stream.get_latest_frame(stream_id_str)
                    
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
                            await video_stream_service.update_stream_status(
                                stream_id, "active", is_streaming=True, last_activity=current_time  
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
                shared_stream.remove_subscriber(stream_id_str)
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
                
                # If user stopped it, don't touch the database!
                if db_check['stop_reason'] == 'user_action' and db_check['is_streaming'] == False:
                    logger.info(
                        f"✅ Stream {stream_id_str} was user-stopped "
                        f"(stop_reason='user_action', is_streaming=FALSE). "
                        f"Skipping database update in finally block."
                    )
                    return
                
                # If database says is_streaming=FALSE and it's not user_action,
                # still don't touch it (might be workspace disabled, etc)
                if db_check['is_streaming'] == False:
                    logger.info(
                        f"✅ Stream {stream_id_str} already marked is_streaming=FALSE in database. "
                        f"Skipping database update."
                    )
                    return
                
                # Only update database if:
                # 1. is_streaming=TRUE (stream should still be running but crashed)
                # 2. stop_reason != 'user_action'
                if stop_event.is_set():
                    # Graceful stop (but not user-initiated)
                    logger.info(
                        f"Stream {stream_id_str} stopped gracefully (system-initiated). "
                        f"Recording stop but keeping is_streaming=TRUE for potential retry."
                    )
                    
                    await video_stream_service.record_camera_stop(
                        stream_id=stream_id,
                        stop_reason='system_error',
                        stopped_by=None,
                        additional_context="Stream stopped cleanly by system"
                    )
                else:
                    # Unexpected termination
                    logger.warning(
                        f"⚠️ Stream {stream_id_str} ended unexpectedly. "
                        f"Retry already scheduled by exception handler."
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
                        success = await video_stream_service.update_stream_status(
                            UUID(stream_id_str), 'active', is_streaming=True, last_activity=current_time
                        )
                        
                        if not success:
                            logger.error(f"Failed to update DB status for {stream_id_str}")
                            # DON'T set is_streaming=False on error!
                            # Just log and continue processing
                            
                    except Exception as e:
                        logger.error(f"Exception updating DB status for {stream_id_str}: {e}")

    def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up StreamProcessingService")
        self._cached_results.clear()
        
        # Shutdown thread pool
        thread_pool.shutdown(wait=False)


stream_processing_service = StreamProcessingService()
