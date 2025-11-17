# stream_one.py - Updated with Location Support
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Response, Depends, Query
from fastapi.responses import JSONResponse
import time
import logging
import asyncio
from collections import defaultdict
import cv2
import numpy as np
import base64
import threading 
import uuid 
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Set, Any, Union
from ultralytics import YOLO
from utils import send_people_count_alert_email, send_fire_alert_email, frame_to_base64, get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists, parse_string_or_list, encoded_string
from database import DatabaseManager
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from config import config
from session_manager import SessionManager 
from user_manager import UserManager
from shared_stream import VideoFileManager
from concurrent.futures import ThreadPoolExecutor
import socket
import re
import os
from starlette.websockets import WebSocketState
from workspaces import check_workspace_membership_and_get_role

session_manager_global = SessionManager()
user_manager_global = UserManager()
db_manager_global = DatabaseManager()

logger = logging.getLogger(__name__) 
router = APIRouter(tags=["stream"]) 

# ThreadPoolExecutor for CPU-bound tasks like YOLO and cv2
thread_pool = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 2 + 4))

class StreamValidator:
    """Handle stream validation with proper timeout and retry logic"""
    
    @staticmethod
    def check_rtsp_connectivity(source: str, timeout: int = 5) -> bool:
        """Quick TCP connectivity check"""
        import socket
        import re
        
        match = re.search(r'@([\d\.]+):(\d+)', source)
        if match:
            host, port = match.groups()
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((host, int(port)))
                sock.close()
                
                if result == 0:
                    logging.info(f"Network connectivity OK: {host}:{port}")
                    return True
                else:
                    logging.error(f"Cannot connect to {host}:{port} (error code: {result})")
                    return False
            except Exception as e:
                logging.error(f"Connectivity check failed: {e}")
                return False
        
        # For non-RTSP sources, skip connectivity check
        return True
    
    @staticmethod
    async def validate_stream_source(source: str, timeout: int = 20) -> bool:
        """Validate stream source with configurable timeout"""
        
        def _check_source():
            test_cap = None
            try:
                # File validation
                if not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and not source.isdigit():
                    if not os.path.exists(source):
                        logging.error(f"Video file does not exist: {source}")
                        return False
                    
                    if not os.access(source, os.R_OK):
                        logging.error(f"Video file is not readable: {source}")
                        return False
                    
                    if os.path.getsize(source) == 0:
                        logging.error(f"Video file is empty: {source}")
                        return False
                
                # Set FFmpeg timeout options (most reliable method)
                os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = f'rtsp_transport;udp|timeout;{timeout * 1000000}'
                
                # Open with FFmpeg backend
                test_cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
                
                # Configure OpenCV timeouts
                if source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    test_cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, timeout * 1000)
                    test_cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
                
                if not test_cap.isOpened():
                    logging.error(f"Cannot open video source: {source}")
                    return False
                
                # Try to read one frame
                ret, frame = test_cap.read()
                
                if not ret or frame is None:
                    logging.error(f"Cannot read from video source: {source}")
                    return False
                
                logging.info(f"Successfully validated source: {source}")
                return True
                
            except Exception as e:
                logging.error(f"Error validating source {source}: {e}")
                return False
            finally:
                if test_cap is not None:
                    test_cap.release()
        
        loop = asyncio.get_event_loop()
        
        try:
            future = loop.run_in_executor(thread_pool, _check_source)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logging.error(f"Validation timeout ({timeout}s) for source: {source}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error during validation: {e}")
            return False
    
    @staticmethod
    async def validate_with_retry(source: str, max_retries: int = 3, timeout: int = 20) -> bool:
        """Validate with retry logic and exponential backoff"""
        
        # Quick connectivity check for RTSP streams
        if source.startswith('rtsp://'):
            if not StreamValidator.check_rtsp_connectivity(source, timeout=150):
                logging.error(f"Network connectivity check failed for: {source}")
                return False
        
        # Retry validation
        for attempt in range(max_retries):
            logging.info(f"Validation attempt {attempt + 1}/{max_retries} for: {source}")
            
            if await StreamValidator.validate_stream_source(source, timeout=timeout):
                logging.info(f"✓ Stream validated successfully: {source}")
                return True
            
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                logging.warning(f"Validation attempt {attempt + 1} failed, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        
        logging.error(f"All validation attempts failed for: {source}")
        return False

class StreamManager:

    def __init__(self):
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_processing_stats: Dict[str, Dict[str, Any]] = {}
        self.param_cache: Dict[str, Dict[str, Any]] = {}
        self.param_cache_ttl = config.get("stream_param_cache_ttl_seconds", 300)
        self.param_cache_last_updated: Dict[str, float] = {}
        self.notifications: List[Dict[str, Any]] = [] # In-memory cache, primary is DB
        self.notification_subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        self.max_notifications = config.get("stream_max_in_memory_notifications", 100)
        self.people_count_notification_cooldowns: Dict[str, float] = {}
        self.people_count_cooldown_duration = 300.0  # 5 minutes in seconds
        self.fire_notification_cooldowns: Dict[str, float] = {}
        self.fire_cooldown_duration = 300.0  # 5 minutes in seconds
        self.fire_detection_states: Dict[str, str] = {}  # Track current fire status per stream
        self.fire_detection_frame_counts: Dict[str, int] = {}  # Track frame counts per stream
        self.global_fire_states: Dict[str, Dict[str, Any]] = {}
        self.frame_buffer_size = config.get("cv_frame_buffer_size", 3)
        self.last_healthcheck = datetime.now(ZoneInfo("Africa/Cairo"))
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        self.db_manager = DatabaseManager() # StreamManager's own instance
        self.validator = StreamValidator()
        self.user_manager = UserManager() 
        self.qdrant_client = QdrantClient(
            url=config.get("qdrant_url", "localhost"), 
            port=config.get("qdrant_port", 6333), 
            timeout=config.get("qdrant_timeout", 30.0)
        )
        self.people_model, self.gender_model, self.fire_model = self._initialize_model() # Sync init is fine
        self.video_file_manager = VideoFileManager()
        self.background_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None

        logging.info("StreamManager initialized - waiting for start_background_tasks()")

    async def start_background_tasks(self):
        try:
            logging.info("Starting StreamManager background tasks...")
            await self._start_background_tasks_internal()
            logging.info("StreamManager background tasks started successfully.")
            return True
        except Exception as e:
            logging.error(f"Failed to start StreamManager background tasks: {e}", exc_info=True)
            return False

    async def _start_background_tasks_internal(self):
        await self.stop_background_tasks() 
        self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
        self.background_task.set_name("manage_streams_loop")
        self.background_task.add_done_callback(self._handle_task_done)
        
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.cleanup_task.set_name("periodic_cleanup_loop")
        self.cleanup_task.add_done_callback(self._handle_task_done)
        logging.info("StreamManager background tasks (internal start) initialized.")

    async def stop_background_tasks(self):
        tasks_to_stop = [
            ("background_task", self.background_task),
            ("cleanup_task", self.cleanup_task),
        ]
        for name, task_instance in tasks_to_stop:
            if task_instance and not task_instance.done():
                task_name_str = task_instance.get_name() if hasattr(task_instance, 'get_name') and task_instance.get_name() else name
                try:
                    task_instance.cancel()
                    timeout_seconds = float(config.get("task_cancel_timeout_seconds", 5.0))
                    await asyncio.wait_for(task_instance, timeout=timeout_seconds)
                    logging.info(f"Task {name} ({task_name_str}) cancelled successfully.")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout cancelling {name} ({task_name_str}).")
                except asyncio.CancelledError:
                    logging.info(f"Task {name} ({task_name_str}) was already cancelled/completed.")
                except Exception as e:
                    logging.error(f"Error cancelling {name} ({task_name_str}): {e}", exc_info=True)
        self.background_task = None
        self.cleanup_task = None

    def _handle_task_done(self, task: asyncio.Task):
        try:
            task_name = task.get_name()
            exception = task.exception()
            if exception:
                logging.error(f"Task '{task_name}' failed: {exception}", exc_info=exception)
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task_name))
            elif task.cancelled():
                 logging.info(f"Task '{task_name}' was cancelled.")
            else:
                 logging.info(f"Task '{task_name}' completed successfully.") # Should not happen for main loops unless intentionally stopped
        except Exception as e: 
            logging.error(f"Error in _handle_task_done for task {task.get_name()}: {e}", exc_info=True)
            # Ensure restart is attempted even if _handle_task_done itself has an issue
            if task.done() and not task.cancelled() and task.exception(): # If it was a real failure
                 asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task.get_name()))

    async def _restart_background_task_if_needed(self, failed_task_name: Optional[str] = None):
        await asyncio.sleep(config.get("stream_manager_restart_delay_seconds", 5.0)) # Ensure float
        logging.info(f"Attempting to restart background task(s), original failure (if any) in: {failed_task_name or 'Unknown'}")
        
        # Check background_task
        needs_restart_manage_streams = (
            failed_task_name == "manage_streams_loop" or 
            not self.background_task or 
            self.background_task.done()
        )
        if needs_restart_manage_streams:
            if self.background_task and not self.background_task.done():
                logging.info(f"Cancelling existing manage_streams_loop task {self.background_task.get_name()} before restart.")
                self.background_task.cancel()
                try:
                    await asyncio.wait_for(self.background_task, timeout=config.get("task_cancel_timeout_seconds", 5.0))
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logging.warning(f"Manage_streams_loop cancellation during restart timed out or was already cancelled.")
                except Exception as e_cancel:
                    logging.error(f"Error cancelling manage_streams_loop during restart: {e_cancel}")
            self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
            self.background_task.set_name("manage_streams_loop")
            self.background_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted manage_streams task.")

        # Check cleanup_task
        needs_restart_cleanup = (
            failed_task_name == "periodic_cleanup_loop" or 
            not self.cleanup_task or 
            self.cleanup_task.done()
        )
        if needs_restart_cleanup:
            if self.cleanup_task and not self.cleanup_task.done():
                logging.info(f"Cancelling existing periodic_cleanup_loop task {self.cleanup_task.get_name()} before restart.")
                self.cleanup_task.cancel()
                try:
                    await asyncio.wait_for(self.cleanup_task, timeout=config.get("task_cancel_timeout_seconds", 5.0))
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logging.warning(f"Periodic_cleanup_loop cancellation during restart timed out or was already cancelled.")
                except Exception as e_cancel:
                     logging.error(f"Error cancelling periodic_cleanup_loop during restart: {e_cancel}")
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.set_name("periodic_cleanup_loop")
            self.cleanup_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted periodic_cleanup task.")

    async def _periodic_cleanup(self):
        while True:
            try:
                current_time_ts = datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                async with self._lock: 
                    expired_keys = [
                        key for key, update_time in self.param_cache_last_updated.items()
                        if current_time_ts - update_time > self.param_cache_ttl
                    ]
                    for key in expired_keys:
                        self.param_cache.pop(key, None)
                        self.param_cache_last_updated.pop(key, None)
                if expired_keys: logging.debug(f"Cleaned {len(expired_keys)} expired param cache entries.")
                
                await self._clean_websocket_connections()

                current_time = time.time()
                expired_cooldown_keys = []
                
                for stream_id_str, last_notification_time in self.fire_notification_cooldowns.items():
                    # Remove entries older than 24 hours that are no longer active
                    if ((current_time - last_notification_time) > 86400 and  # 24 hours
                        stream_id_str not in self.active_streams):
                        expired_cooldown_keys.append(stream_id_str)
                
                for key in expired_cooldown_keys:
                    self.fire_notification_cooldowns.pop(key, None)
                    
                await self.cleanup_old_fire_states()

                if expired_cooldown_keys:
                    logging.debug(f"Cleaned up {len(expired_cooldown_keys)} expired fire notification cooldowns")

                # Clean up expired people count notification cooldowns
                expired_people_cooldown_keys = []
                for stream_id_str, last_notification_time in self.people_count_notification_cooldowns.items():
                    # Remove entries older than 24 hours that are no longer active
                    if ((current_time - last_notification_time) > 86400 and  # 24 hours
                        stream_id_str not in self.active_streams):
                        expired_people_cooldown_keys.append(stream_id_str)

                for key in expired_people_cooldown_keys:
                    self.people_count_notification_cooldowns.pop(key, None)

                if expired_people_cooldown_keys:
                    logging.debug(f"Cleaned up {len(expired_people_cooldown_keys)} expired people count notification cooldowns")

            except asyncio.CancelledError:
                logging.info("Periodic cleanup task cancelled.")
                break
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}", exc_info=True)
            await asyncio.sleep(config.get("stream_cleanup_interval_seconds", 60.0)) # Ensure float

    async def _clean_websocket_connections(self):
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_ws = {ws for ws in websockets if ws.client_state != WebSocketState.CONNECTED}
                if dead_ws:
                    self.notification_subscribers[user_id] -= dead_ws
                    if not self.notification_subscribers[user_id]: del self.notification_subscribers[user_id]
        async with self._lock:
            for stream_id, stream_info in list(self.active_streams.items()):
                if 'clients' in stream_info:
                    dead_clients = {ws for ws in stream_info['clients'] if ws.client_state != WebSocketState.CONNECTED}
                    if dead_clients: stream_info['clients'] -= dead_clients

    async def _check_stream_health(self):
        async with self._health_lock: # Ensure only one health check runs at a time
            if (datetime.now(ZoneInfo("Africa/Cairo")) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2: # Avoid too frequent checks
                return

            current_time_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            streams_to_restart_ids = []
            
            async with self._lock: 
                active_stream_ids_copy = list(self.active_streams.keys())

            for stream_id_str in active_stream_ids_copy:
                try:
                    stream_id_uuid = UUID(stream_id_str)
                    db_stream_state = await self.db_manager.execute_query(
                        "SELECT is_streaming, status FROM video_stream WHERE stream_id = $1", 
                        (stream_id_uuid,), fetch_one=True
                    )

                    if not db_stream_state or not db_stream_state.get('is_streaming', False):
                        logging.info(f"Health check: Stream {stream_id_str} externally stopped or not found. Cleaning up.")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    if db_stream_state.get('status') == 'error':
                        logging.warning(f"Health check: Stream {stream_id_str} in 'error' state in DB. Will attempt restart if stuck based on activity.")
                        
                    async with self._lock: 
                        stream_info_mem = self.active_streams.get(stream_id_str)
                    
                    if not stream_info_mem: continue 

                    last_activity_time_mem = stream_info_mem.get('last_frame_time') or stream_info_mem.get('start_time')
                    time_since_last_frame_memory = float('inf')
                    if last_activity_time_mem:
                        if last_activity_time_mem.tzinfo is None:
                            last_activity_time_mem = last_activity_time_mem.replace(tzinfo=timezone.utc)
                        time_since_last_frame_memory = (current_time_utc - last_activity_time_mem).total_seconds()

                    stale_threshold = config.get("stream_stale_threshold_seconds", 120.0) # Ensure float
                    if time_since_last_frame_memory > stale_threshold:
                        logging.warning(f"Stream {stream_id_str} frozen (in-memory last_frame_time {time_since_last_frame_memory:.1f}s ago). Queuing for restart.")
                        streams_to_restart_ids.append(stream_id_str)
                except Exception as e_loop:
                    logger.error(f"Error during health check for stream {stream_id_str}: {e_loop}", exc_info=True)

            for stream_id_to_restart_str in streams_to_restart_ids:
                logging.info(f"Health check: Restarting frozen stream: {stream_id_to_restart_str}")
                await self._stop_stream(stream_id_to_restart_str, for_restart=True) 
            self.last_healthcheck = datetime.now(ZoneInfo("Africa/Cairo"))
    
    def _initialize_model(self): # Stays sync
        people_model_path = config.get("people_model_path", "yolov8n.pt")
        gender_model_path = config.get("gender_model_path", "gender.pt")
        fire_model_path = config.get("fire_model_path", "fire.pt")
        try:
            people_model = YOLO(people_model_path)
            gender_model = YOLO(gender_model_path)
            fire_model = YOLO(fire_model_path)
            return people_model, gender_model, fire_model
        except Exception as e:
            logging.error(f"Failed to initialize YOLO model from {people_model_path}: {e}, using default yolov8n.pt.", exc_info=True)
            fallback_model = YOLO("yolov8n.pt") # Fallback
            fallback_model.conf = 0.4 
            fallback_model.iou = 0.45
            fallback_model.agnostic = False
            fallback_model.max_det = 100
            return fallback_model

    async def connect_client_to_stream(self, stream_id: str, websocket: WebSocket):
        async with self._lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]['clients'].add(websocket)
                return True
            return False
        
    async def disconnect_client(self, stream_id: str, websocket: WebSocket):
        async with self._lock:
            if stream_id in self.active_streams and 'clients' in self.active_streams[stream_id]:
                self.active_streams[stream_id]['clients'].discard(websocket)

    async def shutdown(self):
        logging.info("Shutting down StreamManager...")
        await self.stop_background_tasks()
        
        async with self._lock: 
            active_stream_ids = list(self.active_streams.keys())
        
        # Use asyncio.gather to stop streams concurrently
        stop_tasks = [self._stop_stream(stream_id, for_restart=False) for stream_id in active_stream_ids]
        await asyncio.gather(*stop_tasks, return_exceptions=True) # Log exceptions if any
        
        async with self._lock: 
            self.active_streams.clear()
        self.stream_processing_stats.clear()

        if hasattr(self, 'video_file_manager'):
           # Stop all shared streams
           for source in list(self.video_file_manager.shared_streams.keys()):
               self.video_file_manager.remove_shared_stream(source)
        
        logging.info("StreamManager shutdown complete.")

    async def get_stream_parameters(self, workspace_id: Union[str, UUID]) -> Dict[str, Any]:
        workspace_id_str = str(workspace_id)
        cache_key = f"params_workspace_{workspace_id_str}"
        current_time_ts = datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        
        async with self._lock: 
            if cache_key in self.param_cache and \
               (current_time_ts - self.param_cache_last_updated.get(cache_key, 0)) < self.param_cache_ttl:
                return self.param_cache[cache_key]

        try:
            ws_params_res = await self.db_manager.execute_query(
                "SELECT frame_delay, frame_skip, conf FROM param_stream WHERE workspace_id = $1", 
                (UUID(workspace_id_str),), fetch_one=True
            )
            if ws_params_res:
                params = {"frame_delay": float(ws_params_res["frame_delay"]),
                          "frame_skip": int(ws_params_res["frame_skip"]),
                          "conf_threshold": float(ws_params_res["conf"])}
            else: # Defaults from stream_one.py and DB schema
                params = {"frame_delay": 0.0, "frame_skip": 300, "conf_threshold": 0.4} 

            async with self._lock: 
                self.param_cache[cache_key] = params
                self.param_cache_last_updated[cache_key] = current_time_ts
            return params
        except Exception as e:
            logging.error(f"Error getting stream parameters for ws {workspace_id_str}: {e}", exc_info=True)
            return {"frame_delay": 0.0, "frame_skip": 300, "conf_threshold": 0.4}

    async def get_stream_by_id(self, stream_id_str: str, user_id_context_str: str) -> Dict[str, Any]:
        # UPDATED QUERY: Include location fields
        stream_q = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, 
                   u.username as owner_username, vs.workspace_id, w.name as workspace_name,
                   vs.is_streaming, vs.user_id as owner_id, vs.created_at, vs.updated_at, vs.last_activity,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
                   vs.latitude, vs.longitude
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id = $1
        """
        stream_res = await self.db_manager.execute_query(stream_q, (UUID(stream_id_str),), fetch_one=True)
        
        if not stream_res:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        await check_workspace_membership_and_get_role(UUID(user_id_context_str), stream_res["workspace_id"])
        
        # UPDATED RESPONSE: Include location data
        return {
            "stream_id": str(stream_res["stream_id"]), 
            "name": stream_res["name"], 
            "path": stream_res["path"], 
            "type": stream_res["type"], 
            "status": stream_res["status"],
            "owner_username": stream_res["owner_username"], 
            "workspace_id": str(stream_res["workspace_id"]),
            "workspace_name": stream_res["workspace_name"],
            # NEW: Location fields
            "location": stream_res["location"],
            "area": stream_res["area"],
            "building": stream_res["building"],
            "zone": stream_res["zone"],
            "floor_level": stream_res["floor_level"],
            "latitude": float(stream_res["latitude"]) if stream_res["latitude"] else None,
            "longitude": float(stream_res["longitude"]) if stream_res["longitude"] else None,
        }

    async def add_notification1(self, user_id: str, workspace_id: str, stream_id: str, camera_name: str, status: str, message: str):
        now_dt = datetime.now(ZoneInfo("Africa/Cairo"))
        notif_id = uuid4()
        # Timestamp as float for JSON, datetime object for DB
        notification_data_json = { 
            "id": str(notif_id), "user_id": user_id, "workspace_id": workspace_id, 
            "stream_id": stream_id, "camera_name": camera_name, "status": status, 
            "message": message, "timestamp": now_dt.timestamp(), "read": False
        }
        
        async with self._notification_lock: # In-memory cache update
            self.notifications.append(notification_data_json) 
            self.notifications = self.notifications[-self.max_notifications:]
        
        logging.info(f"Notification for user {user_id}, ws {workspace_id}: {message}")
        asyncio.create_task(self.deliver_notification_to_subscribers(user_id, notification_data_json))
        
        try: 
            await self.db_manager.execute_query(
                """INSERT INTO notifications 
                   (notification_id, user_id, workspace_id, stream_id, camera_name, status, message, timestamp, is_read, created_at, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                (notif_id, UUID(user_id), UUID(workspace_id), UUID(stream_id) if stream_id else None, # Allow null stream_id
                 camera_name, status, message, now_dt, False, now_dt, now_dt)
            )
        except Exception as e_db:
            logging.error(f"Failed to persist notification {str(notif_id)} to DB: {e_db}", exc_info=True)
        return notification_data_json # Return the JSON version

    async def add_notification(self, user_id: str, workspace_id: str, stream_id: str, camera_name: str, status: str, message: str):
        now_dt = datetime.now(ZoneInfo("Africa/Cairo"))
        notif_id = uuid4()
        
        # Timestamp as float for JSON, datetime object for DB
        notification_data_json = { 
            "id": str(notif_id), 
            "user_id": user_id, 
            "workspace_id": workspace_id, 
            "stream_id": stream_id, 
            "camera_name": camera_name, 
            "status": status, 
            "message": message, 
            "timestamp": now_dt.timestamp(), 
            "read": False
        }
        
        async with self._notification_lock: # In-memory cache update
            self.notifications.append(notification_data_json) 
            self.notifications = self.notifications[-self.max_notifications:]
        
        logging.info(f"Notification for user {user_id}, ws {workspace_id}: {message}")
        
        # For fire alerts, send both regular notification AND immediate alert
        if status == "fire_alert":
            # Send regular notification
            asyncio.create_task(self.deliver_notification_to_subscribers(user_id, notification_data_json))
        else:
            # Send regular notification for non-fire alerts
            asyncio.create_task(self.deliver_notification_to_subscribers(user_id, notification_data_json))
        
        try: 
            await self.db_manager.execute_query(
                """INSERT INTO notifications 
                (notification_id, user_id, workspace_id, stream_id, camera_name, status, message, timestamp, is_read, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                (notif_id, UUID(user_id), UUID(workspace_id), UUID(stream_id) if stream_id else None,
                camera_name, status, message, now_dt, False, now_dt, now_dt)
            )
        except Exception as e_db:
            logging.error(f"Failed to persist notification {str(notif_id)} to DB: {e_db}", exc_info=True)
        
        return notification_data_json 

    async def deliver_notification_to_subscribers1(self, user_id: str, notification: Dict[str, Any]):
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy: return
        
        notif_payload = {"type": "notification", "notification": notification, "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()}
        
        tasks = []
        valid_subscribers_for_gather = []
        for ws in subscribers_for_user_copy:
            if ws.client_state == WebSocketState.CONNECTED:
                tasks.append(ws.send_json(notif_payload))
                valid_subscribers_for_gather.append(ws)
            else: 
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                ws_failed = valid_subscribers_for_gather[i]
                logging.warning(f"Failed to send notification to WS for user {user_id} (client: {ws_failed.client}): {result}")
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))

    async def deliver_notification_to_subscribers2(self, user_id: str, notification: Dict[str, Any]):
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy: 
            return
        
        # Create base notification payload
        base_payload = {
            "type": "notification", 
            "notification": notification, 
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        }
        
        # For fire alerts, send additional emergency data that your frontend expects
        if notification.get("status") == "fire_alert":
            # Send both the regular notification AND a fire emergency alert
            fire_emergency_payload = {
                "type": "fire_emergency",
                "message": notification.get("message"),
                "camera_name": notification.get("camera_name"),
                "stream_id": notification.get("stream_id"),
                "timestamp": notification.get("timestamp"),
                "alert_type": "fire_alert",
                "severity": "high",
                "id": notification.get("id")
            }
            
            # Send fire emergency first (for immediate popup)
            fire_tasks = []
            valid_subscribers_fire = []
            for ws in subscribers_for_user_copy:
                if ws.client_state == WebSocketState.CONNECTED:
                    fire_tasks.append(ws.send_json(fire_emergency_payload))
                    valid_subscribers_fire.append(ws)
                else: 
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

            # Send fire emergency alerts
            if fire_tasks:
                fire_results = await asyncio.gather(*fire_tasks, return_exceptions=True)
                for i, result in enumerate(fire_results):
                    if isinstance(result, Exception):
                        ws_failed = valid_subscribers_fire[i]
                        logging.warning(f"Failed to send fire emergency to WS for user {user_id}: {result}")
                        asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))
            
            # Small delay to ensure fire alert is processed first
            await asyncio.sleep(0.1)
        
        # Send regular notification (this will appear in the notification list)
        tasks = []
        valid_subscribers_for_gather = []
        for ws in subscribers_for_user_copy:
            if ws.client_state == WebSocketState.CONNECTED:
                tasks.append(ws.send_json(base_payload))
                valid_subscribers_for_gather.append(ws)
            else: 
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    ws_failed = valid_subscribers_for_gather[i]
                    logging.warning(f"Failed to send notification to WS for user {user_id}: {result}")
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))

    async def deliver_notification_to_subscribers(self, user_id: str, notification: Dict[str, Any]):
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy: 
            return
        
        # Create base notification payload
        base_payload = {
            "type": "notification", 
            "notification": notification, 
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        }
        
        # For fire alerts, send additional emergency data
        if notification.get("status") == "fire_alert":
            # Send fire emergency alert first (for immediate popup)
            fire_emergency_payload = {
                "type": "fire_emergency",
                "alert_type": "fire_alert",
                "id": notification.get("id"),
                "message": notification.get("message"),
                "camera_name": notification.get("camera_name"),
                "stream_id": notification.get("stream_id"),
                "timestamp": notification.get("timestamp"),
                "severity": "high",
                "location_info": {
                    "location": None,  # Add location data if available
                    "area": None,
                    "building": None,
                    "zone": None
                }
            }
            
            # Send fire emergency first (for immediate popup)
            fire_tasks = []
            valid_subscribers_fire = []
            for ws in subscribers_for_user_copy:
                if ws.client_state == WebSocketState.CONNECTED:
                    fire_tasks.append(ws.send_json(fire_emergency_payload))
                    valid_subscribers_fire.append(ws)
                else: 
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

            # Send fire emergency alerts
            if fire_tasks:
                fire_results = await asyncio.gather(*fire_tasks, return_exceptions=True)
                for i, result in enumerate(fire_results):
                    if isinstance(result, Exception):
                        ws_failed = valid_subscribers_fire[i]
                        logging.warning(f"Failed to send fire emergency to WS for user {user_id}: {result}")
                        asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))
            
            # Small delay to ensure fire alert is processed first
            await asyncio.sleep(0.1)
        
        # Send regular notification (this will appear in the notification list)
        tasks = []
        valid_subscribers_for_gather = []
        for ws in subscribers_for_user_copy:
            if ws.client_state == WebSocketState.CONNECTED:
                tasks.append(ws.send_json(base_payload))
                valid_subscribers_for_gather.append(ws)
            else: 
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    ws_failed = valid_subscribers_for_gather[i]
                    logging.warning(f"Failed to send notification to WS for user {user_id}: {result}")
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))
                                        
    async def get_notifications(self, user_id_str: str, workspace_id_filter: Optional[str] = None, since_timestamp: Optional[float] = None, limit: int = 50, include_read: bool = True) -> List[Dict[str, Any]]:
        query = """
            SELECT notification_id, user_id, workspace_id, stream_id, camera_name, status, message, timestamp, is_read
            FROM notifications WHERE user_id = $1
        """
        params_list: List[Any] = [UUID(user_id_str)]
        param_idx = 2 

        if workspace_id_filter:
            query += f" AND workspace_id = ${param_idx}"
            params_list.append(UUID(workspace_id_filter)); param_idx +=1
        if since_timestamp is not None:
            query += f" AND timestamp >= ${param_idx}" # timestamp in DB is timestamptz
            params_list.append(datetime.fromtimestamp(since_timestamp, tz=timezone.utc)); param_idx +=1
        if not include_read:
            query += f" AND is_read = FALSE"
        
        query += f" ORDER BY timestamp DESC LIMIT ${param_idx}"
        params_list.append(limit)

        db_notifications = await self.db_manager.execute_query(query, tuple(params_list), fetch_all=True)
        db_notifications = db_notifications or []
        
        return [{
            "id": str(row["notification_id"]), "user_id": str(row["user_id"]),
            "workspace_id": str(row["workspace_id"]), 
            "stream_id": str(row["stream_id"]) if row["stream_id"] else None,
            "camera_name": row["camera_name"], "status": row["status"],
            "message": row["message"], 
            "timestamp": row["timestamp"].timestamp(), # Convert timestamptz from DB to float UNIX timestamp
            "read": row["is_read"]
        } for row in db_notifications]

    async def subscribe_to_notifications(self, user_id: str, websocket: WebSocket) -> bool:
        async with self._notification_lock:
            self.notification_subscribers[user_id].add(websocket)
        logging.info(f"WS client subscribed to notifications for user {user_id}")
        try:
            # Match payload of stream_one.py
            await websocket.send_json({"type": "subscription_confirmed", "for_user_id": user_id, "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()})
            return True
        except Exception: # Covers WebSocketClosed, ConnectionClosed, etc.
            await self.unsubscribe_from_notifications(user_id, websocket) 
            return False

    async def unsubscribe_from_notifications(self, user_id: str, websocket: WebSocket):
        async with self._notification_lock:
            if user_id in self.notification_subscribers:
                self.notification_subscribers[user_id].discard(websocket)
                if not self.notification_subscribers[user_id]: del self.notification_subscribers[user_id]
        logging.info(f"WS client unsubscribed from notifications for user {user_id}")

    # async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
    #     async with self._lock:
    #         stream_info = self.active_streams.pop(stream_id_str, None)
        
    #     if not stream_info:
    #         if not for_restart: # If not for restart, ensure DB is updated if stream was missed by manager
    #             await self.db_manager.execute_query(
    #                 "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW(), last_activity = NOW() WHERE stream_id = $1 AND is_streaming = TRUE",
    #                 (UUID(stream_id_str),) 
    #             )
    #         return

    #     stream_uuid = UUID(stream_id_str)
    #     stop_event_obj: Optional[threading.Event] = stream_info.get('stop_event')
    #     task_obj: Optional[asyncio.Task] = stream_info.get('task')

    #     # NEW: Clean up fire notification cooldown tracking
    #     # self.fire_notification_cooldowns.pop(stream_id_str, None)
    #     # Only clean up in-memory state
    #     self.fire_detection_states.pop(stream_id_str, None)
    #     self.fire_detection_frame_counts.pop(stream_id_str, None)
    #     self.people_count_notification_cooldowns.pop(stream_id_str, None)

    #     try:
    #         if stop_event_obj: stop_event_obj.set()
    #         if task_obj and not task_obj.done():
    #             task_obj.cancel()
    #             try: 
    #                 await asyncio.wait_for(task_obj, timeout=config.get("stream_stop_timeout_seconds", 10.0))
    #             except asyncio.CancelledError: logging.debug(f"Stream task for {stream_id_str} cancelled as expected.")
    #             except asyncio.TimeoutError: logging.warning(f"Timeout stopping stream task {stream_id_str}.")
            
    #         logging.info(f"Stream {stream_id_str} processing task signaled to stop locally.")
    #         now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            
    #         if for_restart:
    #             # is_streaming remains TRUE, status indicates it's being restarted
    #             await self.db_manager.execute_query( 
    #                 "UPDATE video_stream SET status = 'processing', last_activity = $1, updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE", 
    #                 (now_utc, stream_uuid)
    #             )
    #             logging.info(f"Stream {stream_id_str} marked 'processing' for restart. is_streaming remains TRUE.")
    #         else:
    #             await self.db_manager.execute_query(
    #                 "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', last_activity = $1, updated_at = $1 WHERE stream_id = $2", 
    #                 (now_utc, stream_uuid)
    #             )   
    #             owner_id = stream_info.get('user_id')
    #             ws_id = stream_info.get('workspace_id')
    #             cam_name = stream_info.get('camera_name', 'Unknown Camera')
    #             if owner_id and ws_id:
    #                 asyncio.create_task(self.add_notification(str(owner_id), str(ws_id), stream_id_str, cam_name, "inactive", f"Camera '{cam_name}' was stopped."))
    #     except Exception as e:
    #         logging.error(f"Error during _stop_stream for {stream_id_str}: {e}", exc_info=True)
    #     finally:
    #         self.stream_processing_stats.pop(stream_id_str, None)

    async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
        async with self._lock:
            stream_info = self.active_streams.pop(stream_id_str, None)
        
        if not stream_info:
            if not for_restart: # If not for restart, ensure DB is updated if stream was missed by manager
                await self.db_manager.execute_query(
                    "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW(), last_activity = NOW() WHERE stream_id = $1 AND is_streaming = TRUE",
                    (UUID(stream_id_str),) 
                )
            return

        stream_uuid = UUID(stream_id_str)
        stop_event_obj: Optional[threading.Event] = stream_info.get('stop_event')
        task_obj: Optional[asyncio.Task] = stream_info.get('task')
        source = stream_info.get('source')

        # Clean up in-memory state
        self.fire_detection_states.pop(stream_id_str, None)
        self.fire_detection_frame_counts.pop(stream_id_str, None)
        self.people_count_notification_cooldowns.pop(stream_id_str, None)

        try:
            # CRITICAL FIX: Unsubscribe from shared stream BEFORE stopping task
            if source and hasattr(self, 'video_file_manager'):
                shared_stream = self.video_file_manager.shared_streams.get(source)
                if shared_stream:
                    logging.info(f"Unsubscribing {stream_id_str} from shared stream: {source}")
                    shared_stream.remove_subscriber(stream_id_str)
                    
                    # Give shared stream a moment to process unsubscription
                    await asyncio.sleep(0.5)
            
            # Signal and cancel the processing task
            if stop_event_obj: 
                stop_event_obj.set()
            
            if task_obj and not task_obj.done():
                task_obj.cancel()
                try: 
                    await asyncio.wait_for(task_obj, timeout=config.get("stream_stop_timeout_seconds", 10.0))
                except asyncio.CancelledError: 
                    logging.debug(f"Stream task for {stream_id_str} cancelled as expected.")
                except asyncio.TimeoutError: 
                    logging.warning(f"Timeout stopping stream task {stream_id_str}. Task may still be running.")
                    # Force kill the task if it's still running
                    if not task_obj.done():
                        logging.error(f"Forcefully terminating stuck task for {stream_id_str}")
            
            logging.info(f"Stream {stream_id_str} processing task signaled to stop locally.")
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            
            if for_restart:
                await self.db_manager.execute_query( 
                    "UPDATE video_stream SET status = 'processing', last_activity = $1, updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE", 
                    (now_utc, stream_uuid)
                )
                logging.info(f"Stream {stream_id_str} marked 'processing' for restart. is_streaming remains TRUE.")
            else:
                await self.db_manager.execute_query(
                    "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', last_activity = $1, updated_at = $1 WHERE stream_id = $2", 
                    (now_utc, stream_uuid)
                )   
                owner_id = stream_info.get('user_id')
                ws_id = stream_info.get('workspace_id')
                cam_name = stream_info.get('camera_name', 'Unknown Camera')
                if owner_id and ws_id:
                    asyncio.create_task(self.add_notification(str(owner_id), str(ws_id), stream_id_str, cam_name, "inactive", f"Camera '{cam_name}' was stopped."))
        except Exception as e:
            logging.error(f"Error during _stop_stream for {stream_id_str}: {e}", exc_info=True)
        finally:
            self.stream_processing_stats.pop(stream_id_str, None)
            
            # ADDITIONAL CLEANUP: Ensure shared stream is cleaned up even on error
            if source and hasattr(self, 'video_file_manager'):
                try:
                    shared_stream = self.video_file_manager.shared_streams.get(source)
                    if shared_stream and stream_id_str in shared_stream.subscribers:
                        logging.warning(f"Final cleanup: removing {stream_id_str} from shared stream {source}")
                        shared_stream.remove_subscriber(stream_id_str)
                except Exception as cleanup_error:
                    logging.error(f"Error in final shared stream cleanup for {stream_id_str}: {cleanup_error}")

    async def start_stream_background(self, stream_id: UUID, owner_id: UUID, owner_username: str, 
                                    camera_name: str, source: str, workspace_id: UUID, 
                                    location_info: Optional[Dict[str, Any]] = None):
        stream_id_str = str(stream_id)
        async with self._lock:
            if stream_id_str in self.active_streams:
                logging.info(f"Stream {stream_id_str} already active or being started.")
                return
            # Tentatively mark as starting to prevent duplicate starts
            self.active_streams[stream_id_str] = {
                'status': 'starting', 
                'task': None, 
                'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                'location_info': location_info or {}  # NEW: Store location info
            } 

        try:
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'processing', last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            
            stop_event = threading.Event() # For sync parts within the async task
            self.stream_processing_stats[stream_id_str] = {"frames_processed": 0, "detection_count": 0, "avg_processing_time": 0.0, "last_updated": datetime.now(ZoneInfo("Africa/Cairo"))}

            # UPDATED: Pass location info to _process_stream
            task = asyncio.create_task(
                self._process_stream_with_sharing(
                    stream_id, camera_name, source, owner_username, 
                    owner_id, workspace_id, stop_event, location_info
                )
            )
            task.set_name(f"process_stream_{stream_id_str}")

            async with self._lock: # Fully update stream info
                self.active_streams[stream_id_str] = {
                    'source': source, 'stop_event': stop_event, 'camera_name': camera_name,
                    'username': owner_username, 'user_id': owner_id, 'workspace_id': workspace_id,
                    'clients': set(), 'latest_frame': None, 'last_frame_time': datetime.now(ZoneInfo("Africa/Cairo")),
                    'task': task, 'start_time': self.active_streams[stream_id_str]['start_time'], # Keep original start time
                    'status': 'active_pending', # Indicates task created, _process_stream will set to 'active'
                    'location_info': location_info or {}  # NEW: Store location info
                }
            
            asyncio.create_task(self._ensure_collection_for_stream_workspace(workspace_id))
            # Notification for "active" should come from _process_stream once successfully connected to source
            logging.info(f"Background processing task created for stream {stream_id_str} ({camera_name}) in ws {workspace_id}")

        except Exception as e:
            logging.error(f"Failed to start stream {stream_id_str} background processing: {e}", exc_info=True)
            async with self._lock: self.active_streams.pop(stream_id_str, None) # Clean up tentative entry
            self.stream_processing_stats.pop(stream_id_str, None)
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'error', is_streaming = FALSE, updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            # Add error notification if start fails critically here
            await self.add_notification(str(owner_id), str(workspace_id), stream_id_str, camera_name, "error", "Failed to initiate stream processing.")

    async def _ensure_collection_for_stream_workspace(self, workspace_id: UUID):
        try:
            await ensure_workspace_qdrant_collection_exists(self.qdrant_client, workspace_id)
        except Exception as e:
            logger.error(f"Failed to ensure Qdrant collection for workspace {workspace_id}: {e}", exc_info=True)
 
    async def get_camera_threshold_settings(self, stream_id: UUID) -> Dict[str, Any]:
        """Get threshold settings for a specific camera."""
        try:
            threshold_res = await self.db_manager.execute_query(
                "SELECT count_threshold_greater, count_threshold_less, alert_enabled FROM video_stream WHERE stream_id = $1", 
                (stream_id,), fetch_one=True
            )
            if threshold_res:
                return {
                    "greater_than": threshold_res.get("count_threshold_greater"),
                    "less_than": threshold_res.get("count_threshold_less"),
                    "alert_enabled": threshold_res.get("alert_enabled", False)
                }
            return {"greater_than": None, "less_than": None, "alert_enabled": False}
        except Exception as e:
            logging.error(f"Error getting threshold settings for stream {stream_id}: {e}", exc_info=True)
            return {"greater_than": None, "less_than": None, "alert_enabled": False}

    def generate_alert_message(self, person_count: int, threshold_settings: Dict[str, Any]) -> str:
        """Generate appropriate alert message based on threshold violation."""
        greater_than = threshold_settings.get("greater_than")
        less_than = threshold_settings.get("less_than")
        
        if greater_than is not None and person_count > greater_than:
            return f"HIGH OCCUPANCY ALERT: {person_count} people detected (threshold: >{greater_than})"
        elif less_than is not None and person_count < less_than:
            return f"LOW OCCUPANCY ALERT: {person_count} people detected (threshold: <{less_than})"
        else:
            return f"THRESHOLD ALERT: {person_count} people detected"

    def insert_detection_data_with_location(self, username: str, camera_id_str: str, camera_name: str, 
                                           count: int, male_count: int, female_count: int, fire_status: str, frame: np.ndarray, workspace_id: UUID, 
                                           location_info: Optional[Dict[str, Any]] = None):
        """Insert detection data with location information into Qdrant."""
        if count == 0: return

        target_collection_name = get_workspace_qdrant_collection_name(workspace_id)
        now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
        point_id_str = str(uuid4())
        
        # Base payload
        payload = {
            "camera_id": camera_id_str, 
            "name": camera_name, 
            "timestamp": now_utc.timestamp(),
            "date": now_utc.strftime("%Y-%m-%d"), 
            "time": now_utc.strftime("%H:%M:%S.%f")[:-3], 
            "person_count": count, 
            "male_count": male_count, 
            "female_count": female_count, 
            "fire_status": fire_status, 
            "frame_base64": frame_to_base64(frame), 
            "username": username,
        }
        
        # NEW: Add location information to payload
        if location_info:
            payload.update({
                "location": location_info.get('location'),
                "area": location_info.get('area'),
                "building": location_info.get('building'),
                "zone": location_info.get('zone'),
                "floor_level": location_info.get('floor_level'),
                "latitude": float(location_info['latitude']) if location_info.get('latitude') else None,
                "longitude": float(location_info['longitude']) if location_info.get('longitude') else None,
            })
        
        vector_size = config.get("qdrant_vector_size", 1) 
        point = qdrant_models.PointStruct(id=point_id_str, vector=[0.0] * vector_size, payload=payload)
        
        try:
            self.qdrant_client.upsert(collection_name=target_collection_name, points=[point], wait=False)
            logging.debug(f"Inserted detection data with location info to {target_collection_name}: {point_id_str}")
        except Exception as e:
            logger.error(f"Error inserting detection data with location to Qdrant ({target_collection_name}): {e}", exc_info=True)

    async def start_stream_in_workspace(self, stream_id_to_start_str: str, requester_user_id_str: str) -> Dict[str, Any]:
        stream_id_obj = UUID(stream_id_to_start_str)
        requester_user_id_obj = UUID(requester_user_id_str)

        stream_q = """
            SELECT vs.name, vs.path, vs.user_id, vs.workspace_id, u.username as owner_username,
                   u.is_active as owner_is_active, u.is_subscribed as owner_is_subscribed, 
                   u.count_of_camera as owner_camera_limit, u.role as owner_system_role
            FROM video_stream vs JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = $1
        """
        stream_data = await self.db_manager.execute_query(stream_q, (stream_id_obj,), fetch_one=True)

        if not stream_data: raise HTTPException(status_code=404, detail="Stream not found")
        
        s_workspace_id_obj = stream_data['workspace_id']
        await check_workspace_membership_and_get_role(requester_user_id_obj, s_workspace_id_obj)

        s_owner_id_obj = stream_data['user_id']
        # Check owner's workspace membership (from stream_one.py)
        owner_membership_q = "SELECT 1 FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
        if not await self.db_manager.execute_query(owner_membership_q, (s_owner_id_obj, s_workspace_id_obj), fetch_one=True):
            raise HTTPException(status_code=403, detail="Stream owner no longer in this workspace.")


        if not stream_data['owner_is_active'] or \
           (not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin'):
            raise HTTPException(status_code=403, detail="Stream owner's account inactive or subscription expired.")

        # Count active streams for this owner in this workspace (match stream_one.py logic)
        active_owner_streams_q = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE"
        active_count_res = await self.db_manager.execute_query(active_owner_streams_q, (s_owner_id_obj, s_workspace_id_obj), fetch_one=True)
        active_count = active_count_res['count'] if active_count_res else 0
        
        if stream_data['owner_system_role'] != 'admin' and active_count >= stream_data['owner_camera_limit']:
            raise HTTPException(status_code=403, detail=f"Stream owner's camera limit ({stream_data['owner_camera_limit']}) reached in this workspace.")

        await self.db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = NOW() WHERE stream_id = $1",
            (stream_id_obj,)
        )
        logging.info(f"User {requester_user_id_str} requested start for stream {stream_id_to_start_str}. Marked for processing by StreamManager.")
        # Match stream_one.py response
        return {"stream_id": str(stream_id_obj), "name": stream_data['name'], "workspace_id": str(s_workspace_id_obj), "message": "Stream start initiated. Will be processed by the stream manager."}

    async def get_workspace_streams(self, user_id_str: str, workspace_id_str_optional: Optional[str] = None) -> Dict[str, Any]:
        user_id_obj = UUID(user_id_str)
        target_workspace_ids_objs: List[UUID] = []

        if workspace_id_str_optional:
            try:
                ws_id_to_check = UUID(workspace_id_str_optional)
                await check_workspace_membership_and_get_role(user_id_obj, ws_id_to_check)
                target_workspace_ids_objs.append(ws_id_to_check)
            except HTTPException as e: # From check_workspace_membership
                 return {"streams": [], "total": 0, "message": f"Access denied or workspace not found: {e.detail}"}
            except ValueError: # Invalid UUID format
                 raise HTTPException(status_code=400, detail="Invalid workspace ID format provided.")
        else: 
            user_workspaces_db = await self.db_manager.execute_query("SELECT workspace_id FROM workspace_members WHERE user_id = $1", (user_id_obj,), fetch_all=True)
            user_workspaces_db = user_workspaces_db or []
            target_workspace_ids_objs = [row['workspace_id'] for row in user_workspaces_db]
        
        if not target_workspace_ids_objs: return {"streams": [], "total": 0} # Match stream_one.py response structure

        # Correct placeholder generation for IN clause with variable number of items
        placeholders_corrected = ', '.join([f'${i+1}' for i in range(len(target_workspace_ids_objs))])

        # UPDATED QUERY: Include location fields
        streams_q = f"""
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.user_id as owner_id, u.username as owner_username,
                   vs.workspace_id, w.name as workspace_name,
                   vs.created_at, vs.updated_at,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
                   vs.latitude, vs.longitude
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.workspace_id IN ({placeholders_corrected})
            ORDER BY w.name, vs.name
        """
        streams_data_db = await self.db_manager.execute_query(streams_q, tuple(target_workspace_ids_objs), fetch_all=True)
        streams_data_db = streams_data_db or []
        
        # UPDATED RESPONSE: Include location data
        formatted_streams = [{
            "stream_id": str(s["stream_id"]), 
            "name": s["name"], 
            "path": s["path"], 
            "type": s["type"], 
            "status": s["status"], 
            "is_streaming": s["is_streaming"],
            "owner_id": str(s["owner_id"]), 
            "owner_username": s["owner_username"],
            "workspace_id": str(s["workspace_id"]), 
            "workspace_name": s["workspace_name"],
            "created_at": s["created_at"].isoformat() if s["created_at"] else None,
            "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
            "can_control": True,
            # NEW: Location fields
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "floor_level": s["floor_level"],
            "latitude": float(s["latitude"]) if s["latitude"] else None,
            "longitude": float(s["longitude"]) if s["longitude"] else None,
        } for s in streams_data_db]
        
        return {"streams": formatted_streams, "total": len(formatted_streams)}

    async def manage_streams_with_deduplication(self):
        """Enhanced manage_streams method that prevents duplicate file access and enables sharing"""
        while True:
            try:
                # Initialize video file manager if not exists
                if not hasattr(self, 'video_file_manager'):
                    self.video_file_manager = VideoFileManager()
                
                # Get streams that should be running
                streams_to_run_query = """
                    SELECT vs.stream_id, vs.name, vs.path, vs.user_id, vs.workspace_id, u.username,
                        vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
                        vs.latitude, vs.longitude
                    FROM video_stream vs JOIN users u ON vs.user_id = u.user_id
                    WHERE vs.is_streaming = TRUE AND u.is_active = TRUE 
                        AND (u.is_subscribed = TRUE OR u.role = 'admin')
                """
                potential_streams_db = await self.db_manager.execute_query(streams_to_run_query, fetch_all=True)
                potential_streams_db = potential_streams_db or []

                async with self._lock: 
                    current_running_ids_mem = set(self.active_streams.keys())

                db_should_run_ids = {str(s['stream_id']) for s in potential_streams_db}

                # Group streams by file path for deduplication analysis
                streams_by_path = defaultdict(list)
                for stream_data in potential_streams_db:
                    streams_by_path[stream_data['path']].append(stream_data)
                
                # Track which sources are already being processed
                currently_active_sources = set()
                async with self._lock:
                    for stream_info in self.active_streams.values():
                        if 'source' in stream_info:
                            currently_active_sources.add(stream_info['source'])
                
                # Process each file path group
                for file_path, streams_for_path in streams_by_path.items():
                    # Sort streams by priority (you can customize this logic)
                    # For now, prioritize by creation order or admin users
                    streams_for_path.sort(key=lambda x: (
                        x.get('user_role') != 'admin',  # Admins first
                        x['stream_id']  # Then by stream_id for consistency
                    ))
                    
                    # Check sharing capability
                    enable_sharing = config.get("enable_stream_sharing", True)
                    max_streams_per_file = config.get("max_streams_per_file", 5 if enable_sharing else 1)
                    
                    # Get currently active streams for this file path
                    active_streams_for_path = [
                        s for s in streams_for_path 
                        if str(s['stream_id']) in current_running_ids_mem
                    ]
                    
                    # Count how many we can start
                    can_start_count = max_streams_per_file - len(active_streams_for_path)
                    
                    # Process streams for this file path
                    started_count = 0
                    for stream_data in streams_for_path:
                        stream_id_str = str(stream_data['stream_id'])
                        
                        # Skip if already running
                        if stream_id_str in current_running_ids_mem:
                            continue
                        
                        # Check if we can start more streams for this file
                        if started_count >= can_start_count:
                            if not enable_sharing:
                                # Mark excess streams as inactive if sharing is disabled
                                logging.info(f"File {file_path} already at capacity, marking stream {stream_id_str} inactive")
                                await self.db_manager.execute_query(
                                    "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW() WHERE stream_id = $1", 
                                    (stream_data['stream_id'],)
                                )
                            else:
                                logging.info(f"File {file_path} at sharing capacity ({max_streams_per_file}), queueing stream {stream_id_str}")
                            continue
                        
                        # Validate owner permissions
                        owner_id_obj, workspace_id_obj = stream_data['user_id'], stream_data['workspace_id']
                        owner_info = await self.db_manager.execute_query(
                            "SELECT count_of_camera, role FROM users WHERE user_id = $1", 
                            (owner_id_obj,), fetch_one=True
                        )
                        
                        if not owner_info:
                            continue
                            
                        owner_camera_limit = owner_info["count_of_camera"]
                        owner_role = owner_info["role"]
                        
                        # Count active streams for this owner in this workspace
                        active_owner_streams_q = """
                            SELECT COUNT(*) as count FROM video_stream 
                            WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE
                        """
                        active_count_res = await self.db_manager.execute_query(
                            active_owner_streams_q, (owner_id_obj, workspace_id_obj), fetch_one=True
                        )
                        current_owner_ws_active_count = active_count_res['count'] if active_count_res else 0

                        # Check camera limits
                        if owner_role != 'admin' and current_owner_ws_active_count >= owner_camera_limit:
                            logging.warning(f"Owner {stream_data['username']} at camera limit ({owner_camera_limit}) in ws {workspace_id_obj}. Stream {stream_id_str} marked inactive.")
                            await self.db_manager.execute_query(
                                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW() WHERE stream_id = $1", 
                                (stream_data['stream_id'],)
                            )
                            continue

                        # Start the stream
                        logging.info(f"ManageStreams: Starting stream {stream_id_str} for {stream_data['username']} (ws: {workspace_id_obj})")
                        
                        location_info = {
                            'location': stream_data.get('location'),
                            'area': stream_data.get('area'),
                            'building': stream_data.get('building'),
                            'zone': stream_data.get('zone'),
                            'floor_level': stream_data.get('floor_level'),
                            'latitude': stream_data.get('latitude'),
                            'longitude': stream_data.get('longitude')
                        }
                        
                        # Use the sharing version of start_stream_background
                        asyncio.create_task(
                            self.start_stream_background_with_sharing(
                                stream_data['stream_id'], owner_id_obj, stream_data['username'], 
                                stream_data['name'], stream_data['path'], workspace_id_obj, location_info
                            )
                        )
                        started_count += 1

                # Stop streams that should no longer be running
                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                for stream_id_to_stop_str in streams_to_stop_ids:
                    logging.info(f"ManageStreams: Stopping stream {stream_id_to_stop_str} (no longer marked to run in DB)")
                    await self._stop_stream(stream_id_to_stop_str, for_restart=False)

                # Periodic cleanup of empty shared streams
                if hasattr(self, 'video_file_manager'):
                    self.video_file_manager.cleanup_empty_streams()

                # Health check
                if (datetime.now(ZoneInfo("Africa/Cairo")) - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
                
            except asyncio.CancelledError:
                logging.info("Manage streams with deduplication task cancelled.")
                break
            except Exception as e:
                logging.error(f"Error in manage_streams_with_deduplication loop: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_manager_poll_interval_seconds", 5.0))

    async def get_video_sharing_stats(self) -> Dict[str, Any]:
        """Get statistics about video sharing and stream distribution"""
        if not hasattr(self, 'video_file_manager'):
            return {"shared_streams": {}, "total_shared_sources": 0}
        
        stats = self.video_file_manager.get_all_stats()
        
        # Add summary information
        total_subscribers = sum(len(stream_stats['subscribers']) for stream_stats in stats.values())
        active_sources = sum(1 for stream_stats in stats.values() if stream_stats['is_running'])
        
        return {
            "shared_streams": stats,
            "total_shared_sources": len(stats),
            "active_shared_sources": active_sources,
            "total_subscribers": total_subscribers,
            "average_subscribers_per_source": total_subscribers / len(stats) if stats else 0
        }

    async def force_restart_shared_stream(self, source_path: str) -> bool:
        """Force restart a shared stream (useful for debugging)"""
        if not hasattr(self, 'video_file_manager'):
            return False
        
        try:
            if source_path in self.video_file_manager.shared_streams:
                shared_stream = self.video_file_manager.shared_streams[source_path]
                
                # Get list of affected stream IDs
                affected_stream_ids = list(shared_stream.subscribers.keys())
                
                # Stop the shared stream
                shared_stream._stop_capture()
                
                # Wait a moment
                await asyncio.sleep(2.0)
                
                # Restart will happen automatically when subscribers reconnect
                logging.info(f"Force restarted shared stream for {source_path}. Affected streams: {affected_stream_ids}")
                return True
            
            return False
        except Exception as e:
            logging.error(f"Error force restarting shared stream {source_path}: {e}", exc_info=True)
            return False

    async def _validate_stream_source(self, source: str) -> bool:
        """Validate stream source before processing - uses StreamValidator"""
        return await self.validator.validate_with_retry(source, max_retries=2, timeout=200)
        
    async def start_stream_background_with_sharing(self, stream_id: UUID, owner_id: UUID, owner_username: str, 
                                    camera_name: str, source: str, workspace_id: UUID, 
                                    location_info: Optional[Dict[str, Any]] = None):
        """Start stream background processing with video sharing support and enhanced logging"""
        stream_id_str = str(stream_id)
        
        logging.info(f"Starting stream background processing for {stream_id_str} ({camera_name}) with source: {source}")
        
        async with self._lock:
            if stream_id_str in self.active_streams:
                existing = self.active_streams[stream_id_str]
                # Check if existing entry is stale
                if existing.get('status') == 'starting':
                    start_time = existing.get('start_time')
                    if start_time and (datetime.now(ZoneInfo("Africa/Cairo")) - start_time).total_seconds() > 30:
                        logging.warning(f"Cleaning up stale 'starting' entry for {stream_id_str}")
                        self.active_streams.pop(stream_id_str, None)
                    else:
                        logging.info(f"Stream {stream_id_str} already being started.")
                        return
                else:
                    logging.info(f"Stream {stream_id_str} already active.")
                    return
            
            # Mark as starting
            self.active_streams[stream_id_str] = {
                'status': 'starting', 
                'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                'location_info': location_info or {}
            }

        try:
            logging.info(f"Updating database status to 'processing' for stream {stream_id_str}")

            await self.load_fire_state_on_stream_start(stream_id)

            await self.db_manager.execute_query(
                "UPDATE video_stream SET status = 'processing', last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", 
                (stream_id,)
            )
            
            stop_event = threading.Event()
            self.stream_processing_stats[stream_id_str] = {
                "frames_processed": 0, 
                "detection_count": 0, 
                "avg_processing_time": 0.0, 
                "last_updated": datetime.now(ZoneInfo("Africa/Cairo"))
            }
            
            logging.info(f"Created processing stats and stop event for stream {stream_id_str}")

            # Use the sharing version of _process_stream
            logging.info(f"Creating async task for stream processing: {stream_id_str}")
            task = asyncio.create_task(
                self._process_stream_with_sharing(
                    stream_id, camera_name, source, owner_username, 
                    owner_id, workspace_id, stop_event, location_info
                )
            )
            task.set_name(f"process_stream_shared_{stream_id_str}")
            
            # Add exception handler to the task
            def task_done_callback(task_obj):
                try:
                    exception = task_obj.exception()
                    if exception:
                        logging.error(f"Stream processing task {stream_id_str} failed with exception: {exception}", exc_info=exception)
                    else:
                        logging.info(f"Stream processing task {stream_id_str} completed successfully")
                except Exception as e:
                    logging.error(f"Error in task_done_callback for stream {stream_id_str}: {e}", exc_info=True)
            
            task.add_done_callback(task_done_callback)

            async with self._lock:
                if stream_id_str in self.active_streams:  # Double-check it wasn't cleaned up
                    self.active_streams[stream_id_str] = {
                        'source': source, 'stop_event': stop_event, 'camera_name': camera_name,
                        'username': owner_username, 'user_id': owner_id, 'workspace_id': workspace_id,
                        'clients': set(), 'latest_frame': None, 'last_frame_time': datetime.now(ZoneInfo("Africa/Cairo")),
                        'task': task, 'start_time': self.active_streams[stream_id_str]['start_time'],
                        'status': 'active_pending',
                        'location_info': location_info or {}
                    }
            
            logging.info(f"Updated active_streams dictionary for {stream_id_str}")
            
            asyncio.create_task(self._ensure_collection_for_stream_workspace(workspace_id))
            logging.info(f"Background processing task with sharing created for stream {stream_id_str} ({camera_name}) in ws {workspace_id}")

        except Exception as e:
            logging.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
            # Use comprehensive cleanup
            await self._cleanup_stream_state(stream_id_str, mark_db_inactive=True)
            await self.add_notification(
                str(owner_id), str(workspace_id), stream_id_str, 
                camera_name, "error", f"Failed to start stream: {str(e)[:100]}"
            )
                
    async def _cleanup_stream_state(self, stream_id_str: str, mark_db_inactive: bool = True):
        """Comprehensive cleanup of stream state"""
        async with self._lock:
            stream_info = self.active_streams.pop(stream_id_str, None)
        
        if stream_info:
            # Stop any running tasks
            task = stream_info.get('task')
            if task and not task.done():
                task.cancel()
            
            # Clean up shared stream subscription
            if 'source' in stream_info and hasattr(self, 'video_file_manager'):
                shared_stream = self.video_file_manager.shared_streams.get(stream_info['source'])
                if shared_stream:
                    shared_stream.remove_subscriber(stream_id_str)
        
        # Clean up processing stats
        self.stream_processing_stats.pop(stream_id_str, None)
        
        # Update database if requested
        if mark_db_inactive:
            await self.db_manager.execute_query(
                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW() WHERE stream_id = $1",
                (UUID(stream_id_str),)
            )

    def detect_objects_with_threshold(self, frame: np.ndarray, conf_threshold: float = 0.45, 
                                    threshold_settings: Dict[str, Any] = None,
                                    stream_id_str: str = None) -> tuple[np.ndarray, int, bool, int, int, str]:
        """Object detection with stream-specific fire detection tracking"""
        if frame is None or frame.size == 0: 
            return np.zeros((100, 100, 3), dtype=np.uint8), 0, False, 0, 0, "no detection"

        # CRITICAL FIX: Use stream-specific frame counting
        if stream_id_str:
            frame_count = self.fire_detection_frame_counts.get(stream_id_str, 0) + 1
            self.fire_detection_frame_counts[stream_id_str] = frame_count
        else:
            # Fallback to global counter (shouldn't happen)
            frame_count = getattr(self, '_frame_count', 0) + 1
            setattr(self, '_frame_count', frame_count)
        
        # Cache previous results (stream-specific)
        cache_key = f"cache_{stream_id_str}" if stream_id_str else "cache_global"
        if not hasattr(self, '_cached_results'):
            self._cached_results = {}
        
        if cache_key not in self._cached_results:
            self._cached_results[cache_key] = {
                'male_count': 0,
                'female_count': 0,
                'fire_status': 'no detection',
                'last_gender_frame': 0,
                'last_fire_frame': 0
            }

        cache = self._cached_results[cache_key]

        # Resize frame if too large
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
            people_results = self.people_model.predict(source=input_frame, conf=conf_threshold, classes=[0], verbose=False)

            person_count = 0
            if people_results and people_results[0].boxes is not None:
                person_count = len(people_results[0].boxes)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 3 == 0:
                try:
                    gender_results = self.gender_model(source=input_frame, conf=0.5, verbose=False)
                    if gender_results and gender_results[0].boxes is not None:
                        male_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 1)
                        female_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 0)
                        cache['male_count'] = male_count
                        cache['female_count'] = female_count
                        cache['last_gender_frame'] = frame_count
                except Exception as e:
                    logging.error(f"Gender detection error for stream {stream_id_str}: {e}")
                    # Keep previous cached values

            # CRITICAL FIX: Fire detection with proper state management
            if frame_count % 10 == 0:  # Run fire detection every 10th frame
                try:
                    fire_results = self.fire_model(source=input_frame, conf=0.85, verbose=False)
                    
                    # Determine current fire status
                    current_fire_status = "no detection"
                    if fire_results and fire_results[0].boxes is not None:
                        classes = [int(box.cls) for box in fire_results[0].boxes]
                        if 0 in classes:
                            current_fire_status = "fire"
                        elif 1 in classes:
                            current_fire_status = "smoke"

                    # Get previous fire detection state for this stream
                    previous_fire_status = cache['fire_status']
                    
                    # IMPORTANT: Update cache immediately when detection runs
                    cache['fire_status'] = current_fire_status
                    cache['last_fire_frame'] = frame_count
                    
                    # Only log significant changes and consider cooldown status
                    if current_fire_status != previous_fire_status:
                        # Check if we're in cooldown to determine log level
                        current_time = time.time()
                        last_notification_time = self.fire_notification_cooldowns.get(stream_id_str, 0)
                        in_cooldown = (current_time - last_notification_time) < self.fire_cooldown_duration
                        
                        if current_fire_status in ["fire", "smoke"]:
                            if previous_fire_status == "no detection":
                                # New fire detection - always log as warning
                                logging.warning(f"🔥 NEW FIRE DETECTION: {current_fire_status.upper()} detected in {stream_id_str} at frame {frame_count}")
                            elif in_cooldown:
                                # State change during cooldown - log as info to reduce noise
                                logging.info(f"🔥 Fire state change during cooldown: {stream_id_str} changed from '{previous_fire_status}' to '{current_fire_status}' at frame {frame_count}")
                            else:
                                # State change outside cooldown - log as warning
                                logging.warning(f"🔥 Fire state change: {stream_id_str} changed from '{previous_fire_status}' to '{current_fire_status}' at frame {frame_count}")
                        else:
                            # Fire cleared
                            if previous_fire_status in ["fire", "smoke"]:
                                logging.warning(f"🌊 FIRE CLEARED: {stream_id_str} changed from '{previous_fire_status}' to 'no detection' at frame {frame_count}")
                    
                    # Debug logging for fire detection results (optional, can be removed in production)
                    if current_fire_status != "no detection" and frame_count % 100 == 0:  # Every 100 frames when fire detected
                        logging.debug(f"Fire detection status for {stream_id_str}: {current_fire_status} (frame {frame_count})")
                    
                    
                except Exception as e:
                    logging.error(f"Fire detection error for stream {stream_id_str}: {e}")
                    # Don't update cache if detection failed, use previous value

            # Use cached results
            male_count = cache['male_count']
            female_count = cache['female_count']
            fire_status = cache['fire_status']

            # Threshold checking for people
            alert_triggered = False
            if threshold_settings and threshold_settings.get("alert_enabled", False):
                greater_than = threshold_settings.get("greater_than")
                less_than = threshold_settings.get("less_than")

                # logging.info(f"THRESHOLD CHECK for {stream_id_str}: "
                #     f"person_count={person_count}, "
                #     f"greater_than={greater_than}, "
                #     f"less_than={less_than}, "
                #     f"alert_enabled={threshold_settings.get('alert_enabled')}")
                
                if greater_than is not None and person_count > greater_than:
                    alert_triggered = True
                    # logging.warning(f"THRESHOLD EXCEEDED: {person_count} > {greater_than} for {stream_id_str}")
                if less_than is not None and person_count < less_than:
                    alert_triggered = True
                    # logging.warning(f"THRESHOLD BELOW: {person_count} < {less_than} for {stream_id_str}")

            # Annotate frame
            annotated_frame = people_results[0].plot(img=input_frame.copy()) if people_results and people_results[0].boxes is not None else input_frame.copy()
            
            # Add person count text
            count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
            count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
            cv2.putText(annotated_frame, count_text, (10, 20), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA)
            
            # FIRE/SMOKE overlay
            if fire_status != "no detection":
                fire_color = (0, 0, 255)  # Red for fire/smoke
                fire_text = f"ALERT: {fire_status.upper()}"
                cv2.putText(annotated_frame, fire_text, 
                        (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 
                        0.7, fire_color, 2, cv2.LINE_AA)
                
                # Add blinking effect for fire/smoke
                if frame_count % 20 < 10:  # Blink every 20 frames
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

    async def send_fire_alert_as_array(self, user_id: str, workspace_id: str, stream_id: str, camera_name: str, message: str):
        """Send fire alert in array format that triggers frontend popup"""
        
        now_dt = datetime.now(ZoneInfo("Africa/Cairo"))
        notif_id = uuid4()
        
        # Save to database first
        try: 
            await self.db_manager.execute_query(
                """INSERT INTO notifications 
                (notification_id, user_id, workspace_id, stream_id, camera_name, status, message, timestamp, is_read, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                (notif_id, UUID(user_id), UUID(workspace_id), UUID(stream_id),
                camera_name, "fire_alert", message, now_dt, False, now_dt, now_dt)
            )
        except Exception as e_db:
            logging.error(f"Failed to persist fire notification to DB: {e_db}")
        
        # Get WebSocket subscribers
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy:
            logging.warning(f"No WebSocket subscribers for user {user_id} to receive fire alert")
            return
        
        # Create array format that triggers your frontend popup
        # Your frontend checks: if (Array.isArray(data)) and data[0].message.includes("fire")
        fire_alert_array = [{
            "id": str(notif_id),
            "message": message,  # Contains "fire" keyword
            "timestamp": now_dt.timestamp(),
            "camera_name": camera_name,
            "read": False
        }]
        
        logging.info(f"Sending fire alert array to {len(subscribers_for_user_copy)} subscribers: {fire_alert_array}")
        
        # Send to all connected WebSockets
        success_count = 0
        for ws in subscribers_for_user_copy:
            if ws.client_state == WebSocketState.CONNECTED:
                try:
                    await ws.send_json(fire_alert_array)  # Send as array
                    success_count += 1
                    logging.info(f"Fire alert sent successfully to WebSocket for user {user_id}")
                except Exception as e:
                    logging.warning(f"Failed to send fire alert to WebSocket: {e}")
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))
            else:
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))
        
        logging.info(f"Fire alert sent to {success_count}/{len(subscribers_for_user_copy)} WebSocket connections")
        return True

    async def _process_stream_with_sharing(self, stream_id: UUID, camera_name: str, source: str, 
                                        owner_username: str, owner_id: UUID, workspace_id: UUID, 
                                        stop_event: threading.Event,
                                        location_info: Optional[Dict[str, Any]] = None):
        """Fixed stream processing with proper frame acquisition and fire notification logic"""
        
        frame_count = 0
        last_db_update_activity = datetime.now(ZoneInfo("Africa/Cairo"))
        stream_id_str = str(stream_id)
        loop = asyncio.get_event_loop()
        shared_stream = None
        
        # Initialize fire detection state for this stream
        self.fire_detection_states[stream_id_str] = "no detection"
        self.fire_detection_frame_counts[stream_id_str] = 0
        
        logging.info(f"Starting _process_stream_with_sharing for {stream_id_str} ({camera_name}) with source: {source}")
        
        try:
            # Get threshold settings for this camera
            logging.debug(f"Getting threshold settings for stream {stream_id_str}")
            threshold_settings = await self.get_camera_threshold_settings(stream_id)
            
            # Validate source before processing
            logging.info(f"Validating stream source: {source}")
            if not await self._validate_stream_source(source):
                raise RuntimeError(f"Invalid or inaccessible video source: {source}")
            
            logging.info(f"Source validation successful for {source}")
            
            # Get stream parameters
            logging.debug(f"Getting stream parameters for workspace {workspace_id}")
            params = await self.get_stream_parameters(workspace_id)
            frame_skip = params.get("frame_skip", 300)
            frame_delay_target = params.get("frame_delay", 0.0)
            conf_threshold = params.get("conf_threshold", 0.4)
            
            logging.info(f"Stream parameters for {stream_id_str}: frame_skip={frame_skip}, delay={frame_delay_target}, conf={conf_threshold}")

            # Get or create shared stream for this source
            logging.info(f"Getting shared stream for source: {source}")
            shared_stream = self.video_file_manager.get_shared_stream(source)
            
            # Check if shared stream is healthy
            if not shared_stream.file_exists and shared_stream.is_file_source:
                raise RuntimeError(f"Video file does not exist or is not accessible: {source}")
            
            logging.info(f"Shared stream obtained for {source}. File exists: {shared_stream.file_exists}, Is file source: {shared_stream.is_file_source}")
            
            # Subscribe to the shared stream
            subscriber_info = {
                'stream_id': stream_id_str,
                'camera_name': camera_name,
                'workspace_id': str(workspace_id)
            }
            
            logging.info(f"Subscribing to shared stream for {stream_id_str}")
            if not shared_stream.add_subscriber(stream_id_str, subscriber_info):
                raise RuntimeError(f"Could not subscribe to shared stream for {source} - max subscribers reached")
            
            logging.info(f"Successfully subscribed to shared stream. Current subscribers: {len(shared_stream.subscribers)}")

            # Wait for shared stream to initialize
            initialization_timeout = 30.0
            wait_start = time.time()
            
            logging.info(f"Waiting for shared stream initialization (timeout: {initialization_timeout}s)")
            
            while (time.time() - wait_start) < initialization_timeout:
                elapsed = time.time() - wait_start
                is_running = shared_stream.is_running
                last_successful_read = getattr(shared_stream, 'last_successful_read', 0)
                
                if is_running and last_successful_read > 0:
                    logging.info(f"Shared stream initialized successfully for {stream_id_str} after {elapsed:.1f}s")
                    break
                    
                if elapsed > 10 and elapsed % 5 < 0.5:
                    stats = shared_stream.get_stats()
                    logging.warning(f"Still waiting for initialization of {source}: {stats}")
                
                await asyncio.sleep(0.5)
            
            if not shared_stream.is_running:
                stats = shared_stream.get_stats()
                raise RuntimeError(f"Shared stream failed to initialize for {source} after {initialization_timeout}s. Stats: {stats}")
            
            # Update database status
            logging.info(f"Updating database status to 'active' for stream {stream_id_str}")
            await self.db_manager.execute_query(
                "UPDATE video_stream SET status = 'active', updated_at = NOW() WHERE stream_id = $1", 
                (stream_id,)
            )
            
            await self.add_notification(
                str(owner_id), str(workspace_id), stream_id_str, 
                camera_name, "active", f"Camera '{camera_name}' started streaming."
            )
            
            async with self._lock:
                if stream_id_str in self.active_streams: 
                    self.active_streams[stream_id_str]['status'] = 'active'
            
            logging.info(f"Stream {stream_id_str} successfully subscribed and activated for shared stream {source}")
            
            # Main processing loop starts here
            logging.info(f"Starting main processing loop for stream {stream_id_str}")
            
            consecutive_frame_failures = 0
            max_consecutive_failures = 60
            last_frame_received = time.time()
            frame_timeout = 30.0
            
            while not stop_event.is_set():
                try:
                    # Check for frame timeout
                    if (time.time() - last_frame_received) > frame_timeout:
                        logging.warning(f"Frame timeout for stream {stream_id_str} - no frames for {frame_timeout}s")
                        if hasattr(self, 'video_file_manager'):
                            await self.force_restart_shared_stream(source)
                        await asyncio.sleep(5.0)
                        last_frame_received = time.time()
                        continue
                    
                    # Get frame from shared stream
                    frame = await loop.run_in_executor(None, shared_stream.get_latest_frame, stream_id_str)
                    
                    if frame is None:
                        # Wait for frame to be available
                        frame_available = await loop.run_in_executor(
                            None, shared_stream.wait_for_frame, 2.0
                        )
                        
                        if not frame_available:
                            consecutive_frame_failures += 1
                            if consecutive_frame_failures >= max_consecutive_failures:
                                raise RuntimeError(f"No frames received from shared stream for {source} after {max_consecutive_failures} attempts")
                            
                            if not shared_stream.is_running:
                                logging.warning(f"Shared stream not running for {source}, attempting restart")
                                await asyncio.sleep(2.0)
                            
                            await asyncio.sleep(0.5)
                            continue
                        
                        # Try getting frame again
                        frame = await loop.run_in_executor(None, shared_stream.get_latest_frame, stream_id_str)
                        if frame is None:
                            await asyncio.sleep(0.1)
                            continue
                    
                    # Successfully received frame
                    consecutive_frame_failures = 0
                    last_frame_received = time.time()
                    frame_count += 1
                    
                    # Validate frame
                    if frame.size == 0 or len(frame.shape) != 3:
                        logging.warning(f"Invalid frame received for stream {stream_id_str}")
                        continue
                    
                    # Apply frame skipping
                    if frame_skip > 0 and frame_count % (frame_skip + 1) != 0:
                        await asyncio.sleep(0.001)
                        continue

                    # Process frame with object detection
                    processing_start_time = datetime.now(ZoneInfo("Africa/Cairo"))
                    
                    processed_frame, person_count, alert_triggered, male_count, female_count, fire_status = await loop.run_in_executor(
                        thread_pool, self.detect_objects_with_threshold, frame, conf_threshold, threshold_settings, stream_id_str
                    )
                    detection_duration = (datetime.now(ZoneInfo("Africa/Cairo")) - processing_start_time).total_seconds()

                    # Update processing stats
                    async with self._lock: 
                        stats = self.stream_processing_stats.get(stream_id_str)
                        if stats:
                            stats["frames_processed"] += 1
                            if person_count > 0: 
                                stats["detection_count"] += 1
                            stats["avg_processing_time"] = (stats.get("avg_processing_time", 0.0) * 0.95) + (detection_duration * 0.05)
                            stats["last_updated"] = datetime.now(ZoneInfo("Africa/Cairo"))
                        
                        # Update stream info
                        stream_info_active = self.active_streams.get(stream_id_str)
                        if stream_info_active: 
                            stream_info_active['latest_frame'] = processed_frame
                            stream_info_active['last_frame_time'] = datetime.now(ZoneInfo("Africa/Cairo"))
                    
                                        # Send notifications for people threshold alerts with cooldown and email
                    if alert_triggered:
                        current_time = time.time()
                        
                        # Check cooldown for people count notifications
                        last_people_notification = self.people_count_notification_cooldowns.get(stream_id_str, 0)
                        time_since_last_people_alert = current_time - last_people_notification
                        people_cooldown_active = time_since_last_people_alert < self.people_count_cooldown_duration
                        
                        if not people_cooldown_active:
                            try:
                                alert_message = self.generate_alert_message(person_count, threshold_settings)
                                
                                logging.warning(f"SENDING PEOPLE COUNT NOTIFICATION: {alert_message} for stream {stream_id_str}")
                                
                                # Send notification
                                await self.add_notification(
                                    str(owner_id), str(workspace_id), stream_id_str, 
                                    camera_name, "alert", alert_message
                                )
                                
                                # Send email
                                user_email = await self.get_user_email_for_stream(owner_id)
                                if user_email:
                                    await send_people_count_alert_email(
                                        user_email, camera_name, person_count, threshold_settings, location_info
                                    )
                                
                                # Update cooldown timestamp
                                self.people_count_notification_cooldowns[stream_id_str] = current_time
                                
                                next_available = datetime.fromtimestamp(current_time + self.people_count_cooldown_duration)
                                logging.warning(f"PEOPLE COUNT NOTIFICATION SENT for {stream_id_str}. "
                                            f"Next available: {next_available}")
                                
                            except Exception as e:
                                logging.error(f"Error sending people count notification for {stream_id_str}: {e}")
                        else:
                            remaining = self.people_count_cooldown_duration - time_since_last_people_alert
                            logging.debug(f"People count notification BLOCKED for {stream_id_str}: "
                                        f"cooldown active, {remaining/60:.1f} minutes remaining")
                        
                    # Fire notification with proper state management and cooldown
                    current_time = time.time()
                    
                    # Get previous fire detection state for this stream
                    previous_fire_status = self.fire_detection_states.get(stream_id_str, "no detection")
                                        

                    if fire_status in ["fire", "smoke"]:
                        # CRITICAL: Get PERSISTENT fire state from database (source of truth)
                        persistent_state = await self.get_persistent_fire_state(stream_id)
                        db_fire_status = persistent_state['fire_status']
                        db_last_notification = persistent_state['last_notification_time']
                        
                        # Check current cooldown status
                        current_time = datetime.now(ZoneInfo("Africa/Cairo"))
                        cooldown_active = False
                        time_since_last_notification = float('inf')
                        
                        if db_last_notification:
                            time_since_last_notification = (current_time - db_last_notification).total_seconds()
                            cooldown_active = time_since_last_notification < self.fire_cooldown_duration
                        
                        # DECISION: Only consider it "NEW" if database shows no recent fire AND no cooldown
                        is_truly_new_fire = (db_fire_status == "no detection" and not cooldown_active)
                        
                        # # logging for debugging
                        # logging.info(f"FIRE DETECTION CHECK for {stream_id_str}: "
                        #             f"current={fire_status}, db_status={db_fire_status}, "
                        #             f"cooldown_active={cooldown_active}, "
                        #             f"time_since_last={time_since_last_notification:.1f}s, "
                        #             f"is_new={is_truly_new_fire}")
                        
                        # Log state changes appropriately
                        if is_truly_new_fire:
                            logging.warning(f"🔥 NEW FIRE DETECTION: {fire_status.upper()} detected in {stream_id_str} at frame {frame_count}")
                        elif cooldown_active:
                            remaining = self.fire_cooldown_duration - time_since_last_notification
                            logging.info(f"🔥 Fire detected during cooldown: {stream_id_str} status='{fire_status}' at frame {frame_count}. Cooldown: {remaining/60:.1f}min remaining")
                        
                        # DECISION: Send notification ONLY if truly new AND no cooldown
                        should_send_notification = is_truly_new_fire
                        
                        if should_send_notification:
                            try:
                                # Create alert message
                                alert_message = f"🔥 FIRE/SMOKE ALERT: {fire_status.upper()} detected in {camera_name}"
                                
                                logging.warning(f"SENDING FIRE NOTIFICATION: {alert_message} for stream {stream_id_str}")

                                await self.send_fire_alert_as_array(str(owner_id), str(workspace_id), stream_id_str, camera_name, alert_message)
                                
                                # fire_notification = {
                                #     "id": str(uuid4()),
                                #     "user_id": str(owner_id),
                                #     "workspace_id": str(workspace_id),
                                #     "stream_id": stream_id_str,
                                #     "camera_name": camera_name,
                                #     "status": "fire_alert",
                                #     "message": alert_message,
                                #     "timestamp": current_time,
                                #     "read": False
                                # }
                                
                                # # Send the notification using the special fire alert method
                                # await self.send_fire_alert_to_frontend(str(owner_id), fire_notification, location_info)
                                
                                # # Send notification
                                # await self.add_notification(
                                #     str(owner_id), str(workspace_id), stream_id_str,
                                #     camera_name, "fire_alert", alert_message
                                # )

                                # await self.send_immediate_fire_alert(
                                #     str(workspace_id), {
                                #         "type": "fire_emergency",
                                #         "alert_type": "fire_alert", 
                                #         "message": alert_message,
                                #         "camera_name": camera_name,
                                #         "fire_status": fire_status,
                                #         "stream_id": stream_id_str,
                                #         "location_info": location_info,
                                #         "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp(),
                                #         "severity": "high"
                                #     }
                                # )
                                
                                # Send email
                                user_email = await self.get_user_email_for_stream(owner_id)
                                if user_email:
                                    await send_fire_alert_email(user_email, camera_name, fire_status, location_info)
                                
                                # CRITICAL: Update database state with notification time
                                await self.update_persistent_fire_state(
                                    stream_id, fire_status, current_time, current_time
                                )
                                
                                # Also update in-memory cooldown for immediate checks
                                self.fire_notification_cooldowns[stream_id_str] = current_time.timestamp()
                                
                                next_available = current_time + timedelta(seconds=self.fire_cooldown_duration)
                                logging.warning(f"🔥 FIRE NOTIFICATION SENT for {stream_id_str}. "
                                            f"Next available: {next_available}")
                                
                            except Exception as e:
                                logging.error(f"Error sending fire notification for {stream_id_str}: {e}")
                        else:
                            # Update detection state in database without notification
                            await self.update_persistent_fire_state(stream_id, fire_status, current_time)
                            
                            if cooldown_active:
                                remaining = self.fire_cooldown_duration - time_since_last_notification
                                logging.debug(f"Fire notification BLOCKED for {stream_id_str}: "
                                            f"cooldown active, {remaining/60:.1f} minutes remaining")

                    else:
                        # Fire cleared - update database state
                        persistent_state = await self.get_persistent_fire_state(stream_id)
                        if persistent_state['fire_status'] in ["fire", "smoke"]:
                            logging.info(f"🌊 FIRE CLEARED: {stream_id_str} changed to 'no detection' at frame {frame_count}")
                            
                            # Send "cleared" notification but preserve cooldown
                            await self.add_notification(
                                str(owner_id), str(workspace_id), stream_id_str,
                                camera_name, "info", f"Fire/smoke cleared in {camera_name}"
                            )
                        
                        # Update state to 'no detection' but preserve notification cooldown in database
                        await self.update_persistent_fire_state(stream_id, "no detection")


                    # Always update in-memory fire state (for immediate reference)
                    self.fire_detection_states[stream_id_str] = fire_status


                    # Insert detection data if people detected
                    if person_count > 0:
                        await loop.run_in_executor(
                            thread_pool, self.insert_detection_data_with_location, 
                            owner_username, stream_id_str, camera_name, person_count, 
                            male_count, female_count, fire_status, processed_frame, 
                            workspace_id, location_info
                        )

                    # Periodic database activity update
                    now_utc_loop = datetime.now(ZoneInfo("Africa/Cairo"))
                    if (now_utc_loop - last_db_update_activity).total_seconds() > 10.0:
                        await self.db_manager.execute_query("UPDATE video_stream SET last_activity = NOW() WHERE stream_id = $1", (stream_id,))
                        last_db_update_activity = now_utc_loop
                    
                    # Frame rate control
                    current_iteration_duration = (datetime.now(ZoneInfo("Africa/Cairo")) - processing_start_time).total_seconds()
                    sleep_duration = max(0, frame_delay_target - current_iteration_duration)
                    await asyncio.sleep(sleep_duration if sleep_duration > 0 else 0.001)
                    
                except Exception as e_loop:
                    consecutive_frame_failures += 1
                    logging.error(f"Error in processing loop for stream {stream_id_str}: {e_loop}", exc_info=True)
                    
                    if consecutive_frame_failures >= max_consecutive_failures:
                        raise RuntimeError(f"Too many consecutive failures in processing loop: {e_loop}")
                    
                    await asyncio.sleep(2.0)
            
            logging.info(f"Processing loop ended normally for stream {stream_id_str}")
                
        except asyncio.CancelledError:
            logging.info(f"Stream processing task for {stream_id_str} ({camera_name}) was cancelled.")
        except RuntimeError as e:
            logging.error(f"Unrecoverable stream error for {stream_id_str} ({camera_name}): {e}")
            await self.db_manager.execute_query(
                "UPDATE video_stream SET status = 'error', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", 
                (stream_id,)
            )
            await self.add_notification(
                str(owner_id), str(workspace_id), stream_id_str, 
                camera_name, "error", f"Stream error: {str(e)[:100]}"
            )
        except Exception as e:
            logging.error(f"General error in _process_stream_with_sharing for {stream_id_str}: {e}", exc_info=True)
            await self.db_manager.execute_query(
                "UPDATE video_stream SET status = 'error', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", 
                (stream_id,)
            )
            await self.add_notification(
                str(owner_id), str(workspace_id), stream_id_str, 
                camera_name, "error", "Unexpected stream error. Check logs."
            )
        finally:
            # Unsubscribe from shared stream
            if shared_stream:
                shared_stream.remove_subscriber(stream_id_str)
                logging.info(f"Stream {stream_id_str} unsubscribed from shared stream for {source}")
            
            # Cleanup fire detection state
            # self.fire_detection_states.pop(stream_id_str, None)
            # self.fire_notification_cooldowns.pop(stream_id_str, None)
            # Only clean up in-memory state
            # self.fire_detection_states.pop(stream_id_str, None)
            self.fire_detection_frame_counts.pop(stream_id_str, None)
            self.people_count_notification_cooldowns.pop(stream_id_str, None)
            
            logging.info(f"Stream processing ended for {stream_id_str} ({camera_name})")
            
            async with self._lock: 
                self.active_streams.pop(stream_id_str, None)
            self.stream_processing_stats.pop(stream_id_str, None)

            # Update database if stream ended unexpectedly
            if not stop_event.is_set():
                logging.info(f"Stream {stream_id_str} ended unexpectedly, updating database status")
                current_db_status = await self.db_manager.execute_query(
                    "SELECT status, is_streaming FROM video_stream WHERE stream_id = $1", (stream_id,), fetch_one=True
                )
                if current_db_status and not (current_db_status.get('status') == 'error' and not current_db_status.get('is_streaming', True)):
                    await self.db_manager.execute_query(
                        "UPDATE video_stream SET status = 'inactive', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", 
                        (stream_id,)
                    )

    async def get_user_email_for_stream(self, user_id: UUID) -> Optional[str]:
        """Get user email from database"""
        try:
            user_data = await self.db_manager.execute_query(
                "SELECT email FROM users WHERE user_id = $1 AND is_active = TRUE",
                (user_id,), fetch_one=True
            )
            return user_data['email'] if user_data else None
        except Exception as e:
            logger.error(f"Error getting user email for user_id {user_id}: {e}", exc_info=True)
            return None

    async def get_fire_notification_cooldown_status(self, stream_id_str: str) -> Dict[str, Any]:
        """Get fire notification cooldown status for a stream."""
        current_time = time.time()
        last_notification_time = self.fire_notification_cooldowns.get(stream_id_str, 0)
        
        if last_notification_time == 0:
            return {
                "stream_id": stream_id_str,
                "can_notify": True,
                "last_notification": None,
                "cooldown_remaining": 0
            }
        
        time_since_last = current_time - last_notification_time
        can_notify = time_since_last >= self.fire_cooldown_duration
        cooldown_remaining = max(0, self.fire_cooldown_duration - time_since_last)
        
        return {
            "stream_id": stream_id_str,
            "can_notify": can_notify,
            "last_notification": datetime.fromtimestamp(last_notification_time, tz=timezone.utc).isoformat(),
            "cooldown_remaining": cooldown_remaining,
            "cooldown_remaining_minutes": cooldown_remaining / 60.0
        }

    async def load_persistent_fire_state(self, stream_id_str: str):
        """Load fire notification state from database to survive stream restarts"""
        try:
            # Check for recent fire notifications in database
            recent_fire_query = """
                SELECT MAX(timestamp) as last_fire_notification
                FROM notifications 
                WHERE stream_id = $1 AND status = 'fire_alert' 
                AND timestamp >= $2
            """
            
            # Look back 2 hours to catch any recent fire notifications
            lookback_time = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(hours=2)
            
            result = await self.db_manager.execute_query(
                recent_fire_query, 
                (UUID(stream_id_str), lookback_time),
                fetch_one=True
            )
            
            if result and result['last_fire_notification']:
                last_notification_timestamp = result['last_fire_notification'].timestamp()
                current_time = time.time()
                
                # If last notification was within cooldown period, restore cooldown
                time_since_last = current_time - last_notification_timestamp
                if time_since_last < self.fire_cooldown_duration:
                    self.fire_notification_cooldowns[stream_id_str] = last_notification_timestamp
                    remaining = self.fire_cooldown_duration - time_since_last
                    logging.info(f"Restored fire cooldown for {stream_id_str}: {remaining/60:.1f} minutes remaining")
                
        except Exception as e:
            logging.error(f"Error loading persistent fire state for {stream_id_str}: {e}")

    async def send_fire_notification_if_allowed(self, stream_id_str: str, fire_status: str, 
                                            owner_id: UUID, workspace_id: UUID, 
                                            camera_name: str, location_info: Dict[str, Any] = None):
        """Centralized fire notification with bulletproof cooldown management"""
        
        current_time = time.time()
        
        # Get previous fire state
        previous_fire_status = self.fire_detection_states.get(stream_id_str, "no detection")
        is_new_fire_detection = previous_fire_status == "no detection" and fire_status in ["fire", "smoke"]
        
        # Get cooldown info
        last_notification_time = self.fire_notification_cooldowns.get(stream_id_str, 0)
        time_since_last_notification = current_time - last_notification_time
        
        # STRICT cooldown check
        cooldown_active = time_since_last_notification < self.fire_cooldown_duration
        
        # Log detailed state for debugging
        logging.info(f"FIRE NOTIFICATION CHECK: stream={stream_id_str}, "
                    f"fire_status={fire_status}, previous={previous_fire_status}, "
                    f"is_new={is_new_fire_detection}, cooldown_active={cooldown_active}, "
                    f"time_since_last={time_since_last_notification:.1f}s")
        
        # DECISION LOGIC: Send notification only if:
        # 1. This is a NEW fire detection AND no cooldown active, OR
        # 2. Cooldown has fully expired for ongoing fire
        should_send = (is_new_fire_detection and not cooldown_active) or \
                    (not is_new_fire_detection and not cooldown_active)
        
        if should_send:
            try:
                # Double-check cooldown one more time before sending
                current_cooldown_check = time.time() - self.fire_notification_cooldowns.get(stream_id_str, 0)
                if current_cooldown_check < self.fire_cooldown_duration:
                    logging.warning(f"BLOCKED: Fire notification blocked by final cooldown check for {stream_id_str}")
                    return False
                
                # Create alert message
                if is_new_fire_detection:
                    alert_message = f"🔥 FIRE/SMOKE EMERGENCY: {fire_status.upper()} detected in {camera_name}"
                    log_type = "NEW DETECTION"
                else:
                    alert_message = f"🔥 FIRE/SMOKE ONGOING: {fire_status.upper()} still detected in {camera_name}"
                    log_type = "COOLDOWN EXPIRED"
                
                logging.warning(f"SENDING FIRE NOTIFICATION ({log_type}): {alert_message}")
                
                # Send notification
                await self.add_notification(
                    str(owner_id), str(workspace_id), stream_id_str,
                    camera_name, "fire_alert", alert_message
                )
                
                # Send email
                user_email = await self.get_user_email_for_stream(owner_id)
                if user_email:
                    await send_fire_alert_email(user_email, camera_name, fire_status, location_info)
                
                # CRITICAL: Update cooldown timestamp AFTER successful notification
                notification_time = time.time()
                self.fire_notification_cooldowns[stream_id_str] = notification_time
                
                logging.warning(f"🔥 FIRE NOTIFICATION SENT for {stream_id_str}. "
                            f"Next available: {datetime.fromtimestamp(notification_time + self.fire_cooldown_duration)}")
                
                return True
                
            except Exception as e:
                logging.error(f"Error sending fire notification for {stream_id_str}: {e}")
                return False
        else:
            # Log why notification was blocked
            if cooldown_active:
                remaining = self.fire_cooldown_duration - time_since_last_notification
                logging.info(f"Fire notification BLOCKED for {stream_id_str}: "
                            f"cooldown active, {remaining/60:.1f} minutes remaining")
            return False
    
    async def send_fire_alert_to_frontend(self, user_id: str, notification: Dict[str, Any], location_info: Optional[Dict[str, Any]] = None):
        """Send fire alert in the format that frontend expects"""
        
        # First add to database
        now_dt = datetime.now(ZoneInfo("Africa/Cairo"))
        notif_id = UUID(notification["id"])
        
        try: 
            await self.db_manager.execute_query(
                """INSERT INTO notifications 
                (notification_id, user_id, workspace_id, stream_id, camera_name, status, message, timestamp, is_read, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                (notif_id, UUID(notification["user_id"]), UUID(notification["workspace_id"]), 
                UUID(notification["stream_id"]) if notification["stream_id"] else None,
                notification["camera_name"], notification["status"], notification["message"], 
                now_dt, False, now_dt, now_dt)
            )
        except Exception as e_db:
            logging.error(f"Failed to persist fire notification {str(notif_id)} to DB: {e_db}", exc_info=True)
        
        # Add to in-memory cache
        async with self._notification_lock:
            self.notifications.append(notification) 
            self.notifications = self.notifications[-self.max_notifications:]
        
        # Get subscribers
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy:
            return
        
        # Create the message format that your frontend expects
        # Based on your frontend code, it expects an array format for fire alerts
        fire_alert_array = [{
            "id": notification["id"],
            "message": notification["message"],
            "timestamp": notification["timestamp"],
            "camera_name": notification["camera_name"],
            "read": False
        }]
        
        # Send as array format (this will trigger your frontend fire alert popup)
        alert_payload = fire_alert_array
        
        logging.info(f"Sending fire alert array to frontend for user {user_id}: {alert_payload}")
        
        # Send to all websockets
        tasks = []
        valid_subscribers = []
        for ws in subscribers_for_user_copy:
            if ws.client_state == WebSocketState.CONNECTED:
                tasks.append(ws.send_json(alert_payload))
                valid_subscribers.append(ws)
            else: 
                asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = 0
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    ws_failed = valid_subscribers[i]
                    logging.warning(f"Failed to send fire alert to WS for user {user_id}: {result}")
                    asyncio.create_task(self.unsubscribe_from_notifications(user_id, ws_failed))
                else:
                    success_count += 1
            
            logging.info(f"Fire alert sent successfully to {success_count}/{len(tasks)} websockets for user {user_id}")
        
        return notification
        
    async def get_persistent_fire_state(self, stream_id: UUID) -> Dict[str, Any]:
        """Get persistent fire state from database"""
        try:
            result = await self.db_manager.execute_query(
                """SELECT fire_status, last_detection_time, last_notification_time 
                FROM fire_detection_state WHERE stream_id = $1""",
                (stream_id,), fetch_one=True
            )
            
            if result:
                return {
                    'fire_status': result['fire_status'],
                    'last_detection_time': result['last_detection_time'],
                    'last_notification_time': result['last_notification_time']
                }
            return {
                'fire_status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }
        except Exception as e:
            logging.error(f"Error getting persistent fire state for {stream_id}: {e}")
            return {
                'fire_status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }

    async def diagnose_stream_activation_failure(self, stream_id_str: str) -> Dict[str, Any]:
        """Diagnose why a stream failed to become active"""
        try:
            stream_uuid = UUID(stream_id_str)
            
            # Check database state
            db_state = await self.db_manager.execute_query(
                "SELECT status, is_streaming, last_activity, updated_at FROM video_stream WHERE stream_id = $1",
                (stream_uuid,), fetch_one=True
            )
            
            # Check in-memory state
            async with self._lock:
                memory_state = self.active_streams.get(stream_id_str, {})
            
            # Check shared stream state
            shared_stream_info = {}
            if hasattr(self, 'video_file_manager'):
                # Find the source for this stream
                for source, shared_stream in self.video_file_manager.shared_streams.items():
                    if stream_id_str in shared_stream.subscribers:
                        shared_stream_info = {
                            'source': source,
                            'is_running': shared_stream.is_running,
                            'subscriber_count': len(shared_stream.subscribers),
                            'last_successful_read': getattr(shared_stream, 'last_successful_read', 0),
                            'stats': shared_stream.get_stats()
                        }
                        break
            
            return {
                'stream_id': stream_id_str,
                'db_state': db_state,
                'memory_state': {
                    'status': memory_state.get('status'),
                    'task_done': memory_state.get('task') and memory_state.get('task').done(),
                    'start_time': memory_state.get('start_time'),
                    'last_frame_time': memory_state.get('last_frame_time')
                },
                'shared_stream_info': shared_stream_info
            }
            
        except Exception as e:
            return {'error': str(e)}

    async def should_send_fire_notification1(self, stream_id: UUID, current_fire_status: str) -> bool:
        """Determine if fire notification should be sent based on persistent state and cooldown"""
        try:
            # Get persistent state from database
            persistent_state = await self.get_persistent_fire_state(stream_id)
            last_notification_time = persistent_state['last_notification_time']
            
            # If current status is not fire/smoke, never send notification
            if current_fire_status not in ['fire', 'smoke']:
                return False
            
            # Check cooldown - this is the ONLY condition that matters
            current_time = datetime.now(ZoneInfo("Africa/Cairo"))
            
            if last_notification_time is None:
                # Never sent a notification before - send it
                logging.info(f"First-time fire notification for {stream_id}")
                return True
            
            # Calculate time since last notification
            time_since_last = (current_time - last_notification_time).total_seconds()
            cooldown_expired = time_since_last >= self.fire_cooldown_duration
            
            if cooldown_expired:
                logging.info(f"Fire notification cooldown expired for {stream_id}. "
                            f"Time since last: {time_since_last/60:.1f} minutes")
                return True
            else:
                remaining = self.fire_cooldown_duration - time_since_last
                logging.debug(f"Fire notification blocked by cooldown for {stream_id}. "
                            f"Remaining: {remaining/60:.1f} minutes")
                return False
                
        except Exception as e:
            logging.error(f"Error checking fire notification decision for {stream_id}: {e}")
            return False

    async def should_send_fire_notification(self, stream_id: UUID, current_fire_status: str) -> bool:
        """
        SIMPLIFIED: Check if fire notification should be sent based on cooldown only.
        This method is now mainly used by debug endpoints.
        """
        if current_fire_status not in ['fire', 'smoke']:
            return False
        
        stream_id_str = str(stream_id)
        current_time = time.time()
        
        # Check in-memory cooldown (primary source)
        last_notification_time = self.fire_notification_cooldowns.get(stream_id_str, 0)
        time_since_last = current_time - last_notification_time
        
        # Also check database as backup
        persistent_state = await self.get_persistent_fire_state(stream_id)
        if persistent_state['last_notification_time']:
            time_since_db = (datetime.now(ZoneInfo("Africa/Cairo")) - persistent_state['last_notification_time']).total_seconds()
            time_since_last = min(time_since_last, time_since_db)
        
        return time_since_last >= self.fire_cooldown_duration

    async def update_persistent_fire_state(self, stream_id: UUID, fire_status: str, 
                                        detection_time: datetime = None, 
                                        notification_time: datetime = None):
        """Update persistent fire state in database with proper UPSERT"""
        try:
            now = datetime.now(ZoneInfo("Africa/Cairo"))
            detection_time = detection_time or now
            
            # Use UPSERT to handle both insert and update cases
            await self.db_manager.execute_query(
                """INSERT INTO fire_detection_state 
                (stream_id, fire_status, last_detection_time, last_notification_time, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (stream_id) 
                DO UPDATE SET 
                    fire_status = EXCLUDED.fire_status,
                    last_detection_time = EXCLUDED.last_detection_time,
                    last_notification_time = CASE 
                        WHEN EXCLUDED.last_notification_time IS NOT NULL 
                        THEN EXCLUDED.last_notification_time 
                        ELSE fire_detection_state.last_notification_time 
                    END,
                    updated_at = EXCLUDED.updated_at""",
                (stream_id, fire_status, detection_time, notification_time, now, now)
            )
            
            logging.debug(f"Updated persistent fire state for {stream_id}: "
                        f"status={fire_status}, notification_time={notification_time}")
            
        except Exception as e:
            logging.error(f"Error updating persistent fire state for {stream_id}: {e}")

    async def load_fire_state_on_stream_start(self, stream_id: UUID):
        """Load persistent fire state when stream starts - synchronize with database"""
        stream_id_str = str(stream_id)
        try:
            # ALWAYS load from database first
            persistent_state = await self.get_persistent_fire_state(stream_id)
            
            # Set in-memory state from database
            self.fire_detection_states[stream_id_str] = persistent_state['fire_status']
            
            # Restore cooldown if within cooldown period
            if persistent_state['last_notification_time']:
                last_notification_timestamp = persistent_state['last_notification_time'].timestamp()
                current_time = time.time()
                time_since_last = current_time - last_notification_timestamp
                
                if time_since_last < self.fire_cooldown_duration:
                    self.fire_notification_cooldowns[stream_id_str] = last_notification_timestamp
                    remaining = self.fire_cooldown_duration - time_since_last
                    logging.info(f"Restored fire cooldown for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_remaining={remaining/60:.1f}min")
                else:
                    logging.info(f"Fire state loaded for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_expired")
            else:
                logging.info(f"Fire state loaded for {stream_id_str}: "
                        f"fire_status='{persistent_state['fire_status']}', "
                        f"no_previous_notifications")
            
        except Exception as e:
            logging.error(f"Error loading fire state for {stream_id}: {e}")
            # Set safe defaults
            self.fire_detection_states[stream_id_str] = "no detection"

    async def cleanup_old_fire_states(self):
        """Clean up old fire detection states - but preserve recent cooldowns"""
        try:
            # Remove fire states for deleted streams
            await self.db_manager.execute_query(
                """DELETE FROM fire_detection_state 
                WHERE stream_id NOT IN (SELECT stream_id FROM video_stream)"""
            )
            
            # Clean up very old notification states (older than 24 hours)
            # This preserves the 10-minute cooldown while cleaning up old data
            day_ago = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(days=1)
            await self.db_manager.execute_query(
                """UPDATE fire_detection_state 
                SET last_notification_time = NULL 
                WHERE last_notification_time < $1""",
                (day_ago,)
            )
            
            logging.debug("Cleaned up old fire detection states")
            
        except Exception as e:
            logging.error(f"Error cleaning up fire states: {e}")

    async def send_immediate_fire_alert(self, workspace_id_str: str, alert_data: Dict[str, Any]):
        """Send immediate fire alert to all notification subscribers in the workspace"""
        try:
            # Get all users in this workspace
            workspace_users_query = """
                SELECT user_id FROM workspace_members WHERE workspace_id = $1
            """
            workspace_users = await self.db_manager.execute_query(
                workspace_users_query, (UUID(workspace_id_str),), fetch_all=True
            )
            
            if not workspace_users:
                return
                
            # Send alert to all users in workspace
            for user_row in workspace_users:
                user_id_str = str(user_row['user_id'])
                
                # Check if user has active notification websocket connections
                subscribers_for_user = []
                async with self._notification_lock:
                    subscribers_for_user = list(self.notification_subscribers.get(user_id_str, set()))
                
                if subscribers_for_user:
                    # Send fire alert to all connected websockets for this user
                    fire_alert_payload = {
                        "type": "fire_emergency",
                        "alert": alert_data,
                        "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    }
                    
                    # Send to all websockets
                    send_tasks = []
                    for ws in subscribers_for_user:
                        if ws.client_state == WebSocketState.CONNECTED:
                            send_tasks.append(ws.send_json(fire_alert_payload))
                    
                    # Send all alerts concurrently
                    if send_tasks:
                        results = await asyncio.gather(*send_tasks, return_exceptions=True)
                        success_count = sum(1 for r in results if not isinstance(r, Exception))
                        logging.info(f"Fire alert sent to {success_count}/{len(send_tasks)} websockets for user {user_id_str}")
                        
        except Exception as e:
            logging.error(f"Error sending immediate fire alert for workspace {workspace_id_str}: {e}")


stream_manager = StreamManager() 

async def initialize_stream_manager(): 
    try:
        await stream_manager.start_background_tasks()
        logging.info("Stream manager background tasks initialized successfully via initialize_stream_manager()")
    except Exception as e:
        logging.error(f"Failed to initialize StreamManager tasks: {e}", exc_info=True)
        asyncio.create_task(stream_manager._restart_background_task_if_needed("initialization_failure"))

async def send_ping(websocket: WebSocket):
    try:
        ping_interval = float(config.get("websocket_ping_interval", 30.0))
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(ping_interval)
            # Double-check state before sending ping
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "type": "ping", 
                        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    })
                except RuntimeError as e:
                    if "close message has been sent" in str(e).lower():
                        logging.debug("Ping failed: WebSocket already closing")
                        break
                    else:
                        raise
            else:
                break 
    except (WebSocketDisconnect, asyncio.CancelledError, ConnectionResetError, RuntimeError):
        logging.debug("Ping task for WebSocket ended (disconnect/cancel/error).")
    except Exception as e:
        logging.error(f"Error in WebSocket ping task: {e}", exc_info=True)

async def _safe_close_websocket(websocket: WebSocket, username_for_log: Optional[str] = None):
    """Safely close a WebSocket connection with proper state checking."""
    try:
        current_state = websocket.client_state
        
        # Only attempt to close if the websocket is in a state that allows closing
        if current_state == WebSocketState.CONNECTED:
            logging.debug(f"Closing websocket for {username_for_log}, state: {current_state}")
            await websocket.close()
        elif current_state == WebSocketState.CONNECTING:
            # Wait a bit for connection to establish or fail, then try to close
            logging.debug(f"Websocket connecting for {username_for_log}, waiting before close")
            await asyncio.sleep(0.1)
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
        elif current_state == WebSocketState.DISCONNECTED:
            logging.debug(f"Websocket already disconnected for {username_for_log}")
        elif current_state == WebSocketState.CLOSING:
            logging.debug(f"Websocket already closing for {username_for_log}")
        else:
            logging.debug(f"Websocket in unexpected state {current_state} for {username_for_log}")
            
    except RuntimeError as e:
        # This is expected if the websocket is already closed/disconnected
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in [
            "websocket is not connected", 
            "already closed", 
            "cannot call", 
            "close message has been sent"
        ]):
            logging.debug(f"Websocket already closed for {username_for_log}: {e}")
        else:
            logging.warning(f"RuntimeError closing websocket for {username_for_log}: {e}")
    except Exception as e:
        logging.warning(f"Unexpected error closing websocket for {username_for_log}: {e}")

@router.post("/start_stream/{stream_id_str}")
async def start_workspace_stream_endpoint( # Renamed to match stream_one.py, path changed
    stream_id_str: str, 
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    try:
        requester_user_id = str(current_user_data["user_id"])
        result = await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id)
        return JSONResponse(content={"status": "success", "details": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Error in /start_stream/{stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.post("/stop_stream/{stream_id_str}") # Path changed
async def stop_workspace_stream_endpoint( # Renamed
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        stream_id_uuid = UUID(stream_id_str)

        stream_info_db = await db_manager_global.execute_query(
            "SELECT workspace_id, name, user_id FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )

        if not stream_info_db: raise HTTPException(status_code=404, detail="Stream not found.")
        
        s_workspace_id, s_name, s_owner_id = stream_info_db['workspace_id'], stream_info_db['name'], stream_info_db['user_id']
        
        await check_workspace_membership_and_get_role(UUID(requester_user_id_str), s_workspace_id)

        updated_rows = await db_manager_global.execute_query(
            "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
            (datetime.now(ZoneInfo("Africa/Cairo")), stream_id_uuid), return_rowcount=True
        )
        
        if updated_rows and updated_rows > 0:
             await stream_manager.add_notification(str(s_owner_id), str(s_workspace_id), stream_id_str, s_name, "inactive", f"Camera '{s_name}' stopped by {requester_username}.")
             return JSONResponse(content={"status": "success", "message": f"Stream '{s_name}' stop request processed."}) # Match stream_one
        else: 
            current_status_db = await db_manager_global.execute_query("SELECT status, is_streaming FROM video_stream WHERE stream_id = $1", (stream_id_uuid,), fetch_one=True)
            if current_status_db and not current_status_db.get('is_streaming', True):
                return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' was already stopped."})
            return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' could not be stopped or was not found active."}) # Match stream_one

    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except ValueError: # For UUID conversion error
        return JSONResponse(status_code=400, content={"status":"error", "message": "Invalid stream ID format."})
    except Exception as e:
        logging.error(f"Error stopping stream {stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.websocket("/stream")
async def websocket_workspace_stream(websocket: WebSocket):
    stream_id_str: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    user_id_for_log: Optional[str] = None
    websocket_closed = False

    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        stream_id_str = query_params.get("stream_id")
        if not stream_id_str:
            await websocket.send_json({"status": "error", "message": "stream_id is required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
            websocket_closed = True
            await websocket.close(1008)
            return
        
        requester_user_id_str = token_data.user_id
        user_id_for_log = requester_user_id_str 
        stream_id_uuid = UUID(stream_id_str)

        # Get stream details with location data
        stream_details_query = """
            SELECT vs.workspace_id, vs.user_id as owner_id, vs.name, vs.is_streaming, vs.status, 
                   u.username as owner_username,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level
            FROM video_stream vs 
            JOIN users u ON vs.user_id = u.user_id 
            WHERE vs.stream_id = $1
        """
        stream_details_db = await db_manager_global.execute_query(stream_details_query, (stream_id_uuid,), fetch_one=True)
        
        if not stream_details_db:
            await websocket.send_json({"status": "error", "message": "Stream not found."})
            websocket_closed = True
            await websocket.close(1008)
            return

        s_workspace_id = stream_details_db['workspace_id']
        s_name = stream_details_db['name']
        s_is_streaming_db = stream_details_db['is_streaming']
        s_status_db = stream_details_db['status']
        s_owner_username = stream_details_db['owner_username']
        
        # Extract location info for response
        location_data = {
            "location": stream_details_db.get('location'),
            "area": stream_details_db.get('area'),
            "building": stream_details_db.get('building'),
            "zone": stream_details_db.get('zone'),
            "floor_level": stream_details_db.get('floor_level')
        }

        requester_role_info = await check_workspace_membership_and_get_role(UUID(requester_user_id_str), s_workspace_id)
        
        stream_is_active_in_manager = False
        async with stream_manager._lock:
            current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
            if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                stream_is_active_in_manager = True
        
        if not s_is_streaming_db or not stream_is_active_in_manager:
            logging.info(f"WS: Stream {stream_id_str} not active (DB: {s_is_streaming_db}, Mgr: {stream_is_active_in_manager}, Status: {s_status_db}). Attempting start by {requester_user_id_str}.")
            
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']:
                await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."})
                websocket_closed = True
                await websocket.close(1008)
                return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str)
                # Wait for stream_manager to pick it up
                for _ in range(config.get("stream_ws_start_wait_attempts", 10)): 
                    async with stream_manager._lock:
                         current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True
                        break
                    await asyncio.sleep(1.0)
                if not stream_is_active_in_manager:
                    db_state_after_start = await db_manager_global.execute_query(
                        "SELECT status, is_streaming from video_stream where stream_id = $1", 
                        (stream_id_uuid,), fetch_one=True
                    )
                    logger.warning(f"Stream {stream_id_str} failed to become active in StreamManager for WS. DB state: {db_state_after_start}")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize. Check server logs."})
                    websocket_closed = True
                    await websocket.close(1011)
                    return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"})
                websocket_closed = True
                await websocket.close(1011)
                return

        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."})
            websocket_closed = True
            await websocket.close(1011)
            return

        # Check connection before sending confirmation
        if websocket.client_state != WebSocketState.CONNECTED:
            logging.warning(f"WebSocket disconnected before sending confirmation for {stream_id_str}")
            return

        # Send connection confirmation with location data
        await websocket.send_json({
            "status": "connected", 
            "message": f"Connected to stream: {s_name}",
            "stream_id": stream_id_str, 
            "owner": s_owner_username, 
            "workspace_id": str(s_workspace_id), 
            "your_role": requester_role_info.get("role"),
            "location_info": location_data
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066

        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            async with stream_manager._lock: 
                stream_info = stream_manager.active_streams.get(stream_id_str, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(
                            thread_pool, frame_to_base64, latest_frame_np
                        )
            
            if not stream_ok:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({"status":"info", "message":"Stream ended or became inactive."})
                break
            
            if latest_frame_b64 and websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "stream_id": stream_id_str, 
                        "frame": latest_frame_b64,
                        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    })
                except RuntimeError as e:
                    if "close message has been sent" in str(e).lower():
                        logging.debug(f"Frame send failed: WebSocket already closing for stream {stream_id_str}")
                        break
                    else:
                        raise
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
        websocket_closed = True
    except asyncio.CancelledError:
        logging.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled (User: {user_id_for_log or 'unknown'}).")
    except ValueError as ve: 
        logging.warning(f"WS stream error (User: {user_id_for_log or 'unknown'}, Stream: {stream_id_str or 'unknown'}): Invalid ID format - {ve}")
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid stream ID format."})
                websocket_closed = True
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"WS stream error ({stream_id_str or 'unknown'}, User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error."})
                websocket_closed = True
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        if stream_id_str:
            await stream_manager.disconnect_client(stream_id_str, websocket)
        
        # Close websocket safely only if not already closed
        if not websocket_closed:
            await _safe_close_websocket(websocket, user_id_for_log)

@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    websocket_closed = False
    connection_start_time = time.time()

    try:
        await websocket.accept()
        
        # ADD connection delay to prevent rapid reconnections
        await asyncio.sleep(0.5)
        
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
            websocket_closed = True
            await websocket.close(1008)
            return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager_global.get_user_by_id(UUID(user_id_str))
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

        # ADD connection stability check
        if websocket.client_state != WebSocketState.CONNECTED:
            logging.warning(f"WebSocket disconnected during authentication for {username_for_log}")
            return
        
        # ADD connection rate limiting
        connection_duration = time.time() - connection_start_time
        if connection_duration < 2.0:  # If connection is too fast, add delay
            await asyncio.sleep(2.0 - connection_duration)

        logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({
            "status": "connected", 
            "message": "Connected to notification stream",
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        })
        
        # Get initial notifications with error handling
        try:
            initial_notifications = await stream_manager.get_notifications(
                user_id_str, workspace_id_filter=None, include_read=False, limit=20
            )
            
            # Send initial notifications if any and still connected
            if initial_notifications and websocket.client_state == WebSocketState.CONNECTED:
                formatted_notifications = []
                for notification in initial_notifications:
                    formatted_notifications.append({
                        "id": notification.get("id"),
                        "user_id": notification.get("user_id"),
                        "workspace_id": notification.get("workspace_id"),
                        "stream_id": notification.get("stream_id"),
                        "camera_name": notification.get("camera_name"),
                        "status": notification.get("status"),
                        "message": notification.get("message"),
                        "timestamp": notification.get("timestamp"),
                        "read": notification.get("read", False)
                    })
                
                await websocket.send_json({
                    "type": "initial_notifications",
                    "notifications": formatted_notifications,
                    "count": len(formatted_notifications)
                })
        except Exception as e_notif:
            logging.error(f"Error getting initial notifications for {username_for_log}: {e_notif}")
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "status": "warning", 
                    "message": "Subscription failed, notifications may be delayed"
                })
        
        # ADD ping with longer interval for stability
        ping_task = asyncio.create_task(send_ping_with_stability_check(websocket, username_for_log))
        logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

        # Main message loop with improved error handling
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while websocket.client_state == WebSocketState.CONNECTED and consecutive_errors < max_consecutive_errors:
            try:
                receive_timeout = float(config.get("websocket_receive_timeout", 60.0))  # Increased timeout
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Reset error counter on successful message
                consecutive_errors = 0
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue
                    
                elif message.get("type") == "mark_read":
                    await handle_mark_read_message(message, user_id_str, username_for_log, websocket)
                
                elif message.get("type") == "acknowledge_fire_alert":
                    alert_id = message.get("alert_id")
                    logging.info(f"Fire alert {alert_id} acknowledged by {username_for_log}")
                            
                else:
                    logging.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

            except asyncio.TimeoutError:
                # Send keepalive on timeout
                if websocket.client_state == WebSocketState.CONNECTED:
                    try:
                        await websocket.send_json({
                            "type": "keepalive", 
                            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                        })
                    except Exception:
                        break
                continue
                
            except WebSocketDisconnect:
                logging.info(f"Notify WS: Client {username_for_log} disconnected normally")
                websocket_closed = True
                break
                
            except Exception as e_recv:
                consecutive_errors += 1
                logging.error(f"Notify WS: Error receiving from {username_for_log} (#{consecutive_errors}): {e_recv}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logging.error(f"Too many consecutive errors for {username_for_log}, closing connection")
                    break
                    
                await asyncio.sleep(1.0)  # Brief pause before retrying
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
        websocket_closed = True
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error."})
                websocket_closed = True
                await websocket.close(1011)
            except Exception:
                pass
    finally:
        # Cleanup with improved error handling
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await asyncio.wait_for(ping_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        if user_id_str:
            try:
                await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
            except Exception as e_unsub:
                logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        if not websocket_closed:
            await _safe_close_websocket(websocket, username_for_log)

async def send_ping_with_stability_check(websocket: WebSocket, username_for_log: str):
    try:
        ping_interval = float(config.get("websocket_ping_interval", 45.0))  # Longer interval
        consecutive_ping_failures = 0
        max_ping_failures = 3
        
        while (websocket.client_state == WebSocketState.CONNECTED and 
               consecutive_ping_failures < max_ping_failures):
            await asyncio.sleep(ping_interval)
            
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "type": "ping", 
                        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    })
                    consecutive_ping_failures = 0  # Reset on success
                except Exception as e:
                    consecutive_ping_failures += 1
                    logging.warning(f"Ping failed for {username_for_log} (#{consecutive_ping_failures}): {e}")
                    if consecutive_ping_failures >= max_ping_failures:
                        break
            else:
                break
                
    except Exception as e:
        logging.debug(f"Ping task ended for {username_for_log}: {e}")

async def handle_mark_read_message(message: dict, user_id_str: str, username_for_log: str, websocket: WebSocket):
    try:
        notif_ids_raw = message.get("notification_ids", [])
        if isinstance(notif_ids_raw, list) and user_id_str:
            updated_count = 0
            valid_notif_ids_to_mark: List[UUID] = []
            
            for nid_str_raw in notif_ids_raw:
                try:
                    valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                except ValueError:
                    logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
            
            if valid_notif_ids_to_mark:
                for nid_uuid in valid_notif_ids_to_mark:
                    try:
                        res = await db_manager_global.execute_query(
                            "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                            (datetime.now(ZoneInfo("Africa/Cairo")), nid_uuid, UUID(user_id_str)), 
                            return_rowcount=True
                        )
                        if res and res > 0:
                            updated_count += 1
                    except Exception as e_mark:
                        logging.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
            
            # Send acknowledgment
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "ack_mark_read", 
                    "ids": notif_ids_raw, 
                    "updated_count": updated_count
                })
    except Exception as e:
        logging.error(f"Error handling mark_read for {username_for_log}: {e}")

@router.get("/notify")
async def get_http_notifications(
    since: Optional[float] = Query(None, description="Timestamp (seconds since epoch) to get notifications from"),
    limit: int = Query(50, ge=1, le=200),
    include_read: bool = Query(True),
    workspace_id: Optional[str] = Query(None, description="Filter by specific workspace ID"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    try:
        # stream_one.py uses workspace_id directly.
        notifications = await stream_manager.get_notifications(user_id_str, workspace_id, since, limit, include_read)
        return { # Match stream_one.py payload
            "status": "success", "count": len(notifications),
            "notifications": notifications, "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        }
    except Exception as e:
        logging.error(f"Error getting HTTP notifications for user {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}") # Match stream_one

@router.post("/notify/{notification_id_str}/read")
async def mark_notification_read_endpoint(
    notification_id_str: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    try:
        notification_id_uuid = UUID(notification_id_str)
        updated_rows = await db_manager_global.execute_query(
            "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3",
            (datetime.now(ZoneInfo("Africa/Cairo")), notification_id_uuid, UUID(user_id_str)), return_rowcount=True
        )
        
        if updated_rows and updated_rows > 0:
            return {"status": "success", "message": "Notification marked as read"}
        else: # Check if it exists or was already read (match stream_one.py logic)
            exists_res = await db_manager_global.execute_query("SELECT is_read FROM notifications WHERE notification_id = $1 AND user_id = $2", (notification_id_uuid, UUID(user_id_str)), fetch_one=True)
            if not exists_res: raise HTTPException(status_code=404, detail="Notification not found or not yours.")
            # if exists_res.get("is_read"): return {"status": "info", "message": "Notification was already marked as read."} # This is covered by updated_rows = 0
            return {"status": "info", "message": "Notification was already marked as read or no change made."} # Match stream_one.py

    except ValueError: raise HTTPException(status_code=400, detail="Invalid notification ID format.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error marking notification {notification_id_str} as read for {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to mark notification as read: {str(e)}")


@router.get("/streams/locations/search")
async def search_streams_by_location_filters(
    q: Optional[str] = Query(None, description="Search query for location names"),
    location_type: Optional[Union[str, List[str]]] = Query(None, description="Filter by location type(s): building, floor_level, zone, area, location"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific zone(s)"),
    workspace_id: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Search for streams using location-based filters and text search."""
    
    # Parse all the filter parameters
    location_type = parse_string_or_list(location_type)
    locations = parse_string_or_list(locations)
    areas = parse_string_or_list(areas)
    buildings = parse_string_or_list(buildings)
    floor_levels = parse_string_or_list(floor_levels)
    zones = parse_string_or_list(zones)
    
    user_id_str = str(current_user_data["user_id"])
    username = current_user_data.get("username", "unknown")
    
    try:
        # Complete query to match /source/user fields
        base_query = """
            SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, 
                   vs.type, vs.status, vs.is_streaming, vs.created_at, vs.updated_at,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                   vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                   w.name as workspace_name, vs.workspace_id
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        
        # Add text search with special handling for location_type + q combination
        if q:
            # Check if q contains comma-separated values and we have a specific location_type
            q_values = parse_string_or_list(q)
            
            if location_type and len(location_type) == 1 and q_values and len(q_values) > 1:
                # Handle specific case: location_type=areas&q=Area 1,Area 2
                location_field_map = {
                    "area": "vs.area",
                    "areas": "vs.area", 
                    "building": "vs.building",
                    "buildings": "vs.building",
                    "floor_level": "vs.floor_level",
                    "floor_levels": "vs.floor_level",
                    "zone": "vs.zone",
                    "zones": "vs.zone",
                    "location": "vs.location",
                    "locations": "vs.location"
                }
                
                field_name = location_field_map.get(location_type[0])
                if field_name:
                    # Use exact match for specific values
                    param_count += 1
                    q_placeholders = ", ".join([f"${param_count + i}" for i in range(len(q_values))])
                    base_query += f" AND {field_name} IN ({q_placeholders})"
                    params.extend(q_values)
                    param_count += len(q_values) - 1
                else:
                    # Fallback to general text search
                    param_count += 1
                    search_condition = f"""
                        AND (
                            vs.name ILIKE ${param_count} OR 
                            vs.building ILIKE ${param_count} OR 
                            vs.floor_level ILIKE ${param_count} OR 
                            vs.zone ILIKE ${param_count} OR 
                            vs.area ILIKE ${param_count} OR 
                            vs.location ILIKE ${param_count}
                        )
                    """
                    base_query += search_condition
                    params.append(f"%{q}%")
            else:
                # Regular text search
                param_count += 1
                search_condition = f"""
                    AND (
                        vs.name ILIKE ${param_count} OR 
                        vs.building ILIKE ${param_count} OR 
                        vs.floor_level ILIKE ${param_count} OR 
                        vs.zone ILIKE ${param_count} OR 
                        vs.area ILIKE ${param_count} OR 
                        vs.location ILIKE ${param_count}
                    )
                """
                base_query += search_condition
                params.append(f"%{q}%")
        
        # Add location type filter (now supports multiple types)
        if location_type:
            type_conditions = []
            for loc_type in location_type:
                if loc_type == "building":
                    type_conditions.append("(vs.building IS NOT NULL AND vs.building != '')")
                elif loc_type == "floor_level":
                    type_conditions.append("(vs.floor_level IS NOT NULL AND vs.floor_level != '')")
                elif loc_type == "zone":
                    type_conditions.append("(vs.zone IS NOT NULL AND vs.zone != '')")
                elif loc_type == "area":
                    type_conditions.append("(vs.area IS NOT NULL AND vs.area != '')")
                elif loc_type == "location":
                    type_conditions.append("(vs.location IS NOT NULL AND vs.location != '')")
            
            if type_conditions:
                base_query += f" AND ({' OR '.join(type_conditions)})"
        
        # Add specific location filters
        if locations:
            param_count += 1
            location_placeholders = ", ".join([f"${param_count + i}" for i in range(len(locations))])
            base_query += f" AND vs.location IN ({location_placeholders})"
            params.extend(locations)
            param_count += len(locations) - 1
        
        if areas:
            param_count += 1
            area_placeholders = ", ".join([f"${param_count + i}" for i in range(len(areas))])
            base_query += f" AND vs.area IN ({area_placeholders})"
            params.extend(areas)
            param_count += len(areas) - 1
        
        if buildings:
            param_count += 1
            building_placeholders = ", ".join([f"${param_count + i}" for i in range(len(buildings))])
            base_query += f" AND vs.building IN ({building_placeholders})"
            params.extend(buildings)
            param_count += len(buildings) - 1
        
        if floor_levels:
            param_count += 1
            floor_placeholders = ", ".join([f"${param_count + i}" for i in range(len(floor_levels))])
            base_query += f" AND vs.floor_level IN ({floor_placeholders})"
            params.extend(floor_levels)
            param_count += len(floor_levels) - 1
        
        if zones:
            param_count += 1
            zone_placeholders = ", ".join([f"${param_count + i}" for i in range(len(zones))])
            base_query += f" AND vs.zone IN ({zone_placeholders})"
            params.extend(zones)
        
        base_query += " ORDER BY u.username, vs.created_at DESC"
        
        # Rest of the function remains the same...
        results = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        results = results or []
        
        # Format results to match /source/user response structure exactly
        formatted_results = [
            {
                "id": str(r["stream_id"]),
                "user_id": str(r["user_id"]),
                "owner_username": r["owner_username"],
                "name": r["name"],
                "path": r["path"],
                "type": r["type"],
                "status": r["status"],
                "is_streaming": r["is_streaming"],
                "location": r["location"],
                "area": r["area"],
                "building": r["building"],
                "floor_level": r["floor_level"],
                "zone": r["zone"],
                "latitude": float(r["latitude"]) if r["latitude"] else None,
                "longitude": float(r["longitude"]) if r["longitude"] else None,
                "count_threshold_greater": r["count_threshold_greater"],
                "count_threshold_less": r["count_threshold_less"],
                "alert_enabled": r["alert_enabled"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                "workspace_id": str(r["workspace_id"]) if r["workspace_id"] else None,
                "workspace_name": r["workspace_name"],
                "static_base64": encoded_string
            } for r in results
        ]
        
        return formatted_results
        
    except ValueError as ve:
        logger.error(f"Invalid data for location search for user {username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except Exception as e:
        logger.error(f"Error searching streams by location for user {username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while searching streams.")


@router.get("/debug/fire-cooldown/{stream_id}")
async def debug_fire_cooldown_status(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Debug endpoint to check fire notification cooldown status"""
    try:
        status = await stream_manager.get_fire_notification_cooldown_status(stream_id)
        return JSONResponse(content={
            "status": "success",
            "cooldown_info": status,
            "current_time": time.time(),
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting fire cooldown status for {stream_id}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.get("/debug/fire-state/{stream_id}")
async def debug_fire_state(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Debug endpoint to check persistent fire state"""
    try:
        stream_uuid = UUID(stream_id)
        persistent_state = await stream_manager.get_persistent_fire_state(stream_uuid)
        
        cooldown_info = {}
        if persistent_state['last_notification_time']:
            time_since_last = (datetime.now(ZoneInfo("Africa/Cairo")) - persistent_state['last_notification_time']).total_seconds()
            cooldown_info = {
                'time_since_last_notification': time_since_last,
                'cooldown_remaining': max(0, stream_manager.fire_cooldown_duration - time_since_last),
                'can_notify': time_since_last >= stream_manager.fire_cooldown_duration
            }
        
        return {
            "status": "success",
            "persistent_state": {
                "fire_status": persistent_state['fire_status'],
                "last_detection_time": persistent_state['last_detection_time'].isoformat() if persistent_state['last_detection_time'] else None,
                "last_notification_time": persistent_state['last_notification_time'].isoformat() if persistent_state['last_notification_time'] else None
            },
            "cooldown_info": cooldown_info,
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/fire-test/{stream_id}")
async def test_fire_cooldown_system(
    stream_id: str,
    action: str = Query("status", description="Action: status, reset, force_notify"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Test endpoint for fire cooldown system"""
    try:
        stream_uuid = UUID(stream_id)
        current_time = datetime.now(ZoneInfo("Africa/Cairo"))
        
        if action == "status":
            # Get comprehensive status
            persistent_state = await stream_manager.get_persistent_fire_state(stream_uuid)
            cooldown_info = await stream_manager.get_fire_notification_cooldown_status(stream_id)
            
            in_memory_state = stream_manager.fire_detection_states.get(stream_id, "unknown")
            
            return {
                "status": "success",
                "stream_id": stream_id,
                "current_time": current_time.isoformat(),
                "persistent_state": {
                    "fire_status": persistent_state['fire_status'],
                    "last_detection_time": persistent_state['last_detection_time'].isoformat() if persistent_state['last_detection_time'] else None,
                    "last_notification_time": persistent_state['last_notification_time'].isoformat() if persistent_state['last_notification_time'] else None
                },
                "in_memory_state": in_memory_state,
                "cooldown_info": cooldown_info,
                "next_notification_available": (
                    persistent_state['last_notification_time'] + timedelta(seconds=stream_manager.fire_cooldown_duration)
                ).isoformat() if persistent_state['last_notification_time'] else "immediately"
            }
        
        elif action == "reset":
            # Reset cooldown for testing
            stream_manager.fire_notification_cooldowns.pop(stream_id, None)
            await stream_manager.update_persistent_fire_state(stream_uuid, "no detection")
            return {"status": "success", "message": "Fire state and cooldown reset"}
        
        elif action == "force_notify":
            # Force a test notification (bypasses cooldown)
            await stream_manager.add_notification(
                str(current_user_data["user_id"]), 
                "test-workspace", 
                stream_id,
                "Test Camera", 
                "fire_alert", 
                "🔥 TEST FIRE NOTIFICATION - This is a test"
            )
            return {"status": "success", "message": "Test notification sent"}
        
        else:
            return {"status": "error", "message": "Invalid action. Use: status, reset, or force_notify"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/stream-diagnosis/{stream_id}")
async def diagnose_stream_issues(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Diagnose stream activation and processing issues"""
    try:
        diagnosis = await stream_manager.diagnose_stream_activation_failure(stream_id)
        return {"status": "success", "diagnosis": diagnosis}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/people-cooldown/{stream_id}")
async def debug_people_count_cooldown_status(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Debug endpoint to check people count notification cooldown status"""
    try:
        current_time = time.time()
        last_notification_time = stream_manager.people_count_notification_cooldowns.get(stream_id, 0)
        
        if last_notification_time == 0:
            status = {
                "stream_id": stream_id,
                "can_notify": True,
                "last_notification": None,
                "cooldown_remaining": 0
            }
        else:
            time_since_last = current_time - last_notification_time
            can_notify = time_since_last >= stream_manager.people_count_cooldown_duration
            cooldown_remaining = max(0, stream_manager.people_count_cooldown_duration - time_since_last)
            
            status = {
                "stream_id": stream_id,
                "can_notify": can_notify,
                "last_notification": datetime.fromtimestamp(last_notification_time, tz=timezone.utc).isoformat(),
                "cooldown_remaining": cooldown_remaining,
                "cooldown_remaining_minutes": cooldown_remaining / 60.0
            }
        
        return JSONResponse(content={
            "status": "success",
            "cooldown_info": status,
            "current_time": current_time,
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting people count cooldown status for {stream_id}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/debug/validate-stream")
async def debug_validate_stream(
    source: str = Query(..., description="Stream source URL or file path"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Debug endpoint to test stream validation"""
    try:
        start_time = time.time()
        
        # Check connectivity first (for RTSP)
        connectivity_ok = False
        if source.startswith('rtsp://'):
            connectivity_ok = StreamValidator.check_rtsp_connectivity(source, timeout=150)
            if not connectivity_ok:
                return {
                    "status": "failed",
                    "message": "Network connectivity check failed",
                    "source": source,
                    "duration": time.time() - start_time
                }
        
        # Validate stream
        is_valid = await StreamValidator.validate_with_retry(source, max_retries=2, timeout=8)
        
        duration = time.time() - start_time
        
        return {
            "status": "success" if is_valid else "failed",
            "valid": is_valid,
            "source": source,
            "duration": duration,
            "connectivity_check": connectivity_ok if source.startswith('rtsp://') else "not applicable"
        }
        
    except Exception as e:
        logging.error(f"Error validating stream {source}: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "source": source
        }

# @router.websocket("/notify")
# async def websocket_notify(websocket: WebSocket):
#     user_id_str: Optional[str] = None
#     username_for_log: Optional[str] = None
#     ping_task: Optional[asyncio.Task] = None
#     websocket_closed = False  # Track if we've already closed the connection

#     try:
#         await websocket.accept()
#         token = await session_manager_global.get_token_from_websocket(websocket)
#         if not token:
#             await websocket.send_json({"status": "error", "message": "Authentication token required."})
#             websocket_closed = True
#             await websocket.close(1008)
#             return

#         token_data = await session_manager_global.verify_token(token, "access")
#         if not token_data or await session_manager_global.is_token_blacklisted(token):
#             await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
#             websocket_closed = True
#             await websocket.close(1008)
#             return
        
#         user_id_str = token_data.user_id
#         user_db_data = await user_manager_global.get_user_by_id(UUID(user_id_str))
#         username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

#         logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
#         # Check if still connected before sending
#         if websocket.client_state != WebSocketState.CONNECTED:
#             logging.warning(f"WebSocket disconnected during authentication for {username_for_log}")
#             return
            
#         await websocket.send_json({
#             "status": "connected", 
#             "message": "Connected to notification stream",
#             "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
#         })
        
#         # Get initial notifications
#         initial_notifications = await stream_manager.get_notifications(
#             user_id_str, workspace_id_filter=None, include_read=False, limit=20
#         )
        
#         # Format notifications
#         formatted_notifications = []
#         for notification in initial_notifications:
#             formatted_notifications.append({
#                 "id": notification.get("id"),  # Use "id" not "notification_id"
#                 "user_id": notification.get("user_id"),
#                 "workspace_id": notification.get("workspace_id"),
#                 "stream_id": notification.get("stream_id"),
#                 "camera_name": notification.get("camera_name"),
#                 "status": notification.get("status"),
#                 "message": notification.get("message"),
#                 "timestamp": notification.get("timestamp"),  # Already in correct format
#                 "read": notification.get("read", False)
#             })
        
#         # Send initial notifications if any and still connected
#         if formatted_notifications and websocket.client_state == WebSocketState.CONNECTED:
#             await websocket.send_json(formatted_notifications)
        
#         # Subscribe to notifications
#         subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
#         if not subscription_success:
#             logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
#             if websocket.client_state == WebSocketState.CONNECTED:
#                 await websocket.send_json({
#                     "status": "warning", 
#                     "message": "Subscription failed, notifications may be delayed"
#                 })
        
#         ping_task = asyncio.create_task(send_ping(websocket))
#         logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

#         # Main message loop
#         while websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
#                 message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
#                 if message.get("type") == "pong": 
#                     logging.debug(f"Notification WS: Pong received from {username_for_log}")
#                     continue
                    
#                 elif message.get("type") == "mark_read":
#                     notif_ids_raw = message.get("notification_ids", [])
#                     if isinstance(notif_ids_raw, list) and user_id_str:
#                         updated_count = 0
#                         valid_notif_ids_to_mark: List[UUID] = []
                        
#                         for nid_str_raw in notif_ids_raw:
#                             try:
#                                 valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
#                             except ValueError:
#                                 logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
#                         if valid_notif_ids_to_mark:
#                             for nid_uuid in valid_notif_ids_to_mark:
#                                 try:
#                                     res = await db_manager_global.execute_query(
#                                         "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
#                                         (datetime.now(ZoneInfo("Africa/Cairo")), nid_uuid, UUID(user_id_str)), 
#                                         return_rowcount=True
#                                     )
#                                     if res and res > 0:
#                                         updated_count += 1
#                                 except Exception as e_mark:
#                                     logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        
#                         # Send acknowledgment only if still connected
#                         if websocket.client_state == WebSocketState.CONNECTED:
#                             await websocket.send_json({
#                                 "type": "ack_mark_read", 
#                                 "ids": notif_ids_raw, 
#                                 "updated_count": updated_count
#                             })
#                 else:
#                     logging.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

#             except asyncio.TimeoutError:
#                 continue  # No message from client, normal
#             except WebSocketDisconnect:
#                 logging.info(f"Notify WS: Client {username_for_log} disconnected normally")
#                 websocket_closed = True
#                 break
#             except asyncio.CancelledError:
#                 logging.info(f"Notify WS: Task cancelled for {username_for_log}")
#                 raise
#             except Exception as e_recv:
#                 logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True)
#                 break
    
#     except WebSocketDisconnect:
#         logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
#         websocket_closed = True
#     except asyncio.CancelledError:
#         logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
#     except Exception as e_outer:
#         logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
#         # Try to send error message if connection is still viable
#         if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 await websocket.send_json({"status": "error", "message": "Internal server error."})
#                 websocket_closed = True
#                 await websocket.close(1011)
#             except Exception:
#                 pass  # Ignore error during error handling
#     finally:
#         # Comprehensive cleanup
#         logging.debug(f"Notify WS: Starting cleanup for {username_for_log or 'unknown'}")

#         # Cancel ping task first
#         if ping_task and not ping_task.done():
#             ping_task.cancel()
#             try:
#                 await asyncio.wait_for(ping_task, timeout=1.0)
#             except (asyncio.CancelledError, asyncio.TimeoutError):
#                 pass
#             except Exception as e_ping:
#                 logging.debug(f"Error cancelling ping task for {username_for_log}: {e_ping}")

#         # Unsubscribe from notifications
#         if user_id_str:
#             try:
#                 await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
#             except Exception as e_unsub:
#                 logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

#         # Close websocket safely only if not already closed
#         if not websocket_closed:
#             await _safe_close_websocket(websocket, username_for_log)
        
#         logging.debug(f"Notify WS: Cleanup completed for {username_for_log or 'unknown'}")


# @router.get("/streams/locations/search")
# async def search_streams_by_location(
#     location_type: str = Query(..., description="Type of location filter: location, area, building, zone, floor_level"),
#     q: str = Query(..., description="Search query for the location"),
#     current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
# ):
#     """Search streams by location information"""
#     try:
#         user_id_str = str(current_user_data["user_id"])
#         user_id_obj = UUID(user_id_str)
        
#         # Get user's workspaces
#         user_workspaces_db = await db_manager_global.execute_query(
#             "SELECT workspace_id FROM workspace_members WHERE user_id = $1", 
#             (user_id_obj,), fetch_all=True
#         )
#         user_workspaces_db = user_workspaces_db or []
#         target_workspace_ids_objs = [row['workspace_id'] for row in user_workspaces_db]
        
#         if not target_workspace_ids_objs:
#             return JSONResponse(content={
#                 "status": "success",
#                 "streams": [],
#                 "total": 0,
#                 "message": "No workspaces found for user"
#             })
        
#         # Build the query based on location_type
#         location_column_map = {
#             "location": "vs.location",
#             "area": "vs.area", 
#             "building": "vs.building",
#             "zone": "vs.zone",
#             "floor_level": "vs.floor_level"
#         }
        
#         if location_type not in location_column_map:
#             return JSONResponse(status_code=400, content={
#                 "status": "error",
#                 "message": f"Invalid location_type. Must be one of: {list(location_column_map.keys())}"
#             })
        
#         location_column = location_column_map[location_type]
        
#         # Create placeholders for workspace IDs
#         workspace_placeholders = ', '.join([f'${i+2}' for i in range(len(target_workspace_ids_objs))])
        
#         # Build the search query
#         search_query = f"""
#             SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
#                    vs.user_id as owner_id, u.username as owner_username,
#                    vs.workspace_id, w.name as workspace_name,
#                    vs.created_at, vs.updated_at,
#                    vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
#                    vs.latitude, vs.longitude,
#                    vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled
#             FROM video_stream vs
#             JOIN users u ON vs.user_id = u.user_id
#             JOIN workspaces w ON vs.workspace_id = w.workspace_id
#             WHERE vs.workspace_id IN ({workspace_placeholders})
#             AND {location_column} ILIKE $1
#             ORDER BY w.name, vs.name
#         """
        
#         # Execute the query
#         search_pattern = f"%{q}%"
#         query_params = [search_pattern] + target_workspace_ids_objs
        
#         streams_data_db = await db_manager_global.execute_query(
#             search_query, tuple(query_params), fetch_all=True
#         )
#         streams_data_db = streams_data_db or []
        
#         # Format results
#         formatted_streams = []
#         for s in streams_data_db:
#             formatted_streams.append({
#                 "id": str(s["stream_id"]),  # Make sure this matches your frontend expectation
#                 "stream_id": str(s["stream_id"]),
#                 "name": s["name"],
#                 "path": s["path"],
#                 "type": s["type"],
#                 "status": s["status"],
#                 "is_streaming": s["is_streaming"],
#                 "owner_id": str(s["owner_id"]),
#                 "owner_username": s["owner_username"],
#                 "workspace_id": str(s["workspace_id"]),
#                 "workspace_name": s["workspace_name"],
#                 "created_at": s["created_at"].isoformat() if s["created_at"] else None,
#                 "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
#                 "can_control": True,
#                 # Location fields
#                 "location": s["location"],
#                 "area": s["area"],
#                 "building": s["building"],
#                 "zone": s["zone"],
#                 "floor_level": s["floor_level"],
#                 "latitude": float(s["latitude"]) if s["latitude"] else None,
#                 "longitude": float(s["longitude"]) if s["longitude"] else None,
#                 # Alert thresholds
#                 "count_threshold_greater": s["count_threshold_greater"],
#                 "count_threshold_less": s["count_threshold_less"],
#                 "alert_enabled": s["alert_enabled"]
#             })
        
#         return JSONResponse(content={
#             "status": "success", 
#             "streams": formatted_streams,
#             "total": len(formatted_streams),
#             "search_criteria": {
#                 "location_type": location_type,
#                 "query": q
#             }
#         })
        
#     except Exception as e:
#         logging.error(f"Error searching streams by location for user {user_id_str}: {e}", exc_info=True)
#         return JSONResponse(status_code=500, content={
#             "status": "error",
#             "message": "Internal server error during location search"
#         })
