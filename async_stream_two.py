# async_stream_two.py
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
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Union
from ultralytics import YOLO
from async_utils import send_fire_alert_email, frame_to_base64, get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists, parse_string_or_list, encoded_string # ensure_... is async
from async_database import DatabaseManager
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from async_config import config
from async_session_manager import SessionManager 
from async_user_manager import UserManager
from shared_stream import VideoFileManager
import concurrent.futures
import os
from starlette.websockets import WebSocketState
from async_workspaces import check_workspace_membership_and_get_role
from schemas_models import ThresholdSettings

session_manager_global = SessionManager()
user_manager_global = UserManager()
db_manager_global = DatabaseManager()

logger = logging.getLogger(__name__) 
router = APIRouter(tags=["stream"]) 

# ThreadPoolExecutor for CPU-bound tasks like YOLO and cv2
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 2 + 4))


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
        self.fire_notification_cooldowns: Dict[str, float] = {}
        self.fire_cooldown_duration = 600.0  # 10 minutes in seconds
        self.frame_buffer_size = config.get("cv_frame_buffer_size", 3)
        self.last_healthcheck = datetime.now(timezone.utc)
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        self.db_manager = DatabaseManager() # StreamManager's own instance
        self.user_manager = UserManager() 
        self.qdrant_client = QdrantClient(
            url=config.get("qdrant_url", "localhost"), 
            port=config.get("qdrant_port", 6333), 
            timeout=config.get("qdrant_timeout", 30.0)
        )
        self.people_model, self.gender_model, self.fire_model = self._initialize_model() # Sync init is fine
        
        self.video_file_manager = VideoFileManager()
        self.fire_detection_states: Dict[str, str] = {}  # Track current fire status per stream
        self.fire_detection_frame_counts: Dict[str, int] = {}  # Track frame counts per stream

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
                current_time_ts = datetime.now(timezone.utc).timestamp()
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
                    
                if expired_cooldown_keys:
                    logging.debug(f"Cleaned up {len(expired_cooldown_keys)} expired fire notification cooldowns")
                
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
            if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2: # Avoid too frequent checks
                return

            current_time_utc = datetime.now(timezone.utc)
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
            self.last_healthcheck = datetime.now(timezone.utc)
    
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
        current_time_ts = datetime.now(timezone.utc).timestamp()
        
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

    async def add_notification(self, user_id: str, workspace_id: str, stream_id: str, camera_name: str, status: str, message: str):
        now_dt = datetime.now(timezone.utc)
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

    async def deliver_notification_to_subscribers(self, user_id: str, notification: Dict[str, Any]):
        subscribers_for_user_copy: List[WebSocket] = []
        async with self._notification_lock:
            subscribers_for_user_copy = list(self.notification_subscribers.get(user_id, set()))

        if not subscribers_for_user_copy: return
        
        notif_payload = {"type": "notification", "notification": notification, "server_time": datetime.now(timezone.utc).timestamp()}
        
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
            await websocket.send_json({"type": "subscription_confirmed", "for_user_id": user_id, "timestamp": datetime.now(timezone.utc).timestamp()})
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

        # NEW: Clean up fire notification cooldown tracking
        self.fire_notification_cooldowns.pop(stream_id_str, None)

        try:
            if stop_event_obj: stop_event_obj.set()
            if task_obj and not task_obj.done():
                task_obj.cancel()
                try: 
                    await asyncio.wait_for(task_obj, timeout=config.get("stream_stop_timeout_seconds", 5.0))
                except asyncio.CancelledError: logging.debug(f"Stream task for {stream_id_str} cancelled as expected.")
                except asyncio.TimeoutError: logging.warning(f"Timeout stopping stream task {stream_id_str}.")
            
            logging.info(f"Stream {stream_id_str} processing task signaled to stop locally.")
            now_utc = datetime.now(timezone.utc)
            
            if for_restart:
                # is_streaming remains TRUE, status indicates it's being restarted
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
                'start_time': datetime.now(timezone.utc),
                'location_info': location_info or {}  # NEW: Store location info
            } 

        try:
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'processing', last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            
            stop_event = threading.Event() # For sync parts within the async task
            self.stream_processing_stats[stream_id_str] = {"frames_processed": 0, "detection_count": 0, "avg_processing_time": 0.0, "last_updated": datetime.now(timezone.utc)}

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
                    'clients': set(), 'latest_frame': None, 'last_frame_time': datetime.now(timezone.utc),
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

    def generate_fire_alert_message(self, fire_status: str, stream_id_str: str) -> str:
        """Generate fire alert message with cooldown information."""
        base_message = f"Fire/smoke detected: {fire_status}"
        # base_message = f"🔥 FIRE/SMOKE ALERT: {fire_status.upper()} detected"
        
        # Add cooldown info for transparency
        current_time = time.time()
        last_notification = self.fire_notification_cooldowns.get(stream_id_str, 0)
        
        if last_notification > 0:
            time_since_last = (current_time - last_notification) / 60.0  # in minutes
            cooldown_minutes = self.fire_cooldown_duration / 60.0
            if time_since_last < cooldown_minutes:
                base_message += f" (Last alert: {time_since_last:.1f}min ago)"
        
        # base_message += " - Take immediate action!"
        
        return base_message

    def insert_detection_data_with_location(self, username: str, camera_id_str: str, camera_name: str, 
                                           count: int, male_count: int, female_count: int, fire_status: str, frame: np.ndarray, workspace_id: UUID, 
                                           location_info: Optional[Dict[str, Any]] = None):
        """Insert detection data with location information into Qdrant."""
        if count == 0: return

        target_collection_name = get_workspace_qdrant_collection_name(workspace_id)
        now_utc = datetime.now(timezone.utc)
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
                if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
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
        """Validate stream source before processing"""
        loop = asyncio.get_event_loop()
        
        def _check_source():
            try:
                # Check if it's a file
                if not source.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')) and not source.isdigit():
                    import os
                    if not os.path.exists(source):
                        logging.error(f"Video file does not exist: {source}")
                        return False
                    
                    if not os.access(source, os.R_OK):
                        logging.error(f"Video file is not readable: {source}")
                        return False
                    
                    file_size = os.path.getsize(source)
                    if file_size == 0:
                        logging.error(f"Video file is empty: {source}")
                        return False
                
                # Quick validation with OpenCV
                test_cap = cv2.VideoCapture(source)
                if not test_cap.isOpened():
                    logging.error(f"Cannot open video source: {source}")
                    test_cap.release()
                    return False
                
                # Try to read one frame
                ret, frame = test_cap.read()
                test_cap.release()
                
                if not ret or frame is None:
                    logging.error(f"Cannot read from video source: {source}")
                    return False
                
                return True
                
            except Exception as e:
                logging.error(f"Error validating source {source}: {e}")
                return False
        
        return await loop.run_in_executor(thread_pool, _check_source)

    async def debug_all_streams(self) -> Dict[str, Any]:
        """Get debug information for all streams"""
        
        # Get all streams from database
        try:
            db_streams = await self.db_manager.execute_query(
                "SELECT stream_id, name, status, is_streaming FROM video_stream WHERE is_streaming = TRUE",
                fetch_all=True
            )
            db_streams = db_streams or []
        except Exception as e:
            db_streams = []
            logging.error(f"Failed to get streams from database: {e}")
        
        # Get memory streams
        async with self._lock:
            memory_stream_ids = list(self.active_streams.keys())
        
        # Get shared streams
        shared_streams = {}
        if hasattr(self, 'video_file_manager'):
            shared_streams = self.video_file_manager.get_all_stats()
        
        all_stream_ids = set()
        all_stream_ids.update(str(stream['stream_id']) for stream in db_streams)
        all_stream_ids.update(memory_stream_ids)
        
        detailed_status = {}
        for stream_id_str in all_stream_ids:
            try:
                detailed_status[stream_id_str] = await self.get_detailed_stream_status(stream_id_str)
            except Exception as e:
                detailed_status[stream_id_str] = {"error": f"Failed to get status: {e}"}
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_db_streams": len(db_streams),
            "total_memory_streams": len(memory_stream_ids),
            "total_shared_streams": len(shared_streams),
            "db_stream_ids": [str(s['stream_id']) for s in db_streams],
            "memory_stream_ids": memory_stream_ids,
            "shared_stream_sources": list(shared_streams.keys()),
            "detailed_status": detailed_status,
            "shared_streams_detail": shared_streams
        }

    async def log_stream_manager_state(self):
        """Log current state of stream manager for debugging"""
        try:
            debug_info = await self.debug_all_streams()
            
            logging.info("=== STREAM MANAGER STATE DEBUG ===")
            logging.info(f"Database streams: {debug_info['total_db_streams']}")
            logging.info(f"Memory streams: {debug_info['total_memory_streams']}")
            logging.info(f"Shared streams: {debug_info['total_shared_streams']}")
            
            if debug_info['total_memory_streams'] > 0:
                logging.info(f"Active memory stream IDs: {debug_info['memory_stream_ids']}")
            
            # Log any discrepancies
            db_ids = set(debug_info['db_stream_ids'])
            memory_ids = set(debug_info['memory_stream_ids'])
            
            only_in_db = db_ids - memory_ids
            only_in_memory = memory_ids - db_ids
            
            if only_in_db:
                logging.warning(f"Streams in DB but not in memory: {only_in_db}")
            
            if only_in_memory:
                logging.warning(f"Streams in memory but not in DB: {only_in_memory}")
            
            # Log problematic streams
            for stream_id, status in debug_info['detailed_status'].items():
                if 'error' in status:
                    logging.error(f"Stream {stream_id} has errors: {status}")
                elif status.get('memory_status', {}).get('status') in ['starting', 'active_pending']:
                    logging.warning(f"Stream {stream_id} stuck in {status.get('memory_status', {}).get('status')} state")
            
            logging.info("=== END STREAM MANAGER STATE DEBUG ===")
            
        except Exception as e:
            logging.error(f"Error logging stream manager state: {e}", exc_info=True)

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
                    if start_time and (datetime.now(timezone.utc) - start_time).total_seconds() > 30:
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
                'start_time': datetime.now(timezone.utc),
                'location_info': location_info or {}
            }

        try:
            logging.info(f"Updating database status to 'processing' for stream {stream_id_str}")
            await self.db_manager.execute_query(
                "UPDATE video_stream SET status = 'processing', last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", 
                (stream_id,)
            )
            
            stop_event = threading.Event()
            self.stream_processing_stats[stream_id_str] = {
                "frames_processed": 0, 
                "detection_count": 0, 
                "avg_processing_time": 0.0, 
                "last_updated": datetime.now(timezone.utc)
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
                        'clients': set(), 'latest_frame': None, 'last_frame_time': datetime.now(timezone.utc),
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

    def detect_objects_with_threshold(self, frame: np.ndarray, conf_threshold: float = 0.4, 
                                    threshold_settings: Dict[str, Any] = None,
                                    stream_id_str: str = None) -> tuple[np.ndarray, int, bool, int, int, str]:
        """FIXED: Object detection with stream-specific fire detection tracking"""
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
            people_results = self.people_model.predict(source=input_frame, conf=conf_threshold, classes=[0], verbose=False)

            person_count = 0
            if people_results and people_results[0].boxes is not None:
                person_count = len(people_results[0].boxes)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 3 == 0:
                gender_results = self.gender_model(source=input_frame, conf=0.3, verbose=False)
                if gender_results and gender_results[0].boxes is not None:
                    male_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 1)
                    female_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 0)
                    cache['male_count'] = male_count
                    cache['female_count'] = female_count
                    cache['last_gender_frame'] = frame_count

            # CRITICAL FIX: Fire detection with proper state management
            if frame_count % 10 == 0:  # Run fire detection every 10th frame
                fire_results = self.fire_model(source=input_frame, conf=0.3, verbose=False)
                
                # Determine current fire status
                current_fire_status = "no detection"
                if fire_results and fire_results[0].boxes is not None:
                    classes = [int(box.cls) for box in fire_results[0].boxes]
                    if 0 in classes:
                        current_fire_status = "fire"
                    elif 1 in classes:
                        current_fire_status = "smoke"
                
                # IMPORTANT: Only update cache when we actually run detection
                cache['fire_status'] = current_fire_status
                cache['last_fire_frame'] = frame_count
                
                # Update global fire state for this stream
                if stream_id_str:
                    previous_status = self.fire_detection_states.get(stream_id_str, "no detection")
                    self.fire_detection_states[stream_id_str] = current_fire_status
                    
                    # Log fire state changes
                    if current_fire_status != previous_status:
                        if current_fire_status in ["fire", "smoke"]:
                            logger.warning(f"🔥 FIRE STATE CHANGE: {stream_id_str} changed from '{previous_status}' to '{current_fire_status}' at frame {frame_count}")
                        else:
                            logger.info(f"🌊 FIRE CLEARED: {stream_id_str} changed from '{previous_status}' to '{current_fire_status}' at frame {frame_count}")

            # Use cached results
            male_count = cache['male_count']
            female_count = cache['female_count']
            fire_status = cache['fire_status']

            # Threshold checking for people
            alert_triggered = False
            if threshold_settings and threshold_settings.get("alert_enabled", False):
                greater_than = threshold_settings.get("greater_than")
                less_than = threshold_settings.get("less_than")
                
                if greater_than is not None and person_count > greater_than:
                    alert_triggered = True
                elif less_than is not None and person_count < less_than:
                    alert_triggered = True

            # Annotate frame
            annotated_frame = people_results[0].plot(img=input_frame.copy()) if people_results and people_results[0].boxes is not None else input_frame.copy()
            
            # Add person count
            count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
            count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
            cv2.putText(annotated_frame, count_text, (10, 20), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA)
            
            # FIRE/SMOKE overlay
            if fire_status != "no detection":
                fire_color = (0, 0, 255)  # Red for fire/smoke
                cv2.putText(annotated_frame, f"ALERT: {fire_status.upper()}", 
                        (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 
                        0.7, fire_color, 2, cv2.LINE_AA)

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
            logger.error(f"Object detection error: {e}", exc_info=True)
            return frame.copy(), 0, False, 0, 0, "no detection"

    async def _process_stream_with_sharing(self, stream_id: UUID, camera_name: str, source: str, 
                                        owner_username: str, owner_id: UUID, workspace_id: UUID, 
                                        stop_event: threading.Event,
                                        location_info: Optional[Dict[str, Any]] = None):
        """Fixed stream processing with proper frame acquisition and fire notification logic"""
        
        frame_count = 0
        last_db_update_activity = datetime.now(timezone.utc)
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
                    
                    # FIXED: Get frame from shared stream
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

                    # FIXED: Process frame with object detection (now frame is properly defined)
                    processing_start_time = datetime.now(timezone.utc)
                    
                    processed_frame, person_count, alert_triggered, male_count, female_count, fire_status = await loop.run_in_executor(
                        thread_pool, self.detect_objects_with_threshold, frame, conf_threshold, threshold_settings, stream_id_str
                    )
                    detection_duration = (datetime.now(timezone.utc) - processing_start_time).total_seconds()

                    # Update processing stats
                    async with self._lock: 
                        stats = self.stream_processing_stats.get(stream_id_str)
                        if stats:
                            stats["frames_processed"] += 1
                            if person_count > 0: 
                                stats["detection_count"] += 1
                            stats["avg_processing_time"] = (stats.get("avg_processing_time", 0.0) * 0.95) + (detection_duration * 0.05)
                            stats["last_updated"] = datetime.now(timezone.utc)
                        
                        # Update stream info
                        stream_info_active = self.active_streams.get(stream_id_str)
                        if stream_info_active: 
                            stream_info_active['latest_frame'] = processed_frame
                            stream_info_active['last_frame_time'] = datetime.now(timezone.utc)
                    
                    # Send notifications for people threshold alerts
                    if alert_triggered:
                        alert_message = self.generate_alert_message(person_count, threshold_settings)
                        await self.add_notification(
                            str(owner_id), str(workspace_id), stream_id_str, 
                            camera_name, "alert", alert_message
                        )

                    # FIXED: Fire notification with proper cooldown check
                    if fire_status in ["fire", "smoke"]:
                        current_time = time.time()
                        
                        # Get last notification time
                        last_fire_notification_time = self.fire_notification_cooldowns.get(stream_id_str, 0)
                        time_since_last_notification = current_time - last_fire_notification_time
                        cooldown_period = self.fire_cooldown_duration  # 600 seconds
                        
                        # Debug logging
                        logging.debug(f"FIRE CHECK: stream={stream_id_str}, status={fire_status}, "
                                    f"time_since_last={time_since_last_notification:.1f}s, "
                                    f"cooldown_period={cooldown_period}s, "
                                    f"can_notify={time_since_last_notification >= cooldown_period}")
                        
                        # ONLY send notification if cooldown period has passed
                        if time_since_last_notification >= cooldown_period:
                            try:
                                # Create fire alert message
                                alert_message = f"FIRE/SMOKE EMERGENCY: {fire_status.upper()} detected in {camera_name}"
                                
                                logging.warning(f"SENDING FIRE NOTIFICATION: {alert_message} for stream {stream_id_str}")
                                
                                # Send app notification with special fire_alert status
                                await self.add_notification(
                                    str(owner_id), str(workspace_id), stream_id_str, 
                                    camera_name, "fire_alert", alert_message
                                )
                                
                                # Send email notification
                                user_email = await self.get_user_email_for_stream(owner_id)
                                if user_email:
                                    email_sent = await send_fire_alert_email(
                                        user_email, camera_name, fire_status, location_info
                                    )
                                    logging.info(f"Fire alert email result: {email_sent}")
                                
                                # UPDATE COOLDOWN TIMESTAMP - CRITICAL
                                self.fire_notification_cooldowns[stream_id_str] = current_time
                                
                                logging.warning(f"FIRE NOTIFICATION SENT for {stream_id_str}: {fire_status}. "
                                            f"Next notification available in {cooldown_period/60:.1f} minutes")
                                            
                            except Exception as e:
                                logging.error(f"Error sending fire notification for stream {stream_id_str}: {e}", exc_info=True)
                                # Don't update cooldown if notification failed
                        else:
                            # In cooldown period
                            remaining_cooldown = cooldown_period - time_since_last_notification
                            remaining_minutes = remaining_cooldown / 60.0
                            
                            # Only log occasionally to avoid spam
                            if frame_count % 300 == 0:  # Every ~10 seconds
                                logging.info(f"Fire detected in {stream_id_str} but notification in cooldown. "
                                        f"Remaining: {remaining_minutes:.1f} minutes")

                    # Insert detection data if people detected
                    if person_count > 0:
                        await loop.run_in_executor(
                            thread_pool, self.insert_detection_data_with_location, 
                            owner_username, stream_id_str, camera_name, person_count, 
                            male_count, female_count, fire_status, processed_frame, 
                            workspace_id, location_info
                        )

                    # Periodic database activity update
                    now_utc_loop = datetime.now(timezone.utc)
                    if (now_utc_loop - last_db_update_activity).total_seconds() > 10.0:
                        await self.db_manager.execute_query("UPDATE video_stream SET last_activity = NOW() WHERE stream_id = $1", (stream_id,))
                        last_db_update_activity = now_utc_loop
                    
                    # Frame rate control
                    current_iteration_duration = (datetime.now(timezone.utc) - processing_start_time).total_seconds()
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
            self.fire_detection_states.pop(stream_id_str, None)
            self.fire_notification_cooldowns.pop(stream_id_str, None)
            self.fire_detection_frame_counts.pop(stream_id_str, None)
            
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

    async def get_detailed_stream_status(self, stream_id_str: str) -> Dict[str, Any]:
        """Get detailed status information for debugging stream issues"""
        
        def serialize_value(value):
            """Helper function to serialize various Python types to JSON-serializable formats"""
            if value is None:
                return None
            elif isinstance(value, (str, int, float, bool)):
                return value
            elif isinstance(value, datetime):
                return {
                    "type": "datetime",
                    "value": value.isoformat(),
                    "timestamp": value.timestamp()
                }
            elif isinstance(value, UUID):
                return {
                    "type": "UUID", 
                    "value": str(value)
                }
            elif isinstance(value, set):
                return {
                    "type": "set",
                    "count": len(value),
                    "items": [serialize_value(item) for item in list(value)[:5]]  # Show first 5 items
                }
            elif isinstance(value, np.ndarray):
                return {
                    "type": "numpy.ndarray",
                    "shape": list(value.shape),
                    "dtype": str(value.dtype),
                    "size": value.size
                }
            elif isinstance(value, threading.Event):
                return {
                    "type": "threading.Event",
                    "is_set": value.is_set()
                }
            elif isinstance(value, asyncio.Task):
                return {
                    "type": "asyncio.Task",
                    "name": getattr(value, 'get_name', lambda: 'unknown')(),
                    "done": value.done(),
                    "cancelled": value.cancelled(),
                    "exception": str(value.exception()) if value.done() and not value.cancelled() else None
                }
            elif isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                return [serialize_value(item) for item in value]
            else:
                return {
                    "type": str(type(value).__name__),
                    "value": str(value)
                }
        
        # Database status
        try:
            db_status = await self.db_manager.execute_query(
                """SELECT stream_id, name, path, status, is_streaming, last_activity, 
                        updated_at, created_at, location, area, building, zone, 
                        floor_level, latitude, longitude
                FROM video_stream WHERE stream_id = $1""",
                (UUID(stream_id_str),), fetch_one=True
            )
            
            if db_status:
                db_status_serialized = {k: serialize_value(v) for k, v in db_status.items()}
            else:
                db_status_serialized = {"error": "Stream not found in database"}
                
        except Exception as e:
            db_status_serialized = {"error": f"Database query failed: {e}"}
        
        # Memory status
        async with self._lock:
            memory_status = self.active_streams.get(stream_id_str)
            if memory_status:
                memory_status_serialized = serialize_value(memory_status)
            else:
                memory_status_serialized = {"error": "Not found in memory"}
        
        # Processing stats
        processing_stats = self.stream_processing_stats.get(stream_id_str)
        if processing_stats:
            processing_stats_serialized = serialize_value(processing_stats)
        else:
            processing_stats_serialized = {"error": "No processing stats available"}
        
        # Shared stream status (if using sharing)
        shared_stream_status_serialized = None
        if hasattr(self, 'video_file_manager') and db_status and 'path' in db_status:
            try:
                source_path = db_status['path']
                if source_path in self.video_file_manager.shared_streams:
                    shared_stream = self.video_file_manager.shared_streams[source_path]
                    shared_stream_status = shared_stream.get_stats()
                    shared_stream_status_serialized = serialize_value(shared_stream_status)
                else:
                    shared_stream_status_serialized = {"info": "No shared stream found for this source"}
            except Exception as e:
                shared_stream_status_serialized = {"error": f"Failed to get shared stream status: {e}"}
        
        return {
            "stream_id": stream_id_str,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database_status": db_status_serialized,
            "memory_status": memory_status_serialized,
            "processing_stats": processing_stats_serialized,
            "shared_stream_status": shared_stream_status_serialized
        }

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
                        "timestamp": datetime.now(timezone.utc).timestamp()
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


# === Endpoints to match stream_one.py ===

@router.get("/streams") # Path changed
async def get_all_workspace_streams_endpoint( # Renamed
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional, defaults to user's active or all accessible)"), # Match stream_one description
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    try:
        user_id_str = str(current_user_data["user_id"])
        result = await stream_manager.get_workspace_streams(user_id_str, workspace_id)
        return JSONResponse(content={"status": "success", "data": result}) # Match stream_one response structure
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Error getting workspace streams: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

# === ADDING MISSING BULK OPERATION ENDPOINTS ===
@router.post("/start_all_streams", summary="Start all eligible streams in a workspace")
async def start_all_streams_in_workspace(
    workspace_id: UUID = Query(..., description="The ID of the workspace for which to start all streams"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    _user_id_val = current_user_data["user_id"]
    if isinstance(_user_id_val, UUID):
        requester_user_id = _user_id_val
    else:
        try:
            requester_user_id = UUID(str(_user_id_val))
        except ValueError:
            logger.error(f"Invalid user_id format encountered in start_all_streams: {_user_id_val}")
            raise HTTPException(status_code=400, detail="Invalid user ID format in token.")
            
    requester_username = current_user_data["username"]

    try:
        workspace_role_data = await check_workspace_membership_and_get_role(requester_user_id, workspace_id)
        if workspace_role_data.get("role") not in ["owner", "admin", "member"]:
            raise HTTPException(status_code=403, detail="User does not have permission to start all streams in this workspace.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error during permission check for start_all_streams by {requester_username} in ws {workspace_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error verifying workspace permissions.")

    results = []
    streams_started_count = 0
    streams_processed_count = 0

    try:
        streams_to_consider_query = """
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1 AND vs.is_streaming = FALSE;
        """
        streams_to_consider = await db_manager_global.execute_query(streams_to_consider_query, (workspace_id,), fetch_all=True)
        streams_to_consider = streams_to_consider or []
        
        streams_processed_count = len(streams_to_consider)
        if not streams_to_consider:
            return JSONResponse(content={"status": "info", "message": "No streams available to start...", "workspace_id": str(workspace_id), "streams_processed": 0, "streams_started": 0, "details": []})

        owner_active_streams_count_query = """
            SELECT user_id, COUNT(*) as active_count FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE GROUP BY user_id;
        """
        active_counts_db = await db_manager_global.execute_query(owner_active_streams_count_query, (workspace_id,), fetch_all=True)
        active_counts_db = active_counts_db or []
        owner_active_streams_map = {row['user_id']: row['active_count'] for row in active_counts_db}

        for stream_data in streams_to_consider:
            stream_id, stream_name, owner_id = stream_data['stream_id'], stream_data['name'], stream_data['owner_id']
            
            if not stream_data['owner_is_active']:
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream owner is inactive."}); continue
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream owner not subscribed (and not admin)." }); continue

            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get(owner_id, 0)

            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": f"Owner camera limit ({owner_limit}) reached."}); continue

            await db_manager_global.execute_query(
                "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                (datetime.now(timezone.utc), stream_id)
            )
            streams_started_count += 1
            owner_active_streams_map[owner_id] = current_owner_active_count + 1 
            results.append({"stream_id": str(stream_id), "name": stream_name, "status": "initiated", "message": "Stream start initiated."})

        return JSONResponse(content={"status": "success" if streams_started_count == streams_processed_count else "partial_success", "workspace_id": str(workspace_id), "streams_processed": streams_processed_count, "streams_started": streams_started_count, "details": results})
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error in start_all_streams for ws {workspace_id} by {requester_username}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error starting streams.", "workspace_id": str(workspace_id), "details": results})

@router.post("/stop_all_streams", summary="Stop all running streams in a workspace")
async def stop_all_streams_in_workspace(
    workspace_id: UUID = Query(..., description="The ID of the workspace for which to stop all streams"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    _user_id_val = current_user_data["user_id"]
    if isinstance(_user_id_val, UUID):
        requester_user_id = _user_id_val
    else:
        try:
            requester_user_id = UUID(str(_user_id_val))
        except ValueError:
            logger.error(f"Invalid user_id format encountered in stop_all_streams: {_user_id_val}")
            raise HTTPException(status_code=400, detail="Invalid user ID format in token.")

    requester_username = current_user_data["username"]

    try:
        workspace_role_data = await check_workspace_membership_and_get_role(requester_user_id, workspace_id)
        if workspace_role_data.get("role") not in ["owner", "admin", "member"]:
            raise HTTPException(status_code=403, detail="User does not have permission to stop all streams in this workspace.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error during permission check for stop_all_streams by {requester_username} in ws {workspace_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error verifying workspace permissions.")

    results = []
    streams_stopped_count = 0
    streams_processed_count = 0
    
    try:
        streams_to_stop_query = """
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id
            FROM video_stream vs WHERE vs.workspace_id = $1 AND vs.is_streaming = TRUE;
        """
        streams_to_stop = await db_manager_global.execute_query(streams_to_stop_query, (workspace_id,), fetch_all=True)
        streams_to_stop = streams_to_stop or []

        streams_processed_count = len(streams_to_stop)
        if not streams_to_stop:
            return JSONResponse(content={"status": "info", "message": "No streams running to stop.", "workspace_id": str(workspace_id), "streams_processed": 0, "streams_stopped": 0, "details": []})

        for stream_data in streams_to_stop:
            stream_id, stream_name, owner_id = stream_data['stream_id'], stream_data['name'], stream_data['owner_id']
            updated_rows = await db_manager_global.execute_query(
                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
                (datetime.now(timezone.utc), stream_id), return_rowcount=True
            )
            if updated_rows and updated_rows > 0:
                streams_stopped_count += 1
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "stopped", "message": "Stream stop request processed."})
                await stream_manager.add_notification(str(owner_id), str(workspace_id), str(stream_id), stream_name, "inactive", f"Camera '{stream_name}' stopped by {requester_username} (bulk action).")
            else:
                 results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream already stopped or issue."})

        return JSONResponse(content={"status": "success" if streams_stopped_count == streams_processed_count else "partial_success", "workspace_id": str(workspace_id), "streams_processed": streams_processed_count, "streams_stopped": streams_stopped_count, "details": results})
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error in stop_all_streams for ws {workspace_id} by {requester_username}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error stopping streams.", "workspace_id": str(workspace_id), "details": results})

@router.post("/start_all_streams_v2")
async def start_all_streams_v2_endpoint( # Name to match stream_one
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        update_query = """
            UPDATE video_stream vs SET is_streaming = TRUE, status = 'processing', updated_at = $1
            FROM workspace_members wm
            WHERE vs.workspace_id = wm.workspace_id AND wm.user_id = $2 AND vs.is_streaming = FALSE
            RETURNING vs.stream_id;
        """ # RETURNING to get count
        now_utc = datetime.now(timezone.utc)
        updated_streams = await db_manager_global.execute_query(update_query, (now_utc, UUID(requester_user_id_str)), fetch_all=True)
        updated_rows = len(updated_streams) if updated_streams else 0

        if updated_rows == 0:
            return JSONResponse(content={"status": "info", "message": "No inactive streams to start in your accessible workspaces."})
        logging.info(f"User {requester_username} requested to start all v2 streams. {updated_rows} streams marked for starting.")
        return JSONResponse(content={"status": "success", "message": f"{updated_rows} streams marked to start."})
    except Exception as e:
        logging.error(f"Error in /start_all_streams_v2: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.post("/stop_all_streams_v2")
async def stop_all_streams_v2_endpoint( # Name to match stream_one
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id
            FROM video_stream vs JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE
        """
        streams_to_stop = await db_manager_global.execute_query(query, (UUID(requester_user_id_str),), fetch_all=True)
        streams_to_stop = streams_to_stop or []

        if not streams_to_stop:
            return JSONResponse(content={"status": "info", "message": "No active streams to stop in your accessible workspaces."})

        stream_ids = [s["stream_id"] for s in streams_to_stop] # These are already UUIDs from DB
        placeholders = ','.join([f'${i+2}' for i in range(len(stream_ids))]) # $1 for time, $2... for stream_ids
        
        update_query = f"""
            UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1
            WHERE stream_id IN ({placeholders})
        """
        now_utc = datetime.now(timezone.utc)
        await db_manager_global.execute_query(update_query, (now_utc, *stream_ids))

        for stream in streams_to_stop:
            await stream_manager.add_notification(str(stream["user_id"]), str(stream["workspace_id"]), str(stream["stream_id"]), stream["name"], "inactive", f"Camera '{stream['name']}' stopped by {requester_username} (v2 bulk action).")
        logging.info(f"User {requester_username} stopped {len(streams_to_stop)} streams (v2).")
        return JSONResponse(content={"status": "success", "message": f"{len(streams_to_stop)} streams stopped."})
    except Exception as e:
        logging.error(f"Error in /stop_all_streams_v2: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

# === NEW: LOCATION-BASED FILTERING ENDPOINT ===
@router.get("/streams/by-location")
async def get_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Get streams filtered by location criteria."""
    try:
        user_id_str = str(current_user_data["user_id"])
        
        # Build query with location filters
        base_query = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                   vs.latitude, vs.longitude, u.username as owner_username,
                   w.name as workspace_name, vs.workspace_id
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
            
        base_query += " ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name"
        
        streams = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        streams = streams or []
        
        formatted_streams = [{
            "stream_id": str(s["stream_id"]),
            "name": s["name"],
            "status": s["status"],
            "is_streaming": s["is_streaming"],
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "floor_level": s["floor_level"],
            "latitude": float(s["latitude"]) if s["latitude"] else None,
            "longitude": float(s["longitude"]) if s["longitude"] else None,
            "owner_username": s["owner_username"],
            "workspace_name": s["workspace_name"],
            "workspace_id": str(s["workspace_id"])
        } for s in streams]
        
        return JSONResponse(content={
            "status": "success",
            "streams": formatted_streams,
            "total": len(formatted_streams),
            "filters_applied": {
                "location": location,
                "area": area, 
                "building": building,
                "floor_level": floor_level,
                "zone": zone
            }
        })
        
    except Exception as e:
        logging.error(f"Error getting streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.websocket("/stream1") # Path changed
async def websocket_workspace_stream(websocket: WebSocket): # Name kept from async_stream
    stream_id_str: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    user_id_for_log: Optional[str] = None

    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        stream_id_str = query_params.get("stream_id")
        if not stream_id_str:
            await websocket.send_json({"status": "error", "message": "stream_id is required."}); await websocket.close(1008); return # Payload match

        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return # Payload match

        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return # Payload match
        
        requester_user_id_str = token_data.user_id
        user_id_for_log = requester_user_id_str 
        stream_id_uuid = UUID(stream_id_str)

        # UPDATED: Include location data in connection confirmation
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
            await websocket.send_json({"status": "error", "message": "Stream not found."}); await websocket.close(1008); return

        s_workspace_id = stream_details_db['workspace_id']
        s_name = stream_details_db['name']
        s_is_streaming_db = stream_details_db['is_streaming']
        s_status_db = stream_details_db['status']
        s_owner_username = stream_details_db['owner_username']
        
        # NEW: Extract location info for response
        location_data = {
            "location": stream_details_db.get('location'),
            "area": stream_details_db.get('area'),
            "building": stream_details_db.get('building'),
            "zone": stream_details_db.get('zone'),
            "floor_level": stream_details_db.get('floor_level')
        }

        requester_role_info = await check_workspace_membership_and_get_role(UUID(requester_user_id_str), s_workspace_id) # throws HTTPException on fail
        
        stream_is_active_in_manager = False
        async with stream_manager._lock:
            current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
            if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                stream_is_active_in_manager = True
        
        if not s_is_streaming_db or not stream_is_active_in_manager:
            logging.info(f"WS: Stream {stream_id_str} not active (DB: {s_is_streaming_db}, Mgr: {stream_is_active_in_manager}, Status: {s_status_db}). Attempting start by {requester_user_id_str}.")
            # Role check from stream_one.py before attempting start
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']: # owner can also start
                 await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."}); await websocket.close(1008); return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str) # This marks for processing
                # Wait for stream_manager to pick it up
                for _ in range(config.get("stream_ws_start_wait_attempts", 10)): 
                    async with stream_manager._lock:
                         current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True; break
                    await asyncio.sleep(1.0)
                if not stream_is_active_in_manager:
                    db_state_after_start = await db_manager_global.execute_query("SELECT status, is_streaming from video_stream where stream_id = $1", (stream_id_uuid,), fetch_one=True)
                    logger.warning(f"Stream {stream_id_str} failed to become active in StreamManager for WS. DB state: {db_state_after_start}")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize. Check server logs."}); await websocket.close(1011); return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"}); await websocket.close(1011); return

        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."}); await websocket.close(1011); return

        # UPDATED: Include location data in connection response
        await websocket.send_json({
            "status": "connected", 
            "message": f"Connected to stream: {s_name}",
            "stream_id": stream_id_str, 
            "owner": s_owner_username, 
            "workspace_id": str(s_workspace_id), 
            "your_role": requester_role_info.get("role"),
            # NEW: Location information
            "location_info": location_data
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        target_fps = config.get("websocket_client_fps", 15.0) # Match stream_one.py (15fps)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066 # (approx 15fps)

        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            async with stream_manager._lock: 
                stream_info = stream_manager.active_streams.get(stream_id_str, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        # Consider if frame_to_base64 should be in executor if it's slow
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(thread_pool, frame_to_base64, latest_frame_np)
            
            if not stream_ok:
                await websocket.send_json({"status":"info", "message":"Stream ended or became inactive."}); break
            
            if latest_frame_b64:
                await websocket.send_json({ # Match stream_one.py payload
                    "stream_id": stream_id_str, "frame": latest_frame_b64,
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
            await asyncio.sleep(target_frame_interval)
    except WebSocketDisconnect:
        logging.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled (User: {user_id_for_log or 'unknown'}).")
    except ValueError as ve: 
        logging.warning(f"WS stream error (User: {user_id_for_log or 'unknown'}, Stream: {stream_id_str or 'unknown'}): Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Invalid stream ID format."}); await websocket.close(1008)
            except: pass # Ignore error on close if already closing
    except Exception as e:
        logging.error(f"WS stream error ({stream_id_str or 'unknown'}, User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}); await websocket.close(1011)
            except: pass
    finally:
        if ping_task and not ping_task.done(): ping_task.cancel()
        if stream_id_str: await stream_manager.disconnect_client(stream_id_str, websocket)
        try:
            # Ensure websocket is closed if not already
            if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
            # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
            # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
            # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        except RuntimeError:
            pass  # Ignore double-close errors

@router.websocket("/notify2")
async def websocket_notify(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None

    try:
        await websocket.accept()
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token: 
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return

        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token): # Payload match
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager_global.get_user_by_id(UUID(user_id_str)) # Fetch user for logging
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

        logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({ # Payload match
            "status": "connected", 
            "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get initial notifications
        initial_notifications = await stream_manager.get_notifications(user_id_str, workspace_id_filter=None, include_read=False, limit=20)
        
        # Format notifications
        formatted_notifications = []
        for notification in initial_notifications:
            formatted_notifications.append({
                "id": notification.get("notification_id"),
                "user_id": notification.get("user_id"),
                "workspace_id": notification.get("workspace_id"),
                "stream_id": notification.get("stream_id"),
                "camera_name": notification.get("camera_name"),
                "status": notification.get("status"),
                "message": notification.get("message"),
                "timestamp": notification.get("created_at").timestamp() if notification.get("created_at") else datetime.now(timezone.utc).timestamp(),
                "read": notification.get("is_read", False)
            })
        
        if formatted_notifications: # Payload match
            await websocket.send_json(formatted_notifications)  # Send as array directly
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            # Don't close here - let it continue
        
        ping_task = asyncio.create_task(send_ping(websocket))
        logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Timeout from config, matching stream_one.py key
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue 
                elif message.get("type") == "mark_read":
                    notif_ids_raw = message.get("notification_ids") # stream_one uses "notification_ids"
                    if isinstance(notif_ids_raw, list) and user_id_str:
                        updated_count = 0
                        valid_notif_ids_to_mark: List[UUID] = []
                        for nid_str_raw in notif_ids_raw:
                            try: valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                            except ValueError: logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
                        if valid_notif_ids_to_mark:
                            # Batched update would be more efficient for many IDs
                            for nid_uuid in valid_notif_ids_to_mark:
                                try:
                                    res = await db_manager_global.execute_query(
                                        "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                                        (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)), return_rowcount=True
                                    )
                                    if res and res > 0: updated_count +=1
                                except Exception as e_mark: logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        # Ack structure from stream_one.py
                        await websocket.send_json({"type": "ack_mark_read", "ids": notif_ids_raw, "updated_count": updated_count}) 

            except asyncio.TimeoutError: continue # No message from client, normal
            except WebSocketDisconnect: logging.info(f"Notify WS: Client {username_for_log} disconnected normally"); break # Client disconnected
            except asyncio.CancelledError: logging.info(f"Notify WS: Task cancelled for {username_for_log}"); raise # Propagate cancellation
            except Exception as e_recv:
                logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True); break
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
    except asyncio.CancelledError:
        logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}) # Payload match
            except: pass # Ignore error on close
    finally:
        # Cleanup
        logging.debug(f"Notify WS: Starting cleanup for {username_for_log or 'unknown'}")

        # Cancel ping task
        if ping_task and not ping_task.done(): 
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass

        # Unsubscribe from notifications
        if user_id_str:
            try:
                await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
            except Exception as e_unsub:
                logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        # # Close websocket only if it's still connected
        # try:
        #     # Ensure websocket is closed if not already
        #     # if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        #     # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        #     # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        #     if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        # except Exception as e_close:
        #     logging.debug(f"Error closing websocket for {username_for_log}: {e_close}")
        
        await _safe_close_websocket(websocket, username_for_log)

        logging.debug(f"Notify WS: Connection cleanup for {username_for_log or 'unknown'}.")

@router.websocket("/notify1")
async def websocket_notify1(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None

    try:
        await websocket.accept()
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token: # Payload match stream_one.py
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return

        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token): # Payload match
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager_global.get_user_by_id(UUID(user_id_str)) # Fetch user for logging
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"
            
        await websocket.send_json({ # Payload match
            "status": "connected", "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get workspace_id from token if available, else None for all user's notifications.
        # stream_one.py's initial_notifications: workspace_id_filter=None, include_read=False, limit=20
        initial_notifications = await stream_manager.get_notifications(user_id_str, workspace_id_filter=None, include_read=False, limit=20)
        if initial_notifications: # Payload match
            await websocket.send_json({"type": "notifications_batch", "notifications": initial_notifications, "server_time": datetime.now(timezone.utc).timestamp()})
        
        if not await stream_manager.subscribe_to_notifications(user_id_str, websocket):
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            # Optionally close if subscription is critical, stream_one.py doesn't explicitly
        
        ping_task = asyncio.create_task(send_ping(websocket))

        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Timeout from config, matching stream_one.py key
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue 
                elif message.get("type") == "mark_read":
                    notif_ids_raw = message.get("notification_ids") # stream_one uses "notification_ids"
                    if isinstance(notif_ids_raw, list) and user_id_str:
                        updated_count = 0
                        valid_notif_ids_to_mark: List[UUID] = []
                        for nid_str_raw in notif_ids_raw:
                            try: valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                            except ValueError: logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
                        if valid_notif_ids_to_mark:
                            # Batched update would be more efficient for many IDs
                            for nid_uuid in valid_notif_ids_to_mark:
                                try:
                                    res = await db_manager_global.execute_query(
                                        "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                                        (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)), return_rowcount=True
                                    )
                                    if res and res > 0: updated_count +=1
                                except Exception as e_mark: logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        # Ack structure from stream_one.py
                        await websocket.send_json({"type": "ack_mark_read", "ids": notif_ids_raw}) # Send back original list of IDs processed

            except asyncio.TimeoutError: continue # No message from client, normal
            except WebSocketDisconnect: break # Client disconnected
            except asyncio.CancelledError: raise # Propagate cancellation
            except Exception as e_recv:
                logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True); break
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
    except asyncio.CancelledError:
        logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}) # Payload match
            except: pass # Ignore error on close
    finally:
        if user_id_str: await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
        if ping_task and not ping_task.done(): ping_task.cancel()
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        logging.debug(f"Notify WS: Connection cleanup for {username_for_log or 'unknown'}.")

@router.post("/streams/{stream_id}/thresholds")
async def update_stream_thresholds(
    stream_id: str,
    settings: ThresholdSettings,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Update count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Verify stream exists and user has access
        stream_info = await db_manager_global.execute_query(
            "SELECT workspace_id, name FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_membership_and_get_role(UUID(requester_user_id_str), stream_info['workspace_id'])
        
        # Validate thresholds
        if (settings.count_threshold_greater is not None and 
            settings.count_threshold_less is not None and 
            settings.count_threshold_greater <= settings.count_threshold_less):
            raise HTTPException(status_code=400, detail="Greater threshold must be higher than less threshold")
        
        # Update thresholds
        await db_manager_global.execute_query(
            """UPDATE video_stream 
               SET count_threshold_greater = $1, count_threshold_less = $2, 
                   alert_enabled = $3, updated_at = NOW() 
               WHERE stream_id = $4""",
            (settings.count_threshold_greater, settings.count_threshold_less, 
             settings.alert_enabled, stream_id_uuid)
        )
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Thresholds updated for stream '{stream_info['name']}'",
            "settings": {
                "count_threshold_greater": settings.count_threshold_greater,
                "count_threshold_less": settings.count_threshold_less,
                "alert_enabled": settings.alert_enabled
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logging.error(f"Error updating stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/streams/{stream_id}/thresholds")
async def get_stream_thresholds(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Get count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Get stream info with thresholds
        stream_info = await db_manager_global.execute_query(
            """SELECT vs.workspace_id, vs.name, vs.count_threshold_greater, 
                      vs.count_threshold_less, vs.alert_enabled
               FROM video_stream vs WHERE vs.stream_id = $1""",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_membership_and_get_role(UUID(requester_user_id_str), stream_info['workspace_id'])
        
        return JSONResponse(content={
            "status": "success",
            "stream_id": stream_id,
            "name": stream_info['name'],
            "thresholds": {
                "count_threshold_greater": stream_info.get('count_threshold_greater'),
                "count_threshold_less": stream_info.get('count_threshold_less'),
                "alert_enabled": stream_info.get('alert_enabled', False)
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logging.error(f"Error getting stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/workspaces/{workspace_id}/alert-streams")
async def get_workspace_alert_streams(
    workspace_id: str,
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Get all streams with alerts enabled in a workspace."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        workspace_id_uuid = UUID(workspace_id)
        
        # Check workspace membership
        await check_workspace_membership_and_get_role(UUID(requester_user_id_str), workspace_id_uuid)
        
        # Get streams with alerts enabled
        alert_streams = await db_manager_global.execute_query(
            """SELECT stream_id, name, location, area, building, zone,
                      count_threshold_greater, count_threshold_less, 
                      alert_enabled, status, is_streaming
               FROM video_stream 
               WHERE workspace_id = $1 AND alert_enabled = TRUE
               ORDER BY building, zone, area, location, name""",
            (workspace_id_uuid,), fetch_all=True
        )
        
        formatted_streams = [{
            "stream_id": str(s["stream_id"]),
            "name": s["name"],
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "status": s["status"],
            "is_streaming": s["is_streaming"],
            "thresholds": {
                "count_threshold_greater": s.get('count_threshold_greater'),
                "count_threshold_less": s.get('count_threshold_less'),
                "alert_enabled": s.get('alert_enabled', False)
            }
        } for s in alert_streams or []]
        
        return JSONResponse(content={
            "status": "success",
            "workspace_id": workspace_id,
            "alert_streams": formatted_streams,
            "total": len(formatted_streams)
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workspace ID format")
    except Exception as e:
        logging.error(f"Error getting workspace alert streams: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

# === LOCATION-BASED STREAM MANAGEMENT ENDPOINTS ===

@router.post("/streams/start-by-location")
async def start_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Start all streams matching the specified location criteria."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        # Build query to find matching streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id, vs.location, vs.area, 
                   vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = FALSE
        """
        
        params = [UUID(requester_user_id_str)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.workspace_id, vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        streams_to_consider = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        streams_to_consider = streams_to_consider or []
        
        if not streams_to_consider:
            return JSONResponse(content={
                "status": "info",
                "message": "No inactive streams found matching the specified location criteria",
                "filters_applied": {
                    "location": location,
                    "area": area,
                    "building": building,
                    "zone": zone,
                    "floor_level": floor_level,
                    "workspace_id": workspace_id
                },
                "streams_processed": 0,
                "streams_started": 0,
                "details": []
            })
        
        # Get current active stream counts per owner per workspace
        workspace_ids = list(set(str(s['workspace_id']) for s in streams_to_consider))
        workspace_placeholders = ','.join([f'${i+1}' for i in range(len(workspace_ids))])
        
        owner_active_streams_query = f"""
            SELECT user_id, workspace_id, COUNT(*) as active_count 
            FROM video_stream
            WHERE workspace_id IN ({workspace_placeholders}) AND is_streaming = TRUE 
            GROUP BY user_id, workspace_id
        """
        active_counts_db = await db_manager_global.execute_query(
            owner_active_streams_query, 
            tuple(UUID(wid) for wid in workspace_ids), 
            fetch_all=True
        )
        active_counts_db = active_counts_db or []
        
        # Create lookup map: (user_id, workspace_id) -> active_count
        owner_active_streams_map = {
            (row['user_id'], row['workspace_id']): row['active_count'] 
            for row in active_counts_db
        }
        
        results = []
        streams_started_count = 0
        streams_processed_count = len(streams_to_consider)
        
        for stream_data in streams_to_consider:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            owner_id = stream_data['user_id']
            ws_id = stream_data['workspace_id']
            
            # Check owner eligibility
            if not stream_data['owner_is_active']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream owner is inactive"
                })
                continue
                
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream owner not subscribed (and not admin)"
                })
                continue
            
            # Check camera limits
            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get((owner_id, ws_id), 0)
            
            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": f"Owner camera limit ({owner_limit}) reached"
                })
                continue
            
            # Start the stream
            await db_manager_global.execute_query(
                "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                (datetime.now(timezone.utc), stream_id)
            )
            
            streams_started_count += 1
            owner_active_streams_map[(owner_id, ws_id)] = current_owner_active_count + 1
            
            results.append({
                "stream_id": str(stream_id),
                "name": stream_name,
                "workspace": stream_data['workspace_name'],
                "location_info": {
                    "location": stream_data['location'],
                    "area": stream_data['area'],
                    "building": stream_data['building'],
                    "zone": stream_data['zone'],
                    "floor_level": stream_data['floor_level']
                },
                "status": "initiated",
                "message": "Stream start initiated"
            })
        
        status = "success" if streams_started_count == streams_processed_count else "partial_success"
        if streams_started_count == 0:
            status = "info"
            
        return JSONResponse(content={
            "status": status,
            "message": f"Started {streams_started_count} of {streams_processed_count} streams matching location criteria",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams_processed": streams_processed_count,
            "streams_started": streams_started_count,
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error starting streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })

@router.post("/streams/stop-by-location")
async def stop_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Stop all streams matching the specified location criteria."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        # Build query to find matching active streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id, vs.location, vs.area,
                   vs.building, vs.zone, vs.floor_level, w.name as workspace_name
            FROM video_stream vs
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE
        """
        
        params = [UUID(requester_user_id_str)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.workspace_id, vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        streams_to_stop = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        streams_to_stop = streams_to_stop or []
        
        if not streams_to_stop:
            return JSONResponse(content={
                "status": "info",
                "message": "No active streams found matching the specified location criteria",
                "filters_applied": {
                    "location": location,
                    "area": area,
                    "building": building,
                    "zone": zone,
                    "floor_level": floor_level,
                    "workspace_id": workspace_id
                },
                "streams_processed": 0,
                "streams_stopped": 0,
                "details": []
            })
        
        results = []
        streams_stopped_count = 0
        streams_processed_count = len(streams_to_stop)
        
        for stream_data in streams_to_stop:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            owner_id = stream_data['user_id']
            ws_id = stream_data['workspace_id']
            
            updated_rows = await db_manager_global.execute_query(
                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
                (datetime.now(timezone.utc), stream_id), return_rowcount=True
            )
            
            if updated_rows and updated_rows > 0:
                streams_stopped_count += 1
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "stopped",
                    "message": "Stream stop request processed"
                })
                
                # Send notification
                await stream_manager.add_notification(
                    str(owner_id), str(ws_id), str(stream_id), stream_name, 
                    "inactive", f"Camera '{stream_name}' stopped by {requester_username} (location-based action)"
                )
            else:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream already stopped or issue occurred"
                })
        
        status = "success" if streams_stopped_count == streams_processed_count else "partial_success"
        if streams_stopped_count == 0:
            status = "info"
            
        return JSONResponse(content={
            "status": status,
            "message": f"Stopped {streams_stopped_count} of {streams_processed_count} streams matching location criteria",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams_processed": streams_processed_count,
            "streams_stopped": streams_stopped_count,
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error stopping streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })

# === WEBSOCKET ENDPOINTS FOR LOCATION-BASED STREAMING ===

@router.websocket("/stream/by-location")
async def websocket_location_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming multiple cameras by location criteria."""
    user_id_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    connected_streams: Dict[str, bool] = {}
    
    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        
        # Extract location filters from query parameters
        location = query_params.get("location")
        area = query_params.get("area")
        building = query_params.get("building")
        zone = query_params.get("zone")
        floor_level = query_params.get("floor_level")
        workspace_id = query_params.get("workspace_id")
        
        # Validate that at least one filter is provided
        if not any([location, area, building, zone, floor_level]):
            await websocket.send_json({
                "status": "error", 
                "message": "At least one location filter (location, area, building, zone, floor_level) is required"
            })
            await websocket.close(1008)
            return
        
        # Authenticate user
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required"})
            await websocket.close(1008)
            return
            
        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Find matching streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.is_streaming, vs.status,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_for_log)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        matching_streams = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        matching_streams = matching_streams or []
        
        if not matching_streams:
            await websocket.send_json({
                "status": "error",
                "message": "No streams found matching the specified location criteria"
            })
            await websocket.close(1008)
            return
        
        # Connect to each matching stream
        active_streams = []
        for stream_data in matching_streams:
            stream_id_str = str(stream_data['stream_id'])
            
            # Try to connect to active streams or start inactive ones
            if stream_data['is_streaming'] and stream_data['status'] == 'active':
                if await stream_manager.connect_client_to_stream(stream_id_str, websocket):
                    connected_streams[stream_id_str] = True
                    active_streams.append({
                        "stream_id": stream_id_str,
                        "name": stream_data['name'],
                        "status": "connected",
                        "location_info": {
                            "location": stream_data['location'],
                            "area": stream_data['area'],
                            "building": stream_data['building'],
                            "zone": stream_data['zone'],
                            "floor_level": stream_data['floor_level']
                        }
                    })
                else:
                    active_streams.append({
                        "stream_id": stream_id_str,
                        "name": stream_data['name'],
                        "status": "connection_failed",
                        "location_info": {
                            "location": stream_data['location'],
                            "area": stream_data['area'],
                            "building": stream_data['building'],
                            "zone": stream_data['zone'],
                            "floor_level": stream_data['floor_level']
                        }
                    })
            else:
                active_streams.append({
                    "stream_id": stream_id_str,
                    "name": stream_data['name'],
                    "status": "inactive",
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    }
                })
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": f"Connected to location-based stream",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams": active_streams,
            "total_streams": len(matching_streams),
            "connected_streams": len([s for s in active_streams if s["status"] == "connected"])
        })
        
        if not connected_streams:
            await websocket.send_json({
                "status": "info",
                "message": "No active streams to display. All streams in the specified location are inactive."
            })
            await websocket.close(1000)
            return
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Stream frames from all connected streams
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066
        
        while websocket.client_state == WebSocketState.CONNECTED and connected_streams:
            frames_data = []
            streams_still_active = {}
            
            # Collect frames from all connected streams
            async with stream_manager._lock:
                for stream_id_str in list(connected_streams.keys()):
                    stream_info = stream_manager.active_streams.get(stream_id_str, {})
                    
                    if stream_info and stream_info.get('status') == 'active':
                        streams_still_active[stream_id_str] = True
                        latest_frame_np = stream_info.get('latest_frame')
                        
                        if latest_frame_np is not None:
                            try:
                                frame_b64 = await asyncio.get_event_loop().run_in_executor(
                                    thread_pool, frame_to_base64, latest_frame_np
                                )
                                
                                # Find stream details for this frame
                                stream_details = next(
                                    (s for s in matching_streams if str(s['stream_id']) == stream_id_str), 
                                    None
                                )
                                
                                frames_data.append({
                                    "stream_id": stream_id_str,
                                    "name": stream_details['name'] if stream_details else "Unknown",
                                    "frame": frame_b64,
                                    "location_info": {
                                        "location": stream_details['location'] if stream_details else None,
                                        "area": stream_details['area'] if stream_details else None,
                                        "building": stream_details['building'] if stream_details else None,
                                        "zone": stream_details['zone'] if stream_details else None,
                                        "floor_level": stream_details['floor_level'] if stream_details else None
                                    } if stream_details else {}
                                })
                            except Exception as e:
                                logging.error(f"Error processing frame for stream {stream_id_str}: {e}")
                    else:
                        # Stream is no longer active, remove from connected streams
                        await stream_manager.disconnect_client(stream_id_str, websocket)
            
            # Update connected streams to only include active ones
            connected_streams = streams_still_active
            
            if not connected_streams:
                await websocket.send_json({
                    "status": "info",
                    "message": "All streams have ended or become inactive"
                })
                break
            
            # Send frames if any are available
            if frames_data:
                await websocket.send_json({
                    "type": "multi_stream_frames",
                    "frames": frames_data,
                    "timestamp": datetime.now(timezone.utc).timestamp(),
                    "active_streams": len(connected_streams)
                })
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"Location-based WS client disconnected (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"Location-based WS task cancelled (User: {user_id_for_log or 'unknown'})")
    except ValueError as ve:
        logging.warning(f"Location-based WS error: Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid ID format"})
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"Location-based WS error (User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error"})
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        
        # Disconnect from all connected streams
        for stream_id_str in connected_streams:
            await stream_manager.disconnect_client(stream_id_str, websocket)
        
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()

@router.websocket("/stream/single-by-location")
async def websocket_single_location_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming from the first available camera matching location criteria."""
    user_id_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    current_stream_id: Optional[str] = None
    
    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        
        # Extract location filters
        location = query_params.get("location")
        area = query_params.get("area") 
        building = query_params.get("building")
        zone = query_params.get("zone")
        floor_level = query_params.get("floor_level")
        workspace_id = query_params.get("workspace_id")
        
        # Validate that at least one filter is provided
        if not any([location, area, building, zone, floor_level]):
            await websocket.send_json({
                "status": "error",
                "message": "At least one location filter is required"
            })
            await websocket.close(1008)
            return
        
        # Authenticate user
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required"})
            await websocket.close(1008)
            return
            
        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Find the first matching active stream
        base_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.is_streaming, vs.status,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE AND vs.status = 'active'
        """
        
        params = [UUID(user_id_for_log)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.building, vs.zone, vs.area, vs.location, vs.name LIMIT 1"
        
        matching_stream = await db_manager_global.execute_query(base_query, tuple(params), fetch_one=True)
        
        if not matching_stream:
            await websocket.send_json({
                "status": "error",
                "message": "No active streams found matching the specified location criteria"
            })
            await websocket.close(1008)
            return
        
        current_stream_id = str(matching_stream['stream_id'])
        
        # Check workspace membership
        await check_workspace_membership_and_get_role(UUID(user_id_for_log), matching_stream['workspace_id'])
        
        # Connect to the stream
        if not await stream_manager.connect_client_to_stream(current_stream_id, websocket):
            await websocket.send_json({
                "status": "error",
                "message": "Failed to connect to the stream"
            })
            await websocket.close(1011)
            return
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": f"Connected to stream: {matching_stream['name']}",
            "stream_id": current_stream_id,
            "stream_name": matching_stream['name'],
            "owner": matching_stream['owner_username'],
            "workspace_id": str(matching_stream['workspace_id']),
            "location_info": {
                "location": matching_stream['location'],
                "area": matching_stream['area'],
                "building": matching_stream['building'],
                "zone": matching_stream['zone'],
                "floor_level": matching_stream['floor_level']
            },
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Stream frames
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066
        
        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            
            async with stream_manager._lock:
                stream_info = stream_manager.active_streams.get(current_stream_id, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(
                            thread_pool, frame_to_base64, latest_frame_np
                        )
            
            if not stream_ok:
                await websocket.send_json({
                    "status": "info",
                    "message": "Stream ended or became inactive"
                })
                break
            
            if latest_frame_b64:
                await websocket.send_json({
                    "stream_id": current_stream_id,
                    "frame": latest_frame_b64,
                    "timestamp": datetime.now(timezone.utc).timestamp(),
                    "location_info": {
                        "location": matching_stream['location'],
                        "area": matching_stream['area'],
                        "building": matching_stream['building'],
                        "zone": matching_stream['zone'],
                        "floor_level": matching_stream['floor_level']
                    }
                })
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"Single location WS client disconnected (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"Single location WS task cancelled (User: {user_id_for_log or 'unknown'})")
    except ValueError as ve:
        logging.warning(f"Single location WS error: Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid ID format"})
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"Single location WS error (User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error"})
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        
        if current_stream_id:
            await stream_manager.disconnect_client(current_stream_id, websocket)
        
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()

# === ADDITIONAL UTILITY ENDPOINTS FOR LOCATION-BASED OPERATIONS ===

@router.get("/streams/locations/hierarchy")
async def get_location_hierarchy_for_streams(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Get the location hierarchy for streams with stream counts."""
    try:
        user_id_str = str(current_user_data["user_id"])
        
        base_query = """
            SELECT DISTINCT 
                vs.building, vs.floor_level, vs.zone, vs.area, vs.location,
                COUNT(*) as stream_count,
                COUNT(CASE WHEN vs.is_streaming = TRUE THEN 1 END) as active_streams,
                COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as healthy_streams
            FROM video_stream vs
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        
        base_query += """
            GROUP BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location
            ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location
        """
        
        hierarchy_data = await db_manager_global.execute_query(base_query, tuple(params), fetch_all=True)
        hierarchy_data = hierarchy_data or []
        
        # Organize into hierarchy structure
        buildings = {}
        
        for row in hierarchy_data:
            building = row['building'] or 'Unknown Building'
            floor_level = row['floor_level'] or 'Unknown Floor'
            zone = row['zone'] or 'Unknown Zone'
            area = row['area'] or 'Unknown Area'
            location = row['location'] or 'Unknown Location'
            
            if building not in buildings:
                buildings[building] = {
                    'name': building,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'floors': {}
                }
            
            if floor_level not in buildings[building]['floors']:
                buildings[building]['floors'][floor_level] = {
                    'name': floor_level,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'zones': {}
                }
            
            if zone not in buildings[building]['floors'][floor_level]['zones']:
                buildings[building]['floors'][floor_level]['zones'][zone] = {
                    'name': zone,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'areas': {}
                }
            
            if area not in buildings[building]['floors'][floor_level]['zones'][zone]['areas']:
                buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area] = {
                    'name': area,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'locations': {}
                }
            
            # Add location data
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['locations'][location] = {
                'name': location,
                'total_streams': row['stream_count'],
                'active_streams': row['active_streams'],
                'healthy_streams': row['healthy_streams']
            }
            
            # Aggregate counts upward
            buildings[building]['total_streams'] += row['stream_count']
            buildings[building]['active_streams'] += row['active_streams']
            buildings[building]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['zones'][zone]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['zones'][zone]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['zones'][zone]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['healthy_streams'] += row['healthy_streams']
        
        # Convert to list format
        hierarchy_list = []
        for building_name, building_data in buildings.items():
            building_item = {
                'type': 'building',
                'name': building_name,
                'total_streams': building_data['total_streams'],
                'active_streams': building_data['active_streams'],
                'healthy_streams': building_data['healthy_streams'],
                'children': []
            }
            
            for floor_name, floor_data in building_data['floors'].items():
                floor_item = {
                    'type': 'floor_level',
                    'name': floor_name,
                    'total_streams': floor_data['total_streams'],
                    'active_streams': floor_data['active_streams'],
                    'healthy_streams': floor_data['healthy_streams'],
                    'children': []
                }
                
                for zone_name, zone_data in floor_data['zones'].items():
                    zone_item = {
                        'type': 'zone',
                        'name': zone_name,
                        'total_streams': zone_data['total_streams'],
                        'active_streams': zone_data['active_streams'],
                        'healthy_streams': zone_data['healthy_streams'],
                        'children': []
                    }
                    
                    for area_name, area_data in zone_data['areas'].items():
                        area_item = {
                            'type': 'area',
                            'name': area_name,
                            'total_streams': area_data['total_streams'],
                            'active_streams': area_data['active_streams'],
                            'healthy_streams': area_data['healthy_streams'],
                            'children': []
                        }
                        
                        for location_name, location_data in area_data['locations'].items():
                            location_item = {
                                'type': 'location',
                                'name': location_name,
                                'total_streams': location_data['total_streams'],
                                'active_streams': location_data['active_streams'],
                                'healthy_streams': location_data['healthy_streams'],
                                'children': []
                            }
                            area_item['children'].append(location_item)
                        
                        zone_item['children'].append(area_item)
                    
                    floor_item['children'].append(zone_item)
                
                building_item['children'].append(floor_item)
            
            hierarchy_list.append(building_item)
        
        return JSONResponse(content={
            "status": "success",
            "hierarchy": hierarchy_list,
            "workspace_filter": workspace_id,
            "total_buildings": len(hierarchy_list),
            "summary": {
                "total_streams": sum(b['total_streams'] for b in hierarchy_list),
                "active_streams": sum(b['active_streams'] for b in hierarchy_list),
                "healthy_streams": sum(b['healthy_streams'] for b in hierarchy_list)
            }
        })
        
    except Exception as e:
        logging.error(f"Error getting location hierarchy: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error"
        })

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


@router.post("/start_streams_bulk")
async def start_streams_bulk_endpoint(
    stream_ids: List[str] = Query(..., description="List of stream IDs to start"),
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Start multiple streams from a list of stream IDs."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        if not stream_ids:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "No stream IDs provided"}
            )
        
        if len(stream_ids) > 50:  # Reasonable limit to prevent abuse
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Maximum 50 streams can be started at once"}
            )
        
        # Validate stream ID formats
        valid_stream_uuids = []
        invalid_ids = []
        
        for stream_id_str in stream_ids:
            try:
                valid_stream_uuids.append(UUID(stream_id_str))
            except ValueError:
                invalid_ids.append(stream_id_str)
        
        if invalid_ids:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error", 
                    "message": f"Invalid stream ID format(s): {', '.join(invalid_ids)}"
                }
            )
        
        # Get stream details with workspace and owner info
        placeholders = ','.join([f'${i+1}' for i in range(len(valid_stream_uuids))])
        
        streams_query = f"""
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id, vs.workspace_id, vs.is_streaming,
                   vs.status, vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id IN ({placeholders})
        """
        
        streams_data = await db_manager_global.execute_query(
            streams_query, tuple(valid_stream_uuids), fetch_all=True
        )
        streams_data = streams_data or []
        
        # Check for missing streams
        found_stream_ids = {str(s['stream_id']) for s in streams_data}
        missing_stream_ids = [sid for sid in stream_ids if sid not in found_stream_ids]
        
        results = []
        streams_started_count = 0
        streams_processed_count = len(streams_data)
        
        # Add missing streams to results
        for missing_id in missing_stream_ids:
            results.append({
                "stream_id": missing_id,
                "name": "Unknown",
                "workspace": "Unknown",
                "status": "not_found",
                "message": "Stream not found"
            })
        
        if not streams_data:
            return JSONResponse(content={
                "status": "error" if missing_stream_ids else "info",
                "message": "No valid streams found to process",
                "streams_processed": 0,
                "streams_started": 0,
                "details": results
            })
        
        # Check workspace access for all streams
        workspace_access_checks = {}
        for stream_data in streams_data:
            workspace_id = stream_data['workspace_id']
            if workspace_id not in workspace_access_checks:
                try:
                    await check_workspace_membership_and_get_role(
                        UUID(requester_user_id_str), workspace_id
                    )
                    workspace_access_checks[workspace_id] = True
                except HTTPException:
                    workspace_access_checks[workspace_id] = False
        
        # Get current active stream counts per owner per workspace
        workspace_ids = list(set(s['workspace_id'] for s in streams_data))
        if workspace_ids:
            ws_placeholders = ','.join([f'${i+1}' for i in range(len(workspace_ids))])
            owner_active_streams_query = f"""
                SELECT user_id, workspace_id, COUNT(*) as active_count 
                FROM video_stream
                WHERE workspace_id IN ({ws_placeholders}) AND is_streaming = TRUE 
                GROUP BY user_id, workspace_id
            """
            active_counts_db = await db_manager_global.execute_query(
                owner_active_streams_query, tuple(workspace_ids), fetch_all=True
            )
            active_counts_db = active_counts_db or []
            
            owner_active_streams_map = {
                (row['user_id'], row['workspace_id']): row['active_count'] 
                for row in active_counts_db
            }
        else:
            owner_active_streams_map = {}
        
        # Process each stream
        for stream_data in streams_data:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            workspace_id = stream_data['workspace_id']
            owner_id = stream_data['owner_id']
            
            # Check workspace access
            if not workspace_access_checks.get(workspace_id, False):
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "access_denied",
                    "message": "No access to workspace or workspace not found"
                })
                continue
            
            # Check if already streaming
            if stream_data['is_streaming']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "already_streaming",
                    "message": "Stream is already active"
                })
                continue
            
            # Check owner eligibility
            if not stream_data['owner_is_active']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": "Stream owner is inactive"
                })
                continue
            
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": "Stream owner not subscribed (and not admin)"
                })
                continue
            
            # Check camera limits
            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get((owner_id, workspace_id), 0)
            
            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": f"Owner camera limit ({owner_limit}) reached"
                })
                continue
            
            # Start the stream
            try:
                await db_manager_global.execute_query(
                    "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                    (datetime.now(timezone.utc), stream_id)
                )
                
                streams_started_count += 1
                owner_active_streams_map[(owner_id, workspace_id)] = current_owner_active_count + 1
                
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "initiated",
                    "message": "Stream start initiated"
                })
                
                # Add success notification
                await stream_manager.add_notification(
                    str(owner_id), str(workspace_id), str(stream_id), stream_name,
                    "processing", f"Camera '{stream_name}' start initiated by {requester_username} (bulk action)"
                )
                
            except Exception as e:
                logging.error(f"Error starting stream {stream_id}: {e}", exc_info=True)
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "error",
                    "message": f"Failed to start stream: {str(e)[:100]}"
                })
        
        # Determine overall status
        total_requested = len(stream_ids)
        total_found = len(streams_data)
        
        if streams_started_count == 0:
            status = "info" if total_found == 0 else "failed"
            message = "No streams were started"
        elif streams_started_count == total_requested:
            status = "success"
            message = f"All {streams_started_count} streams started successfully"
        else:
            status = "partial_success"
            message = f"Started {streams_started_count} of {total_requested} requested streams"
        
        return JSONResponse(content={
            "status": status,
            "message": message,
            "summary": {
                "total_requested": total_requested,
                "streams_found": total_found,
                "streams_started": streams_started_count,
                "missing_streams": len(missing_stream_ids),
                "requester": requester_username
            },
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error in bulk stream start: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Internal server error during bulk stream start",
                "details": []
            }
        )

@router.websocket("/start_streams_bulk")
async def websocket_start_streams_bulk(websocket: WebSocket):
    """WebSocket endpoint for bulk starting streams with real-time progress updates."""
    user_id_for_log: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    
    try:
        await websocket.accept()
        
        # Authenticate user
        token = await session_manager_global.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({
                "status": "error", 
                "message": "Authentication token required."
            })
            await websocket.close(1008)
            return
            
        token_data = await session_manager_global.verify_token(token, "access")
        if not token_data or await session_manager_global.is_token_blacklisted(token):
            await websocket.send_json({
                "status": "error", 
                "message": "Invalid or expired token."
            })
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Get user data for logging
        user_db_data = await user_manager_global.get_user_by_id(UUID(user_id_for_log))
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_for_log}"
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": "Connected to bulk stream start service",
            "user": username_for_log,
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Main message processing loop
        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Wait for start request from client
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Handle different message types
                if message.get("type") == "pong":
                    logging.debug(f"Bulk start WS: Pong received from {username_for_log}")
                    continue
                
                elif message.get("type") == "start_streams":
                    stream_ids = message.get("stream_ids", [])
                    
                    if not stream_ids:
                        await websocket.send_json({
                            "type": "error",
                            "message": "No stream IDs provided",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                        continue
                    
                    if len(stream_ids) > 50:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Maximum 50 streams can be started at once",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                        continue
                    
                    # Process the bulk start request
                    await process_bulk_start_request(
                        websocket, stream_ids, user_id_for_log, username_for_log
                    )
                
                else:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Unknown message type: {message.get('type', 'none')}",
                        "timestamp": datetime.now(timezone.utc).timestamp()
                    })
                    
            except asyncio.TimeoutError:
                # No message from client, continue
                continue
            except WebSocketDisconnect:
                break
            except asyncio.CancelledError:
                raise
            except Exception as e_recv:
                logging.error(f"Bulk start WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True)
                if websocket.client_state == WebSocketState.CONNECTED:
                    try:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Error processing message",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                    except:
                        pass
                break
                
    except WebSocketDisconnect:
        logging.info(f"Bulk start WS: Client {username_for_log or 'unknown'} disconnected")
    except asyncio.CancelledError:
        logging.info(f"Bulk start WS task for {username_for_log or 'unknown'} cancelled")
    except Exception as e_outer:
        logging.error(f"Bulk start WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({
                    "status": "error", 
                    "message": "Internal server error"
                })
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()

        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        logging.debug(f"Bulk start WS: Connection cleanup for {username_for_log or 'unknown'}")

async def process_bulk_start_request(
    websocket: WebSocket, 
    stream_ids: List[str], 
    user_id_str: str, 
    username: str
):
    """Process a bulk stream start request with real-time progress updates."""
    
    # Send initial acknowledgment
    await websocket.send_json({
        "type": "start_initiated",
        "total_streams": len(stream_ids),
        "message": f"Starting bulk operation for {len(stream_ids)} streams",
        "timestamp": datetime.now(timezone.utc).timestamp()
    })
    
    try:
        # Validate stream ID formats
        valid_stream_uuids = []
        invalid_ids = []
        
        for i, stream_id_str in enumerate(stream_ids):
            try:
                valid_stream_uuids.append(UUID(stream_id_str))
            except ValueError:
                invalid_ids.append(stream_id_str)
                await websocket.send_json({
                    "type": "stream_error",
                    "stream_id": stream_id_str,
                    "error": "Invalid stream ID format",
                    "progress": {
                        "processed": i + 1,
                        "total": len(stream_ids)
                    },
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
        
        if not valid_stream_uuids:
            await websocket.send_json({
                "type": "bulk_complete",
                "success": False,
                "message": "No valid stream IDs provided",
                "summary": {
                    "total_requested": len(stream_ids),
                    "streams_started": 0,
                    "errors": len(invalid_ids)
                },
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
            return
        
        # Send validation complete
        await websocket.send_json({
            "type": "validation_complete",
            "valid_streams": len(valid_stream_uuids),
            "invalid_streams": len(invalid_ids),
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
        # Get stream details
        placeholders = ','.join([f'${i+1}' for i in range(len(valid_stream_uuids))])
        streams_query = f"""
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id, vs.workspace_id, vs.is_streaming,
                   vs.status, vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id IN ({placeholders})
        """
        
        streams_data = await db_manager_global.execute_query(
            streams_query, tuple(valid_stream_uuids), fetch_all=True
        )
        streams_data = streams_data or []
        
        # Check for missing streams
        found_stream_ids = {str(s['stream_id']) for s in streams_data}
        missing_stream_ids = [sid for sid in stream_ids if sid not in found_stream_ids]
        
        # Send missing streams notifications
        for missing_id in missing_stream_ids:
            await websocket.send_json({
                "type": "stream_not_found",
                "stream_id": missing_id,
                "message": "Stream not found",
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
        
        if not streams_data:
            await websocket.send_json({
                "type": "bulk_complete",
                "success": False,
                "message": "No valid streams found to process",
                "summary": {
                    "total_requested": len(stream_ids),
                    "streams_started": 0,
                    "not_found": len(missing_stream_ids)
                },
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
            return
        
        # Check workspace access
        await websocket.send_json({
            "type": "checking_permissions",
            "message": "Checking workspace permissions",
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
        workspace_access_checks = {}
        for stream_data in streams_data:
            workspace_id = stream_data['workspace_id']
            if workspace_id not in workspace_access_checks:
                try:
                    await check_workspace_membership_and_get_role(
                        UUID(user_id_str), workspace_id
                    )
                    workspace_access_checks[workspace_id] = True
                except HTTPException:
                    workspace_access_checks[workspace_id] = False
        
        # Get current active stream counts
        workspace_ids = list(set(s['workspace_id'] for s in streams_data))
        owner_active_streams_map = {}
        
        if workspace_ids:
            ws_placeholders = ','.join([f'${i+1}' for i in range(len(workspace_ids))])
            owner_active_streams_query = f"""
                SELECT user_id, workspace_id, COUNT(*) as active_count 
                FROM video_stream
                WHERE workspace_id IN ({ws_placeholders}) AND is_streaming = TRUE 
                GROUP BY user_id, workspace_id
            """
            active_counts_db = await db_manager_global.execute_query(
                owner_active_streams_query, tuple(workspace_ids), fetch_all=True
            )
            active_counts_db = active_counts_db or []
            
            owner_active_streams_map = {
                (row['user_id'], row['workspace_id']): row['active_count'] 
                for row in active_counts_db
            }
        
        # Process each stream with real-time updates
        streams_started_count = 0
        processed_count = 0
        
        await websocket.send_json({
            "type": "processing_started",
            "message": "Starting individual stream processing",
            "total_to_process": len(streams_data),
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
        for stream_data in streams_data:
            processed_count += 1
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            workspace_id = stream_data['workspace_id']
            owner_id = stream_data['owner_id']
            
            # Send progress update
            await websocket.send_json({
                "type": "processing_stream",
                "stream_id": str(stream_id),
                "stream_name": stream_name,
                "progress": {
                    "processed": processed_count,
                    "total": len(streams_data)
                },
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
            
            # Check workspace access
            if not workspace_access_checks.get(workspace_id, False):
                await websocket.send_json({
                    "type": "stream_skipped",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "reason": "No access to workspace",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                continue
            
            # Check if already streaming
            if stream_data['is_streaming']:
                await websocket.send_json({
                    "type": "stream_skipped",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "reason": "Already streaming",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                continue
            
            # Check owner eligibility
            if not stream_data['owner_is_active']:
                await websocket.send_json({
                    "type": "stream_skipped",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "reason": "Owner is inactive",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                continue
            
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                await websocket.send_json({
                    "type": "stream_skipped",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "reason": "Owner not subscribed",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                continue
            
            # Check camera limits
            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get((owner_id, workspace_id), 0)
            
            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                await websocket.send_json({
                    "type": "stream_skipped",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "reason": f"Camera limit reached ({owner_limit})",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                continue
            
            # Start the stream
            try:
                await db_manager_global.execute_query(
                    "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                    (datetime.now(timezone.utc), stream_id)
                )
                
                streams_started_count += 1
                owner_active_streams_map[(owner_id, workspace_id)] = current_owner_active_count + 1
                
                await websocket.send_json({
                    "type": "stream_started",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
                
                # Send notification to stream owner
                await stream_manager.add_notification(
                    str(owner_id), str(workspace_id), str(stream_id), stream_name,
                    "processing", f"Camera '{stream_name}' start initiated by {username} (WebSocket bulk)"
                )
                
            except Exception as e:
                logging.error(f"Error starting stream {stream_id} via WebSocket: {e}", exc_info=True)
                await websocket.send_json({
                    "type": "stream_error",
                    "stream_id": str(stream_id),
                    "stream_name": stream_name,
                    "error": f"Failed to start: {str(e)[:100]}",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
        
        # Send completion summary
        total_requested = len(stream_ids)
        success_rate = (streams_started_count / total_requested * 100) if total_requested > 0 else 0
        
        await websocket.send_json({
            "type": "bulk_complete",
            "success": True,
            "message": f"Bulk operation completed: {streams_started_count}/{total_requested} streams started",
            "summary": {
                "total_requested": total_requested,
                "streams_started": streams_started_count,
                "success_rate": round(success_rate, 1),
                "processed": processed_count,
                "missing": len(missing_stream_ids),
                "invalid_ids": len(invalid_ids),
                "requester": username
            },
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
    except Exception as e:
        logging.error(f"Error in WebSocket bulk start processing: {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({
                    "type": "bulk_error",
                    "message": "Internal error during bulk processing",
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
            except:
                pass


@router.get("/debug/shared_streams")
async def debug_shared_streams():
    """Debug endpoint to check shared stream status"""
    if hasattr(stream_manager, 'video_file_manager'):
        stats = stream_manager.video_file_manager.get_all_stats()
        return {"shared_streams": stats}
    return {"error": "Video file manager not available"}

@router.post("/debug/force_restart_shared/{source_path}")
async def debug_force_restart_shared_stream(source_path: str):
    """Force restart a shared stream for debugging"""
    try:
        # URL decode the source path
        import urllib.parse
        decoded_source = urllib.parse.unquote(source_path)
        
        result = await stream_manager.force_restart_shared_stream(decoded_source)
        return {"success": result, "source": decoded_source}
    except Exception as e:
        return {"error": f"Failed to restart shared stream: {e}"}

@router.post("/debug/log_state")
async def debug_log_current_state():
    """Trigger logging of current stream manager state"""
    try:
        await stream_manager.log_stream_manager_state()
        return {"success": True, "message": "State logged to console"}
    except Exception as e:
        return {"error": f"Failed to log state: {e}"}

@router.get("/debug/validate_source/{source_path}")
async def debug_validate_source(source_path: str):
    """Validate a video source for debugging"""
    try:
        import urllib.parse
        decoded_source = urllib.parse.unquote(source_path)
        
        is_valid = await stream_manager._validate_stream_source(decoded_source)
        return {
            "source": decoded_source,
            "is_valid": is_valid,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": f"Failed to validate source: {e}"}


@router.get("/debug/stream/{stream_id}")
async def debug_stream_status(stream_id: str, response: Response):
    """Get detailed debug information for a specific stream"""
    try:
        # Validate stream_id format
        try:
            UUID(stream_id)
        except ValueError:
            response.status_code = 400
            return {"error": "Invalid stream_id format. Must be a valid UUID."}
        
        # Get the detailed status
        status = await stream_manager.get_detailed_stream_status(stream_id)
        
        # Additional validation that the response is JSON serializable
        import json
        try:
            json.dumps(status)
        except TypeError as json_error:
            logging.error(f"JSON serialization error in debug endpoint: {json_error}")
            return {
                "error": "Failed to serialize debug data",
                "stream_id": stream_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": str(json_error)
            }
        
        return status
        
    except Exception as e:
        logging.error(f"Error in debug_stream_status for stream {stream_id}: {e}", exc_info=True)
        response.status_code = 500
        return {
            "error": f"Failed to get stream status: {str(e)}",
            "stream_id": stream_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/debug/streams")
async def debug_all_streams():
    """Get debug information for all streams"""
    try:
        debug_info = await stream_manager.debug_all_streams()
        
        # Validate JSON serializability
        import json
        try:
            json.dumps(debug_info)
        except TypeError as json_error:
            logging.error(f"JSON serialization error in debug all streams: {json_error}")
            return {
                "error": "Failed to serialize all streams debug data",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": str(json_error)
            }
        
        return debug_info
        
    except Exception as e:
        logging.error(f"Error in debug_all_streams: {e}", exc_info=True)
        return {
            "error": f"Failed to get all streams debug info: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/debug/video-sharing")
async def debug_video_sharing():
    """Get debug information about video sharing"""
    try:
        sharing_stats = await stream_manager.get_video_sharing_stats()
        
        # Add additional debug info
        debug_info = {
            **sharing_stats,
            "video_file_manager_exists": hasattr(stream_manager, 'video_file_manager'),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return debug_info
        
    except Exception as e:
        logging.error(f"Error in debug_video_sharing: {e}", exc_info=True)
        return {
            "error": f"Failed to get video sharing debug info: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    