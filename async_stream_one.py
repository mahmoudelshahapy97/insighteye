# async_stream_one.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Response, Depends, Query
from fastapi.responses import JSONResponse
import time
import logging
import asyncio
from collections import defaultdict
import cv2
import numpy as np
import threading 
import uuid # Ensure uuid is imported if not already, though UUID is used from uuid
from uuid import UUID, uuid4
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Any, Union
from ultralytics import YOLO
from async_utils import frame_to_base64, get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists # ensure_... is async
from async_database import DatabaseManager # AsyncDatabaseManager
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from async_config import config
from async_session_manager import SessionManager # Async methods
from async_user_manager import UserManager # Async methods
import concurrent.futures
import os
from starlette.websockets import WebSocketState
from async_workspaces import check_workspace_membership_and_get_role # Async

session_manager_global = SessionManager()
user_manager_global = UserManager()
db_manager_global = DatabaseManager()

logger = logging.getLogger(__name__) # Ensure logger is defined if used standalone
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
        self.yolo_model = self._initialize_model() # Sync init is fine
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
        self.background_task = asyncio.create_task(self.manage_streams())
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

    def _handle_task_done(self, task: asyncio.Task): # This callback must be sync
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
            self.background_task = asyncio.create_task(self.manage_streams())
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
        model_path = config.get("model_path", "yolov8n.pt")
        try:
            model = YOLO(model_path)
            model.conf = config.get("yolo_conf_threshold", 0.4)
            model.iou = config.get("yolo_iou_threshold", 0.45)
            model.agnostic = config.get("yolo_agnostic_nms", False)
            model.max_det = config.get("yolo_max_detections", 100) # Aligned with stream_one.py
            logging.info(f"YOLO Model initialized from {model_path} with conf: {model.conf}, iou: {model.iou}, max_det: {model.max_det}")
            return model
        except Exception as e:
            logging.error(f"Failed to initialize YOLO model from {model_path}: {e}, using default yolov8n.pt.", exc_info=True)
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
        
        logging.info("StreamManager shutdown complete.")

    async def manage_streams(self):
        while True:
            try:
                streams_to_run_query = """
                    SELECT vs.stream_id, vs.name, vs.path, vs.user_id, vs.workspace_id, u.username
                    FROM video_stream vs JOIN users u ON vs.user_id = u.user_id
                    WHERE vs.is_streaming = TRUE AND u.is_active = TRUE 
                          AND (u.is_subscribed = TRUE OR u.role = 'admin')
                """
                potential_streams_db = await self.db_manager.execute_query(streams_to_run_query, fetch_all=True)
                potential_streams_db = potential_streams_db or []

                async with self._lock: 
                    current_running_ids_mem = set(self.active_streams.keys())
                
                db_should_run_ids = {str(s['stream_id']) for s in potential_streams_db}

                for stream_data in potential_streams_db:
                    stream_id_obj, cam_name, source, owner_id_obj, workspace_id_obj, owner_username = \
                        stream_data['stream_id'], stream_data['name'], stream_data['path'], \
                        stream_data['user_id'], stream_data['workspace_id'], stream_data['username']
                    
                    stream_id_str, owner_id_str = str(stream_id_obj), str(owner_id_obj)

                    if stream_id_str in current_running_ids_mem: continue

                    owner_info = await self.db_manager.execute_query("SELECT count_of_camera, role FROM users WHERE user_id = $1", (owner_id_obj,), fetch_one=True)
                    owner_camera_limit = owner_info["count_of_camera"] if owner_info else 5 # Default based on stream_one
                    owner_role = owner_info["role"] if owner_info else "user"
                    
                    active_owner_streams_q = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE" # Count all flagged as streaming
                    active_count_res = await self.db_manager.execute_query(active_owner_streams_q, (owner_id_obj, workspace_id_obj), fetch_one=True)
                    current_owner_ws_active_count = active_count_res['count'] if active_count_res else 0

                    if owner_role == 'admin' or current_owner_ws_active_count < owner_camera_limit:
                        logging.info(f"ManageStreams: Starting stream {stream_id_str} for {owner_username} (ws: {workspace_id_obj}). Limit ok.")
                        # No await here, start_stream_background is fire-and-forget style for the main processing task
                        asyncio.create_task(self.start_stream_background(stream_id_obj, owner_id_obj, owner_username, cam_name, source, workspace_id_obj))
                        # Notification should occur after actual start or if it fails, handled within start_stream_background or _process_stream
                    else:
                        logging.warning(f"Owner {owner_username} at camera limit ({owner_camera_limit}) in ws {workspace_id_obj}. Stream {stream_id_str} will be marked inactive.")
                        await self.db_manager.execute_query("UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW() WHERE stream_id = $1", (stream_id_obj,))

                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                for stream_id_to_stop_str in streams_to_stop_ids:
                    logging.info(f"ManageStreams: Stopping stream {stream_id_to_stop_str} (no longer marked to run in DB or owner/sub issue).")
                    await self._stop_stream(stream_id_to_stop_str, for_restart=False)

                if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
            
            except asyncio.CancelledError:
                logging.info("Manage streams task cancelled.")
                break
            except Exception as e:
                logging.error(f"Error in manage_streams loop: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_manager_poll_interval_seconds", 5.0)) # Ensure float

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
                params = {"frame_delay": 0.0, "frame_skip": 2, "conf_threshold": 0.4} 

            async with self._lock: 
                self.param_cache[cache_key] = params
                self.param_cache_last_updated[cache_key] = current_time_ts
            return params
        except Exception as e:
            logging.error(f"Error getting stream parameters for ws {workspace_id_str}: {e}", exc_info=True)
            return {"frame_delay": 0.0, "frame_skip": 2, "conf_threshold": 0.4}

    async def get_stream_by_id(self, stream_id_str: str, user_id_context_str: str) -> Dict[str, Any]:
        # To match stream_one.py, we'd select fewer fields. Current richer response is fine.
        stream_q = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, 
                   u.username as owner_username, vs.workspace_id, w.name as workspace_name,
                   vs.is_streaming, vs.user_id as owner_id, vs.created_at, vs.updated_at, vs.last_activity
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id = $1
        """
        stream_res = await self.db_manager.execute_query(stream_q, (UUID(stream_id_str),), fetch_one=True)
        
        if not stream_res:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        await check_workspace_membership_and_get_role(UUID(user_id_context_str), stream_res["workspace_id"])
        
        # Matching stream_one.py's fields for the primary response part
        return {
            "stream_id": str(stream_res["stream_id"]), "name": stream_res["name"], "path": stream_res["path"], 
            "type": stream_res["type"], "status": stream_res["status"],
            "owner_username": stream_res["owner_username"], 
            "workspace_id": str(stream_res["workspace_id"]),
            "workspace_name": stream_res["workspace_name"],
            # Optional richer data (can be kept or removed for strict match)
            # "is_streaming": stream_res["is_streaming"],
            # "owner_id": str(stream_res["owner_id"]), 
            # "created_at": stream_res["created_at"].isoformat() if stream_res["created_at"] else None,
            # "updated_at": stream_res["updated_at"].isoformat() if stream_res["updated_at"] else None,
            # "last_activity": stream_res["last_activity"].isoformat() if stream_res["last_activity"] else None,
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


    async def start_stream_background(self, stream_id: UUID, owner_id: UUID, owner_username: str, camera_name: str, source: str, workspace_id: UUID):
        stream_id_str = str(stream_id)
        async with self._lock:
            if stream_id_str in self.active_streams:
                logging.info(f"Stream {stream_id_str} already active or being started.")
                return
            # Tentatively mark as starting to prevent duplicate starts
            self.active_streams[stream_id_str] = {'status': 'starting', 'task': None, 'start_time': datetime.now(timezone.utc)} 

        try:
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'processing', last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            
            stop_event = threading.Event() # For sync parts within the async task
            self.stream_processing_stats[stream_id_str] = {"frames_processed": 0, "detection_count": 0, "avg_processing_time": 0.0, "last_updated": datetime.now(timezone.utc)}

            # Create the stream processing task
            task = asyncio.create_task(self._process_stream(stream_id, camera_name, source, owner_username, owner_id, workspace_id, stop_event))
            task.set_name(f"process_stream_{stream_id_str}")

            async with self._lock: # Fully update stream info
                self.active_streams[stream_id_str] = {
                    'source': source, 'stop_event': stop_event, 'camera_name': camera_name,
                    'username': owner_username, 'user_id': owner_id, 'workspace_id': workspace_id,
                    'clients': set(), 'latest_frame': None, 'last_frame_time': datetime.now(timezone.utc),
                    'task': task, 'start_time': self.active_streams[stream_id_str]['start_time'], # Keep original start time
                    'status': 'active_pending' # Indicates task created, _process_stream will set to 'active'
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
            
    async def _process_stream(self, stream_id: UUID, camera_name: str, source: str, owner_username: str, owner_id: UUID, workspace_id: UUID, stop_event: threading.Event):
        cap = None
        reconnect_attempts = 0
        max_reconnect_attempts = config.get("stream_max_reconnect_attempts", 5)
        frame_count = 0
        last_db_update_activity = datetime.now(timezone.utc)
        stream_id_str = str(stream_id)
        loop = asyncio.get_event_loop()
        
        try:
            params = await self.get_stream_parameters(workspace_id)
            frame_skip = params.get("frame_skip", 2) # Default from stream_one
            frame_delay_target = params.get("frame_delay", 0.0) # Default from stream_one
            conf_threshold = params.get("conf_threshold", 0.4) # Default from stream_one

            cap = await loop.run_in_executor(thread_pool, cv2.VideoCapture, source, cv2.CAP_FFMPEG)

            is_opened_in_executor = await loop.run_in_executor(thread_pool, getattr, cap, 'isOpened') if cap else False
            if not cap or not is_opened_in_executor: # cap.isOpened needs to be called
                for i in range(max_reconnect_attempts):
                    logging.warning(f"Retrying connection to {source} (stream {stream_id_str}), attempt {i+1}")
                    await asyncio.sleep(config.get("stream_reconnect_delay_base_seconds", 2.0) * (i + 1))
                    if cap: await loop.run_in_executor(thread_pool, cap.release)
                    cap = await loop.run_in_executor(thread_pool, cv2.VideoCapture, source, cv2.CAP_FFMPEG)
                    is_opened_in_executor = await loop.run_in_executor(thread_pool, getattr, cap, 'isOpened') if cap else False
                    if cap and is_opened_in_executor: break
                if not cap or not is_opened_in_executor:
                    raise RuntimeError(f"Could not open video source: {source} after retries")

            await loop.run_in_executor(thread_pool, cap.set, cv2.CAP_PROP_BUFFERSIZE, self.frame_buffer_size)
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'active', updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            await self.add_notification(str(owner_id), str(workspace_id), stream_id_str, camera_name, "active", f"Camera '{camera_name}' started streaming.")
            async with self._lock: # Update in-memory status
                if stream_id_str in self.active_streams: self.active_streams[stream_id_str]['status'] = 'active'
            
            while not stop_event.is_set():
                ret, frame = await loop.run_in_executor(thread_pool, cap.read)
                
                if not ret:
                    reconnect_attempts += 1
                    logging.warning(f"Frame read fail from {source} (stream {stream_id_str}), attempt {reconnect_attempts}")
                    if reconnect_attempts >= max_reconnect_attempts:
                        raise RuntimeError(f"Max reconnect attempts reached for {source}")
                    await asyncio.sleep(config.get("stream_reconnect_delay_frame_read_seconds", 5.0))
                    if cap: await loop.run_in_executor(thread_pool, cap.release)
                    cap = await loop.run_in_executor(thread_pool, cv2.VideoCapture, source, cv2.CAP_FFMPEG)
                    is_opened_in_executor = await loop.run_in_executor(thread_pool, getattr, cap, 'isOpened') if cap else False
                    if not cap or not is_opened_in_executor: 
                        await asyncio.sleep(config.get("stream_reconnect_delay_frame_read_seconds", 5.0)) # Extra sleep if still not opened
                    continue 
                
                reconnect_attempts = 0 # Reset on successful frame read
                frame_count += 1
                
                if frame_skip > 0 and frame_count % (frame_skip + 1) != 0:
                    await asyncio.sleep(0.001) # Minimal sleep to yield control
                    continue

                processing_start_time = datetime.now(timezone.utc)
                # detect_objects is sync, run in executor
                processed_frame, person_count = await loop.run_in_executor(
                    thread_pool, self.detect_objects, frame, conf_threshold
                )
                detection_duration = (datetime.now(timezone.utc) - processing_start_time).total_seconds()

                async with self._lock: 
                    stats = self.stream_processing_stats.get(stream_id_str)
                    if stats:
                        stats["frames_processed"] += 1
                        if person_count > 0: stats["detection_count"] += 1
                        stats["avg_processing_time"] = (stats.get("avg_processing_time", 0.0) * 0.95) + (detection_duration * 0.05)
                        stats["last_updated"] = datetime.now(timezone.utc)
                    
                    stream_info_active = self.active_streams.get(stream_id_str)
                    if stream_info_active: 
                        stream_info_active['latest_frame'] = processed_frame
                        stream_info_active['last_frame_time'] = datetime.now(timezone.utc)
                
                if person_count > 0:
                    # insert_detection_data is sync, run in executor
                    await loop.run_in_executor(thread_pool, self.insert_detection_data, owner_username, stream_id_str, camera_name, person_count, processed_frame, workspace_id)

                now_utc_loop = datetime.now(timezone.utc)
                if (now_utc_loop - last_db_update_activity).total_seconds() > config.get("stream_db_activity_update_interval_seconds", 10.0): # Ensure float
                    await self.db_manager.execute_query("UPDATE video_stream SET last_activity = NOW() WHERE stream_id = $1", (stream_id,))
                    last_db_update_activity = now_utc_loop
                
                current_iteration_duration = (datetime.now(timezone.utc) - processing_start_time).total_seconds()
                sleep_duration = max(0, frame_delay_target - current_iteration_duration)
                await asyncio.sleep(sleep_duration if sleep_duration > 0 else 0.001) # Yield control
        
        except asyncio.CancelledError:
            logging.info(f"Stream processing task for {stream_id_str} ({camera_name}) was cancelled.")
        except RuntimeError as e: # Specific for unrecoverable errors like max retries
            logging.error(f"Unrecoverable stream error for {stream_id_str} ({camera_name}): {e}", exc_info=False) 
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'error', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            await self.add_notification(str(owner_id), str(workspace_id), stream_id_str, camera_name, "error", f"Stream error: {str(e)[:100]}")
        except Exception as e: # General errors
            logging.error(f"General error in _process_stream for {stream_id_str} ({camera_name}): {e}", exc_info=True)
            await self.db_manager.execute_query("UPDATE video_stream SET status = 'error', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,))
            await self.add_notification(str(owner_id), str(workspace_id), stream_id_str, camera_name, "error", "Unexpected stream error. Check logs.")
        finally:
            if cap: await loop.run_in_executor(thread_pool, cap.release)
            logging.info(f"Stream processing ended for {stream_id_str} ({camera_name}). Cleaning up in-memory structures.")
            
            async with self._lock: self.active_streams.pop(stream_id_str, None)
            self.stream_processing_stats.pop(stream_id_str, None)

            # If task exited due to an error (not explicit cancellation/stop_event), ensure DB reflects it's no longer streaming
            if not stop_event.is_set() and not isinstance(loop.current_task().exception(), asyncio.CancelledError):
                current_db_status = await self.db_manager.execute_query(
                    "SELECT status, is_streaming FROM video_stream WHERE stream_id = $1", (stream_id,), fetch_one=True
                )
                # Check if it's not already marked as error and non-streaming
                if current_db_status and not (current_db_status.get('status') == 'error' and not current_db_status.get('is_streaming', True)):
                     await self.db_manager.execute_query( # Default to inactive if not an error state that already set is_streaming=false
                         "UPDATE video_stream SET status = 'inactive', is_streaming = FALSE, last_activity = NOW(), updated_at = NOW() WHERE stream_id = $1", (stream_id,)
                     )


    def detect_objects(self, frame: np.ndarray, conf_threshold: float = 0.4) -> tuple[np.ndarray, int]: # Sync, CPU-bound
        if frame is None or frame.size == 0: return np.zeros((100, 100, 3), dtype=np.uint8), 0

        max_dim = config.get("yolo_max_input_dim", 640)
        h, w = frame.shape[:2]
        scale = 1.0
        if h > max_dim or w > max_dim:
            scale = max_dim / max(h, w)
            new_w, new_h = int(w * scale), int(h * scale)
            new_w = max(2, new_w - (new_w % 2)) # Ensure positive and even
            new_h = max(2, new_h - (new_h % 2))
            input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        else:
            input_frame = frame # Use directly if already small enough

        try:
            results = self.yolo_model.predict(source=input_frame, conf=conf_threshold, classes=[0], verbose=False)
            person_count = 0
            
            # Use YOLO's plot() method for annotations on a copy of input_frame
            annotated_frame = results[0].plot(img=input_frame.copy()) if results and results[0].boxes is not None else input_frame.copy()
            
            if results and results[0].boxes is not None:
                for box in results[0].boxes:
                    if int(box.cls[0]) == 0: 
                        person_count += 1
            
            # If original frame was resized, scale annotated_frame back up for display if needed
            # This depends on whether the consumer expects original or processed dimensions.
            # stream_one.py returned the annotated (potentially scaled) input_frame.
            # For consistency, we might return annotated_frame based on input_frame's dimensions.
            # However, results[0].plot() annotates on a copy of input_frame (which might be scaled).
            # If the original frame (before any scaling for YOLO) dimensions are (h,w)
            # and input_frame has dimensions (new_h, new_w),
            # then annotated_frame will also be (new_h, new_w).
            # stream_one.py behavior: cv2.rectangle/putText on `annotated_frame` (which was input_frame.copy()).
            # So, it also returned a frame of `input_frame`'s dimensions. This is consistent.

            # Optional: Add count text if not handled well by plot() or for custom styling (stream_one does this)
            cv2.putText(annotated_frame, f"People: {person_count}", (10, 20), 
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255,255,255), 2, cv2.LINE_AA)

            return annotated_frame, person_count
        except Exception as e:
            logger.error(f"Object detection error: {e}", exc_info=True)
            return frame.copy(), 0 # Return original unannotated frame on error

    def insert_detection_data(self, username: str, camera_id_str: str, camera_name: str, count: int, frame: np.ndarray, workspace_id: UUID): # Sync
        if count == 0: return

        target_collection_name = get_workspace_qdrant_collection_name(workspace_id)
        now_utc = datetime.now(timezone.utc)
        point_id_str = str(uuid4())
        
        payload = {
            "camera_id": camera_id_str, "name": camera_name, "timestamp": now_utc.timestamp(),
            "date": now_utc.strftime("%Y-%m-%d"), "time": now_utc.strftime("%H:%M:%S.%f")[:-3], 
            "person_count": count, "frame_base64": frame_to_base64(frame), "username": username,
        }
        # Match vector size from stream_one.py if it used 1, or make it consistently configurable.
        # stream_one.py used [0.0]*config.get("qdrant_vector_size", 1). So default 1.
        vector_size = config.get("qdrant_vector_size", 1) 
        point = qdrant_models.PointStruct(id=point_id_str, vector=[0.0] * vector_size, payload=payload)
        
        try:
            self.qdrant_client.upsert(collection_name=target_collection_name, points=[point], wait=False) # wait=False is good for perf
        except Exception as e:
            logger.error(f"Error inserting detection data to Qdrant ({target_collection_name}): {e}", exc_info=True)

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

        # Use $1, $2, ... for placeholders with asyncpg
        placeholders = ', '.join([f'${i+len(target_workspace_ids_objs)+1}' for i in range(len(target_workspace_ids_objs))]) # This is wrong.
        # Correct placeholder generation for IN clause with variable number of items
        # The query parameters will be (ws_id1, ws_id2, ...). Placeholders should be $1, $2, ...
        placeholders_corrected = ', '.join([f'${i+1}' for i in range(len(target_workspace_ids_objs))])

        streams_q = f"""
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.user_id as owner_id, u.username as owner_username,
                   vs.workspace_id, w.name as workspace_name,
                   vs.created_at, vs.updated_at
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.workspace_id IN ({placeholders_corrected})
            ORDER BY w.name, vs.name
        """
        streams_data_db = await self.db_manager.execute_query(streams_q, tuple(target_workspace_ids_objs), fetch_all=True)
        streams_data_db = streams_data_db or []
        
        formatted_streams = [{ # Match stream_one.py fields
            "stream_id": str(s["stream_id"]), "name": s["name"], "path": s["path"], "type": s["type"], 
            "status": s["status"], "is_streaming": s["is_streaming"],
            "owner_id": str(s["owner_id"]), "owner_username": s["owner_username"],
            "workspace_id": str(s["workspace_id"]), "workspace_name": s["workspace_name"],
            "created_at": s["created_at"].isoformat() if s["created_at"] else None,
            "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
            "can_control": True # stream_one.py had this, assuming it's always true for listed streams
        } for s in streams_data_db]
        return {"streams": formatted_streams, "total": len(formatted_streams)}

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
        ping_interval = float(config.get("websocket_ping_interval", 30.0)) # Match stream_one.py config key
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(ping_interval)
            if websocket.client_state == WebSocketState.CONNECTED: # Re-check state
                await websocket.send_json({"type": "ping", "timestamp": datetime.now(timezone.utc).timestamp()})
            else: break 
    except (WebSocketDisconnect, asyncio.CancelledError, ConnectionResetError, RuntimeError):
        logging.debug("Ping task for WebSocket ended (disconnect/cancel/error).")
    except Exception as e: # Catch any other exceptions during ping
        logging.error(f"Error in WebSocket ping task: {e}", exc_info=True)

# === Endpoints to match stream_one.py ===

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
            (datetime.now(timezone.utc), stream_id_uuid), return_rowcount=True
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
        # execute_query needs to support RETURNING. Assuming it can give rowcount or list of dicts.
        # Let's assume return_rowcount behavior is sufficient or adapt if needed based on DB manager capabilities.
        # If it returns list of dicts from RETURNING:
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

# === END OF BULK OPERATION ENDPOINTS ===

@router.websocket("/stream") # Path changed
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

        stream_details_db = await db_manager_global.execute_query(
            """SELECT vs.workspace_id, vs.user_id as owner_id, vs.name, vs.is_streaming, vs.status, u.username as owner_username
               FROM video_stream vs JOIN users u ON vs.user_id = u.user_id WHERE vs.stream_id = $1""",
            (stream_id_uuid,), fetch_one=True
        )
        if not stream_details_db:
            await websocket.send_json({"status": "error", "message": "Stream not found."}); await websocket.close(1008); return

        s_workspace_id, s_name, s_is_streaming_db, s_status_db, s_owner_username = \
            stream_details_db['workspace_id'], stream_details_db['name'], \
            stream_details_db['is_streaming'], stream_details_db['status'], stream_details_db['owner_username']

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

        await websocket.send_json({ # Match stream_one.py payload
            "status": "connected", "message": f"Connected to stream: {s_name}",
            "stream_id": stream_id_str, "owner": s_owner_username, 
            "workspace_id": str(s_workspace_id), "your_role": requester_role_info.get("role")
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
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close()


@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
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
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        logging.debug(f"Notify WS: Connection cleanup for {username_for_log or 'unknown'}.")


@router.get("/notify") # HTTP GET, matches stream_one.py
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
            "notifications": notifications, "server_time": datetime.now(timezone.utc).timestamp()
        }
    except Exception as e:
        logging.error(f"Error getting HTTP notifications for user {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}") # Match stream_one

@router.post("/notify/{notification_id_str}/read") # Matches stream_one.py
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
            (datetime.now(timezone.utc), notification_id_uuid, UUID(user_id_str)), return_rowcount=True
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
