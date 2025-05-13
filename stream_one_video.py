from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Response, Depends, Query
from fastapi.responses import JSONResponse
import logging
import asyncio
from collections import defaultdict
import cv2
import numpy as np
import threading
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from ultralytics import YOLO
from functools import lru_cache
from utils import frame_to_base64
from database import execute_db_query#, create_db_pool
from qdrant_client import QdrantClient
from qdrant_client.http import models
from config import config
from session_manager import SessionManager
from user_manager import UserManager
import concurrent.futures
import os

session_manager = SessionManager()
user_manager = UserManager()

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler("app.log", maxBytes=10*1024*1024, backupCount=5),
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["stream-one"]) 

# Global thread pool for CPU-bound operations
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 2))

# Global tracking of active streams
class StreamManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()  # Lock for thread-safe notification operations
        self._batch_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()

        # Data structures
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_processing_stats = {}
        self.user_collections_cache = {}
        self.param_cache = {}
        self.param_cache_ttl = 300  # 5 minutes
        self.param_cache_last_updated = {}
        self.notifications: List[Dict[str, Any]] = []
        self.notification_subscribers = defaultdict(set)
        self.batch_processing_queue = {}

        # Constants and configuration
        self.max_notifications = 100
        self.batch_size = 10  # Increased batch size
        self.batch_timeout = 3.0  # Seconds
        self.frame_buffer_size = 3
        self.base_collection_name = config.get("qdrant_collection_name", "person_counts")

        # Add connection pool for batch database operations
        # # Connection pool
        # self.db_pool = create_db_pool(
        #     min_size=5,
        #     max_size=20,
        #     timeout=60
        # )
        self._db_pool_size = 5  # Adjust based on your database server capacity
        self._db_semaphore = asyncio.Semaphore(self._db_pool_size)
        
        # Initialize healthcheck timer
        self.last_healthcheck = datetime.now()
        self.healthcheck_interval = 60  # seconds

        self.qdrant_client = QdrantClient(
            url=config.get("qdrant_url", "localhost"),
            port=config.get("qdrant_port", 6333)
            # timeout=30,  # Increased timeout for batch operations
            # prefer_grpc=True  # Use gRPC for better performance
        )

        # Use a singleton model instance for better performance
        self.yolo_model = self._initialize_model()
        # self.yolo_model = YOLO(config.get("model_path", "yolov8n.pt"))

        # Thread pool for CPU-bound operations
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(32, (os.cpu_count() or 1) * 2)
        )

        # Start background tasks
        self.background_task = None
        self.cleanup_task = None
        self.batch_processing_task = None

        logging.info("StreamManager initialized - waiting for start_background_tasks() to be called")

        # try:
        #     asyncio.create_task(self._start_background_tasks())

        # except Exception as e:
        #     logging.error(f"Failed to create background task: {e}", exc_info=True)
        #     # Try to restart after delay
        #     asyncio.create_task(self._restart_background_task())

    async def start_background_tasks(self):
        """
        Start all background tasks. This should be called from an async context
        with a running event loop (like during FastAPI startup).
        """
        try:
            logging.info("Starting background tasks...")
            await self._start_background_tasks()
            logging.info("Background tasks started successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to start background tasks: {e}", exc_info=True)
            return False

    async def _start_background_tasks(self):
        """Initialize all background tasks with proper error handling"""
        try:
            # Cancel any existing tasks first (in case of restart)
            await self.stop_background_tasks()
            self.background_task = asyncio.create_task(self.manage_streams())
            self.background_task.add_done_callback(self._handle_task_done)
            
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.add_done_callback(self._handle_task_done)
            
            # self.batch_processing_task = asyncio.create_task(self._batch_processor())
            # self.batch_processing_task.add_done_callback(self._handle_task_done)
            
            logging.info("All background tasks initialized successfully")
        except Exception as e:
            logging.error(f"Failed to create background tasks: {e}", exc_info=True)
            # Instead of trying to create another task, we'll just log
            logging.error("Background tasks initialization failed, will retry on next startup")
    
    async def stop_background_tasks(self):
        """Stop all background tasks safely"""
        for task_name, task in [
            ("background_task", self.background_task),
            ("cleanup_task", self.cleanup_task), 
            # ("batch_processing_task", self.batch_processing_task)
        ]:
            if task and not task.done():
                try:
                    task.cancel()
                    await asyncio.shield(asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=5.0))
                    logging.info(f"Task {task_name} cancelled successfully")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout while cancelling {task_name}")
                except Exception as e:
                    logging.error(f"Error cancelling {task_name}: {e}")

    def _handle_task_done(self, task):
        """Non-coroutine callback that handles task completion and schedules error handling if needed"""
        try:
            # Check if task ended with an exception
            exception = task.exception()
            if exception:
                # Schedule the async error handler to run
                asyncio.create_task(self._handle_task_exception_async(task))
        except asyncio.CancelledError:
            # This is expected during normal shutdown
            logging.info("Background task was cancelled during shutdown")
        except Exception as e:
            # This should rarely happen, as it means exception() itself raised an error
            logging.error(f"Error in task completion handler: {e}")
            # Try to restart the task anyway
            asyncio.create_task(self._restart_background_task())

    async def _handle_task_exception_async(self, task):
        """Async method to handle exceptions in tasks properly"""
        try:
            # Re-raise the exception to log it properly
            exception = task.exception()
            task_name = task.get_name() if hasattr(task, 'get_name') else "unknown"
            logging.error(f"Task {task_name} failed with error: {exception}", exc_info=True)
            
            # Restart appropriate task based on which one failed
            if task == self.background_task:
                logging.info("Restarting stream management task")
                self.background_task = asyncio.create_task(self.manage_streams())
                self.background_task.add_done_callback(self._handle_task_done)
            elif task == self.cleanup_task:
                logging.info("Restarting cleanup task")
                self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
                self.cleanup_task.add_done_callback(self._handle_task_done)
            elif task == self.batch_processing_task:
                logging.info("Restarting batch processing task")
                self.batch_processing_task = asyncio.create_task(self._batch_processor())
                self.batch_processing_task.add_done_callback(self._handle_task_done)
            else:
                logging.warning(f"Unknown task failed: {task}")
                
        except Exception as e:
            logging.error(f"Error handling task exception: {e}", exc_info=True)
            # Last resort - try general restart
            await self._restart_background_task()

    # This method was missing but used in code
    async def _periodic_cleanup(self):
        """Periodically clean up resources like expired cache entries"""
        while True:
            try:
                # Clean up expired parameter cache entries
                current_time = datetime.now().timestamp()
                expired_keys = []
                for key, update_time in self.param_cache_last_updated.items():
                    if current_time - update_time > self.param_cache_ttl:
                        expired_keys.append(key)
                
                # Remove expired entries
                for key in expired_keys:
                    self.param_cache.pop(key, None)
                    self.param_cache_last_updated.pop(key, None)
                
                if expired_keys:
                    logging.debug(f"Cleaned up {len(expired_keys)} expired parameter cache entries")
                    
                # Other cleanup tasks can be added here
                
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}", exc_info=True)
                
            # Run cleanup every minute
            await asyncio.sleep(60)

    # Method to check health of streams - used but was missing
    async def _check_stream_health(self):
        """Check health of active streams and restart if necessary"""
        try:
            current_time = datetime.now()
            streams_to_restart = []
            
            for stream_id, stream_info in list(self.active_streams.items()):
                # Check last frame time - if too old, stream might be frozen
                if 'last_frame_time' in stream_info:
                    time_since_last_frame = (current_time - stream_info['last_frame_time']).total_seconds()
                    if time_since_last_frame > 30:  # No frame for 30 seconds
                        logging.warning(f"Stream {stream_id} may be frozen - no frames for {time_since_last_frame:.1f} seconds")
                        streams_to_restart.append(stream_id)
            
            # Restart problematic streams
            for stream_id in streams_to_restart:
                logging.info(f"Restarting frozen stream: {stream_id}")
                await self._stop_stream(stream_id)
                
                # Get stream details to restart it
                stream_details = execute_db_query(
                    """
                    SELECT user_id, name, path FROM video_stream
                    WHERE stream_id = %s
                    """,
                    (stream_id,),
                    fetch_one=True
                )
                
                if stream_details:
                    user_id, camera_name, source = stream_details
                    username = execute_db_query(
                        "SELECT username FROM users WHERE user_id = %s",
                        (user_id,),
                        fetch_one=True
                    )[0]
                    
                    # Restart stream
                    await self.start_stream_background(stream_id, user_id, username, camera_name, source)
                
        except Exception as e:
            logging.error(f"Error in stream health check: {e}", exc_info=True)

    def _initialize_model(self):
        """Initialize the YOLO model with optimizations"""
        model_path = config.get("model_path", "yolov8n.pt")
        try:
            model = YOLO(model_path)
            # Set optimized inference parameters
            model.conf = 0.4  # Default confidence threshold
            model.iou = 0.45  # Default IoU threshold
            model.agnostic = False  # NMS class-agnostic
            model.multi_label = False  # Multiple labels per box
            model.max_det = 100  # Maximum detections per image
            
            # Use CUDA if available for better performance
            model.to('cuda' if cv2.cuda.getCudaEnabledDeviceCount() > 0 else 'cpu')
            
            logging.info(f"Model initialized on {'CUDA' if cv2.cuda.getCudaEnabledDeviceCount() > 0 else 'CPU'}")
            return model
        except Exception as e:
            logging.error(f"Failed to initialize model: {e}, using default settings")
            return YOLO(model_path)

    async def connect_client_to_stream(self, stream_id: str, websocket):
        """Connect a WebSocket client to an existing stream"""
        if stream_id in self.active_streams:
            self.active_streams[stream_id]['clients'].add(websocket)
            return True
        return False
        
    async def disconnect_client(self, stream_id: str, websocket):
        """Disconnect a WebSocket client from a stream"""
        if stream_id in self.active_streams:
            if websocket in self.active_streams[stream_id]['clients']:
                self.active_streams[stream_id]['clients'].remove(websocket)

    async def shutdown(self):
        """Clean up resources when application is shutting down"""
        logging.info("Shutting down StreamManager...")
        
        # Cancel the background management task
        # if hasattr(self, 'background_task') and not self.background_task.done():
        #     self.background_task.cancel()
        #     try:
        #         await self.background_task
        #     except asyncio.CancelledError:
        #         pass
        # Stop all background tasks
        await self.stop_background_tasks()

        # Stop all active streams
        for stream_id, stream_info in list(self.active_streams.items()):
            if 'stop_event' in stream_info:
                stream_info['stop_event'].set()
            if 'task' in stream_info and not stream_info['task'].done():
                stream_info['task'].cancel()
                try:
                    await stream_info['task']
                except asyncio.CancelledError:
                    pass
        
        # Update database to reflect stream is no longer running
            try:
                execute_db_query(
                    """
                    UPDATE video_stream 
                    SET is_streaming = FALSE, status = 'inactive'
                    WHERE stream_id = %s
                    """, 
                    (stream_id,)
                )
            except Exception as e:
                logging.error(f"Failed to update stream status during shutdown: {e}")

        self.active_streams.clear()
        logging.info("StreamManager shutdown complete")

    async def _restart_background_task(self):
        """Attempt to restart the background task after failure"""
        await asyncio.sleep(5)  # Wait before trying to restart
        try:
            logging.info("Attempting to restart background stream management task")
            self.background_task = asyncio.create_task(self.manage_streams())
            self.background_task.add_done_callback(lambda t: self._handle_task_exception(t))
            logging.info("Successfully restarted background stream management task")
        except Exception as e:
            logging.error(f"Failed to restart background task: {e}", exc_info=True)
            # Try again after a longer delay
            asyncio.create_task(self._delayed_restart())
            
    async def _delayed_restart(self):
        """Try restarting again after a longer delay"""
        await asyncio.sleep(30)  # Longer delay
        try:
            self.background_task = asyncio.create_task(self.manage_streams())
            self.background_task.add_done_callback(lambda t: self._handle_task_exception(t))
        except Exception as e:
            logging.error(f"Failed second attempt to restart background task: {e}", exc_info=True)

    async def _handle_task_exception(self, task):
        """Handle exceptions in background tasks"""
        try:
            # This will raise the exception if there was one
            task.result()
        except asyncio.CancelledError:
            # This is normal during shutdown, don't log as error
            logging.info("Background task was cancelled")
        except Exception as e:
            logging.error(f"Background task failed with error: {e}", exc_info=True)
            # Restart the task after a delay
            asyncio.create_task(self._restart_background_task())

    async def _clean_websocket_connections(self):
        """Clean up dead WebSocket connections"""
        # Clean up notification subscribers
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_connections = set()
                for ws in websockets:
                    try:
                        # Send ping to check connection
                        await ws.send_json({"type": "ping", "timestamp": datetime.now().timestamp()})
                    except Exception:
                        dead_connections.add(ws)
                
                if dead_connections:
                    self.notification_subscribers[user_id] -= dead_connections
                    if not self.notification_subscribers[user_id]:
                        del self.notification_subscribers[user_id]
        
        # Clean up stream clients
        for stream_id, stream_info in list(self.active_streams.items()):
            if 'clients' in stream_info:
                dead_connections = set()
                for ws in stream_info['clients']:
                    try:
                        await ws.send_json({"type": "ping", "timestamp": datetime.now().timestamp()})
                    except Exception:
                        dead_connections.add(ws)
                
                if dead_connections:
                    stream_info['clients'] -= dead_connections

    async def _process_lingering_batches(self):
        """Process any batches that have been waiting too long"""
        current_time = datetime.now().timestamp()
        
        async with self._batch_lock:
            for batch_key, batch_info in list(self.batch_processing_queue.items()):
                if current_time - batch_info["last_processed"] > self.batch_timeout and batch_info["points"]:
                    collection_name = batch_key.split('_', 1)[1]  # Extract collection name
                    points_to_process = batch_info["points"]
                    self.batch_processing_queue[batch_key] = {
                        "points": [],
                        "last_processed": current_time
                    }
                    asyncio.create_task(self._process_qdrant_batch(collection_name, points_to_process))

    async def manage_streams(self):
        """Background task that continuously checks database for streams that should be running"""
        # Use cache to reduce database queries
        user_stream_cache = {}
        user_cache_ttl = 30  # 30 seconds
        user_cache_update = {}
        
        while True:
            try:
                # Get active streams efficiently with a single query
                streams_to_run = execute_db_query(
                    """
                    SELECT vs.stream_id, vs.name, vs.path, vs.user_id, vs.status, u.username, 
                           u.count_of_camera, u.is_active, u.is_subscribed
                    FROM video_stream vs
                    JOIN users u ON vs.user_id = u.user_id
                    WHERE vs.is_streaming = TRUE AND u.is_active = TRUE AND u.is_subscribed = TRUE
                    """,
                    fetch_all=True
                )

                # Process in batches for better efficiency
                current_streams = set(self.active_streams.keys())
                streams_to_activate = set()
                
                # Group streams by user for efficient processing
                user_streams = defaultdict(list)
                for stream in streams_to_run:
                    stream_id = str(stream[0])
                    user_id = str(stream[3])
                    streams_to_activate.add(stream_id)
                    user_streams[user_id].append(stream)
                
                # Process each user's streams
                for user_id, streams in user_streams.items():
                    # Get first stream's user info (all streams for same user have same info)
                    first_stream = streams[0]
                    username = first_stream[5]
                    max_cameras = first_stream[6]
                    is_active = first_stream[7]
                    is_subscribed = first_stream[8]
                    
                    if not is_active or not is_subscribed:
                        continue
                    
                    # Check stream limit for this user
                    cache_key = f"stream_count_{user_id}"
                    active_stream_count = len([s for s in streams if str(s[0]) in self.active_streams])
                    
                    # Update cache
                    user_stream_cache[cache_key] = {
                        "count": active_stream_count,
                        "limit": max_cameras
                    }
                    user_cache_update[cache_key] = datetime.now().timestamp()
                    
                    # Start new streams if under limit
                    streams_to_start = [s for s in streams if str(s[0]) not in self.active_streams]
                    available_slots = max_cameras - active_stream_count
                    
                    for stream in streams_to_start[:available_slots]:
                        stream_id = str(stream[0])
                        camera_name = stream[1]
                        source = stream[2]
                        
                        # Start stream in background
                        await self.start_stream_background(stream_id, user_id, username, camera_name, source)
                        
                        # Add notification
                        await self.add_notification(
                            user_id=user_id,
                            stream_id=stream_id,
                            camera_name=camera_name,
                            status="active",
                            message=f"Camera {camera_name} turned on"
                        )
                
                # Handle streams that need to be stopped
                streams_to_stop = current_streams - streams_to_activate
                for stream_id in streams_to_stop:
                    await self._stop_stream(stream_id)
                    
                # Check health of active streams
                if datetime.now().timestamp() - self.last_healthcheck.timestamp() > self.healthcheck_interval:
                    async with self._health_lock:
                        await self._check_stream_health()
                        self.last_healthcheck = datetime.now()
            
            except Exception as e:
                logging.error(f"Error in stream manager background task: {e}", exc_info=True)
            
            await asyncio.sleep(5)  # Check every 5 seconds

    # @lru_cache(maxsize=100)
    def _get_cached_user_info(self, user_id: str, timeout=60):
        """Get cached user info with timeout to ensure cache is refreshed periodically"""
        try:
            user_info = execute_db_query(
                """
                SELECT username, is_active, is_subscribed, count_of_camera
                FROM users 
                WHERE user_id = %s
                """,
                (user_id,),
                fetch_one=True
            )
            
            if user_info:
                return {
                    "username": user_info[0],
                    "is_active": user_info[1],
                    "is_subscribed": user_info[2],
                    "max_cameras": user_info[3]
                }
        except Exception as e:
            logging.error(f"Error getting user info: {e}")
        
        return None

    # @lru_cache(maxsize=50)
    async def get_stream_parameters(self, user_id: str, camera_id: str = None):
        """Get stream parameters for a user, with optional camera-specific parameters"""
        cache_key = f"params_{user_id}_{camera_id or 'global'}"
        current_time = datetime.now().timestamp()
        
        # Return cached parameters if available and not expired
        if cache_key in self.param_cache and (current_time - self.param_cache_last_updated.get(cache_key, 0)) < self.param_cache_ttl:
            return self.param_cache[cache_key]

        try:
            params = None
            # First check for camera-specific parameters
            if camera_id:
                camera_params = execute_db_query(
                    """
                    SELECT frame_delay, frame_skip, conf
                    FROM param_stream_camera
                    WHERE user_id = %s AND camera_id = %s
                    """,
                    (user_id, camera_id),
                    fetch_one=True
                )
                
                if camera_params:
                    params = {
                        "frame_delay": float(camera_params[0]),
                        "frame_skip": int(camera_params[1]),
                        "conf_threshold": float(camera_params[2])
                    }
      
            # Fall back to user's global parameters
            if not params:

                # Fall back to user's global parameters
                user_params = execute_db_query(
                    """
                    SELECT frame_delay, frame_skip, conf
                    FROM param_stream
                    WHERE user_id = %s
                    """,
                    (user_id,),
                    fetch_one=True
                )
                
                if user_params:
                    params = {
                        "frame_delay": float(user_params[0]),
                        "frame_skip": int(user_params[1]),
                        "conf_threshold": float(user_params[2])
                    }
                   
            # Use default parameters if none found
            if not params:
                params = {
                    "frame_delay": 0.0,
                    "frame_skip": 2,  # Default to skipping some frames
                    "conf_threshold": 0.4
                }

            # Cache the default result
            self.param_cache[cache_key] = params
            self.param_cache_last_updated[cache_key] = current_time
            return params

        except Exception as e:
            logging.error(f"Error getting stream parameters: {e}")
            # Default parameters on error
            return {
                "frame_delay": 0.0,
                "frame_skip": 2,
                "conf_threshold": 0.4
            }

    async def get_stream_by_id(self, stream_id: str, user_id: str):
        """Retrieve a specific stream by ID and verify user access"""
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id must be provided")
            
        if not stream_id:
            raise HTTPException(status_code=400, detail="Stream ID must be provided")
        
        # Use cache key for stream info
        cache_key = f"stream_{stream_id}_{user_id}"
        
        # Check if we have cached stream info
        if cache_key in self.param_cache and (datetime.now().timestamp() - self.param_cache_last_updated.get(cache_key, 0)) < self.param_cache_ttl:
            return self.param_cache[cache_key]

        stream = execute_db_query(
            """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, u.username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = %s AND vs.user_id = %s
            """, 
            (stream_id, user_id), 
            fetch_one=True
        )
        
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found or access denied")
        
        # Ensure user's collection exists in the background
        asyncio.create_task(self.ensure_user_collection_exists(stream[5]))

        stream_info = {
            "stream_id": str(stream[0]), 
            "name": stream[1], 
            "path": stream[2], 
            "type": stream[3],
            "status": stream[4],
            "username": stream[5]
        }

        # Cache the result
        self.param_cache[cache_key] = stream_info
        self.param_cache_last_updated[cache_key] = datetime.now().timestamp()

        return stream_info

    async def add_notification(self, user_id: str, stream_id: str, camera_name: str, status: str, message: str):
        """Add a new notification about camera status change and deliver immediately"""
        # Create notification with current timestamp
        current_time = datetime.now()
        notification = {
            "id": str(uuid.uuid4()),
            "user_id": user_id,
            "stream_id": stream_id,
            "camera_name": camera_name,
            "status": status,
            "message": message,
            "timestamp": current_time.timestamp(),
            "read": False
        }
        
        # Add to notifications with thread safety
        async with self._notification_lock:
            self.notifications.append(notification)
            # Keep only the most recent notifications
            if len(self.notifications) > self.max_notifications:
                self.notifications = self.notifications[-self.max_notifications:]
        
        logging.info(f"Added notification: {notification['message']} for user {user_id}")
        
        # Deliver immediately to any subscribed WebSockets for this user
        await self.deliver_notification_to_subscribers(user_id, notification)
        
        # Use a background task for DB persistence to avoid blocking
        asyncio.create_task(self._persist_notification(notification, user_id, stream_id, camera_name, status, message, current_time))

        return notification
    
    async def _persist_notification(self, notification, user_id, stream_id, camera_name, status, message, timestamp):
        """Persist notification to database in background"""
        try:
            execute_db_query(
                """
                INSERT INTO notifications (notification_id, user_id, stream_id, camera_name, status, message, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (notification["id"], user_id, stream_id, camera_name, status, message, timestamp)
            )
        except Exception as e:
            logging.error(f"Failed to persist notification to database: {e}")
        
    async def deliver_notification_to_subscribers(self, user_id: str, notification: Dict[str, Any]):
        """Deliver a notification to all subscribed WebSockets for this user with improved reliability"""
        if user_id not in self.notification_subscribers:
            return
        
        dead_connections = set()
        successful_deliveries = 0
        server_time = datetime.now().timestamp()
        
        notification_data = {
            "type": "notification",
            "notification": notification,
            "server_time": server_time
        }
        
        for websocket in self.notification_subscribers[user_id]:
            try:
                await websocket.send_json(notification_data)
                successful_deliveries += 1
            except WebSocketDisconnect:
                logging.info(f"WebSocket disconnected while sending notification to user {user_id}")
                dead_connections.add(websocket)
            except Exception as e:
                logging.warning(f"Failed to send notification to WebSocket: {e}")
                dead_connections.add(websocket)
        
        # Clean up any dead connections
        if dead_connections:
            async with self._notification_lock:
                self.notification_subscribers[user_id] -= dead_connections
                if not self.notification_subscribers[user_id]:
                    del self.notification_subscribers[user_id]
        
        if successful_deliveries > 0:
            logging.info(f"Successfully delivered notification to {successful_deliveries} clients for user {user_id}")
        
        return successful_deliveries
        
    def get_notifications(self, user_id: str, since: float = None, limit: int = 50, include_read: bool = True) -> List[Dict[str, Any]]:
        """
        Get notifications for a specific user, with filtering options
        
        Args:
            user_id: The user ID to get notifications for
            since: Optional timestamp to filter notifications from
            limit: Maximum number of notifications to return
            include_read: Whether to include notifications marked as read
            
        Returns:
            List of notification objects
        """
        # Filter by user_id first
        user_notifications = [n for n in self.notifications if n["user_id"] == user_id]
        
        # Apply additional filters
        if since is not None:
            user_notifications = [n for n in user_notifications if n["timestamp"] > since]
        
        if not include_read:
            user_notifications = [n for n in user_notifications if not n.get("read", False)]
        
        # Sort by timestamp (newest first)
        user_notifications.sort(key=lambda n: n["timestamp"], reverse=True)
        
        # Apply limit
        if limit > 0:
            user_notifications = user_notifications[:limit]
        
        return user_notifications

    async def subscribe_to_notifications(self, user_id: str, websocket: WebSocket) -> bool:
        """Subscribe a WebSocket client to receive notifications for a user"""
        async with self._notification_lock:
            self.notification_subscribers[user_id].add(websocket)
            logging.info(f"WebSocket client subscribed to notifications for user {user_id}")
            # Check connection health immediately
            try:
                await websocket.send_json({"type": "subscription_confirmed", "timestamp": datetime.now().timestamp()})
                return True
            except Exception as e:
                logging.warning(f"Could not confirm subscription for user {user_id}: {e}")
                # Remove the subscription if we couldn't confirm it
                if websocket in self.notification_subscribers[user_id]:
                    self.notification_subscribers[user_id].remove(websocket)
                return False

    async def unsubscribe_from_notifications(self, user_id: str, websocket: WebSocket) -> bool:
        """Unsubscribe a WebSocket client from receiving notifications"""
        async with self._notification_lock:
            if user_id in self.notification_subscribers and websocket in self.notification_subscribers[user_id]:
                self.notification_subscribers[user_id].remove(websocket)
                if not self.notification_subscribers[user_id]:
                    del self.notification_subscribers[user_id]
                logging.info(f"WebSocket client unsubscribed from notifications for user {user_id}")
                return True
        return False

    async def _stop_stream(self, stream_id):
        """Stop a stream and clean up resources"""
        stream_info = self.active_streams.get(stream_id)
        if not stream_info:
            return
            
        try:
            # Get stream info for notification before stopping
            result = execute_db_query(
                """
                SELECT vs.user_id, vs.name FROM video_stream vs
                WHERE vs.stream_id = %s
                """, 
                (stream_id,),
                fetch_one=True
            )
            
            if result:
                user_id = result[0]
                camera_name = result[1]
                
                # Add notification
                await self.add_notification(
                    user_id=user_id,
                    stream_id=stream_id,
                    camera_name=camera_name,
                    status="inactive",
                    message=f"Camera {camera_name} turned off"
                )
            
            # Stop the stream
            stream_info['stop_event'].set()
            
            # Cancel the task if it exists and is not done
            if 'task' in stream_info and not stream_info['task'].done():
                stream_info['task'].cancel()
                try:
                    await stream_info['task']
                except asyncio.CancelledError:
                    pass
                
            # Remove from active streams
            async with self._lock:
                self.active_streams.pop(stream_id, None)
                
            logging.info(f"Stream {stream_id} stopped successfully")
            
        except Exception as e:
            logging.error(f"Error stopping stream {stream_id}: {e}")

    async def start_stream_background(self, stream_id, user_id, username, camera_name, source):
        """Start a stream in the background independent of WebSocket connections"""
        async with self._lock:
            # Check if stream is already active
            if stream_id in self.active_streams:
                logging.info(f"Stream {stream_id} already active, not starting again")
                return

            stop_event = threading.Event()
            
            # Update stream status in database
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'processing'
                WHERE stream_id = %s
                """, 
                (stream_id,)
            )
            
            # Initialize statistics
            self.stream_processing_stats[stream_id] = {
                "frames_processed": 0,
                "detection_count": 0,
                "avg_processing_time": 0,
                "last_updated": datetime.now()
            }

            # Store stream information
            self.active_streams[stream_id] = {
                'source': source,
                'stop_event': stop_event,
                'camera_name': camera_name,
                'username': username,
                'user_id': user_id,
                'clients': set(), 
                'latest_frame': None,
                'last_frame_time': datetime.now(),
                'task': asyncio.create_task(self._process_stream(stream_id, camera_name, source, username, user_id, stop_event))
            }
        
        # Ensure user collection exists
        asyncio.create_task(self.ensure_user_collection_exists(username))

        # Log stream startup
        execute_db_query(
            """
            INSERT INTO logs (user_id, action_type, status, content)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, 'stream_start', 'success', f"Started stream {camera_name} (ID: {stream_id})")
        )
        
        logging.info(f"Started background stream {stream_id} for {username}")

    async def _process_stream(self, camera_id, camera_name, source, username, user_id, stop_event):
        """Process stream in the background with optimized frame handling"""
        cap = None
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        frame_count = 0
        last_db_update = datetime.now()
        db_update_interval = 3.0  # seconds
        
        try:
            # Get stream parameters
            params = await self.get_stream_parameters(user_id, camera_id)
            frame_skip = params["frame_skip"]
            frame_delay = params["frame_delay"]
            conf_threshold = params["conf_threshold"]
            
            # Open video capture with optimized settings
            cap = cv2.VideoCapture(source)#, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                for i in range(max_reconnect_attempts):
                    logging.warning(f"Retrying connection to {source}, attempt {i+1}/{max_reconnect_attempts}")
                    await asyncio.sleep(1.0)
                    cap = cv2.VideoCapture(source)#, cv2.CAP_FFMPEG)
                    if cap.isOpened():
                        break
                
                if not cap.isOpened():
                    raise RuntimeError(f"Could not open video source: {source}")
            
            # Optimize capture settings
            cap.set(cv2.CAP_PROP_BUFFERSIZE, self.frame_buffer_size)
            cap.set(cv2.CAP_PROP_FPS, 20)
            
            # Update stream status to active
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'active'
                WHERE stream_id = %s
                """, 
                (camera_id,)
            )
            
            # Stream processing loop
            last_time = datetime.now()
            adaptive_skip = frame_skip
            last_skip_adjustment = datetime.now()
            
            while not stop_event.is_set():
                # Read frame using a separate thread to avoid blocking
                loop = asyncio.get_event_loop()
                ret, frame = await loop.run_in_executor(thread_pool, lambda: cap.read())
                
                if not ret:
                    reconnect_attempts += 1
                    logging.warning(f"Failed to read frame from {source}, attempt {reconnect_attempts}")
                    
                    if reconnect_attempts > max_reconnect_attempts:
                        raise RuntimeError(f"Could not reconnect to {source} after {max_reconnect_attempts} attempts")
                    
                    # Try to reconnect
                    await asyncio.sleep(1.0)
                    if cap:
                        cap.release()
                    cap = cv2.VideoCapture(source)#, cv2.CAP_FFMPEG)
                    continue
                
                # Reset reconnect attempts on successful frame
                reconnect_attempts = 0
                frame_count += 1
                
                # Dynamically adjust frame skip based on load
                if (datetime.now() - last_skip_adjustment).total_seconds() > 10:
                    stream_info = self.active_streams.get(camera_id, {})
                    client_count = len(stream_info.get('clients', set()))
                    processing_time = (datetime.now() - last_time).total_seconds()
                    
                    # Adjust skip rate based on client count and processing time
                    if client_count > 10 or processing_time > 0.1:
                        adaptive_skip = max(frame_skip, 5)
                    elif client_count > 5 or processing_time > 0.05:
                        adaptive_skip = max(frame_skip, 3)
                    else:
                        adaptive_skip = frame_skip
                    
                    last_skip_adjustment = datetime.now()
                
                # Apply frame skip
                if adaptive_skip > 0 and frame_count % (adaptive_skip + 1) != 0:
                    continue
                
                # Process frame in thread pool to avoid blocking the event loop
                start_time = datetime.now()
                processed_frame, person_count = await loop.run_in_executor(
                    thread_pool, 
                    lambda: self.detect_objects(frame, conf_threshold)
                )
                
                # Update stats
                elapsed = (datetime.now() - start_time).total_seconds()
                stats = self.stream_processing_stats.get(camera_id, {})
                if stats:
                    stats["frames_processed"] = stats.get("frames_processed", 0) + 1
                    if person_count > 0:
                        stats["detection_count"] = stats.get("detection_count", 0) + 1
                    
                    # Update average processing time with weighted average
                    prev_avg = stats.get("avg_processing_time", 0)
                    stats["avg_processing_time"] = (prev_avg * 0.9) + (elapsed * 0.1)
                    stats["last_updated"] = datetime.now()
                
                # Update the latest frame for clients
                stream_info = self.active_streams.get(camera_id)
                if stream_info:
                    stream_info['latest_frame'] = processed_frame
                    stream_info['last_frame_time'] = datetime.now()
                
                # Add to detection database if people detected
                if person_count > 0:
                    self.insert_detection_data(
                        username=username,
                        camera_id=camera_id,
                        camera_name=camera_name,
                        count=person_count,
                        frame=processed_frame
                    )
                
                # Update database status periodically
                if (datetime.now() - last_db_update).total_seconds() > db_update_interval:
                    execute_db_query(
                        """
                        UPDATE video_stream
                        SET last_activity = NOW()
                        WHERE stream_id = %s
                        """,
                        (camera_id,)
                    )
                    last_db_update = datetime.now()
                
                # Log performance occasionally
                if frame_count % 500 == 0:
                    fps = 1.0 / max(0.001, elapsed)
                    logging.info(f"Stream {camera_id}: {fps:.2f} FPS, skip={adaptive_skip}, clients={len(stream_info.get('clients', set()))}")
                
                # Controlled delay
                now = datetime.now()
                processing_time = (now - last_time).total_seconds()
                last_time = now
                
                if frame_delay > 0:
                    actual_delay = max(0, frame_delay - processing_time)
                    if actual_delay > 0:
                        await asyncio.sleep(actual_delay)
        
        except Exception as e:
            logging.error(f"Stream processing error: {e}", exc_info=True)
            # Update stream status
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'error', is_streaming = FALSE
                WHERE stream_id = %s
                """, 
                (camera_id,)
            )
            
            # Add notification
            await self.add_notification(
                user_id=user_id,
                stream_id=camera_id,
                camera_name=camera_name,
                status="error",
                message=f"Camera error: {str(e)[:100]}"
            )
        
        finally:
            # Clean up resources
            if cap:
                try:
                    cap.release()
                except Exception:
                    pass
                cap = None # Add this line
                logging.debug(f"Released and dereferenced VideoCapture object for stream {camera_id}")
            
            # Update stream status
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'inactive', is_streaming = FALSE
                WHERE stream_id = %s
                """, 
                (camera_id,)
            )
            
            # Clean up memory
            async with self._lock:
                self.active_streams.pop(camera_id, None)
                self.stream_processing_stats.pop(camera_id, None)

    def detect_objects(self, frame: np.ndarray, conf_threshold: float = 0.4) -> tuple[np.ndarray, int]:
        """Run YOLOv8 detection on a frame"""
        try:
            # Check if the frame is empty
            if frame is None or frame.size == 0:
                return np.zeros((480, 640, 3), dtype=np.uint8), 0
                
            # Resize to standard size for consistent performance
            height, width = frame.shape[:2]
            if height > 480 or width > 640:
                input_frame = cv2.resize(frame, (640, 480))
            else:
                input_frame = frame.copy()

            # Run YOLO model - only detect class 0 (person)
            results = self.yolo_model(input_frame, conf=conf_threshold, classes=[0])

            # Get total count of people
            # person_count = len(results[0].boxes)
            person_count = 0

            # Create a copy for drawing annotations
            annotated_frame = input_frame.copy()

            # Draw bounding boxes for each detected person
            # if results and results[0] and hasattr(results[0], 'boxes') and results[0].boxes is not None:

            for i, box in enumerate(results[0].boxes):

                # if not (hasattr(box, 'conf') and hasattr(box, 'cls') and hasattr(box, 'xyxy')):
                #         logging.warning(f"Box object at index {i} is missing attributes.")
                #         continue
                # if not box.conf or not box.cls or not box.xyxy:
                #     logging.warning(f"Box object at index {i} has empty attributes.")
                #     continue

                # Get confidence
                conf = float(box.conf[0])
                cls_lable = int(box.cls[0])
                
                # Only count person class (0)
                if cls_lable == 0:  # Person class
                    person_count += 1
                    # Get coordinates
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    
                    # Draw rectangle
                    cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    label = f"Person #{i+1}: {conf:.2f}"
                    cv2.putText(annotated_frame, label, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
                    
            text = f"Total People: {person_count}"
            cv2.putText(annotated_frame, text, (40, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

            # # Draw total count with more visible styling
            # # Background rectangle for better visibility
            # text = f"Total People: {person_count}"
            # (text_width, text_height), _ = cv2.getTextSize(
            #     text, cv2.FONT_HERSHEY_SIMPLEX, 1, 2)
            
            # # Ensure coordinates are within the frame dimensions
            # # For a 640x480 frame, (5,5) to (15+text_width, 35) should be fine
            # bg_x1, bg_y1 = 5, 5
            # # bg_x2, bg_y2 = min(input_frame.shape[1] -1, 15 + text_width), min(input_frame.shape[0] -1, 35) # Ensure it doesn't go out of bounds
            # # Ensure text and background don't go out of frame bounds
            # bg_x2 = min(annotated_frame.shape[1] - 1, bg_x1 + text_width + 10) # +10 for padding
            # bg_y2 = min(annotated_frame.shape[0] - 1, bg_y1 + text_height + 10) # +10 for padding

            # cv2.rectangle(annotated_frame, (bg_x1, bg_y1), (bg_x2, bg_y2), (0, 0, 0), -1) # Black background
            # # cv2.rectangle(annotated_frame, (5, 5), (15 + text_width, 35), (0, 0, 0), -1)

            # text_x = bg_x1 + 5
            # text_y = bg_y1 + text_height + 5 # Adjusted for baseline
            # cv2.putText(annotated_frame, text, (text_x, text_y), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            # cv2.putText(annotated_frame, text, (40, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 0), 2)

            return annotated_frame, person_count

        except Exception as e:
            logging.error(f"Object detection error: {e}")
            return frame.copy(), 0

    def insert_detection_data(self, username: str, camera_id: str, camera_name: str, count: int, frame: np.ndarray):
        """Insert detection data into Qdrant"""
        try:
            # Skip if count is 0 to reduce database operations
            if count == 0:
                return

            # Get the user-specific collection name
            collection_name = self.get_user_collection_name(username)

            now = datetime.now()
            point = models.PointStruct(
                id=str(uuid.uuid4()),
                vector=[0.0],
                payload={
                    "camera_id": camera_id,
                    "name": camera_name,
                    "timestamp": now.timestamp(),
                    "date": now.strftime("%Y-%m-%d"),
                    "time": now.strftime("%H:%M:%S"),
                    "person_count": count,
                    "frame": frame_to_base64(frame),
                    "username": username 
                }
            )
            
            # Add to batch queue
            batch_key = f"{username}_{collection_name}"

            # # # Use a lock to avoid race conditions in the batch processing queue
            # asyncio.create_task(self._add_to_batch_queue(batch_key, point, now))
            
            self.qdrant_client.upsert(
                collection_name=collection_name,
                points=[point]
            )
        except Exception as e:
            logging.error(f"Error inserting detection data: {e}")

    def get_user_collection_name(self, username: str) -> str:
        """Generate a user-specific collection name"""
        return f"{self.base_collection_name}_{username}"
    
    async def ensure_user_collection_exists(self, username: str) -> str:
        """Ensure that a collection exists for the user"""
        # Check cache first
        if username in self.user_collections_cache:
            return self.user_collections_cache[username]

        collection_name = self.get_user_collection_name(username)
        
        try:
            # Check if collection exists
            collections = self.qdrant_client.get_collections().collections
            collection_names = [collection.name for collection in collections]
            
            if collection_name not in collection_names:
                # Create collection if it doesn't exist
                self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=models.VectorParams(
                        size=1,  # Using minimal vector size since we're primarily using payload
                        distance=models.Distance.COSINE
                    )
                )

                # # Create collection using thread pool to avoid blocking
                # loop = asyncio.get_event_loop()
                # await loop.run_in_executor(
                #     thread_pool,
                #     lambda: self.qdrant_client.create_collection(
                #         collection_name=collection_name,
                #         vectors_config=models.VectorParams(
                #             size=1,
                #             distance=models.Distance.COSINE
                #         )
                #     )
                # )
                logging.info(f"Created new collection: {collection_name} for user: {username}")
        except Exception as e:
            logging.error(f"Error ensuring collection exists: {e}")
            # Create a default collection if there was an error
            try:
                self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=models.VectorParams(
                        size=1,
                        distance=models.Distance.COSINE
                    )
                )
                # # Create collection using thread pool to avoid blocking
                # loop = asyncio.get_event_loop()
                # await loop.run_in_executor(
                #     thread_pool,
                #     lambda: self.qdrant_client.create_collection(
                #         collection_name=collection_name,
                #         vectors_config=models.VectorParams(
                #             size=1,
                #             distance=models.Distance.COSINE
                #         )
                #     )
                # )
            except Exception:
                pass

        # Cache the result
        self.user_collections_cache[username] = collection_name
        return collection_name

    async def start_stream(self, stream_id: str, user_id: str):
        """Start a specific stream for a user based on stream_id"""
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id must be provided")
        
        if not stream_id:
            raise HTTPException(status_code=400, detail="Stream ID must be provided")
        
        # Verify user subscription status
        user_status = execute_db_query(
            """
            SELECT is_active, is_subscribed, count_of_camera
            FROM users
            WHERE user_id = %s
            """,
            (user_id,),
            fetch_one=True
        )
        
        if not user_status or not user_status[0] or not user_status[1]:
            raise HTTPException(status_code=403, detail="User account is inactive or subscription has expired")
        
        # Check if user has reached their camera limit
        active_streams_count = execute_db_query(
            """
            SELECT COUNT(*)
            FROM video_stream
            WHERE user_id = %s AND is_streaming = TRUE
            """,
            (user_id,),
            fetch_one=True
        )[0]
        
        if active_streams_count >= user_status[2]:  # user_status[2] is count_of_camera
            raise HTTPException(status_code=403, detail=f"Camera limit reached ({active_streams_count}/{user_status[2]})")
        
        # Get the stream details and verify ownership
        stream = await self.get_stream_by_id(stream_id, user_id)
        
        # Update the stream status in the database to indicate it should be running
        try:
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'processing', is_streaming = TRUE
                WHERE stream_id = %s
                """, 
                (stream_id,)
            )
            logging.info(f"Updated stream {stream_id} status to processing and is_streaming to TRUE")
            
            # Log stream start request
            execute_db_query(
                """
                INSERT INTO logs (user_id, action_type, status, content)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, 'stream_request', 'success', f"Requested start of stream {stream['name']} (ID: {stream_id})")
            )
        except Exception as e:
            logging.error(f"Error updating stream status: {e}")
            raise HTTPException(status_code=500, detail="Failed to update stream status")
            
        # The background task will pick up this stream and start it
        return stream

    async def stop_all_streams(self, username: str):
        """Stop all streams for a specific user"""
        try:
            # Get user_id from username
            user_result = execute_db_query(
                "SELECT user_id FROM users WHERE username = %s", 
                (username,),
                fetch_one=True
            )
            
            if not user_result:
                raise HTTPException(status_code=404, detail="User not found")
            
            user_id = user_result[0]
            
            # Get all active streams for this user
            streams = execute_db_query(
                """
                SELECT stream_id 
                FROM video_stream 
                WHERE user_id = %s AND is_streaming = TRUE
                """, 
                (user_id,),
                fetch_all=True
            )
            
            # Update all streams to inactive and not streaming
            execute_db_query(
                """
                UPDATE video_stream 
                SET status = 'inactive', is_streaming = FALSE
                WHERE user_id = %s AND is_streaming = TRUE
                """, 
                (user_id,)
            )
            
            # The number of streams that were stopped
            streams_count = len(streams)
            
            # Log stream stop action
            if streams_count > 0:
                execute_db_query(
                    """
                    INSERT INTO logs (user_id, action_type, status, content)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user_id, 'stop_all_streams', 'success', f"Stopped {streams_count} streams")
                )
            
            logging.info(f"Marked {streams_count} streams as inactive for user {username}")
            
            # The background task will handle actual stopping of the streams
            return {
                "status": "success", 
                "message": f"Stopping {streams_count} streams for user {username}",
                "stopped_streams": streams_count
            }
        except Exception as e:
            logging.error(f"Error stopping all streams: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to stop streams: {str(e)}")
            
    async def start_all_streams(self, username: str):
        """Start all streams for a specific user"""
        try:
            # Get user_id from username
            user_result = execute_db_query(
                "SELECT user_id, is_active, is_subscribed, count_of_camera FROM users WHERE username = %s", 
                (username,),
                fetch_one=True
            )
            
            if not user_result:
                raise HTTPException(status_code=404, detail="User not found")
            
            user_id = user_result[0]
            is_active = user_result[1]
            is_subscribed = user_result[2]
            max_cameras = user_result[3]

            if not is_active or not is_subscribed:
                raise HTTPException(status_code=403, detail="User account is inactive or subscription has expired")
            
            # Get count of already active streams
            active_count = execute_db_query(
                """
                SELECT COUNT(*) 
                FROM video_stream 
                WHERE user_id = %s AND is_streaming = TRUE
                """, 
                (user_id,),
                fetch_one=True
            )[0]
            
            remaining_slots = max_cameras - active_count
            
            if remaining_slots <= 0:
                raise HTTPException(status_code=403, detail=f"Camera limit reached ({active_count}/{max_cameras})")    
            
            # Get inactive streams up to the remaining limit
            streams = execute_db_query(
                """
                SELECT stream_id, name, path 
                FROM video_stream 
                WHERE user_id = %s AND is_streaming = FALSE
                LIMIT %s
                """, 
                (user_id, remaining_slots),
                fetch_all=True
            )
            
            # Mark streams as active in the database
            started_streams = []
            
            for stream in streams:
                stream_id = stream[0]
                name = stream[1]
                path = stream[2]
                
                # Update stream status to active and streaming
                execute_db_query(
                    """
                    UPDATE video_stream 
                    SET status = 'processing', is_streaming = TRUE
                    WHERE stream_id = %s
                    """, 
                    (stream_id,)
                )
                
                # Create a collection for this user if it doesn't exist
                await self.ensure_user_collection_exists(username)
                
                # Add to our list of successfully started streams
                started_streams.append({
                    "id": str(stream_id), 
                    "name": name, 
                    "path": path
                })
                
                # Log stream start request
                execute_db_query(
                    """
                    INSERT INTO logs (user_id, action_type, status, content)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user_id, 'stream_request', 'success', f"Requested start of stream {name} (ID: {stream_id})")
                )
                
                logging.info(f"Started stream {name} (ID: {stream_id}) for user {username}")
                
            streams_count = len(started_streams)

            # The background task will handle actual starting of the streams
            return {
                "status": "success", 
                "message": f"Starting {streams_count} streams for user {username}",
                "started_streams": streams_count,
                "streams": started_streams
            }

        except HTTPException as e:
            # Re-raise HTTP exceptions
            raise

        except Exception as e:
            logging.error(f"Error starting all streams: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to start streams: {str(e)}")


# Global stream manager
stream_manager = StreamManager()

# This function must be called during application startup
async def initialize_stream_manager():
    """Initialize the stream manager's background tasks - call this at app startup"""
    try:
        await stream_manager._start_background_tasks()
        logging.info("Stream manager initialized")

    except Exception as e:
        logging.error(f"Failed to create background task: {e}", exc_info=True)
        # Try to restart after delay
        await stream_manager._restart_background_task()

# Helper function to keep WebSocket connection alive
async def send_ping(websocket: WebSocket):
    """Send periodic pings to keep the WebSocket connection alive"""
    try:
        while True:
            await asyncio.sleep(30)  # Send ping every 30 seconds
            await websocket.send_json({"type": "ping", "timestamp": datetime.now().timestamp()})
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as e:
        logging.error(f"Error in ping task: {e}")

#########################################

@router.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    stream_id = None
    user_id = None
    try:
        await websocket.accept()
        # Extract stream_id from query parameters
        query_params = dict(websocket.query_params)
        stream_id = query_params.get("stream_id")
        
        if not stream_id:
            logging.error(f"no stream_id provided in WebSocket connection")
            await websocket.send_json({"status": "error", "message": "No stream_id provided"})
            await websocket.close(code=1008)  # Policy violation
            return

        # Authenticate user with better error handling
        try:
            token = await session_manager.get_token_from_websocket(websocket)
                
            if not token:
                logging.error("No authentication token provided in WebSocket connection")
                await websocket.send_json({"status": "error", "message": "No authentication token provided"})
                await websocket.close(code=1008)
                return

            token_data = session_manager.verify_token(token, "access")
            if not token_data or session_manager.is_token_blacklisted(token):
                logging.error(f"Invalid or blacklisted token in WebSocket connection for stream {stream_id}")
                await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
                await websocket.close(code=1008)
                return
            
            if session_manager.is_token_blacklisted(token):
                    await websocket.send_json({"status": "error", "message": "Token has been revoked"})
                    await websocket.close(code=1008)
                    return

            user_id = token_data.user_id

        except Exception as auth_error:
            logging.error(f"Authentication error: {auth_error}")
            await websocket.send_json({"status": "error", "message": "Authentication failed"})
            await websocket.close(code=1008)
            return

        # Check if the stream exists and belongs to the user
        stream_data = execute_db_query(
            """
            SELECT vs.stream_id, vs.is_streaming, vs.name, u.username 
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = %s AND vs.user_id = %s
            """, 
            (stream_id, user_id),
            fetch_one=True
        )
        
        if not stream_data:
            logging.error(f"Stream {stream_id} not found or access denied for user {user_id}")
            await websocket.send_json({"status": "error", "message": "Stream not found or access denied"})
            await websocket.close(code=1008)
            return
        
        stream_name = stream_data[2]
        username = stream_data[3]

        # If stream is not already running, try to start it
        if not stream_data[1]:  # is_streaming is FALSE
            try:
                logging.info(f"Starting stream {stream_id} ({stream_name}) for user {username}")
                await stream_manager.start_stream(stream_id, user_id)
                # Wait for stream to initialize
                for attempt in range(1, 6):  # 5 attempts
                    logging.info(f"Waiting for stream {stream_id} to initialize (attempt {attempt}/5)")
                    await asyncio.sleep(1)
                    # Check if stream is now active
                    if stream_id in stream_manager.active_streams:
                        break
                else:
                    logging.error(f"Stream {stream_id} failed to initialize after multiple attempts")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize"})
                    await websocket.close(code=1011)  # Internal error
                    return
            except HTTPException as e:
                logging.error(f"Failed to start stream {stream_id}: {e.detail}")
                await websocket.send_json({"status": "error", "message": e.detail})
                await websocket.close(code=1011)
                return
        
        # More aggressive retry for connecting to the stream
        connected = False
        for attempt in range(1, 6):  # 5 attempts
            connected = await stream_manager.connect_client_to_stream(stream_id, websocket)
            if connected:
                break
            logging.warning(f"Connection attempt {attempt}/5 failed for stream {stream_id}")
            await asyncio.sleep(1)

        if not connected:
            logging.error(f"Failed to connect to stream {stream_id} after multiple attempts")
            await websocket.send_json({"status": "error", "message": "Failed to connect to stream after multiple attempts"})
            await websocket.close(code=1011)
            return

        # Send success message to client
        await websocket.send_json({
            "status": "connected", 
            "message": f"Connected to stream: {stream_name}",
            "stream_id": stream_id
        })
        
        logging.info(f"Client connected to stream {stream_id} ({stream_name}) for user {username}")
        
        # Keep connection alive with ping-pong
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Send frames to this client in a loop
        try:
            while True:
                # Check if stream is still active
                if stream_id not in stream_manager.active_streams:
                    logging.info(f"Stream {stream_id} no longer active, closing connection")
                    break
                    
                # Get latest frame
                stream_info = stream_manager.active_streams.get(stream_id, {})
                latest_frame = stream_info.get('latest_frame')
                
                if latest_frame is not None:
                    # Convert frame to base64
                    frame_base64 = frame_to_base64(latest_frame)
                    
                    # Send frame with metadata
                    frame_data = {
                        "stream_id": stream_id,
                        "frame": frame_base64,
                        "timestamp": datetime.now().timestamp()
                    }
                    
                    await websocket.send_json(frame_data)
                
                # Control frame rate to client
                await asyncio.sleep(0.1)
        
        finally:
            # Clean up ping task
            if 'ping_task' in locals() and not ping_task.done():
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    except WebSocketDisconnect:
        logging.info(f"WebSocket client disconnected from stream {stream_id}")
    
    except Exception as e:
        logging.error(f"WebSocket stream error: {e}", exc_info=True)
        try:
            await websocket.send_json({"status": "error", "message": "Internal server error"})
        except:
            pass

@router.post("/stop_stream/{stream_id}")
async def stop_specific_stream(stream_id: str, username: str = Depends(session_manager.get_current_user)):
    """Stop a specific stream for a user"""
    try:
        # Get user_id from username
        user_result = execute_db_query(
            "SELECT user_id FROM users WHERE username = %s", 
            (username,),
            fetch_one=True
        )
        
        if not user_result:
            return {"status": "error", "message": "User not found"}, 404
            
        user_id = user_result[0]
        
        # Get camera name before stopping
        camera_info = execute_db_query(
            "SELECT name FROM video_stream WHERE stream_id = %s",
            (stream_id,),
            fetch_one=True
        )
        
        camera_name = camera_info[0] if camera_info else "Unknown camera"

        # First update the database to indicate the stream should stop
        # Update the stream status to 'inactive' and is_streaming to FALSE
        execute_db_query(
            """
            UPDATE video_stream 
            SET status = 'inactive', is_streaming = FALSE
            WHERE stream_id = %s
            """, 
            (stream_id,)
        )
        logging.info(f"Updated stream {stream_id} status to inactive and is_streaming to FALSE")
        
        # Add notification about camera turning off
        await stream_manager.add_notification(
            user_id=user_id,
            stream_id=stream_id,
            camera_name=camera_name,
            status="inactive",
            message=f"Camera {camera_name} turned off manually"
        )

        # The background task will handle actual stopping of the stream
        return {"status": "success", "message": f"Stream {stream_id} stopping"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})
        
@router.post("/stop_all_streams")
async def stop_all_user_streams(username: str = Depends(session_manager.get_current_user)):
    """Stop all active streams for the current user"""
    try:
        result = await stream_manager.stop_all_streams(username)
        return JSONResponse(content=result)
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Unexpected error stopping all streams: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@router.post("/start_all_streams")
async def start_all_user_streams(username: str = Depends(session_manager.get_current_user)):
    """Start all inactive streams for the current user"""
    try:
        result = await stream_manager.start_all_streams(username)
        return JSONResponse(content=result)
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Unexpected error starting all streams: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

#########################################

@router.get("/collections/")
async def get_user_collections(username: str = Depends(session_manager.get_current_user)):
    """Get all collections for the current user"""
    try:
        # Get all collections
        collections = stream_manager.qdrant_client.get_collections().collections
        
        # Filter to only include user's collections
        user_prefix = f"{stream_manager.base_collection_name}_{username}"
        user_collections = [
            {
                "name": collection.name,
                "vectors_count": collection.vectors_count
            }
            for collection in collections 
            if collection.name.startswith(user_prefix)
        ]
        
        return JSONResponse({
            "status": "success",
            "collections": user_collections
        })
    except Exception as e:
        logging.error(f"Error getting collections: {e}")
        return JSONResponse({
            "status": "error",
            "message": str(e)
        }, status_code=500)

@router.delete("/collections/{collection_name}")
async def delete_collection(collection_name: str, username: str = Depends(session_manager.get_current_user)):
    """Delete a specific collection (with safety checks)"""
    try:
        # Security check - ensure user can only delete their own collections
        user_prefix = f"{stream_manager.base_collection_name}_{username}"
        if not collection_name.startswith(user_prefix):
            return JSONResponse({
                "status": "error",
                "message": "Access denied: You can only delete your own collections"
            }, status_code=403)
            
        # Delete the collection
        stream_manager.qdrant_client.delete_collection(collection_name=collection_name)
        
        return JSONResponse({
            "status": "success",
            "message": f"Collection {collection_name} deleted successfully"
        })
    except Exception as e:
        logging.error(f"Error deleting collection: {e}")
        return JSONResponse({
            "status": "error",
            "message": str(e)
        }, status_code=500)

@router.post("/collections/reset")
async def reset_user_collections(username: str = Depends(session_manager.get_current_user)):
    """Reset all collections for the current user"""
    try:
        # Get all collections
        collections = stream_manager.qdrant_client.get_collections().collections
        
        # Filter to only include user's collections
        user_prefix = f"{stream_manager.base_collection_name}_{username}"
        user_collections = [
            collection.name
            for collection in collections 
            if collection.name.startswith(user_prefix)
        ]
        
        # Delete each collection
        for collection_name in user_collections:
            stream_manager.qdrant_client.delete_collection(collection_name=collection_name)
        
        # Create a fresh collection
        await stream_manager.ensure_user_collection_exists(username)
        
        return JSONResponse({
            "status": "success",
            "message": f"Reset {len(user_collections)} collections for user {username}"
        })
    except Exception as e:
        logging.error(f"Error resetting collections: {e}")
        return JSONResponse({
            "status": "error",
            "message": str(e)
        }, status_code=500)

#########################################

# Add new endpoint to get notifications
@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
    """
    WebSocket endpoint for real-time notifications.
    Sends notifications to the client as they occur.
    """
    user_id = None
    username = None
    ping_task = None
    notification_task = None
    
    try:
        await websocket.accept()
        
        # Authenticate user with better error handling
        try:
            token = await session_manager.get_token_from_websocket(websocket)
                
            if not token:
                logging.error("No authentication token provided in WebSocket notification connection")
                await websocket.send_json({"status": "error", "message": "No authentication token provided"})
                await websocket.close(code=1008)
                return

            token_data = session_manager.verify_token(token, "access")
            if not token_data or session_manager.is_token_blacklisted(token):
                logging.error(f"Invalid or blacklisted token in WebSocket notification connection")
                await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
                await websocket.close(code=1008)
                return
            
            user_id = token_data.user_id
            username = token_data.username if hasattr(token_data, 'username') else None
            
            if not username:
                # Fetch username if not in token
                user_data = await user_manager.get_user_by_id(user_id)
                username = user_data.get("username") if user_data else None
                
            if not username:
                await websocket.send_json({"status": "error", "message": "User information not found"})
                await websocket.close(code=1008)
                return

        except Exception as auth_error:
            logging.error(f"Authentication error in notification WebSocket: {auth_error}")
            await websocket.send_json({"status": "error", "message": "Authentication failed"})
            await websocket.close(code=1008)
            return

        # Send initial connection confirmation
        await websocket.send_json({
            "status": "connected", 
            "message": "Connected to notification stream",
            "server_time": datetime.now().timestamp()
        })
        
        # Get initial notifications (last 24 hours)
        since = (datetime.now() - timedelta(hours=24)).timestamp()
        initial_notifications = stream_manager.get_notifications(user_id, since)
        
        # Send initial batch of notifications
        if initial_notifications:
            await websocket.send_json({
                "type": "notifications",
                "count": len(initial_notifications),
                "notifications": initial_notifications,
                "server_time": datetime.now().timestamp()
            })
        
        logging.info(f"Client connected to notification stream for user {username}")
        
        # # Keep track of the last notification timestamp for this user
        # last_notification_time = datetime.now().timestamp()
        
        # # Keep connection alive with ping-pong
        # ping_task = asyncio.create_task(send_ping(websocket))
        
        # # Start notification check loop
        # async def check_notifications():
        #     nonlocal last_notification_time
        #     try:
        #         while True:
        #             # Get any new notifications
        #             new_notifications = stream_manager.get_notifications(user_id, last_notification_time)
                    
        #             if new_notifications:
        #                 # Update last notification time
        #                 last_notification_time = max(n["timestamp"] for n in new_notifications)
                        
        #                 # Send new notifications
        #                 await websocket.send_json({
        #                     "type": "notifications",
        #                     "count": len(new_notifications),
        #                     "notifications": new_notifications,
        #                     "server_time": datetime.now().timestamp()
        #                 })
                        
        #                 logging.debug(f"Sent {len(new_notifications)} new notifications to user {username}")
                    
        #             # Check for new notifications every 2 seconds
        #             await asyncio.sleep(2)
        #     except asyncio.CancelledError:
        #         logging.info(f"Notification check task cancelled for user {username}")
        #         raise
        #     except Exception as e:
        #         logging.error(f"Error in notification check task: {e}", exc_info=True)
        #         raise
        
        # notification_task = asyncio.create_task(check_notifications())
        
        # Subscribe this websocket to receive notifications
        await stream_manager.subscribe_to_notifications(user_id, websocket)
        
        # Keep connection alive with ping-pong
        ping_task = asyncio.create_task(send_ping(websocket))

        # Wait for client messages or disconnect
        while True:
            # Use receive_json with a timeout to detect disconnection
            try:
                # Set a timeout for receiving messages
                message = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                
                # Handle client messages if needed
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong", "timestamp": datetime.now().timestamp()})

                elif message.get("type") == "acknowledgeNotification":
                    # Handle notification acknowledgement if needed
                    notification_id = message.get("notificationId")
                    if notification_id:
                        logging.debug(f"Client acknowledged notification {notification_id}")
            
            except asyncio.TimeoutError:
                # This is expected, just continue the loop
                continue
            except WebSocketDisconnect:
                logging.info(f"WebSocket notification client disconnected for user {username}")
                break
            except Exception as e:
                logging.error(f"Error receiving WebSocket message: {e}")
                break
    
    except WebSocketDisconnect:
        logging.info(f"WebSocket notification client disconnected")
    
    except Exception as e:
        logging.error(f"WebSocket notification error: {e}", exc_info=True)
        try:
            await websocket.send_json({"status": "error", "message": "Internal server error"})
        except:
            pass
    
    finally:
        # Unsubscribe from notifications
        if user_id:
            await stream_manager.unsubscribe_from_notifications(user_id, websocket)

        # Clean up tasks
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
                
        # if notification_task and not notification_task.done():
        #     notification_task.cancel()
        #     try:
        #         await notification_task
        #     except asyncio.CancelledError:
        #         pass

        # Log disconnection if we have the username
        if username:
            logging.info(f"Notification WebSocket connection closed for user {username}")

@router.get("/notify")
async def get_notifications(
    since: Optional[float] = Query(None, description="Timestamp to get notifications from"),
    limit: int = Query(50, description="Maximum number of notifications to return"),
    include_read: bool = Query(True, description="Whether to include read notifications"),
    username: str = Depends(session_manager.get_current_user)
):
    """
    Get notifications about camera status changes.
    
    If 'since' parameter is provided, only return notifications after that timestamp.
    Otherwise, return the most recent notifications (limited to the past 24 hours).
    """
    try:

        if not username:
            raise HTTPException(status_code=401, detail="Authentication required")
        
        user_data = await user_manager.get_user_by_username(username)
        user_id = user_data["user_id"]

        # If no 'since' parameter, default to 24 hours ago
        if since is None:
            since = (datetime.now() - timedelta(hours=24)).timestamp()
            
        notifications = stream_manager.get_notifications(
            user_id=user_id, 
            since=since, 
            limit=limit, 
            include_read=include_read
        )
        
        return {
            "status": "success",
            "count": len(notifications),
            "notifications": notifications,
            "server_time": datetime.now().timestamp()
        }
        
    except Exception as e:
        logging.error(f"Error getting notifications: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}")

# Mark notifications as read
@router.post("/notify/{notification_id}/read")
async def mark_notification_read(
    notification_id: str,
    username: str = Depends(session_manager.get_current_user)
):
    """Mark a notification as read"""
    try:
        if not username:
            raise HTTPException(status_code=401, detail="Authentication required")
        
        user_data = await user_manager.get_user_by_username(username)
        user_id = user_data["user_id"]

        # Find and update the notification
        notification_updated = False
        
        async with stream_manager._notification_lock:
            for notification in stream_manager.notifications:
                if notification["id"] == notification_id and notification["user_id"] == user_id:
                    notification["read"] = True
                    notification_updated = True
                    break
        
        if notification_updated:
            # Update database if needed
            try:
                execute_db_query(
                    """
                    UPDATE notifications SET read = TRUE
                    WHERE notification_id = %s AND user_id = %s
                    """,
                    (notification_id, user_id)
                )
            except Exception as e:
                logging.error(f"Failed to update notification in database: {e}")
            
            return {"status": "success", "message": "Notification marked as read"}
        else:
            raise HTTPException(status_code=404, detail="Notification not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error marking notification as read: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to mark notification as read: {str(e)}")
