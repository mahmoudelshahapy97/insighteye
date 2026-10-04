# app/tasks/stream_tasks.py
"""
Celery tasks for batch stream operations.
Fixed to properly handle async code in sync Celery context.
"""
from app.celery_app import celery_app
from uuid import UUID
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def run_async(coro):
    """
    Helper to run async code in sync Celery context.
    Creates a new event loop if needed.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        return loop.run_until_complete(coro)
    finally:
        pass  # Don't close the loop


# ==================== TASK 1: Start All Workspace Streams ====================

@celery_app.task(
    bind=True,
    name='stream_tasks.start_all_workspace_streams',
    max_retries=3,
    default_retry_delay=10
)
def start_all_workspace_streams_task(
    self,
    workspace_id: str,
    user_id: str,
    username: str
) -> Dict[str, Any]:
    """Sync wrapper for async start_all operation."""
    
    async def _async_task():
        task_id = self.request.id
        logger.info(f"[Task {task_id}] Starting all streams for workspace {workspace_id}")
        
        try:
            from app.services.stream_service import stream_manager
            from app.services.database import db_manager
            
            ws_id = UUID(workspace_id)
            user_uuid = UUID(user_id)
            
            self.update_state(state='PROGRESS', meta={'current': 0, 'total': 0, 'status': 'Checking limits...'})
            
            limits = await stream_manager.get_workspace_stream_limits(ws_id)
            
            if limits['available_slots'] <= 0:
                return {
                    "success": False,
                    "message": f"Workspace at capacity ({limits['current_active']}/{limits['total_camera_limit']})",
                    "data": {"started_count": 0, "total_count": 0, "task_id": task_id, "limits": limits}
                }
            
            streams_query = """
                SELECT vs.stream_id, vs.name, vs.user_id
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.workspace_id = $1 AND vs.is_streaming = FALSE
                    AND u.is_active = TRUE AND (u.is_subscribed = TRUE OR u.role IN ('admin', 'superadmin'))
                ORDER BY vs.created_at ASC
            """
            
            db_streams = await db_manager.execute_query(streams_query, (ws_id,), fetch_all=True)
            
            if not db_streams:
                return {"success": True, "message": "No streams to start", "data": {"started_count": 0, "task_id": task_id}}
            
            total_streams = len(db_streams)
            started_count = 0
            failed_streams = []
            
            for idx, stream in enumerate(db_streams):
                self.update_state(state='PROGRESS', meta={
                    'current': idx + 1, 'total': total_streams,
                    'status': f'Processing {stream["name"]}...', 'started': started_count
                })
                
                if started_count >= limits['available_slots']:
                    break
                
                try:
                    await stream_manager.start_stream_in_workspace(
                        stream_id=stream['stream_id'],
                        requester_user_id=user_uuid
                    )
                    started_count += 1
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Failed to start {stream['stream_id']}: {e}")
                    failed_streams.append({'stream_id': str(stream['stream_id']), 'error': str(e)})
            
            return {
                "success": True,
                "message": f"Started {started_count}/{total_streams} streams",
                "data": {
                    "started_count": started_count,
                    "total_count": total_streams,
                    "failed_streams": failed_streams,
                    "task_id": task_id,
                    "completed_at": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
                }
            }
        except Exception as e:
            logger.error(f"[Task {self.request.id}] Failed: {e}", exc_info=True)
            raise self.retry(exc=e, countdown=10)
    
    return run_async(_async_task())


# ==================== TASK 2: Stop All Workspace Streams ====================

@celery_app.task(
    bind=True,
    name='stream_tasks.stop_all_workspace_streams',
    max_retries=3,
    default_retry_delay=10
)
def stop_all_workspace_streams_task(
    self,
    workspace_id: str,
    user_id: str,
    username: str
) -> Dict[str, Any]:
    """Sync wrapper for async stop_all operation."""
    
    async def _async_task():
        task_id = self.request.id
        logger.info(f"[Task {task_id}] Stopping all streams for workspace {workspace_id}")
        
        try:
            from app.services.stream_service import stream_manager
            from app.services.database import db_manager
            
            ws_id = UUID(workspace_id)
            user_uuid = UUID(user_id)
            
            self.update_state(state='PROGRESS', meta={'current': 0, 'total': 0, 'status': 'Finding streams...'})
            
            streams_query = """
                SELECT stream_id, name FROM video_stream
                WHERE workspace_id = $1 AND is_streaming = TRUE
            """
            
            db_streams = await db_manager.execute_query(streams_query, (ws_id,), fetch_all=True)
            
            if not db_streams:
                return {"success": True, "message": "No streams to stop", "data": {"stopped_count": 0, "task_id": task_id}}
            
            total_streams = len(db_streams)
            stopped_count = 0
            failed_streams = []
            
            for idx, stream in enumerate(db_streams):
                self.update_state(state='PROGRESS', meta={
                    'current': idx + 1, 'total': total_streams,
                    'status': f'Stopping {stream["name"]}...', 'stopped': stopped_count
                })
                
                try:
                    await stream_manager.stop_stream_in_workspace(
                        stream_id=stream['stream_id'],
                        requester_user_id=user_uuid,
                        stop_reason='user_action',
                        additional_context=f"Workspace stop (Task {task_id})"
                    )
                    stopped_count += 1
                except Exception as e:
                    logger.error(f"Failed to stop {stream['stream_id']}: {e}")
                    failed_streams.append({'stream_id': str(stream['stream_id']), 'error': str(e)})
            
            return {
                "success": True,
                "message": f"Stopped {stopped_count}/{total_streams} streams",
                "data": {
                    "stopped_count": stopped_count,
                    "total_count": total_streams,
                    "failed_streams": failed_streams,
                    "task_id": task_id,
                    "completed_at": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
                }
            }
        except Exception as e:
            logger.error(f"[Task {self.request.id}] Failed: {e}", exc_info=True)
            raise self.retry(exc=e, countdown=10)
    
    return run_async(_async_task())


# ==================== TASK 3: Batch Start Streams ====================

@celery_app.task(
    bind=True,
    name='stream_tasks.batch_start_streams',
    max_retries=3,
    default_retry_delay=10
)
def batch_start_streams_task(
    self,
    stream_ids: List[str],
    user_id: str,
    username: str,
    workspace_id: str
) -> Dict[str, Any]:
    """Sync wrapper for async batch_start operation."""
    
    async def _async_task():
        task_id = self.request.id
        logger.info(f"[Task {task_id}] Batch starting {len(stream_ids)} streams")
        
        try:
            from app.services.stream_service import stream_manager
            
            user_uuid = UUID(user_id)
            total_streams = len(stream_ids)
            results = []
            
            self.update_state(state='PROGRESS', meta={'current': 0, 'total': total_streams, 'status': 'Starting...'})
            
            for idx, stream_id_str in enumerate(stream_ids):
                self.update_state(state='PROGRESS', meta={
                    'current': idx + 1, 'total': total_streams,
                    'status': f'Stream {idx + 1}/{total_streams}...'
                })
                
                try:
                    result = await stream_manager.start_stream_in_workspace(
                        stream_id=UUID(stream_id_str),
                        requester_user_id=user_uuid
                    )
                    results.append({"stream_id": stream_id_str, "success": True, "data": result})
                    await asyncio.sleep(0.1)
                except Exception as e:
                    results.append({"stream_id": stream_id_str, "success": False, "error": str(e)})
            
            successful = sum(1 for r in results if r["success"])
            
            return {
                "total_requested": total_streams,
                "successful": successful,
                "failed": total_streams - successful,
                "results": results,
                "task_id": task_id,
                "completed_at": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
            }
        except Exception as e:
            logger.error(f"[Task {self.request.id}] Failed: {e}", exc_info=True)
            raise self.retry(exc=e, countdown=10)
    
    return run_async(_async_task())


# ==================== TASK 4: Batch Stop Streams ====================

@celery_app.task(
    bind=True,
    name='stream_tasks.batch_stop_streams',
    max_retries=3,
    default_retry_delay=10
)
def batch_stop_streams_task(
    self,
    stream_ids: List[str],
    user_id: str,
    username: str,
    workspace_id: str
) -> Dict[str, Any]:
    """Sync wrapper for async batch_stop operation."""
    
    async def _async_task():
        task_id = self.request.id
        logger.info(f"[Task {task_id}] Batch stopping {len(stream_ids)} streams")
        
        try:
            from app.services.stream_service import stream_manager
            
            user_uuid = UUID(user_id)
            total_streams = len(stream_ids)
            results = []
            
            self.update_state(state='PROGRESS', meta={'current': 0, 'total': total_streams, 'status': 'Stopping...'})
            
            for idx, stream_id_str in enumerate(stream_ids):
                self.update_state(state='PROGRESS', meta={
                    'current': idx + 1, 'total': total_streams,
                    'status': f'Stream {idx + 1}/{total_streams}...'
                })
                
                try:
                    result = await stream_manager.stop_stream_in_workspace(
                        stream_id=UUID(stream_id_str),
                        requester_user_id=user_uuid,
                        stop_reason='user_action',
                        additional_context=f"Batch stop (Task {task_id})"
                    )
                    results.append({"stream_id": stream_id_str, "success": True, "data": result})
                except Exception as e:
                    results.append({"stream_id": stream_id_str, "success": False, "error": str(e)})
            
            successful = sum(1 for r in results if r["success"])
            
            return {
                "total_requested": total_streams,
                "successful": successful,
                "failed": total_streams - successful,
                "results": results,
                "task_id": task_id,
                "completed_at": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
            }
        except Exception as e:
            logger.error(f"[Task {self.request.id}] Failed: {e}", exc_info=True)
            raise self.retry(exc=e, countdown=10)
    
    return run_async(_async_task())