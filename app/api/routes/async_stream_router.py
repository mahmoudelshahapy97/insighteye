# app/routers/async_stream_router.py
from fastapi import APIRouter, HTTPException, Depends, Response, status
from fastapi.responses import JSONResponse
import logging
from uuid import UUID
from typing import Dict
import concurrent.futures
import os

from app.utils import check_workspace_access
from app.services.session_service import session_manager 
from app.services.workspace_service import workspace_service
from app.services.database import db_manager
from app.schemas import (
    BatchStreamOperation
)

# Import Celery tasks
from app.tasks.stream_tasks import (
    start_all_workspace_streams_task,
    stop_all_workspace_streams_task,
    batch_start_streams_task,
    batch_stop_streams_task
)

router = APIRouter(prefix="/async", tags=["async-streams3"])
logger = logging.getLogger(__name__)

# ThreadPoolExecutor for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 1) * 2 + 4)
)

async def get_workspace_id_for_user(username: str) -> UUID:
    """Helper function to get workspace_id for a user."""
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400, 
            detail="No active workspace. Please set an active workspace."
        )
    return workspace_id_obj


# ==================== CELERY TASK STATUS ENDPOINT ====================

@router.get("/task/{task_id}")
async def get_task_status(
    task_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the status of a background task.
    
    Returns task state, progress, and results if completed.
    """
    from celery.result import AsyncResult
    
    try:
        task = AsyncResult(task_id)
        
        response = {
            "task_id": task_id,
            "state": task.state,
            "ready": task.ready(),
            "successful": task.successful() if task.ready() else None,
        }
        
        if task.state == 'PENDING':
            response["status"] = "Task is waiting to be processed"
        elif task.state == 'PROGRESS':
            response["status"] = "Task is in progress"
            response["info"] = task.info
        elif task.state == 'SUCCESS':
            response["status"] = "Task completed successfully"
            response["result"] = task.result
        elif task.state == 'FAILURE':
            response["status"] = "Task failed"
            response["error"] = str(task.info)
        else:
            response["status"] = f"Task state: {task.state}"
            response["info"] = task.info
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response
        )
        
    except Exception as e:
        logger.error(f"Error getting task status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get task status."
        )


# ==================== CELERY-POWERED BATCH ENDPOINTS ====================

@router.post("/workspace/start-all")
async def async_start_all_workspace_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start all inactive streams in workspace (ASYNC with Celery).
    
    Returns task_id for progress tracking.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        ws_id = await get_workspace_id_for_user(username)
        
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            ws_id,
            required_role="admin"
        )

        # Validate admin access
        await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Submit task to Celery
        task = start_all_workspace_streams_task.apply_async(
            args=[str(ws_id), user_id, username],
            task_id=None  # Let Celery generate ID
        )
        
        logger.info(
            f"Submitted start-all task {task.id} for workspace {ws_id} "
            f"(user {username})"
        )
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "success": True,
                "message": "Batch operation started in background",
                "task_id": task.id,
                "status_url": f"/api/v2/streams/task/{task.id}",
                "workspace_id": str(ws_id)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start workspace streams."
        )


@router.post("/workspace/stop-all")
async def async_stop_all_workspace_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop all active streams in workspace (ASYNC with Celery).
    
    Returns task_id for progress tracking.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        ws_id = await get_workspace_id_for_user(username)
        
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            ws_id,
            required_role="admin"
        )
        
        await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Submit task to Celery
        task = stop_all_workspace_streams_task.apply_async(
            args=[str(ws_id), user_id, username]
        )
        
        logger.info(
            f"Submitted stop-all task {task.id} for workspace {ws_id} "
            f"(user {username})"
        )
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "success": True,
                "message": "Batch operation started in background",
                "task_id": task.id,
                "status_url": f"/api/v2/streams/task/{task.id}",
                "workspace_id": str(ws_id)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to stop workspace streams."
        )


@router.post("/batch/start")
async def async_batch_start_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start multiple streams in batch (ASYNC with Celery).
    
    Returns task_id for progress tracking.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )

        # Submit task to Celery
        task = batch_start_streams_task.apply_async(
            args=[request.stream_ids, user_id, username, str(workspace_id_obj)]
        )
        
        logger.info(
            f"Submitted batch-start task {task.id} for {len(request.stream_ids)} streams "
            f"(user {username})"
        )
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "success": True,
                "message": f"Batch start operation initiated for {len(request.stream_ids)} streams",
                "task_id": task.id,
                "status_url": f"/api/v2/streams/task/{task.id}",
                "total_streams": len(request.stream_ids)
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch start: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch start failed."
        )


@router.post("/batch/stop")
async def async_batch_stop_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop multiple streams in batch (ASYNC with Celery).
    
    Returns task_id for progress tracking.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )

        # Submit task to Celery
        task = batch_stop_streams_task.apply_async(
            args=[request.stream_ids, user_id, username, str(workspace_id_obj)]
        )
        
        logger.info(
            f"Submitted batch-stop task {task.id} for {len(request.stream_ids)} streams "
            f"(user {username})"
        )
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "success": True,
                "message": f"Batch stop operation initiated for {len(request.stream_ids)} streams",
                "task_id": task.id,
                "status_url": f"/api/v2/streams/task/{task.id}",
                "total_streams": len(request.stream_ids)
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch stop: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch stop failed."
        )
