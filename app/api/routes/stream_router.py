# app/routers/stream_router.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Response, Query, status
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState
from fastapi.encoders import jsonable_encoder
import time
import logging
import asyncio
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from typing import Dict, Optional, List
import concurrent.futures
import os
import cv2
import numpy as np

from app.utils import frame_to_base64, check_workspace_access, safe_close_websocket, send_ping, handle_mark_read_message
from app.services.stream_service import stream_manager
from app.services.session_service import session_manager 
from app.services.workspace_service import workspace_service
from app.config.settings import config
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.schemas import (
    StreamStartRequest,
    StreamStopRequest,
    BatchStreamOperation
)

router = APIRouter(tags=["streams3"]) #prefix="/streams3", 
logger = logging.getLogger(__name__)

# ThreadPoolExecutor for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 1) * 2 + 4)
)

async def get_workspace_id_for_user(username: str) -> UUID:
    """
    Helper function to get workspace_id for a user.
    Raises HTTPException if no active workspace found.
    """
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400, 
            detail="No active workspace. Please set an active workspace."
        )
    return workspace_id_obj

# ==================== Stream Lifecycle Endpoints ====================

@router.post("/start", status_code=status.HTTP_200_OK)
async def start_stream(
    request: StreamStartRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start a video stream.
    
    Validates workspace access, checks quota limits, and initiates stream processing.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        stream_id = str(request.stream_id)

        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )
        
        # Start stream with workspace validation
        result = await stream_manager.start_stream_in_workspace(
            stream_id=stream_id,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' started successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start stream: {str(e)}"
        )

@router.post("/stop", status_code=status.HTTP_200_OK)
async def stop_stream(
    request: StreamStopRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop a video stream.
    
    Only stream owner or workspace admin can stop streams.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        stream_id = str(request.stream_id)

        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )
        
        # FIXED: Pass stop_reason='user_action' and additional context
        result = await stream_manager.stop_stream_in_workspace(
            stream_id=stream_id,
            requester_user_id=user_id,
            stop_reason='user_action',  # ← CRITICAL: This was missing!
            additional_context=f"Stopped via API by user {user_id}"
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' stopped successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop stream: {str(e)}"
        )


@router.post("/restart/{stream_id}", status_code=status.HTTP_200_OK)
async def restart_stream(
    stream_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Restart a video stream.
    
    Stops and immediately restarts the stream. Useful for recovering from errors.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        stream_uuid = UUID(stream_id)

        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )
        
        # This handles the stop_reason internally
        result = await stream_manager.restart_stream_in_workspace(
            stream_id=stream_uuid,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' restarted successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restarting stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart stream: {str(e)}"
        )

# ==================== Stream Status and Information ====================

@router.get("/list")
async def list_user_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    List all streams accessible to the user.
    
    Optionally filter by workspace.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]

        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )

        # Get streams
        result = await stream_manager.get_workspace_streams_for_user(
            user_id=user_id,
            workspace_id=workspace_id_obj
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": result
            }
        )
        
    except Exception as e:
        logger.error(f"Error listing streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list streams: {str(e)}"
        )

# ==================== Workspace Stream Management ====================

@router.post("/workspace/start-all")
async def start_all_workspace_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start all inactive streams in a workspace.
    
    Requires workspace admin role.
    Respects workspace quota limits and will start streams up to available capacity.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        # Get workspace_id for the user
        ws_id = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            ws_id,
            required_role="admin"
        )

        # Validate admin access
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Check workspace limits first
        limits = await stream_manager.get_workspace_stream_limits(ws_id)
        
        if limits['available_slots'] <= 0:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": False,
                    "message": f"Workspace at capacity ({limits['current_active']}/{limits['total_camera_limit']})",
                    "data": {
                        "started_count": 0,
                        "total_count": 0,
                        "skipped_quota": 0,
                        "failed_streams": [],
                        "limits": limits
                    }
                }
            )
        
        # Get ALL streams from database that are NOT currently streaming
        streams_query = """
            SELECT vs.stream_id, vs.name, vs.is_streaming, vs.status, vs.user_id,
                   u.is_active as user_active, u.is_subscribed, u.role as user_role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1 
                AND vs.is_streaming = FALSE
                AND u.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
            ORDER BY vs.created_at ASC
        """
        db_streams = await stream_manager.db_manager.execute_query(
            streams_query, (ws_id,), fetch_all=True
        )
        
        if not db_streams:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": "No inactive streams to start",
                    "data": {
                        "started_count": 0,
                        "total_count": 0,
                        "skipped_quota": 0,
                        "failed_streams": []
                    }
                }
            )
        
        # Start streams up to available quota
        started_count = 0
        skipped_quota = 0
        failed_streams = []
        available_slots = limits['available_slots']
        
        for stream in db_streams:
            stream_id = stream['stream_id']
            stream_name = stream['name']
            stream_user_id = stream['user_id']
            
            # Check if we've reached workspace capacity
            if started_count >= available_slots:
                skipped_quota += 1
                logger.debug(f"Skipping stream {stream_id} due to workspace quota")
                continue
            
            try:
                # Validate user can start stream (check personal limits)
                can_start, reason = await stream_manager.can_start_stream_in_workspace(
                    ws_id, stream_user_id
                )
                
                if not can_start:
                    logger.warning(f"Cannot start stream {stream_id}: {reason}")
                    failed_streams.append({
                        'stream_id': str(stream_id),
                        'camera_name': stream_name,
                        'error': reason
                    })
                    continue
                
                # Start the stream
                await stream_manager.start_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=UUID(user_id)
                )
                started_count += 1
                logger.info(f"Started stream {stream_id} ({stream_name}) in workspace {ws_id}")
                
                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Failed to start stream {stream_id}: {e}")
                failed_streams.append({
                    'stream_id': str(stream_id),
                    'camera_name': stream_name,
                    'error': str(e)
                })
        
        # FIXED: Invalidate the cache after starting streams
        # This ensures the next /limits call gets fresh data
        cache_key = f"ws_limits_{ws_id}"
        if hasattr(stream_manager, '_limits_cache') and cache_key in stream_manager._limits_cache:
            del stream_manager._limits_cache[cache_key]
            logger.debug(f"Invalidated limits cache for workspace {ws_id}")
        
        # Calculate remaining quota after this operation
        remaining_slots = available_slots - started_count
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Started {started_count}/{len(db_streams)} streams",
                "data": {
                    "started_count": started_count,
                    "total_count": len(db_streams),
                    "skipped_quota": skipped_quota,
                    "failed_streams": failed_streams,
                    "limits": {
                        **limits,
                        "remaining_after_operation": remaining_slots
                    }
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start workspace streams: {str(e)}"
        )

@router.post("/workspace/stop-all")
async def stop_all_workspace_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop all active streams in a workspace.
    
    Requires workspace admin role.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        # Get workspace_id for the user
        ws_id = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            ws_id,
            required_role="admin"
        )
        
        # Validate admin access
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Get ALL streams from database that should be stopped
        streams_query = """
            SELECT stream_id, name, is_streaming, status
            FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE
        """
        db_streams = await stream_manager.db_manager.execute_query(
            streams_query, (ws_id,), fetch_all=True
        )
        
        if not db_streams:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": "No active streams to stop",
                    "data": {
                        "stopped_count": 0,
                        "total_count": 0,
                        "failed_streams": []
                    }
                }
            )
        
        # Stop all streams
        stopped_count = 0
        failed_streams = []
        
        for stream in db_streams:
            stream_id = stream['stream_id']
            stream_name = stream['name']
            
            try:
                # FIXED: Add stop_reason parameter
                await stream_manager.stop_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=UUID(user_id),
                    stop_reason='user_action',  # ← CRITICAL: This was missing!
                    additional_context=f"Workspace-wide stop by admin {user_id}"
                )
                stopped_count += 1
                logger.info(f"Stopped stream {stream_id} ({stream_name}) in workspace {ws_id}")
                
            except Exception as e:
                logger.error(f"Failed to stop stream {stream_id}: {e}")
                failed_streams.append({
                    'stream_id': str(stream_id),
                    'camera_name': stream_name,
                    'error': str(e)
                })
        
        # Invalidate cache
        cache_key = f"ws_limits_{ws_id}"
        if hasattr(stream_manager, '_limits_cache') and cache_key in stream_manager._limits_cache:
            del stream_manager._limits_cache[cache_key]
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stopped {stopped_count}/{len(db_streams)} streams",
                "data": {
                    "stopped_count": stopped_count,
                    "total_count": len(db_streams),
                    "failed_streams": failed_streams
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop workspace streams: {str(e)}"
        )

# ==================== WebSocket Endpoints ====================

@router.websocket("/ws/{stream_id}")
async def stream_websocket(
    websocket: WebSocket,
    stream_id: str
):
    """
    WebSocket endpoint for real-time stream data.
    
    Streams processed video frames with detection overlays.
    """
    await websocket.accept()
    ping_task = None
    
    try:
        # Get authentication from query params or headers
        token = websocket.query_params.get("token")
        if not token:
            await websocket.send_json({
                "type": "error",
                "message": "Authentication token required"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Validate token and get user
        # Note: Implement token validation based on your auth system
        try:
            user_data = await session_manager.verify_token(token)
            user_id = UUID(user_data["user_id"])
        except Exception as e:
            await websocket.send_json({
                "type": "error",
                "message": "Invalid authentication token"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        stream_uuid = UUID(stream_id)
        
        # Validate access
        await stream_manager.validate_workspace_stream_access(
            user_id=user_id,
            stream_id=stream_uuid
        )
        
        # Connect client
        connected = await stream_manager.connect_client_to_stream(stream_id, websocket)
        if not connected:
            await websocket.send_json({
                "type": "error",
                "message": "Stream not active or not found"
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Send connection confirmation
        await websocket.send_json({
            "type": "connected",
            "stream_id": stream_id,
            "timestamp": datetime.now().isoformat()
        })
        
        # Start ping task
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Main streaming loop
        while True:
            # Get latest frame
            async with stream_manager._lock:
                stream_info = stream_manager.active_streams.get(stream_id)
            
            if not stream_info:
                await websocket.send_json({
                    "type": "stream_ended",
                    "message": "Stream is no longer active"
                })
                break
            
            latest_frame = stream_info.get('latest_frame')
            
            if latest_frame is not None:
                # Encode frame to JPEG
                _, buffer = cv2.imencode('.jpg', latest_frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                frame_bytes = buffer.tobytes()
                
                # Send frame
                await websocket.send_bytes(frame_bytes)
            
            # Small delay to control frame rate
            await asyncio.sleep(0.033)  # ~30 FPS
        
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for stream {stream_id}")
    except Exception as e:
        logger.error(f"WebSocket error for stream {stream_id}: {e}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Stream error: {str(e)}"
            })
        except:
            pass
    finally:
        # Cleanup
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        
        await stream_manager.disconnect_client(stream_id, websocket)
        await safe_close_websocket(websocket)

@router.websocket("/ws/notifications")
async def notifications_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for real-time notifications.
    
    Subscribes to workspace and stream notifications.
    """
    await websocket.accept()
    ping_task = None
    user_id_str = None
    
    try:
        # Get authentication
        token = websocket.query_params.get("token")
        if not token:
            await websocket.send_json({
                "type": "error",
                "message": "Authentication token required"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Validate token
        try:
            user_data = await session_manager.verify_token(token)
            user_id_str = user_data["user_id"]
        except Exception as e:
            await websocket.send_json({
                "type": "error",
                "message": "Invalid authentication token"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Subscribe to notifications
        subscribed = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        
        if not subscribed:
            await websocket.send_json({
                "type": "error",
                "message": "Failed to subscribe to notifications"
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Start ping task
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Keep connection alive
        while True:
            try:
                # Receive messages (for heartbeat/acknowledgment)
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=60.0
                )
                
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                
            except asyncio.TimeoutError:
                # Check if still connected
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
            
    except WebSocketDisconnect:
        logger.info(f"Notification WebSocket disconnected for user {user_id_str}")
    except Exception as e:
        logger.error(f"Notification WebSocket error: {e}", exc_info=True)
    finally:
        # Cleanup
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        
        if user_id_str:
            await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
        
        await safe_close_websocket(websocket)

# ==================== Batch Operations ====================

@router.post("/batch/start")
async def batch_start_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start multiple streams in batch.
    
    Returns success/failure status for each stream.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )

        results = []
        
        for stream_id_str in request.stream_ids:
            try:
                stream_id = UUID(stream_id_str)
                result = await stream_manager.start_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=user_id
                )
                
                results.append({
                    "stream_id": stream_id_str,
                    "success": True,
                    "message": f"Started successfully",
                    "data": result
                })
                
            except Exception as e:
                results.append({
                    "stream_id": stream_id_str,
                    "success": False,
                    "message": "Failed to start",
                    "error": str(e)
                })
        
        successful = sum(1 for r in results if r["success"])
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_requested": len(request.stream_ids),
                "successful": successful,
                "failed": len(request.stream_ids) - successful,
                "results": results
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch start: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch start failed: {str(e)}"
        )

@router.post("/batch/stop")
async def batch_stop_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop multiple streams in batch.
    
    Returns success/failure status for each stream.
    """
    try:
        user_id = str(current_user["user_id"])
        username = current_user["username"]
        
        # Get workspace_id for the user
        workspace_id_obj = await get_workspace_id_for_user(username)
        
        # Verify user has access to this workspace
        await check_workspace_access(
            db_manager,
            UUID(user_id),
            workspace_id_obj,
            required_role=None
        )

        results = []
        
        for stream_id_str in request.stream_ids:
            try:
                stream_id = UUID(stream_id_str)
                
                # FIXED: Add stop_reason parameter
                result = await stream_manager.stop_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=user_id,
                    stop_reason='user_action',  # ← CRITICAL: This was missing!
                    additional_context=f"Batch stop by user {user_id}"
                )
                
                results.append({
                    "stream_id": stream_id_str,
                    "success": True,
                    "message": "Stopped successfully",
                    "data": result
                })
                
            except Exception as e:
                results.append({
                    "stream_id": stream_id_str,
                    "success": False,
                    "message": "Failed to stop",
                    "error": str(e)
                })
        
        successful = sum(1 for r in results if r["success"])
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_requested": len(request.stream_ids),
                "successful": successful,
                "failed": len(request.stream_ids) - successful,
                "results": results
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch stop: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch stop failed: {str(e)}"
        )

# ==================== Stream Control Endpoints ====================

@router.post("/start_stream/{stream_id_str}")
async def start_workspace_stream_endpoint(
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Start a specific stream."""
    try:
        requester_user_id = str(current_user_data["user_id"])
        result = await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id)
        return JSONResponse(content={"status": "success", "details": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.error(f"Error in /start_stream/{stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.post("/stop_stream/{stream_id_str}")
async def stop_workspace_stream_endpoint(
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop a specific stream - LEGACY ENDPOINT.
    
    This endpoint maintains backward compatibility while using the new
    stream manager infrastructure for proper stop handling.
    
    Important: This properly cancels any retry attempts and marks the
    stream as user-stopped to prevent automatic restart.
    """
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data.get("username", "Unknown User")
        
        # Validate stream ID format
        try:
            stream_id_uuid = UUID(stream_id_str)
        except ValueError:
            return JSONResponse(
                status_code=400, 
                content={"status": "error", "message": "Invalid stream ID format."}
            )

        # Get stream info to validate it exists
        stream_info_db = await db_manager.execute_query(
            "SELECT workspace_id, name, user_id FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), 
            fetch_one=True
        )

        if not stream_info_db:
            raise HTTPException(status_code=404, detail="Stream not found.")
        
        s_workspace_id = stream_info_db['workspace_id']
        s_name = stream_info_db['name']
        s_owner_id = stream_info_db['user_id']
        
        # Check workspace access and permissions
        try:
            requester_role_info = await check_workspace_access(
                db_manager,
                UUID(requester_user_id_str),
                s_workspace_id,
            )
        except HTTPException as access_error:
            return JSONResponse(
                status_code=access_error.status_code,
                content={"status": "error", "message": access_error.detail}
            )

        logger.info(
            f"Legacy stop endpoint: User {requester_username} ({requester_user_id_str}) "
            f"stopping stream {stream_id_str} ({s_name})"
        )

        # FIXED: Use stream_manager.stop_stream_in_workspace() instead of direct DB update
        # This ensures:
        # 1. Proper retry cancellation via retry_service
        # 2. Correct database state (retry_count=0, next_retry_at=NULL)
        # 3. Consistent stop_reason='user_action' handling
        # 4. No automatic restart
        result = await stream_manager.stop_stream_in_workspace(
            stream_id=stream_id_uuid,
            requester_user_id=UUID(requester_user_id_str),
            stop_reason='user_action',  # ← CRITICAL: User-initiated stop
            additional_context=f"Stopped by {requester_username} via legacy endpoint /stop_stream"
        )
        
        # Create notification for stream owner (if different from requester)
        try:
            from app.services.notification_service import notification_service
            if str(s_owner_id) != requester_user_id_str:
                await notification_service.create_notification(
                    workspace_id=s_workspace_id,
                    user_id=s_owner_id,
                    status="inactive",
                    message=f"Camera '{s_name}' stopped by {requester_username}.",
                    stream_id=stream_id_uuid,
                    camera_name=s_name
                )
        except Exception as notif_error:
            logger.warning(f"Failed to create notification: {notif_error}")
        
        logger.info(
            f"✅ Stream {stream_id_str} ({s_name}) stopped successfully via legacy endpoint. "
            f"Will NOT auto-restart."
        )
        
        return JSONResponse(content={
            "status": "success", 
            "message": f"Stream '{s_name}' stopped successfully.",
            "data": {
                "stream_id": str(stream_id_uuid),
                "name": s_name,
                "will_auto_restart": False,
                "stopped_by": requester_username
            }
        })

    except HTTPException as e:
        logger.warning(f"HTTP exception in legacy stop endpoint: {e.detail}")
        return JSONResponse(
            status_code=e.status_code, 
            content={"status": "error", "message": e.detail}
        )
    except ValueError as ve:
        logger.warning(f"Value error in legacy stop endpoint: {ve}")
        return JSONResponse(
            status_code=400, 
            content={"status": "error", "message": "Invalid stream ID format."}
        )
    except Exception as e:
        logger.error(
            f"Unexpected error stopping stream {stream_id_str} via legacy endpoint: {e}", 
            exc_info=True
        )
        return JSONResponse(
            status_code=500, 
            content={
                "status": "error", 
                "message": "Internal server error while stopping stream."
            }
        )

# ==================== WebSocket Endpoints ====================

@router.websocket("/stream")
async def websocket_workspace_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming video frames."""
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

        # Authenticate
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
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
        stream_details_db = await db_manager.execute_query(
            stream_details_query, (stream_id_uuid,), fetch_one=True
        )
        
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
        
        # Extract location info
        location_data = {
            "location": stream_details_db.get('location'),
            "area": stream_details_db.get('area'),
            "building": stream_details_db.get('building'),
            "zone": stream_details_db.get('zone'),
            "floor_level": stream_details_db.get('floor_level')
        }

        # Check workspace access
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            s_workspace_id,
        )
        
        # Check if stream is active
        stream_is_active_in_manager = False
        async with stream_manager._lock:
            current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
            if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                stream_is_active_in_manager = True
        
        # Start stream if not active
        if not s_is_streaming_db or not stream_is_active_in_manager:
            logger.info(f"WS: Stream {stream_id_str} not active. Attempting start by {requester_user_id_str}.")
            
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']:
                await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."})
                websocket_closed = True
                await websocket.close(1008)
                return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str)
                
                # Wait for stream to become active
                for _ in range(config.stream_ws_start_wait_attempts):
                    async with stream_manager._lock:
                        current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True
                        break
                    await asyncio.sleep(1.0)
                    
                if not stream_is_active_in_manager:
                    logger.warning(f"Stream {stream_id_str} failed to become active for WS")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize."})
                    websocket_closed = True
                    await websocket.close(1011)
                    return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"})
                websocket_closed = True
                await websocket.close(1011)
                return

        # Connect to stream
        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."})
            websocket_closed = True
            await websocket.close(1011)
            return

        # Check connection before sending confirmation
        if websocket.client_state != WebSocketState.CONNECTED:
            logger.warning(f"WebSocket disconnected before sending confirmation for {stream_id_str}")
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
        
        target_fps = config.websocket_client_fps
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066

        # Main streaming loop
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
                    await websocket.send_json({"status": "info", "message": "Stream ended or became inactive."})
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
                        logger.debug(f"Frame send failed: WebSocket already closing for stream {stream_id_str}")
                        break
                    else:
                        raise
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logger.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
        websocket_closed = True
    except asyncio.CancelledError:
        logger.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled")
    except ValueError as ve:
        logger.warning(f"WS stream error: Invalid ID format - {ve}")
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid stream ID format."})
                websocket_closed = True
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logger.error(f"WS stream error ({stream_id_str or 'unknown'}): {e}", exc_info=True)
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
        
        if not websocket_closed:
            await safe_close_websocket(websocket, user_id_for_log)

@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
    """WebSocket endpoint for real-time notifications."""
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    websocket_closed = False
    connection_start_time = time.time()

    try:
        await websocket.accept()
        
        # Add connection delay to prevent rapid reconnections
        await asyncio.sleep(0.5)
        
        # Authenticate
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
            websocket_closed = True
            await websocket.close(1008)
            return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager.get_user_by_id(UUID(user_id_str))
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

        # Connection stability check
        if websocket.client_state != WebSocketState.CONNECTED:
            logger.warning(f"WebSocket disconnected during authentication for {username_for_log}")
            return
        
        # Connection rate limiting
        connection_duration = time.time() - connection_start_time
        if connection_duration < 2.0:
            await asyncio.sleep(2.0 - connection_duration)

        logger.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({
            "status": "connected",
            "message": "Connected to notification stream",
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        })
        
        # Get initial notifications
        try:
            from app.services.notification_service import notification_service
            initial_notifications_db = await notification_service.get_user_notifications(
                UUID(user_id_str), unread_only=False, limit=20
            )
            
            if initial_notifications_db and websocket.client_state == WebSocketState.CONNECTED:
                formatted_notifications = []
                for notification in initial_notifications_db:
                    formatted_notifications.append({
                        "id": str(notification.get("notification_id")),
                        "user_id": str(notification.get("user_id")),
                        "workspace_id": str(notification.get("workspace_id")),
                        "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                        "camera_name": notification.get("camera_name"),
                        "status": notification.get("status"),
                        "message": notification.get("message"),
                        "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                        "read": notification.get("is_read", False)
                    })
                
                await websocket.send_json({
                    "type": "initial_notifications",
                    "notifications": formatted_notifications,
                    "count": len(formatted_notifications)
                })
        except Exception as e_notif:
            logger.error(f"Error getting initial notifications for {username_for_log}: {e_notif}")
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logger.warning(f"Notify WS: Failed to subscribe {username_for_log}")
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "status": "warning",
                    "message": "Subscription failed, notifications may be delayed"
                })
        
        # Start ping task with longer interval for stability
        ping_task = asyncio.create_task(send_ping(websocket))
        logger.info(f"Notify WS: Setup complete for {username_for_log}")

        # Main message loop with improved error handling
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while websocket.client_state == WebSocketState.CONNECTED and consecutive_errors < max_consecutive_errors:
            try:
                receive_timeout = config.websocket_receive_timeout
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Reset error counter on successful message
                consecutive_errors = 0
                
                if message.get("type") == "pong":
                    logger.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue
                    
                elif message.get("type") == "mark_read":
                    await handle_mark_read_message(message, user_id_str, username_for_log, websocket)

                elif message.get("type") == "acknowledge_fire_alert":
                    # Handle fire alert acknowledgment
                    alert_id = message.get("alert_id")
                    camera_id = message.get("camera_id")
                    
                    logger.warning(
                        f"🔥 Fire alert {alert_id} acknowledged by {username_for_log} "
                        f"for camera {camera_id}"
                    )
                    
                    # Mark notification as read
                    if alert_id:
                        try:
                            from app.services.notification_service import notification_service
                            await notification_service.mark_notification_as_read(UUID(alert_id))
                            
                            # Send confirmation back
                            if websocket.client_state == WebSocketState.CONNECTED:
                                await websocket.send_json({
                                    "type": "acknowledgment_confirmed",
                                    "alert_id": alert_id,
                                    "camera_id": camera_id
                                })
                        except Exception as ack_err:
                            logger.error(f"Error acknowledging fire alert: {ack_err}")            
                else:
                    logger.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

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
                logger.info(f"Notify WS: Client {username_for_log} disconnected normally")
                websocket_closed = True
                break
                
            except Exception as e_recv:
                consecutive_errors += 1
                logger.error(f"Notify WS: Error receiving from {username_for_log} (#{consecutive_errors}): {e_recv}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors for {username_for_log}, closing connection")
                    break
                    
                await asyncio.sleep(1.0)
    
    except WebSocketDisconnect:
        logger.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
        websocket_closed = True
    except Exception as e_outer:
        logger.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
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
                logger.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        if not websocket_closed:
            await safe_close_websocket(websocket, username_for_log)

# ==================== HTTP Notification Endpoints ====================

@router.get("/notify")
async def get_http_notifications(
    since: Optional[float] = Query(None, description="Timestamp to get notifications from"),
    limit: int = Query(50, ge=1, le=200),
    include_read: bool = Query(True),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get notifications via HTTP."""
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    
    try:
        from app.services.notification_service import notification_service
        
        # Parse workspace_id if provided
        ws_id = UUID(workspace_id) if workspace_id else None
        
        # Get notifications
        notifications_db = await notification_service.get_user_notifications(
            UUID(user_id_str),
            workspace_id=ws_id,
            unread_only=not include_read,
            limit=limit
        )
        
        # Filter by timestamp if provided
        if since:
            since_dt = datetime.fromtimestamp(since, tz=ZoneInfo("Africa/Cairo"))
            notifications_db = [
                n for n in notifications_db 
                if n.get("timestamp") and n["timestamp"] > since_dt
            ]
        
        # Format notifications
        formatted_notifications = []
        for notification in notifications_db:
            formatted_notifications.append({
                "id": str(notification.get("notification_id")),
                "user_id": str(notification.get("user_id")),
                "workspace_id": str(notification.get("workspace_id")),
                "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                "camera_name": notification.get("camera_name"),
                "status": notification.get("status"),
                "message": notification.get("message"),
                "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                "read": notification.get("is_read", False)
            })
        
        return {
            "status": "success",
            "count": len(formatted_notifications),
            "notifications": formatted_notifications,
            "server_time": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
        }
        
    except Exception as e:
        logger.error(f"Error getting HTTP notifications for user {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}")

@router.post("/notify/{notification_id_str}/read")
async def mark_notification_read_endpoint(
    notification_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Mark a notification as read."""
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    
    try:
        notification_id_uuid = UUID(notification_id_str)
        
        from app.services.notification_service import notification_service
        success = await notification_service.mark_notification_as_read(notification_id_uuid)
        
        if success:
            return {"status": "success", "message": "Notification marked as read"}
        else:
            # Check if notification exists
            notification = await notification_service.get_notification_by_id(notification_id_uuid)
            if not notification:
                raise HTTPException(status_code=404, detail="Notification not found or not yours.")
            return {"status": "info", "message": "Notification was already marked as read."}

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid notification ID format.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking notification {notification_id_str} as read for {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to mark notification as read: {str(e)}")

@router.get("/workspaces/{workspace_id}/cameras/problematic")
async def get_problematic_cameras(
    workspace_id: UUID,
    hours: int = 24,
    min_failures: int = 3,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get cameras with frequent connection issues.
    Helps identify problematic cameras that need attention.
    """
    # Check workspace access
    await stream_manager.workspace_service.check_workspace_membership_and_get_role(
        user_id=current_user['user_id'],
        workspace_id=workspace_id
    )
    from app.services.video_stream_service import  video_stream_service
    cameras = await video_stream_service.get_frequently_failing_cameras(
        workspace_id=workspace_id,
        hours=hours,
        min_failures=min_failures
    )
    
    return {
        "workspace_id": str(workspace_id),
        "time_window_hours": hours,
        "min_failures_threshold": min_failures,
        "problematic_cameras": cameras,
        "count": len(cameras)
    }

@router.get("/workspaces/{workspace_id}/cameras/stop-history")
async def get_camera_stop_history(
    workspace_id: UUID,
    hours: int = 24,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get history of camera stops with reasons.
    Useful for debugging and monitoring.
    """
    await stream_manager.workspace_service.check_workspace_membership_and_get_role(
        user_id=current_user['user_id'],
        workspace_id=workspace_id
    )
    
    query = """
        SELECT 
            vs.stream_id,
            vs.name,
            vs.location,
            vs.stop_reason,
            vs.stopped_at,
            u.username as stopped_by_username,
            vs.status,
            vs.is_streaming
        FROM video_stream vs
        LEFT JOIN users u ON vs.stopped_by = u.user_id
        WHERE vs.workspace_id = $1
          AND vs.stopped_at > NOW() - INTERVAL '%s hours'
        ORDER BY vs.stopped_at DESC
        LIMIT 100
    """ % hours
    
    history = await db_manager.execute_query(
        query,
        (workspace_id,),
        fetch_all=True
    )
    
    return {
        "workspace_id": str(workspace_id),
        "time_window_hours": hours,
        "stop_history": history
    }


@router.post("/test/fire-alert/{stream_id}")
async def test_fire_alert(
    stream_id: str,
    fire_status: str = "fire",  # or "smoke"
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    TEST ENDPOINT: Manually trigger a fire alert popup.
    Use this to test if the popup system is working.
    """
    try:
        stream_uuid = UUID(stream_id)
        
        # Get stream info
        from app.services.video_stream_service import video_stream_service
        stream_info = await video_stream_service.get_video_stream_by_id(stream_uuid)
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Build location info
        location_info = {
            'location': stream_info.get('location'),
            'area': stream_info.get('area'),
            'building': stream_info.get('building'),
            'zone': stream_info.get('zone'),
            'floor_level': stream_info.get('floor_level'),
        }
        
        # Build location text
        location_parts = []
        if location_info.get('building'):
            location_parts.append(location_info['building'])
        if location_info.get('floor_level'):
            location_parts.append(f"Floor {location_info['floor_level']}")
        if location_info.get('zone'):
            location_parts.append(location_info['zone'])
        
        location_text = " - ".join(location_parts) if location_parts else "Test Location"
        
        alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
        message = f"🔥 TEST {alert_type} ALERT: {fire_status.upper()} detected in {location_text}"
        
        # Create notification
        from app.services.notification_service import notification_service
        notification = await notification_service.create_notification(
            workspace_id=stream_info['workspace_id'],
            user_id=stream_info['user_id'],
            status="urgent",
            message=message,
            stream_id=stream_uuid,
            camera_name=stream_info['name']
        )
        
        # Build and broadcast popup
        popup_alert = {
            "type": "fire_alert_popup",
            "alert": {
                "id": str(notification.get("notification_id")),
                "severity": "critical",
                "alert_type": alert_type.lower(),
                "status": fire_status,
                "camera_name": stream_info['name'],
                "camera_id": str(stream_uuid),
                "location": location_text,
                "location_details": location_info,
                "message": message,
                "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp(),
                "workspace_id": str(stream_info['workspace_id']),
                "user_id": str(stream_info['user_id']),
                "requires_acknowledgment": True,
                "sound_alert": True,
                "priority": "critical"
            }
        }
        
        # Broadcast to current user for testing
        sent_count = await stream_manager.broadcast_notification(
            str(current_user['user_id']),
            popup_alert
        )
        
        return {
            "status": "success",
            "message": f"Test fire alert sent to {sent_count} WebSocket connection(s)",
            "alert": popup_alert
        }
        
    except Exception as e:
        logger.error(f"Error sending test fire alert: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/debug/fire-state/{stream_id}")
async def debug_fire_state(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Debug endpoint to check fire detection state"""
    try:
        from app.services.fire_detection_service import fire_detection_service
        
        stream_uuid = UUID(stream_id)
        fire_state = await fire_detection_service.get_fire_detection_state(stream_uuid)
        
        if not fire_state:
            return {"message": "No fire state found", "can_notify": True}
        
        current_time = datetime.now(ZoneInfo("Africa/Cairo"))
        last_notification = fire_state.get("last_notification_time")
        
        if last_notification:
            time_since_last = (current_time - last_notification).total_seconds() / 60
            can_notify = time_since_last >= 10
            remaining = max(0, 10 - time_since_last)
        else:
            time_since_last = None
            can_notify = True
            remaining = 0
        
        return {
            "fire_status": fire_state.get("fire_status"),
            "last_notification": last_notification.isoformat() if last_notification else None,
            "minutes_since_last": round(time_since_last, 2) if time_since_last else None,
            "cooldown_remaining_minutes": round(remaining, 2),
            "can_notify_now": can_notify
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/stream-health/{stream_id}")
async def debug_stream_health(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Debug endpoint to check if a stream is truly healthy and processing.
    Returns detailed health information.
    """
    try:
        stream_id_str = str(stream_id)
        
        # Check memory state
        async with stream_manager._lock:
            stream_info = stream_manager.active_streams.get(stream_id_str)
            stream_state = stream_manager.stream_states.get(stream_id_str)
        
        if not stream_info:
            return {
                "stream_id": stream_id_str,
                "status": "not_in_memory",
                "message": "Stream not found in active streams"
            }
        
        # Get task info
        task = stream_info.get('task')
        task_alive = task and not task.done()
        task_exception = None
        if task and task.done():
            try:
                task_exception = str(task.exception())
            except:
                pass
        
        # Get frame info
        latest_frame = stream_info.get('latest_frame')
        last_frame_time = stream_info.get('last_frame_time')
        
        frame_age = None
        if last_frame_time:
            frame_age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame_time).total_seconds()
        
        # Get shared stream info
        source = stream_info.get('source')
        shared_stream = None
        if source:
            shared_stream = await stream_manager.video_file_manager.get_shared_stream(source)
            shared_stats = await shared_stream.get_stats() if shared_stream else None
        
        # Database state
        from app.services.video_stream_service import video_stream_service
        db_state = await video_stream_service.get_video_stream_by_id(UUID(stream_id))
        
        # Processing stats
        processing_stats = stream_manager.stream_processing_stats.get(stream_id_str, {})
        
        # Health verdict
        is_healthy = (
            task_alive and
            latest_frame is not None and
            frame_age is not None and
            frame_age < 30 and
            stream_state == StreamState.ACTIVE
        )
        
        return {
            "stream_id": stream_id_str,
            "overall_health": "healthy" if is_healthy else "unhealthy",
            "state": {
                "memory_state": stream_state.value if stream_state else "unknown",
                "status": stream_info.get('status'),
                "database_status": db_state.get('status') if db_state else None,
                "database_is_streaming": db_state.get('is_streaming') if db_state else None
            },
            "task": {
                "alive": task_alive,
                "done": task.done() if task else None,
                "exception": task_exception
            },
            "frames": {
                "has_frame": latest_frame is not None,
                "last_frame_time": last_frame_time.isoformat() if last_frame_time else None,
                "age_seconds": frame_age,
                "is_stale": frame_age > 30 if frame_age else None
            },
            "shared_stream": {
                "source": source,
                "stats": shared_stats if shared_stream else None
            },
            "processing": {
                "frames_processed": processing_stats.get('frames_processed', 0),
                "detection_count": processing_stats.get('detection_count', 0),
                "errors": processing_stats.get('errors', 0)
            },
            "diagnosis": self._diagnose_stream_issue(
                task_alive, latest_frame, frame_age, stream_state
            ),
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error debugging stream health: {e}", exc_info=True)
        return {"error": str(e)}

def _diagnose_stream_issue(self, task_alive, has_frame, frame_age, state):
    """Diagnose what's wrong with a stream"""
    issues = []
    
    if not task_alive:
        issues.append("Processing task is not running")
    
    if not has_frame:
        issues.append("No frames have been received")
    
    if frame_age and frame_age > 30:
        issues.append(f"Frames are stale ({frame_age:.1f}s old)")
    
    if state != StreamState.ACTIVE:
        issues.append(f"Stream state is {state.value}, not ACTIVE")
    
    if not issues:
        return "Stream appears healthy"
    
    return "; ".join(issues)


@router.post("/debug/force-restart/{stream_id}")
async def force_restart_stream(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Force restart a stream even if it appears active.
    Useful when stream is stuck in bad state.
    """
    try:
        user_id = str(current_user_data["user_id"])
        stream_uuid = UUID(stream_id)
        
        # Validate access
        stream_info, _ = await stream_manager.validate_workspace_stream_access(
            user_id=UUID(user_id),
            stream_id=stream_uuid
        )
        
        stream_id_str = str(stream_id)
        
        logger.warning(f"🔧 FORCE RESTART requested for stream {stream_id_str} by user {user_id}")
        
        # Force stop (even if not in memory)
        try:
            await stream_manager._stop_stream(stream_id_str, for_restart=True)
        except Exception as e:
            logger.warning(f"Stop failed (expected if not in memory): {e}")
        
        # Wait a bit
        await asyncio.sleep(2.0)
        
        # Start fresh
        result = await stream_manager.start_stream_in_workspace(
            stream_id=stream_uuid,
            requester_user_id=UUID(user_id)
        )
        
        return {
            "status": "success",
            "message": f"Stream force-restarted",
            "data": result
        }
        
    except Exception as e:
        logger.error(f"Error force-restarting stream: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/admin/cleanup-zombies")
async def cleanup_zombie_streams(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Manually trigger zombie stream cleanup.
    Detects and removes streams stuck in memory but not actually processing.
    """
    # Check if admin
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        zombies = await stream_manager.detect_and_cleanup_zombie_streams()
        
        return {
            "status": "success",
            "zombies_found": len(zombies),
            "zombies_cleaned": zombies,
            "message": f"Cleaned up {len(zombies)} zombie streams"
        }
    
    except Exception as e:
        logger.error(f"Error in zombie cleanup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/memory-stats")
async def get_memory_stats(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Get detailed memory statistics.
    Shows what's actually in memory vs database.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Get memory state
        async with stream_manager._lock:
            memory_stream_ids = list(stream_manager.active_streams.keys())
            memory_details = {}
            
            for stream_id in memory_stream_ids:
                info = stream_manager.active_streams[stream_id]
                task = info.get('task')
                last_frame = info.get('last_frame_time')
                
                memory_details[stream_id] = {
                    'camera_name': info.get('camera_name'),
                    'status': info.get('status'),
                    'has_task': task is not None,
                    'task_alive': task and not task.done() if task else False,
                    'has_frames': info.get('latest_frame') is not None,
                    'last_frame_age': (
                        (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame).total_seconds()
                        if last_frame else None
                    ),
                    'start_time': info.get('start_time').isoformat() if info.get('start_time') else None
                }
        
        # Get database state
        db_streaming_query = """
            SELECT stream_id, name, is_streaming, status, stop_reason
            FROM video_stream
            WHERE is_streaming = TRUE
        """
        db_streaming = await stream_manager.db_manager.execute_query(
            db_streaming_query, fetch_all=True
        )
        
        db_stream_ids = [str(s['stream_id']) for s in db_streaming]
        
        # Find discrepancies
        in_memory_not_db = set(memory_stream_ids) - set(db_stream_ids)
        in_db_not_memory = set(db_stream_ids) - set(memory_stream_ids)
        
        return {
            "memory": {
                "total_streams": len(memory_stream_ids),
                "stream_ids": memory_stream_ids,
                "details": memory_details
            },
            "database": {
                "total_streaming": len(db_stream_ids),
                "stream_ids": db_stream_ids,
                "streams": db_streaming
            },
            "discrepancies": {
                "in_memory_but_not_database": list(in_memory_not_db),
                "in_database_but_not_memory": list(in_db_not_memory),
                "count_mismatch": len(memory_stream_ids) != len(db_stream_ids)
            },
            "shared_streams": {
                "total_sources": len(stream_manager._shared_stream_registry),
                "sources": list(stream_manager._shared_stream_registry.keys())
            },
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error getting memory stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/force-sync")
async def force_sync_memory_database(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Force synchronization between memory and database.
    - Removes streams from memory that database says should stop
    - Does NOT start streams (let management loop handle that)
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Get all streams in memory
        async with stream_manager._lock:
            memory_stream_ids = list(stream_manager.active_streams.keys())
        
        stopped_count = 0
        errors = []
        
        for stream_id_str in memory_stream_ids:
            try:
                # Check database
                db_state = await stream_manager.db_manager.execute_query(
                    """SELECT is_streaming, status, stop_reason, name
                       FROM video_stream
                       WHERE stream_id = $1""",
                    (UUID(stream_id_str),),
                    fetch_one=True
                )
                
                # If not in database OR database says don't stream, stop it
                should_stop = False
                reason = ""
                
                if not db_state:
                    should_stop = True
                    reason = "not_in_database"
                elif not db_state['is_streaming']:
                    should_stop = True
                    reason = f"database_is_streaming_false (status={db_state['status']})"
                elif db_state.get('stop_reason') == 'user_action':
                    should_stop = True
                    reason = "user_stopped"
                
                if should_stop:
                    logger.warning(
                        f"🔧 Force-stopping {stream_id_str}: {reason}"
                    )
                    await stream_manager._stop_stream(stream_id_str, for_restart=False)
                    stopped_count += 1
            
            except Exception as e:
                logger.error(f"Error force-syncing {stream_id_str}: {e}")
                errors.append({
                    'stream_id': stream_id_str,
                    'error': str(e)
                })
        
        return {
            "status": "success",
            "stopped_count": stopped_count,
            "errors": errors,
            "message": f"Force-stopped {stopped_count} streams to sync with database"
        }
    
    except Exception as e:
        logger.error(f"Error in force sync: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/admin/stream/{stream_id}/force-remove")
async def force_remove_stream_from_memory(
    stream_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Forcefully remove a specific stream from memory.
    Last resort when a stream is completely stuck.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        stream_id_str = str(stream_id)
        
        # Check if in memory
        async with stream_manager._lock:
            exists = stream_id_str in stream_manager.active_streams
        
        if not exists:
            return {
                "status": "info",
                "message": f"Stream {stream_id_str} not in memory"
            }
        
        # Force stop
        await stream_manager._stop_stream(stream_id_str, for_restart=False)
        
        # Double-check removal
        async with stream_manager._lock:
            still_exists = stream_id_str in stream_manager.active_streams
        
        if still_exists:
            # Nuclear option: direct removal
            async with stream_manager._lock:
                stream_manager.active_streams.pop(stream_id_str, None)
            stream_manager.stream_workspaces.pop(stream_id_str, None)
            logger.warning(f"⚠️ Nuclear removal of {stream_id_str}")
        
        return {
            "status": "success",
            "message": f"Stream {stream_id_str} forcefully removed from memory"
        }
    
    except Exception as e:
        logger.error(f"Error force-removing stream: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/admin/model-health")
async def check_model_health(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Check if YOLO models are loaded and functioning.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        from app.services.stream_processing_service import stream_processing_service
        
        # Check model status
        model_status = stream_processing_service.get_model_status()
        
        # Get model paths from config
        people_path = config.people_model_path
        gender_path = config.gender_model_path
        fire_path = config.fire_model_path
        
        # Check file existence
        files_exist = {
            "people_model": os.path.exists(people_path),
            "gender_model": os.path.exists(gender_path),
            "fire_model": os.path.exists(fire_path)
        }
        
        # Overall health
        critical_models_ok = model_status['people_model']
        all_models_ok = all(model_status.values())
        
        health_status = "healthy" if critical_models_ok else "critical"
        if critical_models_ok and not all_models_ok:
            health_status = "degraded"
        
        return {
            "status": health_status,
            "models": {
                "people": {
                    "loaded": model_status['people_model'],
                    "path": people_path,
                    "file_exists": files_exist['people_model'],
                    "critical": True
                },
                "gender": {
                    "loaded": model_status['gender_model'],
                    "path": gender_path,
                    "file_exists": files_exist['gender_model'],
                    "critical": False
                },
                "fire": {
                    "loaded": model_status['fire_model'],
                    "path": fire_path,
                    "file_exists": files_exist['fire_model'],
                    "critical": False
                }
            },
            "warnings": [
                msg for msg in [
                    "🚨 CRITICAL: People model not loaded! Detection disabled!" 
                    if not model_status['people_model'] else None,
                    "⚠️ Gender model not loaded - gender detection disabled" 
                    if not model_status['gender_model'] else None,
                    "⚠️ Fire model not loaded - fire detection disabled" 
                    if not model_status['fire_model'] else None,
                ] if msg
            ],
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error checking model health: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/reload-models")
async def reload_models(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint: Force reload of YOLO models.
    Useful if models failed to load during startup.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        from app.services.stream_processing_service import stream_processing_service
        
        logger.warning(f"🔄 Model reload requested by admin {current_user['username']}")
        
        # Reinitialize models
        stream_processing_service._initialize_models()
        
        # Check status
        model_status = stream_processing_service.get_model_status()
        
        return {
            "status": "success",
            "models_loaded": model_status,
            "message": "Models reloaded",
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error reloading models: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/admin/cleanup-zombie-locks")
async def cleanup_zombie_locks(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Manually clean up zombie server locks.
    Requires admin privileges.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        from app.services.distributed_stream_manager import distributed_stream_manager
        
        released = await distributed_stream_manager.cleanup_zombie_locks()
        
        return {
            "success": True,
            "released_locks": released,
            "message": f"Released {released} zombie locks"
        }
        
    except Exception as e:
        logger.error(f"Error in manual cleanup: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/admin/clear-stuck-cameras")
async def clear_stuck_cameras_endpoint(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Admin endpoint to manually clear stuck cameras.
    Use this when cameras won't start after being stopped.
    """
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    from app.services.distributed_stream_manager import distributed_stream_manager
    
    cleared_count = await distributed_stream_manager.clear_stuck_user_stopped_cameras()
    
    return {
        "success": True,
        "cameras_cleared": cleared_count,
        "message": f"Cleared {cleared_count} stuck cameras"
    }
