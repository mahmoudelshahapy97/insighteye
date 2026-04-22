# app/routers/stream_router.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Response, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.websockets import WebSocketState
from fastapi.encoders import jsonable_encoder
import time
import logging
import asyncio
from uuid import uuid4
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import concurrent.futures
import os
import cv2
import numpy as np
import io
from PIL import Image
import base64

from app.utils import frame_to_base64, check_workspace_access, safe_close_websocket, send_ping, handle_mark_read_message
from app.services.stream_service import stream_manager
from app.services.session_service import session_manager 
from app.services.workspace_service import workspace_service
from app.services.s3_service import s3_service
from app.config.settings import config
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.schemas import (
    StreamStartRequest,
    StreamStopRequest,
    BatchStreamOperation,
    FrameRequest,
    BatchFrameResponse,
    FrameMetadata,
    FrameData
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


@router.post("/frames/batch", response_model=BatchFrameResponse)
async def get_last_frames_batch(
    request: FrameRequest,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Retrieve last frames for multiple streams using detection_data_service."""
    user_id_str = str(current_user_data["user_id"])
    username = current_user_data["username"]
    
    try:
        # Use detection_data_service to retrieve single detection
        from app.services.detection_data_service import detection_data_service

        # Get workspace
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Verify workspace access
        await check_workspace_access(
            db_manager,
            UUID(user_id_str),
            workspace_id_obj,
            required_role=None
        )
        
        # Validate stream IDs
        stream_uuids = [UUID(sid) for sid in request.stream_ids]
        
        # Verify streams exist and get metadata
        access_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.location, vs.area,
                   vs.building, vs.floor_level, vs.zone, vs.is_streaming, vs.status
            FROM video_stream vs
            WHERE vs.stream_id = ANY($1) AND vs.workspace_id = $2
        """
        accessible_streams = await db_manager.execute_query(
            access_query,
            (stream_uuids, workspace_id_obj),
            fetch_all=True
        )
        
        if not accessible_streams:
            raise HTTPException(status_code=404, detail="No accessible streams found")
        
        stream_map = {str(s['stream_id']): s for s in accessible_streams}
        
        # Get workspace role for detection_data_service
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=UUID(user_id_str),
            workspace_id=workspace_id_obj
        )
        user_workspace_role = membership.get("role")
        user_system_role = current_user_data.get("role", "user")
        
        frames = []
        errors = []
        
        # Process each stream
        for stream_id in request.stream_ids:
            if stream_id not in stream_map:
                errors.append({
                    "stream_id": stream_id,
                    "error": "Stream not found or access denied"
                })
                continue
            
            try:
                stream_metadata = stream_map[stream_id]
                
                # Build start/end date strings for detection_data_service
                start_date = request.start_time.strftime("%Y-%m-%d") if request.start_time else None
                end_date = request.end_time.strftime("%Y-%m-%d") if request.end_time else None
                start_time = request.start_time.strftime("%H:%M:%S") if request.start_time else None
                end_time = request.end_time.strftime("%H:%M:%S") if request.end_time else None
                
                # Use detection_data_service to retrieve data
                result = await detection_data_service.retrieve_detection_data(
                    workspace_id=workspace_id_obj,
                    user_system_role=user_system_role,
                    user_workspace_role=user_workspace_role,
                    requesting_username=username,
                    camera_id=[stream_id],
                    start_date=start_date,
                    end_date=end_date,
                    start_time=start_time,
                    end_time=end_time,
                    page=1,
                    per_page=request.limit,
                    include_frame=request.include_base64
                )
                
                if not result or not result.get("data"):
                    errors.append({
                        "stream_id": stream_id,
                        "error": "No frames found"
                    })
                    continue
                
                # Process each detection result
                for idx, detection in enumerate(result["data"]):
                    metadata_dict = detection.get("metadata", {})
                    
                    frame_data = FrameData(
                        frame_id=detection.get("id", str(uuid4())),
                        stream_id=stream_id,
                        timestamp=datetime.fromtimestamp(metadata_dict.get("timestamp", 0)) if metadata_dict.get("timestamp") else datetime.now(),
                        frame_number=idx
                    )
                    
                    # Add frame if requested
                    if request.include_base64:
                        frame_base64 = detection.get("frame")
                        if frame_base64:
                            if frame_base64.startswith("s3://"):
                                presigned_url = await s3_service.get_presigned_url(frame_base64)
                                frame_data.frame_base64 = presigned_url
                            else:
                                frame_data.frame_base64 = frame_base64
                        else:
                            logger.warning(f"No frame_base64 found for detection {detection.get('id')}")
                    
                    # Add metadata if requested
                    if request.include_metadata:
                        frame_data.metadata = FrameMetadata(
                            stream_id=stream_id,
                            camera_name=stream_metadata['name'],
                            location=stream_metadata.get('location'),
                            area=stream_metadata.get('area'),
                            building=stream_metadata.get('building'),
                            floor_level=stream_metadata.get('floor_level'),
                            zone=stream_metadata.get('zone'),
                            timestamp=frame_data.timestamp,
                            detection_count=metadata_dict.get('person_count'),
                            detections=None,  # Could be populated if needed
                            is_streaming=stream_metadata['is_streaming'],
                            status=stream_metadata['status']
                        )
                    
                    frames.append(frame_data)
                
            except Exception as e_stream:
                logger.error(f"Error retrieving frames for stream {stream_id}: {e_stream}", exc_info=True)
                errors.append({
                    "stream_id": stream_id,
                    "error": str(e_stream)
                })
        
        return BatchFrameResponse(
            total_streams=len(request.stream_ids),
            total_frames=len(frames),
            frames=frames,
            errors=errors
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in batch frame retrieval: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve frames")


@router.get("/frames/detection/{result_id}")
async def get_detection_with_frame(
    result_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get a specific detection result with frame using detection_data_service."""
    username = current_user_data["username"]
    
    try:
        # Get workspace
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Use detection_data_service to retrieve single detection
        from app.services.detection_data_service import detection_data_service
        
        result = await detection_data_service.retrieve_single_detection(
            result_id=UUID(result_id),
            workspace_id=workspace_id_obj,
            include_frame=True
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Detection not found")
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except ValueError as ve:
        logger.error(f"Invalid result_id format: {ve}")
        raise HTTPException(status_code=400, detail="Invalid result ID format")
    except Exception as e:
        logger.error(f"Error retrieving detection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve detection")

@router.get("/frames/latest/{stream_id}")
async def get_latest_frame(
    stream_id: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(1, ge=1, le=1),
    include_base64: bool = Query(True, description="Include base64 encoded frame"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    username = current_user_data["username"]
    user_id = UUID(str(current_user_data["user_id"]))
    
    # 1. Get workspace
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(status_code=400, detail="No active workspace found.")
    
    # 2. Validate stream exists and get workspace role
    stream_uuid = UUID(stream_id)
    
    stream_query = """
        SELECT vs.stream_id, vs.workspace_id, vs.name
        FROM video_stream vs
        WHERE vs.stream_id = $1 AND vs.workspace_id = $2
    """
    stream_data = await db_manager.execute_query(
        stream_query, (stream_uuid, workspace_id_obj), fetch_one=True
    )
    
    if not stream_data:
        raise HTTPException(status_code=404, detail="Stream not found or access denied")
    
    # 3. Get workspace role
    membership = await workspace_service.check_workspace_membership_and_get_role(
        user_id=user_id,
        workspace_id=workspace_id_obj
    )
    user_workspace_role = membership.get("role")
    
    # 4. Use detection_data_service
    from app.services.detection_data_service import detection_data_service
    
    result = await detection_data_service.retrieve_detection_data(
        workspace_id=workspace_id_obj,
        user_system_role=current_user_data.get("role", "user"),
        user_workspace_role=user_workspace_role,
        requesting_username=username,
        camera_id=[stream_id],
        page=page,
        per_page=per_page,
        include_frame=include_base64
    )
    
    # 5. Process result
    if not result or not result.get("data"):
        raise HTTPException(status_code=404, detail="No data found")
    
    return result


@router.get("/frames/image/{stream_id}/latest", response_class=StreamingResponse)
async def get_latest_frame_image(
    stream_id: str,
    format: str = Query("webp", pattern="^(jpeg|jpg|webp|png)$", description="Image format"),
    quality: int = Query(85, ge=1, le=100, description="Image quality (JPEG/WebP: 1-100, PNG: ignored)"),
    width: Optional[int] = Query(None, ge=1, le=4096, description="Resize width"),
    height: Optional[int] = Query(None, ge=1, le=4096, description="Resize height"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the latest frame as a direct image in multiple formats using detection_data_service.
    
    Supported formats:
    - jpeg/jpg: Standard JPEG format (good compatibility, ~50-70% compression)
    - webp: Modern WebP format (30-50% smaller than JPEG, modern browsers)
    - png: Lossless PNG format (larger files, perfect quality)
    
    Useful for:
    - Displaying in <img> tags
    - Thumbnail generation  
    - Direct image downloads
    - Modern web applications (WebP recommended for best performance)
    
    Example usage: 
    - <img src="/api/frames/image/{stream_id}/latest?format=webp&width=320&height=240" />
    - <img src="/api/frames/image/{stream_id}/latest?format=jpeg&quality=90" />
    - <img src="/api/frames/image/{stream_id}/latest?format=png" /> (lossless)
    """
    try:
        # Normalize format
        img_format = format.lower()
        if img_format == "jpg":
            img_format = "jpeg"
        
        # Get workspace and user info
        username = current_user_data["username"]
        user_id = UUID(str(current_user_data["user_id"]))
        
        # Get workspace
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Validate stream exists and user has access
        stream_uuid = UUID(stream_id)
        
        stream_query = """
            SELECT vs.stream_id, vs.workspace_id, vs.name
            FROM video_stream vs
            WHERE vs.stream_id = $1 AND vs.workspace_id = $2
        """
        stream_data = await db_manager.execute_query(
            stream_query, (stream_uuid, workspace_id_obj), fetch_one=True
        )
        
        if not stream_data:
            raise HTTPException(status_code=404, detail="Stream not found or access denied")
        
        # Get workspace role
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=workspace_id_obj
        )
        user_workspace_role = membership.get("role")
        user_system_role = current_user_data.get("role", "user")
        
        # Use detection_data_service to get latest frame
        from app.services.detection_data_service import detection_data_service
        
        result = await detection_data_service.retrieve_detection_data(
            workspace_id=workspace_id_obj,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=username,
            camera_id=[stream_id],
            page=1,
            per_page=1,
            include_frame=True
        )
        
        # Validate result
        if not result or not result.get("data") or len(result["data"]) == 0:
            raise HTTPException(status_code=404, detail="No frames found for this stream")
        
        # Extract frame from first result
        detection = result["data"][0]
        frame_base64 = detection.get("frame")
        
        if not frame_base64:
            raise HTTPException(status_code=404, detail="Frame data not available")
        
        # Decode base64 to image
        try:
            if frame_base64.startswith("s3://"):
                img_data = await s3_service.get_image_data(frame_base64)
                if not img_data:
                    raise HTTPException(status_code=404, detail="Image not found in S3")
            else:
                img_data = base64.b64decode(frame_base64)
                
            img = Image.open(io.BytesIO(img_data))
        except Exception as decode_error:
            logger.error(f"Failed to decode frame image: {decode_error}")
            raise HTTPException(status_code=500, detail="Failed to decode frame image")
        
        # Resize if requested
        if width or height:
            # Calculate dimensions maintaining aspect ratio
            original_width, original_height = img.size
            
            if width and height:
                # Both specified - use as is
                new_size = (width, height)
            elif width:
                # Only width specified - maintain aspect ratio
                ratio = width / original_width
                new_size = (width, int(original_height * ratio))
            else:
                # Only height specified - maintain aspect ratio
                ratio = height / original_height
                new_size = (int(original_width * ratio), height)
            
            # Resize with high-quality resampling
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        
        # Convert and save based on format
        buffered = io.BytesIO()
        
        if img_format == "webp":
            # WebP format - best compression
            # Convert RGBA to RGB if necessary (WebP supports both, but RGB is smaller)
            if img.mode in ('RGBA', 'LA'):
                # Keep alpha channel for WebP if it exists
                img.save(buffered, format="WEBP", quality=quality, method=6, lossless=False)
            elif img.mode == 'P':
                img = img.convert('RGBA')
                img.save(buffered, format="WEBP", quality=quality, method=6, lossless=False)
            else:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(buffered, format="WEBP", quality=quality, method=6, lossless=False)
            
            media_type = "image/webp"
            file_ext = "webp"
            
        elif img_format == "png":
            # PNG format - lossless
            if img.mode in ('RGBA', 'LA', 'P'):
                if img.mode == 'P':
                    img = img.convert('RGBA')
                img.save(buffered, format="PNG", optimize=True)
            else:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(buffered, format="PNG", optimize=True)
            
            media_type = "image/png"
            file_ext = "png"
            
        else:  # jpeg
            # JPEG format - standard
            # Convert RGBA to RGB (JPEG doesn't support transparency)
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            img.save(buffered, format="JPEG", quality=quality, optimize=True)
            media_type = "image/jpeg"
            file_ext = "jpg"
        
        buffered.seek(0)
        
        # Get metadata for helpful headers
        metadata = detection.get("metadata", {})
        timestamp = metadata.get("timestamp")
        camera_name = metadata.get("name", "unknown")
        
        # Generate filename
        timestamp_str = datetime.fromtimestamp(timestamp).strftime("%Y%m%d_%H%M%S") if timestamp else "latest"
        filename = f"{camera_name}_{timestamp_str}.{file_ext}"
        
        return StreamingResponse(
            buffered,
            media_type=media_type,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
                "Content-Disposition": f'inline; filename="{filename}"',
                "X-Frame-Timestamp": str(timestamp) if timestamp else "unknown",
                "X-Camera-Name": camera_name,
                "X-Image-Format": img_format.upper(),
                "X-Image-Quality": str(quality) if img_format in ["jpeg", "webp"] else "lossless"
            }
        )
        
    except HTTPException:
        raise
    except ValueError as ve:
        logger.error(f"Invalid stream_id format: {ve}")
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error getting frame image for stream {stream_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve frame image")


@router.get("/frames/image/{stream_id}/latest/with-metadata")
async def get_latest_frame_image_with_metadata(
    stream_id: str,
    quality: int = Query(85, ge=1, le=100, description="JPEG quality"),
    width: Optional[int] = Query(None, ge=1, le=4096, description="Resize width"),
    height: Optional[int] = Query(None, ge=1, le=4096, description="Resize height"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the latest frame with complete metadata as JSON.
    
    Returns both the frame (base64) and all detection metadata including:
    - Detection counts (person, male, female)
    - Fire status
    - Location information
    - Camera details
    - Timestamp
    
    Example: GET /api/frames/image/{stream_id}/latest/with-metadata?width=640&height=480
    """
    try:
        # Get workspace and user info
        username = current_user_data["username"]
        user_id = UUID(str(current_user_data["user_id"]))
        
        # Get workspace
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Validate stream exists and user has access
        stream_uuid = UUID(stream_id)
        
        stream_query = """
            SELECT vs.stream_id, vs.workspace_id, vs.name, vs.location, 
                   vs.area, vs.building, vs.floor_level, vs.zone,
                   vs.is_streaming, vs.status
            FROM video_stream vs
            WHERE vs.stream_id = $1 AND vs.workspace_id = $2
        """
        stream_data = await db_manager.execute_query(
            stream_query, (stream_uuid, workspace_id_obj), fetch_one=True
        )
        
        if not stream_data:
            raise HTTPException(status_code=404, detail="Stream not found or access denied")
        
        # Get workspace role
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=workspace_id_obj
        )
        user_workspace_role = membership.get("role")
        user_system_role = current_user_data.get("role", "user")
        
        # Use detection_data_service to get latest frame
        from app.services.detection_data_service import detection_data_service
        
        result = await detection_data_service.retrieve_detection_data(
            workspace_id=workspace_id_obj,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=username,
            camera_id=[stream_id],
            page=1,
            per_page=1,
            include_frame=True
        )
        
        # Validate result
        if not result or not result.get("data") or len(result["data"]) == 0:
            raise HTTPException(status_code=404, detail="No frames found for this stream")
        
        # Extract detection data
        detection = result["data"][0]
        frame_base64 = detection.get("frame")
        
        if not frame_base64:
            raise HTTPException(status_code=404, detail="Frame data not available")
        
        # Process image if resize requested
        processed_frame = frame_base64
        if frame_base64.startswith("s3://"):
            presigned = await s3_service.get_presigned_url(frame_base64)
            processed_frame = presigned
            
        if width or height:
            try:
                if frame_base64.startswith("s3://"):
                    img_data = await s3_service.get_image_data(frame_base64)
                else:
                    img_data = base64.b64decode(frame_base64)
                    
                img = Image.open(io.BytesIO(img_data))
                
                # Calculate dimensions maintaining aspect ratio
                original_width, original_height = img.size
                
                if width and height:
                    new_size = (width, height)
                elif width:
                    ratio = width / original_width
                    new_size = (width, int(original_height * ratio))
                else:
                    ratio = height / original_height
                    new_size = (int(original_width * ratio), height)
                
                # Resize
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                
                # Convert to JPEG
                buffered = io.BytesIO()
                if img.mode in ('RGBA', 'LA', 'P'):
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                img.save(buffered, format="JPEG", quality=quality, optimize=True)
                buffered.seek(0)
                processed_frame = base64.b64encode(buffered.read()).decode('utf-8')
                
            except Exception as e:
                logger.warning(f"Failed to resize image: {e}, returning original")
        
        # Build comprehensive response
        response_data = {
            "frame": {
                "base64": processed_frame,
                "format": "jpeg",
                "quality": quality,
                "dimensions": {
                    "width": width,
                    "height": height,
                    "resized": bool(width or height)
                }
            },
            "detection": {
                "result_id": detection.get("id"),
                "timestamp": detection.get("metadata", {}).get("timestamp"),
                "date": detection.get("metadata", {}).get("date"),
                "time": detection.get("metadata", {}).get("time"),
                "counts": {
                    "person": detection.get("metadata", {}).get("person_count", 0),
                    "male": detection.get("metadata", {}).get("male_count", 0),
                    "female": detection.get("metadata", {}).get("female_count", 0)
                },
                "fire_status": detection.get("metadata", {}).get("fire_status", "no detection")
            },
            "camera": {
                "stream_id": stream_id,
                "name": stream_data["name"],
                "is_streaming": stream_data["is_streaming"],
                "status": stream_data["status"]
            },
            "location": {
                "location": stream_data.get("location"),
                "area": stream_data.get("area"),
                "building": stream_data.get("building"),
                "floor_level": stream_data.get("floor_level"),
                "zone": stream_data.get("zone")
            },
            "metadata": {
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "workspace_id": str(workspace_id_obj)
            }
        }
        
        return JSONResponse(content=response_data)
        
    except HTTPException:
        raise
    except ValueError as ve:
        logger.error(f"Invalid stream_id format: {ve}")
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error getting frame with metadata for stream {stream_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve frame with metadata")


@router.post("/frames/images/batch", response_class=Response)
async def get_batch_frame_images(
    request: FrameRequest,
    output_format: str = Query("json", pattern="^(json|zip)$", description="Response format: json or zip"),
    image_format: str = Query("webp", pattern="^(jpeg|jpg|webp|png)$", description="Image format"),
    quality: int = Query(85, ge=1, le=100, description="Image quality (JPEG/WebP)"),
    width: Optional[int] = Query(None, ge=1, le=4096, description="Resize width"),
    height: Optional[int] = Query(None, ge=1, le=4096, description="Resize height"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get multiple frames as images in batch with metadata.
    
    Supports multiple image formats:
    - jpeg: Standard JPEG (best compatibility)
    - webp: Modern WebP (30-50% smaller, recommended)
    - png: Lossless PNG (largest size, perfect quality)
    
    Supports two output formats:
    - json: Returns array of base64 images with metadata
    - zip: Returns a ZIP file containing images and metadata.json
    
    Example JSON response:
    {
        "frames": [
            {
                "stream_id": "...",
                "camera_name": "...",
                "image_base64": "...",
                "timestamp": "...",
                "detection": {...}
            }
        ],
        "total_frames": 10,
        "format": "json",
        "image_format": "webp"
    }
    
    Example ZIP contents:
    - camera1_20240209_143022.webp
    - camera2_20240209_143023.webp
    - metadata.json
    
    Example usage:
    - POST /frames/images/batch?output_format=zip&image_format=webp&quality=90
    - POST /frames/images/batch?output_format=json&image_format=png
    """
    user_id_str = str(current_user_data["user_id"])
    username = current_user_data["username"]
    
    try:
        from app.services.detection_data_service import detection_data_service
        import zipfile
        
        # Normalize image format
        img_format = image_format.lower()
        if img_format == "jpg":
            img_format = "jpeg"
        
        # Get workspace
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace found.")
        
        # Verify workspace access
        await check_workspace_access(
            db_manager,
            UUID(user_id_str),
            workspace_id_obj,
            required_role=None
        )
        
        # Validate stream IDs
        stream_uuids = [UUID(sid) for sid in request.stream_ids]
        
        # Verify streams exist and get metadata
        access_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.location, vs.area,
                   vs.building, vs.floor_level, vs.zone, vs.is_streaming, vs.status
            FROM video_stream vs
            WHERE vs.stream_id = ANY($1) AND vs.workspace_id = $2
        """
        accessible_streams = await db_manager.execute_query(
            access_query,
            (stream_uuids, workspace_id_obj),
            fetch_all=True
        )
        
        if not accessible_streams:
            raise HTTPException(status_code=404, detail="No accessible streams found")
        
        stream_map = {str(s['stream_id']): s for s in accessible_streams}
        
        # Get workspace role for detection_data_service
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=UUID(user_id_str),
            workspace_id=workspace_id_obj
        )
        user_workspace_role = membership.get("role")
        user_system_role = current_user_data.get("role", "user")
        
        frames_data = []
        metadata_list = []
        
        # Helper function to convert image to specified format
        def convert_image_format(frame_base64_input, target_format, target_quality, resize_width=None, resize_height=None):
            """Convert base64 image to target format and optionally resize"""
            try:
                img_data = base64.b64decode(frame_base64_input)
                img = Image.open(io.BytesIO(img_data))
                
                # Resize if requested
                if resize_width or resize_height:
                    original_width, original_height = img.size
                    
                    if resize_width and resize_height:
                        new_size = (resize_width, resize_height)
                    elif resize_width:
                        ratio = resize_width / original_width
                        new_size = (resize_width, int(original_height * ratio))
                    else:
                        ratio = resize_height / original_height
                        new_size = (int(original_width * ratio), resize_height)
                    
                    img = img.resize(new_size, Image.Resampling.LANCZOS)
                
                # Convert based on format
                buffered = io.BytesIO()
                
                if target_format == "webp":
                    if img.mode in ('RGBA', 'LA'):
                        img.save(buffered, format="WEBP", quality=target_quality, method=6, lossless=False)
                    elif img.mode == 'P':
                        img = img.convert('RGBA')
                        img.save(buffered, format="WEBP", quality=target_quality, method=6, lossless=False)
                    else:
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        img.save(buffered, format="WEBP", quality=target_quality, method=6, lossless=False)
                    file_ext = "webp"
                    
                elif target_format == "png":
                    if img.mode in ('RGBA', 'LA', 'P'):
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        img.save(buffered, format="PNG", optimize=True)
                    else:
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        img.save(buffered, format="PNG", optimize=True)
                    file_ext = "png"
                    
                else:  # jpeg
                    if img.mode in ('RGBA', 'LA', 'P'):
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                        img = background
                    elif img.mode != 'RGB':
                        img = img.convert('RGB')
                    img.save(buffered, format="JPEG", quality=target_quality, optimize=True)
                    file_ext = "jpg"
                
                buffered.seek(0)
                return base64.b64encode(buffered.read()).decode('utf-8'), file_ext
                
            except Exception as e:
                logger.error(f"Failed to convert image: {e}")
                return frame_base64_input, "jpg"  # Return original on error
        
        # Process each stream
        for stream_id in request.stream_ids:
            if stream_id not in stream_map:
                continue
            
            try:
                stream_metadata = stream_map[stream_id]
                
                # Build start/end date strings for detection_data_service
                start_date = request.start_time.strftime("%Y-%m-%d") if request.start_time else None
                end_date = request.end_time.strftime("%Y-%m-%d") if request.end_time else None
                start_time = request.start_time.strftime("%H:%M:%S") if request.start_time else None
                end_time = request.end_time.strftime("%H:%M:%S") if request.end_time else None
                
                # Use detection_data_service to retrieve data
                result = await detection_data_service.retrieve_detection_data(
                    workspace_id=workspace_id_obj,
                    user_system_role=user_system_role,
                    user_workspace_role=user_workspace_role,
                    requesting_username=username,
                    camera_id=[stream_id],
                    start_date=start_date,
                    end_date=end_date,
                    start_time=start_time,
                    end_time=end_time,
                    page=1,
                    per_page=request.limit,
                    include_frame=True
                )
                
                if not result or not result.get("data"):
                    continue
                
                # Process each detection result
                for detection in result["data"]:
                    frame_base64 = detection.get("frame")
                    if not frame_base64:
                        continue
                    
                    metadata_dict = detection.get("metadata", {})
                    timestamp = metadata_dict.get("timestamp")
                    
                    # Convert image to target format
                    processed_frame, file_ext = convert_image_format(
                        frame_base64, 
                        img_format, 
                        quality,
                        width,
                        height
                    )
                    
                    # Generate filename
                    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%Y%m%d_%H%M%S") if timestamp else "unknown"
                    filename = f"{stream_metadata['name']}_{timestamp_str}.{file_ext}"
                    
                    frame_info = {
                        "stream_id": stream_id,
                        "result_id": detection.get("id"),
                        "camera_name": stream_metadata['name'],
                        "filename": filename,
                        "image_base64": processed_frame,
                        "image_format": img_format,
                        "timestamp": timestamp,
                        "detection": {
                            "person_count": metadata_dict.get("person_count", 0),
                            "male_count": metadata_dict.get("male_count", 0),
                            "female_count": metadata_dict.get("female_count", 0),
                            "fire_status": metadata_dict.get("fire_status", "no detection")
                        },
                        "location": {
                            "location": stream_metadata.get('location'),
                            "area": stream_metadata.get('area'),
                            "building": stream_metadata.get('building'),
                            "floor_level": stream_metadata.get('floor_level'),
                            "zone": stream_metadata.get('zone')
                        }
                    }
                    
                    frames_data.append(frame_info)
                    metadata_list.append({
                        "filename": filename,
                        "stream_id": stream_id,
                        "result_id": detection.get("id"),
                        "camera_name": stream_metadata['name'],
                        "timestamp": timestamp,
                        "detection": frame_info["detection"],
                        "location": frame_info["location"]
                    })
                
            except Exception as e_stream:
                logger.error(f"Error retrieving frames for stream {stream_id}: {e_stream}", exc_info=True)
        
        if not frames_data:
            raise HTTPException(status_code=404, detail="No frames found")
        
        # Calculate estimated size savings (WebP vs JPEG)
        size_info = {}
        if img_format == "webp":
            size_info["note"] = "WebP format provides 30-50% smaller file sizes compared to JPEG"
        elif img_format == "png":
            size_info["note"] = "PNG format provides lossless quality but larger file sizes"
        
        # Return based on output format
        if output_format == "json":
            return JSONResponse(content={
                "frames": frames_data,
                "total_frames": len(frames_data),
                "total_streams": len(request.stream_ids),
                "format": "json",
                "image_format": img_format,
                "image_settings": {
                    "quality": quality if img_format in ["jpeg", "webp"] else "lossless",
                    "width": width,
                    "height": height
                },
                "size_info": size_info,
                "retrieved_at": datetime.now(timezone.utc).isoformat()
            })
        
        elif output_format == "zip":
            # Create ZIP file in memory
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Add each image
                for frame_info in frames_data:
                    img_data = base64.b64decode(frame_info["image_base64"])
                    zip_file.writestr(frame_info["filename"], img_data)
                
                # Add metadata JSON
                import json
                metadata_json = {
                    "total_frames": len(frames_data),
                    "total_streams": len(request.stream_ids),
                    "image_format": img_format,
                    "image_settings": {
                        "quality": quality if img_format in ["jpeg", "webp"] else "lossless",
                        "width": width,
                        "height": height
                    },
                    "size_info": size_info,
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "frames": metadata_list
                }
                zip_file.writestr("metadata.json", json.dumps(metadata_json, indent=2))
            
            zip_buffer.seek(0)
            
            # Generate filename
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_filename = f"frames_batch_{img_format}_{timestamp_str}.zip"
            
            return Response(
                content=zip_buffer.read(),
                media_type="application/zip",
                headers={
                    "Content-Disposition": f'attachment; filename="{zip_filename}"'
                }
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in batch frame image retrieval: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve batch frames")


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
