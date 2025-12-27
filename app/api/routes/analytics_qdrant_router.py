# app/routes/analytics_qdrant_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import Optional, Dict, List, Any
from zoneinfo import ZoneInfo
from datetime import date, datetime, timezone
from uuid import UUID
from collections import defaultdict
import logging

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.qdrant_service import qdrant_service
from app.services.database import db_manager
from app.utils import (
    check_workspace_access,
    get_workspace_qdrant_collection_name,
    ensure_workspace_qdrant_collection_exists
)
from qdrant_client.http import models as qdrant_models

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics-qdrant", tags=["Analytics-Qdrant"])


# ========== Helper Functions ==========

async def get_workspace_and_check_access(username: str, user_id: UUID):
    """Helper function to get workspace and check access"""
    _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
    if not workspace_id_obj:
        raise HTTPException(
            status_code=400, 
            detail="No active workspace. Please set an active workspace."
        )
    
    await check_workspace_access(
        qdrant_service.db_manager,
        user_id,
        workspace_id_obj,
        required_role=None,
    )
    
    return workspace_id_obj

async def _fetch_all_qdrant_points(
    collection_name: str,
    filter_obj: Optional[qdrant_models.Filter],
    payload_fields: List[str]
) -> List:
    """Fetch all points from Qdrant with pagination"""
    client = qdrant_service.get_client()
    all_points = []
    batch_size = 1000
    offset = None
    
    while True:
        points, next_offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=filter_obj,
            limit=batch_size,
            offset=offset,
            with_payload=payload_fields,
            with_vectors=False
        )
        
        if not points:
            break
            
        all_points.extend(points)
        
        if next_offset is None or len(points) < batch_size:
            break
            
        offset = next_offset
    
    return all_points


def _build_date_filter(
    start_date: Optional[date],
    end_date: Optional[date],
    user_system_role: str,
    user_workspace_role: Optional[str],
    requesting_username: str
) -> Optional[qdrant_models.Filter]:
    """Build Qdrant filter with date range and user access"""
    must_conditions = []
    
    # User access control
    if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
        if requesting_username:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="username",
                    match=qdrant_models.MatchValue(value=requesting_username)
                )
            )
    
    # Date filters
    if start_date or end_date:
        timestamp_range = {}
        
        if start_date:
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=ZoneInfo("Africa/Cairo"))
            timestamp_range["gte"] = start_dt.timestamp()
        
        if end_date:
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=ZoneInfo("Africa/Cairo"))
            timestamp_range["lte"] = end_dt.timestamp()
        
        if timestamp_range:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="timestamp",
                    range=qdrant_models.Range(**timestamp_range)
                )
            )
    
    return qdrant_models.Filter(must=must_conditions) if must_conditions else None


# ========== Analytics Endpoints ==========

@router.get("/cameras/unique")
async def get_unique_cameras(
    request: Request,
    order_by: str = Query("camera_name", regex="^(camera_name|camera_id)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get list of unique cameras from Qdrant
    
    Chart: Unique Cameras List
    Returns: List of distinct cameras with their IDs and names
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter for user access
        filter_obj = _build_date_filter(None, None, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "location", "area", "building", "zone"]
        )
        
        # Extract unique cameras
        unique_cameras = {}
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                if camera_id and camera_id not in unique_cameras:
                    unique_cameras[camera_id] = {
                        "stream_id": camera_id,
                        "camera_name": point.payload.get("name", "Unknown Camera"),
                        "location": point.payload.get("location"),
                        "area": point.payload.get("area"),
                        "building": point.payload.get("building"),
                        "zone": point.payload.get("zone")
                    }
        
        cameras = list(unique_cameras.values())
        
        # Sort by camera_name or camera_id
        if order_by == "camera_name":
            cameras.sort(key=lambda x: x.get("camera_name", ""))
        else:
            cameras.sort(key=lambda x: x.get("stream_id", ""))
        
        return {
            "success": True,
            "count": len(cameras),
            "data": cameras
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching unique cameras from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/frame-counts")
async def get_frame_counts_per_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(5000, le=10000, description="Max datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get total frame counts per camera from Qdrant
    
    Chart: Total Frames per Camera (Bar Chart)
    Returns: Number of records/frames for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "timestamp"]
        )
        
        # Count frames per camera
        camera_stats = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "total_frames": 0,
            "first_frame": None,
            "last_frame": None
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                timestamp = point.payload.get("timestamp")
                
                if camera_id:
                    stats = camera_stats[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["total_frames"] += 1
                    
                    if timestamp:
                        if stats["first_frame"] is None or timestamp < stats["first_frame"]:
                            stats["first_frame"] = timestamp
                        if stats["last_frame"] is None or timestamp > stats["last_frame"]:
                            stats["last_frame"] = timestamp
        
        # Convert to list and format
        data = []
        for stats in camera_stats.values():
            data.append({
                "camera_name": stats["camera_name"],
                "camera_id": stats["camera_id"],
                "total_frames": stats["total_frames"],
                "first_frame": datetime.fromtimestamp(stats["first_frame"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["first_frame"] else None,
                "last_frame": datetime.fromtimestamp(stats["last_frame"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["last_frame"] else None
            })
        
        # Sort by total_frames descending
        data.sort(key=lambda x: x["total_frames"], reverse=True)
        
        return {
            "success": True,
            "count": len(data),
            "limit_per_camera": limit,
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame counts from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/average-people")
async def get_average_people_per_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people recorded by each camera from Qdrant
    
    Chart: Average People per Camera (Bar Chart)
    Returns: Average person count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "location", "person_count"]
        )
        
        # Calculate statistics per camera
        camera_stats = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "location": None,
            "total_count": 0,
            "max_count": 0,
            "min_count": float('inf'),
            "sample_size": 0
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                person_count = point.payload.get("person_count", 0)
                
                if camera_id is not None:
                    stats = camera_stats[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["location"] = point.payload.get("location")
                    stats["total_count"] += person_count
                    stats["sample_size"] += 1
                    stats["max_count"] = max(stats["max_count"], person_count)
                    stats["min_count"] = min(stats["min_count"], person_count)
        
        # Convert to list and calculate averages
        data = []
        for stats in camera_stats.values():
            if stats["sample_size"] > 0:
                data.append({
                    "camera_name": stats["camera_name"],
                    "camera_id": stats["camera_id"],
                    "location": stats["location"],
                    "avg_person_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "max_person_count": stats["max_count"],
                    "min_person_count": stats["min_count"] if stats["min_count"] != float('inf') else 0,
                    "sample_size": stats["sample_size"]
                })
        
        # Sort by avg_person_count descending
        data.sort(key=lambda x: x["avg_person_count"], reverse=True)
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average people from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/average-gender")
async def get_average_gender_per_camera(
    request: Request,
    gender: str = Query("male", regex="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of males or females recorded by each camera from Qdrant
    
    Chart: Average Gender Count per Camera (Bar Chart)
    Returns: Average male/female count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Determine field to use
        count_field = "male_count" if gender == "male" else "female_count"
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "location", count_field]
        )
        
        # Calculate statistics per camera
        camera_stats = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "location": None,
            "total_count": 0,
            "max_count": 0,
            "min_count": float('inf'),
            "sample_size": 0
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                count = point.payload.get(count_field, 0)
                
                if camera_id is not None:
                    stats = camera_stats[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["location"] = point.payload.get("location")
                    stats["total_count"] += count
                    stats["sample_size"] += 1
                    stats["max_count"] = max(stats["max_count"], count)
                    stats["min_count"] = min(stats["min_count"], count)
        
        # Convert to list and calculate averages
        data = []
        for stats in camera_stats.values():
            if stats["sample_size"] > 0:
                data.append({
                    "camera_name": stats["camera_name"],
                    "camera_id": stats["camera_id"],
                    "location": stats["location"],
                    "avg_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "max_count": stats["max_count"],
                    "min_count": stats["min_count"] if stats["min_count"] != float('inf') else 0,
                    "sample_size": stats["sample_size"],
                    "gender": gender
                })
        
        # Sort by avg_count descending
        data.sort(key=lambda x: x["avg_count"], reverse=True)
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average gender from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/zones/gender-by-weekday")
async def get_gender_by_zone_and_weekday(
    request: Request,
    gender: str = Query("male", regex="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of males/females in each zone by weekday from Qdrant
    
    Chart: Gender Count by Zone and Weekday (Heatmap/Line Chart)
    Returns: Average gender count grouped by zone and day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Determine field to use
        count_field = "male_count" if gender == "male" else "female_count"
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["zone", "timestamp", count_field]
        )
        
        # Group by zone and weekday
        zone_weekday_stats = defaultdict(lambda: {
            "total_count": 0,
            "sample_size": 0
        })
        
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        for point in all_points:
            if point.payload:
                zone = point.payload.get("zone") or "Unknown"
                timestamp = point.payload.get("timestamp")
                count = point.payload.get(count_field, 0)
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    weekday_num = dt.weekday()
                    weekday_name = weekday_names[weekday_num]
                    
                    key = (zone, weekday_num, weekday_name)
                    stats = zone_weekday_stats[key]
                    stats["total_count"] += count
                    stats["sample_size"] += 1
        
        # Convert to list and calculate averages
        data = []
        for (zone, weekday_num, weekday_name), stats in zone_weekday_stats.items():
            if stats["sample_size"] > 0:
                data.append({
                    "zone": zone,
                    "weekday_name": weekday_name,
                    "weekday_num": weekday_num,
                    "avg_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "sample_size": stats["sample_size"],
                    "gender": gender
                })
        
        # Sort by weekday_num, then zone
        data.sort(key=lambda x: (x["weekday_num"], x["zone"]))
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching gender by zone/weekday from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/busiest-hours/by-weekday")
async def get_busiest_hour_per_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the busiest hour for each weekday from Qdrant
    
    Chart: Busiest Hour by Weekday (Bar Chart)
    Returns: Hour with highest average person count for each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["timestamp", "person_count"]
        )
        
        # Group by weekday and hour
        weekday_hour_stats = defaultdict(lambda: {
            "total_count": 0,
            "sample_size": 0
        })
        
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        for point in all_points:
            if point.payload:
                timestamp = point.payload.get("timestamp")
                person_count = point.payload.get("person_count", 0)
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    weekday_num = dt.weekday()
                    weekday_name = weekday_names[weekday_num]
                    hour = dt.hour
                    
                    key = (weekday_num, weekday_name, hour)
                    stats = weekday_hour_stats[key]
                    stats["total_count"] += person_count
                    stats["sample_size"] += 1
        
        # Calculate averages and find busiest hour per weekday
        weekday_busiest = {}
        
        for (weekday_num, weekday_name, hour), stats in weekday_hour_stats.items():
            if stats["sample_size"] > 0:
                avg_count = stats["total_count"] / stats["sample_size"]
                
                if weekday_num not in weekday_busiest or avg_count > weekday_busiest[weekday_num]["avg_person_count"]:
                    weekday_busiest[weekday_num] = {
                        "weekday_name": weekday_name,
                        "weekday_num": weekday_num,
                        "hour": hour,
                        "avg_person_count": round(avg_count, 2)
                    }
        
        # Convert to list
        data = list(weekday_busiest.values())
        data.sort(key=lambda x: x["weekday_num"])
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching busiest hours from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/average-people")
async def get_average_people_by_floor(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level from Qdrant
    
    Chart: Average People by Floor (Bar Chart)
    Returns: Average person count for each floor level
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["floor_level", "person_count"]
        )
        
        # Group by floor level
        floor_stats = defaultdict(lambda: {
            "total_count": 0,
            "max_count": 0,
            "min_count": float('inf'),
            "sample_size": 0
        })
        
        for point in all_points:
            if point.payload:
                floor = point.payload.get("floor_level") or "Unknown"
                person_count = point.payload.get("person_count", 0)
                
                stats = floor_stats[floor]
                stats["total_count"] += person_count
                stats["sample_size"] += 1
                stats["max_count"] = max(stats["max_count"], person_count)
                stats["min_count"] = min(stats["min_count"], person_count)
        
        # Convert to list and calculate averages
        data = []
        for floor, stats in floor_stats.items():
            if stats["sample_size"] > 0:
                data.append({
                    "floor_level": floor,
                    "avg_person_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "max_person_count": stats["max_count"],
                    "min_person_count": stats["min_count"] if stats["min_count"] != float('inf') else 0,
                    "sample_size": stats["sample_size"]
                })
        
        # Sort by floor_level
        data.sort(key=lambda x: x["floor_level"])
        return {
            "success": True,
            "count": len(data),
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average people by floor from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-weekday")
async def get_people_by_floor_and_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and weekday from Qdrant
    
    Chart: People by Floor and Weekday (Grouped Bar/Line Chart)
    Returns: Average person count for each floor on each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter with correct arguments
        filter_obj = _build_date_filter(
            start_date, 
            end_date, 
            user_system_role, 
            user_workspace_role, 
            username
        )
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["floor_level", "timestamp", "person_count"]
        )
        
        # Group by floor and weekday
        floor_weekday_stats = defaultdict(lambda: {
            "total_count": 0,
            "sample_size": 0
        })
        
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        for point in all_points:
            if point.payload:
                floor_level = point.payload.get("floor_level") or "Unknown"
                timestamp = point.payload.get("timestamp")
                person_count = point.payload.get("person_count", 0)
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    weekday_num = dt.weekday()
                    weekday_name = weekday_names[weekday_num]
                    
                    key = (floor_level, weekday_num, weekday_name)
                    stats = floor_weekday_stats[key]
                    stats["total_count"] += person_count
                    stats["sample_size"] += 1
        
        # Convert to list and calculate averages
        data = []
        for (floor_level, weekday_num, weekday_name), stats in floor_weekday_stats.items():
            if stats["sample_size"] > 0:
                data.append({
                    "floor_level": floor_level,
                    "weekday_name": weekday_name,
                    "weekday_num": weekday_num,
                    "avg_person_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "sample_size": stats["sample_size"]
                })
        
        # Sort by weekday then floor
        data.sort(key=lambda x: (x["weekday_num"], x["floor_level"]))
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/weekday data from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-hour")
async def get_people_by_floor_and_hour(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and hour from Qdrant
    
    Chart: People by Floor and Hour (Line Chart/Heatmap)
    Returns: Average person count for each floor at each hour of day
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["floor_level", "timestamp", "person_count"]
        )
        
        # Group by floor and hour
        floor_hour_stats = defaultdict(lambda: {
            "total_count": 0,
            "sample_size": 0
        })
        
        for point in all_points:
            if point.payload:
                floor_level = point.payload.get("floor_level") or "Unknown"
                timestamp = point.payload.get("timestamp")
                person_count = point.payload.get("person_count", 0)
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    hour = dt.hour
                    
                    key = (floor_level, hour)
                    stats = floor_hour_stats[key]
                    stats["total_count"] += person_count
                    stats["sample_size"] += 1
        
        # Convert to list and calculate averages
        data = []
        for (floor_level, hour), stats in floor_hour_stats.items():
            if stats["sample_size"] > 0:
                data.append({
                    "floor_level": floor_level,
                    "hour": hour,
                    "avg_person_count": round(stats["total_count"] / stats["sample_size"], 2),
                    "sample_size": stats["sample_size"]
                })
        
        # Sort by floor_level, then hour
        data.sort(key=lambda x: (x["floor_level"], x["hour"]))
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/hour data from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/frame-comparison")
async def get_camera_frame_comparison(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare camera frame counts to average from Qdrant
    
    Chart: Camera Frame Count Comparison (Bar Chart with threshold line)
    Returns: Each camera's frame count compared to average (Above/Below/Average)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name"]
        )
        
        # Count frames per camera
        camera_frame_counts = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "frame_count": 0
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                if camera_id:
                    stats = camera_frame_counts[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["frame_count"] += 1
        
        # Calculate average
        total_cameras = len(camera_frame_counts)
        if total_cameras == 0:
            return {
                "success": True,
                "summary": {
                    "total_cameras": 0,
                    "above_average": 0,
                    "below_average": 0,
                    "at_average": 0
                },
                "data": []
            }
        
        total_frames = sum(stats["frame_count"] for stats in camera_frame_counts.values())
        avg_frame_count = total_frames / total_cameras
        
        # Build comparison data
        data = []
        above_avg = 0
        below_avg = 0
        at_avg = 0
        
        for stats in camera_frame_counts.values():
            frame_count = stats["frame_count"]
            
            # Determine comparison
            if frame_count > avg_frame_count:
                comparison = "Above Average"
                above_avg += 1
            elif frame_count < avg_frame_count:
                comparison = "Below Average"
                below_avg += 1
            else:
                comparison = "Average"
                at_avg += 1
            
            # Calculate percent difference
            percent_difference = 0
            if avg_frame_count > 0:
                percent_difference = round(
                    ((frame_count - avg_frame_count) / avg_frame_count * 100), 2
                )
            
            data.append({
                "camera_name": stats["camera_name"],
                "camera_id": stats["camera_id"],
                "frame_count": frame_count,
                "avg_frame_count": round(avg_frame_count, 2),
                "comparison": comparison,
                "percent_difference": percent_difference
            })
        
        # Sort by frame_count descending
        data.sort(key=lambda x: x["frame_count"], reverse=True)
        
        return {
            "success": True,
            "summary": {
                "total_cameras": total_cameras,
                "above_average": above_avg,
                "below_average": below_avg,
                "at_average": at_avg
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame comparison from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/timeseries/camera-data")
async def get_camera_timeseries(
    request: Request,
    camera_id: Optional[UUID] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(5000, le=5000, description="Max 5000 datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get time series data for line graph from Qdrant (limited to 5000 datapoints per camera)
    
    Chart: Time Series Line Graph
    Returns: Person count over time for specified camera(s)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build base filter
        must_conditions = []
        
        # User access control
        if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
            if username:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="username",
                        match=qdrant_models.MatchValue(value=username)
                    )
                )
        
        # Camera ID filter
        if camera_id:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="camera_id",
                    match=qdrant_models.MatchValue(value=str(camera_id))
                )
            )
        
        # Date filters
        if start_date or end_date:
            timestamp_range = {}
            
            if start_date:
                start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                timestamp_range["gte"] = start_dt.timestamp()
            
            if end_date:
                end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                timestamp_range["lte"] = end_dt.timestamp()
            
            if timestamp_range:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="timestamp",
                        range=qdrant_models.Range(**timestamp_range)
                    )
                )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Fetch points with ordering by timestamp
        all_points = []
        batch_size = 1000
        offset = None
        
        while len(all_points) < limit * 10:  # Fetch more to account for multiple cameras
            points, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=batch_size,
                offset=offset,
                with_payload=["camera_id", "name", "timestamp", "person_count", 
                             "male_count", "female_count", "location"],
                with_vectors=False,
                order_by=qdrant_models.OrderBy(
                    key="timestamp",
                    direction=qdrant_models.Direction.DESC
                )
            )
            
            if not points:
                break
            
            all_points.extend(points)
            
            if next_offset is None or len(points) < batch_size:
                break
            
            offset = next_offset
        
        # Group by camera and limit per camera
        cameras_data = defaultdict(lambda: {
            "camera_id": None,
            "camera_name": None,
            "location": None,
            "datapoints": []
        })
        
        for point in all_points:
            if point.payload:
                cam_id = point.payload.get("camera_id")
                if cam_id:
                    cam_data = cameras_data[cam_id]
                    
                    # Only add if under limit for this camera
                    if len(cam_data["datapoints"]) < limit:
                        if not cam_data["camera_id"]:
                            cam_data["camera_id"] = cam_id
                            cam_data["camera_name"] = point.payload.get("name", "Unknown Camera")
                            cam_data["location"] = point.payload.get("location")
                        
                        timestamp = point.payload.get("timestamp")
                        if timestamp:
                            cam_data["datapoints"].append({
                                "timestamp": datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo")).isoformat(),
                                "person_count": point.payload.get("person_count", 0),
                                "male_count": point.payload.get("male_count", 0),
                                "female_count": point.payload.get("female_count", 0)
                            })
        
        # Sort datapoints by timestamp ascending for each camera
        for cam_data in cameras_data.values():
            cam_data["datapoints"].sort(key=lambda x: x["timestamp"])
        
        result_data = list(cameras_data.values())
        
        return {
            "success": True,
            "limit_per_camera": limit,
            "cameras_count": len(result_data),
            "data": result_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching timeseries data from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detection-summary")
async def get_fire_detection_summary(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get summary of fire detections across all cameras from Qdrant
    
    Chart: Fire Detection Summary (Pie Chart / Stats Cards)
    Returns: Count of fire detections by status
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["fire_status", "camera_id", "timestamp"]
        )
        
        # Count by fire status
        status_counts = defaultdict(lambda: {
            "detection_count": 0,
            "affected_cameras": set(),
            "days_with_detections": set(),
            "first_detection": None,
            "last_detection": None
        })
        
        for point in all_points:
            if point.payload:
                fire_status = point.payload.get("fire_status", "no detection")
                camera_id = point.payload.get("camera_id")
                timestamp = point.payload.get("timestamp")
                
                stats = status_counts[fire_status]
                stats["detection_count"] += 1
                
                if camera_id:
                    stats["affected_cameras"].add(camera_id)
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    stats["days_with_detections"].add(dt.date())
                    
                    if stats["first_detection"] is None or timestamp < stats["first_detection"]:
                        stats["first_detection"] = timestamp
                    if stats["last_detection"] is None or timestamp > stats["last_detection"]:
                        stats["last_detection"] = timestamp
        
        # Convert to list format
        data = []
        total_detections = 0
        fire_detections = 0
        
        for fire_status, stats in status_counts.items():
            count = stats["detection_count"]
            total_detections += count
            
            if fire_status != "no detection":
                fire_detections += count
            
            data.append({
                "fire_status": fire_status,
                "detection_count": count,
                "affected_cameras": len(stats["affected_cameras"]),
                "days_with_detections": len(stats["days_with_detections"]),
                "first_detection": datetime.fromtimestamp(stats["first_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["first_detection"] else None,
                "last_detection": datetime.fromtimestamp(stats["last_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["last_detection"] else None
            })
        
        # Sort by detection count
        data.sort(key=lambda x: x["detection_count"], reverse=True)
        
        return {
            "success": True,
            "summary": {
                "total_records": total_detections,
                "fire_detections": fire_detections,
                "no_detection_records": total_detections - fire_detections,
                "fire_detection_percentage": round((fire_detections / total_detections * 100), 2) if total_detections > 0 else 0
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detection summary from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-camera")
async def get_fire_detections_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection counts per camera from Qdrant
    
    Chart: Fire Detections by Camera (Bar Chart)
    Returns: Number of fire detections for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "location", "area", "building", "zone", "fire_status", "timestamp"]
        )
        
        # Calculate statistics per camera
        camera_stats = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "location": None,
            "area": None,
            "building": None,
            "zone": None,
            "total_checks": 0,
            "fire_detections": 0,
            "days_with_fire": set(),
            "first_fire_detection": None,
            "last_fire_detection": None
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                fire_status = point.payload.get("fire_status", "no detection")
                timestamp = point.payload.get("timestamp")
                
                if camera_id:
                    stats = camera_stats[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["location"] = point.payload.get("location")
                    stats["area"] = point.payload.get("area")
                    stats["building"] = point.payload.get("building")
                    stats["zone"] = point.payload.get("zone")
                    stats["total_checks"] += 1
                    
                    if fire_status != "no detection":
                        stats["fire_detections"] += 1
                        
                        if timestamp:
                            dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                            stats["days_with_fire"].add(dt.date())
                            
                            if stats["first_fire_detection"] is None or timestamp < stats["first_fire_detection"]:
                                stats["first_fire_detection"] = timestamp
                            if stats["last_fire_detection"] is None or timestamp > stats["last_fire_detection"]:
                                stats["last_fire_detection"] = timestamp
        
        # Convert to list and calculate rates
        data = []
        for stats in camera_stats.values():
            fire_detection_rate = 0
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            data.append({
                "camera_name": stats["camera_name"],
                "camera_id": stats["camera_id"],
                "location": stats["location"],
                "area": stats["area"],
                "building": stats["building"],
                "zone": stats["zone"],
                "total_checks": stats["total_checks"],
                "fire_detections": stats["fire_detections"],
                "days_with_fire": len(stats["days_with_fire"]),
                "first_fire_detection": datetime.fromtimestamp(stats["first_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["first_fire_detection"] else None,
                "last_fire_detection": datetime.fromtimestamp(stats["last_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["last_fire_detection"] else None,
                "fire_detection_rate": fire_detection_rate
            })
        
        # Sort by fire_detections descending
        data.sort(key=lambda x: (x["fire_detections"], x["camera_name"]), reverse=True)
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by camera from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-location")
async def get_fire_detections_by_location(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    group_by: str = Query("location", regex="^(location|area|building|zone|floor_level)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection counts by location grouping from Qdrant
    
    Chart: Fire Detections by Location (Bar Chart / Heatmap)
    Returns: Number of fire detections grouped by location, area, building, zone, or floor
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            [group_by, "camera_id", "fire_status", "timestamp"]
        )
        
        # Group statistics
        location_stats = defaultdict(lambda: {
            "total_checks": 0,
            "fire_detections": 0,
            "cameras_in_group": set(),
            "days_with_fire": set(),
            "first_fire_detection": None,
            "last_fire_detection": None
        })
        
        for point in all_points:
            if point.payload:
                group_value = point.payload.get(group_by) or "Unknown"
                camera_id = point.payload.get("camera_id")
                fire_status = point.payload.get("fire_status", "no detection")
                timestamp = point.payload.get("timestamp")
                
                stats = location_stats[group_value]
                stats["total_checks"] += 1
                
                if camera_id:
                    stats["cameras_in_group"].add(camera_id)
                
                if fire_status != "no detection":
                    stats["fire_detections"] += 1
                    
                    if timestamp:
                        dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                        stats["days_with_fire"].add(dt.date())
                        
                        if stats["first_fire_detection"] is None or timestamp < stats["first_fire_detection"]:
                            stats["first_fire_detection"] = timestamp
                        if stats["last_fire_detection"] is None or timestamp > stats["last_fire_detection"]:
                            stats["last_fire_detection"] = timestamp
        
        # Convert to list
        data = []
        for group_name, stats in location_stats.items():
            fire_detection_rate = 0
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            data.append({
                "group_name": group_name,
                "group_type": group_by,
                "total_checks": stats["total_checks"],
                "fire_detections": stats["fire_detections"],
                "cameras_in_group": len(stats["cameras_in_group"]),
                "days_with_fire": len(stats["days_with_fire"]),
                "first_fire_detection": datetime.fromtimestamp(stats["first_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["first_fire_detection"] else None,
                "last_fire_detection": datetime.fromtimestamp(stats["last_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["last_fire_detection"] else None,
                "fire_detection_rate": fire_detection_rate
            })
        
        # Sort by fire_detections descending
        data.sort(key=lambda x: x["fire_detections"], reverse=True)
        
        return {
            "success": True,
            "group_by": group_by,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by location from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-timeline")
async def get_fire_detections_timeline(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = Query("day", regex="^(hour|day|week|month)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detections over time from Qdrant
    
    Chart: Fire Detections Timeline (Line Chart / Area Chart)
    Returns: Fire detection counts aggregated by time interval
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["timestamp", "fire_status", "camera_id"]
        )
        
        # Group by time period
        timeline_stats = defaultdict(lambda: {
            "total_checks": 0,
            "fire_detections": 0,
            "cameras_checked": set(),
            "cameras_with_fire": set()
        })
        
        for point in all_points:
            if point.payload:
                timestamp = point.payload.get("timestamp")
                fire_status = point.payload.get("fire_status", "no detection")
                camera_id = point.payload.get("camera_id")
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    
                    # Truncate based on interval
                    if interval == "hour":
                        time_key = dt.replace(minute=0, second=0, microsecond=0)
                    elif interval == "day":
                        time_key = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    elif interval == "week":
                        # Get start of week (Monday)
                        days_since_monday = dt.weekday()
                        time_key = (dt - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
                    else:  # month
                        time_key = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    
                    stats = timeline_stats[time_key]
                    stats["total_checks"] += 1
                    
                    if camera_id:
                        stats["cameras_checked"].add(camera_id)
                    
                    if fire_status != "no detection":
                        stats["fire_detections"] += 1
                        if camera_id:
                            stats["cameras_with_fire"].add(camera_id)
        
        # Convert to list
        data = []
        for time_period, stats in timeline_stats.items():
            fire_detection_rate = 0
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            data.append({
                "time_period": time_period.isoformat(),
                "total_checks": stats["total_checks"],
                "fire_detections": stats["fire_detections"],
                "cameras_checked": len(stats["cameras_checked"]),
                "cameras_with_fire": len(stats["cameras_with_fire"]),
                "fire_detection_rate": fire_detection_rate
            })
        
        # Sort by time_period descending
        data.sort(key=lambda x: x["time_period"], reverse=True)
        
        return {
            "success": True,
            "interval": interval,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections timeline from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-weekday")
async def get_fire_detections_by_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection patterns by day of week from Qdrant
    
    Chart: Fire Detections by Weekday (Bar Chart)
    Returns: Fire detection counts for each day of the week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["timestamp", "fire_status"]
        )
        
        # Group by weekday
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekday_stats = defaultdict(lambda: {
            "total_checks": 0,
            "fire_detections": 0,
            "days_sampled": set()
        })
        
        for point in all_points:
            if point.payload:
                timestamp = point.payload.get("timestamp")
                fire_status = point.payload.get("fire_status", "no detection")
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    weekday_num = dt.weekday()
                    weekday_name = weekday_names[weekday_num]
                    
                    stats = weekday_stats[weekday_num]
                    stats["weekday_name"] = weekday_name
                    stats["total_checks"] += 1
                    stats["days_sampled"].add(dt.date())
                    
                    if fire_status != "no detection":
                        stats["fire_detections"] += 1
        
        # Convert to list
        data = []
        for weekday_num, stats in weekday_stats.items():
            fire_detection_rate = 0
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            data.append({
                "weekday_name": stats["weekday_name"],
                "weekday_num": weekday_num,
                "total_checks": stats["total_checks"],
                "fire_detections": stats["fire_detections"],
                "days_sampled": len(stats["days_sampled"]),
                "fire_detection_rate": fire_detection_rate
            })
        
        # Sort by weekday_num
        data.sort(key=lambda x: x["weekday_num"])
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by weekday from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-hour")
async def get_fire_detections_by_hour(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection patterns by hour of day from Qdrant
    
    Chart: Fire Detections by Hour (Line Chart / Heatmap)
    Returns: Fire detection counts for each hour of the day
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["timestamp", "fire_status"]
        )
        
        # Group by hour
        hour_stats = defaultdict(lambda: {
            "total_checks": 0,
            "fire_detections": 0,
            "days_sampled": set()
        })
        
        for point in all_points:
            if point.payload:
                timestamp = point.payload.get("timestamp")
                fire_status = point.payload.get("fire_status", "no detection")
                
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                    hour = dt.hour
                    
                    stats = hour_stats[hour]
                    stats["total_checks"] += 1
                    stats["days_sampled"].add(dt.date())
                    
                    if fire_status != "no detection":
                        stats["fire_detections"] += 1
        
        # Convert to list
        data = []
        for hour, stats in hour_stats.items():
            fire_detection_rate = 0
            avg_detections_per_day = 0
            
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            if len(stats["days_sampled"]) > 0:
                avg_detections_per_day = round(stats["fire_detections"] / len(stats["days_sampled"]), 2)
            
            data.append({
                "hour": hour,
                "total_checks": stats["total_checks"],
                "fire_detections": stats["fire_detections"],
                "days_sampled": len(stats["days_sampled"]),
                "fire_detection_rate": fire_detection_rate,
                "avg_detections_per_day": avg_detections_per_day
            })
        
        # Sort by hour
        data.sort(key=lambda x: x["hour"])
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by hour from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/recent-detections")
async def get_recent_fire_detections(
    request: Request,
    limit: int = Query(50, le=500, description="Max number of recent detections"),
    camera_id: Optional[UUID] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get most recent fire detections from Qdrant
    
    Chart: Recent Fire Detection Events (Table / Timeline)
    Returns: List of recent fire detection events with details
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        must_conditions = []
        
        # User access control
        if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
            if username:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="username",
                        match=qdrant_models.MatchValue(value=username)
                    )
                )
        
        # Camera ID filter
        if camera_id:
            must_conditions.append(
                qdrant_models.FieldCondition(
                    key="camera_id",
                    match=qdrant_models.MatchValue(value=str(camera_id))
                )
            )
        
        # Fire status filter (only fire detections)
        must_conditions.append(
            qdrant_models.FieldCondition(
                key="fire_status",
                match=qdrant_models.MatchExcept(**{"except": ["no detection"]})
            )
        )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Fetch points with ordering by timestamp
        points, _ = client.scroll(
            collection_name=collection_name,
            scroll_filter=filter_obj,
            limit=limit,
            with_payload=["camera_id", "name", "timestamp", "fire_status", "person_count",
                         "location", "area", "building", "zone", "floor_level"],
            with_vectors=False,
            order_by=qdrant_models.OrderBy(
                key="timestamp",
                direction=qdrant_models.Direction.DESC
            )
        )
        
        # Convert to list
        data = []
        for point in points:
            if point.payload:
                timestamp = point.payload.get("timestamp")
                data.append({
                    "result_id": str(point.id),
                    "camera_id": point.payload.get("camera_id"),
                    "camera_name": point.payload.get("name", "Unknown Camera"),
                    "timestamp": datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo")).isoformat() if timestamp else None,
                    "fire_status": point.payload.get("fire_status"),
                    "person_count": point.payload.get("person_count", 0),
                    "location": point.payload.get("location"),
                    "area": point.payload.get("area"),
                    "building": point.payload.get("building"),
                    "zone": point.payload.get("zone"),
                    "floor_level": point.payload.get("floor_level")
                })
        
        return {
            "success": True,
            "limit": limit,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching recent fire detections from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/high-risk-cameras")
async def get_high_risk_cameras(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    threshold_percentage: float = Query(5.0, ge=0, le=100, description="Min fire detection rate to be considered high risk"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get cameras with high fire detection rates from Qdrant
    
    Chart: High Risk Cameras (Table / Alert List)
    Returns: Cameras with fire detection rate above threshold
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    user_system_role = current_user_data.get("system_role", "user")
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        workspace_role_info = await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        user_workspace_role = workspace_role_info.get("role")
        
        # Get Qdrant collection
        collection_name = get_workspace_qdrant_collection_name(workspace_id_obj)
        client = qdrant_service.get_client()
        await ensure_workspace_qdrant_collection_exists(client, workspace_id_obj)
        
        # Build filter
        filter_obj = _build_date_filter(start_date, end_date, user_system_role, user_workspace_role, username)
        
        # Fetch all points
        all_points = await _fetch_all_qdrant_points(
            collection_name,
            filter_obj,
            ["camera_id", "name", "location", "area", "building", "zone", "floor_level", "fire_status", "timestamp"]
        )
        
        # Calculate statistics per camera
        camera_stats = defaultdict(lambda: {
            "camera_name": None,
            "camera_id": None,
            "location": None,
            "area": None,
            "building": None,
            "zone": None,
            "floor_level": None,
            "total_checks": 0,
            "fire_detections": 0,
            "first_fire_detection": None,
            "last_fire_detection": None,
            "days_with_fire": set()
        })
        
        for point in all_points:
            if point.payload:
                camera_id = point.payload.get("camera_id")
                fire_status = point.payload.get("fire_status", "no detection")
                timestamp = point.payload.get("timestamp")
                
                if camera_id:
                    stats = camera_stats[camera_id]
                    stats["camera_id"] = camera_id
                    stats["camera_name"] = point.payload.get("name", "Unknown Camera")
                    stats["location"] = point.payload.get("location")
                    stats["area"] = point.payload.get("area")
                    stats["building"] = point.payload.get("building")
                    stats["zone"] = point.payload.get("zone")
                    stats["floor_level"] = point.payload.get("floor_level")
                    stats["total_checks"] += 1
                    
                    if fire_status != "no detection":
                        stats["fire_detections"] += 1
                        
                        if timestamp:
                            dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo"))
                            stats["days_with_fire"].add(dt.date())
                            
                            if stats["first_fire_detection"] is None or timestamp < stats["first_fire_detection"]:
                                stats["first_fire_detection"] = timestamp
                            if stats["last_fire_detection"] is None or timestamp > stats["last_fire_detection"]:
                                stats["last_fire_detection"] = timestamp
        
        # Filter cameras above threshold and convert to list
        data = []
        for stats in camera_stats.values():
            fire_detection_rate = 0
            if stats["total_checks"] > 0:
                fire_detection_rate = round((stats["fire_detections"] / stats["total_checks"] * 100), 2)
            
            # Only include if above threshold
            if fire_detection_rate >= threshold_percentage:
                data.append({
                    "camera_name": stats["camera_name"],
                    "camera_id": stats["camera_id"],
                    "location": stats["location"],
                    "area": stats["area"],
                    "building": stats["building"],
                    "zone": stats["zone"],
                    "floor_level": stats["floor_level"],
                    "total_checks": stats["total_checks"],
                    "fire_detections": stats["fire_detections"],
                    "fire_detection_rate": fire_detection_rate,
                    "first_fire_detection": datetime.fromtimestamp(stats["first_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["first_fire_detection"] else None,
                    "last_fire_detection": datetime.fromtimestamp(stats["last_fire_detection"], tz=ZoneInfo("Africa/Cairo")).isoformat() if stats["last_fire_detection"] else None,
                    "days_with_fire": len(stats["days_with_fire"])
                })
        
        # Sort by fire_detection_rate descending
        data.sort(key=lambda x: (x["fire_detection_rate"], x["fire_detections"]), reverse=True)
        
        return {
            "success": True,
            "threshold_percentage": threshold_percentage,
            "high_risk_count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching high risk cameras from Qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
   