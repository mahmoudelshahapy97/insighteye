# app/routes/analytics_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import Optional, Dict
from datetime import date
from uuid import UUID
import logging

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.database import db_manager
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/cameras/unique")
async def get_unique_cameras(
    request: Request,
    order_by: str = Query("camera_name", regex="^(camera_name|camera_id)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get list of unique cameras
    
    Chart: Unique Cameras List
    Returns: List of distinct cameras with their IDs and names
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        query = """
            SELECT DISTINCT
                stream_id,
                camera_name,
                location,
                area,
                building,
                zone
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
            ORDER BY camera_name ASC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj)
            
        cameras = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(cameras),
            "data": cameras
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching unique cameras: {e}", exc_info=True)
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
    Get total frame counts per camera
    
    Chart: Total Frames per Camera (Bar Chart)
    Returns: Number of records/frames for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                COUNT(*) AS total_frames,
                MIN(timestamp) AS first_frame,
                MAX(timestamp) AS last_frame
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id
            ORDER BY total_frames DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "limit_per_camera": limit,
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame counts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/average-people")
async def get_average_people_per_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people recorded by each camera
    
    Chart: Average People per Camera (Bar Chart)
    Returns: Average person count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location
            ORDER BY avg_person_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average people: {e}", exc_info=True)
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
    Get average number of males or females recorded by each camera
    
    Chart: Average Gender Count per Camera (Bar Chart)
    Returns: Average male/female count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                AVG({count_column}) AS avg_count,
                MAX({count_column}) AS max_count,
                MIN({count_column}) AS min_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND {count_column} IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location
            ORDER BY avg_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': float(row['avg_count']) if row['avg_count'] else 0,
            'gender': gender
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average gender: {e}", exc_info=True)
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
    Get average number of males/females in each zone by weekday
    
    Chart: Gender Count by Zone and Weekday (Heatmap/Line Chart)
    Returns: Average gender count grouped by zone and day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(zone, 'Unknown') AS zone,
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                AVG({count_column}) AS avg_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND date IS NOT NULL
              AND {count_column} IS NOT NULL
              {date_filter}
            GROUP BY zone, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, zone
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': float(row['avg_count']) if row['avg_count'] else 0,
            'gender': gender,
            'weekday_name': row['weekday_name'].strip()
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching gender by zone/weekday: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/busiest-hours/by-weekday")
async def get_busiest_hour_per_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the busiest hour for each weekday
    
    Chart: Busiest Hour by Weekday (Bar Chart)
    Returns: Hour with highest average person count for each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH hourly_avg AS (
                SELECT
                    TO_CHAR(date, 'Day') AS weekday_name,
                    EXTRACT(DOW FROM date) AS weekday_num,
                    EXTRACT(HOUR FROM time) AS hour,
                    AVG(person_count) AS avg_person_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND date IS NOT NULL
                  AND time IS NOT NULL
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date), EXTRACT(HOUR FROM time)
            ),
            ranked_hours AS (
                SELECT
                    weekday_name,
                    weekday_num,
                    hour,
                    avg_person_count,
                    ROW_NUMBER() OVER (PARTITION BY weekday_num ORDER BY avg_person_count DESC) AS rn
                FROM hourly_avg
            )
            SELECT
                weekday_name,
                weekday_num,
                hour,
                avg_person_count
            FROM ranked_hours
            WHERE rn = 1
            ORDER BY weekday_num
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching busiest hours: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/average-people")
async def get_average_people_by_floor(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level
    
    Chart: Average People by Floor (Bar Chart)
    Returns: Average person count for each floor level
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY floor_level
            ORDER BY floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor averages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-weekday")
async def get_people_by_floor_and_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and weekday
    
    Chart: People by Floor and Weekday (Grouped Bar/Line Chart)
    Returns: Average person count for each floor on each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                AVG(person_count) AS avg_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              AND date IS NOT NULL
              {date_filter}
            GROUP BY floor_level, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/weekday data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-hour")
async def get_people_by_floor_and_hour(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and hour
    
    Chart: People by Floor and Hour (Line Chart/Heatmap)
    Returns: Average person count for each floor at each hour of day
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                EXTRACT(HOUR FROM time) AS hour,
                AVG(person_count) AS avg_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              AND time IS NOT NULL
              {date_filter}
            GROUP BY floor_level, EXTRACT(HOUR FROM time)
            ORDER BY floor_level, hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/hour data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/frame-comparison")
async def get_camera_frame_comparison(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare camera frame counts to average
    
    Chart: Camera Frame Count Comparison (Bar Chart with threshold line)
    Returns: Each camera's frame count compared to average (Above/Below/Average)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH frame_counts AS (
                SELECT
                    camera_name,
                    camera_id,
                    COUNT(*) AS frame_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                GROUP BY camera_name, camera_id
            ),
            avg_frame AS (
                SELECT AVG(frame_count) AS avg_frame_count
                FROM frame_counts
            )
            SELECT
                f.camera_name,
                f.camera_id,
                f.frame_count,
                a.avg_frame_count,
                CASE
                    WHEN f.frame_count > a.avg_frame_count THEN 'Above Average'
                    WHEN f.frame_count < a.avg_frame_count THEN 'Below Average'
                    ELSE 'Average'
                END AS comparison,
                ROUND(((f.frame_count - a.avg_frame_count) / a.avg_frame_count * 100)::numeric, 2) AS percent_difference
            FROM frame_counts f
            CROSS JOIN avg_frame a
            ORDER BY f.frame_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_frame_count': float(row['avg_frame_count']) if row['avg_frame_count'] else 0,
            'percent_difference': float(row['percent_difference']) if row['percent_difference'] else 0
        } for row in results]
        
        # Calculate summary statistics
        total_cameras = len(data)
        above_avg = len([d for d in data if d['comparison'] == 'Above Average'])
        below_avg = len([d for d in data if d['comparison'] == 'Below Average'])
        
        return {
            "success": True,
            "summary": {
                "total_cameras": total_cameras,
                "above_average": above_avg,
                "below_average": below_avg,
                "at_average": total_cameras - above_avg - below_avg
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame comparison: {e}", exc_info=True)
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
    Get time series data for line graph (limited to 5000 datapoints per camera)
    
    Chart: Time Series Line Graph
    Returns: Person count over time for specified camera(s)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        filters = ["workspace_id = $1"]
        
        if camera_id:
            params.append(camera_id)
            filters.append(f"stream_id = ${len(params)}")
            
        if start_date:
            params.append(start_date)
            filters.append(f"date >= ${len(params)}")
        if end_date:
            params.append(end_date)
            filters.append(f"date <= ${len(params)}")
            
        where_clause = " AND ".join(filters)
        
        # Use window function to limit results per camera
        query = f"""
            WITH ranked_data AS (
                SELECT
                    stream_id,
                    camera_name,
                    timestamp,
                    person_count,
                    male_count,
                    female_count,
                    location,
                    ROW_NUMBER() OVER (
                        PARTITION BY stream_id 
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM stream_results
                WHERE {where_clause}
            )
            SELECT
                stream_id,
                camera_name,
                timestamp,
                person_count,
                male_count,
                female_count,
                location
            FROM ranked_data
            WHERE rn <= ${len(params) + 1}
            ORDER BY stream_id, timestamp ASC
        """
        
        params.append(limit)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Group by camera
        cameras_data = {}
        for row in results:
            cam_id = str(row['stream_id'])
            if cam_id not in cameras_data:
                cameras_data[cam_id] = {
                    "camera_id": cam_id,
                    "camera_name": row['camera_name'],
                    "location": row['location'],
                    "datapoints": []
                }
            cameras_data[cam_id]["datapoints"].append({
                "timestamp": row['timestamp'].isoformat(),
                "person_count": row['person_count'],
                "male_count": row['male_count'],
                "female_count": row['female_count']
            })
        
        return {
            "success": True,
            "limit_per_camera": limit,
            "cameras_count": len(cameras_data),
            "data": list(cameras_data.values())
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching timeseries data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detection-summary")
async def get_fire_detection_summary(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get summary of fire detections across all cameras
    
    Chart: Fire Detection Summary (Pie Chart / Stats Cards)
    Returns: Count of fire detections by status
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                fire_status,
                COUNT(*) AS detection_count,
                COUNT(DISTINCT stream_id) AS affected_cameras,
                COUNT(DISTINCT date) AS days_with_detections,
                MIN(timestamp) AS first_detection,
                MAX(timestamp) AS last_detection
            FROM stream_results
            WHERE workspace_id = $1
              {date_filter}
            GROUP BY fire_status
            ORDER BY detection_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [dict(row) for row in results]
        
        # Calculate summary statistics
        total_detections = sum(row['detection_count'] for row in data)
        fire_detections = sum(row['detection_count'] for row in data if row['fire_status'] != 'no detection')
        
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
        logger.error(f"Error fetching fire detection summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-camera")
async def get_fire_detections_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection counts per camera
    
    Chart: Fire Detections by Camera (Bar Chart)
    Returns: Number of fire detections for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                area,
                building,
                zone,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_fire,
                MIN(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS first_fire_detection,
                MAX(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS last_fire_detection,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location, area, building, zone
            ORDER BY fire_detections DESC, camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by camera: {e}", exc_info=True)
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
    Get fire detection counts by location grouping
    
    Chart: Fire Detections by Location (Bar Chart / Heatmap)
    Returns: Number of fire detections grouped by location, area, building, zone, or floor
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE({group_by}, 'Unknown') AS group_name,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                COUNT(DISTINCT camera_id) AS cameras_in_group,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_fire,
                MIN(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS first_fire_detection,
                MAX(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS last_fire_detection,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              {date_filter}
            GROUP BY COALESCE({group_by}, 'Unknown')
            ORDER BY fire_detections DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'group_type': group_by,
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "group_by": group_by,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by location: {e}", exc_info=True)
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
    Get fire detections over time
    
    Chart: Fire Detections Timeline (Line Chart / Area Chart)
    Returns: Fire detection counts aggregated by time interval
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
        
        # Set truncation based on interval
        truncate_expr = {
            'hour': "DATE_TRUNC('hour', timestamp)",
            'day': "DATE_TRUNC('day', timestamp)",
            'week': "DATE_TRUNC('week', timestamp)",
            'month': "DATE_TRUNC('month', timestamp)"
        }[interval]
            
        query = f"""
            SELECT
                {truncate_expr} AS time_period,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                COUNT(DISTINCT stream_id) AS cameras_checked,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN stream_id END) AS cameras_with_fire,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              AND timestamp IS NOT NULL
              {date_filter}
            GROUP BY {truncate_expr}
            ORDER BY time_period DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'time_period': row['time_period'].isoformat(),
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "interval": interval,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-weekday")
async def get_fire_detections_by_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection patterns by day of week
    
    Chart: Fire Detections by Weekday (Bar Chart)
    Returns: Fire detection counts for each day of the week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                COUNT(DISTINCT date) AS days_sampled,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              AND date IS NOT NULL
              {date_filter}
            GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by weekday: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-hour")
async def get_fire_detections_by_hour(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire detection patterns by hour of day
    
    Chart: Fire Detections by Hour (Line Chart / Heatmap)
    Returns: Fire detection counts for each hour of the day
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                EXTRACT(HOUR FROM time) AS hour,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                COUNT(DISTINCT date) AS days_sampled,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(DISTINCT date), 0)), 2) AS avg_detections_per_day
            FROM stream_results
            WHERE workspace_id = $1
              AND time IS NOT NULL
              {date_filter}
            GROUP BY EXTRACT(HOUR FROM time)
            ORDER BY hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0,
            'avg_detections_per_day': float(row['avg_detections_per_day']) if row['avg_detections_per_day'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by hour: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/recent-detections")
async def get_recent_fire_detections(
    request: Request,
    limit: int = Query(50, le=500, description="Max number of recent detections"),
    camera_id: Optional[UUID] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get most recent fire detections
    
    Chart: Recent Fire Detection Events (Table / Timeline)
    Returns: List of recent fire detection events with details
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        camera_filter = ""
        
        if camera_id:
            params.append(camera_id)
            camera_filter = f" AND stream_id = ${len(params)}"
            
        query = f"""
            SELECT
                result_id,
                stream_id,
                camera_id,
                camera_name,
                timestamp,
                fire_status,
                person_count,
                location,
                area,
                building,
                zone,
                floor_level
            FROM stream_results
            WHERE workspace_id = $1
              AND fire_status != 'no detection'
              {camera_filter}
            ORDER BY timestamp DESC
            LIMIT ${len(params) + 1}
        """
        
        params.append(limit)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'timestamp': row['timestamp'].isoformat(),
            'result_id': str(row['result_id']),
            'stream_id': str(row['stream_id'])
        } for row in results]
        
        return {
            "success": True,
            "limit": limit,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching recent fire detections: {e}", exc_info=True)
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
    Get cameras with high fire detection rates
    
    Chart: High Risk Cameras (Table / Alert List)
    Returns: Cameras with fire detection rate above threshold
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH camera_stats AS (
                SELECT
                    camera_name,
                    camera_id,
                    stream_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
                    COUNT(*) AS total_checks,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                    ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                           NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                    MIN(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS first_fire_detection,
                    MAX(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS last_fire_detection,
                    COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_fire
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                GROUP BY camera_name, camera_id, stream_id, location, area, building, zone, floor_level
            )
            SELECT *
            FROM camera_stats
            WHERE fire_detection_rate >= ${len(params) + 1}
            ORDER BY fire_detection_rate DESC, fire_detections DESC
        """
        
        params.append(threshold_percentage)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'stream_id': str(row['stream_id']),
            'fire_detection_rate': float(row['fire_detection_rate']) if row['fire_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "threshold_percentage": threshold_percentage,
            "high_risk_count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching high risk cameras: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/status-by-camera")
async def get_fire_status_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get fire status counts (smoke/fire) per camera
    
    Chart: Fire Status by Camera (Grouped Bar Chart)
    Returns: Count of smoke and fire detections for each camera
    X-axis: camera_name
    Y-axis: count
    Legend: fire_status (smoke/fire)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                area,
                building,
                zone,
                fire_status,
                COUNT(*) AS status_count,
                MIN(timestamp) AS first_detection,
                MAX(timestamp) AS last_detection,
                COUNT(DISTINCT date) AS days_with_detection
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND fire_status IN ('smoke', 'fire')
              {date_filter}
            GROUP BY camera_name, camera_id, location, area, building, zone, fire_status
            ORDER BY camera_name, fire_status
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Group data by camera for easier visualization
        cameras_data = {}
        for row in results:
            cam_name = row['camera_name']
            if cam_name not in cameras_data:
                cameras_data[cam_name] = {
                    'camera_name': cam_name,
                    'camera_id': row['camera_id'],
                    'location': row['location'],
                    'area': row['area'],
                    'building': row['building'],
                    'zone': row['zone'],
                    'smoke_count': 0,
                    'fire_count': 0,
                    'total_detections': 0,
                    'first_detection': None,
                    'last_detection': None
                }
            
            status = row['fire_status']
            count = row['status_count']
            
            if status == 'smoke':
                cameras_data[cam_name]['smoke_count'] = count
            elif status == 'fire':
                cameras_data[cam_name]['fire_count'] = count
                
            cameras_data[cam_name]['total_detections'] += count
            
            # Update timestamps
            if cameras_data[cam_name]['first_detection'] is None or row['first_detection'] < cameras_data[cam_name]['first_detection']:
                cameras_data[cam_name]['first_detection'] = row['first_detection']
            if cameras_data[cam_name]['last_detection'] is None or row['last_detection'] > cameras_data[cam_name]['last_detection']:
                cameras_data[cam_name]['last_detection'] = row['last_detection']
        
        # Convert to list and sort by total detections
        data = sorted(cameras_data.values(), key=lambda x: x['total_detections'], reverse=True)
        
        # Calculate summary statistics
        total_smoke = sum(d['smoke_count'] for d in data)
        total_fire = sum(d['fire_count'] for d in data)
        
        return {
            "success": True,
            "summary": {
                "total_cameras_with_detections": len(data),
                "total_smoke_detections": total_smoke,
                "total_fire_detections": total_fire,
                "cameras_with_smoke": len([d for d in data if d['smoke_count'] > 0]),
                "cameras_with_fire": len([d for d in data if d['fire_count'] > 0])
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire status by camera: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/threshold-violations-by-camera")
async def get_threshold_violations_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get person count threshold violations per camera
    
    Chart: Threshold Violations by Camera (Grouped Bar Chart)
    Returns: Count of times person count exceeded max threshold or fell below min threshold
    X-axis: camera_name
    Y-axis: violation count
    Legend: threshold_type (Above Max Threshold / Below Min Threshold)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND sr.date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND sr.date <= ${len(params)}"
            
        query = f"""
            SELECT
                sr.camera_name,
                sr.camera_id,
                sr.location,
                sr.area,
                sr.building,
                sr.zone,
                vs.count_threshold_greater,
                vs.count_threshold_less,
                COUNT(CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL 
                         AND sr.person_count > vs.count_threshold_greater 
                    THEN 1 
                END) AS above_max_count,
                COUNT(CASE 
                    WHEN vs.count_threshold_less IS NOT NULL 
                         AND sr.person_count < vs.count_threshold_less 
                    THEN 1 
                END) AS below_min_count,
                COUNT(*) AS total_checks,
                AVG(sr.person_count) AS avg_person_count,
                MAX(sr.person_count) AS max_person_count,
                MIN(sr.person_count) AS min_person_count,
                MAX(CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL 
                         AND sr.person_count > vs.count_threshold_greater 
                    THEN sr.timestamp 
                END) AS last_above_max_time,
                MAX(CASE 
                    WHEN vs.count_threshold_less IS NOT NULL 
                         AND sr.person_count < vs.count_threshold_less 
                    THEN sr.timestamp 
                END) AS last_below_min_time
            FROM stream_results sr
            JOIN video_stream vs ON sr.stream_id = vs.stream_id
            WHERE sr.workspace_id = $1
              AND sr.camera_name IS NOT NULL
              AND sr.person_count IS NOT NULL
              AND vs.alert_enabled = TRUE
              AND (vs.count_threshold_greater IS NOT NULL OR vs.count_threshold_less IS NOT NULL)
              {date_filter}
            GROUP BY 
                sr.camera_name, 
                sr.camera_id, 
                sr.location, 
                sr.area, 
                sr.building, 
                sr.zone,
                vs.count_threshold_greater,
                vs.count_threshold_less
            HAVING COUNT(CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL 
                         AND sr.person_count > vs.count_threshold_greater 
                    THEN 1 
                END) > 0
                OR COUNT(CASE 
                    WHEN vs.count_threshold_less IS NOT NULL 
                         AND sr.person_count < vs.count_threshold_less 
                    THEN 1 
                END) > 0
            ORDER BY (
                COUNT(CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL 
                         AND sr.person_count > vs.count_threshold_greater 
                    THEN 1 
                END) + 
                COUNT(CASE 
                    WHEN vs.count_threshold_less IS NOT NULL 
                         AND sr.person_count < vs.count_threshold_less 
                    THEN 1 
                END)
            ) DESC, sr.camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'camera_name': row['camera_name'],
            'camera_id': row['camera_id'],
            'location': row['location'],
            'area': row['area'],
            'building': row['building'],
            'zone': row['zone'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'above_max_count': row['above_max_count'],
            'below_min_count': row['below_min_count'],
            'total_violations': row['above_max_count'] + row['below_min_count'],
            'total_checks': row['total_checks'],
            'violation_rate': round(
                ((row['above_max_count'] + row['below_min_count']) / row['total_checks'] * 100), 
                2
            ) if row['total_checks'] > 0 else 0,
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0,
            'max_person_count': row['max_person_count'],
            'min_person_count': row['min_person_count'],
            'last_above_max_time': row['last_above_max_time'].isoformat() if row['last_above_max_time'] else None,
            'last_below_min_time': row['last_below_min_time'].isoformat() if row['last_below_min_time'] else None
        } for row in results]
        
        # Calculate summary statistics
        total_above_max = sum(d['above_max_count'] for d in data)
        total_below_min = sum(d['below_min_count'] for d in data)
        cameras_with_violations = len(data)
        
        return {
            "success": True,
            "summary": {
                "cameras_with_violations": cameras_with_violations,
                "total_above_max_violations": total_above_max,
                "total_below_min_violations": total_below_min,
                "total_violations": total_above_max + total_below_min,
                "cameras_with_above_max": len([d for d in data if d['above_max_count'] > 0]),
                "cameras_with_below_min": len([d for d in data if d['below_min_count'] > 0])
            },
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching threshold violations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
