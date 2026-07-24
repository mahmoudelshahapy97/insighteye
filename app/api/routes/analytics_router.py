# app/routes/analytics_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import Optional, Dict, List, Union
from datetime import date, datetime
from uuid import UUID
import logging

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.database import db_manager
from app.utils import check_workspace_access, parse_string_or_list

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["Analytics"])


def build_location_filters(
    params: list,
    param_count: int,
    locations: Optional[Union[str, List[str]]] = None,
    areas: Optional[Union[str, List[str]]] = None,
    buildings: Optional[Union[str, List[str]]] = None,
    floor_levels: Optional[Union[str, List[str]]] = None,
    zones: Optional[Union[str, List[str]]] = None
) -> tuple:
    """Helper function to build location filter SQL and params"""
    filters = []
    
    if locations:
        locations = parse_string_or_list(locations)
        if locations:
            param_count += 1
            placeholders = ", ".join([f"${param_count + i}" for i in range(len(locations))])
            filters.append(f"location IN ({placeholders})")
            params.extend(locations)
            param_count += len(locations) - 1
    
    if areas:
        areas = parse_string_or_list(areas)
        if areas:
            param_count += 1
            placeholders = ", ".join([f"${param_count + i}" for i in range(len(areas))])
            filters.append(f"area IN ({placeholders})")
            params.extend(areas)
            param_count += len(areas) - 1
    
    if buildings:
        buildings = parse_string_or_list(buildings)
        if buildings:
            param_count += 1
            placeholders = ", ".join([f"${param_count + i}" for i in range(len(buildings))])
            filters.append(f"building IN ({placeholders})")
            params.extend(buildings)
            param_count += len(buildings) - 1
    
    if floor_levels:
        floor_levels = parse_string_or_list(floor_levels)
        if floor_levels:
            param_count += 1
            placeholders = ", ".join([f"${param_count + i}" for i in range(len(floor_levels))])
            filters.append(f"floor_level IN ({placeholders})")
            params.extend(floor_levels)
            param_count += len(floor_levels) - 1
    
    if zones:
        zones = parse_string_or_list(zones)
        if zones:
            param_count += 1
            placeholders = ", ".join([f"${param_count + i}" for i in range(len(zones))])
            filters.append(f"zone IN ({placeholders})")
            params.extend(zones)
            param_count += len(zones) - 1
    
    return filters, param_count


@router.get("/cameras/unique")
async def get_unique_cameras(
    request: Request,
    order_by: str = Query("camera_name", pattern="^(camera_name|camera_id)$"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get list of unique cameras with location filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
        
        query = f"""
            SELECT DISTINCT
                stream_id,
                camera_name,
                location,
                area,
                building,
                zone,
                floor_level
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {location_where}
            ORDER BY camera_name ASC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        cameras = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(cameras),
            "filters_applied": {
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    limit: int = Query(5000, le=10000, description="Max datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get total frame counts per camera with date and location filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                camera_name,
                MAX(camera_id) AS camera_id,
                MAX(location) AS location,
                MAX(area) AS area,
                MAX(building) AS building,
                MAX(zone) AS zone,
                MAX(floor_level) AS floor_level,
                COUNT(*) AS total_frames,
                MIN(timestamp) AS first_frame,
                MAX(timestamp) AS last_frame
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY camera_name
            ORDER BY total_frames DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "limit_per_camera": limit,
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of people recorded by each camera with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                camera_name,
                MAX(camera_id) AS camera_id,
                MAX(location) AS location,
                MAX(area) AS area,
                MAX(building) AS building,
                MAX(zone) AS zone,
                MAX(floor_level) AS floor_level,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND person_count IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY camera_name
            ORDER BY avg_person_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0,
            'max_person_count': round(float(row['max_person_count'])) if row['max_person_count'] else 0,
            'min_person_count': round(float(row['min_person_count'])) if row['min_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    gender: str = Query("male", pattern="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of males or females recorded by each camera with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                camera_name,
                MAX(camera_id) AS camera_id,
                MAX(location) AS location,
                MAX(area) AS area,
                MAX(building) AS building,
                MAX(zone) AS zone,
                MAX(floor_level) AS floor_level,
                AVG({count_column}) AS avg_count,
                MAX({count_column}) AS max_count,
                MIN({count_column}) AS min_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND {count_column} IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY camera_name
            ORDER BY avg_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': round(float(row['avg_count'])) if row['avg_count'] else 0,
            'max_count': round(float(row['max_count'])) if row['max_count'] else 0,
            'min_count': round(float(row['min_count'])) if row['min_count'] else 0,
            'gender': gender
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    gender: str = Query("male", pattern="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of males/females in each zone by weekday with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            GROUP BY zone, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, zone
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': round(float(row['avg_count'])) if row['avg_count'] else 0,
            'gender': gender,
            'weekday_name': row['weekday_name'].strip()
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get the busiest hour for each weekday with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
                  {location_where}
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
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of people by floor level with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            GROUP BY floor_level
            ORDER BY floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of people by floor level and weekday with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            GROUP BY floor_level, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get average number of people by floor level and hour with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            GROUP BY floor_level, EXTRACT(HOUR FROM time)
            ORDER BY floor_level, hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Compare camera frame counts to average with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            WITH frame_counts AS (
                SELECT
                    camera_name,
                    MAX(camera_id) AS camera_id,
                    MAX(location) AS location,
                    MAX(area) AS area,
                    MAX(building) AS building,
                    MAX(zone) AS zone,
                    MAX(floor_level) AS floor_level,
                    COUNT(*) AS frame_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                  {location_where}
                GROUP BY camera_name
            ),
            avg_frame AS (
                SELECT AVG(frame_count) AS avg_frame_count
                FROM frame_counts
            )
            SELECT
                f.camera_name,
                f.camera_id,
                f.location,
                f.area,
                f.building,
                f.zone,
                f.floor_level,
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
            'avg_frame_count': round(float(row['avg_frame_count'])) if row['avg_frame_count'] else 0,
            'percent_difference': round(float(row['percent_difference'])) if row['percent_difference'] else 0
        } for row in results]
        
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
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    limit: int = Query(5000, le=5000, description="Max 5000 datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get time series data for line graph with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        filters = ["workspace_id = $1"]
        
        if camera_id:
            param_count += 1
            params.append(camera_id)
            filters.append(f"stream_id = ${param_count}")
            
        if start_date:
            param_count += 1
            params.append(start_date)
            filters.append(f"date >= ${param_count}")
        if end_date:
            param_count += 1
            params.append(end_date)
            filters.append(f"date <= ${param_count}")
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        filters.extend(location_filters)
            
        where_clause = " AND ".join(filters)
        
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
                    area,
                    building,
                    zone,
                    floor_level,
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
                location,
                area,
                building,
                zone,
                floor_level
            FROM ranked_data
            WHERE rn <= ${param_count + 1}
            ORDER BY stream_id, timestamp ASC
        """
        
        params.append(limit)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        cameras_data = {}
        for row in results:
            cam_id = str(row['stream_id'])
            if cam_id not in cameras_data:
                cameras_data[cam_id] = {
                    "camera_id": cam_id,
                    "camera_name": row['camera_name'],
                    "location": row['location'],
                    "area": row['area'],
                    "building": row['building'],
                    "zone": row['zone'],
                    "floor_level": row['floor_level'],
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
            "filters_applied": {
                "camera_id": str(camera_id) if camera_id else None,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": list(cameras_data.values())
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching timeseries data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-camera")
async def get_fire_detections_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire detection counts per camera with breakdown by detection type"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                camera_name,
                MAX(camera_id) AS camera_id,
                MAX(location) AS location,
                MAX(area) AS area,
                MAX(building) AS building,
                MAX(zone) AS zone,
                MAX(floor_level) AS floor_level,
                COUNT(*) AS total_checks,
                
                -- Breakdown by detection type
                COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                COUNT(CASE WHEN fire_status = 'no detection' THEN 1 END) AS no_detection_count,
                
                -- Days with detections
                COUNT(DISTINCT CASE WHEN fire_status = 'fire' THEN date END) AS days_with_fire,
                COUNT(DISTINCT CASE WHEN fire_status = 'smoke' THEN date END) AS days_with_smoke,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_any_detection,
                
                -- First and last detection times (any type)
                MIN(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS first_detection,
                MAX(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS last_detection,
                
                -- First and last fire-specific
                MIN(CASE WHEN fire_status = 'fire' THEN timestamp END) AS first_fire_detection,
                MAX(CASE WHEN fire_status = 'fire' THEN timestamp END) AS last_fire_detection,
                
                -- First and last smoke-specific
                MIN(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS first_smoke_detection,
                MAX(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS last_smoke_detection,
                
                -- Detection rates
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate,
                
                -- List all unique detection statuses found
                ARRAY_AGG(DISTINCT fire_status) FILTER (WHERE fire_status != 'no detection') AS detection_types
                
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY camera_name
            ORDER BY total_detections DESC, fire_detections DESC, smoke_detections DESC, camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0,
            'detection_types': row['detection_types'] if row['detection_types'] else []
        } for row in results]
        
        # Calculate summary statistics
        summary = {
            'total_cameras': len(data),
            'cameras_with_fire': sum(1 for d in data if d['fire_detections'] > 0),
            'cameras_with_smoke': sum(1 for d in data if d['smoke_detections'] > 0),
            'cameras_with_any_detection': sum(1 for d in data if d['total_detections'] > 0),
            'total_fire_detections': sum(d['fire_detections'] for d in data),
            'total_smoke_detections': sum(d['smoke_detections'] for d in data),
            'total_all_detections': sum(d['total_detections'] for d in data)
        }
        
        return {
            "success": True,
            "count": len(data),
            "summary": summary,
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by camera: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fire/detection-summary")
async def get_fire_detection_summary(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get summary of fire detections across all cameras with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            GROUP BY fire_status
            ORDER BY detection_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [dict(row) for row in results]
        
        total_detections = sum(row['detection_count'] for row in data)
        fire_detections = sum(row['detection_count'] for row in data if row['fire_status'] == 'fire')
        smoke_detections = sum(row['detection_count'] for row in data if row['fire_status'] == 'smoke')
        no_detection = sum(row['detection_count'] for row in data if row['fire_status'] == 'no detection')
        
        return {
            "success": True,
            "summary": {
                "total_records": total_detections,
                "fire_detections": fire_detections,
                "smoke_detections": smoke_detections,
                "total_fire_and_smoke": fire_detections + smoke_detections,
                "no_detection_records": no_detection,
                "fire_detection_percentage": round((fire_detections / total_detections * 100), 2) if total_detections > 0 else 0,
                "smoke_detection_percentage": round((smoke_detections / total_detections * 100), 2) if total_detections > 0 else 0,
                "combined_detection_percentage": round(((fire_detections + smoke_detections) / total_detections * 100), 2) if total_detections > 0 else 0
            },
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detection summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/detections-by-location")
async def get_fire_detections_by_location(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    group_by: str = Query("location", pattern="^(location|area|building|zone|floor_level)$"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire detection counts by location grouping with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                COALESCE({group_by}, 'Unknown') AS group_name,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                COUNT(DISTINCT camera_id) AS cameras_in_group,
                COUNT(DISTINCT CASE WHEN fire_status = 'fire' THEN date END) AS days_with_fire,
                COUNT(DISTINCT CASE WHEN fire_status = 'smoke' THEN date END) AS days_with_smoke,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_any_detection,
                MIN(CASE WHEN fire_status = 'fire' THEN timestamp END) AS first_fire_detection,
                MAX(CASE WHEN fire_status = 'fire' THEN timestamp END) AS last_fire_detection,
                MIN(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS first_smoke_detection,
                MAX(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS last_smoke_detection,
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              {date_filter}
              {location_where}
            GROUP BY COALESCE({group_by}, 'Unknown')
            ORDER BY total_detections DESC, fire_detections DESC, smoke_detections DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'group_type': group_by,
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "group_by": group_by,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    interval: str = Query("day", pattern="^(hour|day|week|month)$"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire detections over time with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
        
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
                COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                COUNT(DISTINCT stream_id) AS cameras_checked,
                COUNT(DISTINCT CASE WHEN fire_status = 'fire' THEN stream_id END) AS cameras_with_fire,
                COUNT(DISTINCT CASE WHEN fire_status = 'smoke' THEN stream_id END) AS cameras_with_smoke,
                COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN stream_id END) AS cameras_with_any_detection,
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              AND timestamp IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY {truncate_expr}
            ORDER BY time_period DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'time_period': row['time_period'].isoformat(),
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "interval": interval,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire detection patterns by day of week with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                COUNT(DISTINCT date) AS days_sampled,
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate
            FROM stream_results
            WHERE workspace_id = $1
              AND date IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
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
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire detection patterns by hour of day with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                EXTRACT(HOUR FROM time) AS hour,
                COUNT(*) AS total_checks,
                COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                COUNT(DISTINCT date) AS days_sampled,
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate,
                ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(DISTINCT date), 0)), 2) AS avg_fire_per_day,
                ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(DISTINCT date), 0)), 2) AS avg_smoke_per_day,
                ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                       NULLIF(COUNT(DISTINCT date), 0)), 2) AS avg_detections_per_day
            FROM stream_results
            WHERE workspace_id = $1
              AND time IS NOT NULL
              {date_filter}
              {location_where}
            GROUP BY EXTRACT(HOUR FROM time)
            ORDER BY hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0,
            'avg_fire_per_day': round(float(row['avg_fire_per_day'])) if row['avg_fire_per_day'] else 0,
            'avg_smoke_per_day': round(float(row['avg_smoke_per_day'])) if row['avg_smoke_per_day'] else 0,
            'avg_detections_per_day': round(float(row['avg_detections_per_day'])) if row['avg_detections_per_day'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fire detections by hour: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fire/high-risk-cameras")
async def get_high_risk_cameras(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    threshold_percentage: float = Query(5.0, ge=0, le=100, description="Min fire detection rate to be considered high risk"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras with high fire detection rates with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
                    
                    -- Separate fire and smoke counts
                    COUNT(CASE WHEN fire_status = 'fire' THEN 1 END) AS fire_detections,
                    COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END) AS smoke_detections,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS total_detections,
                    
                    -- Detection rates
                    ROUND((COUNT(CASE WHEN fire_status = 'fire' THEN 1 END)::NUMERIC / 
                           NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate,
                    ROUND((COUNT(CASE WHEN fire_status = 'smoke' THEN 1 END)::NUMERIC / 
                           NULLIF(COUNT(*), 0) * 100), 2) AS smoke_detection_rate,
                    ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                           NULLIF(COUNT(*), 0) * 100), 2) AS total_detection_rate,
                    
                    -- Fire-specific timestamps
                    MIN(CASE WHEN fire_status = 'fire' THEN timestamp END) AS first_fire_detection,
                    MAX(CASE WHEN fire_status = 'fire' THEN timestamp END) AS last_fire_detection,
                    
                    -- Smoke-specific timestamps
                    MIN(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS first_smoke_detection,
                    MAX(CASE WHEN fire_status = 'smoke' THEN timestamp END) AS last_smoke_detection,
                    
                    -- Any detection timestamps
                    MIN(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS first_detection,
                    MAX(CASE WHEN fire_status != 'no detection' THEN timestamp END) AS last_detection,
                    
                    -- Days with detections
                    COUNT(DISTINCT CASE WHEN fire_status = 'fire' THEN date END) AS days_with_fire,
                    COUNT(DISTINCT CASE WHEN fire_status = 'smoke' THEN date END) AS days_with_smoke,
                    COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN date END) AS days_with_any_detection
                    
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                  {location_where}
                GROUP BY camera_name, camera_id, stream_id, location, area, building, zone, floor_level
            )
            SELECT *
            FROM camera_stats
            WHERE total_detection_rate >= ${param_count + 1}
            ORDER BY total_detection_rate DESC, fire_detections DESC, smoke_detections DESC
        """
        
        params.append(threshold_percentage)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'camera_name': row['camera_name'],
            'camera_id': row['camera_id'],
            'stream_id': str(row['stream_id']),
            'location': row['location'],
            'area': row['area'],
            'building': row['building'],
            'zone': row['zone'],
            'floor_level': row['floor_level'],
            'total_checks': row['total_checks'],
            'fire_detections': row['fire_detections'],
            'smoke_detections': row['smoke_detections'],
            'total_detections': row['total_detections'],
            'fire_detection_rate': round(float(row['fire_detection_rate'])) if row['fire_detection_rate'] else 0,
            'smoke_detection_rate': round(float(row['smoke_detection_rate'])) if row['smoke_detection_rate'] else 0,
            'total_detection_rate': round(float(row['total_detection_rate'])) if row['total_detection_rate'] else 0,
            'first_fire_detection': row['first_fire_detection'].isoformat() if row['first_fire_detection'] else None,
            'last_fire_detection': row['last_fire_detection'].isoformat() if row['last_fire_detection'] else None,
            'first_smoke_detection': row['first_smoke_detection'].isoformat() if row['first_smoke_detection'] else None,
            'last_smoke_detection': row['last_smoke_detection'].isoformat() if row['last_smoke_detection'] else None,
            'first_detection': row['first_detection'].isoformat() if row['first_detection'] else None,
            'last_detection': row['last_detection'].isoformat() if row['last_detection'] else None,
            'days_with_fire': row['days_with_fire'],
            'days_with_smoke': row['days_with_smoke'],
            'days_with_any_detection': row['days_with_any_detection']
        } for row in results]
        
        # Calculate summary statistics
        summary = {
            'high_risk_count': len(data),
            'total_fire_detections': sum(d['fire_detections'] for d in data),
            'total_smoke_detections': sum(d['smoke_detections'] for d in data),
            'total_all_detections': sum(d['total_detections'] for d in data),
            'cameras_with_fire': len([d for d in data if d['fire_detections'] > 0]),
            'cameras_with_smoke': len([d for d in data if d['smoke_detections'] > 0]),
            'avg_fire_detection_rate': round(sum(d['fire_detection_rate'] for d in data) / len(data), 2) if data else 0,
            'avg_smoke_detection_rate': round(sum(d['smoke_detection_rate'] for d in data) / len(data), 2) if data else 0,
            'avg_total_detection_rate': round(sum(d['total_detection_rate'] for d in data) / len(data), 2) if data else 0
        }
        
        return {
            "success": True,
            "threshold_percentage": threshold_percentage,
            "summary": summary,
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching high risk cameras: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fire/recent-detections")
async def get_recent_fire_detections(
    request: Request,
    limit: int = Query(50, le=500, description="Max number of recent detections"),
    camera_id: Optional[UUID] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get most recent fire detections with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        camera_filter = ""
        
        if camera_id:
            param_count += 1
            params.append(camera_id)
            camera_filter = f" AND stream_id = ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
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
              {location_where}
            ORDER BY timestamp DESC
            LIMIT ${param_count + 1}
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
            "filters_applied": {
                "camera_id": str(camera_id) if camera_id else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching recent fire detections: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/status-by-camera")
async def get_fire_status_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get fire status counts (smoke/fire) per camera with filters"""
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )
        location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
            
        query = f"""
            SELECT
                camera_name,
                MAX(camera_id) AS camera_id,
                MAX(location) AS location,
                MAX(area) AS area,
                MAX(building) AS building,
                MAX(zone) AS zone,
                MAX(floor_level) AS floor_level,
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
              {location_where}
            GROUP BY camera_name, fire_status
            ORDER BY camera_name, fire_status
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
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
                    'floor_level': row['floor_level'],
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
    
        # Continuing from where it was cut off:
        cameras_data[cam_name]['total_detections'] += count
        
        if cameras_data[cam_name]['first_detection'] is None or row['first_detection'] < cameras_data[cam_name]['first_detection']:
            cameras_data[cam_name]['first_detection'] = row['first_detection']
        if cameras_data[cam_name]['last_detection'] is None or row['last_detection'] > cameras_data[cam_name]['last_detection']:
            cameras_data[cam_name]['last_detection'] = row['last_detection']
        
        data = sorted(cameras_data.values(), key=lambda x: x['total_detections'], reverse=True)
        
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
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones)
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching threshold violations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fire/threshold-violations-by-camera")
async def get_threshold_violations_by_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by zone(s)"),
    include_zero_violations: bool = Query(False, description="Include cameras with zero violations"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get person count threshold violations per camera with location filters
    
    FIXED ISSUES:
    1. Added include_zero_violations parameter to optionally show all cameras
    2. Made HAVING clause conditional
    3. Added detailed logging for debugging
    4. Added validation checks for configuration
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        # Step 1: Validate configuration first
        async with db_manager.get_connection() as conn:
            config_check = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_cameras,
                    COUNT(CASE WHEN alert_enabled = TRUE THEN 1 END) as alerts_enabled_count,
                    COUNT(CASE WHEN count_threshold_greater IS NOT NULL THEN 1 END) as has_max_threshold,
                    COUNT(CASE WHEN count_threshold_less IS NOT NULL THEN 1 END) as has_min_threshold,
                    COUNT(CASE WHEN alert_enabled = TRUE 
                               AND (count_threshold_greater IS NOT NULL 
                                    OR count_threshold_less IS NOT NULL) 
                          THEN 1 END) as configured_cameras
                FROM video_stream
                WHERE workspace_id = $1
            """, workspace_id_obj)
            
            logger.info(f"Configuration check for workspace {workspace_id_obj}: {dict(config_check)}")
            
            # Warn if no cameras are properly configured
            if config_check['configured_cameras'] == 0:
                logger.warning(f"No cameras configured with alerts and thresholds in workspace {workspace_id_obj}")
                return {
                    "success": True,
                    "warning": "No cameras have both alerts enabled and thresholds configured",
                    "configuration_status": {
                        "total_cameras": config_check['total_cameras'],
                        "alerts_enabled": config_check['alerts_enabled_count'],
                        "has_max_threshold": config_check['has_max_threshold'],
                        "has_min_threshold": config_check['has_min_threshold'],
                        "properly_configured": config_check['configured_cameras']
                    },
                    "summary": {
                        "cameras_with_violations": 0,
                        "total_above_max_violations": 0,
                        "total_below_min_violations": 0,
                        "total_violations": 0,
                        "cameras_with_above_max": 0,
                        "cameras_with_below_min": 0
                    },
                    "filters_applied": {
                        "start_date": start_date.isoformat() if start_date else None,
                        "end_date": end_date.isoformat() if end_date else None,
                        "locations": parse_string_or_list(locations),
                        "areas": parse_string_or_list(areas),
                        "buildings": parse_string_or_list(buildings),
                        "floor_levels": parse_string_or_list(floor_levels),
                        "zones": parse_string_or_list(zones)
                    },
                    "count": 0,
                    "data": []
                }
        
        # Step 2: Build query parameters
        params = [workspace_id_obj]
        param_count = 1
        date_filter = ""            # applied to stream_results (checks CTE)
        violation_date_filter = ""  # applied to threshold_violations (violations CTE)

        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND sr.date >= ${param_count}"
            violation_date_filter += f' AND tv."timestamp"::date >= ${param_count}'
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND sr.date <= ${param_count}"
            violation_date_filter += f' AND tv."timestamp"::date <= ${param_count}'

        location_filters, param_count = build_location_filters(
            params, param_count, locations, areas, buildings, floor_levels, zones
        )

        # Prefix location filters with "vs." — filters directly on video_stream's current hierarchy
        location_where = ""
        if location_filters:
            prefixed_filters = [f.replace("location", "vs.location")
                                  .replace("area", "vs.area")
                                  .replace("building", "vs.building")
                                  .replace("floor_level", "vs.floor_level")
                                  .replace("zone", "vs.zone")
                               for f in location_filters]
            location_where = " AND " + " AND ".join(prefixed_filters)

        # Step 3: Conditional zero-violations filter based on include_zero_violations parameter
        # NOTE: this must be a WHERE condition, not HAVING - the outer query has no GROUP BY
        # (violations/checks CTEs are already one row per stream_id), and a bare HAVING with
        # no GROUP BY causes Postgres to reject every non-aggregated SELECT column (GroupingError).
        zero_violations_filter = ""
        if not include_zero_violations:
            zero_violations_filter = """
              AND (COALESCE(v.above_max_count, 0) > 0
                   OR COALESCE(v.below_min_count, 0) > 0)
            """

        query = f"""
            WITH violations AS (
                SELECT
                    stream_id,
                    COUNT(*) FILTER (WHERE threshold_type = 'greater_than') AS above_max_count,
                    COUNT(*) FILTER (WHERE threshold_type = 'less_than') AS below_min_count,
                    MAX("timestamp") FILTER (WHERE threshold_type = 'greater_than') AS last_above_max_time,
                    MAX("timestamp") FILTER (WHERE threshold_type = 'less_than') AS last_below_min_time
                FROM threshold_violations tv
                WHERE tv.workspace_id = $1
                  {violation_date_filter}
                GROUP BY stream_id
            ),
            checks AS (
                SELECT
                    sr.stream_id,
                    COUNT(*) AS total_checks,
                    AVG(sr.person_count) AS avg_person_count,
                    MAX(sr.person_count) AS max_person_count,
                    MIN(sr.person_count) AS min_person_count
                FROM stream_results sr
                WHERE sr.workspace_id = $1
                  AND sr.person_count IS NOT NULL
                  {date_filter}
                GROUP BY sr.stream_id
            )
            SELECT
                vs.stream_id,
                vs.name AS camera_name,
                vs.location,
                vs.area,
                vs.building,
                vs.zone,
                vs.floor_level,
                vs.count_threshold_greater,
                vs.count_threshold_less,
                vs.alert_enabled,
                COALESCE(v.above_max_count, 0) AS above_max_count,
                COALESCE(v.below_min_count, 0) AS below_min_count,
                COALESCE(c.total_checks, 0) AS total_checks,
                c.avg_person_count,
                c.max_person_count,
                c.min_person_count,
                v.last_above_max_time,
                v.last_below_min_time
            FROM video_stream vs
            LEFT JOIN checks c ON c.stream_id = vs.stream_id
            LEFT JOIN violations v ON v.stream_id = vs.stream_id
            WHERE vs.workspace_id = $1
              AND vs.alert_enabled = TRUE
              AND (vs.count_threshold_greater IS NOT NULL OR vs.count_threshold_less IS NOT NULL)
              {location_where}
              {zero_violations_filter}
            ORDER BY (COALESCE(v.above_max_count, 0) + COALESCE(v.below_min_count, 0)) DESC, vs.name
        """
        
        logger.info(f"Executing threshold violations query with {len(params)} parameters")
        logger.debug(f"Query parameters: {params}")
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        logger.info(f"Query returned {len(results)} results")
        
        # Step 4: Format results
        data = [{
            'camera_name': row['camera_name'],
            'camera_id': str(row['stream_id']),
            'location': row['location'],
            'area': row['area'],
            'building': row['building'],
            'zone': row['zone'],
            'floor_level': row['floor_level'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'alert_enabled': row['alert_enabled'],
            'above_max_count': row['above_max_count'],
            'below_min_count': row['below_min_count'],
            'total_violations': row['above_max_count'] + row['below_min_count'],
            'total_checks': row['total_checks'],
            'violation_rate': round(
                ((row['above_max_count'] + row['below_min_count']) / row['total_checks'] * 100), 
                2
            ) if row['total_checks'] > 0 else 0,
            'avg_person_count': round(float(row['avg_person_count'])) if row['avg_person_count'] else 0,
            'max_person_count': row['max_person_count'],
            'min_person_count': row['min_person_count'],
            'last_above_max_time': row['last_above_max_time'].isoformat() if row['last_above_max_time'] else None,
            'last_below_min_time': row['last_below_min_time'].isoformat() if row['last_below_min_time'] else None
        } for row in results]
        
        total_above_max = sum(d['above_max_count'] for d in data)
        total_below_min = sum(d['below_min_count'] for d in data)
        cameras_with_violations = len([d for d in data if d['total_violations'] > 0])
        
        response = {
            "success": True,
            "summary": {
                "cameras_with_violations": cameras_with_violations,
                "cameras_monitored": len(data),
                "total_above_max_violations": total_above_max,
                "total_below_min_violations": total_below_min,
                "total_violations": total_above_max + total_below_min,
                "cameras_with_above_max": len([d for d in data if d['above_max_count'] > 0]),
                "cameras_with_below_min": len([d for d in data if d['below_min_count'] > 0])
            },
            "filters_applied": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "locations": parse_string_or_list(locations),
                "areas": parse_string_or_list(areas),
                "buildings": parse_string_or_list(buildings),
                "floor_levels": parse_string_or_list(floor_levels),
                "zones": parse_string_or_list(zones),
                "include_zero_violations": include_zero_violations
            },
            "count": len(data),
            "data": data
        }
        
        logger.info(f"Returning {len(data)} cameras with summary: {response['summary']}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching threshold violations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fire/threshold-violations-config-check")
async def check_threshold_violations_config(
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Diagnostic endpoint to check threshold violation configuration
    
    Returns detailed information about:
    - How many cameras exist
    - How many have alerts enabled
    - How many have thresholds configured
    - Sample person count data
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        async with db_manager.get_connection() as conn:
            # Camera configuration check
            camera_config = await conn.fetch("""
                SELECT 
                    name,
                    alert_enabled,
                    count_threshold_greater,
                    count_threshold_less,
                    status,
                    is_streaming,
                    CASE 
                        WHEN alert_enabled = TRUE 
                             AND (count_threshold_greater IS NOT NULL OR count_threshold_less IS NOT NULL)
                        THEN 'properly_configured'
                        WHEN alert_enabled = FALSE THEN 'alerts_disabled'
                        WHEN count_threshold_greater IS NULL AND count_threshold_less IS NULL THEN 'no_thresholds'
                        ELSE 'partial_config'
                    END as config_status
                FROM video_stream
                WHERE workspace_id = $1
                ORDER BY name
            """, workspace_id_obj)
            
            # Data availability check
            data_check = await conn.fetchrow("""
                SELECT 
                    COUNT(DISTINCT sr.stream_id) as cameras_with_data,
                    COUNT(*) as total_stream_results,
                    MIN(sr.date) as earliest_date,
                    MAX(sr.date) as latest_date,
                    AVG(sr.person_count) as avg_person_count,
                    MIN(sr.person_count) as min_person_count,
                    MAX(sr.person_count) as max_person_count
                FROM stream_results sr
                WHERE sr.workspace_id = $1
            """, workspace_id_obj)
            
            # Potential violations check
            violations_check = await conn.fetch("""
                SELECT 
                    vs.name,
                    vs.count_threshold_greater,
                    vs.count_threshold_less,
                    COUNT(*) as total_readings,
                    MIN(sr.person_count) as min_count_recorded,
                    MAX(sr.person_count) as max_count_recorded,
                    AVG(sr.person_count) as avg_count_recorded,
                    COUNT(CASE 
                        WHEN vs.count_threshold_greater IS NOT NULL 
                             AND sr.person_count > vs.count_threshold_greater 
                        THEN 1 
                    END) as would_trigger_above_max,
                    COUNT(CASE 
                        WHEN vs.count_threshold_less IS NOT NULL 
                             AND sr.person_count < vs.count_threshold_less 
                        THEN 1 
                    END) as would_trigger_below_min
                FROM video_stream vs
                LEFT JOIN stream_results sr ON vs.stream_id = sr.stream_id
                WHERE vs.workspace_id = $1
                  AND vs.alert_enabled = TRUE
                  AND (vs.count_threshold_greater IS NOT NULL OR vs.count_threshold_less IS NOT NULL)
                GROUP BY vs.name, vs.count_threshold_greater, vs.count_threshold_less
            """, workspace_id_obj)
        
        # Format camera configuration
        cameras = [{
            'name': row['name'],
            'alert_enabled': row['alert_enabled'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'status': row['status'],
            'is_streaming': row['is_streaming'],
            'config_status': row['config_status']
        } for row in camera_config]
        
        # Format violations check
        violations_analysis = [{
            'camera_name': row['name'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'total_readings': row['total_readings'],
            'min_count_recorded': row['min_count_recorded'],
            'max_count_recorded': row['max_count_recorded'],
            'avg_count_recorded': round(float(row['avg_count_recorded']), 2) if row['avg_count_recorded'] else 0,
            'would_trigger_above_max': row['would_trigger_above_max'],
            'would_trigger_below_min': row['would_trigger_below_min'],
            'has_violations': (row['would_trigger_above_max'] > 0 or row['would_trigger_below_min'] > 0)
        } for row in violations_check]
        
        config_summary = {
            'total_cameras': len(cameras),
            'alerts_enabled': len([c for c in cameras if c['alert_enabled']]),
            'has_max_threshold': len([c for c in cameras if c['max_threshold'] is not None]),
            'has_min_threshold': len([c for c in cameras if c['min_threshold'] is not None]),
            'properly_configured': len([c for c in cameras if c['config_status'] == 'properly_configured']),
            'cameras_with_data': data_check['cameras_with_data'] if data_check else 0,
            'total_stream_results': data_check['total_stream_results'] if data_check else 0
        }
        
        return {
            "success": True,
            "configuration_summary": config_summary,
            "data_availability": {
                "earliest_date": data_check['earliest_date'].isoformat() if data_check and data_check['earliest_date'] else None,
                "latest_date": data_check['latest_date'].isoformat() if data_check and data_check['latest_date'] else None,
                "avg_person_count": round(float(data_check['avg_person_count']), 2) if data_check and data_check['avg_person_count'] else 0,
                "min_person_count": data_check['min_person_count'] if data_check else None,
                "max_person_count": data_check['max_person_count'] if data_check else None
            },
            "cameras": cameras,
            "violations_analysis": violations_analysis,
            "recommendations": generate_recommendations(config_summary, violations_analysis)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in config check: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def generate_recommendations(config_summary: dict, violations_analysis: list) -> list:
    """Generate actionable recommendations based on configuration"""
    recommendations = []
    
    if config_summary['properly_configured'] == 0:
        recommendations.append({
            "priority": "critical",
            "issue": "No cameras are properly configured",
            "action": "Enable alerts and set thresholds on your cameras",
            "sql": "UPDATE video_stream SET alert_enabled = TRUE, count_threshold_greater = 10, count_threshold_less = 2 WHERE workspace_id = 'your-workspace-id';"
        })
    
    if config_summary['alerts_enabled'] < config_summary['total_cameras']:
        recommendations.append({
            "priority": "high",
            "issue": f"Only {config_summary['alerts_enabled']} of {config_summary['total_cameras']} cameras have alerts enabled",
            "action": "Enable alerts on remaining cameras",
            "sql": "UPDATE video_stream SET alert_enabled = TRUE WHERE workspace_id = 'your-workspace-id' AND alert_enabled = FALSE;"
        })
    
    if config_summary['has_max_threshold'] == 0 and config_summary['has_min_threshold'] == 0:
        recommendations.append({
            "priority": "critical",
            "issue": "No thresholds configured on any camera",
            "action": "Set person count thresholds",
            "sql": "UPDATE video_stream SET count_threshold_greater = 10, count_threshold_less = 2 WHERE workspace_id = 'your-workspace-id';"
        })
    
    cameras_without_violations = [v for v in violations_analysis if not v['has_violations'] and v['total_readings'] > 0]
    if cameras_without_violations:
        recommendations.append({
            "priority": "medium",
            "issue": f"{len(cameras_without_violations)} cameras have data but no violations",
            "action": "Review and adjust thresholds to match actual person count ranges",
            "details": [f"{v['camera_name']}: counts range from {v['min_count_recorded']} to {v['max_count_recorded']}, but thresholds are {v['min_threshold']}-{v['max_threshold']}" 
                       for v in cameras_without_violations[:3]]
        })
    
    if config_summary['total_stream_results'] == 0:
        recommendations.append({
            "priority": "critical",
            "issue": "No stream results data in database",
            "action": "Ensure cameras are streaming and saving results to stream_results table"
        })
    
    return recommendations
