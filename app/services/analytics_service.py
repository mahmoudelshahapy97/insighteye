# app/services/analytics_service.py
from typing import Dict, Any, Optional, List, Union, Tuple
from uuid import UUID
from datetime import date, datetime
from zoneinfo import ZoneInfo
import logging

from app.services.database import db_manager
from app.utils import parse_string_or_list

logger = logging.getLogger(__name__)


class AnalyticsService:
    """Service layer for all analytics database operations"""
    
    def __init__(self):
        self.db_manager = db_manager

    # ========== Helper Methods ==========
    
    def _build_location_filters(
        self,
        params: list,
        param_count: int,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Tuple[List[str], int]:
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

    def _build_date_filter(
        self,
        params: list,
        param_count: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Tuple[str, int]:
        """Build date range filter"""
        date_filter = ""
        
        if start_date:
            param_count += 1
            params.append(start_date)
            date_filter += f" AND date >= ${param_count}"
        
        if end_date:
            param_count += 1
            params.append(end_date)
            date_filter += f" AND date <= ${param_count}"
        
        return date_filter, param_count

    # ========== Camera Analytics ==========
    
    async def get_unique_cameras(
        self,
        workspace_id: UUID,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get list of unique cameras with location filters"""
        try:
            params = [workspace_id]
            param_count = 1
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            cameras = [dict(row) for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching unique cameras: {e}", exc_info=True)
            raise

    async def get_frame_counts_per_camera(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get total frame counts per camera with date and location filters"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                SELECT
                    camera_name,
                    camera_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
                    COUNT(*) AS total_frames,
                    MIN(timestamp) AS first_frame,
                    MAX(timestamp) AS last_frame
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                  {location_where}
                GROUP BY camera_name, camera_id, location, area, building, zone, floor_level
                ORDER BY total_frames DESC
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [dict(row) for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching frame counts: {e}", exc_info=True)
            raise

    async def get_average_people_per_camera(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of people recorded by each camera"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                SELECT
                    camera_name,
                    camera_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
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
                GROUP BY camera_name, camera_id, location, area, building, zone, floor_level
                ORDER BY avg_person_count DESC
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching average people: {e}", exc_info=True)
            raise

    async def get_average_gender_per_camera(
        self,
        workspace_id: UUID,
        gender: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of males or females recorded by each camera"""
        try:
            params = [workspace_id]
            param_count = 1
            count_column = "male_count" if gender == "male" else "female_count"
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                SELECT
                    camera_name,
                    camera_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
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
                GROUP BY camera_name, camera_id, location, area, building, zone, floor_level
                ORDER BY avg_count DESC
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_count': int(round(row['avg_count'])) if row['avg_count'] else 0,
                'gender': gender
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching average gender: {e}", exc_info=True)
            raise

    async def get_camera_frame_comparison(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Compare camera frame counts to average"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                WITH frame_counts AS (
                    SELECT
                        camera_name,
                        camera_id,
                        location,
                        area,
                        building,
                        zone,
                        floor_level,
                        COUNT(*) AS frame_count
                    FROM stream_results
                    WHERE workspace_id = $1
                      AND camera_name IS NOT NULL
                      {date_filter}
                      {location_where}
                    GROUP BY camera_name, camera_id, location, area, building, zone, floor_level
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_frame_count': int(round(row['avg_frame_count'])) if row['avg_frame_count'] else 0,
                'percent_difference': float(row['percent_difference']) if row['percent_difference'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching frame comparison: {e}", exc_info=True)
            raise

    # ========== Zone Analytics ==========
    
    async def get_gender_by_zone_and_weekday(
        self,
        workspace_id: UUID,
        gender: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of males/females in each zone by weekday"""
        try:
            params = [workspace_id]
            param_count = 1
            count_column = "male_count" if gender == "male" else "female_count"
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_count': int(round(row['avg_count'])) if row['avg_count'] else 0,
                'gender': gender,
                'weekday_name': row['weekday_name'].strip()
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching gender by zone/weekday: {e}", exc_info=True)
            raise

    # ========== Time-based Analytics ==========
    
    async def get_busiest_hour_per_weekday(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get the busiest hour for each weekday"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'weekday_name': row['weekday_name'].strip(),
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching busiest hours: {e}", exc_info=True)
            raise

    # ========== Floor Analytics ==========
    
    async def get_average_people_by_floor(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of people by floor level"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching floor averages: {e}", exc_info=True)
            raise

    async def get_people_by_floor_and_weekday(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of people by floor level and weekday"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'weekday_name': row['weekday_name'].strip(),
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detections by camera: {e}", exc_info=True)
            raise
    
    async def get_people_by_floor_and_hour(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get average number of people by floor level and hour"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching floor/hour data: {e}", exc_info=True)
            raise

    # ========== Timeseries Analytics ==========
    
    async def get_camera_timeseries(
        self,
        workspace_id: UUID,
        camera_id: Optional[UUID] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None,
        limit: int = 5000
    ) -> Dict[str, Any]:
        """Get time series data for line graph"""
        try:
            params = [workspace_id]
            param_count = 1
            filters = ["workspace_id = $1"]
            
            if camera_id:
                param_count += 1
                params.append(camera_id)
                filters.append(f"stream_id = ${param_count}")
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            if date_filter:
                # Remove leading " AND "
                filters.append(date_filter.replace(" AND ", "", 1))
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            cameras_data = {}
            if results:
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

        except Exception as e:
            logger.error(f"Error fetching timeseries data: {e}", exc_info=True)
            raise

    # ========== Fire Detection Analytics ==========
    
    async def get_fire_detection_summary(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get summary of fire detections across all cameras"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [dict(row) for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detection summary: {e}", exc_info=True)
            raise

    async def get_fire_detections_by_camera(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire detection counts per camera"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                SELECT
                    camera_name,
                    camera_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
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
                  {location_where}
                GROUP BY camera_name, camera_id, location, area, building, zone, floor_level
                ORDER BY fire_detections DESC, camera_name
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching unique cameras: {e}", exc_info=True)
            raise

    async def get_fire_detections_by_location(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        group_by: str = "location",
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire detection counts by location grouping"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
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
                  {location_where}
                GROUP BY COALESCE({group_by}, 'Unknown')
                ORDER BY fire_detections DESC
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'group_type': group_by,
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detections by location: {e}", exc_info=True)
            raise

    async def get_fire_detections_timeline(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        interval: str = "day",
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire detections over time"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_detections,
                    COUNT(DISTINCT stream_id) AS cameras_checked,
                    COUNT(DISTINCT CASE WHEN fire_status != 'no detection' THEN stream_id END) AS cameras_with_fire,
                    ROUND((COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END)::NUMERIC / 
                           NULLIF(COUNT(*), 0) * 100), 2) AS fire_detection_rate
                FROM stream_results
                WHERE workspace_id = $1
                  AND timestamp IS NOT NULL
                  {date_filter}
                  {location_where}
                GROUP BY {truncate_expr}
                ORDER BY time_period DESC
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'time_period': row['time_period'].isoformat(),
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detections timeline: {e}", exc_info=True)
            raise

    async def get_fire_detections_by_weekday(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire detection patterns by day of week"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
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
                  {location_where}
                GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
                ORDER BY weekday_num
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'weekday_name': row['weekday_name'].strip(),
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detections by weekday: {e}", exc_info=True)
            raise

    async def get_fire_detections_by_hour(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire detection patterns by hour of day"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
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
                  {location_where}
                GROUP BY EXTRACT(HOUR FROM time)
                ORDER BY hour
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0,
                'avg_detections_per_day': float(row['avg_detections_per_day']) if row['avg_detections_per_day'] else 0
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching fire detections by hour: {e}", exc_info=True)
            raise

    async def get_recent_fire_detections(
        self,
        workspace_id: UUID,
        limit: int = 50,
        camera_id: Optional[UUID] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get most recent fire detections"""
        try:
            params = [workspace_id]
            param_count = 1
            camera_filter = ""
            
            if camera_id:
                param_count += 1
                params.append(camera_id)
                camera_filter = f" AND stream_id = ${param_count}"
            
            location_filters, param_count = self._build_location_filters(
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'timestamp': row['timestamp'].isoformat(),
                'result_id': str(row['result_id']),
                'stream_id': str(row['stream_id'])
            } for row in results] if results else []
            
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
            
        except Exception as e:
            logger.error(f"Error fetching recent fire detections: {e}", exc_info=True)
            raise

    async def get_high_risk_cameras(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        threshold_percentage: float = 5.0,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get cameras with high fire detection rates"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
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
                      {location_where}
                    GROUP BY camera_name, camera_id, stream_id, location, area, building, zone, floor_level
                )
                SELECT *
                FROM camera_stats
                WHERE fire_detection_rate >= ${param_count + 1}
                ORDER BY fire_detection_rate DESC, fire_detections DESC
            """
            
            params.append(threshold_percentage)
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                **dict(row),
                'stream_id': str(row['stream_id']),
                'fire_detection_rate': round(float(row['fire_detection_rate']), 2) if row['fire_detection_rate'] else 0
            } for row in results] if results else []
            
            return {
                "success": True,
                "threshold_percentage": threshold_percentage,
                "high_risk_count": len(data),
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
            
        except Exception as e:
            logger.error(f"Error fetching high risk cameras: {e}", exc_info=True)
            raise

    async def get_fire_status_by_camera(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get fire status counts (smoke/fire) per camera"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            location_where = " AND " + " AND ".join(location_filters) if location_filters else ""
                
            query = f"""
                SELECT
                    camera_name,
                    camera_id,
                    location,
                    area,
                    building,
                    zone,
                    floor_level,
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
                GROUP BY camera_name, camera_id, location, area, building, zone, floor_level, fire_status
                ORDER BY camera_name, fire_status
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            cameras_data = {}
            if results:
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
            
        except Exception as e:
            logger.error(f"Error fetching fire status by camera: {e}", exc_info=True)
            raise

    # ========== Threshold Violation Analytics ==========
    
    async def get_threshold_violations_by_camera(
        self,
        workspace_id: UUID,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        locations: Optional[Union[str, List[str]]] = None,
        areas: Optional[Union[str, List[str]]] = None,
        buildings: Optional[Union[str, List[str]]] = None,
        floor_levels: Optional[Union[str, List[str]]] = None,
        zones: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Get person count threshold violations per camera"""
        try:
            params = [workspace_id]
            param_count = 1
            
            date_filter, param_count = self._build_date_filter(params, param_count, start_date, end_date)
            if date_filter:
                # Prefix with sr. for this query
                date_filter = date_filter.replace(" AND date", " AND sr.date")
            
            location_filters, param_count = self._build_location_filters(
                params, param_count, locations, areas, buildings, floor_levels, zones
            )
            
            # Need to prefix location filters with "sr." for this query
            location_where = ""
            if location_filters:
                prefixed_filters = [
                    f.replace("location", "sr.location")
                     .replace("area", "sr.area")
                     .replace("building", "sr.building")
                     .replace("floor_level", "sr.floor_level")
                     .replace("zone", "sr.zone") 
                    for f in location_filters
                ]
                location_where = " AND " + " AND ".join(prefixed_filters)
                
            query = f"""
                SELECT
                    sr.camera_name,
                    sr.camera_id,
                    sr.location,
                    sr.area,
                    sr.building,
                    sr.zone,
                    sr.floor_level,
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
                  {location_where}
                GROUP BY 
                    sr.camera_name, 
                    sr.camera_id, 
                    sr.location, 
                    sr.area, 
                    sr.building, 
                    sr.zone,
                    sr.floor_level,
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
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            data = [{
                'camera_name': row['camera_name'],
                'camera_id': row['camera_id'],
                'location': row['location'],
                'area': row['area'],
                'building': row['building'],
                'zone': row['zone'],
                'floor_level': row['floor_level'],
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
                'avg_person_count': int(round(row['avg_person_count'])) if row['avg_person_count'] else 0,
                'max_person_count': row['max_person_count'],
                'min_person_count': row['min_person_count'],
                'last_above_max_time': row['last_above_max_time'].isoformat() if row['last_above_max_time'] else None,
                'last_below_min_time': row['last_below_min_time'].isoformat() if row['last_below_min_time'] else None
            } for row in results] if results else []
            
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
                "filters_applied": {
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "locations": parse_string_or_list(locations),
                    "areas": parse_string_or_list(areas),
                    "buildings": parse_string_or_list(buildings),
                    "floor_levels": parse_string_or_list(floor_levels),
                    "zones": parse_string_or_list(zones)
                },
                "count": len(data),
                "data": data
            }
            
        except Exception as e:
            logger.error(f"Error fetching threshold violations: {e}", exc_info=True)
            raise


# Global service instance
analytics_service = AnalyticsService()