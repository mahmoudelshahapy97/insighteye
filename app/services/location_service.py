# app/services/location_service.py
################################
from typing import List, Optional, Dict, Any
from uuid import UUID
import logging
import json
import asyncpg
from fastapi import HTTPException

from app.services.database import db_manager
from app.schemas import CameraLocationGroup, CameraGroupResponse

logger = logging.getLogger(__name__)

class LocationService:
    def __init__(self):
        self.db_manager = db_manager

    async def get_cameras_by_location(
        self,
        workspace_id: UUID,
        user_id: UUID,
        user_role: str,
        group_by: str,
        location: Optional[str] = None,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None
    ) -> CameraGroupResponse:
        """Get cameras grouped by location criteria."""
        try:
            conditions = ["vs.workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # Add filters
            if location:
                param_count += 1
                conditions.append(f"vs.location = ${param_count}")
                params.append(location)
            if area:
                param_count += 1
                conditions.append(f"vs.area = ${param_count}")
                params.append(area)
            if building:
                param_count += 1
                conditions.append(f"vs.building = ${param_count}")
                params.append(building)
            if zone:
                param_count += 1
                conditions.append(f"vs.zone = ${param_count}")
                params.append(zone)
            if floor_level:
                param_count += 1
                conditions.append(f"vs.floor_level = ${param_count}")
                params.append(floor_level)
            
            # Add user permission filter if not admin
            if user_role != "admin":
                param_count += 1
                conditions.append(f"vs.user_id = ${param_count}")
                params.append(user_id)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    vs.{group_by} as group_name,
                    COUNT(*) as camera_count,
                    COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_count,
                    COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_count,
                    COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_count,
                    COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_count,
                    ARRAY_AGG(
                        JSON_BUILD_OBJECT(
                            'id', vs.stream_id::text,
                            'name', vs.name,
                            'status', vs.status,
                            'is_streaming', vs.is_streaming,
                            'location', vs.location,
                            'area', vs.area,
                            'building', vs.building,
                            'floor_level', vs.floor_level,
                            'zone', vs.zone,
                            'alert_enabled', vs.alert_enabled,
                            'count_threshold_greater', vs.count_threshold_greater,
                            'count_threshold_less', vs.count_threshold_less,
                            'owner_username', u.username
                        ) ORDER BY vs.name
                    ) as cameras
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE {where_clause} AND vs.{group_by} IS NOT NULL
                GROUP BY vs.{group_by}
                ORDER BY vs.{group_by}
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            groups = []
            total_cameras = 0
            
            for result in results:
                cameras_data = result["cameras"] or []
                parsed_cameras = []
                
                # FIX: Better JSON handling
                for camera_json in cameras_data:
                    if camera_json is None:
                        continue
                        
                    # asyncpg with JSON_BUILD_OBJECT should return dict directly
                    if isinstance(camera_json, dict):
                        parsed_cameras.append(camera_json)
                    elif isinstance(camera_json, str):
                        # Fallback for string representation
                        try:
                            parsed_cameras.append(json.loads(camera_json))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse camera JSON: {camera_json}. Error: {e}")
                    else:
                        logger.warning(f"Unexpected camera data type: {type(camera_json)}")
                
                group = CameraLocationGroup(
                    group_type=group_by,
                    group_name=result["group_name"] or "Unknown",
                    camera_count=result["camera_count"],
                    cameras=parsed_cameras,
                    active_count=result["active_count"] or 0,
                    inactive_count=result["inactive_count"] or 0,
                    streaming_count=result["streaming_count"] or 0,
                    alert_enabled_count=result["alert_enabled_count"] or 0
                )
                groups.append(group)
                total_cameras += group.camera_count
            
            return CameraGroupResponse(
                groups=groups,
                total_groups=len(groups),
                total_cameras=total_cameras,
                group_type=group_by
            )
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error getting cameras by location: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error occurred.")
        except Exception as e:
            logger.error(f"Error getting cameras by location: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to retrieve cameras by location.")
            
    async def get_location_values(
        self,
        workspace_id: UUID,
        user_id: UUID,
        user_role: str,
        field: str,
        filter_field: Optional[str] = None,
        filter_values: Optional[List[str]] = None
    ) -> Dict:
        """Get unique values for a location field with optional filtering."""
        try:
            # FIX: Validate field names to prevent SQL injection
            valid_fields = ['location', 'area', 'building', 'floor_level', 'zone']
            if field not in valid_fields:
                raise ValueError(f"Invalid field: {field}. Must be one of {valid_fields}")
            
            if filter_field and filter_field not in valid_fields:
                raise ValueError(f"Invalid filter_field: {filter_field}. Must be one of {valid_fields}")
            
            conditions = ["workspace_id = $1", f"{field} IS NOT NULL", f"{field} != ''"]
            params = [workspace_id]
            param_count = 1
            
            # Add user filter if not admin
            if user_role != "admin":
                param_count += 1
                conditions.append(f"user_id = ${param_count}")
                params.append(user_id)
            
            # Add filter if provided
            if filter_field and filter_values:
                placeholders = []
                for filter_val in filter_values:
                    param_count += 1
                    placeholders.append(f"${param_count}")
                    params.append(filter_val)
                conditions.append(f"{filter_field} IN ({', '.join(placeholders)})")
            
            where_clause = " AND ".join(conditions)
            
            # Build field list for SELECT
            select_fields = [field]
            if field == "location":
                additional_fields = []
            elif field == "area":
                additional_fields = ["location"]
            elif field == "building":
                additional_fields = ["area", "location"]
            elif field == "floor_level":
                additional_fields = ["building", "area", "location"]
            elif field == "zone":
                additional_fields = ["floor_level", "building", "area", "location"]
            else:
                additional_fields = []
            
            select_fields.extend(additional_fields)
            select_clause = ", ".join(select_fields)
            
            query = f"""
                SELECT DISTINCT {select_clause}, COUNT(*) as camera_count
                FROM video_stream 
                WHERE {where_clause}
                GROUP BY {select_clause}
                ORDER BY {select_clause}
            """
            
            results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            
            values = []
            for r in results:
                value_dict = {field: r[field], "camera_count": r["camera_count"]}
                for add_field in additional_fields:
                    value_dict[add_field] = r[add_field]
                values.append(value_dict)
            
            return {
                f"{field}s": values,
                "total_count": len(values),
                f"filtered_by_{filter_field}s" if filter_field else "filter": filter_values if filter_values else None
            }
            
        except ValueError as ve:
            logger.error(f"Validation error in get_location_values: {ve}")
            raise HTTPException(status_code=400, detail=str(ve))
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error getting {field} values: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error occurred.")
        except Exception as e:
            logger.error(f"Error getting {field} values: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve {field} values.")

location_service = LocationService()