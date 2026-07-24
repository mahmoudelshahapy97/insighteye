# app/services/camera_service.py
from typing import List, Optional, Dict, Any, Tuple, Union
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import logging
import asyncpg
from fastapi import HTTPException, status

from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.postgres_service import  postgres_service
from app.services.workspace_service import workspace_service
from app.utils import (
    ensure_uuid_str,
    check_workspace_access
)
from app.schemas import (
    StreamCreate, StreamUpdate,
    CameraState, CamerasStateResponse,
    LocationHierarchyResponse, CameraAlertSettings, BulkLocationAssignmentWithAlerts
)

logger = logging.getLogger(__name__)

class CameraService:
    def __init__(self):
        self.db_manager = db_manager
        self.user_manager = user_manager
        self.workspace_service = workspace_service
        self.postgres_service = postgres_service

    async def get_user_workspace(self, username: str) -> Tuple[UUID, UUID]:
        """Get user ID and workspace ID for a username."""
        user_id, workspace_id = await self.workspace_service.get_user_and_workspace(username)
        if not workspace_id:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")
        return user_id, workspace_id

    async def check_camera_limit(self, user_id: UUID, workspace_id: UUID, user_system_role: str, allowed_count: int) -> None:
        """Check if user has reached their camera limit."""
        if user_system_role != 'admin':
            count_query = "SELECT COUNT(*) as stream_count FROM video_stream WHERE user_id = $1 AND workspace_id = $2"
            count_result = await self.db_manager.execute_query(count_query, params=(user_id, workspace_id), fetch_one=True)
            current_stream_count = count_result['stream_count'] if count_result else 0
            if current_stream_count >= allowed_count:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Camera limit ({allowed_count}) in workspace '{workspace_id}' exceeded. You currently have {current_stream_count} cameras."
                )

    async def create_camera(
        self, 
        stream: StreamCreate, 
        user_id: UUID, 
        workspace_id: UUID,
        username: str
    ) -> str:
        """Create a new camera stream."""
        try:
            # Check camera limit
            user_db_details = await self.user_manager.get_user_by_id(str(user_id))
            allowed_camera_count = user_db_details.get("count_of_camera", 5)
            user_system_role = user_db_details.get("role", "user")
            
            await self.check_camera_limit(user_id, workspace_id, user_system_role, allowed_camera_count)

            stream_id = uuid4()
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            
            insert_query = """
                INSERT INTO video_stream
                (stream_id, user_id, workspace_id, name, path, type, status, is_streaming,
                 location, area, building, floor_level, zone, latitude, longitude,
                 count_threshold_greater, count_threshold_less, alert_enabled,
                 is_shoplifting_camera,
                 created_at, updated_at, last_activity)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
            """

            await self.db_manager.execute_query(
                insert_query,
                params=(
                    stream_id, user_id, workspace_id, stream.name, stream.path, stream.type,
                    stream.status, stream.is_streaming,
                    getattr(stream, 'location', None), getattr(stream, 'area', None),
                    getattr(stream, 'building', None), getattr(stream, 'floor_level', None),
                    getattr(stream, 'zone', None), getattr(stream, 'latitude', None),
                    getattr(stream, 'longitude', None), getattr(stream, 'count_threshold_greater', None),
                    getattr(stream, 'count_threshold_less', None), getattr(stream, 'alert_enabled', True),
                    getattr(stream, 'is_shoplifting_camera', False),
                    now_utc, now_utc, now_utc
                )
            )
            
            return str(stream_id)
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error creating stream for user {user_id}: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error occurred while creating stream.")

    async def get_user_cameras(
        self, 
        user_id: UUID, 
        workspace_id: UUID,
        user_role: str,
        encoded_string: str,
        return_base64: bool
    ) -> List[Dict]:
        """Get cameras for a user in their workspace."""
        try:
            query_sql_base = """
                SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path,
                       vs.type, vs.status, vs.is_streaming, vs.created_at, vs.updated_at,
                       vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                       vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                       vs.is_shoplifting_camera,
                       w.name as workspace_name
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
            """
            
            if user_role == "admin":
                query_sql = query_sql_base + " WHERE vs.workspace_id = $1 ORDER BY u.username, vs.created_at DESC"
                params = (workspace_id,)
            else:
                query_sql = query_sql_base + " WHERE vs.user_id = $1 AND vs.workspace_id = $2 ORDER BY u.username, vs.created_at DESC"
                params = (user_id, workspace_id)
                
            streams_data = await self.db_manager.execute_query(query_sql, params=params, fetch_all=True)
            
            return [
                {
                    "id": str(s["stream_id"]), 
                    "user_id": str(s["user_id"]), 
                    "owner_username": s["owner_username"],
                    "name": s["name"], 
                    "path": s["path"], 
                    "type": s["type"], 
                    "status": s["status"], 
                    "is_streaming": s["is_streaming"],
                    "location": s["location"],
                    "area": s["area"],
                    "building": s["building"],
                    "floor_level": s["floor_level"],
                    "zone": s["zone"],
                    "latitude": float(s["latitude"]) if s["latitude"] else None,
                    "longitude": float(s["longitude"]) if s["longitude"] else None,
                    "count_threshold_greater": s["count_threshold_greater"],
                    "count_threshold_less": s["count_threshold_less"],
                    "alert_enabled": s["alert_enabled"],
                    "is_shoplifting_camera": s["is_shoplifting_camera"],
                    "created_at": s["created_at"].isoformat() if s["created_at"] else None,
                    "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
                    "workspace_id": str(workspace_id), 
                    "workspace_name": s["workspace_name"],
                    "static_base64": encoded_string if return_base64 else ""
                } for s in streams_data
            ] if streams_data else []
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error retrieving streams for user {user_id}: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error occurred while retrieving streams.")

    async def update_camera(
        self,
        stream_update: StreamUpdate,
        user_id: UUID,
        current_user_role: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Update a single camera. Returns (success, error_message).
        """
        try:
            stream_id_str = ensure_uuid_str(stream_update.id)
            
            # Get stream info
            stream_info_query = "SELECT user_id, workspace_id FROM video_stream WHERE stream_id = $1"
            stream_info = await self.db_manager.execute_query(
                stream_info_query, 
                params=(UUID(stream_id_str),), 
                fetch_one=True
            )

            if not stream_info:
                return False, f"Stream ID {stream_id_str} not found"
            
            stream_owner_id = stream_info["user_id"]
            stream_workspace_id = stream_info["workspace_id"]

            # Check permissions
            can_update = False
            if stream_owner_id == user_id:
                can_update = True
            else:
                try:
                    member_info = await check_workspace_access(
                        self.db_manager,
                        user_id,
                        stream_workspace_id,
                        required_role="admin",
                    )
                    can_update = True
                except HTTPException:
                    pass
            
            if not can_update and current_user_role == "admin":
                can_update = True

            if not can_update:
                return False, f"Unauthorized to update stream {stream_id_str}"
            
            # Build update query
            set_clauses, params = [], []
            param_idx = 1
            snapshot_fields = {}

            # Core fields
            if stream_update.name is not None:
                set_clauses.append(f"name = ${param_idx}")
                params.append(stream_update.name)
                param_idx += 1
                snapshot_fields['camera_name'] = stream_update.name
            if stream_update.path is not None:
                set_clauses.append(f"path = ${param_idx}")
                params.append(stream_update.path)
                param_idx += 1
            if stream_update.type is not None:
                set_clauses.append(f"type = ${param_idx}")
                params.append(stream_update.type)
                param_idx += 1
            if stream_update.status is not None:
                set_clauses.append(f"status = ${param_idx}")
                params.append(stream_update.status)
                param_idx += 1
            if stream_update.is_streaming is not None:
                set_clauses.append(f"is_streaming = ${param_idx}")
                params.append(stream_update.is_streaming)
                param_idx += 1
            
            # Location fields
            location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']
            for field in location_fields:
                if hasattr(stream_update, field) and getattr(stream_update, field) is not None:
                    set_clauses.append(f"{field} = ${param_idx}")
                    params.append(getattr(stream_update, field))
                    param_idx += 1
                    snapshot_fields[field] = getattr(stream_update, field)
            
            # Alert fields
            alert_fields = ['count_threshold_greater', 'count_threshold_less', 'alert_enabled']
            for field in alert_fields:
                if hasattr(stream_update, field) and getattr(stream_update, field) is not None:
                    set_clauses.append(f"{field} = ${param_idx}")
                    params.append(getattr(stream_update, field))
                    param_idx += 1

            if hasattr(stream_update, 'is_shoplifting_camera') and stream_update.is_shoplifting_camera is not None:
                set_clauses.append(f"is_shoplifting_camera = ${param_idx}")
                params.append(stream_update.is_shoplifting_camera)
                param_idx += 1

            if not set_clauses:
                return True, None  # Nothing to update
            
            # Add updated_at
            set_clauses.append(f"updated_at = ${param_idx}")
            params.append(datetime.now(ZoneInfo("Africa/Cairo")))
            param_idx += 1
            
            # Add WHERE clause
            update_query = f"UPDATE video_stream SET {', '.join(set_clauses)} WHERE stream_id = ${param_idx}"
            params.append(UUID(stream_id_str))

            rows_affected = await self.db_manager.execute_query(
                update_query, 
                params=tuple(params), 
                return_rowcount=True
            )
            
            if rows_affected and rows_affected > 0:
                await self._sync_stream_results_snapshot(UUID(stream_id_str), snapshot_fields)
                return True, None
            else:
                return False, f"Update affected 0 rows for stream {stream_id_str}"

        except Exception as e:
            logger.error(f"Error updating stream: {e}", exc_info=True)
            return False, str(e)

    async def _sync_stream_results_snapshot(self, stream_id: UUID, field_values: Dict[str, Any]) -> None:
        """Propagate camera name/hierarchy changes into the stream_results snapshot so charts reflect the latest values."""
        if not field_values:
            return
        set_clauses, params = [], []
        for column, value in field_values.items():
            params.append(value)
            set_clauses.append(f"{column} = ${len(params)}")
        params.append(stream_id)
        query = f"UPDATE stream_results SET {', '.join(set_clauses)} WHERE stream_id = ${len(params)}"
        try:
            await self.db_manager.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error syncing stream_results snapshot for {stream_id}: {e}", exc_info=True)

    async def delete_cameras(
        self,
        camera_ids: List[str],
        user_id: UUID,
        current_user_role: str
    ) -> Tuple[List[str], List[str], List[str]]:
        """
        Delete multiple cameras. 
        Returns (deleted_ids, unauthorized_ids, not_found_ids).
        """
        valid_ids = []
        unauthorized_ids = []
        not_found_ids = []
        workspace_camera_mapping = {}
        
        for id_str_raw in camera_ids:
            try:
                id_str = ensure_uuid_str(id_str_raw)
            except (HTTPException, ValueError) as e:
                unauthorized_ids.append(f"{id_str_raw} (invalid format)")
                continue

            stream_info = await self.db_manager.execute_query(
                "SELECT user_id, workspace_id FROM video_stream WHERE stream_id = $1",
                params=(UUID(id_str),),
                fetch_one=True
            )

            if not stream_info:
                not_found_ids.append(id_str)
                continue
            
            stream_owner_id = stream_info["user_id"]
            stream_workspace_id = stream_info["workspace_id"]
            
            can_delete = False
            if stream_owner_id == user_id:
                can_delete = True
            else:
                try:
                    member_info = await check_workspace_access(
                        self.db_manager,
                        user_id,
                        stream_workspace_id,
                        required_role="admin",
                    )
                    can_delete = True
                except HTTPException:
                    pass
            
            if not can_delete and current_user_role == "admin":
                can_delete = True

            if can_delete:
                valid_ids.append(id_str)
                workspace_str = str(stream_workspace_id)
                if workspace_str not in workspace_camera_mapping:
                    workspace_camera_mapping[workspace_str] = []
                workspace_camera_mapping[workspace_str].append(id_str)
            else:
                unauthorized_ids.append(id_str)
        
        deleted_ids = []
        
        if valid_ids:
            # Delete from PostgreSQL
            placeholders = ", ".join([f"${i+1}" for i in range(len(valid_ids))])
            delete_query = f"DELETE FROM video_stream WHERE stream_id IN ({placeholders})"
            uuid_params = tuple(UUID(id_str) for id_str in valid_ids)
            
            deleted_count = await self.db_manager.execute_query(
                delete_query, 
                params=uuid_params, 
                return_rowcount=True
            )
            
            if deleted_count and deleted_count > 0:
                deleted_ids = valid_ids.copy()
        
        return deleted_ids, unauthorized_ids, not_found_ids

    async def get_workspace_data_timestamp_range(
        self,
        workspace_id: str,
        current_user_data: Dict
    ) -> Dict:
        """Get timestamp range for workspace data."""
        return await self.postgres_service.get_timestamp_range(
            workspace_id=workspace_id,
            camera_ids=None,
            user_system_role=current_user_data.get("role"),
            user_workspace_role=None,
            requesting_username=current_user_data.get("username")
        )

    async def get_camera_state(
        self, 
        workspace_id: UUID
    ) -> CamerasStateResponse:
        """Get camera state summary for a workspace."""
        try:
            query = """
                SELECT vs.stream_id, vs.name, vs.status, vs.is_streaming, u.username as owner_username
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.workspace_id = $1
            """
            camera_rows = await self.db_manager.execute_query(query, params=(workspace_id,), fetch_all=True)
            
            cameras = [
                CameraState(
                    id=str(row["stream_id"]),
                    name=f"{row['name']} (Owner: {row['owner_username']})",
                    status=row["status"],
                    is_streaming=row["is_streaming"]
                ) for row in camera_rows
            ] if camera_rows else []
            
            active_count = sum(1 for cam in cameras if cam.status == "active")
            inactive_count = sum(1 for cam in cameras if cam.is_streaming is False or cam.status == "inactive")
            error_count = sum(1 for cam in cameras if cam.status == "error")
            processing_count = sum(1 for cam in cameras if cam.status == "processing")

            return CamerasStateResponse(
                cameras=cameras,
                total_active=active_count,
                total_inactive=inactive_count,
                total_error=error_count,
                total_processing=processing_count,
                total_cameras=len(cameras)
            )
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error fetching camera state: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error fetching camera state.")

    async def get_location_hierarchy(
        self, 
        workspace_id: UUID
    ) -> LocationHierarchyResponse:
        """Get location hierarchy for a workspace."""
        try:
            hierarchy_query = "SELECT * FROM get_location_hierarchy($1)"
            hierarchy_results = await self.db_manager.execute_query(
                hierarchy_query, 
                (workspace_id,), 
                fetch_all=True
            )
            
            floor_levels = set()
            buildings = set()
            zones = set()
            areas = set()
            locations = set()
            
            hierarchy = []
            for row in hierarchy_results:
                hierarchy.append({
                    "building": row["building"],
                    "floor_level": row["floor_level"],
                    "zone": row["zone"],
                    "area": row["area"],
                    "location": row["location"],
                    "camera_count": row["camera_count"]
                })
                
                if row["floor_level"]:
                    floor_levels.add(row["floor_level"])
                if row["building"]:
                    buildings.add(row["building"])
                if row["zone"]:
                    zones.add(row["zone"])
                if row["area"]:
                    areas.add(row["area"])
                if row["location"]:
                    locations.add(row["location"])
            
            return LocationHierarchyResponse(
                workspace_id=str(workspace_id),
                hierarchy=hierarchy,
                total_locations=len(hierarchy),
                buildings=sorted(list(buildings)),
                floor_level=sorted(list(floor_levels)),
                zones=sorted(list(zones)),
                areas=sorted(list(areas)),
                locations=sorted(list(locations))
            )
            
        except Exception as e:
            logger.error(f"Error getting location hierarchy: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to retrieve location hierarchy.")

    async def update_alert_settings(
        self,
        camera_id: str,
        alert_data: CameraAlertSettings,
        user_id: UUID,
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """Update alert settings for a camera."""
        try:
            # Verify camera and permissions
            camera_check_query = """
                SELECT vs.stream_id, vs.user_id, vs.name 
                FROM video_stream vs
                WHERE vs.stream_id = $1 AND vs.workspace_id = $2
            """
            camera_info = await self.db_manager.execute_query(
                camera_check_query,
                (UUID(camera_id), workspace_id),
                fetch_one=True
            )
            
            if not camera_info:
                raise HTTPException(status_code=404, detail="Camera not found in workspace")
            
            user_role = await check_workspace_access(
                        self.db_manager,
                        user_id,
                        workspace_id,
                    )
            if camera_info["user_id"] != user_id and user_role.get("role") != "admin":
                raise HTTPException(status_code=403, detail="Permission denied")
            
            # Update alert settings
            update_query = """
                UPDATE video_stream 
                SET count_threshold_greater = $1,
                    count_threshold_less = $2,
                    alert_enabled = $3,
                    updated_at = $4
                WHERE stream_id = $5 AND workspace_id = $6
            """
            
            rows_affected = await self.db_manager.execute_query(
                update_query,
                params=(
                    alert_data.count_threshold_greater,
                    alert_data.count_threshold_less,
                    alert_data.alert_enabled,
                    datetime.now(ZoneInfo("Africa/Cairo")),
                    UUID(camera_id),
                    workspace_id
                ),
                return_rowcount=True
            )
            
            if not rows_affected or rows_affected == 0:
                raise HTTPException(status_code=404, detail="Camera not found or no changes made")
            
            return {
                "camera_id": camera_id,
                "camera_name": camera_info["name"],
                "alert_settings": alert_data.model_dump()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating alert settings: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to update alert settings.")

    async def bulk_assign_locations(
        self,
        camera_ids: List[str],
        location_data: BulkLocationAssignmentWithAlerts,
        user_id: UUID,
        workspace_id: UUID,
        user_role: str
    ) -> Tuple[List[str], List[Dict], List[str]]:
        """
        Bulk assign location and alert data to cameras.
        Returns (updated_cameras, failed_cameras, errors).
        """
        updated_cameras = []
        failed_cameras = []
        errors = []
        
        for camera_id_str in camera_ids:
            try:
                # Verify camera
                camera_check_query = """
                    SELECT vs.stream_id, vs.user_id, vs.name 
                    FROM video_stream vs
                    WHERE vs.stream_id = $1 AND vs.workspace_id = $2
                """
                camera_info = await self.db_manager.execute_query(
                    camera_check_query,
                    (UUID(camera_id_str), workspace_id),
                    fetch_one=True
                )
                
                if not camera_info:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Camera not found in workspace"
                    })
                    continue
                
                # Check permissions
                if camera_info["user_id"] != user_id and user_role != "admin":
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Permission denied"
                    })
                    continue
                
                # Build update query
                update_fields = []
                params = []
                param_count = 0
                snapshot_fields = {}

                # Location fields
                for field in ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']:
                    if hasattr(location_data, field) and getattr(location_data, field) is not None:
                        param_count += 1
                        update_fields.append(f"{field} = ${param_count}")
                        params.append(getattr(location_data, field))
                        snapshot_fields[field] = getattr(location_data, field)
                
                # Alert fields
                for field in ['count_threshold_greater', 'count_threshold_less', 'alert_enabled']:
                    if hasattr(location_data, field) and getattr(location_data, field) is not None:
                        param_count += 1
                        update_fields.append(f"{field} = ${param_count}")
                        params.append(getattr(location_data, field))
                
                if not update_fields:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "No data provided for update"
                    })
                    continue
                
                # Add updated_at
                param_count += 1
                update_fields.append(f"updated_at = ${param_count}")
                params.append(datetime.now(ZoneInfo("Africa/Cairo")))
                
                # Add WHERE clause
                param_count += 1
                params.append(UUID(camera_id_str))
                param_count += 1
                params.append(workspace_id)
                
                update_query = f"""
                    UPDATE video_stream 
                    SET {', '.join(update_fields)}
                    WHERE stream_id = ${param_count - 1} AND workspace_id = ${param_count}
                """
                
                rows_affected = await self.db_manager.execute_query(
                    update_query,
                    tuple(params),
                    return_rowcount=True
                )
                
                if rows_affected and rows_affected > 0:
                    await self._sync_stream_results_snapshot(UUID(camera_id_str), snapshot_fields)
                    updated_cameras.append(camera_id_str)
                else:
                    failed_cameras.append({
                        "camera_id": camera_id_str,
                        "error": "Update failed"
                    })
                    
            except Exception as e:
                logger.error(f"Error updating camera {camera_id_str}: {e}", exc_info=True)
                failed_cameras.append({
                    "camera_id": camera_id_str,
                    "error": f"Update error: {str(e)}"
                })
                errors.append(f"Camera {camera_id_str}: {str(e)}")
        
        return updated_cameras, failed_cameras, errors

    async def get_stream_by_id(self, stream_id_str: str, user_id_context_str: str) -> Dict[str, Any]:
        """Get stream information by ID."""
        stream_q = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, 
                   u.username as owner_username, vs.workspace_id, w.name as workspace_name,
                   vs.is_streaming, vs.user_id as owner_id, vs.created_at, vs.updated_at, vs.last_activity,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
                   vs.latitude, vs.longitude
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id = $1
        """
        stream_res = await self.db_manager.execute_query(stream_q, (UUID(stream_id_str),), fetch_one=True)
        
        if not stream_res:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace access
        await check_workspace_access(
            self.db_manager,
            UUID(user_id_context_str),
            stream_res["workspace_id"]
        )

        return {
            "stream_id": str(stream_res["stream_id"]),
            "name": stream_res["name"],
            "path": stream_res["path"],
            "type": stream_res["type"],
            "status": stream_res["status"],
            "owner_username": stream_res["owner_username"],
            "workspace_id": str(stream_res["workspace_id"]),
            "workspace_name": stream_res["workspace_name"],
            "location": stream_res["location"],
            "area": stream_res["area"],
            "building": stream_res["building"],
            "zone": stream_res["zone"],
            "floor_level": stream_res["floor_level"],
            "latitude": float(stream_res["latitude"]) if stream_res["latitude"] else None,
            "longitude": float(stream_res["longitude"]) if stream_res["longitude"] else None,
        }

    async def get_workspace_streams(self, user_id_str: str, workspace_id_str_optional: Optional[str] = None) -> Dict[str, Any]:
        """Get all streams for workspaces accessible to a user."""
        user_id_obj = UUID(user_id_str)
        target_workspace_ids_objs: List[UUID] = []

        if workspace_id_str_optional:
            try:
                ws_id_to_check = UUID(workspace_id_str_optional)
                await check_workspace_access(
                    self.db_manager,
                    user_id_obj,
                    ws_id_to_check
                )
                target_workspace_ids_objs.append(ws_id_to_check)
            except HTTPException as e:
                return {"streams": [], "total": 0, "message": f"Access denied or workspace not found: {e.detail}"}
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid workspace ID format provided.")
        else:
            user_workspaces_db = await self.db_manager.execute_query(
                "SELECT workspace_id FROM workspace_members WHERE user_id = $1",
                (user_id_obj,), fetch_all=True
            )
            user_workspaces_db = user_workspaces_db or []
            target_workspace_ids_objs = [row['workspace_id'] for row in user_workspaces_db]
        
        if not target_workspace_ids_objs:
            return {"streams": [], "total": 0}

        placeholders = ', '.join([f'${i+1}' for i in range(len(target_workspace_ids_objs))])

        streams_q = f"""
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.user_id as owner_id, u.username as owner_username,
                   vs.workspace_id, w.name as workspace_name,
                   vs.created_at, vs.updated_at,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
                   vs.latitude, vs.longitude
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.workspace_id IN ({placeholders})
            ORDER BY w.name, vs.name
        """
        streams_data_db = await self.db_manager.execute_query(streams_q, tuple(target_workspace_ids_objs), fetch_all=True)
        streams_data_db = streams_data_db or []
        
        formatted_streams = [{
            "stream_id": str(s["stream_id"]),
            "name": s["name"],
            "path": s["path"],
            "type": s["type"],
            "status": s["status"],
            "is_streaming": s["is_streaming"],
            "owner_id": str(s["owner_id"]),
            "owner_username": s["owner_username"],
            "workspace_id": str(s["workspace_id"]),
            "workspace_name": s["workspace_name"],
            "created_at": s["created_at"].isoformat() if s["created_at"] else None,
            "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
            "can_control": True,
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "floor_level": s["floor_level"],
            "latitude": float(s["latitude"]) if s["latitude"] else None,
            "longitude": float(s["longitude"]) if s["longitude"] else None,
        } for s in streams_data_db]
        
        return {"streams": formatted_streams, "total": len(formatted_streams)}

    async def search_cameras_by_filters(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None,
        q: Optional[str] = None,
        location_type: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        areas: Optional[List[str]] = None,
        buildings: Optional[List[str]] = None,
        floor_levels: Optional[List[str]] = None,
        zones: Optional[List[str]] = None,
        encoded_string: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Search cameras using location-based filters and text search.
        
        Args:
            user_id: The user performing the search
            workspace_id: Optional workspace to filter by
            q: Text search query
            location_type: Filter by location type(s)
            locations: Filter by specific location(s)
            areas: Filter by specific area(s)
            buildings: Filter by specific building(s)
            floor_levels: Filter by specific floor level(s)
            zones: Filter by specific zone(s)
            encoded_string: Base64 encoded string for static_base64 field
            
        Returns:
            List of camera dictionaries matching the filters
        """
        try:
            # Base query matching /source/user structure
            base_query = """
                SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, 
                    vs.type, vs.status, vs.is_streaming, vs.created_at, vs.updated_at,
                    vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                    vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                    w.name as workspace_name, vs.workspace_id
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
                JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
                WHERE wm.user_id = $1
            """
            
            params = [user_id]
            param_count = 1
            
            # Filter by workspace if provided
            if workspace_id:
                param_count += 1
                base_query += f" AND vs.workspace_id = ${param_count}"
                params.append(workspace_id)
            
            # Handle text search with special case for location_type + comma-separated values
            if q:
                q_values = [v.strip() for v in q.split(',')] if ',' in q else [q]
                
                # Special handling: single location_type with multiple comma-separated values
                if location_type and len(location_type) == 1 and len(q_values) > 1:
                    location_field_map = {
                        "area": "vs.area", "areas": "vs.area",
                        "building": "vs.building", "buildings": "vs.building",
                        "floor_level": "vs.floor_level", "floor_levels": "vs.floor_level",
                        "zone": "vs.zone", "zones": "vs.zone",
                        "location": "vs.location", "locations": "vs.location"
                    }
                    
                    field_name = location_field_map.get(location_type[0])
                    if field_name:
                        # Use exact match for specific values
                        param_count += 1
                        q_placeholders = ", ".join([f"${param_count + i}" for i in range(len(q_values))])
                        base_query += f" AND {field_name} IN ({q_placeholders})"
                        params.extend(q_values)
                        param_count += len(q_values) - 1
                    else:
                        # Fallback to general text search
                        param_count += 1
                        base_query += f"""
                            AND (
                                vs.name ILIKE ${param_count} OR 
                                vs.building ILIKE ${param_count} OR 
                                vs.floor_level ILIKE ${param_count} OR 
                                vs.zone ILIKE ${param_count} OR 
                                vs.area ILIKE ${param_count} OR 
                                vs.location ILIKE ${param_count}
                            )
                        """
                        params.append(f"%{q}%")
                else:
                    # Regular text search
                    param_count += 1
                    base_query += f"""
                        AND (
                            vs.name ILIKE ${param_count} OR 
                            vs.building ILIKE ${param_count} OR 
                            vs.floor_level ILIKE ${param_count} OR 
                            vs.zone ILIKE ${param_count} OR 
                            vs.area ILIKE ${param_count} OR 
                            vs.location ILIKE ${param_count}
                        )
                    """
                    params.append(f"%{q}%")
            
            # Add location type filter (supports multiple types)
            if location_type:
                type_conditions = []
                for loc_type in location_type:
                    if loc_type in ["building", "buildings"]:
                        type_conditions.append("(vs.building IS NOT NULL AND vs.building != '')")
                    elif loc_type in ["floor_level", "floor_levels"]:
                        type_conditions.append("(vs.floor_level IS NOT NULL AND vs.floor_level != '')")
                    elif loc_type in ["zone", "zones"]:
                        type_conditions.append("(vs.zone IS NOT NULL AND vs.zone != '')")
                    elif loc_type in ["area", "areas"]:
                        type_conditions.append("(vs.area IS NOT NULL AND vs.area != '')")
                    elif loc_type in ["location", "locations"]:
                        type_conditions.append("(vs.location IS NOT NULL AND vs.location != '')")
                
                if type_conditions:
                    base_query += f" AND ({' OR '.join(type_conditions)})"
            
            # Add specific location filters
            filter_configs = [
                (locations, "vs.location"),
                (areas, "vs.area"),
                (buildings, "vs.building"),
                (floor_levels, "vs.floor_level"),
                (zones, "vs.zone")
            ]
            
            for filter_values, column_name in filter_configs:
                if filter_values:
                    param_count += 1
                    placeholders = ", ".join([f"${param_count + i}" for i in range(len(filter_values))])
                    base_query += f" AND {column_name} IN ({placeholders})"
                    params.extend(filter_values)
                    param_count += len(filter_values) - 1
            
            base_query += " ORDER BY u.username, vs.created_at DESC"
            
            # Execute query
            results = await self.db_manager.execute_query(base_query, tuple(params), fetch_all=True)
            results = results or []
            
            # Format results to match /source/user response structure
            return [
                {
                    "id": str(r["stream_id"]),
                    "user_id": str(r["user_id"]),
                    "owner_username": r["owner_username"],
                    "name": r["name"],
                    "path": r["path"],
                    "type": r["type"],
                    "status": r["status"],
                    "is_streaming": r["is_streaming"],
                    "location": r["location"],
                    "area": r["area"],
                    "building": r["building"],
                    "floor_level": r["floor_level"],
                    "zone": r["zone"],
                    "latitude": float(r["latitude"]) if r["latitude"] else None,
                    "longitude": float(r["longitude"]) if r["longitude"] else None,
                    "count_threshold_greater": r["count_threshold_greater"],
                    "count_threshold_less": r["count_threshold_less"],
                    "alert_enabled": r["alert_enabled"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                    "workspace_id": str(r["workspace_id"]) if r["workspace_id"] else None,
                    "workspace_name": r["workspace_name"],
                    "static_base64": encoded_string
                } for r in results
            ]
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Database error searching cameras: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error occurred while searching cameras.")
        except Exception as e:
            logger.error(f"Error searching cameras by filters: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected error occurred while searching cameras.")

    async def create_stream(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        name: str,
        path: str,
        stream_type: str = 'rtsp',
        location_info: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Create a new video stream"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        stream_id = uuid4()
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        
        query = """
            INSERT INTO video_stream
            (stream_id, workspace_id, user_id, name, path, type, status, is_streaming,
             location, area, building, zone, floor_level, latitude, longitude,
             created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """
        
        location = location_info.get('location') if location_info else None
        area = location_info.get('area') if location_info else None
        building = location_info.get('building') if location_info else None
        zone = location_info.get('zone') if location_info else None
        floor_level = location_info.get('floor_level') if location_info else None
        latitude = float(location_info['latitude']) if location_info and location_info.get('latitude') else None
        longitude = float(location_info['longitude']) if location_info and location_info.get('longitude') else None
        
        await self.db_manager.execute_query(
            query,
            (stream_id, workspace_id_obj, user_id_obj, name, path, stream_type, 
             'inactive', False, location, area, building, zone, floor_level,
             latitude, longitude, now, now)
        )
        
        return stream_id
    
    async def update_stream(
        self,
        stream_id: Union[str, UUID],
        updates: StreamUpdate
    ) -> bool:
        """Update stream properties"""
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        # Build dynamic update query
        update_fields = []
        params = []
        param_count = 1
        
        update_dict = updates.model_dump(exclude_none=True)
        
        for field, value in update_dict.items():
            if field in ['latitude', 'longitude'] and value is not None:
                value = float(value)
            update_fields.append(f"{field} = ${param_count}")
            params.append(value)
            param_count += 1
        
        if not update_fields:
            return False
        
        # Always update updated_at
        update_fields.append(f"updated_at = ${param_count}")
        params.append(datetime.now(ZoneInfo("Africa/Cairo")))
        param_count += 1
        
        params.append(stream_id_obj)
        
        query = f"""
            UPDATE video_stream
            SET {', '.join(update_fields)}
            WHERE stream_id = ${param_count}
        """
        
        rows_affected = await self.db_manager.execute_query(
            query, tuple(params), return_rowcount=True
        )
        return rows_affected > 0
    
    async def update_stream_status(
        self,
        stream_id: Union[str, UUID],
        status: str,
        is_streaming: Optional[bool] = None
    ) -> bool:
        """Update stream status and streaming state"""
        valid_statuses = ['active', 'inactive', 'error', 'pending']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        if is_streaming is not None:
            query = """
                UPDATE video_stream
                SET status = $1, is_streaming = $2, updated_at = $3
                WHERE stream_id = $4
            """
            params = (status, is_streaming, datetime.now(ZoneInfo("Africa/Cairo")), stream_id_obj)
        else:
            query = """
                UPDATE video_stream
                SET status = $1, updated_at = $2
                WHERE stream_id = $3
            """
            params = (status, datetime.now(ZoneInfo("Africa/Cairo")), stream_id_obj)
        
        rows_affected = await self.db_manager.execute_query(
            query, params, return_rowcount=True
        )
        return rows_affected > 0
    
    async def delete_stream(self, stream_id: Union[str, UUID]) -> bool:
        """Delete a video stream"""
        query = "DELETE FROM video_stream WHERE stream_id = $1"
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (stream_id_obj,), return_rowcount=True
        )
        return rows_affected > 0
    
    async def get_user_stream_count(self, user_id: Union[str, UUID]) -> int:
        """Get count of streams owned by user"""
        query = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        result = await self.db_manager.execute_query(
            query, (user_id_obj,), fetch_one=True
        )
        return result['count'] if result else 0
    
    async def create_param_stream(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        parameters: Dict[str, Any]
    ) -> UUID:
        """Create a parameter stream entry"""
        import json
        
        param_id = uuid4()
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        
        query = """
            INSERT INTO param_stream
            (param_id, workspace_id, user_id, parameters, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        
        await self.db_manager.execute_query(
            query,
            (param_id, workspace_id_obj, user_id_obj, json.dumps(parameters), now, now)
        )
        
        return param_id
    
    async def get_param_stream(
        self,
        workspace_id: Union[str, UUID]
    ) -> Optional[Dict]:
        """Get parameter stream for workspace"""
        query = """
            SELECT param_id, workspace_id, user_id, parameters, created_at, updated_at
            FROM param_stream
            WHERE workspace_id = $1
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        result = await self.db_manager.execute_query(
            query, (workspace_id_obj,), fetch_one=True
        )
        return dict(result) if result else None
    
    async def update_param_stream(
        self,
        workspace_id: Union[str, UUID],
        parameters: Dict[str, Any]
    ) -> bool:
        """Update parameter stream"""
        import json
        
        query = """
            UPDATE param_stream
            SET parameters = $1, updated_at = $2
            WHERE workspace_id = $3
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (json.dumps(parameters), datetime.now(ZoneInfo("Africa/Cairo")), workspace_id_obj),
            return_rowcount=True
        )
        return rows_affected > 0
                
camera_service = CameraService()