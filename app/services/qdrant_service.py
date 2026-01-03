# app/services/qdrant_service.py
from typing import List, Dict, Any, Optional, Union, Tuple, Set
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta, time as dt_time
from collections import defaultdict
import logging
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from fastapi import HTTPException, status

from app.services.workspace_service import workspace_service 
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.utils import (
    parse_camera_ids, make_prediction, parse_date_format, parse_time_string,
    get_workspace_qdrant_collection_name, ensure_workspace_qdrant_collection_exists,
    parse_string_or_list, frame_to_base64
)

from app.schemas import (
    SearchQuery, TimestampRangeResponse,
    LocationSearchQuery, DeleteDataRequest,
    StreamUpdate
)
from app.config.settings import config

logger = logging.getLogger(__name__)

class QdrantService:
    """Service layer for all Qdrant operations"""
    
    def __init__(self):
        self.workspace_service = workspace_service
        self.db_manager = db_manager
        self.user_manager = user_manager
        self.BASE_COLLECTION_NAME = config.qdrant_collection_name
        self._workspace_collection_init_cache: Dict[str, bool] = {}
        self.fire_state_cleanup_interval = config.fire_state_cleanup_interval_seconds
        self.qdrant_client = self._initialize_client()  # Direct initialization
        
    def _initialize_client(self) -> QdrantClient:
        """Initialize Qdrant client"""
        client = QdrantClient(
            url=config.qdrant_url,
            port=config.qdrant_port_http,
            timeout=config.qdrant_query_timeout
        )
        logger.info("Qdrant client initialized.")
        return client

    def get_client(self) -> QdrantClient:
        """Get Qdrant client"""
        if self.qdrant_client is None:
            self.qdrant_client = self._initialize_client()
        return self.qdrant_client
    
    # ========== Detection Data Operations ==========
    
    async def insert_detection_data(
        self, 
        username: str, 
        camera_id_str: str, 
        camera_name: str,
        count: int, 
        male_count: int, 
        female_count: int, 
        fire_status: str, 
        frame: np.ndarray, 
        workspace_id: UUID,
        location_info: Optional[Dict[str, Any]] = None,
        result_id: Optional[UUID] = None
    ) -> bool: #Tuple[bool, Optional[str]]:
        """
        Insert detection data with location information into Qdrant.
        Moved from stream_service.py
        """
        # if count == 0:
        #     return False

        # OR change it to only check if frame is None:
        if frame is None:
            return False

        try:
            client = self.get_client()
            target_collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            point_id_str = str(result_id) if result_id else str(uuid4())
            
            # Base payload
            payload = {
                "result_id": point_id_str,
                "camera_id": camera_id_str,
                "name": camera_name,
                "timestamp": now_utc.timestamp(),
                "date": now_utc.strftime("%Y-%m-%d"),
                "time": now_utc.strftime("%H:%M:%S.%f")[:-3],
                "person_count": count,
                "male_count": male_count,
                "female_count": female_count,
                "fire_status": fire_status,
                "frame_base64": frame_to_base64(frame),
                "username": username,
            }
            
            # Add location information to payload
            if location_info:
                payload.update({
                    "location": location_info.get('location'),
                    "area": location_info.get('area'),
                    "building": location_info.get('building'),
                    "zone": location_info.get('zone'),
                    "floor_level": location_info.get('floor_level'),
                    "latitude": float(location_info['latitude']) if location_info.get('latitude') else None,
                    "longitude": float(location_info['longitude']) if location_info.get('longitude') else None,
                })
            
            vector_size = config.qdrant_vector_size
            point = qdrant_models.PointStruct(
                id=point_id_str, 
                vector=[0.0] * vector_size, 
                payload=payload
            )
            
            client.upsert(
                collection_name=target_collection_name, 
                points=[point], 
                wait=False
            )
            
            logger.debug(f"Inserted detection data with location info to {target_collection_name}: {point_id_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting detection data with location to Qdrant ({target_collection_name}): {e}", exc_info=True)
            return False

    async def batch_insert_detection_data(
        self,
        detection_batch: List[Dict[str, Any]],
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """
        Batch insert multiple detection data points.
        Useful for bulk operations and improved performance.
        """
        try:
            client = self.get_client()
            target_collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            points = []
            vector_size = config.qdrant_vector_size
            
            for detection in detection_batch:
                point_id_str = str(uuid4())
                
                # Process frame if provided
                frame_base64 = None
                if 'frame' in detection and detection['frame'] is not None:
                    frame_base64 = frame_to_base64(detection['frame'])
                
                # Build payload
                payload = {
                    "camera_id": detection['camera_id'],
                    "name": detection['camera_name'],
                    "timestamp": detection['timestamp'],
                    "date": detection['date'],
                    "time": detection['time'],
                    "person_count": detection['person_count'],
                    "male_count": detection.get('male_count', 0),
                    "female_count": detection.get('female_count', 0),
                    "fire_status": detection.get('fire_status', 'no detection'),
                    "frame_base64": frame_base64,
                    "username": detection['username'],
                }
                
                # Add location info if provided
                if 'location_info' in detection and detection['location_info']:
                    location_info = detection['location_info']
                    payload.update({
                        "location": location_info.get('location'),
                        "area": location_info.get('area'),
                        "building": location_info.get('building'),
                        "zone": location_info.get('zone'),
                        "floor_level": location_info.get('floor_level'),
                        "latitude": float(location_info['latitude']) if location_info.get('latitude') else None,
                        "longitude": float(location_info['longitude']) if location_info.get('longitude') else None,
                    })
                
                point = qdrant_models.PointStruct(
                    id=point_id_str,
                    vector=[0.0] * vector_size,
                    payload=payload
                )
                points.append(point)
            
            # Batch upsert
            if points:
                operation_info = client.upsert(
                    collection_name=target_collection_name,
                    points=points,
                    wait=True
                )
                
                logger.info(f"Batch inserted {len(points)} detection points to {target_collection_name}")
                
                return {
                    "success": True,
                    "inserted_count": len(points),
                    "collection": target_collection_name,
                    "operation_id": getattr(operation_info, 'operation_id', None)
                }
            
            return {
                "success": False,
                "message": "No points to insert",
                "inserted_count": 0
            }
            
        except Exception as e:
            logger.error(f"Error batch inserting detection data: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "inserted_count": 0
            }

    async def ensure_workspace_collection(self, workspace_id: UUID) -> bool:
        """
        Ensure Qdrant collection exists for workspace.
        Moved from stream_service.py
        """
        try:
            client = self.get_client()
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            return True
        except Exception as e:
            logger.error(f"Failed to ensure Qdrant collection for workspace {workspace_id}: {e}", exc_info=True)
            return False

    # ========== Collection Statistics ==========
    
    async def get_workspace_statistics(
        self,
        workspace_id: UUID,
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a workspace collection"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Get collection info
            collection_info = client.get_collection(collection_name=collection_name)
            
            # Build filter for user access
            filter_obj = None
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                must_conditions = []
                if requesting_username:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="username",
                            match=qdrant_models.MatchValue(value=requesting_username)
                        )
                    )
                if must_conditions:
                    filter_obj = qdrant_models.Filter(must=must_conditions)
            
            # Get count with filter
            count_result = client.count(
                collection_name=collection_name,
                count_filter=filter_obj
            )
            
            # Get unique cameras
            cameras = await self.get_camera_ids_for_workspace(workspace_id)
            
            # Get timestamp range
            timestamp_range = await self.get_timestamp_range(
                workspace_id=workspace_id,
                camera_ids=None,
                user_system_role=user_system_role,
                user_workspace_role=user_workspace_role,
                requesting_username=requesting_username
            )
            
            return {
                "workspace_id": str(workspace_id),
                "collection_name": collection_name,
                "statistics": {
                    "total_points": count_result.count,
                    "collection_status": str(collection_info.status),
                    "vectors_count": collection_info.vectors_count,
                    "indexed_vectors_count": getattr(collection_info, 'indexed_vectors_count', None),
                    "points_count": collection_info.points_count,
                    "segments_count": collection_info.segments_count,
                    "config": collection_info.config.model_dump(exclude_none=True),
                    "unique_cameras": len(cameras),
                    "camera_list": cameras,
                    "time_range": {
                        "first_date": timestamp_range.first_date,
                        "last_date": timestamp_range.last_date,
                        "first_datetime": timestamp_range.first_datetime,
                        "last_datetime": timestamp_range.last_datetime
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting workspace statistics: {e}", exc_info=True)
            return {
                "workspace_id": str(workspace_id),
                "error": str(e),
                "statistics": {}
            }

    # ========== Data Export Operations ==========
    
    async def export_workspace_data(
        self,
        workspace_id: UUID,
        search_query: Optional[SearchQuery],
        format: str,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        include_frame: bool
    ) -> Tuple[bytes, str, str]:
        """
        Export workspace data in various formats.
        Returns (data_bytes, content_type, filename)
        """
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Build filter
            filter_obj = None
            if search_query:
                filter_obj = self.build_filter_from_query(
                    search_query, user_system_role, user_workspace_role, requesting_username
                )
            
            # Fetch all matching points
            all_points = self._fetch_all_points(client, collection_name, filter_obj)
            
            # Format data based on requested format
            if format.lower() == 'csv':
                return self._export_as_csv(all_points, workspace_id)
            elif format.lower() == 'json':
                return self._export_as_json(all_points, workspace_id)
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Error exporting workspace data: {e}", exc_info=True)
            raise

    def _export_as_csv(self, points: List, workspace_id: UUID) -> Tuple[bytes, str, str]:
        """Export points as CSV"""
        import csv
        import io
        
        output = io.StringIO()
        
        if not points:
            return b"No data available", "text/csv", f"workspace_{workspace_id}_empty.csv"
        
        # Define CSV columns
        fieldnames = [
            'timestamp', 'date', 'time', 'camera_id', 'camera_name',
            'person_count', 'male_count', 'female_count', 'fire_status',
            'location', 'area', 'building', 'floor_level', 'zone',
            'latitude', 'longitude', 'username'
        ]
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for point in points:
            if point.payload:
                row = {
                    'timestamp': point.payload.get('timestamp'),
                    'date': point.payload.get('date'),
                    'time': point.payload.get('time'),
                    'camera_id': point.payload.get('camera_id'),
                    'camera_name': point.payload.get('name'),
                    'person_count': point.payload.get('person_count'),
                    'male_count': point.payload.get('male_count'),
                    'female_count': point.payload.get('female_count'),
                    'fire_status': point.payload.get('fire_status'),
                    'location': point.payload.get('location'),
                    'area': point.payload.get('area'),
                    'building': point.payload.get('building'),
                    'floor_level': point.payload.get('floor_level'),
                    'zone': point.payload.get('zone'),
                    'latitude': point.payload.get('latitude'),
                    'longitude': point.payload.get('longitude'),
                    'username': point.payload.get('username')
                }
                writer.writerow(row)
        
        csv_data = output.getvalue().encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return csv_data, "text/csv", filename

    def _export_as_json(self, points: List, workspace_id: UUID) -> Tuple[bytes, str, str]:
        """Export points as JSON"""
        import json
        
        data = []
        for point in points:
            if point.payload:
                # Exclude frame_base64 from export
                payload_copy = dict(point.payload)
                payload_copy.pop('frame_base64', None)
                data.append(payload_copy)
        
        json_data = json.dumps(data, indent=2, default=str).encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return json_data, "application/json", filename

    # ========== Stream Camera Data Retrieval ==========

    async def get_stream_camera_info(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Get camera information including location data for a stream"""
        try:
            query = """
                SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status,
                    vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                    vs.latitude, vs.longitude, vs.workspace_id,
                    vs.user_id, u.username as owner_username
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE vs.stream_id = $1
            """
            result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
            
            if result:
                return {
                    'stream_id': str(result['stream_id']),
                    'name': result['name'],
                    'path': result['path'],
                    'type': result['type'],
                    'status': result['status'],
                    'workspace_id': str(result['workspace_id']),
                    'user_id': str(result['user_id']),
                    'owner_username': result['owner_username'],
                    'location_info': {
                        'location': result['location'],
                        'area': result['area'],
                        'building': result['building'],
                        'zone': result['zone'],
                        'floor_level': result['floor_level'],
                        'latitude': float(result['latitude']) if result['latitude'] else None,
                        'longitude': float(result['longitude']) if result['longitude'] else None
                    }
                }
            return None
        except Exception as e:
            logger.error(f"Error getting stream camera info for {stream_id}: {e}")
            return None

    async def ensure_fire_detection_state_table(self):
        """Ensure fire_detection_state table exists"""
        try:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS fire_detection_state (
                    stream_id UUID PRIMARY KEY REFERENCES video_stream(stream_id) ON DELETE CASCADE,
                    fire_status VARCHAR(50) NOT NULL DEFAULT 'no detection',
                    last_detection_time TIMESTAMPTZ,
                    last_notification_time TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_fire_state_stream ON fire_detection_state(stream_id);
                CREATE INDEX IF NOT EXISTS idx_fire_state_notification_time ON fire_detection_state(last_notification_time);
            """
            await self.db_manager.execute_query(create_table_query)
            logger.info("Fire detection state table ensured")
        except Exception as e:
            logger.error(f"Error ensuring fire_detection_state table: {e}")

    # ========== User and Workspace Management ==========
    
    async def get_user_workspace_info(
        self, username: str
    ) -> Tuple[Optional[Dict], Optional[UUID]]:
        """Get user details and active workspace"""
        user_details = await self.user_manager.get_user_by_username(username)
        if not user_details:
            logger.debug(f"User '{username}' not found.")
            return None, None
        
        user_id_obj = user_details.get("user_id")
        if not user_id_obj:
            logger.error(f"User details for '{username}' missing user_id.")
            return user_details, None
        
        active_workspace_info = await self.workspace_service.get_active_workspace(user_id_obj)
        
        if active_workspace_info and isinstance(active_workspace_info.get("workspace_id"), UUID):
            return user_details, active_workspace_info["workspace_id"]
        
        logger.debug(f"No active workspace found for user '{username}'")
        return user_details, None
    
    async def check_workspace_membership(
        self, user_id: UUID, workspace_id: UUID, required_role: Optional[str] = None
    ) -> Dict[str, str]:
        """Check workspace membership and get role"""
        query = "SELECT role FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
        member_info = await self.db_manager.execute_query(
            query, (user_id, workspace_id), fetch_one=True
        )
        
        if not member_info:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: You are not a member of this workspace."
            )
        
        user_workspace_role = member_info['role']
        
        if required_role:
            has_permission = False
            if required_role == user_workspace_role:
                has_permission = True
            elif required_role == 'member' and user_workspace_role in ['admin', 'owner']:
                has_permission = True
            elif required_role == 'admin' and user_workspace_role == 'owner':
                has_permission = True
            
            if not has_permission:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Required role: '{required_role}'. Your role: '{user_workspace_role}'."
                )
        
        return {"role": user_workspace_role}
    
    # ========== Query Building ==========
    
    def build_filter_from_query(
        self,
        query: Union[SearchQuery, LocationSearchQuery],
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None,
    ) -> Optional[qdrant_models.Filter]:
        """Build Qdrant filter from search query"""
        must_conditions = []
        
        # Camera ID filter
        self._add_camera_filter(query, must_conditions)
        # Location filters
        self._add_location_filters(query, must_conditions)
        
        # Timestamp filters
        self._add_timestamp_filters(query, must_conditions)
        
        # User access control
        self._apply_user_access_filter(
            must_conditions, user_system_role, user_workspace_role, requesting_username
        )
        
        return qdrant_models.Filter(must=must_conditions) if must_conditions else None
    
    def _add_timestamp_filters(self, query: SearchQuery, must_conditions: list):
        """Add timestamp range filters to conditions"""
        start_timestamp = None
        end_timestamp = None
        
        try:
            if hasattr(query, 'start_date') and query.start_date:
                start_date_obj = parse_date_format(query.start_date)
                start_time_obj = parse_time_string(getattr(query, 'start_time', None), dt_time.min)
                start_datetime = datetime.combine(start_date_obj, start_time_obj).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                start_timestamp = start_datetime.timestamp()
            
            if hasattr(query, 'end_date') and query.end_date:
                end_date_obj = parse_date_format(query.end_date)
                end_time_obj = parse_time_string(
                    getattr(query, 'end_time', None), 
                    dt_time.max.replace(microsecond=0)
                )
                end_datetime = datetime.combine(end_date_obj, end_time_obj).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                end_timestamp = end_datetime.timestamp()
            
            if start_timestamp is not None or end_timestamp is not None:
                timestamp_range = {}
                if start_timestamp is not None: 
                    timestamp_range["gte"] = start_timestamp
                if end_timestamp is not None: 
                    timestamp_range["lte"] = end_timestamp
                if timestamp_range:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="timestamp", 
                            range=qdrant_models.Range(**timestamp_range)
                        )
                    )
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            raise ValueError(f"Invalid date/time format: {e}")
    
    def _apply_user_access_filter(
        self, must_conditions: list, user_system_role: str, 
        user_workspace_role: str, requesting_username: str
    ):
        """Apply user access control to filter"""
        apply_username_filter = True
        
        if user_system_role == 'admin':
            apply_username_filter = False
        elif user_workspace_role in ['admin', 'owner']:
            apply_username_filter = False
        
        if apply_username_filter:
            if requesting_username:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="username", 
                        match=qdrant_models.MatchValue(value=requesting_username)
                    )
                )
            else:
                # Safeguard: apply impossible filter
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="username",
                        match=qdrant_models.MatchValue(value=f"__IMPOSSIBLE_USERNAME_{uuid4()}__")
                    )
                )
    
    def _add_camera_filter(self, query, must_conditions):
        """Handle camera ID filtering logic."""
        # Camera ID filter
        if hasattr(query, 'camera_id') and query.camera_id:
            str_camera_ids = [str(cid) for cid in query.camera_id if cid]
            if str_camera_ids:
                if len(str_camera_ids) == 1:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="camera_id", 
                            match=qdrant_models.MatchValue(value=str_camera_ids[0])
                        )
                    )
                else:
                    must_conditions.append(
                        qdrant_models.Filter(should=[
                            qdrant_models.FieldCondition(
                                key="camera_id", 
                                match=qdrant_models.MatchValue(value=cam_id)
                            ) for cam_id in str_camera_ids
                        ])
                    )

    def _add_location_filters(self, query, must_conditions):
        """Handle location field filtering."""
        # Location filters
        location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
        for field in location_fields:
            if hasattr(query, field):
                field_value = getattr(query, field)
                if field_value:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key=field,
                            match=qdrant_models.MatchValue(value=field_value)
                        )
                    )
        
    # ========== Search Operations ==========
    
    async def search_workspace_data(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        page: int = 1,
        per_page: Optional[int] = 10,
        base64: bool = True
    ) -> Dict[str, Any]:
        """Search workspace data in Qdrant"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        filter_obj = self.build_filter_from_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        # Get count
        count_result = client.count(collection_name=collection_name, count_filter=filter_obj)
        total_count = count_result.count
        
        paginated_data = []
        num_of_pages = 0
        
        if total_count > 0:
            if per_page is None:
                # Return all results
                num_of_pages = 1
                points = self._fetch_all_points(client, collection_name, filter_obj)
            else:
                # Paginated results
                num_of_pages = (total_count + per_page - 1) // per_page
                if page <= num_of_pages:
                    offset = (page - 1) * per_page
                    points, _ = client.scroll(
                        collection_name=collection_name,
                        scroll_filter=filter_obj,
                        limit=per_page,
                        offset=offset,
                        with_payload=True,
                        with_vectors=False
                    )
                else:
                    points = []
            
            # Process points
            for point in points:
                if point.payload:
                    paginated_data.append(self._format_point_data(point, base64))
        
        return {
            "data": paginated_data,
            "current_page": page if per_page is not None else 1,
            "num_of_pages": num_of_pages,
            "total_count": total_count,
            "per_page": per_page
        }
    
    async def search_ordered_data(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        page: int = 1,
        per_page: int = 10
    ) -> Dict[str, Any]:
        """Search with timestamp ordering"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        filter_obj = self.build_filter_from_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        count_result = client.count(collection_name=collection_name, count_filter=filter_obj)
        total_count = count_result.count
        
        paginated_data = []
        num_of_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        
        if total_count > 0 and page <= num_of_pages:
            # Fetch all up to current page
            fetch_limit = page * per_page
            
            points_batch, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=fetch_limit,
                offset=None,  # Must be None with order_by
                with_payload=True,
                with_vectors=False,
                order_by=qdrant_models.OrderBy(
                    key="timestamp", 
                    direction=qdrant_models.Direction.DESC
                )
            )
            
            # Extract current page
            start_idx = (page - 1) * per_page
            points_for_page = points_batch[start_idx:start_idx + per_page]
            
            for point in points_for_page:
                if point.payload:
                    paginated_data.append(self._format_point_data(point))
        
        return {
            "data": paginated_data,
            "current_page": page,
            "num_of_pages": num_of_pages,
            "total_count": total_count,
            "per_page": per_page
        }
    
    # ========== Prediction Operations ==========
    
    async def get_prediction_data(
        self,
        workspace_id: UUID,
        camera_ids: List[Tuple[str, str]],
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> List[Dict[str, Any]]:
        """Get prediction data for cameras"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        predictions = []
        
        for cam_id, cam_name in camera_ids:
            try:
                cam_query = SearchQuery(
                    camera_id=[cam_id],
                    start_date=search_query.start_date,
                    end_date=search_query.end_date,
                    start_time=search_query.start_time,
                    end_time=search_query.end_time
                )
                
                filter_obj = self.build_filter_from_query(
                    cam_query, user_system_role, user_workspace_role, requesting_username
                )
                
                points, _ = client.scroll(
                    collection_name=collection_name,
                    scroll_filter=filter_obj,
                    limit=config.prediction_data_points_limit,
                    with_payload=["timestamp", "person_count", "male_count", "female_count", "fire_status"],
                    with_vectors=False,
                    order_by=qdrant_models.OrderBy(
                        key="timestamp",
                        direction=qdrant_models.Direction.DESC
                    )
                )
                
                if points:
                    prediction_data = self._prepare_prediction_data(points)
                    prediction = make_prediction(cam_id, prediction_data)
                    predictions.append({
                        "camera_id": cam_id,
                        "name": cam_name,
                        "prediction": prediction
                    })
            except Exception as e:
                logger.error(f"Error processing prediction for camera {cam_id}: {e}")
                predictions.append({
                    "camera_id": cam_id,
                    "name": cam_name,
                    "prediction": None,
                    "error": str(e)
                })
        
        return predictions
    
    # ========== Timestamp Operations ==========
    
    async def get_timestamp_range(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: Optional[str],
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> TimestampRangeResponse:
        """Get timestamp range for data"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        filter_obj = self.build_filter_from_query(
            SearchQuery(camera_id=camera_ids),
            user_system_role,
            user_workspace_role,
            requesting_username
        )
        
        min_ts, max_ts = None, None
        
        try:
            # Get earliest timestamp
            points_asc, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=1,
                offset=None,
                with_payload=["timestamp"],
                order_by=qdrant_models.OrderBy(
                    key="timestamp",
                    direction=qdrant_models.Direction.ASC
                )
            )
            if points_asc and points_asc[0].payload:
                min_ts = float(points_asc[0].payload.get("timestamp", 0))
            
            # Get latest timestamp
            points_desc, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=1,
                offset=None,
                with_payload=["timestamp"],
                order_by=qdrant_models.OrderBy(
                    key="timestamp",
                    direction=qdrant_models.Direction.DESC
                )
            )
            if points_desc and points_desc[0].payload:
                max_ts = float(points_desc[0].payload.get("timestamp", 0))
        except Exception as e:
            logger.warning(f"Error getting timestamp range: {e}")
        
        response = TimestampRangeResponse()
        if min_ts and max_ts:
            response.first_timestamp = min_ts
            response.last_timestamp = max_ts
            
            first_dt = datetime.fromtimestamp(min_ts, tz=ZoneInfo("Africa/Cairo"))
            last_dt = datetime.fromtimestamp(max_ts, tz=ZoneInfo("Africa/Cairo"))
            
            response.first_datetime = first_dt.isoformat()
            response.last_datetime = last_dt.isoformat()
            response.first_date = first_dt.date().isoformat()
            response.last_date = last_dt.date().isoformat()
            response.first_time = first_dt.time().isoformat(timespec='seconds')
            response.last_time = last_dt.time().isoformat(timespec='seconds')
        
        if camera_ids:
            response.camera_id = ", ".join(camera_ids) if len(camera_ids) > 1 else camera_ids[0]
        
        return response
    
    # ========== Camera Operations ==========
    
    async def get_camera_ids_for_workspace(self, workspace_id: UUID) -> List[Dict[str, str]]:
        """Get all camera IDs for a workspace"""
        query = """
            SELECT DISTINCT stream_id, name 
            FROM video_stream 
            WHERE workspace_id = $1 
            ORDER BY name
        """
        camera_records = await self.db_manager.execute_query(
            query, (workspace_id,), fetch_all=True
        )
        
        if camera_records:
            return [
                {"id": str(rec["stream_id"]), "name": rec["name"]} 
                for rec in camera_records
            ]
        return []
    
    async def search_cameras_by_location(
        self,
        workspace_id: UUID,
        location_filters: Dict[str, Optional[str]],
        status: Optional[str],
        search_term: Optional[str],
        group_by: str,
        include_inactive: bool,
        user_id: UUID,
        user_role: str
    ) -> Dict[str, Any]:
        """Search cameras with location-based filtering and grouping"""
        conditions = ["vs.workspace_id = $1"]
        params = [workspace_id]
        param_count = 1
        
        # Add location filters
        for field, value in location_filters.items():
            if value:
                param_count += 1
                conditions.append(f"vs.{field} ILIKE ${param_count}")
                params.append(f"%{value}%")
        
        # Status filter
        if status:
            param_count += 1
            conditions.append(f"vs.status = ${param_count}")
            params.append(status)
        elif not include_inactive:
            conditions.append("vs.status != 'inactive'")
        
        # Search term
        if search_term:
            param_count += 1
            conditions.append(f"vs.name ILIKE ${param_count}")
            params.append(f"%{search_term}%")
        
        # Permission filter
        if user_role != "admin":
            param_count += 1
            conditions.append(f"vs.user_id = ${param_count}")
            params.append(user_id)
        
        where_clause = " AND ".join(conditions)
        
        if group_by == "none":
            # Return flat list
            query = f"""
                SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                       vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                       vs.latitude, vs.longitude, vs.created_at, vs.updated_at,
                       u.username as owner_username
                FROM video_stream vs
                JOIN users u ON vs.user_id = u.user_id
                WHERE {where_clause}
                ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name
            """
            
            cameras = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
            camera_list = [self._format_camera_data(cam) for cam in cameras] if cameras else []
            
            return {
                "cameras": camera_list,
                "groups": [],
                "total_count": len(camera_list),
                "group_type": "none"
            }
        else:
            # Return grouped results
            return await self._get_grouped_cameras(where_clause, params, group_by)
    
    # ========== Collection Management ==========
    
    async def create_collection(
        self, collection_name: str, vector_size: int, distance: str
    ) -> Dict[str, str]:
        """Create a new Qdrant collection"""
        client = self.get_client()
        
        # Check if exists
        try:
            client.get_collection(collection_name=collection_name)
            return {"status": "info", "message": f"Collection '{collection_name}' already exists."}
        except Exception:
            pass
        
        # Create collection
        distance_map = {
            "DOT": qdrant_models.Distance.DOT,
            "COSINE": qdrant_models.Distance.COSINE,
            "EUCLID": qdrant_models.Distance.EUCLID
        }
        
        qdrant_distance = distance_map.get(distance.upper())
        if not qdrant_distance:
            raise ValueError("Invalid distance metric")
        
        client.create_collection(
            collection_name=collection_name,
            vectors_config=qdrant_models.VectorParams(
                size=vector_size,
                distance=qdrant_distance
            )
        )
        
        return {"status": "success", "message": f"Collection '{collection_name}' created."}
    
    async def delete_collection(self, collection_name: str) -> Dict[str, str]:
        """Delete a Qdrant collection"""
        client = self.get_client()
        
        try:
            client.get_collection(collection_name=collection_name)
            client.delete_collection(collection_name=collection_name)
            return {"status": "success", "message": f"Collection '{collection_name}' deleted."}
        except Exception as e:
            if "not found" in str(e).lower():
                raise ValueError(f"Collection '{collection_name}' not found.")
            raise
    
    async def list_collections(self) -> Dict[str, List]:
        """List all Qdrant collections"""
        client = self.get_client()
        collections_response = client.get_collections()
        
        collection_info_list = []
        for c in collections_response.collections:
            try:
                details = client.get_collection(collection_name=c.name)
                ws_id_from_name = None
                
                if c.name.startswith(self.BASE_COLLECTION_NAME + "_ws_"):
                    try:
                        ws_id_str = c.name.split("_ws_")[1].replace("_", "-")
                        ws_id_from_name = str(UUID(ws_id_str))
                    except (IndexError, ValueError):
                        logger.warning(f"Could not parse workspace ID from collection: {c.name}")
                
                collection_info_list.append({
                    "name": c.name,
                    "status": str(details.status),
                    "optimizer_status": str(details.optimizer_status),
                    "points_count": details.points_count,
                    "segments_count": details.segments_count,
                    "config": details.config.model_dump(exclude_none=True),
                    "payload_schema": {
                        k: str(v.data_type) for k, v in details.payload_schema.items()
                    } if details.payload_schema else {},
                    "associated_workspace_id": ws_id_from_name
                })
            except Exception as e:
                logger.warning(f"Could not get details for collection {c.name}: {e}")
                collection_info_list.append({
                    "name": c.name,
                    "status": "details_error",
                    "error": str(e)
                })
        
        return {"collections": collection_info_list}
    
    async def get_collection_count(
        self,
        collection_name: str,
        search_query: Optional[SearchQuery],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get count of points in collection"""
        client = self.get_client()
        
        filter_obj = None
        if search_query:
            filter_obj = self.build_filter_from_query(
                search_query, user_system_role, user_workspace_role, requesting_username
            )
        
        count_result = client.count(collection_name=collection_name, count_filter=filter_obj)
        
        return {
            "collection_name": collection_name,
            "count": count_result.count,
            "filters_applied": search_query.model_dump(exclude_none=True) if search_query else {}
        }
    
    # ========== Data Management ==========
    
    async def delete_data(
        self,
        workspace_id: UUID,
        delete_request: DeleteDataRequest,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Delete data from workspace collection"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        search_query = SearchQuery(
            camera_id=parse_camera_ids(delete_request.camera_id) if delete_request.camera_id else None,
            start_date=delete_request.start_date,
            end_date=delete_request.end_date,
            start_time=delete_request.start_time,
            end_time=delete_request.end_time
        )
        
        filter_obj = self.build_filter_from_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        if not filter_obj:
            raise ValueError("No filter criteria provided for deletion")
        
        # Get count before deletion
        count_before = client.count(collection_name=collection_name, count_filter=filter_obj).count
        
        if count_before == 0:
            return {
                "status": "info",
                "message": "No data found matching criteria",
                "deleted_count": 0
            }
        
        # Delete data
        delete_result = client.delete(
            collection_name=collection_name,
            points_selector=qdrant_models.FilterSelector(filter=filter_obj)
        )
        
        if delete_result.status != qdrant_models.UpdateStatus.COMPLETED:
            raise RuntimeError(f"Delete operation failed: {delete_result.status}")
        
        return {
            "status": "success",
            "message": f"Successfully deleted {count_before} data points",
            "deleted_count": count_before
        }
    
    async def delete_from_qdrant(
        self, 
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> List[str]:
        """Delete camera data from Qdrant collections."""
        failures = []
        
        try:
            for workspace_id, camera_ids in workspace_camera_mapping.items():
                try:
                    collection_name = get_workspace_qdrant_collection_name(workspace_id)
                    
                    for camera_id in camera_ids:
                        try:
                            delete_result = self.qdrant_service.qdrant_client.delete(
                                collection_name=collection_name,
                                points_selector=qdrant_models.Filter(
                                    must=[
                                        qdrant_models.FieldCondition(
                                            key="camera_id",
                                            match=qdrant_models.MatchValue(value=camera_id)
                                        )
                                    ]
                                ),
                                wait=True
                            )
                            
                            if hasattr(delete_result, 'status') and delete_result.status == qdrant_models.UpdateStatus.COMPLETED:
                                logger.info(f"Deleted Qdrant points for camera {camera_id}")
                            else:
                                logger.warning(f"Qdrant deletion unclear for camera {camera_id}")
                                
                        except Exception as e_camera:
                            logger.error(f"Failed to delete Qdrant points for camera {camera_id}: {e_camera}", exc_info=True)
                            failures.append(camera_id)
                            
                except Exception as e_workspace:
                    logger.error(f"Failed to access Qdrant collection for workspace {workspace_id}: {e_workspace}", exc_info=True)
                    failures.extend(camera_ids)
                    
        except Exception as e_qdrant:
            logger.error(f"Failed to get Qdrant client: {e_qdrant}", exc_info=True)
            failures.extend([cam_id for cam_list in workspace_camera_mapping.values() for cam_id in cam_list])
        
        return list(set(failures))

    async def delete_user_camera_data(
        self, 
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> Dict:
        """
        Delete all user's camera data from Qdrant collections.
        
        Args:
            workspace_camera_mapping: Dict mapping workspace_id to list of camera_ids
            
        Returns:
            Dictionary with deletion results
        """
        result = {
            "success": True,
            "deleted_cameras": [],
            "failed_cameras": [],
            "collections_affected": []
        }
        
        if not workspace_camera_mapping:
            logger.info("No camera data to delete from Qdrant")
            return result
        
        try:
            client = self.get_client()
            
            for workspace_id_str, camera_ids in workspace_camera_mapping.items():
                try:
                    collection_name = get_workspace_qdrant_collection_name(workspace_id_str)
                    result["collections_affected"].append(collection_name)
                    
                    # Check if collection exists
                    try:
                        client.get_collection(collection_name=collection_name)
                    except Exception:
                        logger.info(f"Collection {collection_name} not found, skipping")
                        continue
                    
                    # Delete data for each camera
                    for camera_id in camera_ids:
                        try:
                            delete_result = client.delete(
                                collection_name=collection_name,
                                points_selector=qdrant_models.FilterSelector(
                                    filter=qdrant_models.Filter(
                                        must=[
                                            qdrant_models.FieldCondition(
                                                key="camera_id",
                                                match=qdrant_models.MatchValue(value=camera_id)
                                            )
                                        ]
                                    )
                                ),
                                wait=True
                            )
                            
                            if hasattr(delete_result, 'status') and \
                            delete_result.status == qdrant_models.UpdateStatus.COMPLETED:
                                result["deleted_cameras"].append(camera_id)
                                logger.info(f"Deleted Qdrant data for camera {camera_id}")
                            else:
                                logger.warning(f"Unclear Qdrant deletion status for camera {camera_id}")
                                result["failed_cameras"].append(camera_id)
                        
                        except Exception as camera_err:
                            logger.error(f"Failed to delete Qdrant data for camera {camera_id}: {camera_err}")
                            result["failed_cameras"].append(camera_id)
                
                except Exception as ws_err:
                    logger.error(f"Failed to process workspace {workspace_id_str} in Qdrant: {ws_err}")
                    result["failed_cameras"].extend(camera_ids)
            
            # Mark as failed if any cameras failed
            if result["failed_cameras"]:
                result["success"] = False
                logger.warning(f"Some cameras failed Qdrant deletion: {result['failed_cameras']}")
        
        except Exception as e:
            logger.error(f"Error during Qdrant deletion: {e}", exc_info=True)
            result["success"] = False
            result["error"] = str(e)
        
        return result

    async def delete_camera_data_from_workspaces(
        self, 
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> List[str]:
        """Delete camera data from Qdrant collections."""
        failures = []
        client = self.get_client()
        
        for workspace_id, camera_ids in workspace_camera_mapping.items():
            try:
                collection_name = get_workspace_qdrant_collection_name(workspace_id)
                
                # Check if collection exists
                try:
                    client.get_collection(collection_name=collection_name)
                except Exception:
                    logger.info(f"Collection {collection_name} not found, skipping")
                    continue
                
                for camera_id in camera_ids:
                    try:
                        delete_result = client.delete(
                            collection_name=collection_name,
                            points_selector=qdrant_models.FilterSelector(
                                filter=qdrant_models.Filter(
                                    must=[
                                        qdrant_models.FieldCondition(
                                            key="camera_id",
                                            match=qdrant_models.MatchValue(value=camera_id)
                                        )
                                    ]
                                )
                            ),
                            wait=True
                        )
                        
                        if hasattr(delete_result, 'status') and \
                        delete_result.status == qdrant_models.UpdateStatus.COMPLETED:
                            logger.info(f"Deleted Qdrant points for camera {camera_id}")
                        else:
                            logger.warning(f"Qdrant deletion unclear for camera {camera_id}")
                            failures.append(camera_id)
                            
                    except Exception as e:
                        logger.error(f"Failed to delete Qdrant points for camera {camera_id}: {e}")
                        failures.append(camera_id)
                        
            except Exception as e:
                logger.error(f"Failed to access Qdrant collection for workspace {workspace_id}: {e}")
                failures.extend(camera_ids)
        
        return list(set(failures))

    async def update_camera_metadata(
        self,
        stream_update: StreamUpdate,
        stream_id: str,
        workspace_id: str
    ) -> None:
        """Update Qdrant metadata for a camera."""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        
        # Ensure collection exists
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        qdrant_payload = {}
        
        if stream_update.name is not None:
            qdrant_payload["name"] = stream_update.name
        
        location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']
        for field in location_fields:
            if hasattr(stream_update, field) and getattr(stream_update, field) is not None:
                value = getattr(stream_update, field)
                if field in ['latitude', 'longitude']:
                    value = float(value)
                qdrant_payload[field] = value
        
        if qdrant_payload:
            client.set_payload(
                collection_name=collection_name,
                payload=qdrant_payload,
                points=qdrant_models.Filter(
                    must=[
                        qdrant_models.FieldCondition(
                            key="camera_id",
                            match=qdrant_models.MatchValue(value=stream_id)
                        )
                    ]
                )
            )
            logger.info(f"Updated Qdrant metadata for camera {stream_id}")
            
    # ========== Location Data Operations ==========
    
    async def get_location_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        location_field: Optional[str] = None,
        filter_conditions: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> List[Dict[str, Any]]:
        """Extract location data from Qdrant collection"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        # Build filter
        must_conditions = []
        
        # User access control
        self._apply_user_access_filter(
            must_conditions, user_system_role, user_workspace_role, requesting_username
        )
        
        # Add location filters
        if filter_conditions:
            for field_name, field_values in filter_conditions.items():
                if field_values:
                    if isinstance(field_values, str):
                        field_values = [field_values]
                    
                    if len(field_values) == 1:
                        must_conditions.append(
                            qdrant_models.FieldCondition(
                                key=field_name,
                                match=qdrant_models.MatchValue(value=field_values[0])
                            )
                        )
                    else:
                        must_conditions.append(
                            qdrant_models.Filter(should=[
                                qdrant_models.FieldCondition(
                                    key=field_name,
                                    match=qdrant_models.MatchValue(value=val)
                                ) for val in field_values
                            ])
                        )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Fetch all points
        all_points = self._fetch_all_points_with_payload(
            client, collection_name, filter_obj,
            ["camera_id", "name", "location", "area", "building", "floor_level", "zone"]
        )
        
        # Process points to extract location data
        location_data = defaultdict(lambda: {
            'count': 0,
            'cameras': set(),
            'details': {}
        })
        
        for point in all_points:
            if not point.payload:
                continue
            
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            # Extract location info
            location_info = {
                'location': point.payload.get('location'),
                'area': point.payload.get('area'),
                'building': point.payload.get('building'),
                'floor_level': point.payload.get('floor_level'),
                'zone': point.payload.get('zone'),
                'camera_name': point.payload.get('name', 'Unknown Camera')
            }
            
            # Create grouping key
            if location_field:
                key_value = location_info.get(location_field)
                if key_value:
                    location_data[key_value]['cameras'].add(camera_id)
                    location_data[key_value]['details'] = location_info
            else:
                key = tuple(sorted([
                    (k, v) for k, v in location_info.items()
                    if k != 'camera_name' and v is not None
                ]))
                if key:
                    location_data[key]['cameras'].add(camera_id)
                    location_data[key]['details'] = location_info
        
        # Format results
        result = []
        for key, data in location_data.items():
            if location_field:
                result.append({
                    location_field: key,
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras']),
                    **{k: v for k, v in data['details'].items()
                       if k != location_field and k != 'camera_name' and v is not None}
                })
            else:
                details = data['details']
                result.append({
                    **details,
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
        
        return result

    async def get_unique_locations(
        self,
        workspace_id: UUID,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique locations with filtering options"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter with user access control
            must_conditions = []
            
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if area:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="area",
                        match=qdrant_models.MatchValue(value=area)
                    )
                )
            if building:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="building",
                        match=qdrant_models.MatchValue(value=building)
                    )
                )
            if floor_level:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="floor_level",
                        match=qdrant_models.MatchValue(value=floor_level)
                    )
                )
            if zone:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="zone",
                        match=qdrant_models.MatchValue(value=zone)
                    )
                )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Fetch all points
            all_points = self._fetch_all_points_with_payload(
                client, collection_name, filter_obj,
                ["camera_id", "location", "area", "building", "floor_level", "zone"]
            )
            
            # Group by location hierarchy
            location_groups = defaultdict(lambda: {'cameras': set()})
            
            for point in all_points:
                if not point.payload:
                    continue
                
                camera_id = point.payload.get('camera_id')
                if not camera_id:
                    continue
                
                location = point.payload.get('location')
                area_val = point.payload.get('area')
                building_val = point.payload.get('building')
                floor_level_val = point.payload.get('floor_level')
                zone_val = point.payload.get('zone')
                
                key = (location, area_val, building_val, floor_level_val, zone_val)
                location_groups[key]['cameras'].add(camera_id)
                location_groups[key]['details'] = {
                    'location': location,
                    'area': area_val,
                    'building': building_val,
                    'floor_level': floor_level_val,
                    'zone': zone_val
                }
            
            # Format results
            locations = []
            for key, data in location_groups.items():
                locations.append({
                    **data['details'],
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
            
            # Sort by location, area, building, floor_level, zone
            locations.sort(key=lambda x: (
                x.get('location') or '',
                x.get('area') or '',
                x.get('building') or '',
                x.get('floor_level') or '',
                x.get('zone') or ''
            ))
            
            return {
                "locations": locations,
                "total_count": len(locations),
                "filtered_by_area": area,
                "filtered_by_building": building,
                "filtered_by_floor_level": floor_level,
                "filtered_by_zone": zone,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique locations: {e}", exc_info=True)
            raise

    async def get_unique_areas(
        self,
        workspace_id: UUID,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        location: Optional[str] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique areas with filtering options"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter with user access control
            must_conditions = []
            
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if building:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="building",
                        match=qdrant_models.MatchValue(value=building)
                    )
                )
            if floor_level:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="floor_level",
                        match=qdrant_models.MatchValue(value=floor_level)
                    )
                )
            if zone:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="zone",
                        match=qdrant_models.MatchValue(value=zone)
                    )
                )
            if location:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="location",
                        match=qdrant_models.MatchValue(value=location)
                    )
                )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Fetch all points
            all_points = self._fetch_all_points_with_payload(
                client, collection_name, filter_obj,
                ["camera_id", "area", "building", "floor_level", "zone", "location"]
            )
            
            # Group by area hierarchy
            area_groups = defaultdict(lambda: {'cameras': set()})
            
            for point in all_points:
                if not point.payload:
                    continue
                
                camera_id = point.payload.get('camera_id')
                if not camera_id:
                    continue
                
                area_val = point.payload.get('area')
                building_val = point.payload.get('building')
                floor_level_val = point.payload.get('floor_level')
                zone_val = point.payload.get('zone')
                location_val = point.payload.get('location')
                
                key = (area_val, building_val, floor_level_val, zone_val, location_val)
                area_groups[key]['cameras'].add(camera_id)
                area_groups[key]['details'] = {
                    'area': area_val,
                    'building': building_val,
                    'floor_level': floor_level_val,
                    'zone': zone_val,
                    'location': location_val
                }
            
            # Format results
            areas = []
            for key, data in area_groups.items():
                areas.append({
                    **data['details'],
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
            
            # Sort by area, building, floor_level, zone, location
            areas.sort(key=lambda x: (
                x.get('area') or '',
                x.get('building') or '',
                x.get('floor_level') or '',
                x.get('zone') or '',
                x.get('location') or ''
            ))
            
            return {
                "areas": areas,
                "total_count": len(areas),
                "filtered_by_building": building,
                "filtered_by_floor_level": floor_level,
                "filtered_by_zone": zone,
                "filtered_by_location": location,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique areas: {e}", exc_info=True)
            raise

    async def get_unique_buildings(
        self,
        workspace_id: UUID,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        area: Optional[str] = None,
        location: Optional[str] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique buildings with filtering options"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter with user access control
            must_conditions = []
            
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if floor_level:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="floor_level",
                        match=qdrant_models.MatchValue(value=floor_level)
                    )
                )
            if zone:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="zone",
                        match=qdrant_models.MatchValue(value=zone)
                    )
                )
            if area:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="area",
                        match=qdrant_models.MatchValue(value=area)
                    )
                )
            if location:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="location",
                        match=qdrant_models.MatchValue(value=location)
                    )
                )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Fetch all points
            all_points = self._fetch_all_points_with_payload(
                client, collection_name, filter_obj,
                ["camera_id", "building", "floor_level", "zone", "area", "location"]
            )
            
            # Group by building hierarchy
            building_groups = defaultdict(lambda: {'cameras': set()})
            
            for point in all_points:
                if not point.payload:
                    continue
                
                camera_id = point.payload.get('camera_id')
                if not camera_id:
                    continue
                
                building_val = point.payload.get('building')
                floor_level_val = point.payload.get('floor_level')
                zone_val = point.payload.get('zone')
                area_val = point.payload.get('area')
                location_val = point.payload.get('location')
                
                key = (building_val, floor_level_val, zone_val, area_val, location_val)
                building_groups[key]['cameras'].add(camera_id)
                building_groups[key]['details'] = {
                    'building': building_val,
                    'floor_level': floor_level_val,
                    'zone': zone_val,
                    'area': area_val,
                    'location': location_val
                }
            
            # Format results
            buildings = []
            for key, data in building_groups.items():
                buildings.append({
                    **data['details'],
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
            
            # Sort by building, floor_level, zone, area, location
            buildings.sort(key=lambda x: (
                x.get('building') or '',
                x.get('floor_level') or '',
                x.get('zone') or '',
                x.get('area') or '',
                x.get('location') or ''
            ))
            
            return {
                "buildings": buildings,
                "total_count": len(buildings),
                "filtered_by_floor_level": floor_level,
                "filtered_by_zone": zone,
                "filtered_by_area": area,
                "filtered_by_location": location,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique buildings: {e}", exc_info=True)
            raise

    async def get_unique_floor_levels(
        self,
        workspace_id: UUID,
        location: Optional[str] = None,
        area: Optional[str] = None,
        building: Optional[str] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique floor levels with filtering options"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter with user access control
            must_conditions = []
            
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location filters
            if location:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="location",
                        match=qdrant_models.MatchValue(value=location)
                    )
                )
            if area:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="area",
                        match=qdrant_models.MatchValue(value=area)
                    )
                )
            if building:
                must_conditions.append(
                    qdrant_models.FieldCondition(
                        key="building",
                        match=qdrant_models.MatchValue(value=building)
                    )
                )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Fetch all points
            all_points = self._fetch_all_points_with_payload(
                client, collection_name, filter_obj,
                ["camera_id", "floor_level", "building", "area", "location"]
            )
            
            # Group by floor level hierarchy
            floor_level_groups = defaultdict(lambda: {'cameras': set()})
            
            for point in all_points:
                if not point.payload:
                    continue
                
                camera_id = point.payload.get('camera_id')
                if not camera_id:
                    continue
                
                floor_level_val = point.payload.get('floor_level')
                building_val = point.payload.get('building')
                area_val = point.payload.get('area')
                location_val = point.payload.get('location')
                
                key = (floor_level_val, building_val, area_val, location_val)
                floor_level_groups[key]['cameras'].add(camera_id)
                floor_level_groups[key]['details'] = {
                    'floor_level': floor_level_val,
                    'building': building_val,
                    'area': area_val,
                    'location': location_val
                }
            
            # Format results
            floor_levels = []
            for key, data in floor_level_groups.items():
                floor_levels.append({
                    **data['details'],
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
            
            # Sort by floor level, building, area, location
            floor_levels.sort(key=lambda x: (
                x.get('floor_level') or '',
                x.get('building') or '',
                x.get('area') or '',
                x.get('location') or ''
            ))
            
            return {
                "floor_levels": floor_levels,
                "total_count": len(floor_levels),
                "filtered_by_location": location,
                "filtered_by_area": area,
                "filtered_by_building": building,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique floor levels: {e}", exc_info=True)
            raise

    async def get_unique_zones(
        self,
        workspace_id: UUID,
        floor_levels: Optional[List[str]] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique zones with their associated location information"""
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter with user access control
            must_conditions = []
            
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add floor level filter
            if floor_levels:
                if len(floor_levels) == 1:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="floor_level",
                            match=qdrant_models.MatchValue(value=floor_levels[0])
                        )
                    )
                else:
                    must_conditions.append(
                        qdrant_models.Filter(should=[
                            qdrant_models.FieldCondition(
                                key="floor_level",
                                match=qdrant_models.MatchValue(value=fl)
                            ) for fl in floor_levels
                        ])
                    )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Fetch all points
            all_points = self._fetch_all_points_with_payload(
                client, collection_name, filter_obj,
                ["camera_id", "zone", "floor_level", "building", "area", "location"]
            )
            
            # Group by zone hierarchy
            zone_groups = defaultdict(lambda: {'cameras': set()})
            
            for point in all_points:
                if not point.payload:
                    continue
                
                camera_id = point.payload.get('camera_id')
                if not camera_id:
                    continue
                
                zone_val = point.payload.get('zone')
                floor_level_val = point.payload.get('floor_level')
                building_val = point.payload.get('building')
                area_val = point.payload.get('area')
                location_val = point.payload.get('location')
                
                key = (zone_val, floor_level_val, building_val, area_val, location_val)
                zone_groups[key]['cameras'].add(camera_id)
                zone_groups[key]['details'] = {
                    'zone': zone_val,
                    'floor_level': floor_level_val,
                    'building': building_val,
                    'area': area_val,
                    'location': location_val
                }
            
            # Format results
            zones = []
            for key, data in zone_groups.items():
                zones.append({
                    **data['details'],
                    'camera_count': len(data['cameras']),
                    'camera_ids': list(data['cameras'])
                })
            
            # Sort by zone, floor_level, building, area, location
            zones.sort(key=lambda x: (
                x.get('zone') or '',
                x.get('floor_level') or '',
                x.get('building') or '',
                x.get('area') or '',
                x.get('location') or ''
            ))
            
            return {
                "zones": zones,
                "total_count": len(zones),
                "filtered_by_floor_levels": floor_levels,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique zones: {e}", exc_info=True)
            raise

    async def get_workspace_cameras(
        self,
        workspace_id: UUID,
        location_filters: Dict[str, Optional[str]],
        status: Optional[str],
        search_term: Optional[str],
        group_by: Optional[str],
        include_inactive: bool,
        user_id: UUID,
        user_role: str
    ) -> Dict[str, Any]:
        """Get cameras in workspace with optional filtering and grouping"""
        try:
            # This method queries the database, not Qdrant
            conditions = ["vs.workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # Add location filters
            for field, value in location_filters.items():
                if value:
                    param_count += 1
                    conditions.append(f"vs.{field} ILIKE ${param_count}")
                    params.append(f"%{value}%")
            
            # Status filter
            if status:
                param_count += 1
                conditions.append(f"vs.status = ${param_count}")
                params.append(status)
            elif not include_inactive:
                conditions.append("vs.status != 'inactive'")
            
            # Search term
            if search_term:
                param_count += 1
                conditions.append(f"vs.name ILIKE ${param_count}")
                params.append(f"%{search_term}%")
            
            # Permission filter
            if user_role != "admin":
                param_count += 1
                conditions.append(f"vs.user_id = ${param_count}")
                params.append(user_id)
            
            where_clause = " AND ".join(conditions)
            
            if not group_by or group_by == "none":
                # Return flat list
                query = f"""
                    SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                        vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                        vs.latitude, vs.longitude, vs.created_at, vs.updated_at,
                        u.username as owner_username
                    FROM video_stream vs
                    JOIN users u ON vs.user_id = u.user_id
                    WHERE {where_clause}
                    ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name
                """
                
                cameras = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
                camera_list = [self._format_camera_data(cam) for cam in cameras] if cameras else []
                
                return {
                    "cameras": camera_list,
                    "groups": [],
                    "total_count": len(camera_list),
                    "group_type": "none"
                }
            else:
                # Return grouped results
                return await self._get_grouped_cameras(where_clause, params, group_by)
                
        except Exception as e:
            logger.error(f"Error getting workspace cameras: {e}", exc_info=True)
            raise

    async def get_location_analytics(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        group_by: str,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get analytics data grouped by location hierarchy"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        filter_obj = self.build_filter_from_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        # Fetch all matching points
        all_points = self._fetch_all_points_with_payload(
            client, collection_name, filter_obj,
            ["camera_id", "name", "location", "area", "building",
             "floor_level", "zone", "person_count", "timestamp"]
        )
        
        # Group data by specified field
        analytics_data = defaultdict(lambda: {
            'data_points': 0,
            'total_person_count': 0,
            'average_person_count': 0,
            'unique_cameras': set(),
            'time_range': {'earliest': None, 'latest': None}
        })
        
        for point in all_points:
            if not point.payload:
                continue
            
            group_key = point.payload.get(group_by)
            if not group_key:
                continue
            
            person_count = point.payload.get('person_count', 0)
            timestamp = point.payload.get('timestamp')
            camera_id = point.payload.get('camera_id')
            
            data = analytics_data[group_key]
            data['data_points'] += 1
            data['total_person_count'] += person_count
            
            if camera_id:
                data['unique_cameras'].add(camera_id)
            
            if timestamp:
                if data['time_range']['earliest'] is None or timestamp < data['time_range']['earliest']:
                    data['time_range']['earliest'] = timestamp
                if data['time_range']['latest'] is None or timestamp > data['time_range']['latest']:
                    data['time_range']['latest'] = timestamp
        
        # Calculate averages and format
        analytics = []
        for group_name, data in analytics_data.items():
            if data['data_points'] > 0:
                data['average_person_count'] = data['total_person_count'] / data['data_points']
            
            # Convert timestamps
            time_range = {}
            if data['time_range']['earliest']:
                time_range['earliest'] = datetime.fromtimestamp(
                    data['time_range']['earliest'], tz=ZoneInfo("Africa/Cairo")
                ).isoformat()
            if data['time_range']['latest']:
                time_range['latest'] = datetime.fromtimestamp(
                    data['time_range']['latest'], tz=ZoneInfo("Africa/Cairo")
                ).isoformat()
            
            analytics.append({
                group_by: group_name,
                'data_points': data['data_points'],
                'total_person_count': data['total_person_count'],
                'average_person_count': round(data['average_person_count'], 2),
                'unique_cameras': len(data['unique_cameras']),
                'camera_ids': list(data['unique_cameras']),
                'time_range': time_range
            })
        
        # Sort by total person count
        analytics.sort(key=lambda x: x['total_person_count'], reverse=True)
        
        return {
            "analytics": analytics,
            "total_groups": len(analytics),
            "group_by": group_by,
            "filters_applied": search_query.model_dump(exclude_none=True),
            "workspace_id": str(workspace_id)
        }
    
    async def get_location_summary(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get comprehensive summary of all location data"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        # Build filter
        must_conditions = []
        self._apply_user_access_filter(
            must_conditions, user_system_role, user_workspace_role, requesting_username
        )
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Get all points
        all_points = self._fetch_all_points_with_payload(
            client, collection_name, filter_obj,
            ["camera_id", "name", "location", "area", "building", "floor_level", "zone"]
        )
        
        # Process data
        unique_values = {
            'locations': set(),
            'areas': set(),
            'buildings': set(),
            'floor_levels': set(),
            'zones': set(),
            'cameras': set()
        }
        
        hierarchy_map = defaultdict(lambda: {
            'cameras': set(),
            'details': {}
        })
        
        for point in all_points:
            if not point.payload:
                continue
            
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            unique_values['cameras'].add(camera_id)
            
            # Extract fields
            location = point.payload.get('location')
            area = point.payload.get('area')
            building = point.payload.get('building')
            floor_level = point.payload.get('floor_level')
            zone = point.payload.get('zone')
            
            if location:
                unique_values['locations'].add(location)
            if area:
                unique_values['areas'].add(area)
            if building:
                unique_values['buildings'].add(building)
            if floor_level:
                unique_values['floor_levels'].add(floor_level)
            if zone:
                unique_values['zones'].add(zone)
            
            # Create hierarchy key
            hierarchy_key = (
                building or 'Unknown Building',
                floor_level or 'Unknown Floor',
                zone or 'Unknown Zone',
                area or 'Unknown Area',
                location or 'Unknown Location'
            )
            
            hierarchy_map[hierarchy_key]['cameras'].add(camera_id)
            hierarchy_map[hierarchy_key]['details'] = {
                'building': building,
                'floor_level': floor_level,
                'zone': zone,
                'area': area,
                'location': location
            }
        
        # Create hierarchy list
        hierarchy = []
        for key, data in hierarchy_map.items():
            hierarchy.append({
                **data['details'],
                'camera_count': len(data['cameras']),
                'camera_ids': list(data['cameras'])
            })
        
        # Sort hierarchy
        hierarchy.sort(key=lambda x: (
            x.get('building') or '',
            x.get('floor_level') or '',
            x.get('zone') or '',
            x.get('area') or '',
            x.get('location') or ''
        ))
        
        summary = {
            'locations': len(unique_values['locations']),
            'areas': len(unique_values['areas']),
            'buildings': len(unique_values['buildings']),
            'floor_levels': len(unique_values['floor_levels']),
            'zones': len(unique_values['zones']),
            'total_cameras': len(unique_values['cameras']),
            'unique_cameras': list(unique_values['cameras'])
        }
        
        return {
            "summary": summary,
            "hierarchy": hierarchy,
            "workspace_id": str(workspace_id),
            "data_source": "qdrant"
        }
    
    async def search_location_data(
        self,
        workspace_id: UUID,
        search_term: str,
        search_fields: List[str],
        exact_match: bool,
        limit: int,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Search location data in Qdrant"""
        client = self.get_client()
        collection_name = get_workspace_qdrant_collection_name(workspace_id)
        await ensure_workspace_qdrant_collection_exists(client, workspace_id)
        
        # Build base filter
        must_conditions = []
        self._apply_user_access_filter(
            must_conditions, user_system_role, user_workspace_role, requesting_username
        )
        
        valid_fields = ["location", "area", "building", "floor_level", "zone"]
        
        if exact_match:
            # Add exact match conditions
            search_conditions = []
            for field in search_fields:
                if field in valid_fields:
                    search_conditions.append(
                        qdrant_models.FieldCondition(
                            key=field,
                            match=qdrant_models.MatchValue(value=search_term)
                        )
                    )
            if search_conditions:
                must_conditions.append(
                    qdrant_models.Filter(should=search_conditions)
                )
        
        filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
        
        # Get points
        all_points = self._fetch_all_points_with_payload(
            client, collection_name, filter_obj,
            ["camera_id", "name", "location", "area", "building", "floor_level", "zone"],
            limit=limit * 10  # Get more for filtering
        )
        
        # Filter results
        matching_locations = defaultdict(lambda: {
            'cameras': set(),
            'details': {},
            'match_fields': set()
        })
        
        search_term_lower = search_term.lower()
        
        for point in all_points:
            if not point.payload:
                continue
            
            camera_id = point.payload.get('camera_id')
            if not camera_id:
                continue
            
            # Check matches
            match_found = False
            matched_fields = []
            
            for field in search_fields:
                if field in valid_fields:
                    field_value = point.payload.get(field)
                    if field_value:
                        if exact_match:
                            if field_value.lower() == search_term_lower:
                                match_found = True
                                matched_fields.append(field)
                        else:
                            if search_term_lower in field_value.lower():
                                match_found = True
                                matched_fields.append(field)
            
            if match_found:
                location_key = (
                    point.payload.get('location', ''),
                    point.payload.get('area', ''),
                    point.payload.get('building', ''),
                    point.payload.get('floor_level', ''),
                    point.payload.get('zone', '')
                )
                
                matching_locations[location_key]['cameras'].add(camera_id)
                matching_locations[location_key]['details'] = {
                    'location': point.payload.get('location'),
                    'area': point.payload.get('area'),
                    'building': point.payload.get('building'),
                    'floor_level': point.payload.get('floor_level'),
                    'zone': point.payload.get('zone')
                }
                matching_locations[location_key]['match_fields'].update(matched_fields)
        
        # Format results
        results = []
        for key, data in list(matching_locations.items())[:limit]:
            results.append({
                **data['details'],
                'camera_count': len(data['cameras']),
                'camera_ids': list(data['cameras']),
                'matched_fields': list(data['match_fields'])
            })
        
        # Sort by relevance
        results.sort(key=lambda x: (len(x['matched_fields']), x['camera_count']), reverse=True)
        
        return {
            "results": results,
            "search_term": search_term,
            "search_fields": search_fields,
            "exact_match": exact_match,
            "total_matches": len(results),
            "workspace_id": str(workspace_id)
        }

    async def delete_all_workspace_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """
        Delete ALL data from a workspace collection.
        This is a destructive operation that removes all points.
        Access control is still enforced based on user role.
        """
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter for user access control
            must_conditions = []
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            # Get total count before deletion
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            count_before = client.count(collection_name=collection_name, count_filter=filter_obj).count
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No data found to delete",
                    "deleted_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Perform deletion
            if filter_obj:
                # User-filtered deletion (non-admin users)
                delete_result = client.delete(
                    collection_name=collection_name,
                    points_selector=qdrant_models.FilterSelector(filter=filter_obj),
                    wait=True
                )
            else:
                # Admin deletion - clear entire collection more efficiently
                # Using delete with empty filter or recreating collection
                delete_result = client.delete(
                    collection_name=collection_name,
                    points_selector=qdrant_models.FilterSelector(
                        filter=qdrant_models.Filter(must=[])  # Matches all
                    ),
                    wait=True
                )
            
            if delete_result.status != qdrant_models.UpdateStatus.COMPLETED:
                logger.error(f"Delete operation failed with status: {delete_result.status}")
                raise RuntimeError(f"Delete operation failed: {delete_result.status}")
            
            # Verify deletion
            count_after = client.count(collection_name=collection_name, count_filter=filter_obj).count
            actual_deleted = count_before - count_after
            
            logger.info(f"Deleted {actual_deleted} points from workspace {workspace_id} collection")
            
            return {
                "status": "success",
                "message": f"Successfully deleted all workspace data",
                "deleted_count": actual_deleted,
                "workspace_id": str(workspace_id),
                "collection_name": collection_name
            }
            
        except Exception as e:
            logger.error(f"Error deleting all workspace data: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to delete workspace data: {str(e)}",
                "deleted_count": 0,
                "workspace_id": str(workspace_id)
            }

    async def get_collection_total_count(self, collection_name: str) -> int:
        """Get total count of points in a collection (without filters)"""
        try:
            client = self.get_client()
            count_result = client.count(collection_name=collection_name)
            return count_result.count
        except Exception as e:
            logger.error(f"Error getting collection count: {e}")
            return 0

    async def preview_metadata_update(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Preview data points that would be affected by a metadata update.
        Useful for confirming the scope before executing bulk updates.
        
        Args:
            workspace_id: Target workspace UUID
            camera_ids: Optional list of camera IDs to filter
            user_system_role: System role of requesting user
            user_workspace_role: Workspace role of requesting user
            requesting_username: Username of requester
            limit: Maximum number of sample points to return (default: 10)
            
        Returns:
            Dictionary containing total count and sample data points with current metadata
        """
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter
            must_conditions = []
            
            # Add camera ID filter if specified
            if camera_ids:
                if len(camera_ids) == 1:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="camera_id",
                            match=qdrant_models.MatchValue(value=camera_ids[0])
                        )
                    )
                else:
                    must_conditions.append(
                        qdrant_models.Filter(should=[
                            qdrant_models.FieldCondition(
                                key="camera_id",
                                match=qdrant_models.MatchValue(value=cam_id)
                            ) for cam_id in camera_ids
                        ])
                    )
            
            # Apply user access control
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            # Get total count of points that would be affected
            total_count = client.count(
                collection_name=collection_name, 
                count_filter=filter_obj
            ).count
            
            # Get sample points to preview
            sample_points, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=limit,
                with_payload=[
                    "camera_id", "name", "timestamp", "date", "time",
                    "location", "area", "building", "floor_level", "zone",
                    "latitude", "longitude", "username"
                ],
                with_vectors=False
            )
            
            # Format preview data
            preview_data = []
            unique_cameras = set()
            
            for point in sample_points:
                if point.payload:
                    camera_id = point.payload.get("camera_id")
                    if camera_id:
                        unique_cameras.add(camera_id)
                    
                    preview_data.append({
                        "point_id": str(point.id),
                        "camera_id": camera_id,
                        "camera_name": point.payload.get("name", "Unknown Camera"),
                        "timestamp": point.payload.get("timestamp"),
                        "date": point.payload.get("date"),
                        "time": point.payload.get("time"),
                        "username": point.payload.get("username"),
                        "current_metadata": {
                            "location": point.payload.get("location"),
                            "area": point.payload.get("area"),
                            "building": point.payload.get("building"),
                            "floor_level": point.payload.get("floor_level"),
                            "zone": point.payload.get("zone"),
                            "latitude": point.payload.get("latitude"),
                            "longitude": point.payload.get("longitude")
                        }
                    })
            
            # Build result summary
            result = {
                "total_points_affected": total_count,
                "unique_cameras_affected": len(unique_cameras),
                "camera_ids_in_preview": list(unique_cameras),
                "preview_limit": limit,
                "sample_points": preview_data,
                "workspace_id": str(workspace_id),
                "collection_name": collection_name,
                "filters_applied": {
                    "camera_ids": camera_ids if camera_ids else "all cameras",
                    "user_filter": "applied" if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner'] else "none"
                }
            }
            
            # Add helpful metadata statistics
            if preview_data:
                # Count how many points have each location field populated
                field_stats = {
                    "location": 0,
                    "area": 0,
                    "building": 0,
                    "floor_level": 0,
                    "zone": 0,
                    "latitude": 0,
                    "longitude": 0
                }
                
                for point in preview_data:
                    metadata = point.get("current_metadata", {})
                    for field in field_stats.keys():
                        if metadata.get(field) is not None:
                            field_stats[field] += 1
                
                result["metadata_statistics"] = {
                    "sample_size": len(preview_data),
                    "fields_populated": field_stats,
                    "fields_populated_percentage": {
                        field: round((count / len(preview_data)) * 100, 1)
                        for field, count in field_stats.items()
                    }
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error previewing metadata update for workspace {workspace_id}: {e}", exc_info=True)
            return {
                "error": str(e),
                "total_points_affected": 0,
                "unique_cameras_affected": 0,
                "sample_points": [],
                "workspace_id": str(workspace_id),
                "collection_name": get_workspace_qdrant_collection_name(workspace_id) if workspace_id else "unknown"
            }

    async def bulk_update_metadata(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        metadata_updates: Dict[str, Any],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """
        Bulk update metadata for camera data points in Qdrant.
        Now supports ALL fields from StreamUpdate model.
        
        Args:
            workspace_id: Target workspace UUID
            camera_ids: Optional list of camera IDs to update (None = all cameras)
            metadata_updates: Dictionary of metadata fields to update
            user_system_role: System role of requesting user
            user_workspace_role: Workspace role of requesting user
            requesting_username: Username of requester
            
        Returns:
            Dictionary with update results
        """
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter conditions
            must_conditions = []
            
            # Add camera ID filter if specified
            if camera_ids:
                if len(camera_ids) == 1:
                    must_conditions.append(
                        qdrant_models.FieldCondition(
                            key="camera_id",
                            match=qdrant_models.MatchValue(value=camera_ids[0])
                        )
                    )
                else:
                    must_conditions.append(
                        qdrant_models.Filter(should=[
                            qdrant_models.FieldCondition(
                                key="camera_id",
                                match=qdrant_models.MatchValue(value=cam_id)
                            ) for cam_id in camera_ids
                        ])
                    )
            
            # Apply user access control
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            filter_obj = qdrant_models.Filter(must=must_conditions) if must_conditions else None
            
            if not filter_obj:
                raise ValueError("No filter criteria specified for metadata update")
            
            # Get count of points to update
            count_before = client.count(collection_name=collection_name, count_filter=filter_obj).count
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No data points found matching criteria",
                    "updated_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Prepare payload - ALL fields can be updated now
            payload_updates = {}
            
            # Basic camera info fields
            if 'name' in metadata_updates and metadata_updates['name'] is not None:
                payload_updates['name'] = metadata_updates['name']
            
            if 'path' in metadata_updates and metadata_updates['path'] is not None:
                payload_updates['path'] = metadata_updates['path']
            
            if 'type' in metadata_updates and metadata_updates['type'] is not None:
                payload_updates['type'] = metadata_updates['type']
            
            if 'status' in metadata_updates and metadata_updates['status'] is not None:
                payload_updates['status'] = metadata_updates['status']
            
            if 'is_streaming' in metadata_updates and metadata_updates['is_streaming'] is not None:
                payload_updates['is_streaming'] = metadata_updates['is_streaming']
            
            # Location fields
            location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
            for field in location_fields:
                if field in metadata_updates and metadata_updates[field] is not None:
                    payload_updates[field] = metadata_updates[field]
            
            # Coordinate fields - convert to float
            if 'latitude' in metadata_updates and metadata_updates['latitude'] is not None:
                payload_updates['latitude'] = float(metadata_updates['latitude'])
            
            if 'longitude' in metadata_updates and metadata_updates['longitude'] is not None:
                payload_updates['longitude'] = float(metadata_updates['longitude'])
            
            # Alert threshold fields
            if 'count_threshold_greater' in metadata_updates and metadata_updates['count_threshold_greater'] is not None:
                payload_updates['count_threshold_greater'] = int(metadata_updates['count_threshold_greater'])
            
            if 'count_threshold_less' in metadata_updates and metadata_updates['count_threshold_less'] is not None:
                payload_updates['count_threshold_less'] = int(metadata_updates['count_threshold_less'])
            
            if 'alert_enabled' in metadata_updates and metadata_updates['alert_enabled'] is not None:
                payload_updates['alert_enabled'] = bool(metadata_updates['alert_enabled'])
            
            if not payload_updates:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided for update",
                    "updated_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Perform the update
            update_result = client.set_payload(
                collection_name=collection_name,
                payload=payload_updates,
                points=filter_obj,
                wait=True
            )
            
            if update_result.status != qdrant_models.UpdateStatus.COMPLETED:
                logger.error(f"Metadata update failed with status: {update_result.status}")
                raise RuntimeError(f"Metadata update failed: {update_result.status}")
            
            logger.info(f"Updated metadata for {count_before} points in workspace {workspace_id}")
            
            return {
                "status": "success",
                "message": f"Successfully updated metadata for {count_before} data points",
                "updated_count": count_before,
                "updated_fields": list(payload_updates.keys()),
                "workspace_id": str(workspace_id),
                "collection_name": collection_name,
                "camera_ids": camera_ids if camera_ids else "all"
            }
            
        except ValueError as ve:
            logger.error(f"Validation error in metadata update: {ve}")
            return {
                "status": "error",
                "message": str(ve),
                "updated_count": 0,
                "workspace_id": str(workspace_id)
            }
        except Exception as e:
            logger.error(f"Error updating metadata: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to update metadata: {str(e)}",
                "updated_count": 0,
                "workspace_id": str(workspace_id)
            }

    async def update_camera_metadata_by_id(
        self,
        workspace_id: UUID,
        camera_id: str,
        stream_update: StreamUpdate,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """
        Update metadata for a specific camera by ID using StreamUpdate model.
        Now handles ALL fields from StreamUpdate.
        
        Args:
            workspace_id: Target workspace UUID
            camera_id: Specific camera ID to update
            stream_update: StreamUpdate model containing update fields
            user_system_role: System role of requesting user
            user_workspace_role: Workspace role of requesting user
            requesting_username: Username of requester
            
        Returns:
            Dictionary with update results
        """
        try:
            client = self.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Ensure collection exists
            await ensure_workspace_qdrant_collection_exists(client, workspace_id)
            
            # Build filter for specific camera
            must_conditions = [
                qdrant_models.FieldCondition(
                    key="camera_id",
                    match=qdrant_models.MatchValue(value=camera_id)
                )
            ]
            
            # Apply user access control
            self._apply_user_access_filter(
                must_conditions, user_system_role, user_workspace_role, requesting_username
            )
            
            filter_obj = qdrant_models.Filter(must=must_conditions)
            
            # Get count of points for this camera
            count_before = client.count(
                collection_name=collection_name, 
                count_filter=filter_obj
            ).count
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": f"No data points found for camera {camera_id}",
                    "updated_count": 0,
                    "camera_id": camera_id,
                    "workspace_id": str(workspace_id)
                }
            
            # Prepare payload updates from StreamUpdate - ALL FIELDS
            payload_updates = {}
            
            # Basic camera fields
            if stream_update.name is not None:
                payload_updates['name'] = stream_update.name
            
            if stream_update.path is not None:
                payload_updates['path'] = stream_update.path
            
            if stream_update.type is not None:
                payload_updates['type'] = stream_update.type
            
            if stream_update.status is not None:
                payload_updates['status'] = stream_update.status
            
            if stream_update.is_streaming is not None:
                payload_updates['is_streaming'] = stream_update.is_streaming
            
            # Location fields
            location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
            for field in location_fields:
                if hasattr(stream_update, field):
                    value = getattr(stream_update, field)
                    if value is not None:
                        payload_updates[field] = value
            
            # Coordinate fields
            if stream_update.latitude is not None:
                payload_updates['latitude'] = float(stream_update.latitude)
            
            if stream_update.longitude is not None:
                payload_updates['longitude'] = float(stream_update.longitude)
            
            # Alert fields
            if stream_update.count_threshold_greater is not None:
                payload_updates['count_threshold_greater'] = int(stream_update.count_threshold_greater)
            
            if stream_update.count_threshold_less is not None:
                payload_updates['count_threshold_less'] = int(stream_update.count_threshold_less)
            
            if stream_update.alert_enabled is not None:
                payload_updates['alert_enabled'] = bool(stream_update.alert_enabled)
            
            if not payload_updates:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided for update",
                    "updated_count": 0,
                    "camera_id": camera_id,
                    "workspace_id": str(workspace_id)
                }
            
            # Perform the update
            update_result = client.set_payload(
                collection_name=collection_name,
                payload=payload_updates,
                points=filter_obj,
                wait=True
            )
            
            if update_result.status != qdrant_models.UpdateStatus.COMPLETED:
                logger.error(f"Metadata update failed with status: {update_result.status}")
                raise RuntimeError(f"Metadata update failed: {update_result.status}")
            
            logger.info(f"Updated metadata for camera {camera_id}: {count_before} points updated")
            
            return {
                "status": "success",
                "message": f"Successfully updated metadata for camera {camera_id}",
                "updated_count": count_before,
                "updated_fields": list(payload_updates.keys()),
                "camera_id": camera_id,
                "camera_name": stream_update.name if stream_update.name else "unchanged",
                "workspace_id": str(workspace_id),
                "collection_name": collection_name
            }
            
        except ValueError as ve:
            logger.error(f"Validation error updating camera {camera_id}: {ve}")
            return {
                "status": "error",
                "message": str(ve),
                "updated_count": 0,
                "camera_id": camera_id,
                "workspace_id": str(workspace_id)
            }
        except Exception as e:
            logger.error(f"Error updating camera {camera_id} metadata: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to update camera metadata: {str(e)}",
                "updated_count": 0,
                "camera_id": camera_id,
                "workspace_id": str(workspace_id)
            }
    
    # ========== Helper Methods ==========
    
    def _fetch_all_points(
        self, client: QdrantClient, collection_name: str, filter_obj
    ) -> List:
        """Fetch all points handling Qdrant batch limitations"""
        all_points = []
        batch_size = 1000
        offset = 0
        
        while True:
            batch_points, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=batch_size,
                offset=offset,
                with_payload=True,
                with_vectors=False
            )
            all_points.extend(batch_points)
            
            if len(batch_points) < batch_size:
                break
            offset += batch_size
        
        return all_points
    
    def _fetch_all_points_with_payload(
        self, client: QdrantClient, collection_name: str, filter_obj,
        payload_fields: List[str], limit: Optional[int] = None
    ) -> List:
        """Fetch points with specific payload fields"""
        all_points = []
        batch_size = 1000
        offset = None
        
        while True:
            try:
                    
                if limit and len(all_points) >= limit:
                    break
                
                current_limit = min(batch_size, limit - len(all_points)) if limit else batch_size

                points, next_offset = client.scroll(
                    collection_name=collection_name,
                    scroll_filter=filter_obj,
                    limit=current_limit,
                    offset=offset,
                    with_payload=payload_fields,
                    with_vectors=False
                )

                if not points:
                    break
                
                all_points.extend(points)
                
                if next_offset is None or len(points) < current_limit:
                    break
                offset = next_offset
                
            except Exception as e:
                logger.error(f"Error scrolling through points in {collection_name}: {e}", exc_info=True)
                break
        
        return all_points[:limit] if limit else all_points

    def _format_point_data(self, point, include_base64: bool = True) -> Dict[str, Any]:
        """Format point data for response"""
        frame_data = point.payload.get("frame_base64") if include_base64 else None
        
        return {
            "id": str(point.id),
            "frame": frame_data,
            "metadata": {
                "camera_id": point.payload.get("camera_id"),
                "name": point.payload.get("name", "Unknown Camera"),
                "timestamp": point.payload.get("timestamp"),
                "date": point.payload.get("date"),
                "time": point.payload.get("time"),
                "person_count": point.payload.get("person_count", 0),
                "male_count": point.payload.get("male_count", 0),
                "female_count": point.payload.get("female_count", 0),
                "fire_status": point.payload.get("fire_status", "no detection"),
                "owner_username": point.payload.get("username"),
                "location": point.payload.get("location"),
                "area": point.payload.get("area"),
                "building": point.payload.get("building"),
                "floor_level": point.payload.get("floor_level"),
                "zone": point.payload.get("zone")
            }
        }
    
    def _prepare_prediction_data(self, points) -> List[Dict[str, Any]]:
        """Prepare data for prediction"""
        data = []
        for p in points:
            if p.payload:
                data.append({
                    "metadata": {
                        "camera_id": p.payload.get("camera_id"),
                        "timestamp": p.payload.get("timestamp"),
                        "person_count": p.payload.get("person_count", 0),
                        "male_count": p.payload.get("male_count", 0),
                        "female_count": p.payload.get("female_count", 0),
                        "fire_status": p.payload.get("fire_status", "no detection")
                    }
                })
        
        # Sort by timestamp for prediction
        data.sort(key=lambda x: x['metadata'].get('timestamp', 0))
        return data
    
    def _format_camera_data(self, cam) -> Dict[str, Any]:
        """Format camera data for response"""
        return {
            "id": str(cam["stream_id"]),
            "name": cam["name"],
            "path": cam["path"],
            "type": cam["type"],
            "status": cam["status"],
            "is_streaming": cam["is_streaming"],
            "location": cam["location"],
            "area": cam["area"],
            "building": cam["building"],
            "floor_level": cam["floor_level"],
            "zone": cam["zone"],
            "latitude": float(cam["latitude"]) if cam["latitude"] else None,
            "longitude": float(cam["longitude"]) if cam["longitude"] else None,
            "owner_username": cam["owner_username"],
            "created_at": cam["created_at"].isoformat() if cam["created_at"] else None,
            "updated_at": cam["updated_at"].isoformat() if cam["updated_at"] else None
        }
    
    async def _get_grouped_cameras(
        self, where_clause: str, params: List, group_by: str
    ) -> Dict[str, Any]:
        """Get cameras grouped by specified field"""
        group_field = f"vs.{group_by}"
        
        query = f"""
            SELECT 
                {group_field} as group_name,
                COUNT(*) as camera_count,
                COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_count,
                COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_count,
                COUNT(CASE WHEN vs.status = 'error' THEN 1 END) as error_count,
                COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_count,
                ARRAY_AGG(
                    JSON_BUILD_OBJECT(
                        'id', vs.stream_id::text,
                        'name', vs.name,
                        'path', vs.path,
                        'type', vs.type,
                        'status', vs.status,
                        'is_streaming', vs.is_streaming,
                        'location', vs.location,
                        'area', vs.area,
                        'building', vs.building,
                        'floor_level', vs.floor_level,
                        'zone', vs.zone,
                        'latitude', vs.latitude,
                        'longitude', vs.longitude,
                        'owner_username', u.username,
                        'created_at', vs.created_at,
                        'updated_at', vs.updated_at
                    ) ORDER BY vs.name
                ) as cameras
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE {where_clause} AND {group_field} IS NOT NULL
            GROUP BY {group_field}
            ORDER BY {group_field}
        """
        
        results = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
        
        groups = []
        total_cameras = 0
        
        for result in results:
            processed_cameras = []
            if result["cameras"]:
                for cam in result["cameras"]:
                    cam_data = cam.copy()
                    if cam_data.get("created_at"):
                        cam_data["created_at"] = cam_data["created_at"].isoformat()
                    if cam_data.get("updated_at"):
                        cam_data["updated_at"] = cam_data["updated_at"].isoformat()
                    if cam_data.get("latitude"):
                        cam_data["latitude"] = float(cam_data["latitude"])
                    if cam_data.get("longitude"):
                        cam_data["longitude"] = float(cam_data["longitude"])
                    processed_cameras.append(cam_data)
            
            group = {
                "group_type": group_by,
                "group_name": result["group_name"] or "Unknown",
                "camera_count": result["camera_count"],
                "cameras": processed_cameras,
                "active_count": result["active_count"] or 0,
                "inactive_count": result["inactive_count"] or 0,
                "streaming_count": result["streaming_count"] or 0
            }
            groups.append(group)
            total_cameras += group["camera_count"]
        
        return {
            "cameras": [],
            "groups": groups,
            "total_count": total_cameras,
            "group_type": group_by
        }
    

qdrant_service = QdrantService()