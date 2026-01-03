# app/services/elasticsearch_service.py
from typing import List, Dict, Any, Optional, Union, Tuple, Set
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta, time as dt_time
from collections import defaultdict
import logging
import numpy as np
from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch.exceptions import NotFoundError
from fastapi import HTTPException, status

from app.services.database import db_manager
from app.services.workspace_service import workspace_service 
from app.services.user_service import user_manager
from app.utils import (
    parse_camera_ids, make_prediction, parse_date_format, parse_time_string,
    get_workspace_elasticsearch_index_name, ensure_workspace_elasticsearch_index_exists,
    parse_string_or_list, frame_to_base64
)

from app.schemas import (
    SearchQuery, TimestampRangeResponse,
    LocationSearchQuery, DeleteDataRequest,
    StreamUpdate
)
from app.config.settings import config

logger = logging.getLogger(__name__)

class ElasticsearchService:
    """Service layer for all Elasticsearch operations"""
    
    def __init__(self):
        self.es_client = None
        self.workspace_service = workspace_service
        self.db_manager = db_manager
        self.user_manager = user_manager
        self.BASE_INDEX_NAME = config.elasticsearch_index_name
        self._workspace_index_init_cache: Dict[str, bool] = {}
        self.fire_state_cleanup_interval = config.fire_state_cleanup_interval_seconds
        
    def get_client(self) -> AsyncElasticsearch:
        """Get or initialize Elasticsearch client"""
        if self.es_client is None:
            hosts = config.elasticsearch_hosts

            client_options = {
                "hosts": hosts,
                "request_timeout": config.elasticsearch_timeout,
                "max_retries": 3,
                "retry_on_timeout": True,
            }
            
            es_version = config.elasticsearch_version  # Default to 8
            if es_version in ["7", "8"]:
                # For elasticsearch-py 9.x connecting to ES 7.x/8.x servers
                client_options["headers"] = {
                    "Accept": f"application/vnd.elasticsearch+json; compatible-with={es_version}"
                }
                logger.info(f"Initializing Elasticsearch client with compatibility mode for ES {es_version}.x")
            
            self.es_client = AsyncElasticsearch(**client_options)
            logger.info(f"Elasticsearch client initialized with hosts: {hosts}")
        return self.es_client
    
    async def initialize(self):
        """Initialize service on startup"""
        self.get_client()
        # Test connection
        try:
            info = await self.es_client.info()
            logger.info(f"Connected to Elasticsearch cluster: {info['cluster_name']}")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
    
    async def close(self):
        """Close Elasticsearch client"""
        if self.es_client:
            await self.es_client.close()
            logger.info("Elasticsearch client closed.")

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
        location_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Insert detection data with location information into Elasticsearch"""
        if count == 0:
            return False

        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            now_utc = datetime.now(ZoneInfo("Africa/Cairo"))
            doc_id = str(uuid4())
            
            # Base document
            document = {
                "camera_id": camera_id_str,
                "name": camera_name,
                "timestamp": now_utc.timestamp(),
                "datetime": now_utc.isoformat(),
                "date": now_utc.strftime("%Y-%m-%d"),
                "time": now_utc.strftime("%H:%M:%S.%f")[:-3],
                "person_count": count,
                "male_count": male_count,
                "female_count": female_count,
                "fire_status": fire_status,
                "frame_base64": frame_to_base64(frame),
                "username": username,
            }
            
            # Add location information
            if location_info:
                document.update({
                    "location": location_info.get('location'),
                    "area": location_info.get('area'),
                    "building": location_info.get('building'),
                    "zone": location_info.get('zone'),
                    "floor_level": location_info.get('floor_level'),
                    "latitude": float(location_info['latitude']) if location_info.get('latitude') else None,
                    "longitude": float(location_info['longitude']) if location_info.get('longitude') else None,
                })
                
                # Add geo_point for location-based queries
                if location_info.get('latitude') and location_info.get('longitude'):
                    document["location_point"] = {
                        "lat": float(location_info['latitude']),
                        "lon": float(location_info['longitude'])
                    }
            
            # Index document
            await client.index(
                index=index_name,
                id=doc_id,
                document=document,
                refresh=False  # Don't wait for refresh for better performance
            )
            
            logger.debug(f"Inserted detection data to {index_name}: {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting detection data to Elasticsearch ({index_name}): {e}", exc_info=True)
            return False

    async def batch_insert_detection_data(
        self,
        detection_batch: List[Dict[str, Any]],
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """Batch insert multiple detection data points using bulk API"""
        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            actions = []
            
            for detection in detection_batch:
                doc_id = str(uuid4())
                
                # Process frame if provided
                frame_base64 = None
                if 'frame' in detection and detection['frame'] is not None:
                    frame_base64 = frame_to_base64(detection['frame'])
                
                # Build document
                document = {
                    "camera_id": detection['camera_id'],
                    "name": detection['camera_name'],
                    "timestamp": detection['timestamp'],
                    "datetime": datetime.fromtimestamp(detection['timestamp'], tz=ZoneInfo("Africa/Cairo")).isoformat(),
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
                    document.update({
                        "location": location_info.get('location'),
                        "area": location_info.get('area'),
                        "building": location_info.get('building'),
                        "zone": location_info.get('zone'),
                        "floor_level": location_info.get('floor_level'),
                        "latitude": float(location_info['latitude']) if location_info.get('latitude') else None,
                        "longitude": float(location_info['longitude']) if location_info.get('longitude') else None,
                    })
                    
                    if location_info.get('latitude') and location_info.get('longitude'):
                        document["location_point"] = {
                            "lat": float(location_info['latitude']),
                            "lon": float(location_info['longitude'])
                        }
                
                actions.append({
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": document
                })
            
            # Bulk index
            if actions:
                success, failed = await helpers.async_bulk(
                    client,
                    actions,
                    chunk_size=500,
                    raise_on_error=False
                )
                
                logger.info(f"Batch inserted {success} detection points to {index_name}")
                
                return {
                    "success": True,
                    "inserted_count": success,
                    "failed_count": len(failed) if failed else 0,
                    "index": index_name
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

    async def ensure_workspace_index(self, workspace_id: UUID) -> bool:
        """Ensure Elasticsearch index exists for workspace"""
        try:
            client = self.get_client()
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            return True
        except Exception as e:
            logger.error(f"Failed to ensure Elasticsearch index for workspace {workspace_id}: {e}", exc_info=True)
            return False

    # ========== Index Statistics ==========
    
    async def get_workspace_statistics(
        self,
        workspace_id: UUID,
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a workspace index"""
        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query for user access
            query = {"match_all": {}}
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                if requesting_username:
                    query = {"term": {"username": requesting_username}}
            
            # Get count
            count_result = await client.count(index=index_name, query=query)
            total_count = count_result['count']
            
            # Get index stats
            stats = await client.indices.stats(index=index_name)
            index_stats = stats['indices'][index_name]
            
            # Get unique cameras using aggregation
            cameras_agg = await client.search(
                index=index_name,
                size=0,
                aggs={
                    "unique_cameras": {
                        "terms": {
                            "field": "camera_id.keyword",
                            "size": 10000
                        }
                    }
                }
            )
            
            cameras = [
                bucket['key'] 
                for bucket in cameras_agg['aggregations']['unique_cameras']['buckets']
            ]
            
            # Get timestamp range
            timestamp_range = await self.get_timestamp_range(
                workspace_id=workspace_id,
                camera_ids=None,
                user_system_role=user_system_role,
                user_workspace_role=user_workspace_role,
                requesting_username=requesting_username
            )
            
            size_bytes = index_stats['total']['store']['size_in_bytes']
            size_human = self._format_bytes(size_bytes)

            return {
                "workspace_id": str(workspace_id),
                "index_name": index_name,
                "statistics": {
                    "total_documents": total_count,
                    "index_size_bytes": size_bytes,
                    "index_size_human": size_human,
                    "doc_count": index_stats['total']['docs']['count'],
                    "deleted_docs": index_stats['total']['docs']['deleted'],
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
        """Export workspace data in various formats"""
        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Build query
            es_query = self._build_elasticsearch_query(
                search_query, user_system_role, user_workspace_role, requesting_username
            ) if search_query else {"match_all": {}}
            
            # Fetch all matching documents using scroll API
            all_docs = await self._scroll_all_documents(client, index_name, es_query)
            
            # Format data based on requested format
            if format.lower() == 'csv':
                return self._export_as_csv(all_docs, workspace_id)
            elif format.lower() == 'json':
                return self._export_as_json(all_docs, workspace_id)
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Error exporting workspace data: {e}", exc_info=True)
            raise

    def _export_as_csv(self, documents: List[Dict], workspace_id: UUID) -> Tuple[bytes, str, str]:
        """Export documents as CSV"""
        import csv
        import io
        
        output = io.StringIO()
        
        if not documents:
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
        
        for doc in documents:
            source = doc.get('_source', {})
            row = {
                'timestamp': source.get('timestamp'),
                'date': source.get('date'),
                'time': source.get('time'),
                'camera_id': source.get('camera_id'),
                'camera_name': source.get('name'),
                'person_count': source.get('person_count'),
                'male_count': source.get('male_count'),
                'female_count': source.get('female_count'),
                'fire_status': source.get('fire_status'),
                'location': source.get('location'),
                'area': source.get('area'),
                'building': source.get('building'),
                'floor_level': source.get('floor_level'),
                'zone': source.get('zone'),
                'latitude': source.get('latitude'),
                'longitude': source.get('longitude'),
                'username': source.get('username')
            }
            writer.writerow(row)
        
        csv_data = output.getvalue().encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return csv_data, "text/csv", filename

    def _export_as_json(self, documents: List[Dict], workspace_id: UUID) -> Tuple[bytes, str, str]:
        """Export documents as JSON"""
        import json
        
        data = []
        for doc in documents:
            source = doc.get('_source', {})
            # Exclude frame_base64 from export
            source_copy = dict(source)
            source_copy.pop('frame_base64', None)
            data.append(source_copy)
        
        json_data = json.dumps(data, indent=2, default=str).encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return json_data, "application/json", filename

    # ========== Fire Detection State Management ==========

    async def get_persistent_fire_state(self, stream_id: UUID) -> Dict[str, Any]:
        """Get persistent fire state from database"""
        try:
            result = await self.db_manager.execute_query(
                """SELECT fire_status, last_detection_time, last_notification_time 
                FROM fire_detection_state WHERE stream_id = $1""",
                (stream_id,), fetch_one=True
            )
            
            if result:
                return {
                    'fire_status': result['fire_status'],
                    'last_detection_time': result['last_detection_time'],
                    'last_notification_time': result['last_notification_time']
                }
            return {
                'fire_status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }
        except Exception as e:
            logger.error(f"Error getting persistent fire state for {stream_id}: {e}")
            return {
                'fire_status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }

    async def update_persistent_fire_state(
        self, 
        stream_id: UUID, 
        fire_status: str,
        detection_time: datetime = None,
        notification_time: datetime = None
    ):
        """Update persistent fire state in database with proper UPSERT"""
        try:
            now = datetime.now(ZoneInfo("Africa/Cairo"))
            detection_time = detection_time or now
            
            await self.db_manager.execute_query(
                """INSERT INTO fire_detection_state 
                (stream_id, fire_status, last_detection_time, last_notification_time, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (stream_id) 
                DO UPDATE SET 
                    fire_status = EXCLUDED.fire_status,
                    last_detection_time = EXCLUDED.last_detection_time,
                    last_notification_time = CASE 
                        WHEN EXCLUDED.last_notification_time IS NOT NULL 
                        THEN EXCLUDED.last_notification_time 
                        ELSE fire_detection_state.last_notification_time 
                    END,
                    updated_at = EXCLUDED.updated_at""",
                (stream_id, fire_status, detection_time, notification_time, now, now)
            )
            
            logger.debug(f"Updated persistent fire state for {stream_id}: "
                        f"status={fire_status}, notification_time={notification_time}")
            
        except Exception as e:
            logger.error(f"Error updating persistent fire state for {stream_id}: {e}")

    async def load_fire_state_on_stream_start(self, stream_id: UUID) -> Dict[str, Any]:
        """Load persistent fire state when stream starts"""
        stream_id_str = str(stream_id)
        try:
            persistent_state = await self.get_persistent_fire_state(stream_id)
            
            cooldown_info = {
                'fire_status': persistent_state['fire_status'],
                'should_restore_cooldown': False,
                'cooldown_remaining': 0,
                'last_notification_timestamp': None
            }
            
            if persistent_state['last_notification_time']:
                last_notification_timestamp = persistent_state['last_notification_time'].timestamp()
                import time
                current_time = time.time()
                time_since_last = current_time - last_notification_timestamp
                
                fire_cooldown_duration = 300.0
                if time_since_last < fire_cooldown_duration:
                    cooldown_info['should_restore_cooldown'] = True
                    cooldown_info['cooldown_remaining'] = fire_cooldown_duration - time_since_last
                    cooldown_info['last_notification_timestamp'] = last_notification_timestamp
                    
                    logger.info(f"Fire cooldown should be restored for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_remaining={cooldown_info['cooldown_remaining']/60:.1f}min")
                else:
                    logger.info(f"Fire state loaded for {stream_id_str}: "
                            f"fire_status='{persistent_state['fire_status']}', "
                            f"cooldown_expired")
            else:
                logger.info(f"Fire state loaded for {stream_id_str}: "
                        f"fire_status='{persistent_state['fire_status']}', "
                        f"no_previous_notifications")
            
            return cooldown_info
            
        except Exception as e:
            logger.error(f"Error loading fire state for {stream_id}: {e}")
            return {
                'fire_status': 'no detection',
                'should_restore_cooldown': False,
                'cooldown_remaining': 0,
                'last_notification_timestamp': None
            }

    async def cleanup_old_fire_states(self):
        """Clean up old fire detection states"""
        try:
            await self.db_manager.execute_query(
                """DELETE FROM fire_detection_state 
                WHERE stream_id NOT IN (SELECT stream_id FROM video_stream)"""
            )
            
            day_ago = datetime.now(ZoneInfo("Africa/Cairo")) - timedelta(days=1)
            await self.db_manager.execute_query(
                """UPDATE fire_detection_state 
                SET last_notification_time = NULL 
                WHERE last_notification_time < $1""",
                (day_ago,)
            )
            
            logger.debug("Cleaned up old fire detection states")
            
        except Exception as e:
            logger.error(f"Error cleaning up fire states: {e}")

    async def insert_detection_data_with_location(
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
        location_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Alias for insert_detection_data that explicitly shows location support"""
        return await self.insert_detection_data(
            username=username,
            camera_id_str=camera_id_str,
            camera_name=camera_name,
            count=count,
            male_count=male_count,
            female_count=female_count,
            fire_status=fire_status,
            frame=frame,
            workspace_id=workspace_id,
            location_info=location_info
        )

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
    
    def _build_elasticsearch_query(
        self,
        query: Union[SearchQuery, LocationSearchQuery],
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build Elasticsearch query from search query"""
        must_clauses = []
        
        # Camera ID filter
        self._add_camera_filter(query, must_clauses)
        
        # Location filters
        self._add_location_filters(query, must_clauses)
        
        # Timestamp filters
        if hasattr(query, 'start_date') or hasattr(query, 'end_date'):
            self._add_timestamp_filter(query, must_clauses)
        
        # User access control
        self._apply_user_access_filter(
            must_clauses, user_system_role, user_workspace_role, requesting_username
        )
        
        if must_clauses:
            return {"bool": {"must": must_clauses}}
        return {"match_all": {}}
    
    def _add_camera_filter(self, query, must_clauses):
        """Handle camera ID filtering logic."""
        if hasattr(query, 'camera_id') and query.camera_id:
            str_camera_ids = [str(cid) for cid in query.camera_id if cid]
            if str_camera_ids:
                if len(str_camera_ids) == 1:
                    must_clauses.append({"term": {"camera_id.keyword": str_camera_ids[0]}})
                else:
                    must_clauses.append({"terms": {"camera_id.keyword": str_camera_ids}})
    
    def _add_location_filters(self, query, must_clauses):
        """Handle location field filtering."""
        location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
        for field in location_fields:
            if hasattr(query, field):
                field_value = getattr(query, field)
                if field_value:
                    must_clauses.append({"term": {f"{field}.keyword": field_value}})
    
    def _add_timestamp_filter(self, query: SearchQuery, must_clauses: list):
        """Add timestamp range filter"""
        range_filter = {}
        
        try:
            if hasattr(query, 'start_date') and query.start_date:
                start_date_obj = parse_date_format(query.start_date)
                start_time_obj = parse_time_string(getattr(query, 'start_time', None), dt_time.min)
                start_datetime = datetime.combine(start_date_obj, start_time_obj).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                range_filter["gte"] = start_datetime.timestamp()
            
            if hasattr(query, 'end_date') and query.end_date:
                end_date_obj = parse_date_format(query.end_date)
                end_time_obj = parse_time_string(
                    getattr(query, 'end_time', None), 
                    dt_time.max.replace(microsecond=0)
                )
                end_datetime = datetime.combine(end_date_obj, end_time_obj).replace(tzinfo=ZoneInfo("Africa/Cairo"))
                range_filter["lte"] = end_datetime.timestamp()
            
            if range_filter:
                must_clauses.append({"range": {"timestamp": range_filter}})
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            raise ValueError(f"Invalid date/time format: {e}")
    
    def _apply_user_access_filter(
        self, must_clauses: list, user_system_role: str, 
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
                must_clauses.append({"term": {"username.keyword": requesting_username}})
            else:
                # Safeguard: apply impossible filter
                must_clauses.append({"term": {"username.keyword": f"__IMPOSSIBLE_USERNAME_{uuid4()}__"}})

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
        """Search workspace data in Elasticsearch"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        es_query = self._build_elasticsearch_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        # Get count
        count_result = await client.count(index=index_name, query=es_query)
        total_count = count_result['count']
        
        paginated_data = []
        num_of_pages = 0
        
        if total_count > 0:
            if per_page is None:
                # Return all results using scroll
                num_of_pages = 1
                documents = await self._scroll_all_documents(client, index_name, es_query)
            else:
                # Paginated results
                num_of_pages = (total_count + per_page - 1) // per_page
                if page <= num_of_pages:
                    offset = (page - 1) * per_page
                    result = await client.search(
                        index=index_name,
                        query=es_query,
                        from_=offset,
                        size=per_page,
                        sort=[{"timestamp": {"order": "desc"}}]
                    )
                    documents = result['hits']['hits']
                else:
                    documents = []
            
            # Process documents
            for doc in documents:
                paginated_data.append(self._format_document_data(doc, base64))
        
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
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        es_query = self._build_elasticsearch_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        count_result = await client.count(index=index_name, query=es_query)
        total_count = count_result['count']
        
        paginated_data = []
        num_of_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        
        if total_count > 0 and page <= num_of_pages:
            offset = (page - 1) * per_page
            
            result = await client.search(
                index=index_name,
                query=es_query,
                from_=offset,
                size=per_page,
                sort=[{"timestamp": {"order": "desc"}}]
            )
            
            for doc in result['hits']['hits']:
                paginated_data.append(self._format_document_data(doc))
        
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
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
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
                
                es_query = self._build_elasticsearch_query(
                    cam_query, user_system_role, user_workspace_role, requesting_username
                )
                
                result = await client.search(
                    index=index_name,
                    query=es_query,
                    size=config.gprediction_data_points_limit,
                    sort=[{"timestamp": {"order": "desc"}}],
                    _source=["timestamp", "person_count", "male_count", "female_count", "fire_status"]
                )
                
                documents = result['hits']['hits']
                
                if documents:
                    prediction_data = self._prepare_prediction_data(documents)
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
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> TimestampRangeResponse:
        """Get timestamp range for data"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        es_query = self._build_elasticsearch_query(
            SearchQuery(camera_id=camera_ids),
            user_system_role,
            user_workspace_role,
            requesting_username
        )
        
        min_ts, max_ts = None, None
        
        try:
            # Get timestamp range using aggregations
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "min_timestamp": {"min": {"field": "timestamp"}},
                    "max_timestamp": {"max": {"field": "timestamp"}}
                }
            )
            
            aggs = result.get('aggregations', {})
            if aggs.get('min_timestamp', {}).get('value'):
                min_ts = aggs['min_timestamp']['value']
            if aggs.get('max_timestamp', {}).get('value'):
                max_ts = aggs['max_timestamp']['value']
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
    
    # ========== Index Management ==========
    
    async def create_index(
        self, index_name: str, mappings: Optional[Dict] = None
    ) -> Dict[str, str]:
        """Create a new Elasticsearch index"""
        client = self.get_client()
        
        # Check if exists
        try:
            exists = await client.indices.exists(index=index_name)
            if exists:
                return {"status": "info", "message": f"Index '{index_name}' already exists."}
        except Exception:
            pass
        
        # Default mappings for detection data
        if mappings is None:
            mappings = {
                "properties": {
                    "camera_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "timestamp": {"type": "double"},
                    "datetime": {"type": "date"},
                    "date": {"type": "keyword"},
                    "time": {"type": "keyword"},
                    "person_count": {"type": "integer"},
                    "male_count": {"type": "integer"},
                    "female_count": {"type": "integer"},
                    "fire_status": {"type": "keyword"},
                    "frame_base64": {"type": "text", "index": False},
                    "username": {"type": "keyword"},
                    "location": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "area": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "building": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "zone": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "floor_level": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "latitude": {"type": "float"},
                    "longitude": {"type": "float"},
                    "location_point": {"type": "geo_point"}
                }
            }
        
        # Create index
        await client.indices.create(
            index=index_name,
            mappings=mappings
        )
        
        return {"status": "success", "message": f"Index '{index_name}' created."}
    
    async def delete_index(self, index_name: str) -> Dict[str, str]:
        """Delete an Elasticsearch index"""
        client = self.get_client()
        
        try:
            exists = await client.indices.exists(index=index_name)
            if not exists:
                raise ValueError(f"Index '{index_name}' not found.")
            
            await client.indices.delete(index=index_name)
            return {"status": "success", "message": f"Index '{index_name}' deleted."}
        except NotFoundError:
            raise ValueError(f"Index '{index_name}' not found.")
    
    async def list_indices(self) -> Dict[str, List]:
        """List all Elasticsearch indices"""
        client = self.get_client()
        
        # Get all indices matching pattern
        indices = await client.indices.get(index=f"{self.BASE_INDEX_NAME}*")
        
        index_info_list = []
        for index_name, index_data in indices.items():
            try:
                stats = await client.indices.stats(index=index_name)
                index_stats = stats['indices'][index_name]
                
                ws_id_from_name = None
                if index_name.startswith(self.BASE_INDEX_NAME + "_ws_"):
                    try:
                        ws_id_str = index_name.split("_ws_")[1].replace("_", "-")
                        ws_id_from_name = str(UUID(ws_id_str))
                    except (IndexError, ValueError):
                        logger.warning(f"Could not parse workspace ID from index: {index_name}")
                
                size_bytes = index_stats['total']['store']['size_in_bytes']
                size_human = self._format_bytes(size_bytes)
                
                index_info_list.append({
                    "name": index_name,
                    "health": index_stats['health'],
                    "status": index_stats['status'],
                    "docs_count": index_stats['total']['docs']['count'],
                    "docs_deleted": index_stats['total']['docs']['deleted'],
                    "store_size_bytes": size_bytes,
                    "store_size": size_human,
                    # "store_size": index_stats['total']['store']['size'],
                    "mappings": index_data.get('mappings', {}),
                    "associated_workspace_id": ws_id_from_name
                })
            except Exception as e:
                logger.warning(f"Could not get details for index {index_name}: {e}")
                index_info_list.append({
                    "name": index_name,
                    "status": "details_error",
                    "error": str(e)
                })
        
        return {"indices": index_info_list}
    
    async def get_index_count(
        self,
        index_name: str,
        search_query: Optional[SearchQuery],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get count of documents in index"""
        client = self.get_client()
        
        es_query = {"match_all": {}}
        if search_query:
            es_query = self._build_elasticsearch_query(
                search_query, user_system_role, user_workspace_role, requesting_username
            )
        
        count_result = await client.count(index=index_name, query=es_query)
        
        return {
            "index_name": index_name,
            "count": count_result['count'],
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
        """Delete data from workspace index"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        search_query = SearchQuery(
            camera_id=parse_camera_ids(delete_request.camera_id) if delete_request.camera_id else None,
            start_date=delete_request.start_date,
            end_date=delete_request.end_date,
            start_time=delete_request.start_time,
            end_time=delete_request.end_time
        )
        
        es_query = self._build_elasticsearch_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        if es_query == {"match_all": {}}:
            raise ValueError("No filter criteria provided for deletion")
        
        # Get count before deletion
        count_before = await client.count(index=index_name, query=es_query)
        deleted_count = count_before['count']
        
        if deleted_count == 0:
            return {
                "status": "info",
                "message": "No data found matching criteria",
                "deleted_count": 0
            }
        
        # Delete data
        delete_result = await client.delete_by_query(
            index=index_name,
            query=es_query,
            refresh=True
        )
        
        return {
            "status": "success",
            "message": f"Successfully deleted {delete_result['deleted']} data points",
            "deleted_count": delete_result['deleted']
        }
    
    async def delete_user_camera_data(
        self, 
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> Dict:
        """Delete all user's camera data from Elasticsearch indices"""
        result = {
            "success": True,
            "deleted_cameras": [],
            "failed_cameras": [],
            "indices_affected": []
        }
        
        if not workspace_camera_mapping:
            logger.info("No camera data to delete from Elasticsearch")
            return result
        
        try:
            client = self.get_client()
            
            for workspace_id_str, camera_ids in workspace_camera_mapping.items():
                try:
                    index_name = get_workspace_elasticsearch_index_name(workspace_id_str)
                    result["indices_affected"].append(index_name)
                    
                    # Check if index exists
                    exists = await client.indices.exists(index=index_name)
                    if not exists:
                        logger.info(f"Index {index_name} not found, skipping")
                        continue
                    
                    # Delete data for each camera
                    for camera_id in camera_ids:
                        try:
                            delete_result = await client.delete_by_query(
                                index=index_name,
                                query={"term": {"camera_id.keyword": camera_id}},
                                refresh=True
                            )
                            
                            if delete_result.get('deleted', 0) > 0:
                                result["deleted_cameras"].append(camera_id)
                                logger.info(f"Deleted {delete_result['deleted']} documents for camera {camera_id}")
                            else:
                                logger.info(f"No documents found for camera {camera_id}")
                                result["deleted_cameras"].append(camera_id)
                        
                        except Exception as camera_err:
                            logger.error(f"Failed to delete Elasticsearch data for camera {camera_id}: {camera_err}")
                            result["failed_cameras"].append(camera_id)
                
                except Exception as ws_err:
                    logger.error(f"Failed to process workspace {workspace_id_str} in Elasticsearch: {ws_err}")
                    result["failed_cameras"].extend(camera_ids)
            
            # Mark as failed if any cameras failed
            if result["failed_cameras"]:
                result["success"] = False
                logger.warning(f"Some cameras failed Elasticsearch deletion: {result['failed_cameras']}")
        
        except Exception as e:
            logger.error(f"Error during Elasticsearch deletion: {e}", exc_info=True)
            result["success"] = False
            result["error"] = str(e)
        
        return result

    async def delete_camera_data_from_workspaces(
        self, 
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> List[str]:
        """Delete camera data from Elasticsearch indices"""
        failures = []
        client = self.get_client()
        
        for workspace_id, camera_ids in workspace_camera_mapping.items():
            try:
                index_name = get_workspace_elasticsearch_index_name(workspace_id)
                
                # Check if index exists
                exists = await client.indices.exists(index=index_name)
                if not exists:
                    logger.info(f"Index {index_name} not found, skipping")
                    continue
                
                for camera_id in camera_ids:
                    try:
                        delete_result = await client.delete_by_query(
                            index=index_name,
                            query={"term": {"camera_id.keyword": camera_id}},
                            refresh=True
                        )
                        
                        if delete_result.get('deleted', 0) > 0:
                            logger.info(f"Deleted Elasticsearch documents for camera {camera_id}")
                        else:
                            logger.warning(f"No documents found for camera {camera_id}")
                            
                    except Exception as e:
                        logger.error(f"Failed to delete Elasticsearch data for camera {camera_id}: {e}")
                        failures.append(camera_id)
                        
            except Exception as e:
                logger.error(f"Failed to access Elasticsearch index for workspace {workspace_id}: {e}")
                failures.extend(camera_ids)
        
        return list(set(failures))

    async def update_camera_metadata(
        self,
        stream_update: StreamUpdate,
        stream_id: str,
        workspace_id: str
    ) -> None:
        """Update Elasticsearch metadata for a camera"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        
        # Ensure index exists
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        # Build update script
        update_fields = {}
        
        if stream_update.name is not None:
            update_fields["name"] = stream_update.name
        
        location_fields = ['location', 'area', 'building', 'floor_level', 'zone', 'latitude', 'longitude']
        for field in location_fields:
            if hasattr(stream_update, field) and getattr(stream_update, field) is not None:
                value = getattr(stream_update, field)
                if field in ['latitude', 'longitude']:
                    value = float(value)
                update_fields[field] = value
        
        if update_fields:
            # Update location_point if lat/lon changed
            if 'latitude' in update_fields or 'longitude' in update_fields:
                # Need to get current values to build complete geo_point
                pass  # Handle in update_by_query script
            
            # Build update script
            script_source = "ctx._source.putAll(params.fields)"
            
            await client.update_by_query(
                index=index_name,
                query={"term": {"camera_id.keyword": stream_id}},
                script={
                    "source": script_source,
                    "params": {"fields": update_fields},
                    "lang": "painless"
                },
                refresh=True
            )
            logger.info(f"Updated Elasticsearch metadata for camera {stream_id}")
            
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
        """Extract location data from Elasticsearch index"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        # Build query
        must_clauses = []
        
        # User access control
        self._apply_user_access_filter(
            must_clauses, user_system_role, user_workspace_role, requesting_username
        )
        
        # Add location filters
        if filter_conditions:
            for field_name, field_values in filter_conditions.items():
                if field_values:
                    if isinstance(field_values, str):
                        field_values = [field_values]
                    
                    if len(field_values) == 1:
                        must_clauses.append({"term": {f"{field_name}.keyword": field_values[0]}})
                    else:
                        must_clauses.append({"terms": {f"{field_name}.keyword": field_values}})
        
        es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
        
        # Use aggregations to get unique location combinations
        agg_fields = ["camera_id", "name", "location", "area", "building", "floor_level", "zone"]
        
        if location_field:
            # Group by specific field
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "locations": {
                        "terms": {
                            "field": f"{location_field}.keyword",
                            "size": 10000
                        },
                        "aggs": {
                            "unique_cameras": {
                                "cardinality": {"field": "camera_id.keyword"}
                            }
                        }
                    }
                }
            )
            
            location_data = []
            for bucket in result['aggregations']['locations']['buckets']:
                location_data.append({
                    location_field: bucket['key'],
                    'camera_count': bucket['unique_cameras']['value'],
                    'doc_count': bucket['doc_count']
                })
            
            return location_data
        else:
            # Get all unique location combinations
            documents = await self._scroll_all_documents(
                client, index_name, es_query,
                source_includes=agg_fields
            )
            
            # Process documents to extract location data
            location_data = defaultdict(lambda: {
                'count': 0,
                'cameras': set(),
                'details': {}
            })
            
            for doc in documents:
                source = doc.get('_source', {})
                camera_id = source.get('camera_id')
                
                if not camera_id:
                    continue
                
                location_info = {
                    'location': source.get('location'),
                    'area': source.get('area'),
                    'building': source.get('building'),
                    'floor_level': source.get('floor_level'),
                    'zone': source.get('zone'),
                    'camera_name': source.get('name', 'Unknown Camera')
                }
                
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query with filters
            must_clauses = []
            
            # User access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if area:
                must_clauses.append({"term": {"area.keyword": area}})
            if building:
                must_clauses.append({"term": {"building.keyword": building}})
            if floor_level:
                must_clauses.append({"term": {"floor_level.keyword": floor_level}})
            if zone:
                must_clauses.append({"term": {"zone.keyword": zone}})
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Use composite aggregation to get locations with their full hierarchy
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "location_groups": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {"location": {"terms": {"field": "location.keyword"}}},
                                {"area": {"terms": {"field": "area.keyword", "missing_bucket": True}}},
                                {"building": {"terms": {"field": "building.keyword", "missing_bucket": True}}},
                                {"floor_level": {"terms": {"field": "floor_level.keyword", "missing_bucket": True}}},
                                {"zone": {"terms": {"field": "zone.keyword", "missing_bucket": True}}}
                            ]
                        },
                        "aggs": {
                            "camera_count": {"cardinality": {"field": "camera_id.keyword"}},
                            "camera_ids": {"terms": {"field": "camera_id.keyword", "size": 1000}}
                        }
                    }
                }
            )
            
            # Process results
            locations = []
            for bucket in result['aggregations']['location_groups']['buckets']:
                key_data = bucket['key']
                camera_ids = [b['key'] for b in bucket['camera_ids']['buckets']]
                
                locations.append({
                    'location': key_data.get('location'),
                    'area': key_data.get('area'),
                    'building': key_data.get('building'),
                    'floor_level': key_data.get('floor_level'),
                    'zone': key_data.get('zone'),
                    'camera_count': bucket['camera_count']['value'],
                    'camera_ids': camera_ids
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query with filters
            must_clauses = []
            
            # User access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if building:
                must_clauses.append({"term": {"building.keyword": building}})
            if floor_level:
                must_clauses.append({"term": {"floor_level.keyword": floor_level}})
            if zone:
                must_clauses.append({"term": {"zone.keyword": zone}})
            if location:
                must_clauses.append({"term": {"location.keyword": location}})
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Use composite aggregation to get areas with their full hierarchy
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "area_groups": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {"area": {"terms": {"field": "area.keyword"}}},
                                {"building": {"terms": {"field": "building.keyword", "missing_bucket": True}}},
                                {"floor_level": {"terms": {"field": "floor_level.keyword", "missing_bucket": True}}},
                                {"zone": {"terms": {"field": "zone.keyword", "missing_bucket": True}}},
                                {"location": {"terms": {"field": "location.keyword", "missing_bucket": True}}}
                            ]
                        },
                        "aggs": {
                            "camera_count": {"cardinality": {"field": "camera_id.keyword"}},
                            "camera_ids": {"terms": {"field": "camera_id.keyword", "size": 1000}}
                        }
                    }
                }
            )
            
            # Process results
            areas = []
            for bucket in result['aggregations']['area_groups']['buckets']:
                key_data = bucket['key']
                camera_ids = [b['key'] for b in bucket['camera_ids']['buckets']]
                
                areas.append({
                    'area': key_data.get('area'),
                    'building': key_data.get('building'),
                    'floor_level': key_data.get('floor_level'),
                    'zone': key_data.get('zone'),
                    'location': key_data.get('location'),
                    'camera_count': bucket['camera_count']['value'],
                    'camera_ids': camera_ids
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query with filters
            must_clauses = []
            
            # User access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location hierarchy filters
            if floor_level:
                must_clauses.append({"term": {"floor_level.keyword": floor_level}})
            if zone:
                must_clauses.append({"term": {"zone.keyword": zone}})
            if area:
                must_clauses.append({"term": {"area.keyword": area}})
            if location:
                must_clauses.append({"term": {"location.keyword": location}})
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Use composite aggregation to get buildings with their full hierarchy
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "building_groups": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {"building": {"terms": {"field": "building.keyword"}}},
                                {"floor_level": {"terms": {"field": "floor_level.keyword", "missing_bucket": True}}},
                                {"zone": {"terms": {"field": "zone.keyword", "missing_bucket": True}}},
                                {"area": {"terms": {"field": "area.keyword", "missing_bucket": True}}},
                                {"location": {"terms": {"field": "location.keyword", "missing_bucket": True}}}
                            ]
                        },
                        "aggs": {
                            "camera_count": {"cardinality": {"field": "camera_id.keyword"}},
                            "camera_ids": {"terms": {"field": "camera_id.keyword", "size": 1000}}
                        }
                    }
                }
            )
            
            # Process results
            buildings = []
            for bucket in result['aggregations']['building_groups']['buckets']:
                key_data = bucket['key']
                camera_ids = [b['key'] for b in bucket['camera_ids']['buckets']]
                
                buildings.append({
                    'building': key_data.get('building'),
                    'floor_level': key_data.get('floor_level'),
                    'zone': key_data.get('zone'),
                    'area': key_data.get('area'),
                    'location': key_data.get('location'),
                    'camera_count': bucket['camera_count']['value'],
                    'camera_ids': camera_ids
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query with filters
            must_clauses = []
            
            # User access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add location filters
            if location:
                must_clauses.append({"term": {"location.keyword": location}})
            if area:
                must_clauses.append({"term": {"area.keyword": area}})
            if building:
                must_clauses.append({"term": {"building.keyword": building}})
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Use composite aggregation to get floor levels with their location info
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "floor_level_groups": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {"floor_level": {"terms": {"field": "floor_level.keyword"}}},
                                {"building": {"terms": {"field": "building.keyword", "missing_bucket": True}}},
                                {"area": {"terms": {"field": "area.keyword", "missing_bucket": True}}},
                                {"location": {"terms": {"field": "location.keyword", "missing_bucket": True}}}
                            ]
                        },
                        "aggs": {
                            "camera_count": {"cardinality": {"field": "camera_id.keyword"}},
                            "camera_ids": {"terms": {"field": "camera_id.keyword", "size": 1000}}
                        }
                    }
                }
            )
            
            # Process results
            floor_levels = []
            for bucket in result['aggregations']['floor_level_groups']['buckets']:
                key_data = bucket['key']
                camera_ids = [b['key'] for b in bucket['camera_ids']['buckets']]
                
                floor_levels.append({
                    'floor_level': key_data.get('floor_level'),
                    'building': key_data.get('building'),
                    'area': key_data.get('area'),
                    'location': key_data.get('location'),
                    'camera_count': bucket['camera_count']['value'],
                    'camera_ids': camera_ids
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query with filters
            must_clauses = []
            
            # User access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            # Add floor level filter
            if floor_levels:
                if len(floor_levels) == 1:
                    must_clauses.append({"term": {"floor_level.keyword": floor_levels[0]}})
                else:
                    must_clauses.append({"terms": {"floor_level.keyword": floor_levels}})
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Use composite aggregation to get zones with their full location hierarchy
            result = await client.search(
                index=index_name,
                query=es_query,
                size=0,
                aggs={
                    "zone_groups": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {"zone": {"terms": {"field": "zone.keyword"}}},
                                {"floor_level": {"terms": {"field": "floor_level.keyword", "missing_bucket": True}}},
                                {"building": {"terms": {"field": "building.keyword", "missing_bucket": True}}},
                                {"area": {"terms": {"field": "area.keyword", "missing_bucket": True}}},
                                {"location": {"terms": {"field": "location.keyword", "missing_bucket": True}}}
                            ]
                        },
                        "aggs": {
                            "camera_count": {"cardinality": {"field": "camera_id.keyword"}},
                            "camera_ids": {"terms": {"field": "camera_id.keyword", "size": 1000}}
                        }
                    }
                }
            )
            
            # Process results
            zones = []
            for bucket in result['aggregations']['zone_groups']['buckets']:
                key_data = bucket['key']
                camera_ids = [b['key'] for b in bucket['camera_ids']['buckets']]
                
                zones.append({
                    'zone': key_data.get('zone'),
                    'floor_level': key_data.get('floor_level'),
                    'building': key_data.get('building'),
                    'area': key_data.get('area'),
                    'location': key_data.get('location'),
                    'camera_count': bucket['camera_count']['value'],
                    'camera_ids': camera_ids
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
            # This method queries the database, not Elasticsearch
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
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        es_query = self._build_elasticsearch_query(
            search_query, user_system_role, user_workspace_role, requesting_username
        )
        
        # Use aggregations for analytics
        result = await client.search(
            index=index_name,
            query=es_query,
            size=0,
            aggs={
                "grouped_data": {
                    "terms": {
                        "field": f"{group_by}.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "total_person_count": {"sum": {"field": "person_count"}},
                        "avg_person_count": {"avg": {"field": "person_count"}},
                        "unique_cameras": {"cardinality": {"field": "camera_id.keyword"}},
                        "time_range": {
                            "stats": {"field": "timestamp"}
                        }
                    }
                }
            }
        )
        
        analytics = []
        for bucket in result['aggregations']['grouped_data']['buckets']:
            time_stats = bucket['time_range']
            time_range = {}
            
            if time_stats.get('min'):
                time_range['earliest'] = datetime.fromtimestamp(
                    time_stats['min'], tz=ZoneInfo("Africa/Cairo")
                ).isoformat()
            if time_stats.get('max'):
                time_range['latest'] = datetime.fromtimestamp(
                    time_stats['max'], tz=ZoneInfo("Africa/Cairo")
                ).isoformat()
            
            analytics.append({
                group_by: bucket['key'],
                'data_points': bucket['doc_count'],
                'total_person_count': int(bucket['total_person_count']['value']),
                'average_person_count': round(bucket['avg_person_count']['value'], 2),
                'unique_cameras': bucket['unique_cameras']['value'],
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
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        # Build query
        must_clauses = []
        self._apply_user_access_filter(
            must_clauses, user_system_role, user_workspace_role, requesting_username
        )
        es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
        
        # Use aggregations to get summary
        result = await client.search(
            index=index_name,
            query=es_query,
            size=0,
            aggs={
                "unique_locations": {"cardinality": {"field": "location.keyword"}},
                "unique_areas": {"cardinality": {"field": "area.keyword"}},
                "unique_buildings": {"cardinality": {"field": "building.keyword"}},
                "unique_floor_levels": {"cardinality": {"field": "floor_level.keyword"}},
                "unique_zones": {"cardinality": {"field": "zone.keyword"}},
                "unique_cameras": {"cardinality": {"field": "camera_id.keyword"}},
                "hierarchy": {
                    "composite": {
                        "size": 10000,
                        "sources": [
                            {"building": {"terms": {"field": "building.keyword"}}},
                            {"floor_level": {"terms": {"field": "floor_level.keyword"}}},
                            {"zone": {"terms": {"field": "zone.keyword"}}},
                            {"area": {"terms": {"field": "area.keyword"}}},
                            {"location": {"terms": {"field": "location.keyword"}}}
                        ]
                    },
                    "aggs": {
                        "camera_count": {"cardinality": {"field": "camera_id.keyword"}}
                    }
                }
            }
        )
        
        aggs = result['aggregations']
        
        summary = {
            'locations': aggs['unique_locations']['value'],
            'areas': aggs['unique_areas']['value'],
            'buildings': aggs['unique_buildings']['value'],
            'floor_levels': aggs['unique_floor_levels']['value'],
            'zones': aggs['unique_zones']['value'],
            'total_cameras': aggs['unique_cameras']['value']
        }
        
        # Build hierarchy
        hierarchy = []
        for bucket in aggs['hierarchy']['buckets']:
            key_data = bucket['key']
            hierarchy.append({
                'building': key_data.get('building'),
                'floor_level': key_data.get('floor_level'),
                'zone': key_data.get('zone'),
                'area': key_data.get('area'),
                'location': key_data.get('location'),
                'camera_count': bucket['camera_count']['value']
            })
        
        return {
            "summary": summary,
            "hierarchy": hierarchy,
            "workspace_id": str(workspace_id),
            "data_source": "elasticsearch"
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
        """Search location data in Elasticsearch"""
        client = self.get_client()
        index_name = get_workspace_elasticsearch_index_name(workspace_id)
        await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
        
        # Build base query
        must_clauses = []
        self._apply_user_access_filter(
            must_clauses, user_system_role, user_workspace_role, requesting_username
        )
        
        valid_fields = ["location", "area", "building", "floor_level", "zone"]
        
        # Add search conditions
        search_conditions = []
        for field in search_fields:
            if field in valid_fields:
                if exact_match:
                    search_conditions.append({"term": {f"{field}.keyword": search_term}})
                else:
                    search_conditions.append({
                        "wildcard": {f"{field}.keyword": f"*{search_term}*"}
                    })
        
        if search_conditions:
            must_clauses.append({"bool": {"should": search_conditions, "minimum_should_match": 1}})
        
        es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
        
        # Get matching documents
        result = await client.search(
            index=index_name,
            query=es_query,
            size=limit * 10,  # Get more for processing
            _source=["camera_id", "name", "location", "area", "building", "floor_level", "zone"]
        )
        
        # Process results
        matching_locations = defaultdict(lambda: {
            'cameras': set(),
            'details': {},
            'match_fields': set()
        })
        
        search_term_lower = search_term.lower()
        
        for doc in result['hits']['hits']:
            source = doc['_source']
            camera_id = source.get('camera_id')
            
            if not camera_id:
                continue
            
            # Check matches
            matched_fields = []
            for field in search_fields:
                if field in valid_fields:
                    field_value = source.get(field)
                    if field_value:
                        if exact_match:
                            if field_value.lower() == search_term_lower:
                                matched_fields.append(field)
                        else:
                            if search_term_lower in field_value.lower():
                                matched_fields.append(field)
            
            if matched_fields:
                location_key = (
                    source.get('location', ''),
                    source.get('area', ''),
                    source.get('building', ''),
                    source.get('floor_level', ''),
                    source.get('zone', '')
                )
                
                matching_locations[location_key]['cameras'].add(camera_id)
                matching_locations[location_key]['details'] = {
                    'location': source.get('location'),
                    'area': source.get('area'),
                    'building': source.get('building'),
                    'floor_level': source.get('floor_level'),
                    'zone': source.get('zone')
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
        Delete ALL data from a workspace index.
        This is a destructive operation that removes all documents.
        Access control is still enforced based on user role.
        """
        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query for user access control
            must_clauses = []
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Get total count before deletion
            count_before = await client.count(index=index_name, query=es_query)
            total_count = count_before['count']
            
            if total_count == 0:
                return {
                    "status": "info",
                    "message": "No data found to delete",
                    "deleted_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Perform deletion
            delete_result = await client.delete_by_query(
                index=index_name,
                query=es_query,
                refresh=True,
                wait_for_completion=True
            )
            
            deleted_count = delete_result.get('deleted', 0)
            
            logger.info(f"Deleted {deleted_count} documents from workspace {workspace_id} index")
            
            return {
                "status": "success",
                "message": f"Successfully deleted all workspace data",
                "deleted_count": deleted_count,
                "workspace_id": str(workspace_id),
                "index_name": index_name
            }
            
        except Exception as e:
            logger.error(f"Error deleting all workspace data: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to delete workspace data: {str(e)}",
                "deleted_count": 0,
                "workspace_id": str(workspace_id)
            }

    async def get_index_total_count(self, index_name: str) -> int:
        """Get total count of documents in an index (without filters)"""
        try:
            client = self.get_client()
            count_result = await client.count(index=index_name)
            return count_result['count']
        except Exception as e:
            logger.error(f"Error getting index count: {e}")
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
        Preview data documents that would be affected by a metadata update.
        Useful for confirming the scope before executing bulk updates.
        
        Args:
            workspace_id: Target workspace UUID
            camera_ids: Optional list of camera IDs to filter
            user_system_role: System role of requesting user
            user_workspace_role: Workspace role of requesting user
            requesting_username: Username of requester
            limit: Maximum number of sample documents to return (default: 10)
            
        Returns:
            Dictionary containing total count and sample documents with current metadata
        """
        try:
            client = self.get_client()
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query
            must_clauses = []
            
            # Add camera ID filter if specified
            if camera_ids:
                if len(camera_ids) == 1:
                    must_clauses.append({"term": {"camera_id.keyword": camera_ids[0]}})
                else:
                    must_clauses.append({"terms": {"camera_id.keyword": camera_ids}})
            
            # Apply user access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
            
            # Get total count of documents that would be affected
            count_result = await client.count(index=index_name, query=es_query)
            total_count = count_result['count']
            
            # Get sample documents to preview
            sample_result = await client.search(
                index=index_name,
                query=es_query,
                size=limit,
                _source=[
                    "camera_id", "name", "timestamp", "date", "time",
                    "location", "area", "building", "floor_level", "zone",
                    "latitude", "longitude", "username"
                ],
                sort=[{"timestamp": {"order": "desc"}}]
            )
            
            # Format preview data
            preview_data = []
            unique_cameras = set()
            
            for hit in sample_result['hits']['hits']:
                source = hit.get('_source', {})
                camera_id = source.get("camera_id")
                
                if camera_id:
                    unique_cameras.add(camera_id)
                
                preview_data.append({
                    "document_id": hit.get('_id'),
                    "camera_id": camera_id,
                    "camera_name": source.get("name", "Unknown Camera"),
                    "timestamp": source.get("timestamp"),
                    "date": source.get("date"),
                    "time": source.get("time"),
                    "username": source.get("username"),
                    "current_metadata": {
                        "location": source.get("location"),
                        "area": source.get("area"),
                        "building": source.get("building"),
                        "floor_level": source.get("floor_level"),
                        "zone": source.get("zone"),
                        "latitude": source.get("latitude"),
                        "longitude": source.get("longitude")
                    }
                })
            
            # Build result summary
            result = {
                "total_documents_affected": total_count,
                "unique_cameras_affected": len(unique_cameras),
                "camera_ids_in_preview": list(unique_cameras),
                "preview_limit": limit,
                "sample_documents": preview_data,
                "workspace_id": str(workspace_id),
                "index_name": index_name,
                "filters_applied": {
                    "camera_ids": camera_ids if camera_ids else "all cameras",
                    "user_filter": "applied" if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner'] else "none"
                }
            }
            
            # Add helpful metadata statistics
            if preview_data:
                field_stats = {
                    "location": 0,
                    "area": 0,
                    "building": 0,
                    "floor_level": 0,
                    "zone": 0,
                    "latitude": 0,
                    "longitude": 0
                }
                
                for doc in preview_data:
                    metadata = doc.get("current_metadata", {})
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
                "total_documents_affected": 0,
                "unique_cameras_affected": 0,
                "sample_documents": [],
                "workspace_id": str(workspace_id),
                "index_name": get_workspace_elasticsearch_index_name(workspace_id) if workspace_id else "unknown"
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
        Bulk update metadata for camera data documents in Elasticsearch.
        Supports ALL fields from StreamUpdate model.
        
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query
            must_clauses = []
            
            # Add camera ID filter if specified
            if camera_ids:
                if len(camera_ids) == 1:
                    must_clauses.append({"term": {"camera_id.keyword": camera_ids[0]}})
                else:
                    must_clauses.append({"terms": {"camera_id.keyword": camera_ids}})
            
            # Apply user access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            es_query = {"bool": {"must": must_clauses}} if must_clauses else None
            
            if not es_query:
                raise ValueError("No filter criteria specified for metadata update")
            
            # Get count of documents to update
            count_result = await client.count(index=index_name, query=es_query)
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No documents found matching criteria",
                    "updated_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Prepare update fields - ALL fields can be updated
            update_fields = {}
            
            # Basic camera info fields
            if 'name' in metadata_updates and metadata_updates['name'] is not None:
                update_fields['name'] = metadata_updates['name']
            
            if 'path' in metadata_updates and metadata_updates['path'] is not None:
                update_fields['path'] = metadata_updates['path']
            
            if 'type' in metadata_updates and metadata_updates['type'] is not None:
                update_fields['type'] = metadata_updates['type']
            
            if 'status' in metadata_updates and metadata_updates['status'] is not None:
                update_fields['status'] = metadata_updates['status']
            
            if 'is_streaming' in metadata_updates and metadata_updates['is_streaming'] is not None:
                update_fields['is_streaming'] = metadata_updates['is_streaming']
            
            # Location fields
            location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
            for field in location_fields:
                if field in metadata_updates and metadata_updates[field] is not None:
                    update_fields[field] = metadata_updates[field]
            
            # Coordinate fields - convert to float
            if 'latitude' in metadata_updates and metadata_updates['latitude'] is not None:
                update_fields['latitude'] = float(metadata_updates['latitude'])
            
            if 'longitude' in metadata_updates and metadata_updates['longitude'] is not None:
                update_fields['longitude'] = float(metadata_updates['longitude'])
            
            # Alert threshold fields
            if 'count_threshold_greater' in metadata_updates and metadata_updates['count_threshold_greater'] is not None:
                update_fields['count_threshold_greater'] = int(metadata_updates['count_threshold_greater'])
            
            if 'count_threshold_less' in metadata_updates and metadata_updates['count_threshold_less'] is not None:
                update_fields['count_threshold_less'] = int(metadata_updates['count_threshold_less'])
            
            if 'alert_enabled' in metadata_updates and metadata_updates['alert_enabled'] is not None:
                update_fields['alert_enabled'] = bool(metadata_updates['alert_enabled'])
            
            if not update_fields:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided for update",
                    "updated_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Update location_point if lat/lon are being updated
            if 'latitude' in update_fields or 'longitude' in update_fields:
                # Need to handle geo_point update - may need to fetch current values
                # if only one coordinate is being updated
                pass
            
            # Build update script
            script_source = "ctx._source.putAll(params.fields)"
            
            # Perform the update
            update_result = await client.update_by_query(
                index=index_name,
                query=es_query,
                script={
                    "source": script_source,
                    "params": {"fields": update_fields},
                    "lang": "painless"
                },
                refresh=True,
                wait_for_completion=True
            )
            
            updated_count = update_result.get('updated', 0)
            
            logger.info(f"Updated metadata for {updated_count} documents in workspace {workspace_id}")
            
            return {
                "status": "success",
                "message": f"Successfully updated metadata for {updated_count} documents",
                "updated_count": updated_count,
                "updated_fields": list(update_fields.keys()),
                "workspace_id": str(workspace_id),
                "index_name": index_name,
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
        Handles ALL fields from StreamUpdate.
        
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
            index_name = get_workspace_elasticsearch_index_name(workspace_id)
            
            # Ensure index exists
            await ensure_workspace_elasticsearch_index_exists(client, workspace_id)
            
            # Build query for specific camera
            must_clauses = [
                {"term": {"camera_id.keyword": camera_id}}
            ]
            
            # Apply user access control
            self._apply_user_access_filter(
                must_clauses, user_system_role, user_workspace_role, requesting_username
            )
            
            es_query = {"bool": {"must": must_clauses}}
            
            # Get count of documents for this camera
            count_result = await client.count(index=index_name, query=es_query)
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": f"No documents found for camera {camera_id}",
                    "updated_count": 0,
                    "camera_id": camera_id,
                    "workspace_id": str(workspace_id)
                }
            
            # Prepare update fields from StreamUpdate - ALL FIELDS
            update_fields = {}
            
            # Basic camera fields
            if stream_update.name is not None:
                update_fields['name'] = stream_update.name
            
            if stream_update.path is not None:
                update_fields['path'] = stream_update.path
            
            if stream_update.type is not None:
                update_fields['type'] = stream_update.type
            
            if stream_update.status is not None:
                update_fields['status'] = stream_update.status
            
            if stream_update.is_streaming is not None:
                update_fields['is_streaming'] = stream_update.is_streaming
            
            # Location fields
            location_fields = ['location', 'area', 'building', 'floor_level', 'zone']
            for field in location_fields:
                if hasattr(stream_update, field):
                    value = getattr(stream_update, field)
                    if value is not None:
                        update_fields[field] = value
            
            # Coordinate fields
            if stream_update.latitude is not None:
                update_fields['latitude'] = float(stream_update.latitude)
            
            if stream_update.longitude is not None:
                update_fields['longitude'] = float(stream_update.longitude)
            
            # Update location_point if coordinates are provided
            if stream_update.latitude is not None and stream_update.longitude is not None:
                update_fields['location_point'] = {
                    "lat": float(stream_update.latitude),
                    "lon": float(stream_update.longitude)
                }
            
            # Alert fields
            if stream_update.count_threshold_greater is not None:
                update_fields['count_threshold_greater'] = int(stream_update.count_threshold_greater)
            
            if stream_update.count_threshold_less is not None:
                update_fields['count_threshold_less'] = int(stream_update.count_threshold_less)
            
            if stream_update.alert_enabled is not None:
                update_fields['alert_enabled'] = bool(stream_update.alert_enabled)
            
            if not update_fields:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided for update",
                    "updated_count": 0,
                    "camera_id": camera_id,
                    "workspace_id": str(workspace_id)
                }
            
            # Build update script
            script_source = "ctx._source.putAll(params.fields)"
            
            # Perform the update
            update_result = await client.update_by_query(
                index=index_name,
                query=es_query,
                script={
                    "source": script_source,
                    "params": {"fields": update_fields},
                    "lang": "painless"
                },
                refresh=True,
                wait_for_completion=True
            )
            
            updated_count = update_result.get('updated', 0)
            
            logger.info(f"Updated metadata for camera {camera_id}: {updated_count} documents updated")
            
            return {
                "status": "success",
                "message": f"Successfully updated metadata for camera {camera_id}",
                "updated_count": updated_count,
                "updated_fields": list(update_fields.keys()),
                "camera_id": camera_id,
                "camera_name": stream_update.name if stream_update.name else "unchanged",
                "workspace_id": str(workspace_id),
                "index_name": index_name
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
    
    async def _scroll_all_documents(
        self, 
        client: AsyncElasticsearch, 
        index_name: str, 
        query: Dict,
        source_includes: Optional[List[str]] = None
    ) -> List[Dict]:
        """Fetch all documents using scroll API"""
        all_docs = []
        scroll_time = '2m'
        
        # Initial search
        response = await client.search(
            index=index_name,
            query=query,
            scroll=scroll_time,
            size=1000,
            _source=source_includes if source_includes else True
        )
        
        scroll_id = response.get('_scroll_id')
        hits = response['hits']['hits']
        all_docs.extend(hits)
        
        # Continue scrolling
        while len(hits) > 0:
            response = await client.scroll(
                scroll_id=scroll_id,
                scroll=scroll_time
            )
            scroll_id = response.get('_scroll_id')
            hits = response['hits']['hits']
            all_docs.extend(hits)
        
        # Clear scroll
        if scroll_id:
            try:
                await client.clear_scroll(scroll_id=scroll_id)
            except Exception as e:
                logger.warning(f"Failed to clear scroll: {e}")
        
        return all_docs
    
    def _format_document_data(self, doc: Dict, include_base64: bool = True) -> Dict[str, Any]:
        """Format document data for response"""
        source = doc.get('_source', {})
        frame_data = source.get("frame_base64") if include_base64 else None
        
        return {
            "id": doc.get('_id'),
            "frame": frame_data,
            "metadata": {
                "camera_id": source.get("camera_id"),
                "name": source.get("name", "Unknown Camera"),
                "timestamp": source.get("timestamp"),
                "date": source.get("date"),
                "time": source.get("time"),
                "person_count": source.get("person_count", 0),
                "male_count": source.get("male_count", 0),
                "female_count": source.get("female_count", 0),
                "fire_status": source.get("fire_status", "no detection"),
                "owner_username": source.get("username"),
                "location": source.get("location"),
                "area": source.get("area"),
                "building": source.get("building"),
                "floor_level": source.get("floor_level"),
                "zone": source.get("zone")
            }
        }
    
    def _prepare_prediction_data(self, documents: List[Dict]) -> List[Dict[str, Any]]:
        """Prepare data for prediction"""
        data = []
        for doc in documents:
            source = doc.get('_source', {})
            data.append({
                "metadata": {
                    "camera_id": source.get("camera_id"),
                    "timestamp": source.get("timestamp"),
                    "person_count": source.get("person_count", 0),
                    "male_count": source.get("male_count", 0),
                    "female_count": source.get("female_count", 0),
                    "fire_status": source.get("fire_status", "no detection")
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
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f}PB"

elasticsearch_service = ElasticsearchService()